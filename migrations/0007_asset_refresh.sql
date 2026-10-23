-- Durable state for bounded whole-article refresh after confirmed signed-URL expiry.

ALTER TABLE jobs DROP CONSTRAINT IF EXISTS jobs_job_type_check;
ALTER TABLE jobs ADD CONSTRAINT jobs_job_type_check CHECK (
    job_type IN (
        'source_sync',
        'feed_rebuild',
        'article_backfill',
        'credential_refresh',
        'asset_repair',
        'asset_refresh'
    )
);

ALTER TABLE asset_repair_jobs
    ALTER COLUMN asset_record_id DROP NOT NULL;
ALTER TABLE asset_repair_jobs
    DROP CONSTRAINT IF EXISTS asset_repair_jobs_asset_record_id_fkey;
ALTER TABLE asset_repair_jobs
    ADD CONSTRAINT asset_repair_jobs_asset_record_id_fkey
    FOREIGN KEY (asset_record_id) REFERENCES asset_records (id) ON DELETE SET NULL;

ALTER TABLE asset_repair_jobs
    ADD COLUMN refresh_source_id UUID,
    ADD COLUMN refresh_review_id TEXT,
    ADD COLUMN chain_id UUID,
    ADD COLUMN chain_ordinal BIGINT,
    ADD COLUMN expired_asset_record_id UUID,
    ADD COLUMN expiry_refunded BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE asset_repair_jobs
    ADD CONSTRAINT asset_repair_jobs_refresh_shape_check CHECK (
        (
            refresh_source_id IS NULL
            AND refresh_review_id IS NULL
            AND chain_id IS NULL
            AND chain_ordinal IS NULL
            AND expired_asset_record_id IS NULL
            AND admission_class IN ('public_repair', 'internal_repair')
            AND (NOT expiry_refunded OR admission_class = 'public_repair')
        )
        OR (
            refresh_source_id IS NOT NULL
            AND refresh_review_id IS NOT NULL
            AND btrim(refresh_review_id) <> ''
            AND asset_record_id IS NULL
            AND chain_id IS NOT NULL
            AND chain_ordinal IS NOT NULL
            AND chain_ordinal > 0
            AND expired_asset_record_id IS NOT NULL
            AND admission_class = 'internal_refresh'
            AND expiry_refunded = FALSE
        )
    );

CREATE INDEX IF NOT EXISTS asset_repair_jobs_asset_idx
    ON asset_repair_jobs (asset_record_id, created_at DESC)
    WHERE asset_record_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS asset_repair_jobs_refresh_article_idx
    ON asset_repair_jobs (refresh_source_id, refresh_review_id)
    WHERE refresh_source_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS asset_repair_jobs_refresh_chain_idx
    ON asset_repair_jobs (expired_asset_record_id, chain_id, chain_ordinal)
    WHERE expired_asset_record_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS asset_repair_jobs_active_idx
    ON asset_repair_jobs (admission_class, created_at)
    WHERE admission_class IN ('public_repair', 'internal_repair', 'internal_refresh');

CREATE TABLE IF NOT EXISTS asset_article_refresh_lineage (
    source_id UUID NOT NULL,
    review_id TEXT NOT NULL CHECK (btrim(review_id) <> ''),
    attempts BIGINT NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_allowed_at TIMESTAMPTZ,
    exhausted BOOLEAN NOT NULL DEFAULT FALSE,
    last_outcome TEXT CHECK (
        last_outcome IS NULL
        OR last_outcome IN ('succeeded', 'inconclusive', 'superseded', 'deleted', 'exhausted')
    ),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (source_id, review_id),
    FOREIGN KEY (source_id, review_id)
        REFERENCES articles (source_id, review_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS asset_article_refresh_lineage_due_idx
    ON asset_article_refresh_lineage (next_allowed_at, source_id, review_id)
    WHERE NOT exhausted;

CREATE TABLE IF NOT EXISTS asset_record_refresh_state (
    asset_record_id UUID PRIMARY KEY,
    requested_generation BIGINT NOT NULL DEFAULT 0 CHECK (requested_generation >= 0),
    completed_generation BIGINT NOT NULL DEFAULT 0 CHECK (completed_generation >= 0),
    served_generation BIGINT NOT NULL DEFAULT 0 CHECK (served_generation >= 0),
    active_chain_id UUID,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (completed_generation <= served_generation),
    CHECK (served_generation <= requested_generation)
);

CREATE TABLE IF NOT EXISTS asset_refresh_generation_entries (
    asset_record_id UUID NOT NULL,
    generation BIGINT NOT NULL CHECK (generation > 0),
    ordinal BIGINT NOT NULL CHECK (ordinal > 0),
    source_id UUID NOT NULL,
    review_id TEXT NOT NULL CHECK (btrim(review_id) <> ''),
    state TEXT NOT NULL DEFAULT 'unseen' CHECK (
        state IN ('unseen', 'in_flight', 'cooling', 'done')
    ),
    not_before TIMESTAMPTZ,
    PRIMARY KEY (asset_record_id, generation, ordinal),
    UNIQUE (asset_record_id, generation, source_id, review_id),
    FOREIGN KEY (asset_record_id)
        REFERENCES asset_record_refresh_state (asset_record_id)
        ON DELETE CASCADE,
    CHECK ((state = 'cooling') = (not_before IS NOT NULL))
);

CREATE INDEX IF NOT EXISTS asset_refresh_generation_entries_select_idx
    ON asset_refresh_generation_entries
        (asset_record_id, generation, state, not_before, ordinal);

CREATE FUNCTION asset_refresh_generation_prunable(asset_id UUID, generation_id BIGINT)
RETURNS BOOLEAN
LANGUAGE sql
STABLE
AS $$
    SELECT EXISTS (
        SELECT 1 FROM asset_record_refresh_state s
        WHERE s.asset_record_id = asset_id
          AND s.completed_generation >= generation_id
          AND (s.active_chain_id IS NULL OR s.served_generation <> generation_id)
    )
    AND NOT EXISTS (
        SELECT 1 FROM asset_refresh_generation_entries e
        WHERE e.asset_record_id = asset_id AND e.generation = generation_id
          AND e.state <> 'done'
    )
    AND NOT EXISTS (
        SELECT 1 FROM asset_repair_jobs r JOIN jobs j ON j.id = r.job_id
        WHERE r.expired_asset_record_id = asset_id
          AND j.status IN ('queued', 'running', 'retry_wait', 'deferred')
          AND (j.payload_json->>'generation' IS NULL
               OR j.payload_json->>'generation' = generation_id::TEXT)
    );
$$;

CREATE FUNCTION reject_asset_refresh_snapshot_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        IF current_setting('werrss.asset_refresh_prune', TRUE) IS DISTINCT FROM 'on'
           OR NOT asset_refresh_generation_prunable(OLD.asset_record_id, OLD.generation) THEN
            RAISE EXCEPTION 'asset refresh generation snapshot entries are immutable';
        END IF;
        RETURN OLD;
    END IF;

    IF (NEW.asset_record_id, NEW.generation, NEW.ordinal, NEW.source_id, NEW.review_id)
       IS DISTINCT FROM
       (OLD.asset_record_id, OLD.generation, OLD.ordinal, OLD.source_id, OLD.review_id) THEN
        RAISE EXCEPTION 'asset refresh generation snapshot identity is immutable';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER asset_refresh_generation_entries_identity_immutable
    BEFORE UPDATE OR DELETE ON asset_refresh_generation_entries
    FOR EACH ROW
    EXECUTE FUNCTION reject_asset_refresh_snapshot_mutation();

CREATE FUNCTION prune_asset_refresh_generations()
RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
    removed BIGINT;
    previous_guard TEXT := current_setting('werrss.asset_refresh_prune', TRUE);
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended('asset-capacity', 0));
    PERFORM pg_advisory_xact_lock(hashtextextended('asset-repair-admission', 0));
    PERFORM set_config('werrss.asset_refresh_prune', 'on', TRUE);
    DELETE FROM asset_refresh_generation_entries e
    WHERE asset_refresh_generation_prunable(e.asset_record_id, e.generation);
    GET DIAGNOSTICS removed = ROW_COUNT;
    PERFORM set_config('werrss.asset_refresh_prune', COALESCE(previous_guard, ''), TRUE);
    RETURN removed;
END;
$$;

CREATE FUNCTION prune_orphan_asset_refresh_state()
RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
    removed BIGINT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended('asset-capacity', 0));
    PERFORM pg_advisory_xact_lock(hashtextextended('asset-repair-admission', 0));
    DELETE FROM asset_record_refresh_state s
    WHERE s.active_chain_id IS NULL
      AND s.requested_generation = s.completed_generation
      AND NOT EXISTS (
          SELECT 1 FROM asset_records a WHERE a.id = s.asset_record_id
      )
      AND NOT EXISTS (
          SELECT 1 FROM asset_refresh_generation_entries e
          WHERE e.asset_record_id = s.asset_record_id
      )
      AND NOT EXISTS (
          SELECT 1 FROM asset_repair_jobs r JOIN jobs j ON j.id = r.job_id
          WHERE j.status IN ('queued', 'running', 'retry_wait', 'deferred')
            AND (r.asset_record_id = s.asset_record_id
                 OR r.expired_asset_record_id = s.asset_record_id)
      )
      AND NOT EXISTS (
          SELECT 1 FROM jobs j
          WHERE j.job_type = 'asset_repair'
            AND j.payload_json->>'asset_id' = s.asset_record_id::TEXT
      );
    GET DIAGNOSTICS removed = ROW_COUNT;
    RETURN removed;
END;
$$;
