-- Durable admission state for public asset-cache misses.
-- The repair job is deliberately separate from source synchronization: a
-- public request may ask for one missing image without scheduling a source
-- fetch or requiring a browser worker.

ALTER TABLE jobs DROP CONSTRAINT IF EXISTS jobs_job_type_check;
ALTER TABLE jobs ADD CONSTRAINT jobs_job_type_check CHECK (
    job_type IN (
        'source_sync',
        'feed_rebuild',
        'article_backfill',
        'credential_refresh',
        'asset_repair'
    )
);

CREATE TABLE IF NOT EXISTS asset_repair_states (
    asset_record_id UUID PRIMARY KEY REFERENCES asset_records (id) ON DELETE CASCADE,
    admitted_attempts BIGINT NOT NULL DEFAULT 0 CHECK (admitted_attempts >= 0),
    next_allowed_at TIMESTAMPTZ,
    exhausted BOOLEAN NOT NULL DEFAULT FALSE,
    last_error TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS asset_repair_jobs (
    job_id UUID PRIMARY KEY REFERENCES jobs (id) ON DELETE CASCADE,
    asset_record_id UUID NOT NULL REFERENCES asset_records (id) ON DELETE CASCADE,
    admission_class TEXT NOT NULL DEFAULT 'public_repair' CHECK (
        admission_class IN ('public_repair', 'internal_repair', 'internal_refresh')
    ),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS asset_repair_jobs_asset_idx
    ON asset_repair_jobs (asset_record_id, created_at DESC);

CREATE INDEX IF NOT EXISTS asset_repair_jobs_active_idx
    ON asset_repair_jobs (admission_class, created_at)
    WHERE admission_class IN ('public_repair', 'internal_repair', 'internal_refresh');
