-- Durable coordination for blob operations that cannot be committed with the
-- PostgreSQL metadata transaction (filesystem/object-store backends).
CREATE TABLE IF NOT EXISTS asset_blob_deletion_queue (
    blob_id UUID PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS asset_blob_cleanup_queue (
    blob_id UUID PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
