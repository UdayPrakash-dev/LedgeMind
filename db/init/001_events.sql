-- Event store: one row per thing that happened. Rows are never updated or deleted.
CREATE TABLE IF NOT EXISTS events (
    id          UUID        PRIMARY KEY,                 -- event id (UUIDv7, made in Go)
    stream_id   TEXT        NOT NULL,                    -- which wallet / saga this event belongs to
    version     BIGINT      NOT NULL CHECK (version > 0),-- 1, 2, 3 ... per stream
    type        TEXT        NOT NULL,                    -- e.g. WalletOpened, WalletDebited
    payload     JSONB       NOT NULL DEFAULT '{}'::jsonb,-- event data
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- The key rule: a stream can never have two events with the same version.
    -- If two requests race to write version 5, Postgres rejects the second one.
    CONSTRAINT events_stream_version_uniq UNIQUE (stream_id, version)
);

-- Make the table append-only: block UPDATE and DELETE at the database level.
CREATE OR REPLACE FUNCTION events_block_changes() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'events table is append-only';
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS events_no_update ON events;
CREATE TRIGGER events_no_update
    BEFORE UPDATE OR DELETE ON events
    FOR EACH ROW EXECUTE FUNCTION events_block_changes();
