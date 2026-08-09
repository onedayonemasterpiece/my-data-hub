-- Bootstrap PostgreSQL capabilities and shared schemas.
-- Target: PostgreSQL 18 + pgvector.
CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS citext;
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE SCHEMA IF NOT EXISTS hub_meta;
CREATE SCHEMA IF NOT EXISTS hub;
CREATE SCHEMA IF NOT EXISTS analysis;
CREATE SCHEMA IF NOT EXISTS orchestration;
CREATE SCHEMA IF NOT EXISTS sync;
CREATE SCHEMA IF NOT EXISTS region_talk;
CREATE SCHEMA IF NOT EXISTS migration;
CREATE SCHEMA IF NOT EXISTS joplin;

CREATE OR REPLACE FUNCTION hub_meta.set_updated_at()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION hub_meta.reject_update_delete()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION '% is append-only', TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME;
END;
$$;

CREATE TABLE hub.canonical_state (
    singleton           boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    canonical_revision  bigint NOT NULL DEFAULT 0 CHECK (canonical_revision >= 0),
    schema_revision     integer NOT NULL DEFAULT 1 CHECK (schema_revision >= 1),
    updated_at          timestamptz NOT NULL DEFAULT now(),
    metadata            jsonb NOT NULL DEFAULT '{}'::jsonb
);

INSERT INTO hub.canonical_state (singleton)
VALUES (true)
ON CONFLICT (singleton) DO NOTHING;
