-- Bounded Russian blogger workload and separate exact E5/BGE search spaces.
CREATE SCHEMA search;

CREATE TABLE hub.project_actor (
    project_id          uuid NOT NULL REFERENCES hub.project(project_id) ON DELETE CASCADE,
    actor_id            uuid NOT NULL REFERENCES hub.actor(actor_id) ON DELETE RESTRICT,
    membership_kind     text NOT NULL DEFAULT 'blogger'
                        CHECK (membership_kind IN ('blogger', 'source', 'editor', 'owner', 'other')),
    status              text NOT NULL DEFAULT 'included'
                        CHECK (status IN ('candidate', 'included', 'excluded', 'archived')),
    provenance_event_id uuid REFERENCES hub.provenance_event(provenance_event_id) ON DELETE SET NULL,
    metadata            jsonb NOT NULL DEFAULT '{}'::jsonb,
    added_at            timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (project_id, actor_id, membership_kind)
);

CREATE TABLE region_talk.blogger_profile (
    actor_id                    uuid PRIMARY KEY REFERENCES hub.actor(actor_id) ON DELETE CASCADE,
    legacy_record_id            text NOT NULL UNIQUE,
    export_batch_id             uuid NOT NULL REFERENCES migration.export_batch(export_batch_id) ON DELETE RESTRICT,
    confirmation_status         text NOT NULL,
    region_relation_status      text NOT NULL,
    geography_signal            text,
    geography_provenance        text,
    source_updated_at           timestamptz NOT NULL,
    public_evidence_url         text,
    requires_review             boolean NOT NULL DEFAULT false,
    profile_revision            bigint NOT NULL DEFAULT 1 CHECK (profile_revision >= 1),
    created_at                  timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at                  timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TRIGGER blogger_profile_set_updated_at
BEFORE UPDATE ON region_talk.blogger_profile
FOR EACH ROW EXECUTE FUNCTION hub_meta.set_updated_at();

CREATE TABLE migration.duplicate_group (
    duplicate_group_id  uuid PRIMARY KEY,
    export_batch_id     uuid NOT NULL REFERENCES migration.export_batch(export_batch_id) ON DELETE RESTRICT,
    identity_kind       text NOT NULL CHECK (identity_kind IN ('account_url', 'handle', 'source_identity', 'other')),
    identity_hash       text NOT NULL CHECK (identity_hash ~ '^[a-f0-9]{64}$'),
    decision_status     text NOT NULL DEFAULT 'pending'
                        CHECK (decision_status IN ('pending', 'same_actor', 'different_actors', 'quarantined')),
    canonical_actor_id  uuid REFERENCES hub.actor(actor_id) ON DELETE RESTRICT,
    reason              text NOT NULL,
    decided_by          text,
    decided_at          timestamptz,
    created_at          timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (export_batch_id, identity_kind, identity_hash),
    CHECK ((decision_status = 'pending') = (decided_at IS NULL))
);

CREATE TABLE migration.duplicate_group_member (
    duplicate_group_id  uuid NOT NULL REFERENCES migration.duplicate_group(duplicate_group_id) ON DELETE RESTRICT,
    raw_record_id       uuid NOT NULL REFERENCES migration.raw_record(raw_record_id) ON DELETE RESTRICT,
    actor_id            uuid REFERENCES hub.actor(actor_id) ON DELETE RESTRICT,
    evidence            jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at          timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (duplicate_group_id, raw_record_id)
);

-- Raw source payloads and file manifests are immutable after landing. Status and
-- row-disposition tables remain the only mutable migration projections.
CREATE TRIGGER export_batch_kind_append_only
BEFORE UPDATE OR DELETE ON migration.export_batch_kind
FOR EACH ROW EXECUTE FUNCTION hub_meta.reject_update_delete();
CREATE TRIGGER export_file_append_only
BEFORE UPDATE OR DELETE ON migration.export_file
FOR EACH ROW EXECUTE FUNCTION hub_meta.reject_update_delete();
CREATE TRIGGER raw_record_append_only
BEFORE UPDATE OR DELETE ON migration.raw_record
FOR EACH ROW EXECUTE FUNCTION hub_meta.reject_update_delete();
CREATE TRIGGER duplicate_group_member_append_only
BEFORE UPDATE OR DELETE ON migration.duplicate_group_member
FOR EACH ROW EXECUTE FUNCTION hub_meta.reject_update_delete();

CREATE VIEW region_talk.bloggers_ru_v1 AS
SELECT
    actor.actor_id AS blogger_id,
    actor.display_name,
    actor.actor_type AS actor_kind,
    actor.summary AS public_description,
    profile.geography_signal,
    profile.geography_provenance,
    profile.legacy_record_id AS source_legacy_id,
    profile.source_updated_at,
    profile.confirmation_status,
    profile.requires_review,
    membership.project_id,
    coalesce(
        jsonb_agg(
            jsonb_build_object(
                'platform', account.platform,
                'handle', account.handle,
                'url', account.url
            ) ORDER BY account.platform, account.normalized_url
        ) FILTER (WHERE account.account_id IS NOT NULL),
        '[]'::jsonb
    ) AS public_accounts
FROM region_talk.blogger_profile profile
JOIN hub.actor actor ON actor.actor_id = profile.actor_id
JOIN hub.project_actor membership
  ON membership.actor_id = actor.actor_id
 AND membership.membership_kind = 'blogger'
 AND membership.status = 'included'
LEFT JOIN hub.external_account account
  ON account.actor_id = actor.actor_id
 AND account.status = 'active'
GROUP BY actor.actor_id, actor.display_name, actor.actor_type, actor.summary,
    profile.geography_signal, profile.geography_provenance, profile.legacy_record_id,
    profile.source_updated_at, profile.confirmation_status, profile.requires_review,
    membership.project_id;

CREATE TABLE search.document (
    search_document_id  uuid PRIMARY KEY,
    actor_id            uuid NOT NULL REFERENCES hub.actor(actor_id) ON DELETE CASCADE,
    representation_kind text NOT NULL CHECK (representation_kind IN ('blogger_compact_v1')),
    document_text       text NOT NULL CHECK (octet_length(document_text) BETWEEN 1 AND 32768),
    input_hash          text NOT NULL CHECK (input_hash ~ '^[a-f0-9]{64}$'),
    document_revision   bigint NOT NULL CHECK (document_revision >= 1),
    is_current          boolean NOT NULL DEFAULT true,
    source_revision     bigint NOT NULL CHECK (source_revision >= 0),
    search_vector       tsvector GENERATED ALWAYS AS (
        to_tsvector('pg_catalog.russian', document_text)
    ) STORED,
    created_at          timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (actor_id, representation_kind, input_hash)
);
CREATE INDEX search_document_fts_idx ON search.document USING gin(search_vector);
CREATE UNIQUE INDEX search_document_actor_current_idx ON search.document(actor_id, representation_kind)
    WHERE is_current;

CREATE TABLE search.embedding_model (
    model_id                uuid PRIMARY KEY,
    model_key               text NOT NULL UNIQUE,
    provider_model_id       text NOT NULL,
    exact_revision          text NOT NULL CHECK (exact_revision ~ '^[a-f0-9]{40}$'),
    dimensions              integer NOT NULL CHECK (dimensions IN (768, 1024)),
    normalization_contract  text NOT NULL,
    document_contract       text NOT NULL,
    query_contract          text NOT NULL,
    runtime_manifest_sha256 text CHECK (runtime_manifest_sha256 IS NULL OR runtime_manifest_sha256 ~ '^[a-f0-9]{64}$'),
    status                  text NOT NULL DEFAULT 'registered'
                            CHECK (status IN ('registered', 'active', 'retired', 'blocked')),
    created_at              timestamptz NOT NULL DEFAULT clock_timestamp()
);

INSERT INTO search.embedding_model (
    model_id, model_key, provider_model_id, exact_revision, dimensions,
    normalization_contract, document_contract, query_contract, status
) VALUES
    ('9c2c5d32-cdb7-5c3b-9d9f-50161df3e2b4', 'e5-multilingual-base-v1',
     'intfloat/multilingual-e5-base', 'd128750597153bb5987e10b1c3493a34e5a4502a', 768,
     'attention-mask-mean-pool+l2', 'passage-prefix|max512', 'query-prefix|max512', 'registered'),
    ('cc441a1c-b88b-564a-bf5e-e80458247367', 'bge-m3-dense-v1',
     'BAAI/bge-m3', '5617a9f61b028005a4858fdac845db406aefb181', 1024,
     'dense-only+l2', 'no-instruction|max8192', 'no-instruction|max8192', 'registered');

CREATE TABLE search.embedding_job (
    embedding_job_id   uuid PRIMARY KEY,
    search_document_id uuid NOT NULL REFERENCES search.document(search_document_id) ON DELETE CASCADE,
    representation_kind text NOT NULL,
    model_id            uuid NOT NULL REFERENCES search.embedding_model(model_id) ON DELETE RESTRICT,
    input_hash          text NOT NULL CHECK (input_hash ~ '^[a-f0-9]{64}$'),
    status              text NOT NULL DEFAULT 'pending'
                        CHECK (status IN ('pending', 'leased', 'running', 'succeeded', 'retryable_failed', 'dead', 'cancelled', 'quarantined')),
    attempt_count       integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    lease_until         timestamptz,
    result_sha256       text CHECK (result_sha256 IS NULL OR result_sha256 ~ '^[a-f0-9]{64}$'),
    failure_reason      text,
    created_at          timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at          timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (search_document_id, representation_kind, model_id, input_hash)
);
CREATE TRIGGER embedding_job_set_updated_at
BEFORE UPDATE ON search.embedding_job
FOR EACH ROW EXECUTE FUNCTION hub_meta.set_updated_at();

CREATE TABLE search.embedding_768 (
    search_document_id  uuid NOT NULL REFERENCES search.document(search_document_id) ON DELETE CASCADE,
    model_id            uuid NOT NULL REFERENCES search.embedding_model(model_id) ON DELETE RESTRICT,
    input_hash          text NOT NULL CHECK (input_hash ~ '^[a-f0-9]{64}$'),
    result_sha256       text NOT NULL CHECK (result_sha256 ~ '^[a-f0-9]{64}$'),
    embedding           halfvec(768) NOT NULL,
    created_at          timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (search_document_id, model_id, input_hash)
);
CREATE TRIGGER embedding_768_append_only
BEFORE UPDATE OR DELETE ON search.embedding_768
FOR EACH ROW EXECUTE FUNCTION hub_meta.reject_update_delete();

CREATE TABLE search.embedding_1024 (
    search_document_id  uuid NOT NULL REFERENCES search.document(search_document_id) ON DELETE CASCADE,
    model_id            uuid NOT NULL REFERENCES search.embedding_model(model_id) ON DELETE RESTRICT,
    input_hash          text NOT NULL CHECK (input_hash ~ '^[a-f0-9]{64}$'),
    result_sha256       text NOT NULL CHECK (result_sha256 ~ '^[a-f0-9]{64}$'),
    embedding           halfvec(1024) NOT NULL,
    created_at          timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (search_document_id, model_id, input_hash)
);
CREATE TRIGGER embedding_1024_append_only
BEFORE UPDATE OR DELETE ON search.embedding_1024
FOR EACH ROW EXECUTE FUNCTION hub_meta.reject_update_delete();

CREATE TABLE search.index_registry (
    index_key           text PRIMARY KEY,
    model_id            uuid NOT NULL REFERENCES search.embedding_model(model_id) ON DELETE RESTRICT,
    dimensions          integer NOT NULL CHECK (dimensions IN (768, 1024)),
    access_method       text NOT NULL DEFAULT 'exact' CHECK (access_method IN ('exact', 'hnsw')),
    status              text NOT NULL DEFAULT 'exact_only'
                        CHECK (status IN ('exact_only', 'candidate', 'active', 'rejected', 'retired')),
    benchmark_receipt_sha256 text CHECK (benchmark_receipt_sha256 IS NULL OR benchmark_receipt_sha256 ~ '^[a-f0-9]{64}$'),
    index_size_bytes    bigint CHECK (index_size_bytes IS NULL OR index_size_bytes >= 0),
    measured_recall     numeric CHECK (measured_recall IS NULL OR measured_recall BETWEEN 0 AND 1),
    measured_p95_ms     numeric CHECK (measured_p95_ms IS NULL OR measured_p95_ms >= 0),
    updated_at          timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (status <> 'active' OR (access_method = 'hnsw' AND benchmark_receipt_sha256 IS NOT NULL))
);

CREATE VIEW search.embedding_coverage AS
SELECT
    model.model_key,
    model.exact_revision,
    model.dimensions,
    count(document.search_document_id) FILTER (WHERE document.is_current) AS expected_documents,
    CASE model.dimensions
        WHEN 768 THEN count(e768.search_document_id) FILTER (WHERE document.is_current)
        WHEN 1024 THEN count(e1024.search_document_id) FILTER (WHERE document.is_current)
    END AS completed_documents
FROM search.embedding_model model
CROSS JOIN search.document document
LEFT JOIN search.embedding_768 e768
  ON model.dimensions = 768 AND e768.model_id = model.model_id
 AND e768.search_document_id = document.search_document_id AND e768.input_hash = document.input_hash
LEFT JOIN search.embedding_1024 e1024
  ON model.dimensions = 1024 AND e1024.model_id = model.model_id
 AND e1024.search_document_id = document.search_document_id AND e1024.input_hash = document.input_hash
GROUP BY model.model_key, model.exact_revision, model.dimensions;

-- New business relations must join the same commit-time epoch boundary introduced
-- by migration 0011. Read-only models/registries are intentionally excluded.
DO $$
DECLARE relation text;
BEGIN
    FOREACH relation IN ARRAY ARRAY[
        'hub.project_actor', 'region_talk.blogger_profile',
        'migration.duplicate_group', 'migration.duplicate_group_member',
        'search.document', 'search.embedding_job', 'search.embedding_768', 'search.embedding_1024'
    ] LOOP
        EXECUTE format(
            'CREATE CONSTRAINT TRIGGER mdh_epoch_write_guard '
            'AFTER INSERT OR UPDATE OR DELETE ON %s '
            'DEFERRABLE INITIALLY DEFERRED FOR EACH ROW '
            'EXECUTE FUNCTION master_control.enforce_write_epoch()', relation
        );
    END LOOP;
END
$$;

REVOKE ALL ON SCHEMA search FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA search FROM PUBLIC;
GRANT USAGE ON SCHEMA search TO mdh_mcp_reader, mdh_mcp_editor, mdh_canonical_committer, mdh_monitoring;
GRANT SELECT ON search.document, search.embedding_model, search.embedding_job,
    search.embedding_768, search.embedding_1024, search.index_registry,
    search.embedding_coverage TO mdh_mcp_reader, mdh_monitoring;
GRANT SELECT, INSERT, UPDATE ON search.document, search.embedding_job, search.index_registry
    TO mdh_canonical_committer;
GRANT SELECT, INSERT ON search.embedding_768, search.embedding_1024 TO mdh_canonical_committer;
GRANT SELECT ON region_talk.bloggers_ru_v1, migration.row_accounting,
    migration.batch_accounting TO mdh_mcp_reader;
REVOKE ALL ON migration.raw_record, migration.export_file FROM mdh_mcp_reader, mdh_mcp_editor;
REVOKE UPDATE, DELETE ON migration.raw_record, migration.export_file,
    migration.export_batch_kind FROM mdh_migration_operator;

UPDATE hub.canonical_state
SET schema_revision = 12,
    updated_at = clock_timestamp()
WHERE singleton = true;
