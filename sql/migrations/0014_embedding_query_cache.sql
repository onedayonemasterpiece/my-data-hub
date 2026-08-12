CREATE TABLE search.query_embedding_768 (
    query_sha256 text NOT NULL CHECK (query_sha256 ~ '^[a-f0-9]{64}$'),
    model_id uuid NOT NULL REFERENCES search.embedding_model(model_id) ON DELETE RESTRICT,
    model_revision text NOT NULL CHECK (model_revision ~ '^[a-f0-9]{40}$'),
    vector_sha256 text NOT NULL CHECK (vector_sha256 ~ '^[a-f0-9]{64}$'),
    embedding halfvec(768) NOT NULL,
    created_revision bigint NOT NULL CHECK (created_revision >= 1),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(query_sha256,model_id,model_revision)
);
CREATE TABLE search.query_embedding_1024 (
    query_sha256 text NOT NULL CHECK (query_sha256 ~ '^[a-f0-9]{64}$'),
    model_id uuid NOT NULL REFERENCES search.embedding_model(model_id) ON DELETE RESTRICT,
    model_revision text NOT NULL CHECK (model_revision ~ '^[a-f0-9]{40}$'),
    vector_sha256 text NOT NULL CHECK (vector_sha256 ~ '^[a-f0-9]{64}$'),
    embedding halfvec(1024) NOT NULL,
    created_revision bigint NOT NULL CHECK (created_revision >= 1),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(query_sha256,model_id,model_revision)
);
CREATE TRIGGER query_embedding_768_append_only BEFORE UPDATE OR DELETE ON search.query_embedding_768
FOR EACH ROW EXECUTE FUNCTION hub_meta.reject_update_delete();
CREATE TRIGGER query_embedding_1024_append_only BEFORE UPDATE OR DELETE ON search.query_embedding_1024
FOR EACH ROW EXECUTE FUNCTION hub_meta.reject_update_delete();
CREATE CONSTRAINT TRIGGER mdh_epoch_write_guard AFTER INSERT OR UPDATE OR DELETE ON search.query_embedding_768
DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION master_control.enforce_write_epoch();
CREATE CONSTRAINT TRIGGER mdh_epoch_write_guard AFTER INSERT OR UPDATE OR DELETE ON search.query_embedding_1024
DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION master_control.enforce_write_epoch();
GRANT SELECT ON search.query_embedding_768,search.query_embedding_1024 TO mdh_mcp_reader,mdh_monitoring;
GRANT SELECT,INSERT ON search.query_embedding_768,search.query_embedding_1024 TO mdh_canonical_committer;
UPDATE hub.canonical_state SET schema_revision=14,updated_at=clock_timestamp() WHERE singleton=true;
