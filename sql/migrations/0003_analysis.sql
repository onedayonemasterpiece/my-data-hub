-- Immutable analysis/model evidence and model-specific vector projections.
CREATE TABLE analysis.model (
    model_id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    provider            text NOT NULL,
    name                text NOT NULL,
    version             text NOT NULL,
    task                text NOT NULL,
    dimensions          integer CHECK (dimensions IS NULL OR dimensions > 0),
    distance_metric     text CHECK (distance_metric IS NULL OR distance_metric IN ('cosine', 'l2', 'inner_product')),
    encoder_contract    text,
    configuration       jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at          timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX analysis_model_identity_uq
    ON analysis.model (provider, name, version, task, coalesce(encoder_contract, ''));

CREATE TABLE analysis.result (
    result_id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    content_id          uuid NOT NULL REFERENCES hub.content_item(content_id) ON DELETE CASCADE,
    project_id          uuid REFERENCES hub.project(project_id) ON DELETE CASCADE,
    model_id            uuid REFERENCES analysis.model(model_id) ON DELETE RESTRICT,
    result_kind         text NOT NULL,
    policy_version      text,
    input_fingerprint   text NOT NULL,
    output_fingerprint  text NOT NULL,
    status              text NOT NULL DEFAULT 'succeeded'
                        CHECK (status IN ('succeeded', 'partial', 'failed', 'quarantined', 'superseded')),
    result              jsonb NOT NULL,
    evidence            jsonb NOT NULL DEFAULT '{}'::jsonb,
    run_id              uuid,
    created_at          timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX analysis_result_content_idx
    ON analysis.result (content_id, result_kind, created_at DESC);
CREATE INDEX analysis_result_project_idx
    ON analysis.result (project_id, result_kind, created_at DESC)
    WHERE project_id IS NOT NULL;
CREATE UNIQUE INDEX analysis_result_identity_uq
    ON analysis.result (
        content_id,
        coalesce(project_id, '00000000-0000-0000-0000-000000000000'::uuid),
        coalesce(model_id, '00000000-0000-0000-0000-000000000000'::uuid),
        result_kind,
        coalesce(policy_version, ''),
        input_fingerprint,
        output_fingerprint
    );
CREATE TRIGGER analysis_result_append_only
BEFORE UPDATE OR DELETE ON analysis.result
FOR EACH ROW EXECUTE FUNCTION hub_meta.reject_update_delete();

CREATE TABLE analysis.embedding (
    embedding_id        uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    content_id          uuid NOT NULL REFERENCES hub.content_item(content_id) ON DELETE CASCADE,
    project_id          uuid REFERENCES hub.project(project_id) ON DELETE CASCADE,
    model_id            uuid NOT NULL REFERENCES analysis.model(model_id) ON DELETE RESTRICT,
    embedding_kind      text NOT NULL,
    dimensions          integer NOT NULL CHECK (dimensions IN (384, 768, 1024)),
    input_fingerprint   text NOT NULL,
    content_hash        text NOT NULL,
    vector_384          vector(384),
    vector_768          vector(768),
    vector_1024         vector(1024),
    metadata            jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at          timestamptz NOT NULL DEFAULT now(),
    CHECK (
        (dimensions = 384 AND vector_384 IS NOT NULL AND vector_768 IS NULL AND vector_1024 IS NULL)
        OR (dimensions = 768 AND vector_384 IS NULL AND vector_768 IS NOT NULL AND vector_1024 IS NULL)
        OR (dimensions = 1024 AND vector_384 IS NULL AND vector_768 IS NULL AND vector_1024 IS NOT NULL)
    )
);
CREATE UNIQUE INDEX analysis_embedding_identity_uq
    ON analysis.embedding (
        content_id,
        coalesce(project_id, '00000000-0000-0000-0000-000000000000'::uuid),
        model_id,
        embedding_kind,
        input_fingerprint
    );
CREATE INDEX embedding_content_idx
    ON analysis.embedding (content_id, embedding_kind, created_at DESC);
CREATE INDEX embedding_384_hnsw_cosine_idx
    ON analysis.embedding USING hnsw (vector_384 vector_cosine_ops)
    WHERE dimensions = 384;
CREATE INDEX embedding_768_hnsw_cosine_idx
    ON analysis.embedding USING hnsw (vector_768 vector_cosine_ops)
    WHERE dimensions = 768;
CREATE INDEX embedding_1024_hnsw_cosine_idx
    ON analysis.embedding USING hnsw (vector_1024 vector_cosine_ops)
    WHERE dimensions = 1024;
CREATE TRIGGER embedding_append_only
BEFORE UPDATE OR DELETE ON analysis.embedding
FOR EACH ROW EXECUTE FUNCTION hub_meta.reject_update_delete();
