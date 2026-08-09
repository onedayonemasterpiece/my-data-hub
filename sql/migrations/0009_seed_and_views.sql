-- Initial seed, operational read models and safe lease helpers.
-- Region Talk is present but paused until the migration/cutover gates are satisfied.
INSERT INTO hub.project (slug, name, description, status, metadata)
VALUES (
    'region-talk',
    'Region Talk / О Калининграде говорят',
    'Первый workload my-data-hub: поиск и редакционный отбор внешних публикаций о Калининградской области.',
    'paused',
    '{"first_workload": true, "legacy_store": "ydb", "migration_required": true}'::jsonb
)
ON CONFLICT (slug) DO UPDATE
SET description = EXCLUDED.description,
    metadata = hub.project.metadata || EXCLUDED.metadata;

INSERT INTO analysis.model (
    provider, name, version, task, dimensions, distance_metric,
    encoder_contract, configuration
)
VALUES
    (
        'intfloat', 'multilingual-e5-base', 'UNPINNED', 'text-embedding',
        768, 'cosine', 'e5_semantic_bank_scores_v1',
        '{"production_ready": false, "pin_before_activation": true}'::jsonb
    ),
    (
        'BAAI', 'bge-m3', 'UNPINNED', 'text-embedding',
        1024, 'cosine', 'bge_m3_flagembedding_dense_v1',
        '{"production_ready": false, "pin_before_activation": true}'::jsonb
    )
ON CONFLICT DO NOTHING;

CREATE OR REPLACE VIEW orchestration.queue_summary AS
SELECT
    p.workload,
    p.name AS pipeline_name,
    p.version AS pipeline_version,
    p.status AS pipeline_status,
    ps.stage_key,
    wi.status,
    count(wi.work_item_id) AS item_count,
    min(wi.created_at) AS oldest_created_at,
    min(wi.available_at) AS earliest_available_at
FROM orchestration.pipeline p
JOIN orchestration.pipeline_stage ps ON ps.pipeline_id = p.pipeline_id
LEFT JOIN orchestration.work_item wi ON wi.stage_id = ps.stage_id
GROUP BY p.workload, p.name, p.version, p.status, ps.stage_key, wi.status;

CREATE OR REPLACE VIEW orchestration.queue_health AS
SELECT
    p.workload,
    ps.stage_key,
    count(wi.work_item_id) FILTER (
        WHERE wi.status IN ('pending', 'failed_retryable')
          AND wi.available_at <= now()
    ) AS actionable_count,
    count(wi.work_item_id) FILTER (
        WHERE wi.status IN ('leased', 'running')
          AND wi.lease_expires_at < now()
    ) AS expired_lease_count,
    min(wi.available_at) FILTER (
        WHERE wi.status IN ('pending', 'failed_retryable')
    ) AS oldest_actionable_at,
    max(wi.attempt_count) AS max_attempt_count
FROM orchestration.pipeline p
JOIN orchestration.pipeline_stage ps ON ps.pipeline_id = p.pipeline_id
LEFT JOIN orchestration.work_item wi ON wi.stage_id = ps.stage_id
GROUP BY p.workload, ps.stage_key;

CREATE OR REPLACE VIEW migration.region_talk_accounting AS
SELECT
    export_batch_id,
    row_kind,
    expected_row_count,
    raw_count,
    normalized_count,
    deduplicated_count,
    intentionally_excluded_count,
    retained_raw_count,
    quarantined_count,
    undispositioned_count,
    raw_count_matches_manifest,
    fully_accounted,
    cutover_ready
FROM migration.row_accounting;

CREATE OR REPLACE VIEW region_talk.funnel_current AS
SELECT
    (SELECT count(*) FROM region_talk.source) AS sources_total,
    (SELECT count(*) FROM region_talk.source WHERE status = 'active') AS sources_active,
    (SELECT count(*)
       FROM hub.content_item ci
       JOIN hub.project_content pc USING (content_id)
       JOIN hub.project p USING (project_id)
      WHERE p.slug = 'region-talk') AS content_total,
    (SELECT count(*) FROM region_talk.post_evaluation WHERE eligible) AS text_eligible,
    (SELECT count(DISTINCT content_id)
       FROM region_talk.image_evaluation
      WHERE verdict IN ('strong', 'acceptable')) AS media_ready,
    (SELECT count(*) FROM region_talk.publication_candidate WHERE status = 'ready') AS ready_candidates,
    (SELECT count(*) FROM region_talk.publication_candidate WHERE status = 'approved') AS approved_candidates,
    (SELECT count(*) FROM region_talk.publication_candidate WHERE status = 'published') AS published_candidates;

CREATE OR REPLACE VIEW sync.outbox_health AS
SELECT status, count(*) AS item_count, min(created_at) AS oldest_created_at
FROM sync.external_outbox
GROUP BY status;

CREATE OR REPLACE FUNCTION orchestration.claim_work_items(
    p_stage_id uuid,
    p_lease_owner text,
    p_limit integer,
    p_lease_seconds integer
)
RETURNS SETOF orchestration.work_item
LANGUAGE plpgsql
AS $$
BEGIN
    IF p_limit < 1 OR p_limit > 500 THEN
        RAISE EXCEPTION 'claim limit must be between 1 and 500';
    END IF;
    IF p_lease_seconds < 30 OR p_lease_seconds > 86400 THEN
        RAISE EXCEPTION 'lease seconds must be between 30 and 86400';
    END IF;
    IF length(trim(p_lease_owner)) = 0 THEN
        RAISE EXCEPTION 'lease owner must not be empty';
    END IF;

    RETURN QUERY
    WITH candidates AS (
        SELECT wi.work_item_id
        FROM orchestration.work_item wi
        JOIN orchestration.pipeline_stage ps ON ps.stage_id = wi.stage_id
        JOIN orchestration.pipeline p ON p.pipeline_id = wi.pipeline_id
        WHERE wi.stage_id = p_stage_id
          AND p.status = 'active'
          AND ps.enabled = true
          AND (
              (wi.status IN ('pending', 'failed_retryable') AND wi.available_at <= now())
              OR (wi.status IN ('leased', 'running') AND wi.lease_expires_at < now())
          )
          AND wi.attempt_count < ps.max_attempts
          AND NOT EXISTS (
              SELECT 1
              FROM orchestration.work_item_dependency d
              JOIN orchestration.work_item parent
                ON parent.work_item_id = d.depends_on_work_item_id
              WHERE d.work_item_id = wi.work_item_id
                AND parent.status <> 'succeeded'
          )
        ORDER BY wi.priority ASC, wi.available_at ASC, wi.queue_seq ASC
        FOR UPDATE OF wi SKIP LOCKED
        LIMIT p_limit
    )
    UPDATE orchestration.work_item wi
       SET status = 'leased',
           lease_owner = p_lease_owner,
           lease_token = gen_random_uuid(),
           lease_expires_at = now() + make_interval(secs => p_lease_seconds),
           attempt_count = wi.attempt_count + 1,
           updated_at = now()
      FROM candidates c
     WHERE wi.work_item_id = c.work_item_id
    RETURNING wi.*;
END;
$$;

UPDATE hub.canonical_state
SET schema_revision = 9,
    updated_at = now()
WHERE singleton = true;
