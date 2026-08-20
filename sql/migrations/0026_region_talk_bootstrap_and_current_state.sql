-- Region Talk v5: deterministic paused pipeline bootstrap and monotonic current-state replay.
-- Earlier migrations remain immutable.  This migration makes a fresh master sufficient for
-- source-frontier canonicalization while retaining fail-closed publication/notification gates.

DO $migration$
DECLARE
    v_pipeline_id uuid;
    v_definition jsonb := $definition${"schema_version":"my-data-hub-pipeline.v1","workload":"region-talk","name":"region-talk-main","version":"1.0.0","status":"paused","planning_policy":"region-talk-pressure-aware.v1","stages":[{"key":"reconcile_worker_results","version":"v1","compute_lane":"local","priority":10,"max_attempts":5,"timeout_seconds":300,"contract":"my-data-hub-notebook-result.v1"},{"key":"exact_url_intake","version":"v1","compute_lane":"local","priority":20,"max_attempts":5,"timeout_seconds":300,"contract":"region-talk.exact-url.v1"},{"key":"source_discovery","version":"v1","compute_lane":"kaggle-candidate-report","priority":110,"max_attempts":3,"timeout_seconds":1200,"contract":"region-talk.source-discovery.v1"},{"key":"post_discovery","version":"v1","compute_lane":"kaggle-candidate-report","priority":100,"max_attempts":3,"timeout_seconds":1200,"contract":"region-talk.post-discovery.v1"},{"key":"e5_embedding","version":"v1","compute_lane":"kaggle-e5","priority":60,"max_attempts":3,"timeout_seconds":900,"contract":"e5_semantic_bank_scores_v1"},{"key":"bge_m3_embedding","version":"v1","compute_lane":"kaggle-bge-m3","priority":50,"max_attempts":3,"timeout_seconds":1200,"contract":"bge_m3_flagembedding_dense_v1"},{"key":"vector_fusion","version":"v1","compute_lane":"local","priority":40,"max_attempts":3,"timeout_seconds":300,"contract":"region-talk.vector-fusion.v1"},{"key":"text_eligibility","version":"v5","compute_lane":"local","priority":35,"max_attempts":3,"timeout_seconds":300,"contract":"region_talk_publication_eligibility_v5"},{"key":"image_scoring","version":"v1","compute_lane":"kaggle-image-diagnostic","priority":30,"max_attempts":3,"timeout_seconds":1200,"contract":"region-talk.image-diagnostic.v1"},{"key":"source_profile","version":"v1","compute_lane":"kaggle-source-profile","priority":28,"max_attempts":3,"timeout_seconds":1200,"contract":"region-talk.source-profile.v1"},{"key":"final_verifier","version":"v1","compute_lane":"local","priority":25,"max_attempts":3,"timeout_seconds":600,"contract":"region-talk.final-verifier.v1"},{"key":"writer","version":"v1","compute_lane":"kaggle-writer","priority":18,"max_attempts":3,"timeout_seconds":900,"contract":"region-talk.writer.v1"},{"key":"review_dispatch","version":"v1","compute_lane":"local-side-effect","priority":15,"max_attempts":5,"timeout_seconds":300,"contract":"region-talk.review-card.v1"},{"key":"review_sync","version":"v1","compute_lane":"local","priority":12,"max_attempts":5,"timeout_seconds":300,"contract":"region-talk.review-decision.v1"},{"key":"publication_plan","version":"v1","compute_lane":"local","priority":11,"max_attempts":3,"timeout_seconds":300,"contract":"region-talk.publication-plan.v1"},{"key":"publication_dispatch","version":"v1","compute_lane":"local-side-effect","priority":10,"max_attempts":5,"timeout_seconds":300,"contract":"region-talk.publication-receipt.v1","enabled_by_default":false},{"key":"health_metrics","version":"v1","compute_lane":"local","priority":200,"max_attempts":2,"timeout_seconds":120,"contract":"region-talk.health-metrics.v1"}]}$definition$::jsonb;
    v_stage jsonb;
BEGIN
    INSERT INTO orchestration.pipeline(workload,name,version,status,definition)
    VALUES('region-talk','region-talk-main','1.0.0','paused',v_definition)
    ON CONFLICT(workload,name,version) DO UPDATE
       SET status='paused',definition=excluded.definition,updated_at=clock_timestamp()
    RETURNING pipeline_id INTO v_pipeline_id;

    FOR v_stage IN SELECT value FROM jsonb_array_elements(v_definition->'stages') LOOP
        INSERT INTO orchestration.pipeline_stage(
            pipeline_id,stage_key,stage_version,compute_lane,priority,max_attempts,
            timeout_seconds,contract,enabled
        ) VALUES(
            v_pipeline_id,v_stage->>'key',v_stage->>'version',v_stage->>'compute_lane',
            (v_stage->>'priority')::integer,(v_stage->>'max_attempts')::integer,
            (v_stage->>'timeout_seconds')::integer,
            jsonb_build_object('name',v_stage->>'contract'),
            coalesce((v_stage->>'enabled_by_default')::boolean,true)
        )
        ON CONFLICT(pipeline_id,stage_key,stage_version) DO UPDATE SET
            compute_lane=excluded.compute_lane,priority=excluded.priority,
            max_attempts=excluded.max_attempts,timeout_seconds=excluded.timeout_seconds,
            contract=excluded.contract,enabled=excluded.enabled,updated_at=clock_timestamp();
    END LOOP;

    IF NOT EXISTS(
        SELECT 1 FROM orchestration.pipeline pipeline
        JOIN orchestration.pipeline_stage stage USING(pipeline_id)
        WHERE pipeline.pipeline_id=v_pipeline_id AND pipeline.status='paused'
          AND stage.stage_key='source_discovery' AND stage.stage_version='v1' AND stage.enabled
    ) OR NOT EXISTS(
        SELECT 1 FROM orchestration.pipeline_stage stage
        WHERE stage.pipeline_id=v_pipeline_id AND stage.stage_key='publication_dispatch'
          AND stage.stage_version='v1' AND NOT stage.enabled
    ) THEN
        RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='Region Talk paused pipeline registration is incomplete';
    END IF;
END
$migration$;

-- Exact-payload observations may advance their accepted revision and evidence pointer, but
-- they must never move the source clock backwards.  Changed payloads older than that
-- monotonic clock remain stale even when they arrive in a later canonical revision.
CREATE OR REPLACE FUNCTION migration.region_talk_claim_canonical_state(
    requested_identity_kind text,requested_identity_key text,requested_target_table text,
    requested_target_id uuid,requested_raw_record_id uuid,requested_export_batch_id uuid,
    incoming_source_updated_at timestamptz,incoming_payload_sha256 text,
    requested_canonical_revision bigint
) RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog
AS $$
DECLARE
    head migration.region_talk_canonical_state_head%ROWTYPE;
    v_disposition text; should_apply boolean:=false;
BEGIN
    IF requested_identity_kind NOT IN(
        'source_candidate','source_status','work_item','publication_plan','review_decision'
    ) OR requested_identity_key IS NULL OR length(requested_identity_key) NOT BETWEEN 1 AND 5000
       OR incoming_payload_sha256 !~ '^[a-f0-9]{64}$' OR requested_canonical_revision<1 THEN
        RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='canonical current-state observation is invalid';
    END IF;
    SELECT * INTO head FROM migration.region_talk_canonical_state_head current_head
     WHERE current_head.identity_kind=requested_identity_kind
       AND current_head.identity_key=requested_identity_key FOR UPDATE;
    IF NOT FOUND THEN
        INSERT INTO migration.region_talk_canonical_state_head(
            identity_kind,identity_key,target_table,target_id,source_updated_at,payload_sha256,
            canonical_revision,export_batch_id,raw_record_id
        ) VALUES(requested_identity_kind,requested_identity_key,requested_target_table,requested_target_id,
                 incoming_source_updated_at,incoming_payload_sha256,requested_canonical_revision,
                 requested_export_batch_id,requested_raw_record_id);
        v_disposition:='initial';
    ELSE
        IF head.target_table<>requested_target_table OR head.target_id<>requested_target_id THEN
            RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='canonical current-state target identity changed';
        END IF;
        IF incoming_payload_sha256=head.payload_sha256 THEN
            v_disposition:='replay';
            UPDATE migration.region_talk_canonical_state_head current_head
               SET canonical_revision=requested_canonical_revision,
                   export_batch_id=requested_export_batch_id,raw_record_id=requested_raw_record_id,
                   source_updated_at=CASE
                       WHEN current_head.source_updated_at IS NULL THEN incoming_source_updated_at
                       WHEN incoming_source_updated_at IS NULL THEN current_head.source_updated_at
                       ELSE greatest(current_head.source_updated_at,incoming_source_updated_at) END,
                   updated_at=clock_timestamp()
             WHERE current_head.identity_kind=requested_identity_kind
               AND current_head.identity_key=requested_identity_key;
        ELSIF incoming_source_updated_at IS NULL AND head.source_updated_at IS NOT NULL
           OR incoming_source_updated_at<head.source_updated_at
           OR requested_canonical_revision<=head.canonical_revision THEN
            v_disposition:='stale';
        ELSE
            v_disposition:='applied'; should_apply:=true;
            UPDATE migration.region_talk_canonical_state_head current_head
               SET source_updated_at=incoming_source_updated_at,payload_sha256=incoming_payload_sha256,
                   canonical_revision=requested_canonical_revision,export_batch_id=requested_export_batch_id,
                   raw_record_id=requested_raw_record_id,updated_at=clock_timestamp()
             WHERE current_head.identity_kind=requested_identity_kind
               AND current_head.identity_key=requested_identity_key;
        END IF;
    END IF;
    INSERT INTO migration.region_talk_canonical_state_observation(
        raw_record_id,identity_kind,identity_key,target_table,target_id,source_updated_at,
        payload_sha256,canonical_revision,export_batch_id,disposition,prior_payload_sha256
    ) VALUES(requested_raw_record_id,requested_identity_kind,requested_identity_key,
             requested_target_table,requested_target_id,incoming_source_updated_at,
             incoming_payload_sha256,requested_canonical_revision,requested_export_batch_id,
             v_disposition,head.payload_sha256)
    ON CONFLICT(raw_record_id) DO NOTHING;
    RETURN should_apply;
END
$$;

-- Migration 0024 already persisted the first immutable source_status row.  When 0025
-- establishes its first current-state head, apply only the mutable source projection;
-- later changed observations continue through the normal ordered refresh function.
CREATE FUNCTION migration.apply_region_talk_initial_source_status()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog
AS $$
DECLARE
    raw migration.raw_record%ROWTYPE;
    body jsonb;
    v_status text;
BEGIN
    IF NEW.identity_kind<>'source_status' OR NEW.target_table<>'region_talk.source' THEN
        RETURN NEW;
    END IF;
    SELECT * INTO STRICT raw FROM migration.raw_record
     WHERE raw_record_id=NEW.raw_record_id AND export_batch_id=NEW.export_batch_id;
    body:=migration.region_talk_direct_body(raw.payload);
    v_status:=lower(coalesce(body->>'source_queue_status',body->>'queue_status',
                             body->>'status',body->>'state','unknown'));
    UPDATE region_talk.source source
       SET status=CASE v_status WHEN 'active' THEN 'active' WHEN 'paused' THEN 'paused'
                  WHEN 'excluded' THEN 'excluded' WHEN 'terminal' THEN 'terminal'
                  ELSE source.status END,
           evidence=source.evidence||jsonb_build_object(
               'current_status_raw_record_id',raw.raw_record_id,
               'current_status_export_batch_id',NEW.export_batch_id)
     WHERE source.source_id=NEW.target_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='initial source status target is absent';
    END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER region_talk_canonical_state_head_initial_status
AFTER INSERT ON migration.region_talk_canonical_state_head
FOR EACH ROW EXECUTE FUNCTION migration.apply_region_talk_initial_source_status();


-- Durable, typed handshake for post-import queue formation.  The pipeline LOGIN
-- cannot choose SQL, tables, stages, or side effects; it submits only this fixed DAG.
CREATE TABLE migration.region_talk_post_import_stage_run (
    stage_run_id          uuid PRIMARY KEY,
    task_run_id           uuid NOT NULL,
    export_batch_id       uuid NOT NULL UNIQUE
                          REFERENCES migration.region_talk_direct_snapshot(export_batch_id) ON DELETE RESTRICT,
    canonical_revision    bigint NOT NULL CHECK(canonical_revision>=1),
    prepare_request_sha256 text NOT NULL CHECK(prepare_request_sha256 ~ '^[a-f0-9]{64}$'),
    preparation_sha256    text NOT NULL UNIQUE CHECK(preparation_sha256 ~ '^[a-f0-9]{64}$'),
    preparation           jsonb NOT NULL,
    state                 text NOT NULL CHECK(state IN('PREPARED','COMPLETE','WAITING_WORK','FAILED')),
    commit_request_sha256 text CHECK(commit_request_sha256 IS NULL OR commit_request_sha256 ~ '^[a-f0-9]{64}$'),
    final_receipt_sha256  text CHECK(final_receipt_sha256 IS NULL OR final_receipt_sha256 ~ '^[a-f0-9]{64}$'),
    final_receipt         jsonb,
    created_at            timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at          timestamptz,
    UNIQUE(task_run_id,export_batch_id),
    CHECK ((state='PREPARED')=(final_receipt IS NULL)),
    CHECK ((final_receipt IS NULL)=(final_receipt_sha256 IS NULL)),
    CHECK ((final_receipt IS NULL)=(commit_request_sha256 IS NULL))
);

CREATE TABLE migration.region_talk_post_import_candidate_outcome (
    stage_run_id          uuid NOT NULL
                          REFERENCES migration.region_talk_post_import_stage_run(stage_run_id) ON DELETE RESTRICT,
    candidate_id          uuid NOT NULL,
    candidate_revision    integer NOT NULL CHECK(candidate_revision>=1),
    revision_fingerprint  text NOT NULL CHECK(revision_fingerprint ~ '^[a-f0-9]{64}$'),
    disposition           text NOT NULL CHECK(disposition IN('QUEUED_REVIEW','WAITING_WORK','FAILED_TERMINAL')),
    review_basis          text CHECK(review_basis IN('LEGACY_SELECTED','CURRENT_EVIDENCE')),
    queue_rank            integer CHECK(queue_rank IS NULL OR queue_rank>=1),
    outcome               jsonb NOT NULL,
    created_at            timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(stage_run_id,candidate_id,candidate_revision),
    FOREIGN KEY(candidate_id,candidate_revision)
        REFERENCES region_talk.candidate_revision(candidate_id,revision) ON DELETE RESTRICT,
    CHECK ((disposition='QUEUED_REVIEW')=(queue_rank IS NOT NULL))
);

CREATE TABLE migration.region_talk_post_import_stage_receipt (
    stage_run_id          uuid NOT NULL
                          REFERENCES migration.region_talk_post_import_stage_run(stage_run_id) ON DELETE RESTRICT,
    stage                 text NOT NULL,
    contract_version      text NOT NULL,
    status                text NOT NULL CHECK(status IN(
                              'SUCCEEDED','WAITING_WORK','SKIPPED_BLOCKED',
                              'FAILED_RETRYABLE','FAILED_TERMINAL')),
    attempt               integer NOT NULL CHECK(attempt>=1),
    max_attempts          integer NOT NULL CHECK(max_attempts BETWEEN 1 AND 100),
    timeout_seconds       integer NOT NULL CHECK(timeout_seconds BETWEEN 1 AND 86400),
    rows_observed         bigint NOT NULL CHECK(rows_observed>=0),
    rows_changed          bigint NOT NULL CHECK(rows_changed>=0),
    work_request_count    bigint NOT NULL CHECK(work_request_count>=0),
    input_sha256          text NOT NULL CHECK(input_sha256 ~ '^[a-f0-9]{64}$'),
    output_sha256         text NOT NULL CHECK(output_sha256 ~ '^[a-f0-9]{64}$'),
    receipt_sha256        text NOT NULL CHECK(receipt_sha256 ~ '^[a-f0-9]{64}$'),
    receipt               jsonb NOT NULL,
    started_at            timestamptz NOT NULL,
    completed_at          timestamptz NOT NULL,
    PRIMARY KEY(stage_run_id,stage),
    CHECK(completed_at>=started_at)
);

CREATE TABLE region_talk.post_import_review_queue (
    stage_run_id          uuid NOT NULL
                          REFERENCES migration.region_talk_post_import_stage_run(stage_run_id) ON DELETE RESTRICT,
    candidate_id          uuid NOT NULL,
    candidate_revision    integer NOT NULL,
    queue_rank            integer NOT NULL CHECK(queue_rank>=1),
    review_basis          text NOT NULL CHECK(review_basis IN('LEGACY_SELECTED','CURRENT_EVIDENCE')),
    revision_fingerprint  text NOT NULL CHECK(revision_fingerprint ~ '^[a-f0-9]{64}$'),
    publication_dispatch  boolean NOT NULL DEFAULT false CHECK(NOT publication_dispatch),
    notification_dispatch boolean NOT NULL DEFAULT false CHECK(NOT notification_dispatch),
    created_at            timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(stage_run_id,candidate_id,candidate_revision),
    UNIQUE(stage_run_id,queue_rank),
    FOREIGN KEY(candidate_id,candidate_revision)
        REFERENCES region_talk.candidate_revision(candidate_id,revision) ON DELETE RESTRICT
);

CREATE TRIGGER region_talk_post_import_candidate_outcome_append_only
BEFORE UPDATE OR DELETE ON migration.region_talk_post_import_candidate_outcome
FOR EACH ROW EXECUTE FUNCTION hub_meta.reject_update_delete();
CREATE TRIGGER region_talk_post_import_stage_receipt_append_only
BEFORE UPDATE OR DELETE ON migration.region_talk_post_import_stage_receipt
FOR EACH ROW EXECUTE FUNCTION hub_meta.reject_update_delete();
CREATE TRIGGER region_talk_post_import_review_queue_append_only
BEFORE UPDATE OR DELETE ON region_talk.post_import_review_queue
FOR EACH ROW EXECUTE FUNCTION hub_meta.reject_update_delete();

CREATE FUNCTION migration.region_talk_stage_uuid5(value text)
RETURNS uuid
LANGUAGE plpgsql
IMMUTABLE
STRICT
SET search_path=pg_catalog
AS $$
DECLARE
    digest_hex text;
    variant integer;
BEGIN
    digest_hex:=encode(public.digest(
        decode('54a0dba71e4b4d56a143173304989e85','hex')||convert_to(value,'UTF8'),'sha1'),'hex');
    digest_hex:=overlay(digest_hex placing '5' from 13 for 1);
    variant:=((get_byte(decode(substr(digest_hex,17,2),'hex'),0)>>4) & 3)+8;
    digest_hex:=overlay(digest_hex placing substr('0123456789abcdef',variant+1,1) from 17 for 1);
    RETURN (substr(digest_hex,1,8)||'-'||substr(digest_hex,9,4)||'-'||substr(digest_hex,13,4)||'-'||
            substr(digest_hex,17,4)||'-'||substr(digest_hex,21,12))::uuid;
END
$$;

CREATE FUNCTION migration.execute_region_talk_post_import_stages(
    requested_task_run_id uuid,requested_export_batch_id uuid,requested_request jsonb
) RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog
AS $$
DECLARE
    fixed_dag constant jsonb := '[
      {"stage":"canonical_import","contract_version":"region-talk-direct-snapshot-receipt.v2","dependencies":[],"max_attempts":1,"timeout_seconds":3600},
      {"stage":"e5_embedding","contract_version":"e5_semantic_bank_scores_v1","dependencies":["canonical_import"],"max_attempts":3,"timeout_seconds":900},
      {"stage":"bge_m3_embedding","contract_version":"bge_m3_flagembedding_dense_v1","dependencies":["canonical_import"],"max_attempts":3,"timeout_seconds":1200},
      {"stage":"vector_fusion","contract_version":"region-talk.vector-fusion.v1","dependencies":["e5_embedding","bge_m3_embedding"],"max_attempts":3,"timeout_seconds":300},
      {"stage":"image_scoring","contract_version":"region-talk.image-diagnostic.v1","dependencies":["vector_fusion"],"max_attempts":3,"timeout_seconds":1200},
      {"stage":"final_verifier","contract_version":"region-talk.final-verifier.v1","dependencies":["image_scoring"],"max_attempts":3,"timeout_seconds":600},
      {"stage":"writer","contract_version":"region-talk.writer.v1","dependencies":["final_verifier"],"max_attempts":3,"timeout_seconds":900},
      {"stage":"review_queue","contract_version":"region-talk.review-queue.v1","dependencies":["writer"],"max_attempts":3,"timeout_seconds":300}
    ]'::jsonb;
    v_operation text:=requested_request->>'operation';
    v_stage_run_id uuid;
    v_expected_stage_run_id uuid;
    v_canonical_revision bigint;
    v_prepare_hash text;
    v_commit_hash text;
    v_candidates jsonb;
    v_preparation_base jsonb;
    v_preparation jsonb;
    v_existing migration.region_talk_post_import_stage_run%ROWTYPE;
    v_outcome jsonb;
    v_work jsonb;
    v_stage_receipt jsonb;
    v_candidate jsonb;
    v_pipeline_id uuid;
    v_pipeline_stage orchestration.pipeline_stage%ROWTYPE;
    v_project_id uuid;
    v_status text;
    v_queue_count bigint:=0;
    v_work_count bigint:=0;
    v_rows_observed bigint:=0;
    v_rows_changed bigint:=0;
    v_receipts jsonb:='[]'::jsonb;
    v_receipt_base jsonb;
    v_receipt jsonb;
    v_receipt_sha text;
    v_expected_work_id uuid;
    v_prior_work orchestration.work_item%ROWTYPE;
BEGIN
    IF requested_request IS NULL OR jsonb_typeof(requested_request)<>'object'
       OR requested_request->>'schema_version'<>'region-talk-post-import-stage-request.v1'
       OR v_operation NOT IN('prepare','commit')
       OR requested_request->>'task_run_id' IS DISTINCT FROM requested_task_run_id::text
       OR requested_request->>'export_batch_id' IS DISTINCT FROM requested_export_batch_id::text
       OR requested_request->'ordered_stages' IS DISTINCT FROM fixed_dag
       OR requested_request->'publication_dispatch' IS DISTINCT FROM 'false'::jsonb
       OR requested_request->'notification_dispatch' IS DISTINCT FROM 'false'::jsonb
       OR requested_request->>'requested_at' IS NULL THEN
        RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='post-import stage request violates fixed contract';
    END IF;
    BEGIN
        v_stage_run_id:=(requested_request->>'stage_run_id')::uuid;
        PERFORM (requested_request->>'requested_at')::timestamptz;
    EXCEPTION WHEN OTHERS THEN
        RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='post-import stage request identity or time is invalid';
    END;
    v_expected_stage_run_id:=migration.region_talk_stage_uuid5(
        'region-talk-stage-run:'||requested_task_run_id::text||':'||requested_export_batch_id::text);
    IF v_stage_run_id<>v_expected_stage_run_id THEN
        RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='post-import stage run identity is not deterministic';
    END IF;

    PERFORM master_control.assert_registered_task_credential('region_talk',requested_task_run_id);
    SELECT accepted.canonical_revision INTO STRICT v_canonical_revision
      FROM region_talk.accepted_snapshot_v2 accepted
     WHERE accepted.export_batch_id=requested_export_batch_id
       AND accepted.task_run_id=requested_task_run_id;
    SELECT project_id INTO STRICT v_project_id FROM hub.project WHERE slug::text='region-talk';
    SELECT pipeline_id INTO STRICT v_pipeline_id FROM orchestration.pipeline
     WHERE workload='region-talk' AND name='region-talk-main' AND version='1.0.0' AND status='paused';

    IF v_operation='prepare' THEN
        IF requested_request - ARRAY[
            'schema_version','operation','stage_run_id','task_run_id','export_batch_id',
            'ordered_stages','requested_at','publication_dispatch','notification_dispatch'
        ]::text[] <> '{}'::jsonb THEN
            RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='post-import preparation request has unknown fields';
        END IF;
        -- requested_at is transport evidence, not semantic identity.  Excluding it
        -- lets an exact deterministic stage_run reconcile after response loss.
        v_prepare_hash:=encode(sha256(convert_to((requested_request-'requested_at')::text,'UTF8')),'hex');
        SELECT coalesce(jsonb_agg(jsonb_build_object(
                   'content_id',content.content_id,'candidate_id',candidate.candidate_id,
                   'candidate_revision',revision.revision,
                   'revision_fingerprint',revision.revision_fingerprint,
                   'canonical_url',coalesce(content.canonical_url,''),
                   'content_lane',CASE WHEN content.content_type='article' THEN 'article' ELSE 'social' END,
                   'canonical_source_key',coalesce(NULLIF(content.normalized_url,''),NULLIF(content.canonical_url,''),
                                                   'content:'||content.content_id::text),
                   'topics','[]'::jsonb,'content_type',content.content_type,'quality_score',0,
                   'legacy_selected',candidate.status IN('ready','in_review','approved'),
                   'evidence',jsonb_build_object(
                       'e5_embedding',jsonb_build_object('status','MISSING','input_fingerprint',
                           encode(sha256(convert_to(revision.revision_fingerprint||chr(31)||'e5_embedding','UTF8')),'hex'),'attempt_count',0),
                       'bge_m3_embedding',jsonb_build_object('status','MISSING','input_fingerprint',
                           encode(sha256(convert_to(revision.revision_fingerprint||chr(31)||'bge_m3_embedding','UTF8')),'hex'),'attempt_count',0),
                       'vector_fusion',jsonb_build_object('status','MISSING','input_fingerprint',
                           encode(sha256(convert_to(revision.revision_fingerprint||chr(31)||'vector_fusion','UTF8')),'hex'),'attempt_count',0),
                       'image_scoring',jsonb_build_object('status','MISSING','input_fingerprint',
                           encode(sha256(convert_to(revision.revision_fingerprint||chr(31)||'image_scoring','UTF8')),'hex'),'attempt_count',0),
                       'final_verifier',jsonb_build_object('status','MISSING','input_fingerprint',
                           encode(sha256(convert_to(revision.revision_fingerprint||chr(31)||'final_verifier','UTF8')),'hex'),'attempt_count',0),
                       'writer',jsonb_build_object('status','MISSING','input_fingerprint',
                           encode(sha256(convert_to(revision.revision_fingerprint||chr(31)||'writer','UTF8')),'hex'),'attempt_count',0)
                   )) ORDER BY candidate.candidate_id),'[]'::jsonb)
          INTO v_candidates
          FROM hub.content_item content
          JOIN region_talk.publication_candidate candidate USING(content_id)
          JOIN region_talk.candidate_revision revision
            ON revision.candidate_id=candidate.candidate_id AND revision.revision=candidate.current_revision
         WHERE content.metadata->>'region_talk_snapshot_id'=requested_export_batch_id::text;
        v_preparation_base:=jsonb_build_object(
            'schema_version','region-talk-post-import-stage-preparation.v1',
            'stage_run_id',v_stage_run_id,'task_run_id',requested_task_run_id,
            'export_batch_id',requested_export_batch_id,'canonical_revision',v_canonical_revision,
            'status','PREPARED','candidates',v_candidates,
            'publication_dispatch',false,'notification_dispatch',false);
        v_preparation:=v_preparation_base||jsonb_build_object('preparation_sha256',
            encode(sha256(convert_to(v_preparation_base::text,'UTF8')),'hex'));
        INSERT INTO migration.region_talk_post_import_stage_run(
            stage_run_id,task_run_id,export_batch_id,canonical_revision,prepare_request_sha256,
            preparation_sha256,preparation,state
        ) VALUES(v_stage_run_id,requested_task_run_id,requested_export_batch_id,v_canonical_revision,
                 v_prepare_hash,v_preparation->>'preparation_sha256',v_preparation,'PREPARED')
        ON CONFLICT(stage_run_id) DO NOTHING;
        SELECT * INTO STRICT v_existing FROM migration.region_talk_post_import_stage_run
         WHERE stage_run_id=v_stage_run_id FOR UPDATE;
        IF v_existing.task_run_id<>requested_task_run_id
           OR v_existing.export_batch_id<>requested_export_batch_id
           OR v_existing.canonical_revision<>v_canonical_revision
           OR v_existing.prepare_request_sha256<>v_prepare_hash THEN
            RAISE EXCEPTION USING ERRCODE='23505',MESSAGE='post-import preparation idempotency conflict';
        END IF;
        RETURN v_existing.preparation;
    END IF;

    IF requested_request - ARRAY[
        'schema_version','operation','stage_run_id','task_run_id','export_batch_id','ordered_stages',
        'requested_at','publication_dispatch','notification_dispatch','preparation_sha256',
        'candidate_outcomes','stage_receipts'
    ]::text[] <> '{}'::jsonb
       OR jsonb_typeof(requested_request->'candidate_outcomes')<>'array'
       OR jsonb_typeof(requested_request->'stage_receipts')<>'array'
       OR jsonb_array_length(requested_request->'stage_receipts')<>jsonb_array_length(fixed_dag) THEN
        RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='post-import commit request violates fixed contract';
    END IF;
    v_commit_hash:=encode(sha256(convert_to((requested_request-'requested_at')::text,'UTF8')),'hex');
    SELECT * INTO STRICT v_existing FROM migration.region_talk_post_import_stage_run
     WHERE stage_run_id=v_stage_run_id FOR UPDATE;
    IF v_existing.task_run_id<>requested_task_run_id
       OR v_existing.export_batch_id<>requested_export_batch_id
       OR v_existing.canonical_revision<>v_canonical_revision
       OR requested_request->>'preparation_sha256'<>v_existing.preparation_sha256 THEN
        RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='post-import commit is outside exact preparation';
    END IF;
    IF v_existing.state<>'PREPARED' THEN
        IF v_existing.commit_request_sha256<>v_commit_hash THEN
            RAISE EXCEPTION USING ERRCODE='23505',MESSAGE='post-import commit idempotency conflict';
        END IF;
        RETURN v_existing.final_receipt;
    END IF;

    v_candidates:=v_existing.preparation->'candidates';
    FOR v_outcome IN SELECT value FROM jsonb_array_elements(requested_request->'candidate_outcomes') LOOP
        IF jsonb_typeof(v_outcome)<>'object' OR v_outcome - ARRAY[
            'candidate_id','candidate_revision','revision_fingerprint','disposition',
            'review_basis','queue_rank','work_requests'
        ]::text[] <> '{}'::jsonb OR jsonb_typeof(v_outcome->'work_requests')<>'array'
           OR v_outcome->>'disposition' NOT IN('QUEUED_REVIEW','WAITING_WORK','FAILED_TERMINAL')
           OR v_outcome->>'revision_fingerprint' !~ '^[a-f0-9]{64}$' THEN
            RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='post-import candidate outcome is invalid';
        END IF;
        SELECT value INTO STRICT v_candidate FROM jsonb_array_elements(v_candidates)
         WHERE value->>'candidate_id'=v_outcome->>'candidate_id'
           AND value->>'candidate_revision'=v_outcome->>'candidate_revision'
           AND value->>'revision_fingerprint'=v_outcome->>'revision_fingerprint';
        IF (v_outcome->>'disposition'='QUEUED_REVIEW' AND (
                v_outcome->>'review_basis'<>'LEGACY_SELECTED'
                OR coalesce((v_candidate->>'legacy_selected')::boolean,false) IS NOT TRUE
                OR (v_outcome->>'queue_rank')::integer<1))
           OR (v_outcome->>'disposition'<>'QUEUED_REVIEW' AND v_outcome->'queue_rank'<>'null'::jsonb)
           OR (v_outcome->>'review_basis'='CURRENT_EVIDENCE') THEN
            RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='post-import review outcome lacks current or selected basis';
        END IF;
        INSERT INTO migration.region_talk_post_import_candidate_outcome(
            stage_run_id,candidate_id,candidate_revision,revision_fingerprint,
            disposition,review_basis,queue_rank,outcome
        ) VALUES(v_stage_run_id,(v_outcome->>'candidate_id')::uuid,
                 (v_outcome->>'candidate_revision')::integer,v_outcome->>'revision_fingerprint',
                 v_outcome->>'disposition',NULLIF(v_outcome->>'review_basis',''),
                 NULLIF(v_outcome->>'queue_rank','')::integer,v_outcome);
        v_rows_observed:=v_rows_observed+1;
        IF v_outcome->>'disposition'='QUEUED_REVIEW' THEN
            INSERT INTO region_talk.post_import_review_queue(
                stage_run_id,candidate_id,candidate_revision,queue_rank,review_basis,
                revision_fingerprint,publication_dispatch,notification_dispatch
            ) VALUES(v_stage_run_id,(v_outcome->>'candidate_id')::uuid,
                     (v_outcome->>'candidate_revision')::integer,(v_outcome->>'queue_rank')::integer,
                     v_outcome->>'review_basis',v_outcome->>'revision_fingerprint',false,false);
            v_queue_count:=v_queue_count+1; v_rows_changed:=v_rows_changed+1;
        END IF;
        FOR v_work IN SELECT value FROM jsonb_array_elements(v_outcome->'work_requests') LOOP
            IF jsonb_typeof(v_work)<>'object' OR v_work - ARRAY[
                'schema_version','work_item_id','stage','contract_version','subject_type','subject_id',
                'input_fingerprint','status','attempt_count','max_attempts','timeout_seconds','reason',
                'publication_dispatch','notification_dispatch'
            ]::text[] <> '{}'::jsonb
               OR v_work->>'schema_version'<>'region-talk-stage-work-request.v1'
               OR v_work->>'subject_type'<>'region_talk.candidate'
               OR v_work->>'subject_id'<>v_outcome->>'candidate_id'
               OR v_work->>'input_fingerprint' !~ '^[a-f0-9]{64}$'
               OR v_work->>'status' NOT IN('PENDING','FAILED_RETRYABLE')
               OR v_work->'publication_dispatch' IS DISTINCT FROM 'false'::jsonb
               OR v_work->'notification_dispatch' IS DISTINCT FROM 'false'::jsonb THEN
                RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='post-import work request is invalid';
            END IF;
            SELECT stage.* INTO STRICT v_pipeline_stage FROM orchestration.pipeline_stage stage
             WHERE stage.pipeline_id=v_pipeline_id AND stage.stage_key=v_work->>'stage'
               AND stage.stage_version='v1' AND stage.enabled;
            IF v_pipeline_stage.contract->>'name'<>v_work->>'contract_version'
               OR v_pipeline_stage.max_attempts<>(v_work->>'max_attempts')::integer
               OR v_pipeline_stage.timeout_seconds<>(v_work->>'timeout_seconds')::integer
               OR (v_work->>'attempt_count')::integer NOT BETWEEN 0 AND v_pipeline_stage.max_attempts THEN
                RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='post-import work request differs from registered stage';
            END IF;
            v_expected_work_id:=migration.region_talk_stage_uuid5(
                'region-talk-work:'||v_stage_run_id::text||':'||(v_outcome->>'candidate_id')||':'||
                (v_outcome->>'candidate_revision')||':'||(v_work->>'stage')||':'||
                (v_work->>'input_fingerprint'));
            IF (v_work->>'work_item_id')::uuid<>v_expected_work_id THEN
                RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='post-import work identity is not deterministic';
            END IF;
            INSERT INTO orchestration.work_item(
                work_item_id,pipeline_id,stage_id,project_id,subject_type,subject_id,dedupe_key,
                input_fingerprint,priority,payload,status,attempt_count,available_at
            ) VALUES(v_expected_work_id,v_pipeline_id,v_pipeline_stage.stage_id,v_project_id,
                     'region_talk.candidate',(v_outcome->>'candidate_id')::uuid,
                     'post-import:'||v_stage_run_id::text||':'||v_expected_work_id::text,
                     v_work->>'input_fingerprint',v_pipeline_stage.priority,
                     jsonb_build_object('schema_version','region-talk-stage-work-payload.v1',
                         'stage_run_id',v_stage_run_id,'candidate_revision',(v_outcome->>'candidate_revision')::integer,
                         'revision_fingerprint',v_outcome->>'revision_fingerprint','reason',v_work->>'reason',
                         'publication_dispatch',false,'notification_dispatch',false),
                     CASE v_work->>'status' WHEN 'PENDING' THEN 'pending' ELSE 'failed_retryable' END,
                     (v_work->>'attempt_count')::integer,clock_timestamp())
            ON CONFLICT(work_item_id) DO NOTHING;
            SELECT * INTO STRICT v_prior_work FROM orchestration.work_item WHERE work_item_id=v_expected_work_id;
            IF v_prior_work.pipeline_id<>v_pipeline_id OR v_prior_work.stage_id<>v_pipeline_stage.stage_id
               OR v_prior_work.subject_id<>(v_outcome->>'candidate_id')::uuid
               OR v_prior_work.input_fingerprint<>v_work->>'input_fingerprint'
               OR v_prior_work.payload->'publication_dispatch'<>'false'::jsonb
               OR v_prior_work.payload->'notification_dispatch'<>'false'::jsonb THEN
                RAISE EXCEPTION USING ERRCODE='23505',MESSAGE='post-import work idempotency conflict';
            END IF;
            v_work_count:=v_work_count+1; v_rows_changed:=v_rows_changed+1;
        END LOOP;
    END LOOP;
    IF (SELECT count(*) FROM migration.region_talk_post_import_candidate_outcome outcome
        WHERE outcome.stage_run_id=v_stage_run_id)<>jsonb_array_length(v_candidates) THEN
        RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='post-import outcomes do not cover exact preparation';
    END IF;

    FOR v_stage_receipt IN SELECT value FROM jsonb_array_elements(requested_request->'stage_receipts') LOOP
        IF jsonb_typeof(v_stage_receipt)<>'object' OR v_stage_receipt - ARRAY[
            'stage','contract_version','status','attempt','max_attempts','timeout_seconds',
            'rows_observed','rows_changed','work_request_count','input_sha256','output_sha256',
            'receipt_sha256','started_at','completed_at'
        ]::text[] <> '{}'::jsonb
           OR v_stage_receipt->>'status' NOT IN(
               'SUCCEEDED','WAITING_WORK','SKIPPED_BLOCKED','FAILED_RETRYABLE','FAILED_TERMINAL')
           OR v_stage_receipt->>'input_sha256' !~ '^[a-f0-9]{64}$'
           OR v_stage_receipt->>'output_sha256' !~ '^[a-f0-9]{64}$'
           OR v_stage_receipt->>'receipt_sha256' !~ '^[a-f0-9]{64}$'
           OR NOT EXISTS(SELECT 1 FROM jsonb_array_elements(fixed_dag) fixed
                         WHERE fixed->>'stage'=v_stage_receipt->>'stage'
                           AND fixed->>'contract_version'=v_stage_receipt->>'contract_version'
                           AND fixed->>'max_attempts'=v_stage_receipt->>'max_attempts'
                           AND fixed->>'timeout_seconds'=v_stage_receipt->>'timeout_seconds') THEN
            RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='post-import stage receipt is invalid';
        END IF;
        INSERT INTO migration.region_talk_post_import_stage_receipt(
            stage_run_id,stage,contract_version,status,attempt,max_attempts,timeout_seconds,
            rows_observed,rows_changed,work_request_count,input_sha256,output_sha256,receipt_sha256,
            receipt,started_at,completed_at
        ) VALUES(v_stage_run_id,v_stage_receipt->>'stage',v_stage_receipt->>'contract_version',
                 v_stage_receipt->>'status',(v_stage_receipt->>'attempt')::integer,
                 (v_stage_receipt->>'max_attempts')::integer,(v_stage_receipt->>'timeout_seconds')::integer,
                 (v_stage_receipt->>'rows_observed')::bigint,(v_stage_receipt->>'rows_changed')::bigint,
                 (v_stage_receipt->>'work_request_count')::bigint,v_stage_receipt->>'input_sha256',
                 v_stage_receipt->>'output_sha256',v_stage_receipt->>'receipt_sha256',v_stage_receipt,
                 (v_stage_receipt->>'started_at')::timestamptz,
                 (v_stage_receipt->>'completed_at')::timestamptz);
        v_receipts:=v_receipts||jsonb_build_array(v_stage_receipt);
    END LOOP;
    IF (SELECT count(DISTINCT receipt.stage) FROM migration.region_talk_post_import_stage_receipt receipt
        WHERE receipt.stage_run_id=v_stage_run_id)<>jsonb_array_length(fixed_dag) THEN
        RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='post-import stage receipts are incomplete';
    END IF;

    v_status:=CASE
        WHEN EXISTS(SELECT 1 FROM migration.region_talk_post_import_candidate_outcome outcome
                    WHERE outcome.stage_run_id=v_stage_run_id AND outcome.disposition='FAILED_TERMINAL')
             OR EXISTS(SELECT 1 FROM migration.region_talk_post_import_stage_receipt receipt
                       WHERE receipt.stage_run_id=v_stage_run_id AND receipt.status='FAILED_TERMINAL') THEN 'FAILED'
        WHEN v_work_count>0 OR EXISTS(
                    SELECT 1 FROM migration.region_talk_post_import_candidate_outcome outcome
                    WHERE outcome.stage_run_id=v_stage_run_id AND outcome.disposition='WAITING_WORK') THEN 'WAITING_WORK'
        ELSE 'COMPLETE' END;
    v_receipt_base:=jsonb_build_object(
        'schema_version','region-talk-post-import-stage-receipt.v1','stage_run_id',v_stage_run_id,
        'task_run_id',requested_task_run_id,'export_batch_id',requested_export_batch_id,
        'canonical_revision',v_canonical_revision,'status',v_status,'stage_receipts',v_receipts,
        'queue_revision',v_canonical_revision,'queue_count',v_queue_count,
        'work_request_count',v_work_count,'rows_observed',v_rows_observed,
        'rows_changed',v_rows_changed,'publication_dispatch',false,'notification_dispatch',false);
    v_receipt_sha:=encode(sha256(convert_to(v_receipt_base::text,'UTF8')),'hex');
    v_receipt:=v_receipt_base||jsonb_build_object('receipt_sha256',v_receipt_sha);
    UPDATE migration.region_talk_post_import_stage_run
       SET state=v_status,commit_request_sha256=v_commit_hash,final_receipt_sha256=v_receipt_sha,
           final_receipt=v_receipt,completed_at=clock_timestamp()
     WHERE stage_run_id=v_stage_run_id AND state='PREPARED';
    RETURN v_receipt;
END
$$;

REVOKE ALL ON migration.region_talk_post_import_stage_run,
    migration.region_talk_post_import_candidate_outcome,
    migration.region_talk_post_import_stage_receipt,
    region_talk.post_import_review_queue
    FROM PUBLIC,mdh_mcp_reader,mdh_mcp_editor,mdh_region_talk_pipeline;
REVOKE EXECUTE ON FUNCTION
    migration.region_talk_claim_canonical_state(text,text,text,uuid,uuid,uuid,timestamptz,text,bigint),
    migration.refresh_region_talk_canonical_current_state(uuid,bigint),
    migration.apply_region_talk_initial_source_status(),
    migration.region_talk_stage_uuid5(text),
    migration.execute_region_talk_post_import_stages(uuid,uuid,jsonb)
    FROM PUBLIC,mdh_mcp_reader,mdh_mcp_editor,mdh_region_talk_pipeline;
GRANT USAGE ON SCHEMA migration,region_talk TO mdh_region_talk_pipeline;
GRANT EXECUTE ON FUNCTION migration.execute_region_talk_post_import_stages(uuid,uuid,jsonb)
    TO mdh_region_talk_pipeline;

UPDATE hub.canonical_state SET schema_revision=26,updated_at=clock_timestamp() WHERE singleton=true;
