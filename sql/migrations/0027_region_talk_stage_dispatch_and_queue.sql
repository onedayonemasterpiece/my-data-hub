-- Region Talk v6: non-regressing replay, verified stage evidence, and bounded dispatch.
-- All entry points remain task/ACTIVE-epoch bound and publication stays disabled.

CREATE OR REPLACE FUNCTION migration.region_talk_claim_canonical_state(
    requested_identity_kind text,requested_identity_key text,requested_target_table text,
    requested_target_id uuid,requested_raw_record_id uuid,requested_export_batch_id uuid,
    incoming_source_updated_at timestamptz,incoming_payload_sha256 text,
    requested_canonical_revision bigint
) RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog
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
            -- An identical observation is audit evidence only.  In particular, an older
            -- export cannot move any current pointer, revision, or source clock backwards.
            v_disposition:='replay';
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

-- Python's canonical JSON hashing contract (UTF-8, sorted keys, compact separators).
CREATE FUNCTION migration.region_talk_canonical_json(value jsonb) RETURNS text
LANGUAGE plpgsql IMMUTABLE STRICT SET search_path=pg_catalog
AS $$
DECLARE rendered text;
BEGIN
    CASE jsonb_typeof(value)
      WHEN 'object' THEN
        SELECT '{'||coalesce(string_agg(to_jsonb(key)::text||':'||
               migration.region_talk_canonical_json(val),',' ORDER BY key),'')||'}'
          INTO rendered FROM jsonb_each(value) AS item(key,val);
      WHEN 'array' THEN
        SELECT '['||coalesce(string_agg(migration.region_talk_canonical_json(val),',' ORDER BY ord),'')||']'
          INTO rendered FROM jsonb_array_elements(value) WITH ORDINALITY AS item(val,ord);
      ELSE rendered:=value::text;
    END CASE;
    RETURN rendered;
END
$$;

CREATE FUNCTION migration.region_talk_json_sha256(value jsonb) RETURNS text
LANGUAGE sql IMMUTABLE STRICT SET search_path=pg_catalog
RETURN encode(public.digest(convert_to(migration.region_talk_canonical_json(value),'UTF8'),'sha256'),'hex');

CREATE TABLE migration.region_talk_stage_worker_result (
    work_item_id         uuid NOT NULL REFERENCES orchestration.work_item(work_item_id) ON DELETE RESTRICT,
    attempt              integer NOT NULL CHECK(attempt>=1),
    task_run_id          uuid NOT NULL,
    export_batch_id      uuid NOT NULL REFERENCES migration.region_talk_direct_snapshot(export_batch_id),
    stage_run_id         uuid NOT NULL REFERENCES migration.region_talk_post_import_stage_run(stage_run_id),
    master_instance_id   uuid NOT NULL,
    epoch                bigint NOT NULL CHECK(epoch>=1),
    stage                text NOT NULL,
    contract_version     text NOT NULL,
    subject_type         text NOT NULL CHECK(subject_type='region_talk.candidate'),
    subject_id           uuid NOT NULL,
    candidate_revision   integer NOT NULL CHECK(candidate_revision>=1),
    revision_fingerprint text NOT NULL CHECK(revision_fingerprint ~ '^[a-f0-9]{64}$'),
    input_fingerprint    text NOT NULL CHECK(input_fingerprint ~ '^[a-f0-9]{64}$'),
    effect_id            uuid NOT NULL,
    result_status        text NOT NULL CHECK(result_status IN(
                             'SUCCEEDED','FAILED_RETRYABLE','FAILED_TERMINAL')),
    result_metadata      jsonb NOT NULL CHECK(octet_length(result_metadata::text)<=65536),
    metadata_sha256      text NOT NULL CHECK(metadata_sha256 ~ '^[a-f0-9]{64}$'),
    result_sha256        text NOT NULL CHECK(result_sha256 ~ '^[a-f0-9]{64}$'),
    completed_at         timestamptz NOT NULL,
    received_at          timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(work_item_id,attempt),
    UNIQUE(effect_id),
    UNIQUE(stage_run_id,stage,subject_id,candidate_revision,input_fingerprint,attempt)
);
CREATE TRIGGER region_talk_stage_worker_result_append_only
BEFORE UPDATE OR DELETE ON migration.region_talk_stage_worker_result
FOR EACH ROW EXECUTE FUNCTION hub_meta.reject_update_delete();

CREATE TABLE migration.region_talk_post_import_cycle_outcome (
    stage_run_id uuid NOT NULL REFERENCES migration.region_talk_post_import_stage_run(stage_run_id),
    stage_cycle integer NOT NULL CHECK(stage_cycle>=2),
    candidate_id uuid NOT NULL,
    candidate_revision integer NOT NULL CHECK(candidate_revision>=1),
    revision_fingerprint text NOT NULL CHECK(revision_fingerprint ~ '^[a-f0-9]{64}$'),
    disposition text NOT NULL CHECK(disposition IN('QUEUED_REVIEW','WAITING_WORK','FAILED_TERMINAL')),
    outcome jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(stage_run_id,stage_cycle,candidate_id,candidate_revision)
);
CREATE TABLE migration.region_talk_post_import_cycle_receipt (
    stage_run_id uuid NOT NULL REFERENCES migration.region_talk_post_import_stage_run(stage_run_id),
    stage_cycle integer NOT NULL CHECK(stage_cycle>=2),
    stage text NOT NULL, receipt_sha256 text NOT NULL CHECK(receipt_sha256 ~ '^[a-f0-9]{64}$'),
    receipt jsonb NOT NULL, created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(stage_run_id,stage_cycle,stage)
);
CREATE TRIGGER region_talk_post_import_cycle_outcome_append_only
BEFORE UPDATE OR DELETE ON migration.region_talk_post_import_cycle_outcome
FOR EACH ROW EXECUTE FUNCTION hub_meta.reject_update_delete();
CREATE TRIGGER region_talk_post_import_cycle_receipt_append_only
BEFORE UPDATE OR DELETE ON migration.region_talk_post_import_cycle_receipt
FOR EACH ROW EXECUTE FUNCTION hub_meta.reject_update_delete();

ALTER FUNCTION migration.execute_region_talk_post_import_stages(uuid,uuid,jsonb)
    RENAME TO execute_region_talk_post_import_stages_v1_unverified;

-- Validate the pure transform's receipt hashes against server-owned preparation and
-- counters before the v1 persistence function sees the request.  PREPARE overlays only
-- exact, immutable landed results bound to the current task epoch and candidate revision.
CREATE FUNCTION migration.execute_region_talk_post_import_stages(
    requested_task_run_id uuid,requested_export_batch_id uuid,requested_request jsonb
) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog
AS $$
DECLARE
    v_response jsonb; v_run migration.region_talk_post_import_stage_run%ROWTYPE;
    v_receipt jsonb; v_fixed jsonb; v_input jsonb; v_output jsonb;
    v_candidate jsonb; v_candidates jsonb:='[]'::jsonb; v_evidence jsonb;
    v_stage text; v_fingerprint text; v_status text; v_attempt integer;
    v_landed migration.region_talk_stage_worker_result%ROWTYPE;
    v_registration master_control.task_credential_registration%ROWTYPE;
    v_cycle integer; v_outcome jsonb; v_queue_count bigint:=0;
    v_work_count bigint:=0; v_base jsonb; v_final jsonb; v_commit_hash text;
BEGIN
    v_registration:=master_control.assert_registered_task_credential(
        'region_talk',requested_task_run_id);
    IF requested_request->>'operation'='commit' THEN
        SELECT * INTO STRICT v_run FROM migration.region_talk_post_import_stage_run
         WHERE task_run_id=requested_task_run_id AND export_batch_id=requested_export_batch_id;
        FOR v_receipt IN SELECT value FROM jsonb_array_elements(requested_request->'stage_receipts') LOOP
            SELECT value INTO STRICT v_fixed FROM jsonb_array_elements(requested_request->'ordered_stages')
             WHERE value->>'stage'=v_receipt->>'stage';
            v_input:=jsonb_build_object(
                'stage_run_id',v_run.stage_run_id,'preparation_sha256',v_run.preparation_sha256,
                'stage',v_receipt->>'stage','candidate_revisions',coalesce((
                  SELECT jsonb_agg(jsonb_build_array(candidate->>'candidate_id',
                    (candidate->>'candidate_revision')::integer,candidate->>'revision_fingerprint')
                    ORDER BY candidate->>'candidate_id')
                    FROM jsonb_array_elements(v_run.preparation->'candidates') candidate),'[]'::jsonb));
            v_output:=jsonb_build_object('status',v_receipt->>'status',
                'rows_observed',(v_receipt->>'rows_observed')::bigint,
                'rows_changed',(v_receipt->>'rows_changed')::bigint,
                'work_request_count',(v_receipt->>'work_request_count')::bigint);
            -- Pydantic serializes UTC datetimes as ``Z`` after the pure transform
            -- hashed their ISO ``+00:00`` spelling. Normalize to that semantic UTC
            -- spelling before independently recomputing the receipt digest.
            v_fixed:=jsonb_set(v_receipt-'receipt_sha256','{started_at}',to_jsonb(
                to_char((v_receipt->>'started_at')::timestamptz AT TIME ZONE 'UTC',
                        CASE WHEN v_receipt->>'started_at' LIKE '%.%'
                          THEN 'YYYY-MM-DD"T"HH24:MI:SS.US' ELSE 'YYYY-MM-DD"T"HH24:MI:SS' END)||'+00:00'));
            v_fixed:=jsonb_set(v_fixed,'{completed_at}',to_jsonb(
                to_char((v_receipt->>'completed_at')::timestamptz AT TIME ZONE 'UTC',
                        CASE WHEN v_receipt->>'completed_at' LIKE '%.%'
                          THEN 'YYYY-MM-DD"T"HH24:MI:SS.US' ELSE 'YYYY-MM-DD"T"HH24:MI:SS' END)||'+00:00'));
            IF v_receipt->>'input_sha256'<>migration.region_talk_json_sha256(v_input)
               OR v_receipt->>'output_sha256'<>migration.region_talk_json_sha256(v_output)
               OR v_receipt->>'receipt_sha256'<>migration.region_talk_json_sha256(v_fixed) THEN
                RAISE EXCEPTION USING ERRCODE='22023',
                    MESSAGE='post-import stage receipt hash verification failed',
                    DETAIL='stage='||(v_receipt->>'stage')||
                      ' expected_input='||migration.region_talk_json_sha256(v_input)||
                      ' received_input='||(v_receipt->>'input_sha256')||
                      ' expected_output='||migration.region_talk_json_sha256(v_output)||
                      ' received_output='||(v_receipt->>'output_sha256')||
                      ' expected_receipt='||migration.region_talk_json_sha256(v_fixed)||
                      ' received_receipt='||(v_receipt->>'receipt_sha256');
            END IF;
        END LOOP;
        -- Cycle one remains in the original immutable tables.  A later PREPARE may
        -- incorporate newly landed evidence; persist subsequent transforms in the
        -- append-only cycle tables rather than overwriting the first receipt.
        IF v_run.state='PREPARED' AND EXISTS(
          SELECT 1 FROM migration.region_talk_post_import_stage_receipt old
           WHERE old.stage_run_id=v_run.stage_run_id) THEN
            SELECT coalesce(max(cycle.stage_cycle),1)+1 INTO v_cycle
              FROM migration.region_talk_post_import_cycle_receipt cycle
             WHERE cycle.stage_run_id=v_run.stage_run_id;
            IF jsonb_array_length(requested_request->'candidate_outcomes')<>
               jsonb_array_length(v_run.preparation->'candidates') THEN
                RAISE EXCEPTION USING ERRCODE='22023',
                    MESSAGE='post-import cycle outcomes do not cover preparation';
            END IF;
            FOR v_outcome IN SELECT value FROM jsonb_array_elements(
              requested_request->'candidate_outcomes') LOOP
                IF NOT EXISTS(SELECT 1 FROM jsonb_array_elements(v_run.preparation->'candidates') c
                  WHERE c->>'candidate_id'=v_outcome->>'candidate_id'
                    AND c->>'candidate_revision'=v_outcome->>'candidate_revision'
                    AND c->>'revision_fingerprint'=v_outcome->>'revision_fingerprint') THEN
                    RAISE EXCEPTION USING ERRCODE='22023',
                        MESSAGE='post-import cycle outcome differs from preparation';
                END IF;
                INSERT INTO migration.region_talk_post_import_cycle_outcome(
                  stage_run_id,stage_cycle,candidate_id,candidate_revision,revision_fingerprint,
                  disposition,outcome)
                VALUES(v_run.stage_run_id,v_cycle,(v_outcome->>'candidate_id')::uuid,
                  (v_outcome->>'candidate_revision')::integer,v_outcome->>'revision_fingerprint',
                  v_outcome->>'disposition',v_outcome);
                v_work_count:=v_work_count+jsonb_array_length(v_outcome->'work_requests');
                IF v_outcome->>'disposition'='QUEUED_REVIEW' THEN
                    INSERT INTO region_talk.post_import_review_queue(
                      stage_run_id,candidate_id,candidate_revision,queue_rank,review_basis,
                      revision_fingerprint,publication_dispatch,notification_dispatch)
                    VALUES(v_run.stage_run_id,(v_outcome->>'candidate_id')::uuid,
                      (v_outcome->>'candidate_revision')::integer,(v_outcome->>'queue_rank')::integer,
                      v_outcome->>'review_basis',v_outcome->>'revision_fingerprint',false,false)
                    ON CONFLICT(stage_run_id,candidate_id,candidate_revision) DO NOTHING;
                    v_queue_count:=v_queue_count+1;
                END IF;
            END LOOP;
            FOR v_receipt IN SELECT value FROM jsonb_array_elements(
              requested_request->'stage_receipts') LOOP
                INSERT INTO migration.region_talk_post_import_cycle_receipt(
                  stage_run_id,stage_cycle,stage,receipt_sha256,receipt)
                VALUES(v_run.stage_run_id,v_cycle,v_receipt->>'stage',
                  v_receipt->>'receipt_sha256',v_receipt);
            END LOOP;
            v_status:=CASE WHEN EXISTS(SELECT 1 FROM jsonb_array_elements(
                requested_request->'candidate_outcomes') o WHERE o->>'disposition'='FAILED_TERMINAL')
              THEN 'FAILED' WHEN v_work_count>0 OR EXISTS(SELECT 1 FROM jsonb_array_elements(
                requested_request->'candidate_outcomes') o WHERE o->>'disposition'='WAITING_WORK')
              THEN 'WAITING_WORK' ELSE 'COMPLETE' END;
            v_base:=jsonb_build_object('schema_version','region-talk-post-import-stage-receipt.v1',
              'stage_run_id',v_run.stage_run_id,'task_run_id',requested_task_run_id,
              'export_batch_id',requested_export_batch_id,'canonical_revision',v_run.canonical_revision,
              'status',v_status,'stage_receipts',requested_request->'stage_receipts',
              'queue_revision',v_run.canonical_revision,'queue_count',v_queue_count,
              'work_request_count',v_work_count,
              'rows_observed',jsonb_array_length(v_run.preparation->'candidates'),
              'rows_changed',v_queue_count+v_work_count,'publication_dispatch',false,
              'notification_dispatch',false);
            v_final:=v_base||jsonb_build_object('receipt_sha256',
              migration.region_talk_json_sha256(v_base));
            v_commit_hash:=migration.region_talk_json_sha256(requested_request-'requested_at');
            UPDATE migration.region_talk_post_import_stage_run SET state=v_status,
              commit_request_sha256=v_commit_hash,final_receipt_sha256=v_final->>'receipt_sha256',
              final_receipt=v_final,completed_at=clock_timestamp()
             WHERE stage_run_id=v_run.stage_run_id;
            RETURN v_final;
        END IF;
        RETURN migration.execute_region_talk_post_import_stages_v1_unverified(
            requested_task_run_id,requested_export_batch_id,requested_request);
    END IF;

    v_response:=migration.execute_region_talk_post_import_stages_v1_unverified(
        requested_task_run_id,requested_export_batch_id,requested_request);
    SELECT * INTO STRICT v_run FROM migration.region_talk_post_import_stage_run
     WHERE task_run_id=requested_task_run_id AND export_batch_id=requested_export_batch_id FOR UPDATE;
    FOR v_candidate IN SELECT value FROM jsonb_array_elements(v_response->'candidates') LOOP
        v_evidence:=v_candidate->'evidence';
        FOREACH v_stage IN ARRAY ARRAY[
          'e5_embedding','bge_m3_embedding','vector_fusion','image_scoring','final_verifier','writer'
        ] LOOP
            v_fingerprint:=v_evidence->v_stage->>'input_fingerprint';
            SELECT landed.* INTO v_landed
              FROM migration.region_talk_stage_worker_result landed
             WHERE landed.stage_run_id=v_run.stage_run_id AND landed.stage=v_stage
               AND landed.subject_id=(v_candidate->>'candidate_id')::uuid
               AND landed.candidate_revision=(v_candidate->>'candidate_revision')::integer
               AND landed.revision_fingerprint=v_candidate->>'revision_fingerprint'
               AND landed.input_fingerprint=v_fingerprint
               AND landed.task_run_id=requested_task_run_id
               AND landed.export_batch_id=requested_export_batch_id
               AND landed.master_instance_id=v_registration.master_instance_id
               AND landed.epoch=v_registration.epoch
             ORDER BY landed.attempt DESC LIMIT 1;
            IF FOUND THEN
                v_status:=CASE v_landed.result_status WHEN 'SUCCEEDED' THEN 'CURRENT'
                  ELSE v_landed.result_status END;
                v_attempt:=v_landed.attempt;
            ELSE
                SELECT coalesce(max(work.attempt_count),0) INTO v_attempt
                  FROM orchestration.work_item work JOIN orchestration.pipeline_stage stage USING(stage_id)
                 WHERE work.payload->>'stage_run_id'=v_run.stage_run_id::text
                   AND work.subject_id=(v_candidate->>'candidate_id')::uuid
                   AND stage.stage_key=v_stage;
                v_status:=CASE WHEN EXISTS(
                  SELECT 1 FROM migration.region_talk_stage_worker_result stale
                   WHERE stale.stage_run_id=v_run.stage_run_id AND stale.stage=v_stage
                     AND stale.subject_id=(v_candidate->>'candidate_id')::uuid)
                  THEN 'STALE' ELSE 'MISSING' END;
            END IF;
            v_evidence:=jsonb_set(v_evidence,ARRAY[v_stage],jsonb_build_object(
                'status',v_status,'input_fingerprint',v_fingerprint,'attempt_count',v_attempt));
        END LOOP;
        v_candidates:=v_candidates||jsonb_build_array(
            jsonb_set(v_candidate,'{evidence}',v_evidence));
    END LOOP;
    v_response:=jsonb_set(v_response,'{candidates}',v_candidates);
    v_response:=v_response-'preparation_sha256';
    v_response:=v_response||jsonb_build_object(
        'preparation_sha256',migration.region_talk_json_sha256(v_response));
    IF v_response IS DISTINCT FROM v_run.preparation THEN
        UPDATE migration.region_talk_post_import_stage_run SET
          preparation=v_response,preparation_sha256=v_response->>'preparation_sha256',
          state='PREPARED',commit_request_sha256=NULL,final_receipt_sha256=NULL,
          final_receipt=NULL,completed_at=NULL
         WHERE stage_run_id=v_run.stage_run_id;
    END IF;
    RETURN v_response;
END
$$;

-- Every mutable lease is constrained to a deterministic Region Talk work row.  The
-- immutable landed result is the only evidence PREPARE is allowed to mark CURRENT.
CREATE FUNCTION migration.claim_region_talk_stage_work(
    requested_task_run_id uuid,requested_export_batch_id uuid,requested_request jsonb
) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog
AS $$
DECLARE
    registration master_control.task_credential_registration%ROWTYPE;
    accepted region_talk.accepted_snapshot_v2%ROWTYPE;
    item orchestration.work_item%ROWTYPE;
    stage orchestration.pipeline_stage%ROWTYPE;
    run migration.region_talk_post_import_stage_run%ROWTYPE;
    candidate region_talk.publication_candidate%ROWTYPE;
    content hub.content_item%ROWTYPE;
    v_lease_token uuid; v_owner text; v_attempt integer; v_effect_id uuid;
    v_payload jsonb; v_base jsonb; v_status text; v_upstream jsonb;
    v_lease_expires timestamptz;
BEGIN
    IF requested_request->>'schema_version'<>'region-talk-stage-work-claim.v1'
       OR requested_request - ARRAY['schema_version','lease_token','lease_owner','requested_at',
          'publication_dispatch','notification_dispatch']::text[] <> '{}'::jsonb
       OR requested_request->'publication_dispatch' IS DISTINCT FROM 'false'::jsonb
       OR requested_request->'notification_dispatch' IS DISTINCT FROM 'false'::jsonb
       OR length(coalesce(requested_request->>'lease_owner','')) NOT BETWEEN 1 AND 200 THEN
        RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='Region Talk work claim violates fixed contract';
    END IF;
    BEGIN
        v_lease_token:=(requested_request->>'lease_token')::uuid;
        PERFORM (requested_request->>'requested_at')::timestamptz;
    EXCEPTION WHEN OTHERS THEN
        RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='Region Talk work claim identity is invalid';
    END;
    v_owner:=requested_request->>'lease_owner';
    registration:=master_control.assert_registered_task_credential('region_talk',requested_task_run_id);
    SELECT * INTO STRICT accepted FROM region_talk.accepted_snapshot_v2
     WHERE task_run_id=requested_task_run_id AND export_batch_id=requested_export_batch_id;
    SELECT * INTO STRICT run FROM migration.region_talk_post_import_stage_run
     WHERE task_run_id=requested_task_run_id AND export_batch_id=requested_export_batch_id;

    UPDATE orchestration.work_item exhausted SET status='failed_terminal',
           lease_owner=NULL,lease_token=NULL,lease_expires_at=NULL,
           last_error=jsonb_build_object('schema_version','region-talk-stage-failure.v1',
             'reason','lease expired after maximum attempts')
      FROM orchestration.pipeline_stage exhausted_stage
     WHERE exhausted.stage_id=exhausted_stage.stage_id
       AND exhausted.payload->>'stage_run_id'=run.stage_run_id::text
       AND exhausted.status='leased' AND exhausted.lease_expires_at<=clock_timestamp()
       AND exhausted.attempt_count>=exhausted_stage.max_attempts;

    SELECT work.* INTO item
      FROM orchestration.work_item work
      JOIN orchestration.pipeline pipeline USING(pipeline_id)
      JOIN orchestration.pipeline_stage pipeline_stage USING(stage_id)
      JOIN region_talk.publication_candidate current_candidate
        ON current_candidate.candidate_id=work.subject_id
      JOIN region_talk.candidate_revision current_revision
        ON current_revision.candidate_id=current_candidate.candidate_id
       AND current_revision.revision=current_candidate.current_revision
     WHERE pipeline.workload='region-talk' AND pipeline.name='region-talk-main'
       AND pipeline.status='paused' AND work.subject_type='region_talk.candidate'
       AND work.payload->>'stage_run_id'=run.stage_run_id::text
       AND work.payload->>'candidate_revision'=current_candidate.current_revision::text
       AND work.payload->>'revision_fingerprint'=current_revision.revision_fingerprint
       AND (work.status IN('pending','failed_retryable') OR
            (work.status='leased' AND work.lease_expires_at<=clock_timestamp()))
       AND work.available_at<=clock_timestamp()
       AND work.attempt_count<pipeline_stage.max_attempts
       AND CASE pipeline_stage.stage_key
         WHEN 'vector_fusion' THEN
           (SELECT count(DISTINCT landed.stage)=2
              FROM migration.region_talk_stage_worker_result landed
             WHERE landed.stage_run_id=run.stage_run_id AND landed.subject_id=work.subject_id
               AND landed.result_status='SUCCEEDED'
               AND landed.stage IN('e5_embedding','bge_m3_embedding'))
         WHEN 'image_scoring' THEN EXISTS(SELECT 1 FROM migration.region_talk_stage_worker_result landed
           WHERE landed.stage_run_id=run.stage_run_id AND landed.subject_id=work.subject_id
             AND landed.stage='vector_fusion' AND landed.result_status='SUCCEEDED')
         WHEN 'final_verifier' THEN EXISTS(SELECT 1 FROM migration.region_talk_stage_worker_result landed
           WHERE landed.stage_run_id=run.stage_run_id AND landed.subject_id=work.subject_id
             AND landed.stage='image_scoring' AND landed.result_status='SUCCEEDED')
         WHEN 'writer' THEN EXISTS(SELECT 1 FROM migration.region_talk_stage_worker_result landed
           WHERE landed.stage_run_id=run.stage_run_id AND landed.subject_id=work.subject_id
             AND landed.stage='final_verifier' AND landed.result_status='SUCCEEDED')
         ELSE true END
     ORDER BY work.priority,work.queue_seq FOR UPDATE OF work SKIP LOCKED LIMIT 1;
    IF NOT FOUND THEN
        v_status:=CASE run.state WHEN 'COMPLETE' THEN 'COMPLETE' WHEN 'FAILED' THEN 'FAILED'
          ELSE CASE WHEN EXISTS(
            SELECT 1 FROM orchestration.work_item failed
             WHERE failed.payload->>'stage_run_id'=run.stage_run_id::text
               AND failed.status='failed_terminal') THEN 'FAILED'
          ELSE CASE WHEN EXISTS(
            SELECT 1 FROM orchestration.work_item pending
             WHERE pending.payload->>'stage_run_id'=run.stage_run_id::text
               AND pending.status IN('pending','failed_retryable','leased','running'))
            THEN 'WAITING_DEPENDENCY' ELSE 'EMPTY' END END END;
        v_base:=jsonb_build_object(
            'schema_version','region-talk-stage-work-claim-receipt.v1','status',v_status,
            'master_instance_id',registration.master_instance_id,'epoch',registration.epoch,
            'task_run_id',requested_task_run_id,'export_batch_id',requested_export_batch_id,
            'stage_run_id',run.stage_run_id,'work_item_id',NULL,'publication_dispatch',false,
            'notification_dispatch',false);
        RETURN v_base||jsonb_build_object('receipt_sha256',migration.region_talk_json_sha256(v_base));
    END IF;
    SELECT * INTO STRICT stage FROM orchestration.pipeline_stage WHERE stage_id=item.stage_id;
    SELECT * INTO STRICT candidate FROM region_talk.publication_candidate WHERE candidate_id=item.subject_id;
    SELECT * INTO STRICT content FROM hub.content_item WHERE content_id=candidate.content_id;
    v_attempt:=item.attempt_count+1;
    v_effect_id:=migration.region_talk_stage_uuid5('region-talk-stage-effect:'||item.work_item_id::text||':'||
                  v_attempt::text||':'||item.input_fingerprint);
    SELECT coalesce(jsonb_agg(jsonb_build_object(
      'stage',prior.stage,'contract_version',prior.contract_version,
      'input_fingerprint',prior.input_fingerprint,'result_sha256',prior.result_sha256,
      'result_metadata',prior.result_metadata) ORDER BY prior.stage),'[]'::jsonb)
      INTO v_upstream FROM migration.region_talk_stage_worker_result prior
     WHERE prior.stage_run_id=run.stage_run_id AND prior.subject_id=item.subject_id
       AND prior.candidate_revision=candidate.current_revision
       AND prior.result_status='SUCCEEDED';
    v_payload:=jsonb_build_object(
        'schema_version','region-talk-stage-work-execution.v1','stage_run_id',run.stage_run_id,
        'candidate_id',candidate.candidate_id,'candidate_revision',candidate.current_revision,
        'revision_fingerprint',item.payload->>'revision_fingerprint','content_id',content.content_id,
        'content_type',content.content_type,'canonical_url',coalesce(content.canonical_url,''),
        'input_fingerprint',item.input_fingerprint,'upstream_results',v_upstream,
        'canonical_source_key',coalesce(NULLIF(content.normalized_url,''),NULLIF(content.canonical_url,''),
          'content:'||content.content_id::text),
        'input_data',CASE WHEN stage.stage_key IN('e5_embedding','bge_m3_embedding') THEN
          jsonb_build_object('schema_version','region-talk-stage-text-input.v1',
            'text',left(concat_ws(E'\n\n',content.title,content.summary),262144),
            'text_sha256',encode(sha256(convert_to(left(concat_ws(E'\n\n',content.title,content.summary),262144),'UTF8')),'hex'),
            'topics','[]'::jsonb)
          WHEN stage.stage_key='image_scoring' THEN jsonb_build_object(
            'schema_version','region-talk-image-input.v1','availability','UNAVAILABLE',
            'reason','accepted canonical row has no verified private image artifact')
          ELSE jsonb_build_object('schema_version','region-talk-upstream-stage-input.v1',
            'upstream_results',v_upstream) END,
        'publication_dispatch',false,'notification_dispatch',false);
    v_lease_expires:=clock_timestamp()+make_interval(secs=>stage.timeout_seconds);
    UPDATE orchestration.work_item SET status='leased',attempt_count=v_attempt,
           lease_owner=v_owner,lease_token=v_lease_token,
           lease_expires_at=v_lease_expires
     WHERE work_item_id=item.work_item_id;
    v_base:=jsonb_build_object(
        'schema_version','region-talk-stage-work-claim-receipt.v1','status','CLAIMED',
        'master_instance_id',registration.master_instance_id,'epoch',registration.epoch,
        'task_run_id',requested_task_run_id,'export_batch_id',requested_export_batch_id,
        'stage_run_id',run.stage_run_id,'work_item_id',item.work_item_id,'stage',stage.stage_key,
        'contract_version',stage.contract->>'name','subject_type',item.subject_type,
        'subject_id',item.subject_id,'input_fingerprint',item.input_fingerprint,
        'attempt',v_attempt,'max_attempts',stage.max_attempts,'timeout_seconds',stage.timeout_seconds,
        'effect_id',v_effect_id,'lease_token',v_lease_token,
        'lease_expires_at',v_lease_expires,
        'payload',v_payload,'publication_dispatch',false,'notification_dispatch',false);
    RETURN v_base||jsonb_build_object('receipt_sha256',migration.region_talk_json_sha256(v_base));
END
$$;

CREATE FUNCTION migration.submit_region_talk_stage_result(
    requested_task_run_id uuid,requested_export_batch_id uuid,requested_result jsonb
) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog
AS $$
DECLARE
    registration master_control.task_credential_registration%ROWTYPE;
    item orchestration.work_item%ROWTYPE; stage orchestration.pipeline_stage%ROWTYPE;
    existing migration.region_talk_stage_worker_result%ROWTYPE;
    v_metadata_hash text; v_base jsonb; v_effect uuid;
BEGIN
    IF requested_result->>'schema_version'<>'region-talk-stage-worker-result.v1'
       OR requested_result - ARRAY['schema_version','master_instance_id','epoch','task_run_id',
          'export_batch_id','stage_run_id','work_item_id','stage','contract_version','subject_type',
          'subject_id','candidate_revision','revision_fingerprint','input_fingerprint','attempt',
          'effect_id','lease_token','result_status','result_metadata','metadata_sha256','result_sha256',
          'completed_at','publication_dispatch','notification_dispatch']::text[] <> '{}'::jsonb
       OR requested_result->>'task_run_id' IS DISTINCT FROM requested_task_run_id::text
       OR requested_result->>'export_batch_id' IS DISTINCT FROM requested_export_batch_id::text
       OR requested_result->>'subject_type'<>'region_talk.candidate'
       OR requested_result->>'result_status' NOT IN('SUCCEEDED','FAILED_RETRYABLE','FAILED_TERMINAL')
       OR requested_result->>'result_sha256' !~ '^[a-f0-9]{64}$'
       OR requested_result->'publication_dispatch' IS DISTINCT FROM 'false'::jsonb
       OR requested_result->'notification_dispatch' IS DISTINCT FROM 'false'::jsonb
       OR jsonb_typeof(requested_result->'result_metadata')<>'object'
       OR requested_result->'result_metadata'->>'schema_version'<>'region-talk-stage-result-metadata.v1'
       OR (requested_result->'result_metadata') - ARRAY['schema_version','stage','contract_version',
          'subject_type','subject_id','candidate_revision','revision_fingerprint','input_fingerprint',
          'producer_exact_id','metrics','artifact_sha256']::text[] <> '{}'::jsonb
       OR length(coalesce(requested_result->'result_metadata'->>'producer_exact_id',''))
          NOT BETWEEN 1 AND 500
       OR jsonb_typeof(requested_result->'result_metadata'->'metrics')<>'object'
       OR (requested_result->'result_metadata'->>'artifact_sha256' IS NOT NULL AND
           requested_result->'result_metadata'->>'artifact_sha256' !~ '^[a-f0-9]{64}$')
       OR octet_length((requested_result->'result_metadata')::text)>65536 THEN
        RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='Region Talk worker result violates fixed contract';
    END IF;
    registration:=master_control.assert_registered_task_credential('region_talk',requested_task_run_id);
    IF requested_result->>'master_instance_id'<>registration.master_instance_id::text
       OR (requested_result->>'epoch')::bigint<>registration.epoch THEN
        RAISE EXCEPTION USING ERRCODE='42501',MESSAGE='Region Talk result crosses task epoch';
    END IF;
    SELECT * INTO STRICT item FROM orchestration.work_item
     WHERE work_item_id=(requested_result->>'work_item_id')::uuid FOR UPDATE;
    SELECT * INTO STRICT stage FROM orchestration.pipeline_stage WHERE stage_id=item.stage_id;
    v_effect:=migration.region_talk_stage_uuid5('region-talk-stage-effect:'||item.work_item_id::text||':'||
      (requested_result->>'attempt')||':'||item.input_fingerprint);
    v_metadata_hash:=migration.region_talk_json_sha256(requested_result->'result_metadata');
    IF item.payload->>'stage_run_id'<>requested_result->>'stage_run_id'
       OR NOT EXISTS(SELECT 1 FROM migration.region_talk_post_import_stage_run exact_run
          WHERE exact_run.stage_run_id=(requested_result->>'stage_run_id')::uuid
            AND exact_run.task_run_id=requested_task_run_id
            AND exact_run.export_batch_id=requested_export_batch_id)
       OR item.subject_type<>requested_result->>'subject_type'
       OR item.subject_id<>(requested_result->>'subject_id')::uuid
       OR item.input_fingerprint<>requested_result->>'input_fingerprint'
       OR item.attempt_count<>(requested_result->>'attempt')::integer
       OR item.lease_token<>(requested_result->>'lease_token')::uuid
       OR item.lease_expires_at<=clock_timestamp()
       OR stage.stage_key<>requested_result->>'stage'
       OR stage.contract->>'name'<>requested_result->>'contract_version'
       OR item.payload->>'candidate_revision'<>requested_result->>'candidate_revision'
       OR item.payload->>'revision_fingerprint'<>requested_result->>'revision_fingerprint'
       OR v_effect<>(requested_result->>'effect_id')::uuid
       OR v_metadata_hash<>requested_result->>'metadata_sha256'
       OR requested_result->'result_metadata'->>'stage'<>stage.stage_key
       OR requested_result->'result_metadata'->>'contract_version'<>stage.contract->>'name'
       OR requested_result->'result_metadata'->>'subject_id'<>item.subject_id::text
       OR requested_result->'result_metadata'->>'candidate_revision'<>
          requested_result->>'candidate_revision'
       OR requested_result->'result_metadata'->>'revision_fingerprint'<>
          requested_result->>'revision_fingerprint'
       OR requested_result->'result_metadata'->>'input_fingerprint'<>item.input_fingerprint THEN
        RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='Region Talk result differs from exact lease';
    END IF;
    INSERT INTO migration.region_talk_stage_worker_result(
      work_item_id,attempt,task_run_id,export_batch_id,stage_run_id,master_instance_id,epoch,
      stage,contract_version,subject_type,subject_id,candidate_revision,revision_fingerprint,
      input_fingerprint,effect_id,result_status,result_metadata,metadata_sha256,result_sha256,completed_at
    ) VALUES(item.work_item_id,(requested_result->>'attempt')::integer,requested_task_run_id,
      requested_export_batch_id,(requested_result->>'stage_run_id')::uuid,
      registration.master_instance_id,registration.epoch,stage.stage_key,stage.contract->>'name',
      item.subject_type,item.subject_id,(requested_result->>'candidate_revision')::integer,
      requested_result->>'revision_fingerprint',item.input_fingerprint,v_effect,
      requested_result->>'result_status',requested_result->'result_metadata',v_metadata_hash,
      requested_result->>'result_sha256',(requested_result->>'completed_at')::timestamptz)
    ON CONFLICT(work_item_id,attempt) DO NOTHING;
    SELECT * INTO STRICT existing FROM migration.region_talk_stage_worker_result
     WHERE work_item_id=item.work_item_id AND attempt=(requested_result->>'attempt')::integer;
    IF existing.effect_id<>v_effect OR existing.metadata_sha256<>v_metadata_hash
       OR existing.result_sha256<>requested_result->>'result_sha256'
       OR existing.result_status<>requested_result->>'result_status' THEN
        RAISE EXCEPTION USING ERRCODE='23505',MESSAGE='Region Talk result idempotency conflict';
    END IF;
    UPDATE orchestration.work_item SET status=CASE existing.result_status
        WHEN 'SUCCEEDED' THEN 'succeeded' WHEN 'FAILED_TERMINAL' THEN 'failed_terminal'
        ELSE 'failed_retryable' END,
      lease_owner=NULL,lease_token=NULL,lease_expires_at=NULL,
      result_ref=jsonb_build_object('schema_version','region-talk-stage-result-ref.v1',
        'attempt',existing.attempt,'result_sha256',existing.result_sha256,
        'metadata_sha256',existing.metadata_sha256),
      last_error=CASE WHEN existing.result_status='SUCCEEDED' THEN NULL ELSE jsonb_build_object(
        'schema_version','region-talk-stage-failure.v1','result_sha256',existing.result_sha256) END
     WHERE work_item_id=item.work_item_id;
    v_base:=jsonb_build_object('schema_version','region-talk-stage-worker-result-receipt.v1',
      'accepted',true,'master_instance_id',registration.master_instance_id,'epoch',registration.epoch,
      'task_run_id',requested_task_run_id,'export_batch_id',requested_export_batch_id,
      'stage_run_id',existing.stage_run_id,'work_item_id',existing.work_item_id,
      'stage',existing.stage,'subject_id',existing.subject_id,'input_fingerprint',existing.input_fingerprint,
      'attempt',existing.attempt,'effect_id',existing.effect_id,'result_status',existing.result_status,
      'metadata_sha256',existing.metadata_sha256,'result_sha256',existing.result_sha256,
      'publication_dispatch',false,'notification_dispatch',false);
    RETURN v_base||jsonb_build_object('receipt_sha256',migration.region_talk_json_sha256(v_base));
END
$$;

CREATE FUNCTION migration.region_talk_stage_work_status(
    requested_task_run_id uuid,requested_export_batch_id uuid,requested_request jsonb
) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog
AS $$
DECLARE registration master_control.task_credential_registration%ROWTYPE;
  run migration.region_talk_post_import_stage_run%ROWTYPE; item orchestration.work_item%ROWTYPE;
  v_id uuid; v_base jsonb; v_counts jsonb;
BEGIN
  IF requested_request->>'schema_version'<>'region-talk-stage-work-status-request.v1'
     OR requested_request - ARRAY['schema_version','work_item_id','requested_at']::text[]<>'{}'::jsonb
     OR requested_request->>'requested_at' IS NULL THEN
    RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='Region Talk status request violates fixed contract';
  END IF;
  BEGIN PERFORM (requested_request->>'requested_at')::timestamptz;
  EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='Region Talk status request time is invalid';
  END;
  registration:=master_control.assert_registered_task_credential('region_talk',requested_task_run_id);
  SELECT * INTO STRICT run FROM migration.region_talk_post_import_stage_run
   WHERE task_run_id=requested_task_run_id AND export_batch_id=requested_export_batch_id;
  IF requested_request->>'work_item_id' IS NOT NULL THEN
    v_id:=(requested_request->>'work_item_id')::uuid;
    SELECT * INTO STRICT item FROM orchestration.work_item
     WHERE work_item_id=v_id AND payload->>'stage_run_id'=run.stage_run_id::text;
    v_base:=jsonb_build_object('schema_version','region-talk-stage-work-status.v1','scope','WORK_ITEM',
      'master_instance_id',registration.master_instance_id,'epoch',registration.epoch,
      'task_run_id',requested_task_run_id,'export_batch_id',requested_export_batch_id,
      'stage_run_id',run.stage_run_id,'work_item_id',item.work_item_id,'subject_type',item.subject_type,
      'subject_id',item.subject_id,'input_fingerprint',item.input_fingerprint,'attempt',item.attempt_count,
      'status',upper(item.status),'lease_expires_at',item.lease_expires_at,
      'result_ref',item.result_ref,'publication_dispatch',false,'notification_dispatch',false);
  ELSE
    SELECT coalesce(jsonb_object_agg(status,total),'{}'::jsonb) INTO v_counts FROM (
      SELECT status,count(*) total FROM orchestration.work_item
       WHERE payload->>'stage_run_id'=run.stage_run_id::text GROUP BY status) counted;
    v_base:=jsonb_build_object('schema_version','region-talk-stage-work-status.v1','scope','STAGE_RUN',
      'master_instance_id',registration.master_instance_id,'epoch',registration.epoch,
      'task_run_id',requested_task_run_id,'export_batch_id',requested_export_batch_id,
      'stage_run_id',run.stage_run_id,'work_item_id',NULL,'status',run.state,'counts',v_counts,
      'publication_dispatch',false,'notification_dispatch',false);
  END IF;
  RETURN v_base||jsonb_build_object('receipt_sha256',migration.region_talk_json_sha256(v_base));
END
$$;

-- Only exact-current review rows or current publication plans enter the MCP queue.
-- A candidate represented by both is projected once; dispatch remains disabled.
CREATE OR REPLACE VIEW region_talk.publication_queue_v3 AS
SELECT candidate.candidate_id,
       CASE WHEN review_queue.candidate_id IS NOT NULL THEN 'in_review' ELSE candidate.status END AS candidate_status,
       candidate.current_revision,content.content_id,content.content_type,content.title,content.summary,
       content.canonical_url,plan.publication_plan_id,plan.channel,plan.status AS plan_status,
       plan.scheduled_for,plan.payload->>'legacy_status' AS legacy_status,accepted.canonical_revision,
       greatest(candidate.updated_at,coalesce(review_queue.created_at,candidate.updated_at)) AS updated_at,
       review.decision AS review_decision,review.actor_ref AS review_actor_ref,
       review.reason AS review_reason,review.occurred_at AS review_occurred_at
FROM region_talk.accepted_snapshot_v2 accepted
JOIN hub.content_item content ON content.metadata->>'region_talk_snapshot_id'=accepted.export_batch_id::text
JOIN region_talk.publication_candidate candidate USING(content_id)
LEFT JOIN region_talk.publication_plan plan
  ON plan.candidate_id=candidate.candidate_id AND plan.candidate_revision=candidate.current_revision
LEFT JOIN LATERAL(
  SELECT queue.* FROM region_talk.post_import_review_queue queue
  JOIN migration.region_talk_post_import_stage_run stage_run USING(stage_run_id)
   WHERE queue.candidate_id=candidate.candidate_id
     AND queue.candidate_revision=candidate.current_revision
     AND stage_run.export_batch_id=accepted.export_batch_id
     AND stage_run.canonical_revision=accepted.canonical_revision
     AND NOT queue.publication_dispatch AND NOT queue.notification_dispatch
   ORDER BY queue.created_at DESC,queue.stage_run_id DESC LIMIT 1
) review_queue ON true
LEFT JOIN LATERAL(
  SELECT decision_row.decision,decision_row.actor_ref,decision_row.reason,decision_row.occurred_at
    FROM region_talk.review_decision decision_row
   WHERE decision_row.candidate_id=candidate.candidate_id
     AND decision_row.candidate_revision=candidate.current_revision
   ORDER BY decision_row.occurred_at DESC,decision_row.review_decision_id DESC LIMIT 1
) review ON true
WHERE plan.publication_plan_id IS NOT NULL OR review_queue.candidate_id IS NOT NULL;

CREATE OR REPLACE VIEW region_talk.publication_queue_summary_v3 AS
SELECT candidate_status,plan_status,channel,count(*) AS item_count,
       min(scheduled_for) AS earliest_scheduled_for,max(updated_at) AS latest_update
FROM region_talk.publication_queue_v3 GROUP BY candidate_status,plan_status,channel;

REVOKE ALL ON migration.region_talk_stage_worker_result,
    migration.region_talk_post_import_cycle_outcome,
    migration.region_talk_post_import_cycle_receipt FROM PUBLIC,mdh_mcp_reader,
    mdh_mcp_editor,mdh_region_talk_pipeline;
REVOKE EXECUTE ON FUNCTION migration.region_talk_canonical_json(jsonb),
    migration.region_talk_json_sha256(jsonb),
    migration.execute_region_talk_post_import_stages_v1_unverified(uuid,uuid,jsonb),
    migration.execute_region_talk_post_import_stages(uuid,uuid,jsonb),
    migration.claim_region_talk_stage_work(uuid,uuid,jsonb),
    migration.submit_region_talk_stage_result(uuid,uuid,jsonb),
    migration.region_talk_stage_work_status(uuid,uuid,jsonb)
    FROM PUBLIC,mdh_mcp_reader,mdh_mcp_editor,mdh_region_talk_pipeline;
GRANT EXECUTE ON FUNCTION migration.execute_region_talk_post_import_stages(uuid,uuid,jsonb),
    migration.claim_region_talk_stage_work(uuid,uuid,jsonb),
    migration.submit_region_talk_stage_result(uuid,uuid,jsonb),
    migration.region_talk_stage_work_status(uuid,uuid,jsonb) TO mdh_region_talk_pipeline;
GRANT SELECT ON region_talk.publication_queue_v3,
    region_talk.publication_queue_summary_v3 TO mdh_mcp_reader;

UPDATE hub.canonical_state SET schema_revision=27,updated_at=clock_timestamp() WHERE singleton=true;
