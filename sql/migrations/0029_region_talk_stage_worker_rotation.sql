-- Region Talk v8: rotate long-running private stage worker credentials without
-- changing the deterministic worker task, dispatch, effect, work item, attempt, or lease.

CREATE TABLE migration.region_talk_stage_worker_generation (
    dispatch_id              uuid NOT NULL
                              REFERENCES migration.region_talk_stage_dispatch_claim(dispatch_id),
    worker_generation        bigint NOT NULL CHECK(worker_generation>=1),
    effect_id                uuid NOT NULL,
    supervisor_task_run_id   uuid NOT NULL,
    supervisor_credential_id uuid NOT NULL,
    supervisor_generation    bigint NOT NULL CHECK(supervisor_generation>=1),
    worker_task_run_id       uuid NOT NULL,
    worker_credential_id     uuid NOT NULL UNIQUE
                              REFERENCES master_control.task_credential_registration(credential_id),
    master_instance_id       uuid NOT NULL,
    epoch                    bigint NOT NULL CHECK(epoch>=1),
    worker_command_sha256    text NOT NULL CHECK(worker_command_sha256 ~ '^[a-f0-9]{64}$'),
    worker_task_token_sha256 text NOT NULL CHECK(worker_task_token_sha256 ~ '^[a-f0-9]{64}$'),
    claim_receipt_sha256     text NOT NULL CHECK(claim_receipt_sha256 ~ '^[a-f0-9]{64}$'),
    prior_worker_generation  bigint CHECK(prior_worker_generation IS NULL OR prior_worker_generation>=1),
    prior_worker_binding_sha256 text CHECK(
                               prior_worker_binding_sha256 IS NULL OR
                               prior_worker_binding_sha256 ~ '^[a-f0-9]{64}$'),
    worker_binding_sha256    text NOT NULL UNIQUE CHECK(worker_binding_sha256 ~ '^[a-f0-9]{64}$'),
    binding_receipt          jsonb NOT NULL,
    bound_at                 timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(dispatch_id,worker_generation),
    UNIQUE(dispatch_id,worker_credential_id),
    CHECK((prior_worker_generation IS NULL)=(prior_worker_binding_sha256 IS NULL))
);
CREATE TRIGGER region_talk_stage_worker_generation_append_only
BEFORE UPDATE OR DELETE ON migration.region_talk_stage_worker_generation
FOR EACH ROW EXECUTE FUNCTION hub_meta.reject_update_delete();

CREATE VIEW migration.region_talk_stage_worker_generation_status_v1 AS
SELECT generation.*,
       CASE WHEN generation.worker_generation=max(generation.worker_generation)
                  OVER(PARTITION BY generation.dispatch_id)
            THEN 'ACTIVE' ELSE 'FENCED' END AS binding_status
FROM migration.region_talk_stage_worker_generation generation;

CREATE FUNCTION migration.capture_region_talk_initial_worker_generation()
RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog
AS $$
BEGIN
  INSERT INTO migration.region_talk_stage_worker_generation(
    dispatch_id,worker_generation,effect_id,supervisor_task_run_id,
    supervisor_credential_id,supervisor_generation,worker_task_run_id,
    worker_credential_id,master_instance_id,epoch,worker_command_sha256,
    worker_task_token_sha256,claim_receipt_sha256,prior_worker_generation,
    prior_worker_binding_sha256,worker_binding_sha256,binding_receipt,bound_at)
  VALUES(NEW.dispatch_id,NEW.worker_generation,NEW.effect_id,NEW.supervisor_task_run_id,
    NEW.supervisor_credential_id,NEW.supervisor_generation,NEW.worker_task_run_id,
    NEW.worker_credential_id,NEW.master_instance_id,NEW.epoch,NEW.worker_command_sha256,
    NEW.worker_task_token_sha256,NEW.claim_receipt_sha256,NULL,NULL,
    NEW.worker_binding_sha256,NEW.binding_receipt,NEW.bound_at)
  ON CONFLICT(dispatch_id,worker_generation) DO NOTHING;
  RETURN NEW;
END
$$;
CREATE TRIGGER region_talk_stage_worker_binding_capture_generation
AFTER INSERT ON migration.region_talk_stage_worker_binding
FOR EACH ROW EXECUTE FUNCTION migration.capture_region_talk_initial_worker_generation();

INSERT INTO migration.region_talk_stage_worker_generation(
  dispatch_id,worker_generation,effect_id,supervisor_task_run_id,
  supervisor_credential_id,supervisor_generation,worker_task_run_id,
  worker_credential_id,master_instance_id,epoch,worker_command_sha256,
  worker_task_token_sha256,claim_receipt_sha256,prior_worker_generation,
  prior_worker_binding_sha256,worker_binding_sha256,binding_receipt,bound_at)
SELECT binding.dispatch_id,binding.worker_generation,binding.effect_id,
  binding.supervisor_task_run_id,binding.supervisor_credential_id,
  binding.supervisor_generation,binding.worker_task_run_id,binding.worker_credential_id,
  binding.master_instance_id,binding.epoch,binding.worker_command_sha256,
  binding.worker_task_token_sha256,binding.claim_receipt_sha256,NULL,NULL,
  binding.worker_binding_sha256,binding.binding_receipt,binding.bound_at
FROM migration.region_talk_stage_worker_binding binding
ON CONFLICT(dispatch_id,worker_generation) DO NOTHING;

CREATE FUNCTION migration.rotate_region_talk_stage_worker_credential(
    requested_supervisor_task_run_id uuid,requested_export_batch_id uuid,requested_request jsonb
) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog
AS $$
DECLARE supervisor master_control.task_credential_registration%ROWTYPE;
  worker master_control.task_credential_registration%ROWTYPE;
  credential master_control.credential_binding%ROWTYPE;
  claim migration.region_talk_stage_dispatch_claim%ROWTYPE;
  item orchestration.work_item%ROWTYPE;
  current_binding migration.region_talk_stage_worker_generation%ROWTYPE;
  existing migration.region_talk_stage_worker_generation%ROWTYPE;
  v_new_generation bigint; v_base jsonb; v_receipt jsonb; v_binding_hash text;
BEGIN
  IF requested_request->>'schema_version'<>'region-talk-stage-worker-rotate.v1'
     OR requested_request - ARRAY['schema_version','dispatch_id','effect_id','work_item_id',
       'worker_task_run_id','prior_worker_generation','prior_worker_binding_sha256',
       'new_worker_credential_id','new_worker_generation','new_worker_command_sha256',
       'new_worker_task_token_sha256','requested_at','publication_dispatch',
       'notification_dispatch']::text[] <> '{}'::jsonb
     OR requested_request->>'prior_worker_binding_sha256' !~ '^[a-f0-9]{64}$'
     OR requested_request->>'new_worker_command_sha256' !~ '^[a-f0-9]{64}$'
     OR requested_request->>'new_worker_task_token_sha256' !~ '^[a-f0-9]{64}$'
     OR requested_request->'publication_dispatch' IS DISTINCT FROM 'false'::jsonb
     OR requested_request->'notification_dispatch' IS DISTINCT FROM 'false'::jsonb THEN
    RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='stage worker rotation violates fixed contract';
  END IF;
  BEGIN
    v_new_generation:=(requested_request->>'new_worker_generation')::bigint;
    PERFORM (requested_request->>'requested_at')::timestamptz;
  EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='stage worker rotation identity is invalid';
  END;
  supervisor:=master_control.assert_registered_task_credential(
    'region_talk',requested_supervisor_task_run_id);
  IF EXISTS(SELECT 1 FROM migration.region_talk_stage_worker_generation worker_binding
             WHERE worker_binding.worker_credential_id=supervisor.credential_id) THEN
    RAISE EXCEPTION USING ERRCODE='42501',MESSAGE='worker credential cannot rotate worker authority';
  END IF;
  SELECT * INTO STRICT claim FROM migration.region_talk_stage_dispatch_claim dispatch
   WHERE dispatch.dispatch_id=(requested_request->>'dispatch_id')::uuid
     AND dispatch.effect_id=(requested_request->>'effect_id')::uuid
     AND dispatch.work_item_id=(requested_request->>'work_item_id')::uuid
     AND dispatch.worker_task_run_id=(requested_request->>'worker_task_run_id')::uuid
     AND dispatch.supervisor_task_run_id=requested_supervisor_task_run_id
     AND dispatch.export_batch_id=requested_export_batch_id FOR UPDATE;
  IF claim.master_instance_id<>supervisor.master_instance_id OR claim.epoch<>supervisor.epoch
     OR claim.lease_expires_at<=clock_timestamp() THEN
    RAISE EXCEPTION USING ERRCODE='42501',MESSAGE='stage worker rotation crosses supervisor authority';
  END IF;
  SELECT * INTO existing FROM migration.region_talk_stage_worker_generation generation
   WHERE generation.dispatch_id=claim.dispatch_id
     AND generation.worker_generation=v_new_generation;
  IF FOUND THEN
    IF existing.worker_credential_id<>(requested_request->>'new_worker_credential_id')::uuid
       OR existing.worker_task_run_id<>claim.worker_task_run_id
       OR existing.worker_command_sha256<>requested_request->>'new_worker_command_sha256'
       OR existing.worker_task_token_sha256<>requested_request->>'new_worker_task_token_sha256'
       OR existing.prior_worker_generation<>(requested_request->>'prior_worker_generation')::bigint
       OR existing.prior_worker_binding_sha256<>
          requested_request->>'prior_worker_binding_sha256' THEN
      RAISE EXCEPTION USING ERRCODE='23505',MESSAGE='stage worker rotation idempotency conflict';
    END IF;
    RETURN existing.binding_receipt;
  END IF;
  SELECT * INTO STRICT item FROM orchestration.work_item work
   WHERE work.work_item_id=claim.work_item_id FOR UPDATE;
  IF item.status<>'leased' OR item.lease_token<>claim.lease_token
     OR item.attempt_count<>claim.attempt OR item.input_fingerprint<>claim.input_fingerprint THEN
    RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='stage worker rotation is outside exact live lease';
  END IF;
  SELECT * INTO STRICT current_binding
    FROM migration.region_talk_stage_worker_generation generation
   WHERE generation.dispatch_id=claim.dispatch_id
   ORDER BY generation.worker_generation DESC LIMIT 1;
  IF (requested_request->>'prior_worker_generation')::bigint<>current_binding.worker_generation
     OR requested_request->>'prior_worker_binding_sha256'<>current_binding.worker_binding_sha256
     OR v_new_generation<>current_binding.worker_generation+1 THEN
    RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='stage worker rotation is stale or skips a generation';
  END IF;
  SELECT registration.* INTO STRICT worker
    FROM master_control.task_credential_registration registration
   WHERE registration.credential_id=(requested_request->>'new_worker_credential_id')::uuid
     AND registration.worker_kind='region_talk'
     AND registration.task_run_id=claim.worker_task_run_id
     AND registration.generation=v_new_generation;
  SELECT binding.* INTO STRICT credential FROM master_control.credential_binding binding
   WHERE binding.credential_id=worker.credential_id AND binding.principal=worker.principal
     AND binding.master_instance_id=worker.master_instance_id AND binding.epoch=worker.epoch
     AND binding.revoked_at IS NULL AND binding.expires_at>clock_timestamp();
  IF worker.credential_id=supervisor.credential_id OR worker.principal=supervisor.principal
     OR worker.master_instance_id<>supervisor.master_instance_id OR worker.epoch<>supervisor.epoch
     OR worker.command_sha256<>requested_request->>'new_worker_command_sha256'
     OR worker.task_token_sha256<>requested_request->>'new_worker_task_token_sha256' THEN
    RAISE EXCEPTION USING ERRCODE='42501',MESSAGE='rotated worker credential differs from exact task authority';
  END IF;
  v_base:=jsonb_build_object('schema_version','region-talk-stage-worker-rotate-receipt.v1',
    'rotated',true,'master_instance_id',supervisor.master_instance_id,'epoch',supervisor.epoch,
    'supervisor_task_run_id',requested_supervisor_task_run_id,
    'export_batch_id',requested_export_batch_id,'stage_run_id',claim.stage_run_id,
    'dispatch_id',claim.dispatch_id,'work_item_id',claim.work_item_id,'effect_id',claim.effect_id,
    'worker_task_run_id',claim.worker_task_run_id,
    'prior_worker_generation',current_binding.worker_generation,
    'prior_worker_binding_sha256',current_binding.worker_binding_sha256,
    'worker_credential_id',worker.credential_id,'worker_generation',worker.generation,
    'publication_dispatch',false,'notification_dispatch',false);
  v_binding_hash:=migration.region_talk_json_sha256(v_base);
  v_receipt:=v_base||jsonb_build_object('worker_binding_sha256',v_binding_hash,
    'receipt_sha256',migration.region_talk_json_sha256(
      v_base||jsonb_build_object('worker_binding_sha256',v_binding_hash)));
  INSERT INTO migration.region_talk_stage_worker_generation(
    dispatch_id,worker_generation,effect_id,supervisor_task_run_id,
    supervisor_credential_id,supervisor_generation,worker_task_run_id,
    worker_credential_id,master_instance_id,epoch,worker_command_sha256,
    worker_task_token_sha256,claim_receipt_sha256,prior_worker_generation,
    prior_worker_binding_sha256,worker_binding_sha256,binding_receipt)
  VALUES(claim.dispatch_id,worker.generation,claim.effect_id,claim.supervisor_task_run_id,
    supervisor.credential_id,supervisor.generation,claim.worker_task_run_id,
    worker.credential_id,worker.master_instance_id,worker.epoch,worker.command_sha256,
    worker.task_token_sha256,claim.source_claim_sha256,current_binding.worker_generation,
    current_binding.worker_binding_sha256,v_binding_hash,v_receipt);
  RETURN v_receipt;
END
$$;

CREATE OR REPLACE FUNCTION migration.fetch_region_talk_stage_work_payload(
    requested_worker_task_run_id uuid,requested_effect_id uuid,requested_request jsonb
) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog
AS $$
DECLARE worker master_control.task_credential_registration%ROWTYPE;
  binding migration.region_talk_stage_worker_generation%ROWTYPE;
  claim migration.region_talk_stage_dispatch_claim%ROWTYPE; item orchestration.work_item%ROWTYPE;
  v_base jsonb;
BEGIN
  IF requested_request->>'schema_version'<>'region-talk-stage-work-payload-fetch.v1'
     OR requested_request - ARRAY['schema_version','worker_task_run_id','dispatch_id','effect_id',
       'worker_binding_sha256','requested_at','publication_dispatch','notification_dispatch']::text[]<>'{}'::jsonb
     OR requested_request->>'worker_task_run_id' IS DISTINCT FROM requested_worker_task_run_id::text
     OR requested_request->>'effect_id' IS DISTINCT FROM requested_effect_id::text
     OR requested_request->>'worker_binding_sha256' !~ '^[a-f0-9]{64}$'
     OR requested_request->'publication_dispatch' IS DISTINCT FROM 'false'::jsonb
     OR requested_request->'notification_dispatch' IS DISTINCT FROM 'false'::jsonb THEN
    RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='stage payload fetch violates fixed contract';
  END IF;
  BEGIN PERFORM (requested_request->>'requested_at')::timestamptz;
  EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='stage payload fetch time is invalid'; END;
  worker:=master_control.assert_registered_task_credential('region_talk',requested_worker_task_run_id);
  SELECT exact_binding.* INTO STRICT binding
    FROM migration.region_talk_stage_worker_generation exact_binding
   WHERE exact_binding.effect_id=requested_effect_id
     AND exact_binding.dispatch_id=(requested_request->>'dispatch_id')::uuid
     AND exact_binding.worker_task_run_id=requested_worker_task_run_id
     AND exact_binding.worker_credential_id=worker.credential_id
     AND exact_binding.worker_generation=worker.generation
     AND exact_binding.worker_generation=(SELECT max(current.worker_generation)
       FROM migration.region_talk_stage_worker_generation current
       WHERE current.dispatch_id=exact_binding.dispatch_id)
     AND exact_binding.worker_binding_sha256=requested_request->>'worker_binding_sha256';
  SELECT * INTO STRICT claim FROM migration.region_talk_stage_dispatch_claim dispatch
   WHERE dispatch.dispatch_id=binding.dispatch_id;
  SELECT * INTO STRICT item FROM orchestration.work_item work
   WHERE work.work_item_id=claim.work_item_id FOR UPDATE;
  IF binding.master_instance_id<>worker.master_instance_id OR binding.epoch<>worker.epoch
     OR claim.lease_expires_at<=clock_timestamp() OR item.status<>'leased'
     OR item.lease_token<>claim.lease_token OR item.attempt_count<>claim.attempt
     OR item.input_fingerprint<>claim.input_fingerprint THEN
    RAISE EXCEPTION USING ERRCODE='42501',MESSAGE='stage payload fetch is outside exact live lease';
  END IF;
  v_base:=jsonb_build_object('schema_version','region-talk-stage-work-payload-receipt.v1',
    'master_instance_id',worker.master_instance_id,'epoch',worker.epoch,
    'supervisor_task_run_id',claim.supervisor_task_run_id,'worker_task_run_id',worker.task_run_id,
    'export_batch_id',claim.export_batch_id,'stage_run_id',claim.stage_run_id,
    'dispatch_id',claim.dispatch_id,'work_item_id',claim.work_item_id,'effect_id',claim.effect_id,
    'stage',claim.stage,'contract_version',claim.contract_version,'subject_type',claim.subject_type,
    'subject_id',claim.subject_id,'input_fingerprint',claim.input_fingerprint,'attempt',claim.attempt,
    'lease_token',claim.lease_token,'lease_expires_at',claim.lease_expires_at,
    'worker_binding_sha256',binding.worker_binding_sha256,'payload',claim.payload,
    'publication_dispatch',false,'notification_dispatch',false);
  RETURN v_base||jsonb_build_object('receipt_sha256',migration.region_talk_json_sha256(v_base));
END
$$;


CREATE OR REPLACE FUNCTION migration.submit_region_talk_stage_worker_result(
    requested_worker_task_run_id uuid,requested_effect_id uuid,requested_result jsonb
) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog
AS $$
DECLARE worker master_control.task_credential_registration%ROWTYPE;
  binding migration.region_talk_stage_worker_generation%ROWTYPE;
  claim migration.region_talk_stage_dispatch_claim%ROWTYPE; item orchestration.work_item%ROWTYPE;
  existing migration.region_talk_stage_worker_result%ROWTYPE;
  v_metadata_hash text; v_base jsonb;
BEGIN
  IF requested_result->>'schema_version'<>'region-talk-stage-worker-direct-result.v1'
     OR requested_result - ARRAY['schema_version','worker_task_run_id','dispatch_id','effect_id',
       'worker_binding_sha256','work_item_id','attempt','result_status','result_metadata',
       'metadata_sha256','result_sha256','completed_at','publication_dispatch','notification_dispatch']::text[]
       <> '{}'::jsonb
     OR requested_result->>'worker_task_run_id' IS DISTINCT FROM requested_worker_task_run_id::text
     OR requested_result->>'effect_id' IS DISTINCT FROM requested_effect_id::text
     OR requested_result->>'result_status' NOT IN('SUCCEEDED','FAILED_RETRYABLE','FAILED_TERMINAL')
     OR requested_result->>'metadata_sha256' !~ '^[a-f0-9]{64}$'
     OR requested_result->>'result_sha256' !~ '^[a-f0-9]{64}$'
     OR requested_result->'publication_dispatch' IS DISTINCT FROM 'false'::jsonb
     OR requested_result->'notification_dispatch' IS DISTINCT FROM 'false'::jsonb
     OR jsonb_typeof(requested_result->'result_metadata')<>'object'
     OR (requested_result->'result_metadata') - ARRAY['schema_version','stage','contract_version',
       'subject_type','subject_id','candidate_revision','revision_fingerprint','input_fingerprint',
       'producer_exact_id','metrics','artifact_sha256']::text[] <> '{}'::jsonb
     OR length(coalesce(requested_result->'result_metadata'->>'producer_exact_id',''))
       NOT BETWEEN 1 AND 500
     OR jsonb_typeof(requested_result->'result_metadata'->'metrics')<>'object'
     OR (requested_result->'result_metadata'->>'artifact_sha256' IS NOT NULL AND
         requested_result->'result_metadata'->>'artifact_sha256' !~ '^[a-f0-9]{64}$')
     OR octet_length((requested_result->'result_metadata')::text)>65536 THEN
    RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='direct stage worker result violates fixed contract';
  END IF;
  worker:=master_control.assert_registered_task_credential('region_talk',requested_worker_task_run_id);
  SELECT exact_binding.* INTO STRICT binding
    FROM migration.region_talk_stage_worker_generation exact_binding
   WHERE exact_binding.effect_id=requested_effect_id
     AND exact_binding.dispatch_id=(requested_result->>'dispatch_id')::uuid
     AND exact_binding.worker_task_run_id=requested_worker_task_run_id
     AND exact_binding.worker_credential_id=worker.credential_id
     AND exact_binding.worker_generation=worker.generation
     AND exact_binding.worker_generation=(SELECT max(current.worker_generation)
       FROM migration.region_talk_stage_worker_generation current
       WHERE current.dispatch_id=exact_binding.dispatch_id)
     AND exact_binding.worker_binding_sha256=requested_result->>'worker_binding_sha256';
  SELECT * INTO STRICT claim FROM migration.region_talk_stage_dispatch_claim dispatch
   WHERE dispatch.dispatch_id=binding.dispatch_id;
  SELECT * INTO STRICT item FROM orchestration.work_item work
   WHERE work.work_item_id=claim.work_item_id FOR UPDATE;
  v_metadata_hash:=migration.region_talk_json_sha256(requested_result->'result_metadata');
  IF binding.master_instance_id<>worker.master_instance_id OR binding.epoch<>worker.epoch
     OR requested_result->>'work_item_id'<>claim.work_item_id::text
     OR (requested_result->>'attempt')::integer<>claim.attempt
     OR requested_result->>'metadata_sha256'<>v_metadata_hash
     OR requested_result->'result_metadata'->>'schema_version'<>'region-talk-stage-result-metadata.v1'
     OR requested_result->'result_metadata'->>'stage'<>claim.stage
     OR requested_result->'result_metadata'->>'contract_version'<>claim.contract_version
     OR requested_result->'result_metadata'->>'subject_type'<>claim.subject_type
     OR requested_result->'result_metadata'->>'subject_id'<>claim.subject_id::text
     OR requested_result->'result_metadata'->>'candidate_revision'<>claim.payload->>'candidate_revision'
     OR requested_result->'result_metadata'->>'revision_fingerprint'<>claim.payload->>'revision_fingerprint'
     OR requested_result->'result_metadata'->>'input_fingerprint'<>claim.input_fingerprint THEN
    RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='direct worker result differs from exact binding';
  END IF;
  IF NOT EXISTS(SELECT 1 FROM migration.region_talk_stage_worker_result landed
    WHERE landed.work_item_id=claim.work_item_id AND landed.attempt=claim.attempt) THEN
    IF claim.lease_expires_at<=clock_timestamp() OR item.status<>'leased'
       OR item.lease_token<>claim.lease_token
       OR item.attempt_count<>claim.attempt OR item.input_fingerprint<>claim.input_fingerprint THEN
      RAISE EXCEPTION USING ERRCODE='42501',MESSAGE='direct worker result is outside exact live lease';
    END IF;
  END IF;
  INSERT INTO migration.region_talk_stage_worker_result(
    work_item_id,attempt,task_run_id,export_batch_id,stage_run_id,master_instance_id,epoch,
    stage,contract_version,subject_type,subject_id,candidate_revision,revision_fingerprint,
    input_fingerprint,effect_id,result_status,result_metadata,metadata_sha256,result_sha256,completed_at)
  VALUES(claim.work_item_id,claim.attempt,claim.supervisor_task_run_id,claim.export_batch_id,
    claim.stage_run_id,claim.master_instance_id,claim.epoch,claim.stage,claim.contract_version,
    claim.subject_type,claim.subject_id,(claim.payload->>'candidate_revision')::integer,
    claim.payload->>'revision_fingerprint',claim.input_fingerprint,claim.effect_id,
    requested_result->>'result_status',requested_result->'result_metadata',v_metadata_hash,
    requested_result->>'result_sha256',(requested_result->>'completed_at')::timestamptz)
  ON CONFLICT(work_item_id,attempt) DO NOTHING;
  SELECT * INTO STRICT existing FROM migration.region_talk_stage_worker_result landed
   WHERE landed.work_item_id=claim.work_item_id AND landed.attempt=claim.attempt;
  IF existing.effect_id<>claim.effect_id OR existing.metadata_sha256<>v_metadata_hash
     OR existing.result_sha256<>requested_result->>'result_sha256'
     OR existing.result_status<>requested_result->>'result_status'
     OR existing.master_instance_id<>binding.master_instance_id OR existing.epoch<>binding.epoch THEN
    RAISE EXCEPTION USING ERRCODE='23505',MESSAGE='direct worker result idempotency conflict';
  END IF;
  UPDATE orchestration.work_item SET status=CASE existing.result_status
      WHEN 'SUCCEEDED' THEN 'succeeded' WHEN 'FAILED_TERMINAL' THEN 'failed_terminal'
      ELSE 'failed_retryable' END,lease_owner=NULL,lease_token=NULL,lease_expires_at=NULL,
    result_ref=jsonb_build_object('schema_version','region-talk-stage-result-ref.v1',
      'attempt',existing.attempt,'result_sha256',existing.result_sha256,
      'metadata_sha256',existing.metadata_sha256),
    last_error=CASE WHEN existing.result_status='SUCCEEDED' THEN NULL ELSE jsonb_build_object(
      'schema_version','region-talk-stage-failure.v1','result_sha256',existing.result_sha256) END
   WHERE work_item_id=claim.work_item_id;
  v_base:=jsonb_build_object('schema_version','region-talk-stage-worker-direct-result-receipt.v1',
    'accepted',true,'master_instance_id',binding.master_instance_id,'epoch',binding.epoch,
    'supervisor_task_run_id',claim.supervisor_task_run_id,
    'worker_task_run_id',binding.worker_task_run_id,'export_batch_id',claim.export_batch_id,
    'stage_run_id',claim.stage_run_id,'dispatch_id',claim.dispatch_id,
    'work_item_id',claim.work_item_id,'effect_id',claim.effect_id,'stage',claim.stage,
    'subject_id',claim.subject_id,'input_fingerprint',claim.input_fingerprint,'attempt',claim.attempt,
    'result_status',existing.result_status,'metadata_sha256',existing.metadata_sha256,
    'result_sha256',existing.result_sha256,'worker_binding_sha256',binding.worker_binding_sha256,
    'publication_dispatch',false,'notification_dispatch',false);
  RETURN v_base||jsonb_build_object('receipt_sha256',migration.region_talk_json_sha256(v_base));
END
$$;


CREATE OR REPLACE FUNCTION migration.region_talk_stage_supervisor_status(
    requested_supervisor_task_run_id uuid,requested_export_batch_id uuid,requested_request jsonb
) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog
AS $$
DECLARE supervisor master_control.task_credential_registration%ROWTYPE;
  run migration.region_talk_post_import_stage_run%ROWTYPE; v_base jsonb; v_items jsonb;
BEGIN
  IF requested_request->>'schema_version'<>'region-talk-stage-supervisor-status-request.v1'
     OR requested_request - ARRAY['schema_version','requested_at']::text[]<>'{}'::jsonb
     OR requested_request->>'requested_at' IS NULL THEN
    RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='stage supervisor status violates fixed contract';
  END IF;
  BEGIN PERFORM (requested_request->>'requested_at')::timestamptz;
  EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='stage supervisor status time is invalid'; END;
  supervisor:=master_control.assert_registered_task_credential(
    'region_talk',requested_supervisor_task_run_id);
  IF EXISTS(SELECT 1 FROM migration.region_talk_stage_worker_generation worker
             WHERE worker.worker_credential_id=supervisor.credential_id) THEN
    RAISE EXCEPTION USING ERRCODE='42501',MESSAGE='worker credential cannot poll supervisor status';
  END IF;
  SELECT * INTO STRICT run FROM migration.region_talk_post_import_stage_run
   WHERE task_run_id=requested_supervisor_task_run_id AND export_batch_id=requested_export_batch_id;
  SELECT coalesce(jsonb_agg(jsonb_build_object('dispatch_id',claim.dispatch_id,
    'work_item_id',claim.work_item_id,'effect_id',claim.effect_id,
    'worker_task_run_id',claim.worker_task_run_id,'stage',claim.stage,
    'input_fingerprint',claim.input_fingerprint,'attempt',claim.attempt,
    'lease_expires_at',claim.lease_expires_at,'worker_binding_sha256',binding.worker_binding_sha256,
    'work_status',work.status,'result_ref',work.result_ref)
    ORDER BY claim.created_at,claim.dispatch_id),'[]'::jsonb) INTO v_items
   FROM migration.region_talk_stage_dispatch_claim claim
   JOIN orchestration.work_item work USING(work_item_id)
   LEFT JOIN LATERAL(SELECT generation.worker_binding_sha256
     FROM migration.region_talk_stage_worker_generation generation
    WHERE generation.dispatch_id=claim.dispatch_id
    ORDER BY generation.worker_generation DESC LIMIT 1) binding ON true
   WHERE claim.supervisor_task_run_id=requested_supervisor_task_run_id
     AND claim.export_batch_id=requested_export_batch_id;
  v_base:=jsonb_build_object('schema_version','region-talk-stage-supervisor-status-receipt.v1',
    'master_instance_id',supervisor.master_instance_id,'epoch',supervisor.epoch,
    'supervisor_task_run_id',requested_supervisor_task_run_id,
    'export_batch_id',requested_export_batch_id,'stage_run_id',run.stage_run_id,
    'status',run.state,'items',v_items,'publication_dispatch',false,'notification_dispatch',false);
  RETURN v_base||jsonb_build_object('receipt_sha256',migration.region_talk_json_sha256(v_base));
END
$$;


REVOKE ALL ON migration.region_talk_stage_worker_generation,
  migration.region_talk_stage_worker_generation_status_v1
  FROM PUBLIC,mdh_mcp_reader,mdh_mcp_editor,mdh_region_talk_pipeline;
REVOKE EXECUTE ON FUNCTION
  migration.capture_region_talk_initial_worker_generation(),
  migration.rotate_region_talk_stage_worker_credential(uuid,uuid,jsonb)
  FROM PUBLIC,mdh_mcp_reader,mdh_mcp_editor,mdh_region_talk_pipeline;
GRANT EXECUTE ON FUNCTION
  migration.rotate_region_talk_stage_worker_credential(uuid,uuid,jsonb)
  TO mdh_region_talk_pipeline;

UPDATE hub.canonical_state SET schema_revision=29,updated_at=clock_timestamp() WHERE singleton=true;
