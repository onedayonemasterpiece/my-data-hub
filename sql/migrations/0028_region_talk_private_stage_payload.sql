-- Region Talk v7: keep canonical stage payloads on the direct master data plane.
-- The supervisor/control path receives identities and hashes only.  A separately
-- registered worker LOGIN fetches the bounded payload from PostgreSQL and submits its
-- result directly; raw lease material and business content never traverse devstand.

CREATE TABLE migration.region_talk_stage_dispatch_claim (
    dispatch_id             uuid PRIMARY KEY,
    claim_request_id        uuid NOT NULL,
    supervisor_task_run_id  uuid NOT NULL,
    supervisor_credential_id uuid NOT NULL
                             REFERENCES master_control.task_credential_registration(credential_id),
    supervisor_generation   bigint NOT NULL CHECK(supervisor_generation>=1),
    export_batch_id         uuid NOT NULL REFERENCES migration.region_talk_direct_snapshot(export_batch_id),
    stage_run_id            uuid NOT NULL REFERENCES migration.region_talk_post_import_stage_run(stage_run_id),
    work_item_id            uuid NOT NULL REFERENCES orchestration.work_item(work_item_id),
    effect_id               uuid NOT NULL UNIQUE,
    worker_task_run_id      uuid NOT NULL UNIQUE,
    master_instance_id      uuid NOT NULL,
    epoch                   bigint NOT NULL CHECK(epoch>=1),
    stage                   text NOT NULL,
    contract_version        text NOT NULL,
    subject_type            text NOT NULL CHECK(subject_type='region_talk.candidate'),
    subject_id              uuid NOT NULL,
    input_fingerprint       text NOT NULL CHECK(input_fingerprint ~ '^[a-f0-9]{64}$'),
    attempt                 integer NOT NULL CHECK(attempt>=1),
    max_attempts            integer NOT NULL CHECK(max_attempts BETWEEN 1 AND 20),
    timeout_seconds         integer NOT NULL CHECK(timeout_seconds BETWEEN 1 AND 10800),
    lease_token             uuid NOT NULL,
    lease_token_sha256      text NOT NULL CHECK(lease_token_sha256 ~ '^[a-f0-9]{64}$'),
    lease_capability_sha256 text NOT NULL CHECK(lease_capability_sha256 ~ '^[a-f0-9]{64}$'),
    lease_expires_at        timestamptz NOT NULL,
    payload                 jsonb NOT NULL CHECK(
                               payload->>'schema_version'='region-talk-stage-work-execution.v1'
                               AND octet_length(payload::text)<=1048576),
    source_claim_sha256     text NOT NULL CHECK(source_claim_sha256 ~ '^[a-f0-9]{64}$'),
    metadata_receipt_sha256 text NOT NULL CHECK(metadata_receipt_sha256 ~ '^[a-f0-9]{64}$'),
    metadata_receipt        jsonb NOT NULL,
    created_at              timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(supervisor_task_run_id,export_batch_id,claim_request_id),
    UNIQUE(supervisor_task_run_id,work_item_id,attempt)
);

CREATE TABLE migration.region_talk_stage_worker_binding (
    dispatch_id             uuid PRIMARY KEY
                             REFERENCES migration.region_talk_stage_dispatch_claim(dispatch_id),
    effect_id               uuid NOT NULL UNIQUE,
    supervisor_task_run_id  uuid NOT NULL,
    supervisor_credential_id uuid NOT NULL,
    supervisor_generation   bigint NOT NULL CHECK(supervisor_generation>=1),
    worker_task_run_id      uuid NOT NULL UNIQUE,
    worker_credential_id    uuid NOT NULL UNIQUE
                             REFERENCES master_control.task_credential_registration(credential_id),
    worker_generation       bigint NOT NULL CHECK(worker_generation>=1),
    master_instance_id      uuid NOT NULL,
    epoch                   bigint NOT NULL CHECK(epoch>=1),
    worker_command_sha256   text NOT NULL CHECK(worker_command_sha256 ~ '^[a-f0-9]{64}$'),
    worker_task_token_sha256 text NOT NULL CHECK(worker_task_token_sha256 ~ '^[a-f0-9]{64}$'),
    claim_receipt_sha256    text NOT NULL CHECK(claim_receipt_sha256 ~ '^[a-f0-9]{64}$'),
    worker_binding_sha256   text NOT NULL UNIQUE CHECK(worker_binding_sha256 ~ '^[a-f0-9]{64}$'),
    binding_receipt         jsonb NOT NULL,
    bound_at                timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK(worker_task_run_id<>supervisor_task_run_id),
    CHECK(worker_credential_id<>supervisor_credential_id)
);

CREATE TRIGGER region_talk_stage_dispatch_claim_append_only
BEFORE UPDATE OR DELETE ON migration.region_talk_stage_dispatch_claim
FOR EACH ROW EXECUTE FUNCTION hub_meta.reject_update_delete();
CREATE TRIGGER region_talk_stage_worker_binding_append_only
BEFORE UPDATE OR DELETE ON migration.region_talk_stage_worker_binding
FOR EACH ROW EXECUTE FUNCTION hub_meta.reject_update_delete();

CREATE FUNCTION migration.claim_region_talk_stage_work_metadata(
    requested_supervisor_task_run_id uuid,requested_export_batch_id uuid,requested_request jsonb
) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog
AS $$
DECLARE
    registration master_control.task_credential_registration%ROWTYPE;
    existing migration.region_talk_stage_dispatch_claim%ROWTYPE;
    v_claim_request_id uuid; v_lease_token uuid; v_source jsonb; v_base jsonb; v_receipt jsonb;
    v_dispatch_id uuid; v_worker_task_run_id uuid; v_lease_hash text; v_capability_hash text;
BEGIN
    IF requested_request->>'schema_version'<>'region-talk-stage-work-metadata-claim.v2'
       OR requested_request - ARRAY['schema_version','claim_request_id','lease_owner','requested_at',
            'publication_dispatch','notification_dispatch']::text[] <> '{}'::jsonb
       OR length(coalesce(requested_request->>'lease_owner','')) NOT BETWEEN 1 AND 200
       OR requested_request->'publication_dispatch' IS DISTINCT FROM 'false'::jsonb
       OR requested_request->'notification_dispatch' IS DISTINCT FROM 'false'::jsonb THEN
        RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='stage metadata claim violates fixed contract';
    END IF;
    BEGIN
        v_claim_request_id:=(requested_request->>'claim_request_id')::uuid;
        PERFORM (requested_request->>'requested_at')::timestamptz;
    EXCEPTION WHEN OTHERS THEN
        RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='stage metadata claim identity is invalid';
    END;
    registration:=master_control.assert_registered_task_credential(
        'region_talk',requested_supervisor_task_run_id);
    IF EXISTS(SELECT 1 FROM migration.region_talk_stage_worker_binding worker
               WHERE worker.worker_credential_id=registration.credential_id) THEN
        RAISE EXCEPTION USING ERRCODE='42501',MESSAGE='worker credential cannot claim supervisor work';
    END IF;
    SELECT * INTO existing FROM migration.region_talk_stage_dispatch_claim claim
     WHERE claim.supervisor_task_run_id=requested_supervisor_task_run_id
       AND claim.export_batch_id=requested_export_batch_id
       AND claim.claim_request_id=v_claim_request_id;
    IF FOUND THEN
        IF existing.supervisor_credential_id<>registration.credential_id
           OR existing.supervisor_generation<>registration.generation
           OR existing.master_instance_id<>registration.master_instance_id
           OR existing.epoch<>registration.epoch THEN
            RAISE EXCEPTION USING ERRCODE='42501',MESSAGE='metadata claim replay crosses supervisor authority';
        END IF;
        RETURN existing.metadata_receipt;
    END IF;

    v_lease_token:=gen_random_uuid();
    v_source:=migration.claim_region_talk_stage_work(
      requested_supervisor_task_run_id,requested_export_batch_id,jsonb_build_object(
        'schema_version','region-talk-stage-work-claim.v1','lease_token',v_lease_token,
        'lease_owner',requested_request->>'lease_owner','requested_at',requested_request->>'requested_at',
        'publication_dispatch',false,'notification_dispatch',false));
    IF v_source->>'status'<>'CLAIMED' THEN
        v_base:=jsonb_build_object('schema_version','region-talk-stage-work-metadata-claim-receipt.v2',
          'status',v_source->>'status','master_instance_id',registration.master_instance_id,
          'epoch',registration.epoch,'supervisor_task_run_id',requested_supervisor_task_run_id,
          'export_batch_id',requested_export_batch_id,'stage_run_id',v_source->'stage_run_id',
          'work_item_id',NULL,'dispatch_id',NULL,'worker_task_run_id',NULL,'effect_id',NULL,
          'publication_dispatch',false,'notification_dispatch',false);
        RETURN v_base||jsonb_build_object('receipt_sha256',migration.region_talk_json_sha256(v_base));
    END IF;
    v_dispatch_id:=migration.region_talk_stage_uuid5('region-talk-stage-dispatch:'||
      requested_supervisor_task_run_id::text||':'||(v_source->>'work_item_id')||':'||
      (v_source->>'attempt')||':'||(v_source->>'input_fingerprint'));
    v_worker_task_run_id:=migration.region_talk_stage_uuid5('region-talk-stage-worker:'||
      requested_supervisor_task_run_id::text||':'||(v_source->>'work_item_id')||':'||
      (v_source->>'attempt'));
    v_lease_hash:=encode(sha256(convert_to(v_lease_token::text,'UTF8')),'hex');
    v_capability_hash:=migration.region_talk_json_sha256(jsonb_build_object(
      'dispatch_id',v_dispatch_id,'effect_id',v_source->>'effect_id','work_item_id',v_source->>'work_item_id',
      'input_fingerprint',v_source->>'input_fingerprint','attempt',(v_source->>'attempt')::integer,
      'lease_token_sha256',v_lease_hash,'master_instance_id',registration.master_instance_id,
      'epoch',registration.epoch));
    v_base:=jsonb_build_object(
      'schema_version','region-talk-stage-work-metadata-claim-receipt.v2','status','CLAIMED',
      'master_instance_id',registration.master_instance_id,'epoch',registration.epoch,
      'supervisor_task_run_id',requested_supervisor_task_run_id,
      'export_batch_id',requested_export_batch_id,'stage_run_id',v_source->'stage_run_id',
      'work_item_id',v_source->'work_item_id','effect_id',v_source->'effect_id',
      'dispatch_id',v_dispatch_id,'worker_task_run_id',v_worker_task_run_id,
      'stage',v_source->>'stage','contract_version',v_source->>'contract_version',
      'subject_type',v_source->>'subject_type','subject_id',v_source->'subject_id',
      'input_fingerprint',v_source->>'input_fingerprint','attempt',(v_source->>'attempt')::integer,
      'max_attempts',(v_source->>'max_attempts')::integer,
      'timeout_seconds',(v_source->>'timeout_seconds')::integer,
      'lease_expires_at',v_source->'lease_expires_at','lease_token_sha256',v_lease_hash,
      'lease_capability_sha256',v_capability_hash,
      'claim_receipt_sha256',v_source->>'receipt_sha256',
      'publication_dispatch',false,'notification_dispatch',false);
    v_receipt:=v_base||jsonb_build_object('receipt_sha256',migration.region_talk_json_sha256(v_base));
    INSERT INTO migration.region_talk_stage_dispatch_claim(
      dispatch_id,claim_request_id,supervisor_task_run_id,supervisor_credential_id,
      supervisor_generation,export_batch_id,stage_run_id,work_item_id,effect_id,worker_task_run_id,
      master_instance_id,epoch,stage,contract_version,subject_type,subject_id,input_fingerprint,
      attempt,max_attempts,timeout_seconds,lease_token,lease_token_sha256,lease_capability_sha256,
      lease_expires_at,payload,source_claim_sha256,metadata_receipt_sha256,metadata_receipt)
    VALUES(v_dispatch_id,v_claim_request_id,requested_supervisor_task_run_id,
      registration.credential_id,registration.generation,requested_export_batch_id,
      (v_source->>'stage_run_id')::uuid,(v_source->>'work_item_id')::uuid,
      (v_source->>'effect_id')::uuid,v_worker_task_run_id,registration.master_instance_id,
      registration.epoch,v_source->>'stage',v_source->>'contract_version',v_source->>'subject_type',
      (v_source->>'subject_id')::uuid,v_source->>'input_fingerprint',(v_source->>'attempt')::integer,
      (v_source->>'max_attempts')::integer,(v_source->>'timeout_seconds')::integer,v_lease_token,
      v_lease_hash,v_capability_hash,(v_source->>'lease_expires_at')::timestamptz,
      v_source->'payload',v_source->>'receipt_sha256',v_receipt->>'receipt_sha256',v_receipt);
    RETURN v_receipt;
END
$$;

CREATE FUNCTION migration.bind_region_talk_stage_worker(
    requested_supervisor_task_run_id uuid,requested_export_batch_id uuid,requested_request jsonb
) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog
AS $$
DECLARE
    supervisor master_control.task_credential_registration%ROWTYPE;
    worker master_control.task_credential_registration%ROWTYPE;
    claim migration.region_talk_stage_dispatch_claim%ROWTYPE;
    existing migration.region_talk_stage_worker_binding%ROWTYPE;
    v_base jsonb; v_receipt jsonb; v_binding_hash text;
BEGIN
    IF requested_request->>'schema_version'<>'region-talk-stage-worker-bind.v1'
       OR requested_request - ARRAY['schema_version','dispatch_id','effect_id','claim_receipt_sha256',
          'worker_task_run_id','worker_credential_id','worker_generation','worker_command_sha256',
          'worker_task_token_sha256','requested_at','publication_dispatch','notification_dispatch']::text[]
          <> '{}'::jsonb
       OR requested_request->>'claim_receipt_sha256' !~ '^[a-f0-9]{64}$'
       OR requested_request->>'worker_command_sha256' !~ '^[a-f0-9]{64}$'
       OR requested_request->>'worker_task_token_sha256' !~ '^[a-f0-9]{64}$'
       OR requested_request->'publication_dispatch' IS DISTINCT FROM 'false'::jsonb
       OR requested_request->'notification_dispatch' IS DISTINCT FROM 'false'::jsonb THEN
        RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='stage worker binding violates fixed contract';
    END IF;
    BEGIN PERFORM (requested_request->>'requested_at')::timestamptz;
    EXCEPTION WHEN OTHERS THEN
      RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='stage worker binding time is invalid'; END;
    supervisor:=master_control.assert_registered_task_credential(
      'region_talk',requested_supervisor_task_run_id);
    SELECT * INTO STRICT claim FROM migration.region_talk_stage_dispatch_claim dispatch
     WHERE dispatch.dispatch_id=(requested_request->>'dispatch_id')::uuid
       AND dispatch.effect_id=(requested_request->>'effect_id')::uuid
       AND dispatch.supervisor_task_run_id=requested_supervisor_task_run_id
       AND dispatch.export_batch_id=requested_export_batch_id FOR UPDATE;
    IF claim.supervisor_credential_id<>supervisor.credential_id
       OR claim.supervisor_generation<>supervisor.generation
       OR claim.master_instance_id<>supervisor.master_instance_id OR claim.epoch<>supervisor.epoch
       OR claim.source_claim_sha256<>requested_request->>'claim_receipt_sha256'
       OR claim.worker_task_run_id<>(requested_request->>'worker_task_run_id')::uuid
       OR claim.lease_expires_at<=clock_timestamp() THEN
        RAISE EXCEPTION USING ERRCODE='42501',MESSAGE='stage worker binding crosses exact supervisor claim';
    END IF;
    SELECT * INTO STRICT worker FROM master_control.task_credential_registration registration
     WHERE registration.credential_id=(requested_request->>'worker_credential_id')::uuid
       AND registration.task_run_id=claim.worker_task_run_id
       AND registration.generation=(requested_request->>'worker_generation')::bigint
       AND registration.worker_kind='region_talk';
    IF worker.credential_id=supervisor.credential_id OR worker.task_run_id=supervisor.task_run_id
       OR worker.master_instance_id<>supervisor.master_instance_id OR worker.epoch<>supervisor.epoch
       OR worker.command_sha256<>requested_request->>'worker_command_sha256'
       OR worker.task_token_sha256<>requested_request->>'worker_task_token_sha256' THEN
        RAISE EXCEPTION USING ERRCODE='42501',MESSAGE='worker credential differs from exact child task';
    END IF;
    v_base:=jsonb_build_object('schema_version','region-talk-stage-worker-bind-receipt.v1',
      'bound',true,'master_instance_id',supervisor.master_instance_id,'epoch',supervisor.epoch,
      'supervisor_task_run_id',requested_supervisor_task_run_id,
      'export_batch_id',requested_export_batch_id,'stage_run_id',claim.stage_run_id,
      'dispatch_id',claim.dispatch_id,'work_item_id',claim.work_item_id,'effect_id',claim.effect_id,
      'worker_task_run_id',worker.task_run_id,'worker_credential_id',worker.credential_id,
      'worker_generation',worker.generation,'lease_capability_sha256',claim.lease_capability_sha256,
      'publication_dispatch',false,'notification_dispatch',false);
    v_binding_hash:=migration.region_talk_json_sha256(v_base);
    v_receipt:=v_base||jsonb_build_object('worker_binding_sha256',v_binding_hash,
      'receipt_sha256',migration.region_talk_json_sha256(
        v_base||jsonb_build_object('worker_binding_sha256',v_binding_hash)));
    INSERT INTO migration.region_talk_stage_worker_binding(
      dispatch_id,effect_id,supervisor_task_run_id,supervisor_credential_id,
      supervisor_generation,worker_task_run_id,worker_credential_id,worker_generation,
      master_instance_id,epoch,worker_command_sha256,worker_task_token_sha256,
      claim_receipt_sha256,worker_binding_sha256,binding_receipt)
    VALUES(claim.dispatch_id,claim.effect_id,claim.supervisor_task_run_id,
      claim.supervisor_credential_id,claim.supervisor_generation,worker.task_run_id,
      worker.credential_id,worker.generation,worker.master_instance_id,worker.epoch,
      worker.command_sha256,worker.task_token_sha256,claim.source_claim_sha256,
      v_binding_hash,v_receipt) ON CONFLICT(dispatch_id) DO NOTHING;
    SELECT * INTO STRICT existing FROM migration.region_talk_stage_worker_binding binding
     WHERE binding.dispatch_id=claim.dispatch_id;
    IF existing.worker_credential_id<>worker.credential_id
       OR existing.worker_task_run_id<>worker.task_run_id
       OR existing.worker_generation<>worker.generation
       OR existing.worker_binding_sha256<>v_binding_hash THEN
      RAISE EXCEPTION USING ERRCODE='23505',MESSAGE='stage worker binding idempotency conflict';
    END IF;
    RETURN existing.binding_receipt;
END
$$;

CREATE FUNCTION migration.fetch_region_talk_stage_work_payload(
    requested_worker_task_run_id uuid,requested_effect_id uuid,requested_request jsonb
) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog
AS $$
DECLARE worker master_control.task_credential_registration%ROWTYPE;
  binding migration.region_talk_stage_worker_binding%ROWTYPE;
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
  SELECT * INTO STRICT binding FROM migration.region_talk_stage_worker_binding exact_binding
   WHERE exact_binding.effect_id=requested_effect_id
     AND exact_binding.dispatch_id=(requested_request->>'dispatch_id')::uuid
     AND exact_binding.worker_task_run_id=requested_worker_task_run_id
     AND exact_binding.worker_credential_id=worker.credential_id
     AND exact_binding.worker_generation=worker.generation
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

CREATE FUNCTION migration.submit_region_talk_stage_worker_result(
    requested_worker_task_run_id uuid,requested_effect_id uuid,requested_result jsonb
) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog
AS $$
DECLARE worker master_control.task_credential_registration%ROWTYPE;
  binding migration.region_talk_stage_worker_binding%ROWTYPE;
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
  SELECT * INTO STRICT binding FROM migration.region_talk_stage_worker_binding exact_binding
   WHERE exact_binding.effect_id=requested_effect_id
     AND exact_binding.dispatch_id=(requested_result->>'dispatch_id')::uuid
     AND exact_binding.worker_task_run_id=requested_worker_task_run_id
     AND exact_binding.worker_credential_id=worker.credential_id
     AND exact_binding.worker_generation=worker.generation
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

CREATE FUNCTION migration.region_talk_stage_supervisor_status(
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
  IF EXISTS(SELECT 1 FROM migration.region_talk_stage_worker_binding worker
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
   LEFT JOIN migration.region_talk_stage_worker_binding binding USING(dispatch_id)
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

REVOKE ALL ON migration.region_talk_stage_dispatch_claim,
  migration.region_talk_stage_worker_binding FROM PUBLIC,mdh_mcp_reader,mdh_mcp_editor,
  mdh_region_talk_pipeline;
REVOKE EXECUTE ON FUNCTION
  migration.claim_region_talk_stage_work(uuid,uuid,jsonb),
  migration.submit_region_talk_stage_result(uuid,uuid,jsonb),
  migration.region_talk_stage_work_status(uuid,uuid,jsonb),
  migration.claim_region_talk_stage_work_metadata(uuid,uuid,jsonb),
  migration.bind_region_talk_stage_worker(uuid,uuid,jsonb),
  migration.fetch_region_talk_stage_work_payload(uuid,uuid,jsonb),
  migration.submit_region_talk_stage_worker_result(uuid,uuid,jsonb),
  migration.region_talk_stage_supervisor_status(uuid,uuid,jsonb)
  FROM PUBLIC,mdh_mcp_reader,mdh_mcp_editor,mdh_region_talk_pipeline;
GRANT EXECUTE ON FUNCTION
  migration.claim_region_talk_stage_work_metadata(uuid,uuid,jsonb),
  migration.bind_region_talk_stage_worker(uuid,uuid,jsonb),
  migration.fetch_region_talk_stage_work_payload(uuid,uuid,jsonb),
  migration.submit_region_talk_stage_worker_result(uuid,uuid,jsonb),
  migration.region_talk_stage_supervisor_status(uuid,uuid,jsonb)
  TO mdh_region_talk_pipeline;

UPDATE hub.canonical_state SET schema_revision=28,updated_at=clock_timestamp() WHERE singleton=true;
