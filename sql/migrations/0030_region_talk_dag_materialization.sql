-- Region Talk v9: materialize the dependency DAG from verified evidence only.
-- Publication and notification remain disabled.  Private work inputs never cross
-- the metadata-only supervisor boundary.

CREATE TABLE migration.region_talk_stage_runtime_pin (
    stage                       text NOT NULL,
    contract_version            text NOT NULL,
    effective_canonical_revision bigint NOT NULL CHECK(effective_canonical_revision>=1),
    pin_generation              bigint NOT NULL CHECK(pin_generation>=1),
    master_instance_id          uuid NOT NULL,
    epoch                       bigint NOT NULL CHECK(epoch>=1),
    model_id                    text NOT NULL CHECK(length(model_id) BETWEEN 1 AND 300),
    model_revision              text NOT NULL CHECK(model_revision ~ '^[a-f0-9]{40}$'),
    encoder_contract            text NOT NULL CHECK(length(encoder_contract) BETWEEN 1 AND 300),
    semantic_bank_version       text,
    semantic_bank_sha256        text CHECK(semantic_bank_sha256 IS NULL OR semantic_bank_sha256 ~ '^[a-f0-9]{64}$'),
    runtime_source_sha256       text NOT NULL CHECK(runtime_source_sha256 ~ '^[a-f0-9]{64}$'),
    asset_manifest_sha256       text NOT NULL CHECK(asset_manifest_sha256 ~ '^[a-f0-9]{64}$'),
    provider_image_identity     text NOT NULL CHECK(
                                  provider_image_identity ~ '^[^@[:space:]]+@sha256:[a-f0-9]{64}$'),
    provider_image_source_commit text NOT NULL CHECK(provider_image_source_commit ~ '^[a-f0-9]{40}$'),
    producer_exact_id           text NOT NULL CHECK(length(producer_exact_id) BETWEEN 1 AND 2000),
    prior_pin_receipt_sha256    text CHECK(
                                  prior_pin_receipt_sha256 IS NULL OR
                                  prior_pin_receipt_sha256 ~ '^[a-f0-9]{64}$'),
    request_sha256              text NOT NULL UNIQUE CHECK(request_sha256 ~ '^[a-f0-9]{64}$'),
    pin_sha256                  text NOT NULL UNIQUE CHECK(pin_sha256 ~ '^[a-f0-9]{64}$'),
    receipt_sha256              text NOT NULL UNIQUE CHECK(receipt_sha256 ~ '^[a-f0-9]{64}$'),
    receipt                     jsonb NOT NULL,
    registered_by               name NOT NULL,
    registered_at               timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(stage,effective_canonical_revision,pin_generation),
    CHECK((pin_generation=1)=(prior_pin_receipt_sha256 IS NULL)),
    CHECK((semantic_bank_version IS NULL)=(semantic_bank_sha256 IS NULL))
);
CREATE TRIGGER region_talk_stage_runtime_pin_append_only
BEFORE UPDATE OR DELETE ON migration.region_talk_stage_runtime_pin
FOR EACH ROW EXECUTE FUNCTION hub_meta.reject_update_delete();

CREATE VIEW migration.region_talk_stage_runtime_pin_current_v1 AS
SELECT pin.*
FROM migration.region_talk_stage_runtime_pin pin
WHERE pin.pin_generation=(
  SELECT max(candidate.pin_generation)
  FROM migration.region_talk_stage_runtime_pin candidate
  WHERE candidate.stage=pin.stage
    AND candidate.effective_canonical_revision=pin.effective_canonical_revision
);

CREATE TABLE migration.region_talk_stage_work_input_v9 (
    work_item_id         uuid PRIMARY KEY REFERENCES orchestration.work_item(work_item_id) ON DELETE RESTRICT,
    task_run_id          uuid NOT NULL,
    export_batch_id      uuid NOT NULL REFERENCES migration.region_talk_direct_snapshot(export_batch_id),
    stage_run_id         uuid NOT NULL REFERENCES migration.region_talk_post_import_stage_run(stage_run_id),
    master_instance_id   uuid NOT NULL,
    epoch                bigint NOT NULL CHECK(epoch>=1),
    stage                text NOT NULL,
    contract_version     text NOT NULL,
    subject_id           uuid NOT NULL,
    candidate_revision   integer NOT NULL CHECK(candidate_revision>=1),
    revision_fingerprint text NOT NULL CHECK(revision_fingerprint ~ '^[a-f0-9]{64}$'),
    input_fingerprint    text NOT NULL CHECK(input_fingerprint ~ '^[a-f0-9]{64}$'),
    input_schema         text NOT NULL,
    input_data           jsonb NOT NULL CHECK(jsonb_typeof(input_data)='object'),
    input_data_sha256    text NOT NULL CHECK(input_data_sha256 ~ '^[a-f0-9]{64}$'),
    upstream_results     jsonb NOT NULL CHECK(jsonb_typeof(upstream_results)='array'),
    upstream_sha256      text NOT NULL CHECK(upstream_sha256 ~ '^[a-f0-9]{64}$'),
    publication_dispatch boolean NOT NULL DEFAULT false CHECK(NOT publication_dispatch),
    notification_dispatch boolean NOT NULL DEFAULT false CHECK(NOT notification_dispatch),
    created_at           timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(stage_run_id,stage,subject_id,candidate_revision,input_fingerprint),
    CHECK(input_data->>'schema_version'=input_schema),
    CHECK(input_data_sha256=migration.region_talk_json_sha256(input_data)),
    CHECK(upstream_sha256=migration.region_talk_json_sha256(upstream_results))
);
CREATE TRIGGER region_talk_stage_work_input_v9_append_only
BEFORE UPDATE OR DELETE ON migration.region_talk_stage_work_input_v9
FOR EACH ROW EXECUTE FUNCTION hub_meta.reject_update_delete();

CREATE FUNCTION migration.register_region_talk_stage_runtime_pin(requested_request jsonb)
RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog
AS $$
DECLARE
  v_state master_control.epoch_state%ROWTYPE; v_current bigint; v_generation bigint;
  v_request_sha text; v_pin_base jsonb; v_pin_sha text; v_base jsonb; v_receipt jsonb;
  v_existing migration.region_talk_stage_runtime_pin%ROWTYPE;
  v_producer text; v_semantic_version text; v_semantic_sha text;
BEGIN
  IF NOT (pg_has_role(session_user,'mdh_owner','member') OR
          pg_has_role(session_user,'mdh_master_controller','member')) THEN
    RAISE EXCEPTION USING ERRCODE='42501',MESSAGE='runtime pin registration requires owner/master controller';
  END IF;
  IF requested_request->>'schema_version'<>'region-talk-stage-runtime-pin.v1'
     OR requested_request - ARRAY['schema_version','stage','contract_version','model_id',
       'model_revision','encoder_contract','semantic_bank_version','semantic_bank_sha256',
       'runtime_source_sha256','asset_manifest_sha256','provider_image_identity',
       'provider_image_source_commit','effective_canonical_revision','master_instance_id','epoch',
       'prior_pin_receipt_sha256','requested_at','publication_dispatch','notification_dispatch']::text[]
       <> '{}'::jsonb
     OR requested_request->>'stage' NOT IN(
       'e5_embedding','bge_m3_embedding','image_scoring','final_verifier','writer')
     OR length(coalesce(requested_request->>'contract_version','')) NOT BETWEEN 1 AND 300
     OR length(coalesce(requested_request->>'model_id','')) NOT BETWEEN 1 AND 300
     OR requested_request->>'model_revision' !~ '^[a-f0-9]{40}$'
     OR length(coalesce(requested_request->>'encoder_contract','')) NOT BETWEEN 1 AND 300
     OR requested_request->>'runtime_source_sha256' !~ '^[a-f0-9]{64}$'
     OR requested_request->>'asset_manifest_sha256' !~ '^[a-f0-9]{64}$'
     OR requested_request->>'provider_image_identity' !~ '^[^@[:space:]]+@sha256:[a-f0-9]{64}$'
     OR requested_request->>'provider_image_source_commit' !~ '^[a-f0-9]{40}$'
     OR requested_request->'publication_dispatch' IS DISTINCT FROM 'false'::jsonb
     OR requested_request->'notification_dispatch' IS DISTINCT FROM 'false'::jsonb THEN
    RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='Region Talk runtime pin violates fixed contract';
  END IF;
  v_semantic_version:=requested_request->>'semantic_bank_version';
  v_semantic_sha:=requested_request->>'semantic_bank_sha256';
  IF requested_request->>'stage' IN('e5_embedding','bge_m3_embedding') THEN
    IF v_semantic_version<>'semantic_bank_v1'
       OR v_semantic_sha IS NULL OR v_semantic_sha !~ '^[a-f0-9]{64}$' THEN
      RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='text runtime pin lacks exact semantic bank';
    END IF;
  ELSIF v_semantic_version IS NOT NULL OR v_semantic_sha IS NOT NULL THEN
    RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='non-text runtime pin cannot name a semantic bank';
  END IF;
  BEGIN
    PERFORM (requested_request->>'requested_at')::timestamptz;
    v_current:=(requested_request->>'effective_canonical_revision')::bigint;
    IF v_current<1 THEN RAISE EXCEPTION 'revision'; END IF;
  EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='runtime pin revision/time is invalid';
  END;
  SELECT * INTO STRICT v_state FROM master_control.epoch_state WHERE singleton=true FOR SHARE;
  IF v_state.gate_state<>'open' OR v_state.lease_until<=clock_timestamp()
     OR requested_request->>'master_instance_id'<>v_state.master_instance_id::text
     OR (requested_request->>'epoch')::bigint<>v_state.current_epoch
     OR v_current<>(SELECT canonical_revision FROM hub.canonical_state WHERE singleton=true) THEN
    RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='runtime pin is outside ACTIVE epoch/revision';
  END IF;
  IF NOT EXISTS(SELECT 1 FROM orchestration.pipeline_stage stage
    JOIN orchestration.pipeline pipeline USING(pipeline_id)
    WHERE pipeline.workload='region-talk' AND pipeline.name='region-talk-main'
      AND pipeline.status='paused' AND stage.stage_key=requested_request->>'stage'
      AND stage.contract->>'name'=requested_request->>'contract_version' AND stage.enabled) THEN
    RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='runtime pin differs from registered paused stage';
  END IF;
  v_request_sha:=migration.region_talk_json_sha256(requested_request-'requested_at');
  SELECT * INTO v_existing FROM migration.region_talk_stage_runtime_pin
   WHERE request_sha256=v_request_sha;
  IF FOUND THEN RETURN v_existing.receipt; END IF;
  PERFORM pg_advisory_xact_lock(hashtextextended(
    'region-talk-pin:'||(requested_request->>'stage')||':'||v_current::text,0));
  SELECT * INTO v_existing FROM migration.region_talk_stage_runtime_pin_current_v1
   WHERE stage=requested_request->>'stage' AND effective_canonical_revision=v_current;
  IF FOUND THEN
    IF requested_request->>'prior_pin_receipt_sha256' IS DISTINCT FROM v_existing.receipt_sha256 THEN
      RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='runtime pin supersession is stale';
    END IF;
    v_generation:=v_existing.pin_generation+1;
  ELSE
    IF requested_request->>'prior_pin_receipt_sha256' IS NOT NULL THEN
      RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='initial runtime pin cannot name a predecessor';
    END IF;
    v_generation:=1;
  END IF;
  v_producer:=(requested_request->>'model_id')||'@'||(requested_request->>'model_revision')||
    '+assets:'||(requested_request->>'asset_manifest_sha256')||
    '+source:'||(requested_request->>'runtime_source_sha256')||
    '+image:'||(requested_request->>'provider_image_identity')||
    '+commit:'||(requested_request->>'provider_image_source_commit');
  v_pin_base:=jsonb_build_object('stage',requested_request->>'stage',
    'contract_version',requested_request->>'contract_version',
    'effective_canonical_revision',v_current,'pin_generation',v_generation,
    'master_instance_id',v_state.master_instance_id,'epoch',v_state.current_epoch,
    'model_id',requested_request->>'model_id','model_revision',requested_request->>'model_revision',
    'encoder_contract',requested_request->>'encoder_contract',
    'semantic_bank_version',to_jsonb(v_semantic_version),
    'semantic_bank_sha256',to_jsonb(v_semantic_sha),
    'runtime_source_sha256',requested_request->>'runtime_source_sha256',
    'asset_manifest_sha256',requested_request->>'asset_manifest_sha256',
    'provider_image_identity',requested_request->>'provider_image_identity',
    'provider_image_source_commit',requested_request->>'provider_image_source_commit',
    'producer_exact_id',v_producer);
  v_pin_sha:=migration.region_talk_json_sha256(v_pin_base);
  v_base:=jsonb_build_object('schema_version','region-talk-stage-runtime-pin-receipt.v1',
    'registered',true)||v_pin_base||jsonb_build_object(
    'prior_pin_receipt_sha256',requested_request->'prior_pin_receipt_sha256',
    'pin_sha256',v_pin_sha,'publication_dispatch',false,'notification_dispatch',false);
  v_receipt:=v_base||jsonb_build_object('receipt_sha256',migration.region_talk_json_sha256(v_base));
  INSERT INTO migration.region_talk_stage_runtime_pin(stage,contract_version,
    effective_canonical_revision,pin_generation,master_instance_id,epoch,model_id,model_revision,
    encoder_contract,semantic_bank_version,semantic_bank_sha256,runtime_source_sha256,
    asset_manifest_sha256,provider_image_identity,provider_image_source_commit,producer_exact_id,
    prior_pin_receipt_sha256,request_sha256,pin_sha256,receipt_sha256,receipt,registered_by)
  VALUES(requested_request->>'stage',requested_request->>'contract_version',v_current,v_generation,
    v_state.master_instance_id,v_state.current_epoch,requested_request->>'model_id',
    requested_request->>'model_revision',requested_request->>'encoder_contract',v_semantic_version,
    v_semantic_sha,requested_request->>'runtime_source_sha256',
    requested_request->>'asset_manifest_sha256',requested_request->>'provider_image_identity',
    requested_request->>'provider_image_source_commit',v_producer,
    requested_request->>'prior_pin_receipt_sha256',v_request_sha,v_pin_sha,
    v_receipt->>'receipt_sha256',v_receipt,session_user);
  RETURN v_receipt;
END
$$;

CREATE FUNCTION migration.region_talk_stage_result_valid_v9(
  requested_stage text,requested_contract text,requested_canonical_revision bigint,
  requested_master_instance_id uuid,requested_epoch bigint,requested_input_data jsonb,
  requested_upstream_results jsonb,requested_result_status text,requested_metadata jsonb,
  requested_result_sha256 text
) RETURNS boolean
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path=pg_catalog
AS $$
DECLARE
  v_metrics jsonb:=requested_metadata->'metrics'; v_pin migration.region_talk_stage_runtime_pin%ROWTYPE;
  v_score record; v_score_count integer:=0; v_expected_evidence text; v_expected_pin jsonb;
  v_allowed text[]; v_failure boolean:=requested_result_status<>'SUCCEEDED';
BEGIN
  IF requested_stage NOT IN('e5_embedding','bge_m3_embedding','vector_fusion',
       'image_scoring','final_verifier','writer')
     OR requested_result_status NOT IN('SUCCEEDED','FAILED_RETRYABLE','FAILED_TERMINAL')
     OR jsonb_typeof(requested_metadata)<>'object'
     OR requested_metadata - ARRAY['schema_version','stage','contract_version','subject_type',
       'subject_id','candidate_revision','revision_fingerprint','input_fingerprint',
       'producer_exact_id','metrics','artifact_sha256']::text[] <> '{}'::jsonb
     OR requested_metadata->>'schema_version'<>'region-talk-stage-result-metadata.v1'
     OR requested_metadata->>'stage'<>requested_stage
     OR requested_metadata->>'contract_version'<>requested_contract
     OR requested_metadata->>'subject_type'<>'region_talk.candidate'
     OR requested_metadata->>'revision_fingerprint' !~ '^[a-f0-9]{64}$'
     OR requested_metadata->>'input_fingerprint' !~ '^[a-f0-9]{64}$'
     OR jsonb_typeof(v_metrics)<>'object'
     OR requested_metadata->>'artifact_sha256' IS NOT NULL
     OR requested_result_sha256<>migration.region_talk_json_sha256(requested_metadata) THEN
    RETURN false;
  END IF;
  IF v_failure THEN
    RETURN requested_metadata->>'producer_exact_id'=
      'my-data-hub:'||requested_stage||'@'||requested_contract
      AND v_metrics - ARRAY['failure_code','failure_message_sha256','retryable']::text[]='{}'::jsonb
      AND length(coalesce(v_metrics->>'failure_code','')) BETWEEN 1 AND 200
      AND v_metrics->>'failure_message_sha256' ~ '^[a-f0-9]{64}$'
      AND jsonb_typeof(v_metrics->'retryable')='boolean'
      AND (v_metrics->>'retryable')::boolean=(requested_result_status='FAILED_RETRYABLE');
  END IF;

  IF requested_stage='vector_fusion' THEN
    RETURN requested_metadata->>'producer_exact_id'=
      'my-data-hub:vector_fusion@region-talk.vector-fusion.v1'
      AND requested_contract='region-talk.vector-fusion.v1'
      AND requested_input_data->>'schema_version'='region-talk-vector-fusion-input.v1'
      AND jsonb_typeof(requested_input_data->'scores')='array'
      AND jsonb_array_length(requested_input_data->'scores')>1
      AND jsonb_typeof(requested_upstream_results)='array'
      AND jsonb_array_length(requested_upstream_results)=2
      AND v_metrics - ARRAY['contract_version','status','reasons','evidence_fingerprint',
        'scores_by_model','fused_scores','positive_class','positive_score','negative_class',
        'negative_score','margin']::text[]='{}'::jsonb
      AND v_metrics->>'contract_version'='region-talk.vector-fusion.v1'
      AND v_metrics->>'status'='fused_e5_bge_m3'
      AND v_metrics->'reasons'='[]'::jsonb
      AND v_metrics->>'evidence_fingerprint' ~ '^[a-f0-9]{64}$'
      AND jsonb_typeof(v_metrics->'scores_by_model')='object'
      AND jsonb_typeof(v_metrics->'fused_scores')='object'
      AND (SELECT count(*) FROM jsonb_object_keys(v_metrics->'fused_scores'))>1
      AND length(coalesce(v_metrics->>'positive_class','')) BETWEEN 1 AND 100
      AND length(coalesce(v_metrics->>'negative_class','')) BETWEEN 1 AND 100
      AND jsonb_typeof(v_metrics->'positive_score')='number'
      AND (v_metrics->>'positive_score')::numeric BETWEEN 0 AND 1
      AND jsonb_typeof(v_metrics->'negative_score')='number'
      AND (v_metrics->>'negative_score')::numeric BETWEEN 0 AND 1
      AND jsonb_typeof(v_metrics->'margin')='number'
      AND (v_metrics->>'margin')::numeric BETWEEN -1 AND 1;
  END IF;

  IF jsonb_typeof(requested_input_data->'runtime_pin')<>'object' THEN RETURN false; END IF;
  v_expected_pin:=requested_input_data->'runtime_pin';
  SELECT * INTO v_pin FROM migration.region_talk_stage_runtime_pin_current_v1 pin
   WHERE pin.stage=requested_stage AND pin.contract_version=requested_contract
     AND pin.effective_canonical_revision=requested_canonical_revision
     AND pin.master_instance_id=requested_master_instance_id AND pin.epoch=requested_epoch
     AND pin.receipt_sha256=v_expected_pin->>'receipt_sha256';
  IF NOT FOUND OR requested_metadata->>'producer_exact_id'<>v_pin.producer_exact_id
     OR v_expected_pin IS DISTINCT FROM v_pin.receipt THEN RETURN false; END IF;

  IF requested_stage IN('e5_embedding','bge_m3_embedding') THEN
    v_allowed:=ARRAY['model_id','model_revision','encoder_contract','text_sha256',
      'semantic_bank_version','semantic_bank_hash','evidence_fingerprint','scores',
      'asset_manifest_sha256','runtime_source_sha256','provider_image_identity',
      'provider_image_source_commit','pin_sha256'];
    IF v_metrics-v_allowed<>'{}'::jsonb
       OR v_metrics->>'model_id'<>v_pin.model_id
       OR v_metrics->>'model_revision'<>v_pin.model_revision
       OR v_metrics->>'encoder_contract'<>v_pin.encoder_contract
       OR v_metrics->>'text_sha256'<>requested_input_data->>'text_sha256'
       OR v_metrics->>'semantic_bank_version'<>v_pin.semantic_bank_version
       OR v_metrics->>'semantic_bank_hash'<>v_pin.semantic_bank_sha256
       OR v_metrics->>'asset_manifest_sha256'<>v_pin.asset_manifest_sha256
       OR v_metrics->>'runtime_source_sha256'<>v_pin.runtime_source_sha256
       OR v_metrics->>'provider_image_identity'<>v_pin.provider_image_identity
       OR v_metrics->>'provider_image_source_commit'<>v_pin.provider_image_source_commit
       OR v_metrics->>'pin_sha256'<>v_pin.pin_sha256
       OR v_metrics->>'evidence_fingerprint' !~ '^[a-f0-9]{64}$'
       OR jsonb_typeof(v_metrics->'scores')<>'object'
       OR (SELECT count(*) FROM jsonb_object_keys(v_metrics->'scores'))<2 THEN RETURN false; END IF;
    FOR v_score IN SELECT key,value FROM jsonb_each(v_metrics->'scores') LOOP
      IF length(v_score.key) NOT BETWEEN 1 AND 100 OR jsonb_typeof(v_score.value)<>'number'
         OR (v_score.value#>>'{}')::numeric NOT BETWEEN 0 AND 1 THEN RETURN false; END IF;
      v_score_count:=v_score_count+1;
    END LOOP;
    v_expected_evidence:=migration.region_talk_json_sha256(jsonb_build_object(
      'contract_version',requested_contract,'model_id',v_pin.model_id,
      'text_hash',v_metrics->>'text_sha256','semantic_bank_version',v_pin.semantic_bank_version,
      'semantic_bank_hash',v_pin.semantic_bank_sha256,'scores',v_metrics->'scores'));
    RETURN v_score_count>=2 AND v_metrics->>'evidence_fingerprint'=v_expected_evidence;
  END IF;

  IF requested_stage='image_scoring' THEN
    v_allowed:=ARRAY['schema_version','decision','actual_image','postcard_score',
      'input_artifact_sha256','model_id','model_revision','encoder_contract',
      'asset_manifest_sha256','runtime_source_sha256','provider_image_identity',
      'provider_image_source_commit','pin_sha256'];
    RETURN v_metrics-v_allowed='{}'::jsonb
      AND v_metrics->>'schema_version'='region-talk.image-diagnostic-result.v1'
      AND requested_input_data->>'schema_version'='region-talk-image-input.v1'
      AND requested_input_data->>'availability'='AVAILABLE'
      AND v_metrics->>'input_artifact_sha256'=requested_input_data->>'artifact_sha256'
      AND v_metrics->>'decision' IN('accept','reject','needs_review')
      AND jsonb_typeof(v_metrics->'actual_image')='boolean'
      AND jsonb_typeof(v_metrics->'postcard_score')='number'
      AND (v_metrics->>'postcard_score')::numeric BETWEEN 0 AND 1
      AND v_metrics->>'model_id'=v_pin.model_id AND v_metrics->>'model_revision'=v_pin.model_revision
      AND v_metrics->>'encoder_contract'=v_pin.encoder_contract
      AND v_metrics->>'asset_manifest_sha256'=v_pin.asset_manifest_sha256
      AND v_metrics->>'runtime_source_sha256'=v_pin.runtime_source_sha256
      AND v_metrics->>'provider_image_identity'=v_pin.provider_image_identity
      AND v_metrics->>'provider_image_source_commit'=v_pin.provider_image_source_commit
      AND v_metrics->>'pin_sha256'=v_pin.pin_sha256;
  ELSIF requested_stage='final_verifier' THEN
    v_allowed:=ARRAY['schema_version','decision','reason_codes','vector_result_sha256',
      'image_result_sha256','model_id','model_revision','encoder_contract',
      'asset_manifest_sha256','runtime_source_sha256','provider_image_identity',
      'provider_image_source_commit','pin_sha256'];
    RETURN v_metrics-v_allowed='{}'::jsonb
      AND v_metrics->>'schema_version'='region-talk.final-verifier-result.v1'
      AND requested_input_data->>'schema_version'='region-talk-final-verifier-input.v1'
      AND v_metrics->>'decision' IN('PASS','REVIEW','REJECT')
      AND jsonb_typeof(v_metrics->'reason_codes')='array'
      AND v_metrics->>'vector_result_sha256'=requested_input_data->>'vector_result_sha256'
      AND v_metrics->>'image_result_sha256'=requested_input_data->>'image_result_sha256'
      AND v_metrics->>'model_id'=v_pin.model_id AND v_metrics->>'model_revision'=v_pin.model_revision
      AND v_metrics->>'encoder_contract'=v_pin.encoder_contract
      AND v_metrics->>'asset_manifest_sha256'=v_pin.asset_manifest_sha256
      AND v_metrics->>'runtime_source_sha256'=v_pin.runtime_source_sha256
      AND v_metrics->>'provider_image_identity'=v_pin.provider_image_identity
      AND v_metrics->>'provider_image_source_commit'=v_pin.provider_image_source_commit
      AND v_metrics->>'pin_sha256'=v_pin.pin_sha256;
  ELSE
    v_allowed:=ARRAY['schema_version','draft_sha256','title_sha256','body_sha256',
      'character_count','final_result_sha256','model_id','model_revision','encoder_contract',
      'asset_manifest_sha256','runtime_source_sha256','provider_image_identity',
      'provider_image_source_commit','pin_sha256'];
    RETURN v_metrics-v_allowed='{}'::jsonb
      AND v_metrics->>'schema_version'='region-talk.writer-result.v1'
      AND requested_input_data->>'schema_version'='region-talk-writer-input.v1'
      AND v_metrics->>'draft_sha256' ~ '^[a-f0-9]{64}$'
      AND v_metrics->>'title_sha256' ~ '^[a-f0-9]{64}$'
      AND v_metrics->>'body_sha256' ~ '^[a-f0-9]{64}$'
      AND jsonb_typeof(v_metrics->'character_count')='number'
      AND (v_metrics->>'character_count')::integer BETWEEN 1 AND 200000
      AND v_metrics->>'final_result_sha256'=requested_input_data->>'final_result_sha256'
      AND v_metrics->>'model_id'=v_pin.model_id AND v_metrics->>'model_revision'=v_pin.model_revision
      AND v_metrics->>'encoder_contract'=v_pin.encoder_contract
      AND v_metrics->>'asset_manifest_sha256'=v_pin.asset_manifest_sha256
      AND v_metrics->>'runtime_source_sha256'=v_pin.runtime_source_sha256
      AND v_metrics->>'provider_image_identity'=v_pin.provider_image_identity
      AND v_metrics->>'provider_image_source_commit'=v_pin.provider_image_source_commit
      AND v_metrics->>'pin_sha256'=v_pin.pin_sha256;
  END IF;
END
$$;

CREATE FUNCTION migration.ensure_region_talk_stage_work_v9(
  requested_task_run_id uuid,requested_export_batch_id uuid,requested_stage_run_id uuid,
  requested_master_instance_id uuid,requested_epoch bigint,requested_candidate_id uuid,
  requested_candidate_revision integer,requested_revision_fingerprint text,
  requested_stage text,requested_contract text,requested_input_data jsonb,
  requested_upstream_results jsonb
) RETURNS text
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog
AS $$
DECLARE
  v_pipeline_id uuid; v_stage orchestration.pipeline_stage%ROWTYPE; v_project_id uuid;
  v_fingerprint text; v_work_id uuid; v_prior orchestration.work_item%ROWTYPE;
  v_prior_input migration.region_talk_stage_work_input_v9%ROWTYPE;
BEGIN
  IF requested_input_data->>'schema_version' IS NULL
     OR jsonb_typeof(requested_upstream_results)<>'array'
     OR requested_revision_fingerprint !~ '^[a-f0-9]{64}$' THEN
    RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='stage work input is invalid';
  END IF;
  SELECT pipeline_id INTO STRICT v_pipeline_id FROM orchestration.pipeline
   WHERE workload='region-talk' AND name='region-talk-main' AND version='1.0.0' AND status='paused';
  SELECT * INTO STRICT v_stage FROM orchestration.pipeline_stage
   WHERE pipeline_id=v_pipeline_id AND stage_key=requested_stage AND stage_version='v1' AND enabled
     AND contract->>'name'=requested_contract;
  SELECT project_id INTO STRICT v_project_id FROM hub.project WHERE slug::text='region-talk';
  v_fingerprint:=CASE WHEN requested_stage IN('e5_embedding','bge_m3_embedding') THEN
    encode(sha256(convert_to(requested_revision_fingerprint||chr(31)||requested_stage,'UTF8')),'hex')
  ELSE migration.region_talk_json_sha256(jsonb_build_object(
    'stage',requested_stage,'contract_version',requested_contract,
    'revision_fingerprint',requested_revision_fingerprint,'input_data',requested_input_data,
    'upstream_results',requested_upstream_results)) END;
  v_work_id:=migration.region_talk_stage_uuid5('region-talk-work:'||requested_stage_run_id::text||':'||
    requested_candidate_id::text||':'||requested_candidate_revision::text||':'||requested_stage||':'||
    v_fingerprint);
  INSERT INTO orchestration.work_item(work_item_id,pipeline_id,stage_id,project_id,subject_type,
    subject_id,dedupe_key,input_fingerprint,priority,payload,status,attempt_count,available_at)
  VALUES(v_work_id,v_pipeline_id,v_stage.stage_id,v_project_id,'region_talk.candidate',
    requested_candidate_id,'post-import:'||requested_stage_run_id::text||':'||v_work_id::text,
    v_fingerprint,v_stage.priority,jsonb_build_object(
      'schema_version','region-talk-stage-work-payload.v1','stage_run_id',requested_stage_run_id,
      'candidate_revision',requested_candidate_revision,
      'revision_fingerprint',requested_revision_fingerprint,
      'reason','dependency-ready exact v9 input','publication_dispatch',false,
      'notification_dispatch',false),'pending',0,clock_timestamp())
  ON CONFLICT(work_item_id) DO NOTHING;
  SELECT * INTO STRICT v_prior FROM orchestration.work_item WHERE work_item_id=v_work_id;
  IF v_prior.pipeline_id<>v_pipeline_id OR v_prior.stage_id<>v_stage.stage_id
     OR v_prior.subject_type<>'region_talk.candidate' OR v_prior.subject_id<>requested_candidate_id
     OR v_prior.input_fingerprint<>v_fingerprint
     OR v_prior.payload->>'stage_run_id'<>requested_stage_run_id::text
     OR v_prior.payload->>'candidate_revision'<>requested_candidate_revision::text
     OR v_prior.payload->>'revision_fingerprint'<>requested_revision_fingerprint
     OR v_prior.payload->'publication_dispatch'<>'false'::jsonb
     OR v_prior.payload->'notification_dispatch'<>'false'::jsonb THEN
    RAISE EXCEPTION USING ERRCODE='23505',MESSAGE='stage work v9 idempotency conflict';
  END IF;
  INSERT INTO migration.region_talk_stage_work_input_v9(work_item_id,task_run_id,export_batch_id,
    stage_run_id,master_instance_id,epoch,stage,contract_version,subject_id,candidate_revision,
    revision_fingerprint,input_fingerprint,input_schema,input_data,input_data_sha256,
    upstream_results,upstream_sha256)
  VALUES(v_work_id,requested_task_run_id,requested_export_batch_id,requested_stage_run_id,
    requested_master_instance_id,requested_epoch,requested_stage,requested_contract,
    requested_candidate_id,requested_candidate_revision,requested_revision_fingerprint,v_fingerprint,
    requested_input_data->>'schema_version',requested_input_data,
    migration.region_talk_json_sha256(requested_input_data),requested_upstream_results,
    migration.region_talk_json_sha256(requested_upstream_results))
  ON CONFLICT(work_item_id) DO NOTHING;
  SELECT * INTO STRICT v_prior_input FROM migration.region_talk_stage_work_input_v9
   WHERE work_item_id=v_work_id;
  IF v_prior_input.task_run_id<>requested_task_run_id
     OR v_prior_input.export_batch_id<>requested_export_batch_id
     OR v_prior_input.stage_run_id<>requested_stage_run_id
     OR v_prior_input.master_instance_id<>requested_master_instance_id
     OR v_prior_input.epoch<>requested_epoch OR v_prior_input.stage<>requested_stage
     OR v_prior_input.contract_version<>requested_contract
     OR v_prior_input.input_data IS DISTINCT FROM requested_input_data
     OR v_prior_input.upstream_results IS DISTINCT FROM requested_upstream_results THEN
    RAISE EXCEPTION USING ERRCODE='23505',MESSAGE='stage private input idempotency conflict';
  END IF;
  RETURN v_fingerprint;
END
$$;

ALTER FUNCTION migration.execute_region_talk_post_import_stages(uuid,uuid,jsonb)
  RENAME TO execute_region_talk_post_import_stages_v8_unmaterialized;

CREATE FUNCTION migration.execute_region_talk_post_import_stages(
  requested_task_run_id uuid,requested_export_batch_id uuid,requested_request jsonb
) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog
AS $$
DECLARE
  v_response jsonb; v_run migration.region_talk_post_import_stage_run%ROWTYPE;
  v_registration master_control.task_credential_registration%ROWTYPE;
  v_candidate jsonb; v_candidates jsonb:='[]'::jsonb; v_evidence jsonb;
  v_content hub.content_item%ROWTYPE; v_revision region_talk.candidate_revision%ROWTYPE;
  v_pin migration.region_talk_stage_runtime_pin%ROWTYPE; v_input jsonb; v_upstream jsonb;
  v_e5 migration.region_talk_stage_worker_result%ROWTYPE;
  v_bge migration.region_talk_stage_worker_result%ROWTYPE;
  v_vector migration.region_talk_stage_worker_result%ROWTYPE;
  v_image migration.region_talk_stage_worker_result%ROWTYPE;
  v_final migration.region_talk_stage_worker_result%ROWTYPE;
  v_writer migration.region_talk_stage_worker_result%ROWTYPE;
  v_e5_fp text; v_bge_fp text; v_vector_fp text; v_image_fp text; v_final_fp text; v_writer_fp text;
  v_manifest jsonb; v_asset hub.content_asset%ROWTYPE; v_stage text; v_fp text;
  v_evidence_work_item_id uuid; v_evidence_attempt integer;
BEGIN
  v_registration:=master_control.assert_registered_task_credential('region_talk',requested_task_run_id);
  v_response:=migration.execute_region_talk_post_import_stages_v8_unmaterialized(
    requested_task_run_id,requested_export_batch_id,requested_request);
  IF requested_request->>'operation'<>'prepare' THEN RETURN v_response; END IF;
  SELECT * INTO STRICT v_run FROM migration.region_talk_post_import_stage_run
   WHERE task_run_id=requested_task_run_id AND export_batch_id=requested_export_batch_id FOR UPDATE;
  FOR v_candidate IN SELECT value FROM jsonb_array_elements(v_response->'candidates') LOOP
    SELECT content.* INTO STRICT v_content FROM hub.content_item content
     WHERE content.content_id=(v_candidate->>'content_id')::uuid
       AND content.metadata->>'region_talk_snapshot_id'=requested_export_batch_id::text;
    SELECT revision.* INTO STRICT v_revision FROM region_talk.candidate_revision revision
     WHERE revision.candidate_id=(v_candidate->>'candidate_id')::uuid
       AND revision.revision=(v_candidate->>'candidate_revision')::integer
       AND revision.revision_fingerprint=v_candidate->>'revision_fingerprint';
    v_evidence:=v_candidate->'evidence';

    SELECT * INTO v_pin FROM migration.region_talk_stage_runtime_pin_current_v1 pin
     WHERE pin.stage='e5_embedding' AND pin.contract_version='e5_semantic_bank_scores_v1'
       AND pin.effective_canonical_revision=v_run.canonical_revision
       AND pin.master_instance_id=v_registration.master_instance_id AND pin.epoch=v_registration.epoch;
    IF FOUND THEN
      v_input:=jsonb_build_object('schema_version','region-talk-stage-text-input.v1',
        'text',left(concat_ws(E'\n\n',v_content.title,v_content.summary,v_content.body_excerpt),262144),
        'text_sha256',encode(sha256(convert_to(left(concat_ws(E'\n\n',v_content.title,
          v_content.summary,v_content.body_excerpt),262144),'UTF8')),'hex'),
        'topics',v_candidate->'topics','runtime_pin',v_pin.receipt);
      v_e5_fp:=migration.ensure_region_talk_stage_work_v9(requested_task_run_id,
        requested_export_batch_id,v_run.stage_run_id,v_registration.master_instance_id,
        v_registration.epoch,(v_candidate->>'candidate_id')::uuid,
        (v_candidate->>'candidate_revision')::integer,v_candidate->>'revision_fingerprint',
        'e5_embedding','e5_semantic_bank_scores_v1',v_input,'[]'::jsonb);
    ELSE v_e5_fp:=v_evidence->'e5_embedding'->>'input_fingerprint'; END IF;

    SELECT * INTO v_pin FROM migration.region_talk_stage_runtime_pin_current_v1 pin
     WHERE pin.stage='bge_m3_embedding' AND pin.contract_version='bge_m3_flagembedding_dense_v1'
       AND pin.effective_canonical_revision=v_run.canonical_revision
       AND pin.master_instance_id=v_registration.master_instance_id AND pin.epoch=v_registration.epoch;
    IF FOUND THEN
      v_input:=jsonb_build_object('schema_version','region-talk-stage-text-input.v1',
        'text',left(concat_ws(E'\n\n',v_content.title,v_content.summary,v_content.body_excerpt),262144),
        'text_sha256',encode(sha256(convert_to(left(concat_ws(E'\n\n',v_content.title,
          v_content.summary,v_content.body_excerpt),262144),'UTF8')),'hex'),
        'topics',v_candidate->'topics','runtime_pin',v_pin.receipt);
      v_bge_fp:=migration.ensure_region_talk_stage_work_v9(requested_task_run_id,
        requested_export_batch_id,v_run.stage_run_id,v_registration.master_instance_id,
        v_registration.epoch,(v_candidate->>'candidate_id')::uuid,
        (v_candidate->>'candidate_revision')::integer,v_candidate->>'revision_fingerprint',
        'bge_m3_embedding','bge_m3_flagembedding_dense_v1',v_input,'[]'::jsonb);
    ELSE v_bge_fp:=v_evidence->'bge_m3_embedding'->>'input_fingerprint'; END IF;

    SELECT landed.* INTO v_e5 FROM migration.region_talk_stage_worker_result landed
      JOIN migration.region_talk_stage_work_input_v9 input USING(work_item_id)
     WHERE landed.stage_run_id=v_run.stage_run_id AND landed.stage='e5_embedding'
       AND landed.subject_id=(v_candidate->>'candidate_id')::uuid
       AND landed.input_fingerprint=v_e5_fp AND landed.result_status='SUCCEEDED'
       AND migration.region_talk_stage_result_valid_v9(landed.stage,landed.contract_version,
         v_run.canonical_revision,landed.master_instance_id,landed.epoch,input.input_data,
         input.upstream_results,landed.result_status,landed.result_metadata,landed.result_sha256)
     ORDER BY landed.attempt DESC LIMIT 1;
    SELECT landed.* INTO v_bge FROM migration.region_talk_stage_worker_result landed
      JOIN migration.region_talk_stage_work_input_v9 input USING(work_item_id)
     WHERE landed.stage_run_id=v_run.stage_run_id AND landed.stage='bge_m3_embedding'
       AND landed.subject_id=(v_candidate->>'candidate_id')::uuid
       AND landed.input_fingerprint=v_bge_fp AND landed.result_status='SUCCEEDED'
       AND migration.region_talk_stage_result_valid_v9(landed.stage,landed.contract_version,
         v_run.canonical_revision,landed.master_instance_id,landed.epoch,input.input_data,
         input.upstream_results,landed.result_status,landed.result_metadata,landed.result_sha256)
     ORDER BY landed.attempt DESC LIMIT 1;
    IF v_e5.work_item_id IS NOT NULL AND v_bge.work_item_id IS NOT NULL THEN
      v_upstream:=jsonb_build_array(jsonb_build_object('stage',v_e5.stage,
        'contract_version',v_e5.contract_version,'input_fingerprint',v_e5.input_fingerprint,
        'result_sha256',v_e5.result_sha256,'result_metadata',v_e5.result_metadata),
        jsonb_build_object('stage',v_bge.stage,'contract_version',v_bge.contract_version,
        'input_fingerprint',v_bge.input_fingerprint,'result_sha256',v_bge.result_sha256,
        'result_metadata',v_bge.result_metadata));
      SELECT jsonb_build_object('schema_version','region-talk-vector-fusion-input.v1',
        'scores',jsonb_agg(score ORDER BY score->>'stage',score->>'label')) INTO v_input FROM (
        SELECT jsonb_build_object('stage','e5_embedding','label',value.key,
          'value',(value.value#>>'{}')::numeric,'result_sha256',v_e5.result_sha256) score
          FROM jsonb_each(v_e5.result_metadata->'metrics'->'scores') value
        UNION ALL
        SELECT jsonb_build_object('stage','bge_m3_embedding','label',value.key,
          'value',(value.value#>>'{}')::numeric,'result_sha256',v_bge.result_sha256)
          FROM jsonb_each(v_bge.result_metadata->'metrics'->'scores') value) scores;
      v_vector_fp:=migration.ensure_region_talk_stage_work_v9(requested_task_run_id,
        requested_export_batch_id,v_run.stage_run_id,v_registration.master_instance_id,
        v_registration.epoch,(v_candidate->>'candidate_id')::uuid,
        (v_candidate->>'candidate_revision')::integer,v_candidate->>'revision_fingerprint',
        'vector_fusion','region-talk.vector-fusion.v1',v_input,v_upstream);
    ELSE v_vector_fp:=v_evidence->'vector_fusion'->>'input_fingerprint'; END IF;

    SELECT landed.* INTO v_vector FROM migration.region_talk_stage_worker_result landed
      JOIN migration.region_talk_stage_work_input_v9 input USING(work_item_id)
     WHERE landed.stage_run_id=v_run.stage_run_id AND landed.stage='vector_fusion'
       AND landed.subject_id=(v_candidate->>'candidate_id')::uuid
       AND landed.input_fingerprint=v_vector_fp AND landed.result_status='SUCCEEDED'
       AND migration.region_talk_stage_result_valid_v9(landed.stage,landed.contract_version,
         v_run.canonical_revision,landed.master_instance_id,landed.epoch,input.input_data,
         input.upstream_results,landed.result_status,landed.result_metadata,landed.result_sha256)
     ORDER BY landed.attempt DESC LIMIT 1;

    SELECT asset.* INTO v_asset FROM hub.content_asset asset
     WHERE asset.content_id=v_content.content_id AND asset.asset_type IN('image','thumbnail')
       AND asset.status='available' AND asset.sha256 ~ '^[a-f0-9]{64}$' AND asset.byte_size>0
       AND asset.metadata->'artifact_manifest'->>'schema_version'=
         'region-talk-media-artifact-manifest.v1'
       AND asset.metadata->'artifact_manifest'->>'candidate_revision'=
         (v_candidate->>'candidate_revision')
       AND asset.metadata->'artifact_manifest'->>'normalized_source_url'=asset.normalized_url
       AND asset.metadata->'artifact_manifest'->>'artifact_sha256'=asset.sha256
       AND asset.metadata->'artifact_manifest'->>'byte_size'=asset.byte_size::text
       AND asset.metadata->'artifact_manifest'->>'content_type'=asset.mime_type
       AND length(coalesce(asset.metadata->'artifact_manifest'->>'source_media_id','')) BETWEEN 1 AND 500
       AND length(coalesce(asset.metadata->'artifact_manifest'->>'object_ref','')) BETWEEN 1 AND 2000
       AND asset.metadata->'artifact_manifest'->>'acquisition_receipt_sha256' ~ '^[a-f0-9]{64}$'
       AND asset.metadata->'artifact_manifest'->'task_readable'='true'::jsonb
       AND asset.metadata->'artifact_manifest'->'publication_dispatch'='false'::jsonb
       AND asset.metadata->'artifact_manifest'->'notification_dispatch'='false'::jsonb
     ORDER BY asset.position,asset.asset_id LIMIT 1;
    v_manifest:=v_asset.metadata->'artifact_manifest';
    IF v_vector.work_item_id IS NOT NULL THEN
      SELECT * INTO v_pin FROM migration.region_talk_stage_runtime_pin_current_v1 pin
       WHERE pin.stage='image_scoring' AND pin.contract_version='region-talk.image-diagnostic.v1'
         AND pin.effective_canonical_revision=v_run.canonical_revision
         AND pin.master_instance_id=v_registration.master_instance_id AND pin.epoch=v_registration.epoch;
      IF FOUND AND v_asset.asset_id IS NOT NULL THEN
        v_upstream:=jsonb_build_array(jsonb_build_object('stage',v_vector.stage,
          'contract_version',v_vector.contract_version,'input_fingerprint',v_vector.input_fingerprint,
          'result_sha256',v_vector.result_sha256,'result_metadata',v_vector.result_metadata));
        v_input:=jsonb_build_object('schema_version','region-talk-image-input.v1',
          'availability','AVAILABLE','asset_id',v_asset.asset_id,
          'source_media_id',v_manifest->>'source_media_id',
          'normalized_source_url',v_asset.normalized_url,'object_ref',v_manifest->>'object_ref',
          'artifact_sha256',v_asset.sha256,'byte_size',v_asset.byte_size,
          'content_type',v_asset.mime_type,
          'acquisition_receipt_sha256',v_manifest->>'acquisition_receipt_sha256',
          'candidate_revision',(v_candidate->>'candidate_revision')::integer,'runtime_pin',v_pin.receipt);
        v_image_fp:=migration.ensure_region_talk_stage_work_v9(requested_task_run_id,
          requested_export_batch_id,v_run.stage_run_id,v_registration.master_instance_id,
          v_registration.epoch,(v_candidate->>'candidate_id')::uuid,
          (v_candidate->>'candidate_revision')::integer,v_candidate->>'revision_fingerprint',
          'image_scoring','region-talk.image-diagnostic.v1',v_input,v_upstream);
      ELSE v_image_fp:=v_evidence->'image_scoring'->>'input_fingerprint'; END IF;
    ELSE v_image_fp:=v_evidence->'image_scoring'->>'input_fingerprint'; END IF;

    SELECT landed.* INTO v_image FROM migration.region_talk_stage_worker_result landed
      JOIN migration.region_talk_stage_work_input_v9 input USING(work_item_id)
     WHERE landed.stage_run_id=v_run.stage_run_id AND landed.stage='image_scoring'
       AND landed.subject_id=(v_candidate->>'candidate_id')::uuid
       AND landed.input_fingerprint=v_image_fp AND landed.result_status='SUCCEEDED'
       AND migration.region_talk_stage_result_valid_v9(landed.stage,landed.contract_version,
         v_run.canonical_revision,landed.master_instance_id,landed.epoch,input.input_data,
         input.upstream_results,landed.result_status,landed.result_metadata,landed.result_sha256)
     ORDER BY landed.attempt DESC LIMIT 1;
    IF v_vector.work_item_id IS NOT NULL AND v_image.work_item_id IS NOT NULL THEN
      SELECT * INTO v_pin FROM migration.region_talk_stage_runtime_pin_current_v1 pin
       WHERE pin.stage='final_verifier' AND pin.contract_version='region-talk.final-verifier.v1'
         AND pin.effective_canonical_revision=v_run.canonical_revision
         AND pin.master_instance_id=v_registration.master_instance_id AND pin.epoch=v_registration.epoch;
      IF FOUND THEN
        v_upstream:=jsonb_build_array(jsonb_build_object('stage',v_vector.stage,
          'contract_version',v_vector.contract_version,'input_fingerprint',v_vector.input_fingerprint,
          'result_sha256',v_vector.result_sha256,'result_metadata',v_vector.result_metadata),
          jsonb_build_object('stage',v_image.stage,'contract_version',v_image.contract_version,
          'input_fingerprint',v_image.input_fingerprint,'result_sha256',v_image.result_sha256,
          'result_metadata',v_image.result_metadata));
        v_input:=jsonb_build_object('schema_version','region-talk-final-verifier-input.v1',
          'candidate_revision',(v_candidate->>'candidate_revision')::integer,
          'revision_fingerprint',v_candidate->>'revision_fingerprint',
          'content_pack',jsonb_build_object('content_id',v_content.content_id,
            'canonical_source_key',v_candidate->>'canonical_source_key',
            'title_sha256',encode(sha256(convert_to(coalesce(v_content.title,''),'UTF8')),'hex'),
            'summary_sha256',encode(sha256(convert_to(coalesce(v_content.summary,''),'UTF8')),'hex')),
          'vector_result_sha256',v_vector.result_sha256,
          'image_result_sha256',v_image.result_sha256,'runtime_pin',v_pin.receipt);
        v_final_fp:=migration.ensure_region_talk_stage_work_v9(requested_task_run_id,
          requested_export_batch_id,v_run.stage_run_id,v_registration.master_instance_id,
          v_registration.epoch,(v_candidate->>'candidate_id')::uuid,
          (v_candidate->>'candidate_revision')::integer,v_candidate->>'revision_fingerprint',
          'final_verifier','region-talk.final-verifier.v1',v_input,v_upstream);
      ELSE v_final_fp:=v_evidence->'final_verifier'->>'input_fingerprint'; END IF;
    ELSE v_final_fp:=v_evidence->'final_verifier'->>'input_fingerprint'; END IF;

    SELECT landed.* INTO v_final FROM migration.region_talk_stage_worker_result landed
      JOIN migration.region_talk_stage_work_input_v9 input USING(work_item_id)
     WHERE landed.stage_run_id=v_run.stage_run_id AND landed.stage='final_verifier'
       AND landed.subject_id=(v_candidate->>'candidate_id')::uuid
       AND landed.input_fingerprint=v_final_fp AND landed.result_status='SUCCEEDED'
       AND migration.region_talk_stage_result_valid_v9(landed.stage,landed.contract_version,
         v_run.canonical_revision,landed.master_instance_id,landed.epoch,input.input_data,
         input.upstream_results,landed.result_status,landed.result_metadata,landed.result_sha256)
     ORDER BY landed.attempt DESC LIMIT 1;
    IF v_final.work_item_id IS NOT NULL THEN
      SELECT * INTO v_pin FROM migration.region_talk_stage_runtime_pin_current_v1 pin
       WHERE pin.stage='writer' AND pin.contract_version='region-talk.writer.v1'
         AND pin.effective_canonical_revision=v_run.canonical_revision
         AND pin.master_instance_id=v_registration.master_instance_id AND pin.epoch=v_registration.epoch;
      IF FOUND THEN
        v_upstream:=jsonb_build_array(jsonb_build_object('stage',v_final.stage,
          'contract_version',v_final.contract_version,'input_fingerprint',v_final.input_fingerprint,
          'result_sha256',v_final.result_sha256,'result_metadata',v_final.result_metadata));
        v_input:=jsonb_build_object('schema_version','region-talk-writer-input.v1',
          'candidate_revision',(v_candidate->>'candidate_revision')::integer,
          'revision_fingerprint',v_candidate->>'revision_fingerprint',
          'content_pack',jsonb_build_object('content_id',v_content.content_id,
            'canonical_source_key',v_candidate->>'canonical_source_key',
            'text_payload_sha256',migration.region_talk_json_sha256(v_revision.text_payload),
            'ordered_media_sha256',migration.region_talk_json_sha256(v_revision.ordered_media)),
          'final_result_sha256',v_final.result_sha256,'runtime_pin',v_pin.receipt);
        v_writer_fp:=migration.ensure_region_talk_stage_work_v9(requested_task_run_id,
          requested_export_batch_id,v_run.stage_run_id,v_registration.master_instance_id,
          v_registration.epoch,(v_candidate->>'candidate_id')::uuid,
          (v_candidate->>'candidate_revision')::integer,v_candidate->>'revision_fingerprint',
          'writer','region-talk.writer.v1',v_input,v_upstream);
      ELSE v_writer_fp:=v_evidence->'writer'->>'input_fingerprint'; END IF;
    ELSE v_writer_fp:=v_evidence->'writer'->>'input_fingerprint'; END IF;

    SELECT landed.* INTO v_writer FROM migration.region_talk_stage_worker_result landed
      JOIN migration.region_talk_stage_work_input_v9 input USING(work_item_id)
     WHERE landed.stage_run_id=v_run.stage_run_id AND landed.stage='writer'
       AND landed.subject_id=(v_candidate->>'candidate_id')::uuid
       AND landed.input_fingerprint=v_writer_fp AND landed.result_status='SUCCEEDED'
       AND migration.region_talk_stage_result_valid_v9(landed.stage,landed.contract_version,
         v_run.canonical_revision,landed.master_instance_id,landed.epoch,input.input_data,
         input.upstream_results,landed.result_status,landed.result_metadata,landed.result_sha256)
     ORDER BY landed.attempt DESC LIMIT 1;

    FOR v_stage,v_fp,v_evidence_work_item_id,v_evidence_attempt IN SELECT * FROM (VALUES
      ('e5_embedding',v_e5_fp,v_e5.work_item_id,v_e5.attempt),
      ('bge_m3_embedding',v_bge_fp,v_bge.work_item_id,v_bge.attempt),
      ('vector_fusion',v_vector_fp,v_vector.work_item_id,v_vector.attempt),
      ('image_scoring',v_image_fp,v_image.work_item_id,v_image.attempt),
      ('final_verifier',v_final_fp,v_final.work_item_id,v_final.attempt),
      ('writer',v_writer_fp,v_writer.work_item_id,v_writer.attempt)
    ) AS evidence(stage,input_fingerprint,work_item_id,attempt) LOOP
      v_evidence:=jsonb_set(v_evidence,ARRAY[v_stage],jsonb_build_object(
        'status',CASE WHEN v_evidence_work_item_id IS NOT NULL THEN 'CURRENT' ELSE 'MISSING' END,
        'input_fingerprint',v_fp,
        'attempt_count',coalesce(v_evidence_attempt,0)));
    END LOOP;
    v_candidates:=v_candidates||jsonb_build_array(jsonb_set(v_candidate,'{evidence}',v_evidence));
  END LOOP;
  v_response:=jsonb_set(v_response,'{candidates}',v_candidates)-'preparation_sha256';
  v_response:=v_response||jsonb_build_object('preparation_sha256',
    migration.region_talk_json_sha256(v_response));
  UPDATE migration.region_talk_post_import_stage_run SET preparation=v_response,
    preparation_sha256=v_response->>'preparation_sha256',state='PREPARED',
    commit_request_sha256=NULL,final_receipt_sha256=NULL,final_receipt=NULL,completed_at=NULL
   WHERE stage_run_id=v_run.stage_run_id AND preparation IS DISTINCT FROM v_response;
  RETURN v_response;
END
$$;

-- The v8 claimer remains the lease state machine.  This wrapper fences legacy
-- untyped rows, then swaps in the immutable task-private v9 input before the
-- metadata-only v2 wrapper persists a dispatch claim.
ALTER FUNCTION migration.claim_region_talk_stage_work(uuid,uuid,jsonb)
  RENAME TO claim_region_talk_stage_work_v8_untyped;

CREATE FUNCTION migration.claim_region_talk_stage_work(
  requested_task_run_id uuid,requested_export_batch_id uuid,requested_request jsonb
) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog
AS $$
DECLARE
  v_run migration.region_talk_post_import_stage_run%ROWTYPE; v_result jsonb;
  v_input migration.region_talk_stage_work_input_v9%ROWTYPE; v_payload jsonb; v_base jsonb;
BEGIN
  PERFORM master_control.assert_registered_task_credential('region_talk',requested_task_run_id);
  SELECT * INTO STRICT v_run FROM migration.region_talk_post_import_stage_run
   WHERE task_run_id=requested_task_run_id AND export_batch_id=requested_export_batch_id;
  UPDATE orchestration.work_item stale SET status='cancelled',
    last_error=jsonb_build_object('schema_version','region-talk-stage-failure.v1',
      'reason','untyped pre-v9 work fenced by exact DAG materialization')
   WHERE stale.payload->>'stage_run_id'=v_run.stage_run_id::text
     AND stale.status IN('pending','failed_retryable')
     AND NOT EXISTS(SELECT 1 FROM migration.region_talk_stage_work_input_v9 exact_input
       WHERE exact_input.work_item_id=stale.work_item_id);
  v_result:=migration.claim_region_talk_stage_work_v8_untyped(
    requested_task_run_id,requested_export_batch_id,requested_request);
  IF v_result->>'status'<>'CLAIMED' THEN RETURN v_result; END IF;
  SELECT * INTO STRICT v_input FROM migration.region_talk_stage_work_input_v9 input
   WHERE input.work_item_id=(v_result->>'work_item_id')::uuid
     AND input.task_run_id=requested_task_run_id
     AND input.export_batch_id=requested_export_batch_id
     AND input.stage_run_id=v_run.stage_run_id
     AND input.input_fingerprint=v_result->>'input_fingerprint';
  v_payload:=jsonb_set(v_result->'payload','{input_data}',v_input.input_data);
  v_payload:=jsonb_set(v_payload,'{upstream_results}',v_input.upstream_results);
  v_result:=jsonb_set(v_result,'{payload}',v_payload)-'receipt_sha256';
  RETURN v_result||jsonb_build_object('receipt_sha256',migration.region_talk_json_sha256(v_result));
END
$$;

ALTER FUNCTION migration.submit_region_talk_stage_worker_result(uuid,uuid,jsonb)
  RENAME TO submit_region_talk_stage_worker_result_v8_unverified;

CREATE FUNCTION migration.submit_region_talk_stage_worker_result(
  requested_worker_task_run_id uuid,requested_effect_id uuid,requested_result jsonb
) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog
AS $$
DECLARE
  v_worker master_control.task_credential_registration%ROWTYPE;
  v_binding migration.region_talk_stage_worker_generation%ROWTYPE;
  v_claim migration.region_talk_stage_dispatch_claim%ROWTYPE;
  v_input migration.region_talk_stage_work_input_v9%ROWTYPE;
  v_run migration.region_talk_post_import_stage_run%ROWTYPE;
BEGIN
  v_worker:=master_control.assert_registered_task_credential('region_talk',requested_worker_task_run_id);
  SELECT exact_binding.* INTO STRICT v_binding
    FROM migration.region_talk_stage_worker_generation exact_binding
   WHERE exact_binding.effect_id=requested_effect_id
     AND exact_binding.dispatch_id=(requested_result->>'dispatch_id')::uuid
     AND exact_binding.worker_task_run_id=requested_worker_task_run_id
     AND exact_binding.worker_credential_id=v_worker.credential_id
     AND exact_binding.worker_generation=v_worker.generation
     AND exact_binding.worker_generation=(SELECT max(current.worker_generation)
       FROM migration.region_talk_stage_worker_generation current
       WHERE current.dispatch_id=exact_binding.dispatch_id)
     AND exact_binding.worker_binding_sha256=requested_result->>'worker_binding_sha256';
  SELECT * INTO STRICT v_claim FROM migration.region_talk_stage_dispatch_claim claim
   WHERE claim.dispatch_id=v_binding.dispatch_id AND claim.effect_id=requested_effect_id
     AND claim.work_item_id=(requested_result->>'work_item_id')::uuid
     AND claim.attempt=(requested_result->>'attempt')::integer;
  SELECT * INTO STRICT v_input FROM migration.region_talk_stage_work_input_v9 input
   WHERE input.work_item_id=v_claim.work_item_id AND input.stage_run_id=v_claim.stage_run_id
     AND input.input_fingerprint=v_claim.input_fingerprint;
  SELECT * INTO STRICT v_run FROM migration.region_talk_post_import_stage_run
   WHERE stage_run_id=v_claim.stage_run_id AND task_run_id=v_claim.supervisor_task_run_id
     AND export_batch_id=v_claim.export_batch_id;
  IF requested_result->>'metadata_sha256' IS DISTINCT FROM
       migration.region_talk_json_sha256(requested_result->'result_metadata')
     OR NOT migration.region_talk_stage_result_valid_v9(v_claim.stage,v_claim.contract_version,
       v_run.canonical_revision,v_claim.master_instance_id,v_claim.epoch,v_input.input_data,
       v_input.upstream_results,requested_result->>'result_status',
       requested_result->'result_metadata',requested_result->>'result_sha256') THEN
    RAISE EXCEPTION USING ERRCODE='22023',
      MESSAGE='direct stage worker result fails exact v9 stage validation';
  END IF;
  RETURN migration.submit_region_talk_stage_worker_result_v8_unverified(
    requested_worker_task_run_id,requested_effect_id,requested_result);
END
$$;

REVOKE ALL ON migration.region_talk_stage_runtime_pin,
  migration.region_talk_stage_work_input_v9 FROM PUBLIC,mdh_mcp_reader,mdh_mcp_editor,
  mdh_region_talk_pipeline;
REVOKE SELECT ON migration.region_talk_stage_runtime_pin_current_v1
  FROM PUBLIC,mdh_mcp_reader,mdh_mcp_editor,mdh_region_talk_pipeline;
REVOKE EXECUTE ON FUNCTION migration.register_region_talk_stage_runtime_pin(jsonb),
  migration.region_talk_stage_result_valid_v9(text,text,bigint,uuid,bigint,jsonb,jsonb,text,jsonb,text),
  migration.ensure_region_talk_stage_work_v9(uuid,uuid,uuid,uuid,bigint,uuid,integer,text,text,text,jsonb,jsonb),
  migration.execute_region_talk_post_import_stages_v8_unmaterialized(uuid,uuid,jsonb),
  migration.claim_region_talk_stage_work_v8_untyped(uuid,uuid,jsonb),
  migration.claim_region_talk_stage_work(uuid,uuid,jsonb),
  migration.submit_region_talk_stage_worker_result_v8_unverified(uuid,uuid,jsonb),
  migration.submit_region_talk_stage_worker_result(uuid,uuid,jsonb)
  FROM PUBLIC,mdh_mcp_reader,mdh_mcp_editor,mdh_region_talk_pipeline;
GRANT EXECUTE ON FUNCTION migration.register_region_talk_stage_runtime_pin(jsonb)
  TO mdh_owner,mdh_master_controller;
GRANT EXECUTE ON FUNCTION migration.execute_region_talk_post_import_stages(uuid,uuid,jsonb),
  migration.claim_region_talk_stage_work(uuid,uuid,jsonb),
  migration.submit_region_talk_stage_worker_result(uuid,uuid,jsonb)
  TO mdh_region_talk_pipeline;

UPDATE hub.canonical_state SET schema_revision=30,updated_at=clock_timestamp()
WHERE singleton=true;
