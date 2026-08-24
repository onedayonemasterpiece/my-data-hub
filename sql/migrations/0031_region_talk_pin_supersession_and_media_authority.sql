-- Region Talk v10: runtime-pin supersession creates a new immutable work identity,
-- and image readiness is backed by an owner/master-registered acquisition/object claim.

CREATE TABLE migration.region_talk_media_artifact_acquisition (
    acquisition_id              uuid PRIMARY KEY,
    task_run_id                 uuid NOT NULL,
    export_batch_id             uuid NOT NULL REFERENCES migration.region_talk_direct_snapshot(export_batch_id),
    stage_run_id                uuid NOT NULL REFERENCES migration.region_talk_post_import_stage_run(stage_run_id),
    canonical_revision          bigint NOT NULL CHECK(canonical_revision>=1),
    master_instance_id          uuid NOT NULL,
    epoch                       bigint NOT NULL CHECK(epoch>=1),
    candidate_id                uuid NOT NULL REFERENCES region_talk.publication_candidate(candidate_id),
    candidate_revision          integer NOT NULL CHECK(candidate_revision>=1),
    candidate_revision_fingerprint text NOT NULL CHECK(candidate_revision_fingerprint ~ '^[a-f0-9]{64}$'),
    content_id                  uuid NOT NULL REFERENCES hub.content_item(content_id),
    asset_id                    uuid NOT NULL REFERENCES hub.content_asset(asset_id),
    source_media_id             text NOT NULL CHECK(length(source_media_id) BETWEEN 1 AND 500),
    normalized_source_url       text NOT NULL CHECK(length(normalized_source_url) BETWEEN 1 AND 4000),
    source_url_sha256           text NOT NULL CHECK(source_url_sha256 ~ '^[a-f0-9]{64}$'),
    object_ref                  text NOT NULL CHECK(length(object_ref) BETWEEN 1 AND 1000),
    artifact_sha256             text NOT NULL CHECK(artifact_sha256 ~ '^[a-f0-9]{64}$'),
    byte_size                   bigint NOT NULL CHECK(byte_size BETWEEN 1 AND 1073741824),
    content_type                text NOT NULL CHECK(length(content_type) BETWEEN 1 AND 200),
    width                       integer CHECK(width IS NULL OR width BETWEEN 1 AND 100000),
    height                      integer CHECK(height IS NULL OR height BETWEEN 1 AND 100000),
    acquisition_evidence_sha256 text NOT NULL CHECK(acquisition_evidence_sha256 ~ '^[a-f0-9]{64}$'),
    request_sha256              text NOT NULL UNIQUE CHECK(request_sha256 ~ '^[a-f0-9]{64}$'),
    receipt_sha256              text NOT NULL UNIQUE CHECK(receipt_sha256 ~ '^[a-f0-9]{64}$'),
    receipt                     jsonb NOT NULL,
    registered_by               name NOT NULL,
    registered_at               timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(stage_run_id,candidate_id,candidate_revision,asset_id,artifact_sha256,object_ref)
);
CREATE TRIGGER region_talk_media_artifact_acquisition_append_only
BEFORE UPDATE OR DELETE ON migration.region_talk_media_artifact_acquisition
FOR EACH ROW EXECUTE FUNCTION hub_meta.reject_update_delete();

CREATE FUNCTION migration.register_region_talk_media_artifact_acquisition(requested_request jsonb)
RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog
AS $$
DECLARE
  v_state master_control.epoch_state%ROWTYPE; v_run migration.region_talk_post_import_stage_run%ROWTYPE;
  v_candidate region_talk.publication_candidate%ROWTYPE;
  v_revision region_talk.candidate_revision%ROWTYPE; v_asset hub.content_asset%ROWTYPE;
  v_request_sha text; v_id uuid; v_base jsonb; v_receipt jsonb;
  v_existing migration.region_talk_media_artifact_acquisition%ROWTYPE;
BEGIN
  IF NOT (pg_has_role(session_user,'mdh_owner','member') OR
          pg_has_role(session_user,'mdh_master_controller','member')) THEN
    RAISE EXCEPTION USING ERRCODE='42501',MESSAGE='media acquisition registration requires owner/master controller';
  END IF;
  IF requested_request->>'schema_version'<>'region-talk-media-artifact-acquisition.v1'
     OR requested_request - ARRAY['schema_version','task_run_id','export_batch_id','stage_run_id',
       'canonical_revision','master_instance_id','epoch','candidate_id','candidate_revision',
       'candidate_revision_fingerprint','content_id','asset_id','source_media_id',
       'normalized_source_url','source_url_sha256','object_ref','artifact_sha256','byte_size',
       'content_type','width','height','acquisition_evidence_sha256','requested_at',
       'publication_dispatch','notification_dispatch']::text[] <> '{}'::jsonb
     OR requested_request->>'candidate_revision_fingerprint' !~ '^[a-f0-9]{64}$'
     OR requested_request->>'source_url_sha256' !~ '^[a-f0-9]{64}$'
     OR requested_request->>'artifact_sha256' !~ '^[a-f0-9]{64}$'
     OR requested_request->>'acquisition_evidence_sha256' !~ '^[a-f0-9]{64}$'
     OR length(coalesce(requested_request->>'source_media_id','')) NOT BETWEEN 1 AND 500
     OR length(coalesce(requested_request->>'normalized_source_url','')) NOT BETWEEN 1 AND 4000
     OR length(coalesce(requested_request->>'object_ref','')) NOT BETWEEN 1 AND 1000
     OR requested_request->>'object_ref' !~ '^[A-Za-z0-9][A-Za-z0-9._/-]*$'
     OR requested_request->>'object_ref' LIKE '%..%'
     OR requested_request->>'object_ref' LIKE '%//%'
     OR length(coalesce(requested_request->>'content_type','')) NOT BETWEEN 1 AND 200
     OR requested_request->'publication_dispatch' IS DISTINCT FROM 'false'::jsonb
     OR requested_request->'notification_dispatch' IS DISTINCT FROM 'false'::jsonb THEN
    RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='media acquisition violates fixed contract';
  END IF;
  BEGIN
    PERFORM (requested_request->>'requested_at')::timestamptz;
    IF (requested_request->>'canonical_revision')::bigint<1
       OR (requested_request->>'epoch')::bigint<1
       OR (requested_request->>'candidate_revision')::integer<1
       OR (requested_request->>'byte_size')::bigint NOT BETWEEN 1 AND 1073741824
       OR (requested_request->>'width' IS NOT NULL AND (requested_request->>'width')::integer<1)
       OR (requested_request->>'height' IS NOT NULL AND (requested_request->>'height')::integer<1) THEN
      RAISE EXCEPTION 'range';
    END IF;
  EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='media acquisition numeric/time identity is invalid';
  END;
  SELECT * INTO STRICT v_state FROM master_control.epoch_state WHERE singleton=true FOR SHARE;
  IF v_state.gate_state<>'open' OR v_state.lease_until<=clock_timestamp()
     OR requested_request->>'master_instance_id'<>v_state.master_instance_id::text
     OR (requested_request->>'epoch')::bigint<>v_state.current_epoch THEN
    RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='media acquisition is outside ACTIVE epoch';
  END IF;
  SELECT * INTO STRICT v_run FROM migration.region_talk_post_import_stage_run run
   WHERE run.stage_run_id=(requested_request->>'stage_run_id')::uuid
     AND run.task_run_id=(requested_request->>'task_run_id')::uuid
     AND run.export_batch_id=(requested_request->>'export_batch_id')::uuid
     AND run.canonical_revision=(requested_request->>'canonical_revision')::bigint
     AND run.canonical_revision=(SELECT canonical_revision FROM hub.canonical_state
       WHERE singleton=true);
  IF NOT EXISTS(SELECT 1 FROM region_talk.accepted_snapshot_v2 accepted
    WHERE accepted.task_run_id=v_run.task_run_id AND accepted.export_batch_id=v_run.export_batch_id
      AND accepted.canonical_revision=v_run.canonical_revision) THEN
    RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='media acquisition lacks current accepted snapshot';
  END IF;
  SELECT * INTO STRICT v_candidate FROM region_talk.publication_candidate candidate
   WHERE candidate.candidate_id=(requested_request->>'candidate_id')::uuid
     AND candidate.content_id=(requested_request->>'content_id')::uuid
     AND candidate.current_revision=(requested_request->>'candidate_revision')::integer;
  SELECT * INTO STRICT v_revision FROM region_talk.candidate_revision revision
   WHERE revision.candidate_id=v_candidate.candidate_id
     AND revision.revision=v_candidate.current_revision
     AND revision.revision_fingerprint=requested_request->>'candidate_revision_fingerprint';
  SELECT * INTO STRICT v_asset FROM hub.content_asset asset
   WHERE asset.asset_id=(requested_request->>'asset_id')::uuid
     AND asset.content_id=v_candidate.content_id AND asset.asset_type IN('image','thumbnail')
     AND asset.status='available' AND asset.source_external_id=requested_request->>'source_media_id'
     AND asset.normalized_url=requested_request->>'normalized_source_url'
     AND encode(sha256(convert_to(coalesce(asset.source_url,''),'UTF8')),'hex')=
       requested_request->>'source_url_sha256'
     AND asset.sha256=requested_request->>'artifact_sha256'
     AND asset.byte_size=(requested_request->>'byte_size')::bigint
     AND asset.mime_type=requested_request->>'content_type'
     AND asset.width IS NOT DISTINCT FROM NULLIF(requested_request->>'width','')::integer
     AND asset.height IS NOT DISTINCT FROM NULLIF(requested_request->>'height','')::integer;
  v_request_sha:=migration.region_talk_json_sha256(requested_request-'requested_at');
  SELECT * INTO v_existing FROM migration.region_talk_media_artifact_acquisition
   WHERE request_sha256=v_request_sha;
  IF FOUND THEN RETURN v_existing.receipt; END IF;
  v_id:=migration.region_talk_stage_uuid5('region-talk-media-acquisition:'||v_run.stage_run_id::text||':'||
    v_candidate.candidate_id::text||':'||v_revision.revision::text||':'||v_asset.asset_id::text||':'||
    v_asset.sha256||':'||(requested_request->>'object_ref'));
  v_base:=jsonb_build_object('schema_version','region-talk-media-artifact-acquisition-receipt.v1',
    'registered',true,'acquisition_id',v_id,'task_run_id',v_run.task_run_id,
    'export_batch_id',v_run.export_batch_id,'stage_run_id',v_run.stage_run_id,
    'canonical_revision',v_run.canonical_revision,'master_instance_id',v_state.master_instance_id,
    'epoch',v_state.current_epoch,'candidate_id',v_candidate.candidate_id,
    'candidate_revision',v_revision.revision,
    'candidate_revision_fingerprint',v_revision.revision_fingerprint,
    'content_id',v_candidate.content_id,'asset_id',v_asset.asset_id,
    'source_media_id',v_asset.source_external_id,'normalized_source_url',v_asset.normalized_url,
    'source_url_sha256',requested_request->>'source_url_sha256',
    'object_ref',requested_request->>'object_ref','artifact_sha256',v_asset.sha256,
    'byte_size',v_asset.byte_size,'content_type',v_asset.mime_type,'width',to_jsonb(v_asset.width),
    'height',to_jsonb(v_asset.height),
    'acquisition_evidence_sha256',requested_request->>'acquisition_evidence_sha256',
    'task_readable',true,'publication_dispatch',false,'notification_dispatch',false);
  v_receipt:=v_base||jsonb_build_object('receipt_sha256',migration.region_talk_json_sha256(v_base));
  INSERT INTO migration.region_talk_media_artifact_acquisition(acquisition_id,task_run_id,
    export_batch_id,stage_run_id,canonical_revision,master_instance_id,epoch,candidate_id,
    candidate_revision,candidate_revision_fingerprint,content_id,asset_id,source_media_id,
    normalized_source_url,source_url_sha256,object_ref,artifact_sha256,byte_size,content_type,
    width,height,acquisition_evidence_sha256,request_sha256,receipt_sha256,receipt,registered_by)
  VALUES(v_id,v_run.task_run_id,v_run.export_batch_id,v_run.stage_run_id,v_run.canonical_revision,
    v_state.master_instance_id,v_state.current_epoch,v_candidate.candidate_id,v_revision.revision,
    v_revision.revision_fingerprint,v_candidate.content_id,v_asset.asset_id,v_asset.source_external_id,
    v_asset.normalized_url,requested_request->>'source_url_sha256',requested_request->>'object_ref',
    v_asset.sha256,v_asset.byte_size,v_asset.mime_type,v_asset.width,v_asset.height,
    requested_request->>'acquisition_evidence_sha256',v_request_sha,v_receipt->>'receipt_sha256',
    v_receipt,session_user);
  RETURN v_receipt;
END
$$;

ALTER FUNCTION migration.ensure_region_talk_stage_work_v9(
  uuid,uuid,uuid,uuid,bigint,uuid,integer,text,text,text,jsonb,jsonb)
  RENAME TO ensure_region_talk_stage_work_v9_static_text_identity;

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
  v_acquisition migration.region_talk_media_artifact_acquisition%ROWTYPE;
  v_admissible boolean:=true;
BEGIN
  IF requested_input_data->>'schema_version' IS NULL
     OR jsonb_typeof(requested_upstream_results)<>'array'
     OR requested_revision_fingerprint !~ '^[a-f0-9]{64}$' THEN
    RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='stage work input is invalid';
  END IF;
  IF requested_stage IN('e5_embedding','bge_m3_embedding','final_verifier','writer') THEN
    v_admissible:=EXISTS(SELECT 1 FROM migration.region_talk_stage_runtime_pin_current_v1 pin
      WHERE pin.stage=requested_stage AND pin.contract_version=requested_contract
        AND pin.master_instance_id=requested_master_instance_id AND pin.epoch=requested_epoch
        AND pin.effective_canonical_revision=(SELECT run.canonical_revision
          FROM migration.region_talk_post_import_stage_run run
          WHERE run.stage_run_id=requested_stage_run_id)
        AND pin.receipt=requested_input_data->'runtime_pin');
  ELSIF requested_stage='image_scoring' THEN
    SELECT * INTO v_acquisition FROM migration.region_talk_media_artifact_acquisition acquisition
     WHERE acquisition.task_run_id=requested_task_run_id
       AND acquisition.export_batch_id=requested_export_batch_id
       AND acquisition.stage_run_id=requested_stage_run_id
       AND acquisition.master_instance_id=requested_master_instance_id
       AND acquisition.epoch=requested_epoch
       AND acquisition.candidate_id=requested_candidate_id
       AND acquisition.candidate_revision=requested_candidate_revision
       AND acquisition.candidate_revision_fingerprint=requested_revision_fingerprint
       AND acquisition.canonical_revision=(SELECT run.canonical_revision
         FROM migration.region_talk_post_import_stage_run run
         WHERE run.stage_run_id=requested_stage_run_id)
       AND acquisition.asset_id=(requested_input_data->>'asset_id')::uuid
       AND acquisition.source_media_id=requested_input_data->>'source_media_id'
       AND acquisition.normalized_source_url=requested_input_data->>'normalized_source_url'
       AND acquisition.object_ref=requested_input_data->>'object_ref'
       AND acquisition.artifact_sha256=requested_input_data->>'artifact_sha256'
       AND acquisition.byte_size=(requested_input_data->>'byte_size')::bigint
       AND acquisition.content_type=requested_input_data->>'content_type';
    v_admissible:=FOUND AND EXISTS(
      SELECT 1 FROM migration.region_talk_stage_runtime_pin_current_v1 pin
       WHERE pin.stage=requested_stage AND pin.contract_version=requested_contract
         AND pin.master_instance_id=requested_master_instance_id AND pin.epoch=requested_epoch
         AND pin.effective_canonical_revision=v_acquisition.canonical_revision
         AND pin.receipt=requested_input_data->'runtime_pin') AND EXISTS(
      SELECT 1 FROM hub.content_asset asset
       WHERE asset.asset_id=v_acquisition.asset_id AND asset.content_id=v_acquisition.content_id
         AND asset.status='available' AND asset.source_external_id=v_acquisition.source_media_id
         AND asset.normalized_url=v_acquisition.normalized_source_url
         AND encode(sha256(convert_to(coalesce(asset.source_url,''),'UTF8')),'hex')=
           v_acquisition.source_url_sha256
         AND asset.sha256=v_acquisition.artifact_sha256
         AND asset.byte_size=v_acquisition.byte_size AND asset.mime_type=v_acquisition.content_type
         AND asset.width IS NOT DISTINCT FROM v_acquisition.width
         AND asset.height IS NOT DISTINCT FROM v_acquisition.height);
    IF v_admissible THEN
      requested_input_data:=jsonb_set(requested_input_data,'{acquisition_receipt}',v_acquisition.receipt);
      requested_input_data:=jsonb_set(requested_input_data,'{acquisition_receipt_sha256}',
        to_jsonb(v_acquisition.receipt_sha256));
    END IF;
  END IF;
  v_fingerprint:=migration.region_talk_json_sha256(jsonb_build_object(
    'stage',requested_stage,'contract_version',requested_contract,
    'revision_fingerprint',requested_revision_fingerprint,'input_data',requested_input_data,
    'upstream_results',requested_upstream_results));
  IF NOT v_admissible THEN RETURN v_fingerprint; END IF;
  SELECT pipeline_id INTO STRICT v_pipeline_id FROM orchestration.pipeline
   WHERE workload='region-talk' AND name='region-talk-main' AND version='1.0.0' AND status='paused';
  SELECT * INTO STRICT v_stage FROM orchestration.pipeline_stage
   WHERE pipeline_id=v_pipeline_id AND stage_key=requested_stage AND stage_version='v1' AND enabled
     AND contract->>'name'=requested_contract;
  SELECT project_id INTO STRICT v_project_id FROM hub.project WHERE slug::text='region-talk';
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
      'reason','dependency-ready exact v10 input','publication_dispatch',false,
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
    RAISE EXCEPTION USING ERRCODE='23505',MESSAGE='stage work v10 idempotency conflict';
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
    RAISE EXCEPTION USING ERRCODE='23505',MESSAGE='stage private input v10 idempotency conflict';
  END IF;
  RETURN v_fingerprint;
END
$$;

ALTER FUNCTION migration.region_talk_stage_result_valid_v9(
  text,text,bigint,uuid,bigint,jsonb,jsonb,text,jsonb,text)
  RENAME TO region_talk_stage_result_valid_v9_unchecked_dependencies;

CREATE FUNCTION migration.region_talk_stage_result_valid_v9(
  requested_stage text,requested_contract text,requested_canonical_revision bigint,
  requested_master_instance_id uuid,requested_epoch bigint,requested_input_data jsonb,
  requested_upstream_results jsonb,requested_result_status text,requested_metadata jsonb,
  requested_result_sha256 text
) RETURNS boolean
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path=pg_catalog
AS $$
DECLARE
  v_upstream jsonb; v_upstream_count integer:=0; v_upstream_valid boolean;
  v_expected_stages text[]; v_acquisition migration.region_talk_media_artifact_acquisition%ROWTYPE;
BEGIN
  IF requested_result_status='SUCCEEDED' AND requested_stage NOT IN('e5_embedding','bge_m3_embedding') THEN
    v_expected_stages:=CASE requested_stage
      WHEN 'vector_fusion' THEN ARRAY['e5_embedding','bge_m3_embedding']
      WHEN 'image_scoring' THEN ARRAY['vector_fusion']
      WHEN 'final_verifier' THEN ARRAY['vector_fusion','image_scoring']
      WHEN 'writer' THEN ARRAY['final_verifier'] ELSE ARRAY[]::text[] END;
    IF jsonb_typeof(requested_upstream_results)<>'array'
       OR jsonb_array_length(requested_upstream_results)<>cardinality(v_expected_stages) THEN
      RETURN false;
    END IF;
    FOR v_upstream IN SELECT value FROM jsonb_array_elements(requested_upstream_results) LOOP
      IF v_upstream - ARRAY['stage','contract_version','input_fingerprint','result_sha256',
           'result_metadata']::text[] <> '{}'::jsonb
         OR NOT (v_upstream->>'stage'=ANY(v_expected_stages))
         OR v_upstream->>'input_fingerprint' !~ '^[a-f0-9]{64}$'
         OR v_upstream->>'result_sha256' !~ '^[a-f0-9]{64}$' THEN RETURN false; END IF;
      SELECT migration.region_talk_stage_result_valid_v9(landed.stage,landed.contract_version,
        requested_canonical_revision,requested_master_instance_id,requested_epoch,input.input_data,
        input.upstream_results,landed.result_status,landed.result_metadata,landed.result_sha256)
        INTO v_upstream_valid
        FROM migration.region_talk_stage_worker_result landed
        JOIN migration.region_talk_stage_work_input_v9 input USING(work_item_id)
       WHERE landed.stage=v_upstream->>'stage'
         AND landed.contract_version=v_upstream->>'contract_version'
         AND landed.input_fingerprint=v_upstream->>'input_fingerprint'
         AND landed.result_sha256=v_upstream->>'result_sha256'
         AND landed.result_metadata=v_upstream->'result_metadata'
         AND landed.result_status='SUCCEEDED'
         AND landed.master_instance_id=requested_master_instance_id
         AND landed.epoch=requested_epoch
         AND landed.subject_id=(requested_metadata->>'subject_id')::uuid
         AND landed.candidate_revision=(requested_metadata->>'candidate_revision')::integer
         AND landed.revision_fingerprint=requested_metadata->>'revision_fingerprint'
       ORDER BY landed.attempt DESC LIMIT 1;
      IF coalesce(v_upstream_valid,false) IS NOT TRUE THEN RETURN false; END IF;
      v_upstream_count:=v_upstream_count+1;
    END LOOP;
    IF v_upstream_count<>cardinality(v_expected_stages)
       OR (SELECT count(DISTINCT value->>'stage') FROM jsonb_array_elements(requested_upstream_results))<>
          cardinality(v_expected_stages) THEN RETURN false; END IF;
  END IF;
  IF requested_result_status='SUCCEEDED' AND requested_stage='image_scoring' THEN
    SELECT * INTO v_acquisition FROM migration.region_talk_media_artifact_acquisition acquisition
     WHERE acquisition.receipt=requested_input_data->'acquisition_receipt'
       AND acquisition.receipt_sha256=requested_input_data->>'acquisition_receipt_sha256'
       AND acquisition.canonical_revision=requested_canonical_revision
       AND acquisition.master_instance_id=requested_master_instance_id
       AND acquisition.epoch=requested_epoch
       AND acquisition.candidate_id=(requested_metadata->>'subject_id')::uuid
       AND acquisition.candidate_revision=(requested_metadata->>'candidate_revision')::integer
       AND acquisition.candidate_revision_fingerprint=requested_metadata->>'revision_fingerprint'
       AND acquisition.asset_id=(requested_input_data->>'asset_id')::uuid
       AND acquisition.artifact_sha256=requested_input_data->>'artifact_sha256'
       AND acquisition.object_ref=requested_input_data->>'object_ref';
    IF NOT FOUND OR NOT EXISTS(SELECT 1 FROM hub.content_asset asset
      WHERE asset.asset_id=v_acquisition.asset_id AND asset.content_id=v_acquisition.content_id
        AND asset.status='available' AND asset.source_external_id=v_acquisition.source_media_id
        AND asset.normalized_url=v_acquisition.normalized_source_url
        AND encode(sha256(convert_to(coalesce(asset.source_url,''),'UTF8')),'hex')=
          v_acquisition.source_url_sha256
        AND asset.sha256=v_acquisition.artifact_sha256 AND asset.byte_size=v_acquisition.byte_size
        AND asset.mime_type=v_acquisition.content_type
        AND asset.width IS NOT DISTINCT FROM v_acquisition.width
        AND asset.height IS NOT DISTINCT FROM v_acquisition.height) THEN RETURN false; END IF;
  END IF;
  RETURN migration.region_talk_stage_result_valid_v9_unchecked_dependencies(requested_stage,
    requested_contract,requested_canonical_revision,requested_master_instance_id,requested_epoch,
    requested_input_data,requested_upstream_results,requested_result_status,requested_metadata,
    requested_result_sha256);
END
$$;

CREATE FUNCTION migration.region_talk_stage_work_input_current_v10(requested_work_item_id uuid)
RETURNS boolean
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path=pg_catalog
AS $$
DECLARE
  v_input migration.region_talk_stage_work_input_v9%ROWTYPE; v_run migration.region_talk_post_import_stage_run%ROWTYPE;
  v_upstream jsonb; v_valid boolean; v_expected integer; v_expected_stages text[];
BEGIN
  SELECT * INTO v_input FROM migration.region_talk_stage_work_input_v9 input
   WHERE input.work_item_id=requested_work_item_id;
  IF NOT FOUND THEN RETURN false; END IF;
  SELECT * INTO STRICT v_run FROM migration.region_talk_post_import_stage_run
   WHERE stage_run_id=v_input.stage_run_id;
  IF v_input.stage IN('e5_embedding','bge_m3_embedding','image_scoring','final_verifier','writer')
     AND NOT EXISTS(SELECT 1 FROM migration.region_talk_stage_runtime_pin_current_v1 pin
       WHERE pin.stage=v_input.stage AND pin.contract_version=v_input.contract_version
         AND pin.effective_canonical_revision=v_run.canonical_revision
         AND pin.master_instance_id=v_input.master_instance_id AND pin.epoch=v_input.epoch
         AND pin.receipt=v_input.input_data->'runtime_pin') THEN RETURN false; END IF;
  IF v_input.stage='image_scoring' AND NOT EXISTS(
    SELECT 1 FROM migration.region_talk_media_artifact_acquisition acquisition
     JOIN hub.content_asset asset USING(asset_id)
    WHERE acquisition.receipt=v_input.input_data->'acquisition_receipt'
      AND acquisition.receipt_sha256=v_input.input_data->>'acquisition_receipt_sha256'
      AND acquisition.stage_run_id=v_input.stage_run_id
      AND acquisition.candidate_id=v_input.subject_id
      AND acquisition.candidate_revision=v_input.candidate_revision
      AND asset.status='available' AND asset.sha256=acquisition.artifact_sha256
      AND asset.byte_size=acquisition.byte_size AND asset.mime_type=acquisition.content_type
      AND asset.normalized_url=acquisition.normalized_source_url
      AND asset.source_external_id=acquisition.source_media_id
      AND encode(sha256(convert_to(coalesce(asset.source_url,''),'UTF8')),'hex')=
        acquisition.source_url_sha256
      AND asset.width IS NOT DISTINCT FROM acquisition.width
      AND asset.height IS NOT DISTINCT FROM acquisition.height) THEN RETURN false; END IF;
  v_expected_stages:=CASE v_input.stage
    WHEN 'vector_fusion' THEN ARRAY['e5_embedding','bge_m3_embedding']
    WHEN 'image_scoring' THEN ARRAY['vector_fusion']
    WHEN 'final_verifier' THEN ARRAY['vector_fusion','image_scoring']
    WHEN 'writer' THEN ARRAY['final_verifier'] ELSE ARRAY[]::text[] END;
  v_expected:=cardinality(v_expected_stages);
  IF jsonb_array_length(v_input.upstream_results)<>v_expected THEN RETURN false; END IF;
  FOR v_upstream IN SELECT value FROM jsonb_array_elements(v_input.upstream_results) LOOP
    IF NOT (v_upstream->>'stage'=ANY(v_expected_stages)) THEN RETURN false; END IF;
    SELECT migration.region_talk_stage_result_valid_v9(landed.stage,landed.contract_version,
      v_run.canonical_revision,v_input.master_instance_id,v_input.epoch,prior_input.input_data,
      prior_input.upstream_results,landed.result_status,landed.result_metadata,landed.result_sha256)
      INTO v_valid FROM migration.region_talk_stage_worker_result landed
     JOIN migration.region_talk_stage_work_input_v9 prior_input USING(work_item_id)
     WHERE landed.stage=v_upstream->>'stage'
       AND landed.contract_version=v_upstream->>'contract_version'
       AND landed.input_fingerprint=v_upstream->>'input_fingerprint'
       AND landed.result_sha256=v_upstream->>'result_sha256'
       AND landed.result_metadata=v_upstream->'result_metadata'
       AND landed.result_status='SUCCEEDED'
       AND landed.stage_run_id=v_input.stage_run_id AND landed.subject_id=v_input.subject_id
       AND landed.candidate_revision=v_input.candidate_revision
       AND landed.revision_fingerprint=v_input.revision_fingerprint
       AND landed.master_instance_id=v_input.master_instance_id AND landed.epoch=v_input.epoch
     ORDER BY landed.attempt DESC LIMIT 1;
    IF coalesce(v_valid,false) IS NOT TRUE THEN RETURN false; END IF;
  END LOOP;
  IF (SELECT count(DISTINCT value->>'stage') FROM jsonb_array_elements(v_input.upstream_results))<>
     v_expected THEN RETURN false; END IF;
  RETURN true;
END
$$;

ALTER FUNCTION migration.claim_region_talk_stage_work(uuid,uuid,jsonb)
  RENAME TO claim_region_talk_stage_work_v9_unfenced_pin;

CREATE FUNCTION migration.claim_region_talk_stage_work(
  requested_task_run_id uuid,requested_export_batch_id uuid,requested_request jsonb
) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog
AS $$
DECLARE v_run migration.region_talk_post_import_stage_run%ROWTYPE;
BEGIN
  PERFORM master_control.assert_registered_task_credential('region_talk',requested_task_run_id);
  SELECT * INTO STRICT v_run FROM migration.region_talk_post_import_stage_run
   WHERE task_run_id=requested_task_run_id AND export_batch_id=requested_export_batch_id;
  UPDATE orchestration.work_item stale SET status='cancelled',
    last_error=jsonb_build_object('schema_version','region-talk-stage-failure.v1',
      'reason','runtime pin, acquisition, or dependency input was superseded')
   WHERE stale.payload->>'stage_run_id'=v_run.stage_run_id::text
     AND stale.status IN('pending','failed_retryable')
     AND NOT migration.region_talk_stage_work_input_current_v10(stale.work_item_id);
  RETURN migration.claim_region_talk_stage_work_v9_unfenced_pin(
    requested_task_run_id,requested_export_batch_id,requested_request);
END
$$;

REVOKE ALL ON migration.region_talk_media_artifact_acquisition
  FROM PUBLIC,mdh_mcp_reader,mdh_mcp_editor,mdh_region_talk_pipeline;
REVOKE EXECUTE ON FUNCTION migration.register_region_talk_media_artifact_acquisition(jsonb),
  migration.ensure_region_talk_stage_work_v9_static_text_identity(
    uuid,uuid,uuid,uuid,bigint,uuid,integer,text,text,text,jsonb,jsonb),
  migration.ensure_region_talk_stage_work_v9(
    uuid,uuid,uuid,uuid,bigint,uuid,integer,text,text,text,jsonb,jsonb),
  migration.region_talk_stage_result_valid_v9_unchecked_dependencies(
    text,text,bigint,uuid,bigint,jsonb,jsonb,text,jsonb,text),
  migration.region_talk_stage_result_valid_v9(
    text,text,bigint,uuid,bigint,jsonb,jsonb,text,jsonb,text),
  migration.region_talk_stage_work_input_current_v10(uuid),
  migration.claim_region_talk_stage_work_v9_unfenced_pin(uuid,uuid,jsonb),
  migration.claim_region_talk_stage_work(uuid,uuid,jsonb)
  FROM PUBLIC,mdh_mcp_reader,mdh_mcp_editor,mdh_region_talk_pipeline;
GRANT EXECUTE ON FUNCTION migration.register_region_talk_media_artifact_acquisition(jsonb)
  TO mdh_owner,mdh_master_controller;
GRANT EXECUTE ON FUNCTION migration.claim_region_talk_stage_work(uuid,uuid,jsonb)
  TO mdh_region_talk_pipeline;

UPDATE hub.canonical_state SET schema_revision=31,updated_at=clock_timestamp()
WHERE singleton=true;
