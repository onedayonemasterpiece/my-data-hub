-- Region Talk v11: private evidence enrichment and evidence-heavy result authority.
-- The sparse v9 work fingerprint remains the dispatch identity.  A separate
-- server-computed enrichment hash binds private content/evidence and never
-- crosses the metadata-only supervisor/control journals.

CREATE TABLE migration.region_talk_heavy_evidence_pack (
    evidence_id uuid PRIMARY KEY,
    task_run_id uuid NOT NULL,
    export_batch_id uuid NOT NULL REFERENCES migration.region_talk_direct_snapshot(export_batch_id),
    stage_run_id uuid NOT NULL REFERENCES migration.region_talk_post_import_stage_run(stage_run_id),
    canonical_revision bigint NOT NULL CHECK(canonical_revision>=1),
    master_instance_id uuid NOT NULL,
    epoch bigint NOT NULL CHECK(epoch>=1),
    candidate_id uuid NOT NULL REFERENCES region_talk.publication_candidate(candidate_id),
    candidate_revision integer NOT NULL CHECK(candidate_revision>=1),
    revision_fingerprint text NOT NULL CHECK(revision_fingerprint ~ '^[a-f0-9]{64}$'),
    content_id uuid NOT NULL REFERENCES hub.content_item(content_id),
    evidence_data jsonb NOT NULL CHECK(jsonb_typeof(evidence_data)='object'),
    evidence_sha256 text NOT NULL CHECK(evidence_sha256 ~ '^[a-f0-9]{64}$'),
    request_sha256 text NOT NULL UNIQUE CHECK(request_sha256 ~ '^[a-f0-9]{64}$'),
    receipt_sha256 text NOT NULL UNIQUE CHECK(receipt_sha256 ~ '^[a-f0-9]{64}$'),
    receipt jsonb NOT NULL,
    registered_by name NOT NULL,
    registered_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(stage_run_id,candidate_id,candidate_revision,evidence_sha256)
);
CREATE TRIGGER region_talk_heavy_evidence_pack_append_only
BEFORE UPDATE OR DELETE ON migration.region_talk_heavy_evidence_pack
FOR EACH ROW EXECUTE FUNCTION hub_meta.reject_update_delete();

CREATE TABLE migration.region_talk_heavy_stage_result_artifact (
    work_item_id uuid NOT NULL REFERENCES orchestration.work_item(work_item_id),
    attempt integer NOT NULL CHECK(attempt>=1),
    stage text NOT NULL CHECK(stage IN('image_scoring','final_verifier','writer')),
    subject_id uuid NOT NULL,
    candidate_revision integer NOT NULL CHECK(candidate_revision>=1),
    revision_fingerprint text NOT NULL CHECK(revision_fingerprint ~ '^[a-f0-9]{64}$'),
    work_input_fingerprint text NOT NULL CHECK(work_input_fingerprint ~ '^[a-f0-9]{64}$'),
    enrichment_sha256 text NOT NULL CHECK(enrichment_sha256 ~ '^[a-f0-9]{64}$'),
    rich_input_fingerprint text NOT NULL CHECK(rich_input_fingerprint ~ '^[a-f0-9]{64}$'),
    result_sha256 text NOT NULL CHECK(result_sha256 ~ '^[a-f0-9]{64}$'),
    result_data jsonb NOT NULL CHECK(jsonb_typeof(result_data)='object'),
    registered_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(work_item_id,attempt),
    UNIQUE(stage,subject_id,candidate_revision,result_sha256)
);
CREATE TRIGGER region_talk_heavy_stage_result_artifact_append_only
BEFORE UPDATE OR DELETE ON migration.region_talk_heavy_stage_result_artifact
FOR EACH ROW EXECUTE FUNCTION hub_meta.reject_update_delete();

-- Correct 0031's ambiguous raw-source hash without rewriting its immutable row.
-- The v2 projection is bound to both current asset columns and the legacy receipt.
CREATE FUNCTION migration.region_talk_media_artifact_acquisition_receipt_v2(requested_acquisition_id uuid)
RETURNS jsonb
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path=pg_catalog
AS $$
DECLARE
  v_acq migration.region_talk_media_artifact_acquisition%ROWTYPE;
  v_asset hub.content_asset%ROWTYPE; v_base jsonb;
BEGIN
  SELECT * INTO STRICT v_acq FROM migration.region_talk_media_artifact_acquisition
   WHERE acquisition_id=requested_acquisition_id;
  IF NOT EXISTS(SELECT 1 FROM master_control.epoch_state state
      WHERE state.singleton AND state.gate_state='open' AND state.lease_until>clock_timestamp()
        AND state.master_instance_id=v_acq.master_instance_id AND state.current_epoch=v_acq.epoch)
     OR NOT EXISTS(SELECT 1 FROM region_talk.publication_candidate candidate
      WHERE candidate.candidate_id=v_acq.candidate_id AND candidate.content_id=v_acq.content_id
        AND candidate.current_revision=v_acq.candidate_revision)
     OR NOT EXISTS(SELECT 1 FROM region_talk.accepted_snapshot_v2 accepted
      WHERE accepted.task_run_id=v_acq.task_run_id AND accepted.export_batch_id=v_acq.export_batch_id
        AND accepted.canonical_revision=v_acq.canonical_revision) THEN
    RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='media acquisition v2 projection is stale';
  END IF;
  SELECT * INTO STRICT v_asset FROM hub.content_asset asset
   WHERE asset.asset_id=v_acq.asset_id AND asset.content_id=v_acq.content_id
     AND asset.status='available' AND asset.source_external_id=v_acq.source_media_id
     AND asset.normalized_url=v_acq.normalized_source_url
     AND encode(sha256(convert_to(coalesce(asset.source_url,''),'UTF8')),'hex')=v_acq.source_url_sha256
     AND asset.sha256=v_acq.artifact_sha256 AND asset.byte_size=v_acq.byte_size
     AND asset.mime_type=v_acq.content_type AND asset.width IS NOT DISTINCT FROM v_acq.width
     AND asset.height IS NOT DISTINCT FROM v_acq.height;
  v_base:=jsonb_build_object(
    'schema_version','region-talk-media-artifact-acquisition-receipt.v2','registered',true,
    'acquisition_id',v_acq.acquisition_id,'task_run_id',v_acq.task_run_id,
    'export_batch_id',v_acq.export_batch_id,'stage_run_id',v_acq.stage_run_id,
    'canonical_revision',v_acq.canonical_revision,'master_instance_id',v_acq.master_instance_id,
    'epoch',v_acq.epoch,'candidate_id',v_acq.candidate_id,
    'candidate_revision',v_acq.candidate_revision,
    'candidate_revision_fingerprint',v_acq.candidate_revision_fingerprint,
    'content_id',v_acq.content_id,'asset_id',v_acq.asset_id,
    'source_media_id',v_acq.source_media_id,'normalized_source_url',v_acq.normalized_source_url,
    'source_url_sha256',encode(sha256(convert_to(v_acq.normalized_source_url,'UTF8')),'hex'),
    'object_ref',v_acq.object_ref,'artifact_sha256',v_acq.artifact_sha256,
    'byte_size',v_acq.byte_size,'content_type',v_acq.content_type,
    'width',to_jsonb(v_acq.width),'height',to_jsonb(v_acq.height),
    'acquisition_evidence_sha256',v_acq.acquisition_evidence_sha256,
    'legacy_receipt_sha256',v_acq.receipt_sha256,'task_readable',true,
    'publication_dispatch',false,'notification_dispatch',false);
  RETURN v_base||jsonb_build_object('receipt_sha256',migration.region_talk_json_sha256(v_base));
END
$$;

CREATE FUNCTION migration.register_region_talk_heavy_evidence_pack(requested_request jsonb)
RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog
AS $$
DECLARE
  v_state master_control.epoch_state%ROWTYPE; v_run migration.region_talk_post_import_stage_run%ROWTYPE;
  v_candidate region_talk.publication_candidate%ROWTYPE; v_revision region_talk.candidate_revision%ROWTYPE;
  v_content hub.content_item%ROWTYPE; v_data jsonb; v_request_sha text; v_evidence_sha text;
  v_id uuid; v_base jsonb; v_receipt jsonb; v_existing migration.region_talk_heavy_evidence_pack%ROWTYPE;
  v_body text; v_source_key text; v_fact jsonb; v_history jsonb;
BEGIN
  IF NOT (pg_has_role(session_user,'mdh_owner','member') OR
          pg_has_role(session_user,'mdh_master_controller','member')) THEN
    RAISE EXCEPTION USING ERRCODE='42501',MESSAGE='heavy evidence registration requires owner/master controller';
  END IF;
  IF requested_request->>'schema_version'<>'region-talk-heavy-evidence-pack.v1'
     OR requested_request - ARRAY['schema_version','task_run_id','export_batch_id','stage_run_id',
       'canonical_revision','master_instance_id','epoch','candidate_id','candidate_revision',
       'revision_fingerprint','content','eligibility_fingerprint','fact_pack','source',
       'source_profile','history','requested_at','publication_dispatch','notification_dispatch']::text[]
       <> '{}'::jsonb
     OR requested_request->>'revision_fingerprint' !~ '^[a-f0-9]{64}$'
     OR requested_request->>'eligibility_fingerprint' !~ '^[a-f0-9]{64}$'
     OR jsonb_typeof(requested_request->'content')<>'object'
     OR jsonb_typeof(requested_request->'fact_pack')<>'object'
     OR jsonb_typeof(requested_request->'source')<>'object'
     OR jsonb_typeof(requested_request->'source_profile')<>'object'
     OR jsonb_typeof(requested_request->'history')<>'array'
     OR jsonb_array_length(requested_request->'history')>5
     OR (requested_request->'fact_pack') - ARRAY['schema_version','candidate_revision_fingerprint',
       'facts','fact_pack_sha256']::text[] <> '{}'::jsonb
     OR (requested_request->'source') - ARRAY['candidate_revision_fingerprint','canonical_source_key',
       'externality_status','source_scope','source_fingerprint']::text[] <> '{}'::jsonb
     OR (requested_request->'source_profile') - ARRAY['candidate_revision_fingerprint',
       'canonical_source_key','source_fingerprint','profile_fingerprint','entity_type',
       'externality_status','dimensions']::text[] <> '{}'::jsonb
     OR (requested_request#>'{source_profile,dimensions}') - ARRAY['publisher_identity',
       'intended_audience','distinctive_value']::text[] <> '{}'::jsonb
     OR octet_length(requested_request::text)>262144
     OR requested_request->'publication_dispatch' IS DISTINCT FROM 'false'::jsonb
     OR requested_request->'notification_dispatch' IS DISTINCT FROM 'false'::jsonb THEN
    RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='heavy evidence pack violates fixed contract';
  END IF;
  BEGIN
    PERFORM (requested_request->>'requested_at')::timestamptz;
    IF (requested_request->>'canonical_revision')::bigint<1 OR
       (requested_request->>'epoch')::bigint<1 OR
       (requested_request->>'candidate_revision')::integer<1 THEN RAISE EXCEPTION 'range'; END IF;
  EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='heavy evidence identity is invalid';
  END;
  SELECT * INTO STRICT v_state FROM master_control.epoch_state WHERE singleton=true FOR SHARE;
  IF v_state.gate_state<>'open' OR v_state.lease_until<=clock_timestamp()
     OR requested_request->>'master_instance_id'<>v_state.master_instance_id::text
     OR (requested_request->>'epoch')::bigint<>v_state.current_epoch THEN
    RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='heavy evidence is outside ACTIVE epoch';
  END IF;
  SELECT * INTO STRICT v_run FROM migration.region_talk_post_import_stage_run run
   WHERE run.stage_run_id=(requested_request->>'stage_run_id')::uuid
     AND run.task_run_id=(requested_request->>'task_run_id')::uuid
     AND run.export_batch_id=(requested_request->>'export_batch_id')::uuid
     AND run.canonical_revision=(requested_request->>'canonical_revision')::bigint
     AND run.canonical_revision=(SELECT canonical_revision FROM hub.canonical_state WHERE singleton=true);
  IF NOT EXISTS(SELECT 1 FROM region_talk.accepted_snapshot_v2 accepted
    WHERE accepted.task_run_id=v_run.task_run_id AND accepted.export_batch_id=v_run.export_batch_id
      AND accepted.canonical_revision=v_run.canonical_revision) THEN
    RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='heavy evidence lacks current accepted snapshot';
  END IF;
  SELECT * INTO STRICT v_candidate FROM region_talk.publication_candidate candidate
   WHERE candidate.candidate_id=(requested_request->>'candidate_id')::uuid
     AND candidate.current_revision=(requested_request->>'candidate_revision')::integer;
  SELECT * INTO STRICT v_revision FROM region_talk.candidate_revision revision
   WHERE revision.candidate_id=v_candidate.candidate_id AND revision.revision=v_candidate.current_revision
     AND revision.revision_fingerprint=requested_request->>'revision_fingerprint';
  SELECT * INTO STRICT v_content FROM hub.content_item WHERE content_id=v_candidate.content_id AND status='active';
  v_body:=coalesce(v_revision.text_payload->>'body_text',v_content.body_excerpt,'');
  v_source_key:=coalesce(v_content.metadata->>'canonical_source_key','');
  IF requested_request->'content' IS DISTINCT FROM jsonb_build_object(
       'title',coalesce(v_content.title,''),'summary',coalesce(v_content.summary,''),
       'body_text',v_body,'text_sha256',encode(sha256(convert_to(CASE WHEN v_body<>'' THEN v_body
         ELSE concat_ws(E'\n\n',nullif(v_content.title,''),nullif(v_content.summary,'')) END,'UTF8')),'hex'),
       'canonical_url',coalesce(v_content.canonical_url,v_content.normalized_url,''),
       'canonical_source_key',v_source_key,'content_type',v_content.content_type)
     OR v_source_key='' OR requested_request#>>'{source,canonical_source_key}'<>v_source_key
     OR requested_request#>>'{source_profile,canonical_source_key}'<>v_source_key
     OR requested_request#>>'{fact_pack,candidate_revision_fingerprint}'<>v_revision.revision_fingerprint
     OR requested_request#>>'{source,candidate_revision_fingerprint}'<>v_revision.revision_fingerprint
     OR requested_request#>>'{source_profile,candidate_revision_fingerprint}'<>v_revision.revision_fingerprint
     OR requested_request#>>'{fact_pack,fact_pack_sha256}'<>
       migration.region_talk_json_sha256((requested_request->'fact_pack')-'fact_pack_sha256')
     OR requested_request#>>'{source,source_fingerprint}'<>
       migration.region_talk_json_sha256((requested_request->'source')-'source_fingerprint')
     OR requested_request#>>'{source_profile,profile_fingerprint}'<>
       migration.region_talk_json_sha256((requested_request->'source_profile')-'profile_fingerprint')
     OR requested_request#>>'{source_profile,source_fingerprint}'<>
       requested_request#>>'{source,source_fingerprint}'
     OR jsonb_array_length(requested_request#>'{fact_pack,facts}') NOT BETWEEN 1 AND 20 THEN
    RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='heavy evidence differs from current canonical candidate';
  END IF;
  IF (SELECT count(DISTINCT value->>'fact_id')
        FROM jsonb_array_elements(requested_request#>'{fact_pack,facts}'))<>
       jsonb_array_length(requested_request#>'{fact_pack,facts}')
     OR (SELECT count(DISTINCT value->>'history_id')
        FROM jsonb_array_elements(requested_request->'history'))<>
       jsonb_array_length(requested_request->'history') THEN
    RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='heavy evidence identities are not unique';
  END IF;
  FOR v_fact IN SELECT value FROM jsonb_array_elements(requested_request#>'{fact_pack,facts}') LOOP
    IF v_fact - ARRAY['fact_id','claim','support_excerpt','source_url','support_sha256']::text[]
         <> '{}'::jsonb
       OR length(coalesce(v_fact->>'fact_id','')) NOT BETWEEN 1 AND 160
       OR length(coalesce(v_fact->>'claim','')) NOT BETWEEN 1 AND 1000
       OR length(coalesce(v_fact->>'support_excerpt','')) NOT BETWEEN 1 AND 1000
       OR v_fact->>'support_sha256'<>encode(sha256(convert_to(v_fact->>'support_excerpt','UTF8')),'hex')
       OR coalesce(v_fact->>'source_url','') !~ '^https://[^/@]+([/:?].*)?$' THEN
      RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='heavy fact evidence is not exact';
    END IF;
  END LOOP;
  FOR v_history IN SELECT value FROM jsonb_array_elements(requested_request->'history') LOOP
    IF v_history - ARRAY['history_id','published_revision_fingerprint','draft_fingerprint',
         'body_text']::text[] <> '{}'::jsonb
       OR length(coalesce(v_history->>'history_id','')) NOT BETWEEN 1 AND 160
       OR v_history->>'published_revision_fingerprint' !~ '^[a-f0-9]{64}$'
       OR v_history->>'draft_fingerprint' !~ '^[a-f0-9]{64}$'
       OR length(coalesce(v_history->>'body_text','')) NOT BETWEEN 1 AND 4000 THEN
      RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='heavy publication history is not exact';
    END IF;
  END LOOP;
  v_data:=jsonb_build_object('content',requested_request->'content',
    'eligibility_fingerprint',requested_request->'eligibility_fingerprint',
    'fact_pack',requested_request->'fact_pack','source',requested_request->'source',
    'source_profile',requested_request->'source_profile','history',requested_request->'history');
  v_evidence_sha:=migration.region_talk_json_sha256(v_data);
  v_request_sha:=migration.region_talk_json_sha256(requested_request-'requested_at');
  SELECT * INTO v_existing FROM migration.region_talk_heavy_evidence_pack WHERE request_sha256=v_request_sha;
  IF FOUND THEN RETURN v_existing.receipt; END IF;
  v_id:=migration.region_talk_stage_uuid5('region-talk-heavy-evidence:'||v_run.stage_run_id::text||':'||
    v_candidate.candidate_id::text||':'||v_revision.revision::text||':'||v_evidence_sha);
  v_base:=jsonb_build_object('schema_version','region-talk-heavy-evidence-pack-receipt.v1',
    'registered',true,'evidence_id',v_id,'task_run_id',v_run.task_run_id,
    'export_batch_id',v_run.export_batch_id,'stage_run_id',v_run.stage_run_id,
    'canonical_revision',v_run.canonical_revision,'master_instance_id',v_state.master_instance_id,
    'epoch',v_state.current_epoch,'candidate_id',v_candidate.candidate_id,
    'candidate_revision',v_revision.revision,'revision_fingerprint',v_revision.revision_fingerprint,
    'content_id',v_candidate.content_id,'evidence_sha256',v_evidence_sha,
    'publication_dispatch',false,'notification_dispatch',false);
  v_receipt:=v_base||jsonb_build_object('receipt_sha256',migration.region_talk_json_sha256(v_base));
  INSERT INTO migration.region_talk_heavy_evidence_pack(evidence_id,task_run_id,export_batch_id,
    stage_run_id,canonical_revision,master_instance_id,epoch,candidate_id,candidate_revision,
    revision_fingerprint,content_id,evidence_data,evidence_sha256,request_sha256,receipt_sha256,
    receipt,registered_by)
  VALUES(v_id,v_run.task_run_id,v_run.export_batch_id,v_run.stage_run_id,v_run.canonical_revision,
    v_state.master_instance_id,v_state.current_epoch,v_candidate.candidate_id,v_revision.revision,
    v_revision.revision_fingerprint,v_candidate.content_id,v_data,v_evidence_sha,v_request_sha,
    v_receipt->>'receipt_sha256',v_receipt,session_user);
  RETURN v_receipt;
END
$$;

CREATE FUNCTION migration.region_talk_heavy_unavailable_v11(
  requested_stage text,requested_work_input_fingerprint text,requested_dag_input jsonb,requested_reason text
) RETURNS jsonb
LANGUAGE sql IMMUTABLE SECURITY DEFINER SET search_path=pg_catalog
AS $$
  WITH base AS (SELECT jsonb_build_object(
    'schema_version','region-talk-heavy-stage-input-receipt.v1','status','UNAVAILABLE',
    'stage',requested_stage,'work_input_fingerprint',requested_work_input_fingerprint,
    'enrichment_sha256',NULL,'dag_input',requested_dag_input,'heavy_input',NULL,
    'reason_code',requested_reason,'publication_dispatch',false,'notification_dispatch',false) value)
  SELECT value||jsonb_build_object('receipt_sha256',migration.region_talk_json_sha256(value)) FROM base
$$;

CREATE FUNCTION migration.region_talk_heavy_stage_input_v11(requested_work_item_id uuid)
RETURNS jsonb
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path=pg_catalog
AS $$
DECLARE
  v_input migration.region_talk_stage_work_input_v9%ROWTYPE;
  v_evidence migration.region_talk_heavy_evidence_pack%ROWTYPE;
  v_acq migration.region_talk_media_artifact_acquisition%ROWTYPE;
  v_media_receipt jsonb; v_manifest_base jsonb; v_manifest jsonb; v_base jsonb;
  v_heavy jsonb; v_enrichment text; v_receipt_base jsonb; v_reason text;
  v_image migration.region_talk_heavy_stage_result_artifact%ROWTYPE;
  v_final migration.region_talk_heavy_stage_result_artifact%ROWTYPE;
  v_vector jsonb; v_image_upstream jsonb; v_final_upstream jsonb;
BEGIN
  SELECT * INTO STRICT v_input FROM migration.region_talk_stage_work_input_v9
   WHERE work_item_id=requested_work_item_id;
  IF v_input.stage NOT IN('image_scoring','final_verifier','writer') THEN
    RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='work item is not an evidence-heavy stage';
  END IF;
  SELECT * INTO v_evidence FROM migration.region_talk_heavy_evidence_pack evidence
   WHERE evidence.task_run_id=v_input.task_run_id AND evidence.export_batch_id=v_input.export_batch_id
     AND evidence.stage_run_id=v_input.stage_run_id AND evidence.master_instance_id=v_input.master_instance_id
     AND evidence.epoch=v_input.epoch AND evidence.candidate_id=v_input.subject_id
     AND evidence.candidate_revision=v_input.candidate_revision
     AND evidence.revision_fingerprint=v_input.revision_fingerprint
   ORDER BY evidence.registered_at DESC LIMIT 1;
  IF NOT FOUND THEN RETURN migration.region_talk_heavy_unavailable_v11(v_input.stage,v_input.input_fingerprint,v_input.input_data,'AUTHORITATIVE_EVIDENCE_PACK_UNAVAILABLE'); END IF;

  IF v_input.stage='image_scoring' THEN
    SELECT * INTO v_acq FROM migration.region_talk_media_artifact_acquisition acquisition
     WHERE acquisition.acquisition_id=(v_input.input_data#>>'{acquisition_receipt,acquisition_id}')::uuid
       AND acquisition.receipt=v_input.input_data->'acquisition_receipt'
       AND acquisition.receipt_sha256=v_input.input_data->>'acquisition_receipt_sha256'
       AND acquisition.stage_run_id=v_input.stage_run_id AND acquisition.candidate_id=v_input.subject_id
       AND acquisition.candidate_revision=v_input.candidate_revision
       AND acquisition.candidate_revision_fingerprint=v_input.revision_fingerprint;
    IF NOT FOUND OR v_acq.byte_size>25000000 OR v_acq.content_type NOT IN('image/jpeg','image/png','image/webp') THEN
      RETURN migration.region_talk_heavy_unavailable_v11(v_input.stage,v_input.input_fingerprint,v_input.input_data,'AUTHORITATIVE_MEDIA_ARTIFACT_UNAVAILABLE');
    END IF;
    v_media_receipt:=migration.region_talk_media_artifact_acquisition_receipt_v2(v_acq.acquisition_id);
    v_manifest_base:=jsonb_build_object('schema_version','region-talk-media-artifact-manifest.v1',
      'candidate_revision_fingerprint',v_input.revision_fingerprint,
      'acquisition_receipts',jsonb_build_array(v_media_receipt),
      'items',jsonb_build_array(jsonb_build_object('asset_id',v_acq.asset_id::text,
        'source_media_id',v_acq.source_media_id,'normalized_source_url',v_acq.normalized_source_url,
        'source_url_sha256',v_media_receipt->>'source_url_sha256','object_ref',v_acq.object_ref,
        'artifact_sha256',v_acq.artifact_sha256,'byte_size',v_acq.byte_size,
        'content_type',v_acq.content_type,'width',to_jsonb(v_acq.width),'height',to_jsonb(v_acq.height))));
    v_manifest:=v_manifest_base||jsonb_build_object('manifest_sha256',
      migration.region_talk_json_sha256(v_manifest_base));
    v_base:=jsonb_build_object('schema_version','region-talk-image-input.v1',
      'work_input_fingerprint',v_input.input_fingerprint,
      'candidate_revision_fingerprint',v_input.revision_fingerprint,
      'content',v_evidence.evidence_data->'content',
      'eligibility_fingerprint',v_evidence.evidence_data->'eligibility_fingerprint',
      'availability','AVAILABLE','unavailable_reason','','artifact_manifest',v_manifest,
      'policy',jsonb_build_object(
        'decision_contract_version','region_talk_article_image_association_v4',
        'acquisition_version','region_talk_http_article_image_evidence_v4',
        'scorer_version','region_talk_cv_clip_laion_nima_legacy_v1',
        'vlm_prompt_version','region_talk_visual_article_association_v3',
        'model_bundle_sha256',v_input.input_data#>>'{runtime_pin,asset_manifest_sha256}',
        'vlm_model_id',v_input.input_data#>>'{runtime_pin,model_id}'),
      'publication_dispatch',false,'notification_dispatch',false);
  ELSIF v_input.stage='final_verifier' THEN
    SELECT value INTO v_vector FROM jsonb_array_elements(v_input.upstream_results)
      WHERE value->>'stage'='vector_fusion' LIMIT 1;
    SELECT value INTO v_image_upstream FROM jsonb_array_elements(v_input.upstream_results)
      WHERE value->>'stage'='image_scoring' LIMIT 1;
    SELECT * INTO v_image FROM migration.region_talk_heavy_stage_result_artifact artifact
     WHERE artifact.stage='image_scoring' AND artifact.subject_id=v_input.subject_id
       AND artifact.candidate_revision=v_input.candidate_revision
       AND artifact.revision_fingerprint=v_input.revision_fingerprint
       AND artifact.result_sha256=v_input.input_data->>'image_result_sha256'
     ORDER BY artifact.attempt DESC LIMIT 1;
    IF v_vector IS NULL OR v_image_upstream IS NULL OR NOT FOUND THEN
      RETURN migration.region_talk_heavy_unavailable_v11(v_input.stage,v_input.input_fingerprint,v_input.input_data,'TYPED_IMAGE_OR_VECTOR_RESULT_UNAVAILABLE');
    END IF;
    v_base:=jsonb_build_object('schema_version','region-talk-final-verifier-input.v1',
      'work_input_fingerprint',v_input.input_fingerprint,
      'candidate_revision_fingerprint',v_input.revision_fingerprint,
      'content',v_evidence.evidence_data->'content','fact_pack',v_evidence.evidence_data->'fact_pack',
      'source',v_evidence.evidence_data->'source','vector_result_sha256',v_input.input_data->>'vector_result_sha256',
      'image_result_sha256',v_input.input_data->>'image_result_sha256','image_result',v_image.result_data,
      'upstream_results',jsonb_build_array(
        jsonb_build_object('stage','vector_fusion','input_fingerprint',v_vector->>'input_fingerprint',
          'result_sha256',v_vector->>'result_sha256','result_metadata_sha256',
          migration.region_talk_json_sha256(v_vector->'result_metadata')),
        jsonb_build_object('stage','image_scoring','input_fingerprint',v_image.rich_input_fingerprint,
          'result_sha256',v_image.result_sha256,'result_metadata_sha256',
          migration.region_talk_json_sha256(v_image_upstream->'result_metadata'))),
      'policy',jsonb_build_object('eligibility_gate_version','region_talk_publication_eligibility_v5',
        'prompt_version','region_talk_final_verifier_v7_grounded_draft',
        'model_id',v_input.input_data#>>'{runtime_pin,model_id}'),
      'publication_dispatch',false,'notification_dispatch',false);
  ELSE
    SELECT value INTO v_final_upstream FROM jsonb_array_elements(v_input.upstream_results)
      WHERE value->>'stage'='final_verifier' LIMIT 1;
    SELECT * INTO v_final FROM migration.region_talk_heavy_stage_result_artifact artifact
     WHERE artifact.stage='final_verifier' AND artifact.subject_id=v_input.subject_id
       AND artifact.candidate_revision=v_input.candidate_revision
       AND artifact.revision_fingerprint=v_input.revision_fingerprint
       AND artifact.result_sha256=v_input.input_data->>'final_result_sha256'
     ORDER BY artifact.attempt DESC LIMIT 1;
    SELECT * INTO v_image FROM migration.region_talk_heavy_stage_result_artifact artifact
     WHERE artifact.stage='image_scoring' AND artifact.subject_id=v_input.subject_id
       AND artifact.candidate_revision=v_input.candidate_revision
       AND artifact.revision_fingerprint=v_input.revision_fingerprint
     ORDER BY artifact.attempt DESC LIMIT 1;
    IF v_final_upstream IS NULL OR v_final.work_item_id IS NULL OR v_image.work_item_id IS NULL THEN
      RETURN migration.region_talk_heavy_unavailable_v11(v_input.stage,v_input.input_fingerprint,v_input.input_data,'TYPED_IMAGE_OR_VERIFIER_RESULT_UNAVAILABLE');
    END IF;
    v_base:=jsonb_build_object('schema_version','region-talk-writer-input.v1',
      'work_input_fingerprint',v_input.input_fingerprint,
      'candidate_revision_fingerprint',v_input.revision_fingerprint,
      'content',v_evidence.evidence_data->'content','fact_pack',v_evidence.evidence_data->'fact_pack',
      'source_profile',v_evidence.evidence_data->'source_profile',
      'image_result_sha256',v_image.result_sha256,'final_result_sha256',v_final.result_sha256,
      'image_result',v_image.result_data,'final_result',v_final.result_data,
      'upstream_results',jsonb_build_array(
        jsonb_build_object('stage','image_scoring','input_fingerprint',v_image.rich_input_fingerprint,
          'result_sha256',v_image.result_sha256,'result_metadata_sha256',
          migration.region_talk_json_sha256(jsonb_build_object('private_result_sha256',v_image.result_sha256))),
        jsonb_build_object('stage','final_verifier','input_fingerprint',v_final.rich_input_fingerprint,
          'result_sha256',v_final.result_sha256,'result_metadata_sha256',
          migration.region_talk_json_sha256(v_final_upstream->'result_metadata'))),
      'history',v_evidence.evidence_data->'history',
      'policy',jsonb_build_object(
        'writer_version','region_talk_editorial_writer_v12_publisher_reader_brief',
        'output_contract','region_talk_editorial_output_v6_publisher_reader_brief',
        'input_contract','region_talk_editorial_input_v3_source_profile',
        'stage_execution_version','region_talk_writer_v12_publisher_reader_brief_v2',
        'media_materialization_contract','region_talk_media_materialization_v1',
        'model_id',v_input.input_data#>>'{runtime_pin,model_id}'),
      'publication_dispatch',false,'notification_dispatch',false);
  END IF;
  v_enrichment:=migration.region_talk_json_sha256(v_base);
  v_heavy:=v_base||jsonb_build_object('enrichment_sha256',v_enrichment);
  v_heavy:=v_heavy||jsonb_build_object('input_fingerprint',migration.region_talk_json_sha256(v_heavy));
  v_receipt_base:=jsonb_build_object('schema_version','region-talk-heavy-stage-input-receipt.v1',
    'status','READY','stage',v_input.stage,'work_input_fingerprint',v_input.input_fingerprint,
    'enrichment_sha256',v_enrichment,'dag_input',v_input.input_data,'heavy_input',v_heavy,
    'reason_code','','publication_dispatch',false,'notification_dispatch',false);
  RETURN v_receipt_base||jsonb_build_object('receipt_sha256',migration.region_talk_json_sha256(v_receipt_base));

END
$$;

CREATE FUNCTION migration.fetch_region_talk_heavy_stage_input(
  requested_worker_task_run_id uuid,requested_effect_id uuid,requested_request jsonb
) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog
AS $$
DECLARE v_sparse jsonb;
BEGIN
  -- Reuse the existing exact child-credential/binding/lease check.  Its payload
  -- remains private to this same worker transaction.
  v_sparse:=migration.fetch_region_talk_stage_work_payload(
    requested_worker_task_run_id,requested_effect_id,requested_request);
  RETURN migration.region_talk_heavy_stage_input_v11((v_sparse->>'work_item_id')::uuid);
END
$$;

CREATE FUNCTION migration.submit_region_talk_heavy_stage_worker_result(
  requested_worker_task_run_id uuid,requested_effect_id uuid,requested_request jsonb
) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog
AS $$
DECLARE
  v_direct jsonb; v_private jsonb; v_work migration.region_talk_stage_work_input_v9%ROWTYPE;
  v_current jsonb; v_existing migration.region_talk_heavy_stage_result_artifact%ROWTYPE;
  v_receipt jsonb;
BEGIN
  IF requested_request->>'schema_version'<>'region-talk-stage-worker-combined-result.v1'
     OR requested_request - ARRAY['schema_version','direct_result','private_result',
       'publication_dispatch','notification_dispatch']::text[] <> '{}'::jsonb
     OR jsonb_typeof(requested_request->'direct_result')<>'object'
     OR requested_request->'publication_dispatch' IS DISTINCT FROM 'false'::jsonb
     OR requested_request->'notification_dispatch' IS DISTINCT FROM 'false'::jsonb THEN
    RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='combined heavy result violates fixed contract';
  END IF;
  v_direct:=requested_request->'direct_result';
  SELECT * INTO STRICT v_work FROM migration.region_talk_stage_work_input_v9 input
   WHERE input.work_item_id=(v_direct->>'work_item_id')::uuid
     AND input.input_fingerprint=v_direct#>>'{result_metadata,input_fingerprint}';
  IF v_work.stage IN('image_scoring','final_verifier','writer') AND v_direct->>'result_status'='SUCCEEDED' THEN
    v_private:=requested_request->'private_result';
    v_current:=migration.region_talk_heavy_stage_input_v11(v_work.work_item_id);
    IF jsonb_typeof(v_private)<>'object'
       OR v_private - ARRAY['schema_version','stage','work_input_fingerprint','enrichment_sha256',
         'input_fingerprint','result_sha256','result_data','publication_dispatch',
         'notification_dispatch']::text[] <> '{}'::jsonb
       OR v_private->>'schema_version'<>'region-talk-heavy-stage-private-result.v1'
       OR v_private->>'stage'<>v_work.stage
       OR v_private->>'work_input_fingerprint'<>v_work.input_fingerprint
       OR v_current->>'status'<>'READY'
       OR v_private->>'enrichment_sha256'<>v_current->>'enrichment_sha256'
       OR v_private->>'input_fingerprint'<>v_current#>>'{heavy_input,input_fingerprint}'
       OR v_private->>'result_sha256'<>v_private#>>'{result_data,result_sha256}'
       OR v_private->>'result_sha256'<>v_direct->>'result_sha256'
       OR v_private->>'result_sha256'<>v_direct#>>'{result_metadata,artifact_sha256}'
       OR v_private->>'input_fingerprint'<>v_private#>>'{result_data,input_fingerprint}'
       OR octet_length((v_private->'result_data')::text)>65536
       OR v_private->'publication_dispatch' IS DISTINCT FROM 'false'::jsonb
       OR v_private->'notification_dispatch' IS DISTINCT FROM 'false'::jsonb
       OR migration.region_talk_json_sha256((v_private->'result_data')-'result_sha256')<>
          v_private->>'result_sha256'
       OR v_private#>>'{result_data,schema_version}'<>(CASE v_work.stage
          WHEN 'image_scoring' THEN 'region-talk-image-scoring-result.v1'
          WHEN 'final_verifier' THEN 'region-talk-final-verifier-result.v1'
          ELSE 'region-talk-writer-result.v1' END) THEN
      RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='private heavy result differs from current enriched work';
    END IF;
    INSERT INTO migration.region_talk_heavy_stage_result_artifact(work_item_id,attempt,stage,
      subject_id,candidate_revision,revision_fingerprint,work_input_fingerprint,
      enrichment_sha256,rich_input_fingerprint,result_sha256,result_data)
    VALUES(v_work.work_item_id,(v_direct->>'attempt')::integer,v_work.stage,v_work.subject_id,
      v_work.candidate_revision,v_work.revision_fingerprint,v_work.input_fingerprint,
      v_private->>'enrichment_sha256',v_private->>'input_fingerprint',
      v_private->>'result_sha256',v_private->'result_data')
    ON CONFLICT(work_item_id,attempt) DO NOTHING;
    SELECT * INTO STRICT v_existing FROM migration.region_talk_heavy_stage_result_artifact
      WHERE work_item_id=v_work.work_item_id AND attempt=(v_direct->>'attempt')::integer;
    IF v_existing.stage<>v_work.stage OR v_existing.subject_id<>v_work.subject_id
       OR v_existing.work_input_fingerprint<>v_work.input_fingerprint
       OR v_existing.enrichment_sha256<>v_private->>'enrichment_sha256'
       OR v_existing.rich_input_fingerprint<>v_private->>'input_fingerprint'
       OR v_existing.result_sha256<>v_private->>'result_sha256'
       OR v_existing.result_data IS DISTINCT FROM v_private->'result_data' THEN
      RAISE EXCEPTION USING ERRCODE='23505',MESSAGE='private heavy result replay differs';
    END IF;
  ELSIF requested_request->'private_result' IS NOT NULL AND
        requested_request->'private_result'<>'null'::jsonb THEN
    RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='non-successful or non-heavy result cannot carry private output';
  END IF;
  v_receipt:=migration.submit_region_talk_stage_worker_result(
    requested_worker_task_run_id,requested_effect_id,v_direct);
  RETURN v_receipt;
END
$$;

REVOKE ALL ON migration.region_talk_heavy_evidence_pack,
  migration.region_talk_heavy_stage_result_artifact
  FROM PUBLIC,mdh_mcp_reader,mdh_mcp_editor,mdh_region_talk_pipeline;
REVOKE EXECUTE ON FUNCTION migration.region_talk_media_artifact_acquisition_receipt_v2(uuid),
  migration.region_talk_heavy_unavailable_v11(text,text,jsonb,text),
  migration.region_talk_heavy_stage_input_v11(uuid),
  migration.register_region_talk_heavy_evidence_pack(jsonb),
  migration.fetch_region_talk_heavy_stage_input(uuid,uuid,jsonb),
  migration.submit_region_talk_heavy_stage_worker_result(uuid,uuid,jsonb),
  migration.submit_region_talk_stage_worker_result(uuid,uuid,jsonb)
  FROM PUBLIC,mdh_mcp_reader,mdh_mcp_editor,mdh_region_talk_pipeline;
GRANT EXECUTE ON FUNCTION migration.register_region_talk_heavy_evidence_pack(jsonb)
  TO mdh_owner,mdh_master_controller;
GRANT EXECUTE ON FUNCTION migration.fetch_region_talk_heavy_stage_input(uuid,uuid,jsonb),
  migration.submit_region_talk_heavy_stage_worker_result(uuid,uuid,jsonb)
  TO mdh_region_talk_pipeline;

UPDATE hub.canonical_state SET schema_revision=32,updated_at=clock_timestamp()
WHERE singleton=true;
