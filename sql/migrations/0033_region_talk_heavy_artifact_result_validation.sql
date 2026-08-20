-- Region Talk v12: admit an exact private heavy result digest as the public
-- metadata artifact while preserving all v9/v10 guard and dependency checks.
-- Prior migrations remain immutable.

ALTER FUNCTION migration.region_talk_stage_result_valid_v9(
  text,text,bigint,uuid,bigint,jsonb,jsonb,text,jsonb,text)
  RENAME TO region_talk_stage_result_valid_v10_metadata_only;

CREATE FUNCTION migration.region_talk_stage_result_valid_v9(
  requested_stage text,requested_contract text,requested_canonical_revision bigint,
  requested_master_instance_id uuid,requested_epoch bigint,requested_input_data jsonb,
  requested_upstream_results jsonb,requested_result_status text,requested_metadata jsonb,
  requested_result_sha256 text
) RETURNS boolean
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path=pg_catalog
AS $$
DECLARE v_metadata jsonb; v_metadata_sha text;
BEGIN
  IF requested_result_status='SUCCEEDED'
     AND requested_stage IN('image_scoring','final_verifier','writer') THEN
    IF requested_metadata->>'artifact_sha256' !~ '^[a-f0-9]{64}$'
       OR requested_metadata->>'artifact_sha256'<>requested_result_sha256 THEN
      RETURN false;
    END IF;
    -- The frozen v9 semantic validator deliberately admitted metadata-only
    -- results.  Normalize only its artifact field/result digest for that
    -- semantic check; the exact rich digest remains present in the stored
    -- metadata/result and is independently bound by 0032's private table.
    v_metadata:=jsonb_set(requested_metadata,'{artifact_sha256}','null'::jsonb);
    v_metadata_sha:=migration.region_talk_json_sha256(v_metadata);
    RETURN migration.region_talk_stage_result_valid_v10_metadata_only(
      requested_stage,requested_contract,requested_canonical_revision,
      requested_master_instance_id,requested_epoch,requested_input_data,
      requested_upstream_results,requested_result_status,v_metadata,v_metadata_sha);
  END IF;
  RETURN migration.region_talk_stage_result_valid_v10_metadata_only(
    requested_stage,requested_contract,requested_canonical_revision,
    requested_master_instance_id,requested_epoch,requested_input_data,
    requested_upstream_results,requested_result_status,requested_metadata,
    requested_result_sha256);
END
$$;

REVOKE EXECUTE ON FUNCTION migration.region_talk_stage_result_valid_v10_metadata_only(
  text,text,bigint,uuid,bigint,jsonb,jsonb,text,jsonb,text),
  migration.region_talk_stage_result_valid_v9(
  text,text,bigint,uuid,bigint,jsonb,jsonb,text,jsonb,text)
  FROM PUBLIC,mdh_mcp_reader,mdh_mcp_editor,mdh_region_talk_pipeline;

-- Validate the private payload against the current server-built evidence pack.
-- This function intentionally duplicates the closed Pydantic boundary in SQL:
-- the database is an authority boundary and cannot trust that a caller used the
-- Python client before invoking the SECURITY DEFINER submit function.
CREATE FUNCTION migration.region_talk_json_object_key_count(requested_value jsonb)
RETURNS integer
LANGUAGE sql IMMUTABLE STRICT SECURITY DEFINER SET search_path=pg_catalog
AS $$ SELECT count(*)::integer FROM jsonb_object_keys(requested_value) $$;

CREATE FUNCTION migration.region_talk_heavy_result_valid_v12(
  requested_stage text,requested_result jsonb,requested_current jsonb,
  requested_metadata jsonb
) RETURNS boolean
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path=pg_catalog
AS $$
DECLARE
  v_input jsonb:=requested_current->'heavy_input';
  v_dag jsonb:=requested_current->'dag_input';
  v_pin jsonb:=requested_current#>'{dag_input,runtime_pin}';
  v_metrics jsonb:=requested_metadata->'metrics';
  v_expected jsonb; v_item jsonb; v_nested jsonb; v_body text;
BEGIN
  IF requested_stage NOT IN('image_scoring','final_verifier','writer')
     OR requested_current->>'status'<>'READY'
     OR jsonb_typeof(requested_result)<>'object'
     OR requested_result->'publication_dispatch' IS DISTINCT FROM 'false'::jsonb
     OR requested_result->'notification_dispatch' IS DISTINCT FROM 'false'::jsonb
     OR requested_result->>'input_fingerprint'<>v_input->>'input_fingerprint'
     OR requested_result->>'candidate_revision_fingerprint'<>
        v_input->>'candidate_revision_fingerprint'
     OR requested_result->>'producer_exact_id'<>v_pin->>'producer_exact_id'
     OR requested_result->>'producer_exact_id'<>requested_metadata->>'producer_exact_id'
     OR requested_result->>'result_sha256'<>requested_metadata->>'artifact_sha256'
     OR requested_result->>'result_sha256' !~ '^[a-f0-9]{64}$'
     OR migration.region_talk_json_sha256(requested_result-'result_sha256')<>
        requested_result->>'result_sha256'
     OR jsonb_typeof(v_metrics)<>'object' THEN
    RETURN false;
  END IF;
  v_expected:=jsonb_build_object(
    'model_id',v_pin->>'model_id','model_revision',v_pin->>'model_revision',
    'encoder_contract',v_pin->>'encoder_contract',
    'asset_manifest_sha256',v_pin->>'asset_manifest_sha256',
    'runtime_source_sha256',v_pin->>'runtime_source_sha256',
    'provider_image_identity',v_pin->>'provider_image_identity',
    'provider_image_source_commit',v_pin->>'provider_image_source_commit',
    'pin_sha256',v_pin->>'pin_sha256');

  IF requested_stage='image_scoring' THEN
    IF migration.region_talk_json_object_key_count(requested_result)<>13
       OR requested_result - ARRAY['schema_version','input_fingerprint',
         'candidate_revision_fingerprint','media_manifest_sha256','producer_exact_id',
         'decision','reason_codes','frames','selected_media_ids','visual_adjudication',
         'publication_dispatch','notification_dispatch','result_sha256']::text[]<>'{}'::jsonb
       OR requested_result->>'schema_version'<>'region-talk-image-scoring-result.v1'
       OR requested_result->>'media_manifest_sha256'<>
          v_input#>>'{artifact_manifest,manifest_sha256}'
       OR requested_result->>'decision' NOT IN
          ('legacy_auto_accept','vlm_visual_accept','needs_visual_review')
       OR jsonb_typeof(requested_result->'reason_codes')<>'array'
       OR jsonb_array_length(requested_result->'reason_codes') NOT BETWEEN 1 AND 20
       OR EXISTS(SELECT 1 FROM jsonb_array_elements(requested_result->'reason_codes') value
                 WHERE jsonb_typeof(value)<>'string' OR length(value#>>'{}') NOT BETWEEN 1 AND 100)
       OR jsonb_typeof(requested_result->'frames')<>'array'
       OR jsonb_array_length(requested_result->'frames') NOT BETWEEN 1 AND 20
       OR jsonb_array_length(requested_result->'frames')<>
          jsonb_array_length(v_input#>'{artifact_manifest,items}')
       OR jsonb_typeof(requested_result->'selected_media_ids')<>'array'
       OR jsonb_array_length(requested_result->'selected_media_ids')>6 THEN RETURN false;
    END IF;
    FOR v_item IN SELECT value FROM jsonb_array_elements(requested_result->'frames') LOOP
      IF migration.region_talk_json_object_key_count(v_item)<>11
         OR v_item - ARRAY['media_id','artifact_sha256','content_text_sha256',
           'scorer_request_fingerprint','cv_overall_media_score','technical_quality_score',
           'clip_visual_fit_score','laion_aesthetic_score','nima_quality_score',
           'overall_media_score','model_bundle_sha256']::text[]<>'{}'::jsonb
         OR jsonb_typeof(v_item->'media_id')<>'string'
         OR jsonb_typeof(v_item->'cv_overall_media_score')<>'number'
         OR jsonb_typeof(v_item->'technical_quality_score')<>'number'
         OR jsonb_typeof(v_item->'clip_visual_fit_score')<>'number'
         OR jsonb_typeof(v_item->'laion_aesthetic_score')<>'number'
         OR jsonb_typeof(v_item->'nima_quality_score')<>'number'
         OR jsonb_typeof(v_item->'overall_media_score')<>'number'
         OR (v_item->>'cv_overall_media_score')::numeric NOT BETWEEN 0 AND 1
         OR (v_item->>'technical_quality_score')::numeric NOT BETWEEN 0 AND 1
         OR (v_item->>'clip_visual_fit_score')::numeric NOT BETWEEN 0 AND 1
         OR (v_item->>'laion_aesthetic_score')::numeric NOT BETWEEN 0 AND 1
         OR (v_item->>'nima_quality_score')::numeric NOT BETWEEN 0 AND 1
         OR (v_item->>'overall_media_score')::numeric NOT BETWEEN 0 AND 1
         OR v_item->>'content_text_sha256'<>v_input#>>'{content,text_sha256}'
         OR v_item->>'model_bundle_sha256'<>v_input#>>'{policy,model_bundle_sha256}'
         OR v_item->>'scorer_request_fingerprint' !~ '^[a-f0-9]{64}$'
         OR NOT EXISTS(SELECT 1 FROM jsonb_array_elements(
              v_input#>'{artifact_manifest,items}') artifact
              WHERE artifact->>'source_media_id'=v_item->>'media_id'
                AND artifact->>'artifact_sha256'=v_item->>'artifact_sha256') THEN RETURN false;
      END IF;
    END LOOP;
    IF EXISTS(SELECT 1 FROM jsonb_array_elements(requested_result->'selected_media_ids') selected
       WHERE jsonb_typeof(selected)<>'string' OR NOT EXISTS(
         SELECT 1 FROM jsonb_array_elements(requested_result->'frames') frame
         WHERE frame->>'media_id'=selected#>>'{}'))
       OR (requested_result->>'decision'='legacy_auto_accept' AND
           jsonb_array_length(requested_result->'selected_media_ids')=0)
       OR (requested_result->>'decision'='needs_visual_review' AND
           jsonb_array_length(requested_result->'selected_media_ids')<>0) THEN RETURN false;
    END IF;
    IF requested_result->>'decision'='vlm_visual_accept' THEN
      v_nested:=requested_result->'visual_adjudication';
      IF jsonb_typeof(v_nested)<>'object'
         OR migration.region_talk_json_object_key_count(v_nested)<>7
         OR v_nested - ARRAY['decision','article_association_supported','selected_media_ids',
           'reason_code','request_fingerprint','model_id','producer_exact_id']::text[]<>'{}'::jsonb
         OR v_nested->>'decision'<>'accept'
         OR v_nested->'article_association_supported' IS DISTINCT FROM 'true'::jsonb
         OR v_nested->'selected_media_ids' IS DISTINCT FROM requested_result->'selected_media_ids'
         OR v_nested->>'request_fingerprint' !~ '^[a-f0-9]{64}$'
         OR coalesce(length(v_nested->>'model_id'),0) NOT BETWEEN 1 AND 200
         OR coalesce(length(v_nested->>'producer_exact_id'),0) NOT BETWEEN 1 AND 500 THEN RETURN false;
      END IF;
    ELSIF requested_result->'visual_adjudication' IS NOT NULL AND
          requested_result->'visual_adjudication'<>'null'::jsonb THEN RETURN false;
    END IF;
    SELECT value INTO v_item FROM jsonb_array_elements(requested_result->'frames') LIMIT 1;
    v_expected:=v_expected||jsonb_build_object(
      'schema_version','region-talk.image-diagnostic-result.v1',
      'decision',CASE WHEN requested_result->>'decision' IN
        ('legacy_auto_accept','vlm_visual_accept') THEN 'accept' ELSE 'needs_review' END,
      'actual_image',true,'postcard_score',v_item->'overall_media_score',
      'input_artifact_sha256',v_dag->>'artifact_sha256');

  ELSIF requested_stage='final_verifier' THEN
    IF migration.region_talk_json_object_key_count(requested_result)<>15
       OR requested_result - ARRAY['schema_version','input_fingerprint',
         'candidate_revision_fingerprint','fact_pack_sha256','source_fingerprint',
         'image_result_sha256','producer_exact_id','decision','reason_codes','grounding',
         'request_fingerprint','model_id','publication_dispatch','notification_dispatch',
         'result_sha256']::text[]<>'{}'::jsonb
       OR requested_result->>'schema_version'<>'region-talk-final-verifier-result.v1'
       OR requested_result->>'fact_pack_sha256'<>v_input#>>'{fact_pack,fact_pack_sha256}'
       OR requested_result->>'source_fingerprint'<>v_input#>>'{source,source_fingerprint}'
       OR requested_result->>'image_result_sha256'<>v_input->>'image_result_sha256'
       OR requested_result->>'model_id'<>v_input#>>'{policy,model_id}'
       OR requested_result->>'decision' NOT IN('accept','reject','needs_review')
       OR requested_result->>'request_fingerprint' !~ '^[a-f0-9]{64}$'
       OR jsonb_typeof(requested_result->'reason_codes')<>'array'
       OR jsonb_array_length(requested_result->'reason_codes') NOT BETWEEN 1 AND 20
       OR jsonb_typeof(requested_result->'grounding')<>'array'
       OR jsonb_array_length(requested_result->'grounding')>20
       OR (requested_result->>'decision'='accept' AND
           jsonb_array_length(requested_result->'grounding')=0) THEN RETURN false;
    END IF;
    FOR v_item IN SELECT value FROM jsonb_array_elements(requested_result->'grounding') LOOP
      IF migration.region_talk_json_object_key_count(v_item)<>2
         OR v_item - ARRAY['claim','fact_ids']::text[]<>'{}'::jsonb
         OR coalesce(length(v_item->>'claim'),0) NOT BETWEEN 1 AND 1000
         OR jsonb_typeof(v_item->'fact_ids')<>'array'
         OR jsonb_array_length(v_item->'fact_ids') NOT BETWEEN 1 AND 20
         OR EXISTS(SELECT 1 FROM jsonb_array_elements(v_item->'fact_ids') fact_id
           WHERE NOT EXISTS(SELECT 1 FROM jsonb_array_elements(v_input#>'{fact_pack,facts}') fact
             WHERE fact->>'fact_id'=fact_id#>>'{}')) THEN RETURN false;
      END IF;
    END LOOP;
    v_expected:=v_expected||jsonb_build_object(
      'schema_version','region-talk.final-verifier-result.v1',
      'decision',CASE requested_result->>'decision' WHEN 'accept' THEN 'PASS'
        WHEN 'needs_review' THEN 'REVIEW' ELSE 'REJECT' END,
      'reason_codes',requested_result->'reason_codes',
      'vector_result_sha256',v_dag->>'vector_result_sha256',
      'image_result_sha256',v_dag->>'image_result_sha256');

  ELSE
    IF migration.region_talk_json_object_key_count(requested_result)<>20
       OR requested_result - ARRAY['schema_version','input_fingerprint',
         'candidate_revision_fingerprint','fact_pack_sha256','source_profile_fingerprint',
         'final_result_sha256','producer_exact_id','status','title','paragraph_one',
         'paragraph_two','grounding','strategy','critic','rewrite_count','request_fingerprint',
         'model_id','publication_dispatch','notification_dispatch','result_sha256']::text[]<>'{}'::jsonb
       OR requested_result->>'schema_version'<>'region-talk-writer-result.v1'
       OR requested_result->>'fact_pack_sha256'<>v_input#>>'{fact_pack,fact_pack_sha256}'
       OR requested_result->>'source_profile_fingerprint'<>
          v_input#>>'{source_profile,profile_fingerprint}'
       OR requested_result->>'final_result_sha256'<>v_input->>'final_result_sha256'
       OR requested_result->>'model_id'<>v_input#>>'{policy,model_id}'
       OR requested_result->>'status'<>'ready_for_operator_review'
       OR coalesce(length(requested_result->>'title'),0) NOT BETWEEN 1 AND 500
       OR coalesce(length(requested_result->>'paragraph_one'),0) NOT BETWEEN 1 AND 2000
       OR coalesce(length(requested_result->>'paragraph_two'),0) NOT BETWEEN 1 AND 2000
       OR requested_result->>'request_fingerprint' !~ '^[a-f0-9]{64}$'
       OR jsonb_typeof(requested_result->'rewrite_count')<>'number'
       OR (requested_result->>'rewrite_count')::numeric NOT BETWEEN 0 AND 1
       OR jsonb_typeof(requested_result->'grounding')<>'array'
       OR jsonb_array_length(requested_result->'grounding') NOT BETWEEN 1 AND 30
       OR jsonb_typeof(requested_result->'strategy')<>'object'
       OR migration.region_talk_json_object_key_count(requested_result->'strategy')<>4
       OR (requested_result->'strategy') - ARRAY['angle','current_hook_fact_ids',
          'source_value_fact_ids','visual_hook_media_ids']::text[]<>'{}'::jsonb
       OR jsonb_typeof(requested_result->'critic')<>'object'
       OR migration.region_talk_json_object_key_count(requested_result->'critic')<>2
       OR (requested_result->'critic') - ARRAY['decision','defects']::text[]<>'{}'::jsonb
       OR requested_result#>>'{critic,decision}'<>'pass'
       OR requested_result#>'{critic,defects}'<>'[]'::jsonb THEN RETURN false;
    END IF;
    FOR v_item IN SELECT value FROM jsonb_array_elements(requested_result->'grounding') LOOP
      IF migration.region_talk_json_object_key_count(v_item)<>2
         OR v_item - ARRAY['claim','fact_ids']::text[]<>'{}'::jsonb
         OR coalesce(length(v_item->>'claim'),0) NOT BETWEEN 1 AND 1000
         OR jsonb_typeof(v_item->'fact_ids')<>'array'
         OR jsonb_array_length(v_item->'fact_ids') NOT BETWEEN 1 AND 20
         OR EXISTS(SELECT 1 FROM jsonb_array_elements(v_item->'fact_ids') fact_id
           WHERE NOT EXISTS(SELECT 1 FROM jsonb_array_elements(v_input#>'{fact_pack,facts}') fact
             WHERE fact->>'fact_id'=fact_id#>>'{}')) THEN RETURN false;
      END IF;
    END LOOP;
    FOREACH v_nested IN ARRAY ARRAY[
      requested_result#>'{strategy,current_hook_fact_ids}',
      requested_result#>'{strategy,source_value_fact_ids}'] LOOP
      IF jsonb_typeof(v_nested)<>'array' OR jsonb_array_length(v_nested) NOT BETWEEN 1 AND 10
         OR EXISTS(SELECT 1 FROM jsonb_array_elements(v_nested) fact_id
           WHERE NOT EXISTS(SELECT 1 FROM jsonb_array_elements(v_input#>'{fact_pack,facts}') fact
             WHERE fact->>'fact_id'=fact_id#>>'{}')) THEN RETURN false;
      END IF;
    END LOOP;
    IF jsonb_typeof(requested_result#>'{strategy,visual_hook_media_ids}')<>'array'
       OR jsonb_array_length(requested_result#>'{strategy,visual_hook_media_ids}')>6
       OR EXISTS(SELECT 1 FROM jsonb_array_elements(
          requested_result#>'{strategy,visual_hook_media_ids}') media_id
          WHERE NOT EXISTS(SELECT 1 FROM jsonb_array_elements(
            v_input#>'{image_result,selected_media_ids}') selected
            WHERE selected=media_id)) THEN RETURN false;
    END IF;
    v_body:=(requested_result->>'paragraph_one')||E'\n\n'||
      (requested_result->>'paragraph_two');
    v_expected:=v_expected||jsonb_build_object(
      'schema_version','region-talk.writer-result.v1',
      'draft_sha256',migration.region_talk_json_sha256(jsonb_build_object(
        'title',requested_result->>'title','body',v_body)),
      'title_sha256',encode(sha256(convert_to(requested_result->>'title','UTF8')),'hex'),
      'body_sha256',encode(sha256(convert_to(v_body,'UTF8')),'hex'),
      'character_count',length(requested_result->>'title')+length(v_body),
      'final_result_sha256',v_dag->>'final_result_sha256');
  END IF;
  RETURN v_metrics=v_expected;
EXCEPTION WHEN data_exception OR invalid_text_representation THEN
  RETURN false;
END
$$;

ALTER FUNCTION migration.submit_region_talk_heavy_stage_worker_result(uuid,uuid,jsonb)
  RENAME TO submit_region_talk_heavy_stage_worker_result_v11_contract_unverified;

CREATE FUNCTION migration.submit_region_talk_heavy_stage_worker_result(
  requested_worker_task_run_id uuid,requested_effect_id uuid,requested_request jsonb
) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog
AS $$
DECLARE v_direct jsonb:=requested_request->'direct_result';
  v_private jsonb:=requested_request->'private_result';
  v_work migration.region_talk_stage_work_input_v9%ROWTYPE; v_current jsonb;
BEGIN
  IF jsonb_typeof(v_direct)='object' AND v_direct->>'result_status'='SUCCEEDED' THEN
    SELECT * INTO v_work FROM migration.region_talk_stage_work_input_v9 input
      WHERE input.work_item_id=(v_direct->>'work_item_id')::uuid;
    IF FOUND AND v_work.stage IN('image_scoring','final_verifier','writer') THEN
      v_current:=migration.region_talk_heavy_stage_input_v11(v_work.work_item_id);
      IF jsonb_typeof(v_private)<>'object'
         OR NOT migration.region_talk_heavy_result_valid_v12(
           v_work.stage,v_private->'result_data',v_current,v_direct->'result_metadata') THEN
        RAISE EXCEPTION USING ERRCODE='22023',
          MESSAGE='private heavy result violates exact stage contract or guard metrics';
      END IF;
    END IF;
  END IF;
  RETURN migration.submit_region_talk_heavy_stage_worker_result_v11_contract_unverified(
    requested_worker_task_run_id,requested_effect_id,requested_request);
END
$$;

REVOKE EXECUTE ON FUNCTION migration.region_talk_json_object_key_count(jsonb),
  migration.region_talk_heavy_result_valid_v12(
  text,jsonb,jsonb,jsonb),
  migration.submit_region_talk_heavy_stage_worker_result_v11_contract_unverified(
  uuid,uuid,jsonb),migration.submit_region_talk_heavy_stage_worker_result(uuid,uuid,jsonb)
  FROM PUBLIC,mdh_mcp_reader,mdh_mcp_editor,mdh_region_talk_pipeline;
GRANT EXECUTE ON FUNCTION migration.submit_region_talk_heavy_stage_worker_result(uuid,uuid,jsonb)
  TO mdh_region_talk_pipeline;

UPDATE hub.canonical_state SET schema_revision=33,updated_at=clock_timestamp()
WHERE singleton=true;
