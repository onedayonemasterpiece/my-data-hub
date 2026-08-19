-- Region Talk v2 lossless direct snapshot and sanitized read projections.
-- Source rows travel only from the dedicated Kaggle pipeline to the ACTIVE
-- PostgreSQL master.  The devstand never receives a row payload or PGDATA.

CREATE TABLE migration.region_talk_direct_snapshot (
    export_batch_id             uuid PRIMARY KEY
                                REFERENCES migration.export_batch(export_batch_id) ON DELETE RESTRICT,
    task_run_id                 uuid NOT NULL UNIQUE,
    master_instance_id          uuid NOT NULL,
    master_epoch                bigint NOT NULL CHECK (master_epoch >= 1),
    request_sha256              text NOT NULL CHECK (request_sha256 ~ '^[a-f0-9]{64}$'),
    pass_a_logical_sha256       text NOT NULL CHECK (pass_a_logical_sha256 ~ '^[a-f0-9]{64}$'),
    pass_b_logical_sha256       text CHECK (pass_b_logical_sha256 IS NULL OR pass_b_logical_sha256 ~ '^[a-f0-9]{64}$'),
    state                       text NOT NULL DEFAULT 'landing'
                                CHECK (state IN ('landing','complete','complete_with_quarantine','failed')),
    publication_effects_enabled boolean NOT NULL DEFAULT false CHECK (NOT publication_effects_enabled),
    failure_code                text CHECK (failure_code IS NULL OR octet_length(failure_code) BETWEEN 1 AND 128),
    receipt                     jsonb,
    created_at                  timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at                timestamptz,
    CHECK ((state = 'landing') = (completed_at IS NULL)),
    CHECK ((state = 'failed') = (failure_code IS NOT NULL))
);

CREATE TABLE migration.region_talk_direct_snapshot_table (
    export_batch_id        uuid NOT NULL
                           REFERENCES migration.region_talk_direct_snapshot(export_batch_id) ON DELETE RESTRICT,
    source_table           text NOT NULL,
    ordinal                smallint NOT NULL CHECK (ordinal BETWEEN 1 AND 5),
    pass_a_row_count       bigint NOT NULL CHECK (pass_a_row_count >= 0),
    pass_a_logical_sha256  text NOT NULL CHECK (pass_a_logical_sha256 ~ '^[a-f0-9]{64}$'),
    pass_b_row_count       bigint CHECK (pass_b_row_count IS NULL OR pass_b_row_count >= 0),
    pass_b_logical_sha256  text CHECK (pass_b_logical_sha256 IS NULL OR pass_b_logical_sha256 ~ '^[a-f0-9]{64}$'),
    PRIMARY KEY (export_batch_id, source_table),
    UNIQUE (export_batch_id, ordinal)
);

CREATE TABLE migration.region_talk_direct_snapshot_page (
    export_batch_id        uuid NOT NULL
                           REFERENCES migration.region_talk_direct_snapshot(export_batch_id) ON DELETE RESTRICT,
    source_table           text NOT NULL,
    page_number            integer NOT NULL CHECK (page_number >= 1),
    first_source_pk        text NOT NULL,
    last_source_pk         text NOT NULL,
    row_count              integer NOT NULL CHECK (row_count BETWEEN 1 AND 500),
    logical_sha256         text NOT NULL CHECK (logical_sha256 ~ '^[a-f0-9]{64}$'),
    landed_at              timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (export_batch_id, source_table, page_number),
    FOREIGN KEY (export_batch_id, source_table)
        REFERENCES migration.region_talk_direct_snapshot_table(export_batch_id, source_table) ON DELETE RESTRICT,
    CHECK (first_source_pk <= last_source_pk)
);

-- Fixed typed projections.  Raw JSON remains private in migration.raw_record;
-- MCP readers receive only the explicit columns in the views below.
CREATE TABLE region_talk.imported_content_v2 (
    item_id                uuid PRIMARY KEY,
    raw_record_id          uuid NOT NULL UNIQUE REFERENCES migration.raw_record(raw_record_id) ON DELETE RESTRICT,
    content_family         text NOT NULL CHECK (content_family IN (
                               'article','post','publication_candidate','discovery_opportunity'
                           )),
    source_pk              text NOT NULL,
    row_kind               text NOT NULL,
    title                  text,
    body_text              text,
    summary                text,
    exact_url              text,
    platform               text,
    external_id            text,
    category               text,
    status                 text,
    source_updated_at      timestamptz,
    imported_at            timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX imported_content_v2_family_status_idx
    ON region_talk.imported_content_v2(content_family, status, imported_at DESC);
CREATE INDEX imported_content_v2_title_search_idx
    ON region_talk.imported_content_v2 USING gin(to_tsvector('pg_catalog.russian', coalesce(title,'') || ' ' || coalesce(summary,'')));

CREATE TABLE region_talk.imported_queue_v2 (
    item_id                uuid PRIMARY KEY,
    raw_record_id          uuid NOT NULL UNIQUE REFERENCES migration.raw_record(raw_record_id) ON DELETE RESTRICT,
    queue_family           text NOT NULL,
    source_pk              text NOT NULL,
    row_kind               text NOT NULL,
    source_ref             text,
    lane                   text,
    status                 text,
    priority_text          text,
    available_at           timestamptz,
    source_updated_at      timestamptz,
    imported_at            timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX imported_queue_v2_family_status_idx
    ON region_talk.imported_queue_v2(queue_family, status, imported_at DESC);

CREATE TABLE region_talk.imported_llm_request_v2 (
    item_id                uuid PRIMARY KEY,
    raw_record_id          uuid NOT NULL UNIQUE REFERENCES migration.raw_record(raw_record_id) ON DELETE RESTRICT,
    source_pk              text NOT NULL,
    row_kind               text NOT NULL,
    request_key            text,
    model_key              text,
    status                 text,
    prompt_sha256          text,
    response_sha256        text,
    budget_units_text      text,
    source_updated_at      timestamptz,
    imported_at            timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE region_talk.imported_discovery_run_v2 (
    item_id                uuid PRIMARY KEY,
    raw_record_id          uuid NOT NULL UNIQUE REFERENCES migration.raw_record(raw_record_id) ON DELETE RESTRICT,
    run_uid                text NOT NULL UNIQUE,
    imported_db_run_id     text,
    generated_at_text      text,
    imported_at_text       text,
    source_updated_at      timestamptz,
    imported_at            timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE OR REPLACE FUNCTION migration.region_talk_direct_body(payload jsonb)
RETURNS jsonb
LANGUAGE sql
IMMUTABLE
SET search_path = pg_catalog
AS $$
SELECT CASE
    WHEN jsonb_typeof(payload->'payload_decoded') = 'object' THEN payload->'payload_decoded'
    WHEN jsonb_typeof(payload->'payload_json') = 'object' THEN payload->'payload_json'
    ELSE payload
END
$$;

CREATE OR REPLACE FUNCTION migration.normalize_region_talk_direct_record(
    requested_raw_record_id uuid,
    requested_source_table text,
    requested_source_pk text,
    requested_row_kind text,
    requested_source_updated_at timestamptz,
    requested_payload jsonb
) RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
    body jsonb := migration.region_talk_direct_body(requested_payload);
    family text;
    disposition text := 'retained_raw';
    reason text := 'valid_unsupported_kind_v2';
    refs jsonb := '[]'::jsonb;
BEGIN
    IF jsonb_typeof(requested_payload) <> 'object' OR requested_row_kind = 'malformed_compact_kind' THEN
        disposition := 'quarantined';
        reason := 'malformed_source_record_v2';
    ELSIF requested_row_kind IN (
        'external_publication_intake_item','publication_candidate_item',
        'processed_post_item','post_live_item','acq_discovery_opportunity_item'
    ) THEN
        family := CASE requested_row_kind
            WHEN 'external_publication_intake_item' THEN 'article'
            WHEN 'publication_candidate_item' THEN 'publication_candidate'
            WHEN 'acq_discovery_opportunity_item' THEN 'discovery_opportunity'
            ELSE 'post'
        END;
        INSERT INTO region_talk.imported_content_v2 (
            item_id,raw_record_id,content_family,source_pk,row_kind,title,body_text,summary,
            exact_url,platform,external_id,category,status,source_updated_at
        ) VALUES (
            requested_raw_record_id,requested_raw_record_id,family,requested_source_pk,requested_row_kind,
            coalesce(body->>'title',body->>'name',requested_payload->>'title'),
            coalesce(body->>'body',body->>'text',body->>'content'),
            coalesce(body->>'summary',body->>'description',body->>'excerpt'),
            coalesce(body->>'exact_url',body->>'url',body->>'source_url',requested_payload->>'context_url'),
            coalesce(body->>'platform',requested_payload->>'platform'),
            coalesce(body->>'external_id',body->>'post_id',requested_payload->>'surface_external_id'),
            coalesce(body->>'category',body->>'content_category',body->>'candidate_type'),
            coalesce(body->>'status',body->>'state'),
            requested_source_updated_at
        ) ON CONFLICT (raw_record_id) DO NOTHING;
        disposition := 'normalized';
        reason := 'typed_content_projection_v2';
        refs := jsonb_build_array(jsonb_build_object(
            'table','region_talk.imported_content_v2','item_id',requested_raw_record_id
        ));
    ELSIF requested_row_kind IN (
        'source_queue_item','source_candidate_item','source_status_item','source_edge_item',
        'post_link_queue_item','candidate_memory_item','image_queue_item',
        'publication_schedule_item','publication_schedule_snapshot',
        'external_publication_review_item','external_publication_review_state_item',
        'external_publication_review_event_item','publication_review_state_item',
        'publication_review_event_item','publication_delivery_item','operator_feedback_item',
        'operator_feedback_latest_item','external_publication_intake_observation_item',
        'external_publication_seen_item','queue_cursor','queue_metrics',
        'state_snapshot','run_state_snapshot','acq_discovery_surface_item'
    ) THEN
        family := CASE requested_row_kind
            WHEN 'source_queue_item' THEN 'source_frontier'
            WHEN 'source_candidate_item' THEN 'source_candidate'
            WHEN 'source_status_item' THEN 'source_status'
            WHEN 'source_edge_item' THEN 'source_edge'
            WHEN 'post_link_queue_item' THEN 'post_intake'
            WHEN 'candidate_memory_item' THEN 'candidate_memory'
            WHEN 'image_queue_item' THEN 'image_processing'
            WHEN 'publication_schedule_item' THEN 'publication_schedule'
            WHEN 'publication_schedule_snapshot' THEN 'publication_schedule'
            WHEN 'external_publication_review_item' THEN 'review'
            WHEN 'external_publication_review_state_item' THEN 'review_state'
            WHEN 'external_publication_review_event_item' THEN 'review_event'
            WHEN 'publication_review_state_item' THEN 'review_state'
            WHEN 'publication_review_event_item' THEN 'review_event'
            WHEN 'publication_delivery_item' THEN 'delivery_history'
            WHEN 'operator_feedback_item' THEN 'operator_feedback'
            WHEN 'operator_feedback_latest_item' THEN 'operator_feedback_latest'
            WHEN 'external_publication_intake_observation_item' THEN 'article_observation'
            WHEN 'external_publication_seen_item' THEN 'article_seen'
            WHEN 'queue_cursor' THEN 'cursor'
            WHEN 'queue_metrics' THEN 'cursor_metrics'
            WHEN 'state_snapshot' THEN 'state_snapshot'
            WHEN 'run_state_snapshot' THEN 'run_state_snapshot'
            ELSE 'discovery_surface'
        END;
        INSERT INTO region_talk.imported_queue_v2 (
            item_id,raw_record_id,queue_family,source_pk,row_kind,source_ref,lane,status,
            priority_text,available_at,source_updated_at
        ) VALUES (
            requested_raw_record_id,requested_raw_record_id,family,requested_source_pk,requested_row_kind,
            coalesce(body->>'source_ref',body->>'source_id',body->>'url',requested_payload->>'url'),
            coalesce(body->>'lane',body->>'priority_lane',body->>'queue'),
            coalesce(body->>'status',body->>'state',requested_payload->>'status'),
            coalesce(body->>'priority',body->>'priority_score'),
            NULL,
            requested_source_updated_at
        ) ON CONFLICT (raw_record_id) DO NOTHING;
        disposition := 'normalized';
        reason := 'typed_queue_projection_v2';
        refs := jsonb_build_array(jsonb_build_object(
            'table','region_talk.imported_queue_v2','item_id',requested_raw_record_id
        ));
    ELSIF requested_row_kind IN ('region_talk_llm_request_item','region_talk_llm_budget_item') THEN
        INSERT INTO region_talk.imported_llm_request_v2 (
            item_id,raw_record_id,source_pk,row_kind,request_key,model_key,status,
            prompt_sha256,response_sha256,budget_units_text,source_updated_at
        ) VALUES (
            requested_raw_record_id,requested_raw_record_id,requested_source_pk,requested_row_kind,
            coalesce(body->>'request_key',body->>'request_id',body->>'idempotency_key'),
            coalesce(body->>'model_key',body->>'model'),
            coalesce(body->>'status',body->>'state'),
            body->>'prompt_sha256',body->>'response_sha256',
            coalesce(body->>'budget_units',body->>'tokens',body->>'cost'),
            requested_source_updated_at
        ) ON CONFLICT (raw_record_id) DO NOTHING;
        disposition := 'normalized';
        reason := 'typed_llm_idempotency_projection_v2';
        refs := jsonb_build_array(jsonb_build_object(
            'table','region_talk.imported_llm_request_v2','item_id',requested_raw_record_id
        ));
    ELSIF requested_row_kind = 'acq_discovery_run_item' THEN
        INSERT INTO region_talk.imported_discovery_run_v2 (
            item_id,raw_record_id,run_uid,imported_db_run_id,generated_at_text,
            imported_at_text,source_updated_at
        ) VALUES (
            requested_raw_record_id,requested_raw_record_id,requested_source_pk,
            requested_payload->>'imported_db_run_id',requested_payload->>'generated_at',
            requested_payload->>'imported_at',requested_source_updated_at
        ) ON CONFLICT (raw_record_id) DO NOTHING;
        disposition := 'normalized';
        reason := 'typed_discovery_run_projection_v2';
        refs := jsonb_build_array(jsonb_build_object(
            'table','region_talk.imported_discovery_run_v2','item_id',requested_raw_record_id
        ));
    ELSIF requested_row_kind = 'external_blogger_evidence_item' THEN
        -- Bloggers are already materialized through the reviewed 266->263
        -- duplicate-resolution path.  The full snapshot records the row but
        -- never creates another actor/profile.
        IF EXISTS (
            SELECT 1 FROM migration.legacy_identity_map legacy
             WHERE legacy.source_system = 'ydb'
               AND legacy.source_table = 'region_talk_external_blogger_evidence'
               AND legacy.source_pk = requested_source_pk
        ) OR EXISTS (
            SELECT 1 FROM region_talk.blogger_profile profile
             WHERE profile.legacy_record_id = requested_source_pk
        ) THEN
            disposition := 'deduplicated';
            reason := 'dedicated_blogger_materialization_reused_v2';
        ELSE
            disposition := 'retained_raw';
            reason := 'awaiting_dedicated_blogger_resolution_v2';
        END IF;
    END IF;

    INSERT INTO migration.row_disposition (
        raw_record_id,mapping_version,disposition,target_refs,reason_code
    ) VALUES (
        requested_raw_record_id,'region-talk-direct-v2',disposition,refs,reason
    ) ON CONFLICT (raw_record_id) DO NOTHING;

    IF NOT EXISTS (
        SELECT 1 FROM migration.row_disposition d
         WHERE d.raw_record_id = requested_raw_record_id
           AND d.mapping_version = 'region-talk-direct-v2'
           AND d.disposition = disposition
           AND d.target_refs = refs
           AND d.reason_code = reason
    ) THEN
        RAISE EXCEPTION USING ERRCODE='23505', MESSAGE='direct row disposition idempotency conflict';
    END IF;
END
$$;

CREATE OR REPLACE FUNCTION migration.assert_region_talk_direct_task(
    requested_export_batch_id uuid,
    requested_task_run_id uuid
) RETURNS migration.region_talk_direct_snapshot
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
    snapshot migration.region_talk_direct_snapshot%ROWTYPE;
    state master_control.epoch_state%ROWTYPE;
    session_is_superuser boolean;
BEGIN
    SELECT rolsuper INTO STRICT session_is_superuser FROM pg_roles WHERE rolname = session_user;
    IF session_is_superuser OR NOT pg_has_role(session_user,'mdh_region_talk_pipeline','member') THEN
        RAISE EXCEPTION USING ERRCODE='42501', MESSAGE='direct snapshot requires exact Region Talk pipeline login';
    END IF;
    PERFORM master_control.assert_session_write_epoch();
    SELECT * INTO STRICT snapshot FROM migration.region_talk_direct_snapshot
     WHERE export_batch_id=requested_export_batch_id AND task_run_id=requested_task_run_id;
    SELECT * INTO STRICT state FROM master_control.epoch_state WHERE singleton=true;
    IF snapshot.master_epoch <> state.current_epoch
       OR snapshot.master_instance_id <> state.master_instance_id
       OR state.gate_state <> 'open'
       OR state.lease_until <= clock_timestamp() THEN
        RAISE EXCEPTION USING ERRCODE='55000', MESSAGE='direct snapshot task is outside ACTIVE epoch';
    END IF;
    RETURN snapshot;
END
$$;

CREATE OR REPLACE FUNCTION migration.begin_region_talk_direct_snapshot(requested_manifest jsonb)
RETURNS uuid
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
    export_id uuid;
    task_id uuid;
    instance_id uuid;
    epoch bigint;
    expected_count bigint;
    observed_count bigint;
    tables jsonb;
    kinds jsonb;
    table_row jsonb;
    kind_row record;
    ordinal integer := 0;
    state master_control.epoch_state%ROWTYPE;
    existing migration.region_talk_direct_snapshot%ROWTYPE;
    existing_batch migration.export_batch%ROWTYPE;
    session_is_superuser boolean;
    expected_names constant text[] := ARRAY[
        'acq_discovery_opportunities','acq_discovery_runs','acq_discovery_surfaces',
        'region_talk_compact_state_kv','region_talk_external_blogger_evidence'
    ];
BEGIN
    SELECT rolsuper INTO STRICT session_is_superuser FROM pg_roles WHERE rolname=session_user;
    IF session_is_superuser OR NOT pg_has_role(session_user,'mdh_region_talk_pipeline','member') THEN
        RAISE EXCEPTION USING ERRCODE='42501', MESSAGE='direct snapshot requires exact Region Talk pipeline login';
    END IF;
    PERFORM master_control.assert_session_write_epoch();
    IF requested_manifest->>'schema_version' <> 'region-talk-direct-snapshot.v2'
       OR requested_manifest->>'publication_effects_enabled' <> 'false'
       OR octet_length(requested_manifest::text) > 1048576 THEN
        RAISE EXCEPTION USING ERRCODE='22023', MESSAGE='invalid direct snapshot manifest';
    END IF;
    BEGIN
        export_id := (requested_manifest->>'export_batch_id')::uuid;
        task_id := (requested_manifest->>'task_run_id')::uuid;
        instance_id := (requested_manifest->>'master_instance_id')::uuid;
        epoch := (requested_manifest->>'master_epoch')::bigint;
        expected_count := (requested_manifest->>'expected_row_count')::bigint;
        tables := requested_manifest->'tables';
        kinds := requested_manifest->'row_kind_counts';
    EXCEPTION WHEN OTHERS THEN
        RAISE EXCEPTION USING ERRCODE='22023', MESSAGE='direct snapshot manifest fields are invalid';
    END;
    IF jsonb_typeof(tables) <> 'array' OR jsonb_array_length(tables) <> 5
       OR jsonb_typeof(kinds) <> 'object' OR expected_count < 0
       OR requested_manifest->>'request_sha256' !~ '^[a-f0-9]{64}$'
       OR requested_manifest->>'logical_sha256' !~ '^[a-f0-9]{64}$'
       OR requested_manifest->>'manifest_sha256' !~ '^[a-f0-9]{64}$' THEN
        RAISE EXCEPTION USING ERRCODE='22023', MESSAGE='direct snapshot inventory is invalid';
    END IF;
    SELECT * INTO STRICT state FROM master_control.epoch_state WHERE singleton=true;
    IF state.gate_state <> 'open' OR state.lease_until <= clock_timestamp()
       OR state.current_epoch <> epoch OR state.master_instance_id <> instance_id THEN
        RAISE EXCEPTION USING ERRCODE='55000', MESSAGE='direct snapshot does not bind ACTIVE epoch';
    END IF;
    SELECT * INTO existing FROM migration.region_talk_direct_snapshot WHERE export_batch_id=export_id;
    IF FOUND THEN
        SELECT * INTO STRICT existing_batch FROM migration.export_batch WHERE export_batch_id=export_id;
        IF existing.task_run_id<>task_id OR existing.master_instance_id<>instance_id
           OR existing.master_epoch<>epoch
           OR existing.request_sha256<>requested_manifest->>'request_sha256'
           OR existing.pass_a_logical_sha256<>requested_manifest->>'logical_sha256'
           OR existing_batch.expected_row_count<>expected_count
           OR existing_batch.manifest_sha256<>requested_manifest->>'manifest_sha256'
           OR existing_batch.source_database<>requested_manifest->>'source_database' THEN
            RAISE EXCEPTION USING ERRCODE='23505', MESSAGE='direct snapshot idempotency conflict';
        END IF;
        RETURN export_id;
    END IF;

    observed_count := 0;
    FOR table_row IN SELECT value FROM jsonb_array_elements(tables) LOOP
        ordinal := ordinal + 1;
        IF table_row->>'source_table' <> expected_names[ordinal]
           OR table_row->>'logical_sha256' !~ '^[a-f0-9]{64}$'
           OR (table_row->>'row_count')::bigint < 0 THEN
            RAISE EXCEPTION USING ERRCODE='22023', MESSAGE='direct snapshot table inventory is invalid';
        END IF;
        observed_count := observed_count + (table_row->>'row_count')::bigint;
    END LOOP;
    IF observed_count <> expected_count THEN
        RAISE EXCEPTION USING ERRCODE='22023', MESSAGE='direct snapshot table total mismatch';
    END IF;
    observed_count := 0;
    FOR kind_row IN SELECT key,value FROM jsonb_each_text(kinds) LOOP
        IF kind_row.key IS NULL OR length(kind_row.key) NOT BETWEEN 1 AND 200
           OR kind_row.key !~ '^[A-Za-z0-9_./:-]+$' OR kind_row.value::bigint < 0 THEN
            RAISE EXCEPTION USING ERRCODE='22023', MESSAGE='direct snapshot kind inventory is invalid';
        END IF;
        observed_count := observed_count + kind_row.value::bigint;
    END LOOP;
    IF observed_count <> expected_count THEN
        RAISE EXCEPTION USING ERRCODE='22023', MESSAGE='direct snapshot kind total mismatch';
    END IF;

    INSERT INTO migration.export_batch (
        export_batch_id,source_system,source_database,source_tables,source_scope,schema_version,
        consistency_mode,expected_row_count,manifest_sha256,logical_sha256,status,metadata
    ) VALUES (
        export_id,'ydb',requested_manifest->>'source_database',
        (SELECT jsonb_agg(entry.value->>'source_table' ORDER BY entry.ordinality)
           FROM jsonb_array_elements(tables) WITH ORDINALITY AS entry(value,ordinality)),
        'region-talk-full-v2','region-talk-direct-snapshot.v2','two_pass_direct',expected_count,
        requested_manifest->>'manifest_sha256',requested_manifest->>'logical_sha256','landing',
        jsonb_build_object('task_run_id',task_id,'master_epoch',epoch,'publication_effects_enabled',false)
    );
    FOR kind_row IN SELECT key,value FROM jsonb_each_text(kinds) LOOP
        INSERT INTO migration.export_batch_kind(export_batch_id,row_kind,expected_row_count)
        VALUES(export_id,kind_row.key,kind_row.value::bigint);
    END LOOP;
    INSERT INTO migration.region_talk_direct_snapshot (
        export_batch_id,task_run_id,master_instance_id,master_epoch,request_sha256,
        pass_a_logical_sha256,publication_effects_enabled
    ) VALUES (
        export_id,task_id,instance_id,epoch,requested_manifest->>'request_sha256',
        requested_manifest->>'logical_sha256',false
    );
    ordinal := 0;
    FOR table_row IN SELECT value FROM jsonb_array_elements(tables) LOOP
        ordinal := ordinal + 1;
        INSERT INTO migration.region_talk_direct_snapshot_table(
            export_batch_id,source_table,ordinal,pass_a_row_count,pass_a_logical_sha256
        ) VALUES (
            export_id,table_row->>'source_table',ordinal,(table_row->>'row_count')::bigint,
            table_row->>'logical_sha256'
        );
    END LOOP;
    RETURN export_id;
END
$$;

CREATE OR REPLACE FUNCTION migration.land_region_talk_direct_page(
    requested_export_batch_id uuid,
    requested_task_run_id uuid,
    requested_page jsonb
) RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
    snapshot migration.region_talk_direct_snapshot%ROWTYPE;
    page_row jsonb;
    payload_text text;
    payload jsonb;
    raw_id uuid;
    v_source_table text := requested_page->>'source_table';
    source_pk text;
    previous_pk text;
    row_kind text;
    payload_sha text;
    logical_sha text;
    source_updated timestamptz;
    page_no integer;
    existing_page migration.region_talk_direct_snapshot_page%ROWTYPE;
    previous_page migration.region_talk_direct_snapshot_page%ROWTYPE;
    existing_raw migration.raw_record%ROWTYPE;
    actual_count integer := 0;
    duplicate_page boolean := false;
BEGIN
    snapshot := migration.assert_region_talk_direct_task(requested_export_batch_id,requested_task_run_id);
    IF requested_page->>'schema_version' <> 'region-talk-direct-page.v2'
       OR jsonb_typeof(requested_page->'rows') <> 'array'
       OR jsonb_array_length(requested_page->'rows') NOT BETWEEN 1 AND 500
       OR octet_length(requested_page::text) > 8388608
       OR requested_page->>'logical_sha256' !~ '^[a-f0-9]{64}$' THEN
        RAISE EXCEPTION USING ERRCODE='22023', MESSAGE='direct snapshot page is invalid';
    END IF;
    BEGIN page_no := (requested_page->>'page_number')::integer;
    EXCEPTION WHEN OTHERS THEN
        RAISE EXCEPTION USING ERRCODE='22023', MESSAGE='direct snapshot page number is invalid';
    END;
    SELECT * INTO existing_page FROM migration.region_talk_direct_snapshot_page
     WHERE export_batch_id=requested_export_batch_id
       AND migration.region_talk_direct_snapshot_page.source_table=v_source_table
       AND page_number=page_no;
    IF FOUND THEN
        IF existing_page.first_source_pk<>requested_page->>'first_source_pk'
           OR existing_page.last_source_pk<>requested_page->>'last_source_pk'
           OR existing_page.row_count<>jsonb_array_length(requested_page->'rows')
           OR existing_page.logical_sha256<>requested_page->>'logical_sha256' THEN
            RAISE EXCEPTION USING ERRCODE='23505', MESSAGE='direct snapshot page idempotency conflict';
        END IF;
        duplicate_page := true;
    END IF;
    IF NOT duplicate_page AND snapshot.state <> 'landing' THEN
        RAISE EXCEPTION USING ERRCODE='55000', MESSAGE='direct snapshot no longer accepts new pages';
    END IF;
    IF NOT duplicate_page THEN
        SELECT * INTO previous_page FROM migration.region_talk_direct_snapshot_page
         WHERE export_batch_id=requested_export_batch_id
           AND migration.region_talk_direct_snapshot_page.source_table=v_source_table
         ORDER BY page_number DESC LIMIT 1;
        IF (NOT FOUND AND page_no<>1) OR (FOUND AND (
            page_no<>previous_page.page_number+1 OR requested_page->>'first_source_pk'<=previous_page.last_source_pk
        )) THEN
            RAISE EXCEPTION USING ERRCODE='55000', MESSAGE='direct snapshot page sequence is not contiguous';
        END IF;
    END IF;

    FOR page_row IN SELECT value FROM jsonb_array_elements(requested_page->'rows') LOOP
        actual_count := actual_count + 1;
        IF page_row->>'source_table'<>v_source_table THEN
            RAISE EXCEPTION USING ERRCODE='22023', MESSAGE='direct snapshot page mixes source tables';
        END IF;
        BEGIN
            raw_id := (page_row->>'raw_record_id')::uuid;
            source_pk := page_row->>'source_pk';
            row_kind := page_row->>'row_kind';
            payload_text := page_row->>'payload_json';
            payload_sha := page_row->>'payload_sha256';
            logical_sha := page_row->>'logical_sha256';
            source_updated := NULLIF(page_row->>'source_updated_at','')::timestamptz;
            payload := payload_text::jsonb;
        EXCEPTION WHEN OTHERS THEN
            RAISE EXCEPTION USING ERRCODE='22023', MESSAGE='direct snapshot row fields are invalid';
        END;
        IF source_pk IS NULL OR length(source_pk) NOT BETWEEN 1 AND 4000
           OR row_kind IS NULL OR row_kind !~ '^[A-Za-z0-9_./:-]+$'
           OR payload_sha !~ '^[a-f0-9]{64}$' OR logical_sha !~ '^[a-f0-9]{64}$'
           OR encode(digest(convert_to(payload_text,'UTF8'),'sha256'),'hex')<>payload_sha
           OR jsonb_typeof(payload)<>'object'
           OR (previous_pk IS NOT NULL AND source_pk<=previous_pk) THEN
            RAISE EXCEPTION USING ERRCODE='22023', MESSAGE='direct snapshot row violates bounded contract';
        END IF;
        previous_pk := source_pk;
        IF NOT EXISTS (
            SELECT 1 FROM migration.export_batch_kind kind
             WHERE kind.export_batch_id=requested_export_batch_id AND kind.row_kind=row_kind
        ) THEN
            RAISE EXCEPTION USING ERRCODE='22023', MESSAGE='direct snapshot row kind absent from pass A';
        END IF;
        INSERT INTO migration.raw_record(
            raw_record_id,export_batch_id,source_table,source_pk,row_kind,source_updated_at,payload,payload_sha256
        ) VALUES (
            raw_id,requested_export_batch_id,v_source_table,source_pk,row_kind,source_updated,payload,payload_sha
        ) ON CONFLICT (export_batch_id,source_table,source_pk) DO NOTHING;
        SELECT * INTO STRICT existing_raw FROM migration.raw_record
         WHERE export_batch_id=requested_export_batch_id AND migration.raw_record.source_table=v_source_table
           AND migration.raw_record.source_pk=source_pk;
        IF existing_raw.raw_record_id<>raw_id OR existing_raw.row_kind<>row_kind
           OR existing_raw.payload_sha256<>payload_sha OR existing_raw.payload<>payload
           OR existing_raw.source_updated_at IS DISTINCT FROM source_updated THEN
            RAISE EXCEPTION USING ERRCODE='23505', MESSAGE='direct snapshot raw row idempotency conflict';
        END IF;
        PERFORM migration.normalize_region_talk_direct_record(
            raw_id,v_source_table,source_pk,row_kind,source_updated,payload
        );
    END LOOP;
    IF previous_pk<>requested_page->>'last_source_pk'
       OR requested_page->>'first_source_pk'<>(requested_page->'rows'->0->>'source_pk') THEN
        RAISE EXCEPTION USING ERRCODE='22023', MESSAGE='direct snapshot page bounds mismatch';
    END IF;
    IF duplicate_page THEN
        RETURN jsonb_build_object('duplicate',true,'row_count',actual_count);
    END IF;
    INSERT INTO migration.region_talk_direct_snapshot_page(
        export_batch_id,source_table,page_number,first_source_pk,last_source_pk,row_count,logical_sha256
    ) VALUES (
        requested_export_batch_id,v_source_table,page_no,requested_page->>'first_source_pk',
        requested_page->>'last_source_pk',actual_count,requested_page->>'logical_sha256'
    );
    RETURN jsonb_build_object('duplicate',false,'row_count',actual_count);
END
$$;

CREATE OR REPLACE FUNCTION migration.finalize_region_talk_direct_snapshot(
    requested_export_batch_id uuid,
    requested_task_run_id uuid,
    requested_pass_b jsonb
) RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
    snapshot migration.region_talk_direct_snapshot%ROWTYPE;
    table_row jsonb;
    expected_table migration.region_talk_direct_snapshot_table%ROWTYPE;
    ordinal integer := 0;
    actual_rows bigint;
    landed bigint;
    dispositioned bigint;
    quarantined bigint;
    final_state text;
    result jsonb;
BEGIN
    snapshot := migration.assert_region_talk_direct_task(requested_export_batch_id,requested_task_run_id);
    IF snapshot.state IN ('complete','complete_with_quarantine') THEN
        RETURN snapshot.receipt;
    END IF;
    IF snapshot.state<>'landing' OR requested_pass_b->>'schema_version'<>'region-talk-direct-pass-b.v2'
       OR requested_pass_b->>'logical_sha256'<>snapshot.pass_a_logical_sha256
       OR jsonb_typeof(requested_pass_b->'tables')<>'array'
       OR jsonb_array_length(requested_pass_b->'tables')<>5 THEN
        RAISE EXCEPTION USING ERRCODE='22023', MESSAGE='direct snapshot pass B receipt is invalid';
    END IF;
    FOR table_row IN SELECT value FROM jsonb_array_elements(requested_pass_b->'tables') LOOP
        ordinal := ordinal+1;
        SELECT * INTO STRICT expected_table FROM migration.region_talk_direct_snapshot_table
         WHERE export_batch_id=requested_export_batch_id AND migration.region_talk_direct_snapshot_table.ordinal=ordinal;
        SELECT count(*) INTO actual_rows FROM migration.raw_record raw
         WHERE raw.export_batch_id=requested_export_batch_id AND raw.source_table=expected_table.source_table;
        IF table_row->>'source_table'<>expected_table.source_table
           OR (table_row->>'row_count')::bigint<>expected_table.pass_a_row_count
           OR table_row->>'logical_sha256'<>expected_table.pass_a_logical_sha256
           OR actual_rows<>expected_table.pass_a_row_count THEN
            RAISE EXCEPTION USING ERRCODE='55000', MESSAGE='direct snapshot two-pass reconciliation failed';
        END IF;
        UPDATE migration.region_talk_direct_snapshot_table
           SET pass_b_row_count=actual_rows,pass_b_logical_sha256=table_row->>'logical_sha256'
         WHERE export_batch_id=requested_export_batch_id AND source_table=expected_table.source_table;
    END LOOP;
    IF EXISTS (
        SELECT 1 FROM migration.row_accounting accounting
         WHERE accounting.export_batch_id=requested_export_batch_id
           AND NOT accounting.fully_accounted
    ) THEN
        RAISE EXCEPTION USING ERRCODE='55000', MESSAGE='direct snapshot has undispositioned or missing rows';
    END IF;
    SELECT count(*),count(disposition.raw_record_id),
           count(*) FILTER (WHERE disposition.disposition='quarantined')
      INTO landed,dispositioned,quarantined
      FROM migration.raw_record raw
      LEFT JOIN migration.row_disposition disposition ON disposition.raw_record_id=raw.raw_record_id
     WHERE raw.export_batch_id=requested_export_batch_id;
    final_state := CASE WHEN quarantined=0 THEN 'complete' ELSE 'complete_with_quarantine' END;
    result := jsonb_build_object(
        'schema_version','region-talk-direct-snapshot-receipt.v2',
        'export_batch_id',requested_export_batch_id,'task_run_id',requested_task_run_id,
        'status',final_state,'expected_row_count',landed,'landed_row_count',landed,
        'dispositioned_row_count',dispositioned,'quarantined_row_count',quarantined,
        'logical_sha256',snapshot.pass_a_logical_sha256,
        'publication_effects_enabled',false,'completed_at',clock_timestamp()
    );
    UPDATE migration.region_talk_direct_snapshot
       SET pass_b_logical_sha256=snapshot.pass_a_logical_sha256,state=final_state,
           receipt=result,completed_at=clock_timestamp()
     WHERE export_batch_id=requested_export_batch_id;
    UPDATE migration.export_batch
       SET status=CASE WHEN quarantined=0 THEN 'accepted' ELSE 'reconciled' END,
           completed_at=clock_timestamp()
     WHERE export_batch_id=requested_export_batch_id;
    RETURN result;
END
$$;

CREATE OR REPLACE FUNCTION migration.fail_region_talk_direct_snapshot(
    requested_export_batch_id uuid,
    requested_task_run_id uuid,
    requested_failure_code text
) RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE snapshot migration.region_talk_direct_snapshot%ROWTYPE;
BEGIN
    snapshot := migration.assert_region_talk_direct_task(requested_export_batch_id,requested_task_run_id);
    IF requested_failure_code IS NULL OR length(requested_failure_code) NOT BETWEEN 1 AND 128 THEN
        RAISE EXCEPTION USING ERRCODE='22023', MESSAGE='direct snapshot failure code is invalid';
    END IF;
    IF snapshot.state IN ('complete','complete_with_quarantine') THEN
        RAISE EXCEPTION USING ERRCODE='55000', MESSAGE='completed direct snapshot cannot fail';
    END IF;
    UPDATE migration.region_talk_direct_snapshot SET state='failed',failure_code=requested_failure_code,
        completed_at=clock_timestamp() WHERE export_batch_id=requested_export_batch_id;
    UPDATE migration.export_batch SET status='rejected',completed_at=clock_timestamp()
     WHERE export_batch_id=requested_export_batch_id;
END
$$;

CREATE VIEW region_talk.snapshot_inventory_v2 AS
SELECT snapshot.export_batch_id,snapshot.task_run_id,snapshot.master_epoch,snapshot.state,
       batch.expected_row_count,count(raw.raw_record_id) AS landed_row_count,
       count(disposition.raw_record_id) AS dispositioned_row_count,
       count(*) FILTER (WHERE disposition.disposition='quarantined') AS quarantined_row_count,
       snapshot.pass_a_logical_sha256 AS logical_sha256,snapshot.created_at,snapshot.completed_at
FROM migration.region_talk_direct_snapshot snapshot
JOIN migration.export_batch batch USING(export_batch_id)
LEFT JOIN migration.raw_record raw USING(export_batch_id)
LEFT JOIN migration.row_disposition disposition USING(raw_record_id)
GROUP BY snapshot.export_batch_id,snapshot.task_run_id,snapshot.master_epoch,snapshot.state,
         batch.expected_row_count,snapshot.pass_a_logical_sha256,snapshot.created_at,snapshot.completed_at;

CREATE VIEW region_talk.articles_v2 AS
SELECT item_id,title,summary,body_text,exact_url,category,status,source_updated_at,imported_at
FROM region_talk.imported_content_v2 WHERE content_family='article';

CREATE VIEW region_talk.posts_v2 AS
SELECT item_id,title,summary,body_text,exact_url,platform,external_id,category,status,
       source_updated_at,imported_at
FROM region_talk.imported_content_v2 WHERE content_family='post';

CREATE VIEW region_talk.queue_v2 AS
SELECT item_id,queue_family,source_ref,lane,status,priority_text,available_at,
       source_updated_at,imported_at
FROM region_talk.imported_queue_v2;

CREATE VIEW region_talk.queue_summary_v2 AS
SELECT queue_family,status,count(*) AS item_count,min(imported_at) AS oldest_imported_at,
       max(source_updated_at) AS latest_source_update
FROM region_talk.imported_queue_v2 GROUP BY queue_family,status;

-- Replace the citext comparison with an explicit text comparison.  The reader
-- no longer needs EXECUTE on an extension-owned equality helper in public.
CREATE OR REPLACE VIEW region_talk.funnel_current AS
SELECT
    (SELECT count(*) FROM region_talk.source) AS sources_total,
    (SELECT count(*) FROM region_talk.source WHERE status = 'active') AS sources_active,
    (SELECT count(*) FROM hub.content_item ci
       JOIN hub.project_content pc USING (content_id)
       JOIN hub.project p USING (project_id)
      WHERE p.slug::text = 'region-talk') AS content_total,
    (SELECT count(*) FROM region_talk.post_evaluation WHERE eligible) AS text_eligible,
    (SELECT count(DISTINCT content_id) FROM region_talk.image_evaluation
      WHERE verdict IN ('strong','acceptable')) AS media_ready,
    (SELECT count(*) FROM region_talk.publication_candidate WHERE status='ready') AS ready_candidates,
    (SELECT count(*) FROM region_talk.publication_candidate WHERE status='approved') AS approved_candidates,
    (SELECT count(*) FROM region_talk.publication_candidate WHERE status='published') AS published_candidates;

DO $$
DECLARE relation text;
BEGIN
    FOREACH relation IN ARRAY ARRAY[
        'migration.region_talk_direct_snapshot','migration.region_talk_direct_snapshot_table',
        'migration.region_talk_direct_snapshot_page','region_talk.imported_content_v2',
        'region_talk.imported_queue_v2','region_talk.imported_llm_request_v2',
        'region_talk.imported_discovery_run_v2'
    ] LOOP
        EXECUTE format(
            'CREATE CONSTRAINT TRIGGER mdh_epoch_write_guard AFTER INSERT OR UPDATE OR DELETE ON %s '
            'DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION master_control.enforce_write_epoch()',
            relation
        );
    END LOOP;
END
$$;

CREATE TRIGGER region_talk_direct_page_append_only
BEFORE UPDATE OR DELETE ON migration.region_talk_direct_snapshot_page
FOR EACH ROW EXECUTE FUNCTION hub_meta.reject_update_delete();
CREATE TRIGGER imported_content_v2_append_only
BEFORE UPDATE OR DELETE ON region_talk.imported_content_v2
FOR EACH ROW EXECUTE FUNCTION hub_meta.reject_update_delete();
CREATE TRIGGER imported_queue_v2_append_only
BEFORE UPDATE OR DELETE ON region_talk.imported_queue_v2
FOR EACH ROW EXECUTE FUNCTION hub_meta.reject_update_delete();
CREATE TRIGGER imported_llm_request_v2_append_only
BEFORE UPDATE OR DELETE ON region_talk.imported_llm_request_v2
FOR EACH ROW EXECUTE FUNCTION hub_meta.reject_update_delete();
CREATE TRIGGER imported_discovery_run_v2_append_only
BEFORE UPDATE OR DELETE ON region_talk.imported_discovery_run_v2
FOR EACH ROW EXECUTE FUNCTION hub_meta.reject_update_delete();

CREATE OR REPLACE FUNCTION master_control.assert_session_write_epoch()
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
    state master_control.epoch_state%ROWTYPE;
    binding master_control.credential_binding%ROWTYPE;
    guarded boolean;
    session_is_superuser boolean;
    observed_at timestamptz := clock_timestamp();
BEGIN
    SELECT rolsuper INTO STRICT session_is_superuser FROM pg_roles WHERE rolname = session_user;
    guarded := NOT session_is_superuser AND (
        pg_has_role(session_user,'mdh_application','member')
        OR pg_has_role(session_user,'mdh_orchestrator','member')
        OR pg_has_role(session_user,'mdh_connector_intake','member')
        OR pg_has_role(session_user,'mdh_mcp_editor','member')
        OR pg_has_role(session_user,'mdh_migration_operator','member')
        OR pg_has_role(session_user,'mdh_canonical_committer','member')
        OR pg_has_role(session_user,'mdh_embedding_worker','member')
        OR pg_has_role(session_user,'mdh_blogger_materializer','member')
        OR pg_has_role(session_user,'mdh_region_talk_pipeline','member')
    );
    IF NOT guarded THEN RETURN; END IF;
    SELECT * INTO STRICT state FROM master_control.epoch_state WHERE singleton=true;
    SELECT * INTO binding FROM master_control.credential_binding WHERE principal=session_user;
    IF NOT FOUND OR binding.revoked_at IS NOT NULL OR binding.expires_at<=observed_at
       OR state.gate_state<>'open' OR state.lease_until<=observed_at
       OR binding.epoch IS DISTINCT FROM state.current_epoch
       OR binding.master_instance_id IS DISTINCT FROM state.master_instance_id THEN
        RAISE EXCEPTION USING ERRCODE='55000', MESSAGE='write rejected by epoch lease gate';
    END IF;
END
$$;

REVOKE ALL ON migration.region_talk_direct_snapshot,migration.region_talk_direct_snapshot_table,
    migration.region_talk_direct_snapshot_page,region_talk.imported_content_v2,
    region_talk.imported_queue_v2,region_talk.imported_llm_request_v2,
    region_talk.imported_discovery_run_v2 FROM PUBLIC,mdh_mcp_reader,mdh_mcp_editor;
REVOKE ALL ON FUNCTION migration.region_talk_direct_body(jsonb),
    migration.normalize_region_talk_direct_record(uuid,text,text,text,timestamptz,jsonb),
    migration.assert_region_talk_direct_task(uuid,uuid),
    migration.begin_region_talk_direct_snapshot(jsonb),
    migration.land_region_talk_direct_page(uuid,uuid,jsonb),
    migration.finalize_region_talk_direct_snapshot(uuid,uuid,jsonb),
    migration.fail_region_talk_direct_snapshot(uuid,uuid,text) FROM PUBLIC;
GRANT USAGE ON SCHEMA migration,region_talk,master_control TO mdh_region_talk_pipeline;
GRANT EXECUTE ON FUNCTION migration.begin_region_talk_direct_snapshot(jsonb),
    migration.land_region_talk_direct_page(uuid,uuid,jsonb),
    migration.finalize_region_talk_direct_snapshot(uuid,uuid,jsonb),
    migration.fail_region_talk_direct_snapshot(uuid,uuid,text) TO mdh_region_talk_pipeline;
GRANT SELECT ON region_talk.snapshot_inventory_v2,region_talk.articles_v2,
    region_talk.posts_v2,region_talk.queue_v2,region_talk.queue_summary_v2,
    region_talk.funnel_current TO mdh_mcp_reader;

UPDATE hub.canonical_state
SET schema_revision=23,updated_at=clock_timestamp()
WHERE singleton=true;
