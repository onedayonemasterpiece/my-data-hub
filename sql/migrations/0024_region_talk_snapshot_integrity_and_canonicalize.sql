-- Region Talk direct snapshot v3 hardening.
--
-- 0023 deliberately remains immutable.  This migration replaces its bounded
-- functions and read views so every accepted digest is reconstructed from
-- persisted evidence, and one exact non-quarantined snapshot is projected into
-- canonical hub/Region Talk/orchestration state in the same transaction as its
-- revision, semantic outbox item, and immutable receipt. Publication dispatch
-- remains disabled.

ALTER TABLE migration.region_talk_direct_snapshot
    ADD COLUMN integrity_verified boolean NOT NULL DEFAULT false,
    ADD COLUMN verified_logical_sha256 text
        CHECK (verified_logical_sha256 IS NULL OR verified_logical_sha256 ~ '^[a-f0-9]{64}$'),
    ADD COLUMN canonical_applied_at timestamptz;

ALTER TABLE migration.region_talk_direct_snapshot_page
    ADD COLUMN submitted_logical_sha256 text
        CHECK (submitted_logical_sha256 IS NULL OR submitted_logical_sha256 ~ '^[a-f0-9]{64}$');

CREATE TABLE migration.region_talk_direct_raw_integrity (
    raw_record_id           uuid PRIMARY KEY
                            REFERENCES migration.raw_record(raw_record_id) ON DELETE RESTRICT,
    export_batch_id         uuid NOT NULL
                            REFERENCES migration.region_talk_direct_snapshot(export_batch_id) ON DELETE RESTRICT,
    source_table            text NOT NULL,
    source_pk               text NOT NULL,
    payload_canonical_text  text NOT NULL,
    row_logical_sha256      text NOT NULL CHECK (row_logical_sha256 ~ '^[a-f0-9]{64}$'),
    created_at              timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (export_batch_id,source_table,source_pk)
);
CREATE TRIGGER region_talk_direct_raw_integrity_append_only
BEFORE UPDATE OR DELETE ON migration.region_talk_direct_raw_integrity
FOR EACH ROW EXECUTE FUNCTION hub_meta.reject_update_delete();

-- One immutable row per database credential generation used by a Region Talk
-- task.  The task UUID is first bound while the exact workload-specific LOGIN
-- is epoch-valid; later calls must present the same credential binding.
CREATE TABLE migration.region_talk_task_credential_binding (
    task_run_id             uuid NOT NULL,
    credential_id           uuid NOT NULL,
    principal               name NOT NULL,
    worker_kind             text NOT NULL CHECK (worker_kind='region_talk'),
    master_instance_id      uuid NOT NULL,
    master_epoch            bigint NOT NULL CHECK (master_epoch>=1),
    export_batch_id         uuid NOT NULL
                            REFERENCES migration.region_talk_direct_snapshot(export_batch_id) ON DELETE RESTRICT,
    credential_expires_at   timestamptz NOT NULL,
    bound_at                timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (task_run_id,credential_id),
    UNIQUE (principal,export_batch_id)
);
CREATE TRIGGER region_talk_task_credential_binding_append_only
BEFORE UPDATE OR DELETE ON migration.region_talk_task_credential_binding
FOR EACH ROW EXECUTE FUNCTION hub_meta.reject_update_delete();

-- Cross-snapshot identity registry. It is evidence, not an alternate business
-- store: target UUIDs always reference the canonical table named in target_table.
CREATE TABLE migration.region_talk_canonical_identity (
    identity_kind           text NOT NULL CHECK (identity_kind IN (
                                'content','source','source_candidate','source_status','work_item',
                                'publication_plan','review_decision'
                            )),
    identity_key            text NOT NULL,
    target_table            text NOT NULL,
    target_id               uuid NOT NULL,
    first_export_batch_id   uuid NOT NULL
                            REFERENCES migration.region_talk_direct_snapshot(export_batch_id) ON DELETE RESTRICT,
    source_raw_record_id    uuid NOT NULL
                            REFERENCES migration.raw_record(raw_record_id) ON DELETE RESTRICT,
    created_at              timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (identity_kind,identity_key)
);
CREATE TRIGGER region_talk_canonical_identity_append_only
BEFORE UPDATE OR DELETE ON migration.region_talk_canonical_identity
FOR EACH ROW EXECUTE FUNCTION hub_meta.reject_update_delete();

CREATE TABLE migration.region_talk_canonical_apply_receipt (
    operation_id            text PRIMARY KEY CHECK (operation_id ~ '^[a-f0-9]{64}$'),
    export_batch_id         uuid NOT NULL UNIQUE
                            REFERENCES migration.region_talk_direct_snapshot(export_batch_id) ON DELETE RESTRICT,
    task_run_id             uuid NOT NULL,
    request_sha256          text NOT NULL CHECK (request_sha256 ~ '^[a-f0-9]{64}$'),
    verified_logical_sha256 text NOT NULL CHECK (verified_logical_sha256 ~ '^[a-f0-9]{64}$'),
    master_instance_id      uuid NOT NULL,
    master_epoch            bigint NOT NULL CHECK (master_epoch>=1),
    credential_id           uuid NOT NULL,
    revision_before         bigint NOT NULL CHECK (revision_before>=0),
    revision_after          bigint NOT NULL UNIQUE CHECK (revision_after>=1),
    affected_rows           bigint NOT NULL CHECK (affected_rows>=0),
    content_rows            bigint NOT NULL CHECK (content_rows>=0),
    source_rows             bigint NOT NULL CHECK (source_rows>=0),
    candidate_rows          bigint NOT NULL CHECK (candidate_rows>=0),
    plan_rows               bigint NOT NULL CHECK (plan_rows>=0),
    outbox_id               uuid NOT NULL UNIQUE REFERENCES sync.external_outbox(outbox_id) ON DELETE RESTRICT,
    receipt                 jsonb NOT NULL,
    receipt_sha256          text NOT NULL CHECK (receipt_sha256 ~ '^[a-f0-9]{64}$'),
    created_at              timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TRIGGER region_talk_canonical_apply_receipt_append_only
BEFORE UPDATE OR DELETE ON migration.region_talk_canonical_apply_receipt
FOR EACH ROW EXECUTE FUNCTION hub_meta.reject_update_delete();

CREATE OR REPLACE FUNCTION migration.region_talk_direct_logical_component(value text)
RETURNS bytea
LANGUAGE sql
IMMUTABLE
SET search_path=pg_catalog
AS $$
SELECT CASE WHEN value IS NULL THEN convert_to('-1:','UTF8')
            ELSE convert_to(octet_length(value)::text||':','UTF8')||convert_to(value,'UTF8') END
$$;

CREATE OR REPLACE FUNCTION migration.region_talk_direct_logical_timestamp(value timestamptz)
RETURNS text
LANGUAGE sql
IMMUTABLE
SET search_path=pg_catalog
AS $$
SELECT CASE WHEN value IS NULL THEN NULL
            ELSE to_char(value AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"') END
$$;

CREATE OR REPLACE FUNCTION migration.region_talk_direct_row_logical_sha256(
    source_table text,source_pk text,row_kind text,source_updated_at timestamptz,payload_sha256 text
) RETURNS text
LANGUAGE sql
IMMUTABLE
SET search_path=pg_catalog
AS $$
SELECT encode(sha256(
    migration.region_talk_direct_logical_component(source_table)||
    migration.region_talk_direct_logical_component(source_pk)||
    migration.region_talk_direct_logical_component(row_kind)||
    migration.region_talk_direct_logical_component(
        migration.region_talk_direct_logical_timestamp(source_updated_at))||
    migration.region_talk_direct_logical_component(payload_sha256)),'hex')
$$;

CREATE OR REPLACE FUNCTION migration.region_talk_direct_page_persisted_sha256(
    requested_export_batch_id uuid,requested_source_table text,requested_first_pk text,requested_last_pk text
) RETURNS text
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path=pg_catalog
AS $$
SELECT encode(sha256(convert_to(coalesce(string_agg(
           integrity.row_logical_sha256||E'\n','' ORDER BY raw.source_pk),''),'UTF8')),'hex')
FROM migration.raw_record raw
JOIN migration.region_talk_direct_raw_integrity integrity USING(raw_record_id)
WHERE raw.export_batch_id=requested_export_batch_id
  AND raw.source_table=requested_source_table
  AND raw.source_pk BETWEEN requested_first_pk AND requested_last_pk
$$;

CREATE OR REPLACE FUNCTION migration.region_talk_direct_table_logical_sha256(
    requested_export_batch_id uuid,requested_source_table text
) RETURNS text
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path=pg_catalog
AS $$
SELECT encode(sha256(convert_to(coalesce(string_agg(
           migration.region_talk_direct_row_logical_sha256(
               raw.source_table,raw.source_pk,raw.row_kind,raw.source_updated_at,raw.payload_sha256
           )||E'\n','' ORDER BY raw.source_pk),''),'UTF8')),'hex')
FROM migration.raw_record raw
JOIN migration.region_talk_direct_raw_integrity integrity USING(raw_record_id)
WHERE raw.export_batch_id=requested_export_batch_id
  AND raw.source_table=requested_source_table
  AND encode(sha256(convert_to(integrity.payload_canonical_text,'UTF8')),'hex')=raw.payload_sha256
  AND integrity.payload_canonical_text::jsonb=raw.payload
  AND integrity.source_table=raw.source_table AND integrity.source_pk=raw.source_pk
$$;

CREATE OR REPLACE FUNCTION migration.region_talk_direct_snapshot_logical_sha256(
    requested_export_batch_id uuid
) RETURNS text
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path=pg_catalog
AS $$
DECLARE item migration.region_talk_direct_snapshot_table%ROWTYPE; framed bytea:=''::bytea;
BEGIN
    FOR item IN SELECT * FROM migration.region_talk_direct_snapshot_table
                WHERE export_batch_id=requested_export_batch_id ORDER BY ordinal LOOP
        framed:=framed||migration.region_talk_direct_logical_component(item.source_table)
                      ||migration.region_talk_direct_logical_component(item.pass_b_row_count::text)
                      ||migration.region_talk_direct_logical_component(item.pass_b_logical_sha256);
    END LOOP;
    RETURN encode(sha256(framed),'hex');
END
$$;

CREATE OR REPLACE FUNCTION migration.region_talk_try_timestamptz(value text)
RETURNS timestamptz
LANGUAGE plpgsql
IMMUTABLE
SET search_path=pg_catalog
AS $$
BEGIN
    IF value IS NULL OR btrim(value)='' THEN RETURN NULL; END IF;
    RETURN value::timestamptz;
EXCEPTION WHEN OTHERS THEN RETURN NULL;
END
$$;

CREATE OR REPLACE FUNCTION migration.region_talk_try_integer(value text)
RETURNS integer
LANGUAGE plpgsql
IMMUTABLE
SET search_path=pg_catalog
AS $$
BEGIN
    IF value IS NULL OR btrim(value)='' THEN RETURN NULL; END IF;
    RETURN value::integer;
EXCEPTION WHEN OTHERS THEN RETURN NULL;
END
$$;

CREATE OR REPLACE FUNCTION migration.assert_region_talk_direct_task(
    requested_export_batch_id uuid,
    requested_task_run_id uuid
) RETURNS migration.region_talk_direct_snapshot
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog
AS $$
DECLARE
    snapshot migration.region_talk_direct_snapshot%ROWTYPE;
    state master_control.epoch_state%ROWTYPE;
    credential master_control.credential_binding%ROWTYPE;
    durable migration.region_talk_task_credential_binding%ROWTYPE;
    session_is_superuser boolean;
BEGIN
    SELECT rolsuper INTO STRICT session_is_superuser FROM pg_roles WHERE rolname=session_user;
    IF session_is_superuser OR NOT pg_has_role(session_user,'mdh_region_talk_pipeline','member') THEN
        RAISE EXCEPTION USING ERRCODE='42501',MESSAGE='direct snapshot requires exact Region Talk pipeline login';
    END IF;
    PERFORM master_control.assert_session_write_epoch();
    SELECT * INTO STRICT snapshot FROM migration.region_talk_direct_snapshot
     WHERE export_batch_id=requested_export_batch_id AND task_run_id=requested_task_run_id;
    SELECT * INTO STRICT state FROM master_control.epoch_state WHERE singleton=true;
    SELECT * INTO STRICT credential FROM master_control.credential_binding
     WHERE principal=session_user AND revoked_at IS NULL AND expires_at>clock_timestamp();
    IF snapshot.master_epoch<>state.current_epoch OR snapshot.master_instance_id<>state.master_instance_id
       OR state.gate_state<>'open' OR state.lease_until<=clock_timestamp()
       OR credential.epoch<>snapshot.master_epoch
       OR credential.master_instance_id<>snapshot.master_instance_id THEN
        RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='direct snapshot task is outside ACTIVE epoch';
    END IF;
    INSERT INTO migration.region_talk_task_credential_binding(
        task_run_id,credential_id,principal,worker_kind,master_instance_id,master_epoch,
        export_batch_id,credential_expires_at
    ) VALUES (
        requested_task_run_id,credential.credential_id,session_user,'region_talk',
        snapshot.master_instance_id,snapshot.master_epoch,requested_export_batch_id,credential.expires_at
    ) ON CONFLICT (task_run_id,credential_id) DO NOTHING;
    SELECT * INTO STRICT durable FROM migration.region_talk_task_credential_binding
     WHERE task_run_id=requested_task_run_id AND credential_id=credential.credential_id;
    IF durable.principal<>session_user OR durable.worker_kind<>'region_talk'
       OR durable.export_batch_id<>requested_export_batch_id
       OR durable.master_instance_id<>snapshot.master_instance_id
       OR durable.master_epoch<>snapshot.master_epoch THEN
        RAISE EXCEPTION USING ERRCODE='42501',MESSAGE='direct snapshot credential is not task-bound';
    END IF;
    RETURN snapshot;
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
    v_disposition text := 'retained_raw';
    v_reason text := 'valid_unsupported_kind_v3';
    v_refs jsonb := '[]'::jsonb;
BEGIN
    IF jsonb_typeof(requested_payload) <> 'object' OR requested_row_kind = 'malformed_compact_kind' THEN
        v_disposition := 'quarantined';
        v_reason := 'malformed_source_record_v2';
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
            coalesce(body->>'exact_url',body->>'canonical_url',body->>'url',body->>'source_url',requested_payload->>'context_url'),
            coalesce(body->>'platform',requested_payload->>'platform'),
            coalesce(body->>'external_id',body->>'post_id',requested_payload->>'surface_external_id'),
            coalesce(body->>'category',body->>'content_category',body->>'candidate_type'),
            coalesce(body->>'publication_status',body->>'status',body->>'state'),
            requested_source_updated_at
        ) ON CONFLICT (raw_record_id) DO NOTHING;
        v_disposition := 'normalized';
        v_reason := 'typed_content_projection_v2';
        v_refs := jsonb_build_array(jsonb_build_object(
            'table','region_talk.imported_content_v2','item_id',requested_raw_record_id
        ));
    ELSIF requested_row_kind IN (
        'online_source_item','external_publication_source_item',
        'source_queue_item','source_candidate_item','source_status_item','source_edge_item',
        'publication_schedule_item',
        'external_publication_review_item','external_publication_review_state_item',
        'external_publication_review_event_item','publication_review_state_item',
        'publication_review_event_item','acq_discovery_surface_item'
    ) THEN
        family := CASE requested_row_kind
            WHEN 'online_source_item' THEN 'source_registry'
            WHEN 'external_publication_source_item' THEN 'source_registry'
            WHEN 'source_queue_item' THEN 'source_frontier'
            WHEN 'source_candidate_item' THEN 'source_candidate'
            WHEN 'source_status_item' THEN 'source_status'
            WHEN 'source_edge_item' THEN 'source_edge'
            WHEN 'publication_schedule_item' THEN 'publication_schedule'
            WHEN 'external_publication_review_item' THEN 'review'
            WHEN 'external_publication_review_state_item' THEN 'review_state'
            WHEN 'external_publication_review_event_item' THEN 'review_event'
            WHEN 'publication_review_state_item' THEN 'review_state'
            WHEN 'publication_review_event_item' THEN 'review_event'
            ELSE 'discovery_surface'
        END;
        INSERT INTO region_talk.imported_queue_v2 (
            item_id,raw_record_id,queue_family,source_pk,row_kind,source_ref,lane,status,
            priority_text,available_at,source_updated_at
        ) VALUES (
            requested_raw_record_id,requested_raw_record_id,family,requested_source_pk,requested_row_kind,
            coalesce(body->>'source_ref',body->>'source_id',body->>'url',requested_payload->>'url'),
            coalesce(body->>'lane',body->>'priority_lane',body->>'queue'),
            coalesce(body->>'source_queue_status',body->>'queue_status',body->>'image_queue_status',body->>'publication_status',body->>'status',body->>'state',requested_payload->>'status'),
            coalesce(body->>'priority',body->>'priority_score'),
            NULL,
            requested_source_updated_at
        ) ON CONFLICT (raw_record_id) DO NOTHING;
        v_disposition := 'normalized';
        v_reason := 'typed_queue_projection_v2';
        v_refs := jsonb_build_array(jsonb_build_object(
            'table','region_talk.imported_queue_v2','item_id',requested_raw_record_id
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
            v_disposition := 'deduplicated';
            v_reason := 'dedicated_blogger_materialization_reused_v2';
        ELSE
            v_disposition := 'retained_raw';
            v_reason := 'awaiting_dedicated_blogger_resolution_v2';
        END IF;
    END IF;

    INSERT INTO migration.row_disposition (
        raw_record_id,mapping_version,disposition,target_refs,reason_code
    ) VALUES (
        requested_raw_record_id,'region-talk-direct-v3',v_disposition,v_refs,v_reason
    ) ON CONFLICT (raw_record_id) DO NOTHING;

    IF NOT EXISTS (
        SELECT 1 FROM migration.row_disposition d
         WHERE d.raw_record_id = requested_raw_record_id
           AND d.mapping_version = 'region-talk-direct-v3'
           AND d.disposition = v_disposition
           AND d.target_refs = v_refs
           AND d.reason_code = v_reason
    ) THEN
        RAISE EXCEPTION USING ERRCODE='23505', MESSAGE='direct row disposition idempotency conflict';
    END IF;
END
$$;


CREATE OR REPLACE FUNCTION migration.land_region_talk_direct_page(
    requested_export_batch_id uuid,
    requested_task_run_id uuid,
    requested_page jsonb
) RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog
AS $$
DECLARE
    snapshot migration.region_talk_direct_snapshot%ROWTYPE;
    page_row jsonb; payload_text text; payload jsonb; raw_id uuid;
    v_source_table text:=requested_page->>'source_table'; v_source_pk text; previous_pk text;
    v_row_kind text; payload_sha text; logical_sha text; server_row_sha text;
    server_page_sha text; server_page_preimage bytea:=''::bytea; source_updated timestamptz;
    page_no integer; actual_count integer:=0; duplicate_page boolean:=false;
    existing_page migration.region_talk_direct_snapshot_page%ROWTYPE;
    previous_page migration.region_talk_direct_snapshot_page%ROWTYPE;
    existing_raw migration.raw_record%ROWTYPE;
    existing_integrity migration.region_talk_direct_raw_integrity%ROWTYPE;
BEGIN
    snapshot:=migration.assert_region_talk_direct_task(requested_export_batch_id,requested_task_run_id);
    IF requested_page->>'schema_version'<>'region-talk-direct-page.v2'
       OR jsonb_typeof(requested_page->'rows')<>'array'
       OR jsonb_array_length(requested_page->'rows') NOT BETWEEN 1 AND 500
       OR octet_length(requested_page::text)>8388608
       OR requested_page->>'logical_sha256' !~ '^[a-f0-9]{64}$' THEN
        RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='direct snapshot page is invalid';
    END IF;
    BEGIN page_no:=(requested_page->>'page_number')::integer;
    EXCEPTION WHEN OTHERS THEN
        RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='direct snapshot page number is invalid';
    END;
    IF NOT EXISTS(SELECT 1 FROM migration.region_talk_direct_snapshot_table
                  WHERE export_batch_id=requested_export_batch_id AND source_table=v_source_table) THEN
        RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='direct snapshot source table is outside manifest';
    END IF;
    SELECT * INTO existing_page FROM migration.region_talk_direct_snapshot_page
     WHERE export_batch_id=requested_export_batch_id AND source_table=v_source_table AND page_number=page_no;
    duplicate_page:=FOUND;
    IF NOT duplicate_page AND snapshot.state<>'landing' THEN
        RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='direct snapshot no longer accepts new pages';
    END IF;
    IF NOT duplicate_page THEN
        SELECT * INTO previous_page FROM migration.region_talk_direct_snapshot_page
         WHERE export_batch_id=requested_export_batch_id AND source_table=v_source_table
         ORDER BY page_number DESC LIMIT 1;
        IF (NOT FOUND AND page_no<>1) OR (FOUND AND (
             page_no<>previous_page.page_number+1
             OR requested_page->>'first_source_pk'<=previous_page.last_source_pk
        )) THEN
            RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='direct snapshot page sequence is not contiguous';
        END IF;
    END IF;

    FOR page_row IN SELECT value FROM jsonb_array_elements(requested_page->'rows') LOOP
        actual_count:=actual_count+1;
        IF page_row->>'source_table'<>v_source_table THEN
            RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='direct snapshot page mixes source tables';
        END IF;
        BEGIN
            raw_id:=(page_row->>'raw_record_id')::uuid;
            v_source_pk:=page_row->>'source_pk'; v_row_kind:=page_row->>'row_kind';
            payload_text:=page_row->>'payload_json'; payload_sha:=page_row->>'payload_sha256';
            logical_sha:=page_row->>'logical_sha256';
            source_updated:=NULLIF(page_row->>'source_updated_at','')::timestamptz;
            payload:=payload_text::jsonb;
        EXCEPTION WHEN OTHERS THEN
            RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='direct snapshot row fields are invalid';
        END;
        server_row_sha:=migration.region_talk_direct_row_logical_sha256(
            v_source_table,v_source_pk,v_row_kind,source_updated,payload_sha
        );
        IF v_source_pk IS NULL OR length(v_source_pk) NOT BETWEEN 1 AND 4000
           OR v_row_kind IS NULL OR v_row_kind !~ '^[A-Za-z0-9_./:-]+$'
           OR payload_sha !~ '^[a-f0-9]{64}$' OR logical_sha !~ '^[a-f0-9]{64}$'
           OR encode(sha256(convert_to(payload_text,'UTF8')),'hex')<>payload_sha
           OR server_row_sha<>logical_sha OR jsonb_typeof(payload)<>'object'
           OR (previous_pk IS NOT NULL AND v_source_pk<=previous_pk) THEN
            RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='direct snapshot row violates server-recomputed contract';
        END IF;
        previous_pk:=v_source_pk;
        server_page_preimage:=server_page_preimage||convert_to(server_row_sha||E'\n','UTF8');
        IF NOT EXISTS(SELECT 1 FROM migration.export_batch_kind kind_contract
                      WHERE kind_contract.export_batch_id=requested_export_batch_id
                        AND kind_contract.row_kind=page_row->>'row_kind') THEN
            RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='direct snapshot row kind absent from pass A';
        END IF;
        INSERT INTO migration.raw_record(
            raw_record_id,export_batch_id,source_table,source_pk,row_kind,source_updated_at,payload,payload_sha256
        ) VALUES(raw_id,requested_export_batch_id,v_source_table,v_source_pk,v_row_kind,source_updated,payload,payload_sha)
        ON CONFLICT(export_batch_id,source_table,source_pk) DO NOTHING;
        SELECT * INTO STRICT existing_raw FROM migration.raw_record landed_raw
         WHERE landed_raw.export_batch_id=requested_export_batch_id
           AND landed_raw.source_table=v_source_table
           AND landed_raw.source_pk=page_row->>'source_pk';
        IF existing_raw.raw_record_id<>raw_id OR existing_raw.row_kind<>v_row_kind
           OR existing_raw.payload_sha256<>payload_sha OR existing_raw.payload<>payload
           OR existing_raw.source_updated_at IS DISTINCT FROM source_updated THEN
            RAISE EXCEPTION USING ERRCODE='23505',MESSAGE='direct snapshot raw row idempotency conflict';
        END IF;
        INSERT INTO migration.region_talk_direct_raw_integrity(
            raw_record_id,export_batch_id,source_table,source_pk,payload_canonical_text,row_logical_sha256
        ) VALUES(raw_id,requested_export_batch_id,v_source_table,v_source_pk,payload_text,server_row_sha)
        ON CONFLICT(raw_record_id) DO NOTHING;
        SELECT * INTO STRICT existing_integrity FROM migration.region_talk_direct_raw_integrity
         WHERE raw_record_id=raw_id;
        IF existing_integrity.export_batch_id<>requested_export_batch_id
           OR existing_integrity.source_table<>v_source_table OR existing_integrity.source_pk<>v_source_pk
           OR existing_integrity.payload_canonical_text<>payload_text
           OR existing_integrity.row_logical_sha256<>server_row_sha THEN
            RAISE EXCEPTION USING ERRCODE='23505',MESSAGE='direct snapshot integrity evidence conflict';
        END IF;
        PERFORM migration.normalize_region_talk_direct_record(
            raw_id,v_source_table,v_source_pk,v_row_kind,source_updated,payload
        );
    END LOOP;
    IF previous_pk<>requested_page->>'last_source_pk'
       OR requested_page->>'first_source_pk'<>(requested_page->'rows'->0->>'source_pk') THEN
        RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='direct snapshot page bounds mismatch';
    END IF;
    server_page_sha:=encode(sha256(server_page_preimage),'hex');
    IF server_page_sha<>requested_page->>'logical_sha256' THEN
        RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='direct snapshot page hash differs from server-recomputed rows';
    END IF;
    IF duplicate_page THEN
        IF existing_page.first_source_pk<>requested_page->>'first_source_pk'
           OR existing_page.last_source_pk<>requested_page->>'last_source_pk'
           OR existing_page.row_count<>actual_count
           OR existing_page.logical_sha256<>server_page_sha
           OR existing_page.submitted_logical_sha256<>requested_page->>'logical_sha256'
           OR migration.region_talk_direct_page_persisted_sha256(
                requested_export_batch_id,v_source_table,existing_page.first_source_pk,existing_page.last_source_pk
              )<>server_page_sha THEN
            RAISE EXCEPTION USING ERRCODE='23505',MESSAGE='direct snapshot page idempotency conflict';
        END IF;
        RETURN jsonb_build_object('duplicate',true,'row_count',actual_count,'server_logical_sha256',server_page_sha);
    END IF;
    INSERT INTO migration.region_talk_direct_snapshot_page(
        export_batch_id,source_table,page_number,first_source_pk,last_source_pk,row_count,
        logical_sha256,submitted_logical_sha256
    ) VALUES(
        requested_export_batch_id,v_source_table,page_no,requested_page->>'first_source_pk',
        requested_page->>'last_source_pk',actual_count,server_page_sha,requested_page->>'logical_sha256'
    );
    RETURN jsonb_build_object('duplicate',false,'row_count',actual_count,'server_logical_sha256',server_page_sha);
END
$$;

CREATE OR REPLACE FUNCTION migration.region_talk_ensure_content(
    requested_raw_record_id uuid,requested_export_batch_id uuid,requested_project_id uuid,requested_content_type text
) RETURNS uuid
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog
AS $$
DECLARE
    raw migration.raw_record%ROWTYPE; body jsonb; exact_url text; v_identity_key text;
    found_id uuid; new_id uuid:=gen_random_uuid(); legacy migration.legacy_identity_map%ROWTYPE;
    safe_type text;
BEGIN
    SELECT * INTO STRICT raw FROM migration.raw_record WHERE raw_record_id=requested_raw_record_id;
    IF raw.export_batch_id<>requested_export_batch_id THEN
        RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='content row is outside canonical snapshot';
    END IF;
    body:=migration.region_talk_direct_body(raw.payload);
    exact_url:=NULLIF(btrim(coalesce(body->>'exact_url',body->>'canonical_url',body->>'url',
                    body->>'source_url',raw.payload->>'context_url')),'');
    v_identity_key:=CASE WHEN exact_url IS NOT NULL THEN 'exact-url:'||exact_url
                       ELSE 'source:'||raw.source_table||':'||raw.source_pk END;
    safe_type:=CASE WHEN requested_content_type IN('post','article','video','link','note','document','other')
                    THEN requested_content_type ELSE 'other' END;
    PERFORM pg_advisory_xact_lock(hashtextextended('region-talk-content'||chr(31)||v_identity_key,0));
    SELECT target_id INTO found_id FROM migration.region_talk_canonical_identity
     WHERE identity_kind='content' AND migration.region_talk_canonical_identity.identity_key=v_identity_key;
    IF found_id IS NULL AND exact_url IS NOT NULL THEN
        SELECT content_id INTO found_id FROM hub.content_identity
         WHERE namespace='region-talk:exact-url' AND normalized_value=exact_url;
    END IF;
    IF found_id IS NULL THEN
        found_id:=new_id;
        INSERT INTO hub.content_item(
            content_id,content_type,title,summary,body_excerpt,canonical_url,normalized_url,
            content_hash,published_at,status,metadata,first_observed_at,last_observed_at
        ) VALUES(
            found_id,safe_type,coalesce(body->>'title',body->>'name'),
            coalesce(body->>'summary',body->>'description',body->>'excerpt'),
            coalesce(body->>'body',body->>'text',body->>'content'),exact_url,exact_url,
            raw.payload_sha256,migration.region_talk_try_timestamptz(
                coalesce(body->>'published_at',body->>'publication_date',body->>'created_at')),
            'active',jsonb_build_object(
                'region_talk_snapshot_id',requested_export_batch_id,'legacy_row_kind',raw.row_kind,
                'legacy_source_table',raw.source_table,'legacy_source_pk',raw.source_pk,
                'legacy_status',coalesce(body->>'publication_status',body->>'status',body->>'state')
            ),coalesce(raw.source_updated_at,clock_timestamp()),coalesce(raw.source_updated_at,clock_timestamp())
        );
        IF exact_url IS NOT NULL THEN
            INSERT INTO hub.content_identity(content_id,namespace,external_id,normalized_value,is_primary,metadata)
            VALUES(found_id,'region-talk:exact-url',coalesce(body->>'external_id',body->>'post_id'),
                   exact_url,true,jsonb_build_object('export_batch_id',requested_export_batch_id));
        ELSE
            INSERT INTO hub.content_identity(content_id,namespace,external_id,normalized_value,is_primary,metadata)
            VALUES(found_id,'region-talk:ydb',NULL,raw.source_table||':'||raw.source_pk,true,
                   jsonb_build_object('export_batch_id',requested_export_batch_id));
        END IF;
    ELSE
        UPDATE hub.content_item existing SET
            title=coalesce(body->>'title',body->>'name',existing.title),
            summary=coalesce(body->>'summary',body->>'description',body->>'excerpt',existing.summary),
            body_excerpt=coalesce(body->>'body',body->>'text',body->>'content',existing.body_excerpt),
            canonical_url=coalesce(exact_url,existing.canonical_url),
            normalized_url=coalesce(exact_url,existing.normalized_url),
            content_hash=raw.payload_sha256,
            last_observed_at=greatest(existing.last_observed_at,coalesce(raw.source_updated_at,clock_timestamp())),
            revision=existing.revision+1,
            metadata=existing.metadata||jsonb_build_object(
                'region_talk_snapshot_id',requested_export_batch_id,'legacy_row_kind',raw.row_kind,
                'legacy_source_table',raw.source_table,'legacy_source_pk',raw.source_pk,
                'legacy_status',coalesce(body->>'publication_status',body->>'status',body->>'state'))
         WHERE existing.content_id=found_id AND (
            existing.content_hash IS DISTINCT FROM raw.payload_sha256
            OR existing.metadata->>'region_talk_snapshot_id' IS DISTINCT FROM requested_export_batch_id::text
         );
    END IF;
    INSERT INTO migration.region_talk_canonical_identity(
        identity_kind,identity_key,target_table,target_id,first_export_batch_id,source_raw_record_id
    ) VALUES('content',v_identity_key,'hub.content_item',found_id,requested_export_batch_id,requested_raw_record_id)
    ON CONFLICT(identity_kind,identity_key) DO NOTHING;
    IF NOT EXISTS(SELECT 1 FROM migration.region_talk_canonical_identity
                  WHERE identity_kind='content' AND migration.region_talk_canonical_identity.identity_key=v_identity_key
                    AND target_table='hub.content_item' AND target_id=found_id) THEN
        RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='content identity changed during canonical apply';
    END IF;
    INSERT INTO hub.project_content(project_id,content_id,status,metadata)
    VALUES(requested_project_id,found_id,'candidate',jsonb_build_object(
           'region_talk_snapshot_id',requested_export_batch_id,'publication_dispatch',false))
    ON CONFLICT(project_id,content_id) DO NOTHING;
    INSERT INTO migration.legacy_identity_map(
        source_system,source_table,source_pk,target_table,target_pk,mapping_version,mapping_kind,evidence
    ) VALUES('ydb',raw.source_table,raw.source_pk,'hub.content_item',jsonb_build_object('content_id',found_id),
             'region-talk-canonical-v3','created',jsonb_build_object('export_batch_id',requested_export_batch_id))
    ON CONFLICT(source_system,source_table,source_pk) DO NOTHING;
    SELECT * INTO STRICT legacy FROM migration.legacy_identity_map
     WHERE source_system='ydb' AND source_table=raw.source_table AND source_pk=raw.source_pk;
    IF legacy.target_table IS DISTINCT FROM 'hub.content_item'
       OR legacy.target_pk->>'content_id' IS DISTINCT FROM found_id::text THEN
        RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='legacy content identity conflicts with canonical target';
    END IF;
    RETURN found_id;
END
$$;

CREATE OR REPLACE FUNCTION migration.region_talk_ensure_source(
    requested_raw_record_id uuid,requested_export_batch_id uuid
) RETURNS uuid
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog
AS $$
DECLARE
    raw migration.raw_record%ROWTYPE; body jsonb; v_source_key text; v_exact_url text;
    found_id uuid; v_platform text; v_external_id text; v_handle text; legacy migration.legacy_identity_map%ROWTYPE;
BEGIN
    SELECT * INTO STRICT raw FROM migration.raw_record WHERE raw_record_id=requested_raw_record_id;
    IF raw.export_batch_id<>requested_export_batch_id THEN
        RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='source row is outside canonical snapshot';
    END IF;
    body:=migration.region_talk_direct_body(raw.payload);
    v_exact_url:=NULLIF(btrim(coalesce(body->>'exact_url',body->>'canonical_url',body->>'url',body->>'source_url')),'');
    v_platform:=NULLIF(lower(btrim(body->>'platform')),'');
    v_external_id:=NULLIF(btrim(coalesce(body->>'external_id',body->>'source_external_id')),'');
    v_handle:=NULLIF(btrim(coalesce(body->>'handle',body->>'username')),'');
    v_source_key:=NULLIF(btrim(coalesce(body->>'source_ref',body->>'source_id',v_exact_url,v_external_id,v_handle)),'');
    IF v_source_key IS NULL THEN v_source_key:='source:'||raw.source_table||':'||raw.source_pk; END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended('region-talk-source'||chr(31)||v_source_key,0));
    SELECT target_id INTO found_id FROM migration.region_talk_canonical_identity
     WHERE identity_kind='source' AND migration.region_talk_canonical_identity.identity_key=v_source_key;
    IF found_id IS NULL AND v_platform IS NOT NULL THEN
        SELECT actor_id INTO found_id FROM hub.external_account account
         WHERE lower(account.platform)=v_platform AND (
              (v_external_id IS NOT NULL AND account.external_id=v_external_id)
              OR (v_exact_url IS NOT NULL AND account.normalized_url=v_exact_url)
              OR (v_handle IS NOT NULL AND lower(account.handle)=lower(v_handle))) LIMIT 1;
    END IF;
    IF found_id IS NULL THEN
        found_id:=gen_random_uuid();
        INSERT INTO hub.actor(actor_id,actor_type,display_name,canonical_name,metadata)
        VALUES(found_id,'unknown',coalesce(body->>'display_name',body->>'name',v_handle,v_source_key),
               coalesce(body->>'canonical_name',body->>'name'),jsonb_build_object(
                   'region_talk_snapshot_id',requested_export_batch_id,'legacy_source_key',v_source_key));
        INSERT INTO region_talk.source(source_id,source_kind,status,evidence)
        VALUES(found_id,coalesce(NULLIF(body->>'source_kind',''),v_platform,'legacy_import'),'candidate',
               jsonb_build_object('legacy_status',coalesce(body->>'source_queue_status',body->>'queue_status',
                                  body->>'status',body->>'state'),
                                  'raw_record_id',requested_raw_record_id,
                                  'export_batch_id',requested_export_batch_id));
        IF v_platform IS NOT NULL AND num_nonnulls(v_external_id,v_exact_url,v_handle)>0 THEN
            INSERT INTO hub.external_account(
                actor_id,platform,external_id,handle,url,normalized_url,status,metadata
            ) VALUES(found_id,v_platform,v_external_id,v_handle,v_exact_url,v_exact_url,'active',
                     jsonb_build_object('region_talk_snapshot_id',requested_export_batch_id));
        END IF;
    ELSIF NOT EXISTS(SELECT 1 FROM region_talk.source WHERE source_id=found_id) THEN
        INSERT INTO region_talk.source(source_id,source_kind,status,evidence)
        VALUES(found_id,coalesce(NULLIF(body->>'source_kind',''),v_platform,'legacy_import'),'candidate',
               jsonb_build_object('raw_record_id',requested_raw_record_id,'export_batch_id',requested_export_batch_id));
    END IF;
    INSERT INTO migration.region_talk_canonical_identity(
        identity_kind,identity_key,target_table,target_id,first_export_batch_id,source_raw_record_id
    ) VALUES('source',v_source_key,'region_talk.source',found_id,requested_export_batch_id,requested_raw_record_id)
    ON CONFLICT(identity_kind,identity_key) DO NOTHING;
    IF NOT EXISTS(SELECT 1 FROM migration.region_talk_canonical_identity
                  WHERE identity_kind='source' AND migration.region_talk_canonical_identity.identity_key=v_source_key
                    AND target_table='region_talk.source' AND target_id=found_id) THEN
        RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='source identity changed during canonical apply';
    END IF;
    INSERT INTO migration.legacy_identity_map(
        source_system,source_table,source_pk,target_table,target_pk,mapping_version,mapping_kind,evidence
    ) VALUES('ydb',raw.source_table,raw.source_pk,'region_talk.source',jsonb_build_object('source_id',found_id),
             'region-talk-canonical-v3','created',jsonb_build_object('export_batch_id',requested_export_batch_id))
    ON CONFLICT(source_system,source_table,source_pk) DO NOTHING;
    SELECT * INTO STRICT legacy FROM migration.legacy_identity_map
     WHERE source_system='ydb' AND source_table=raw.source_table AND source_pk=raw.source_pk;
    IF legacy.target_table IS DISTINCT FROM 'region_talk.source'
       OR legacy.target_pk->>'source_id' IS DISTINCT FROM found_id::text THEN
        RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='legacy source identity conflicts with canonical target';
    END IF;
    RETURN found_id;
END
$$;

CREATE OR REPLACE FUNCTION migration.canonicalize_region_talk_direct_snapshot(
    requested_export_batch_id uuid,requested_task_run_id uuid,requested_operation_id text,
    requested_request_sha256 text,requested_verified_logical_sha256 text
) RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog
AS $$
DECLARE
    snapshot migration.region_talk_direct_snapshot%ROWTYPE;
    prior migration.region_talk_canonical_apply_receipt%ROWTYPE;
    raw migration.raw_record%ROWTYPE; body jsonb; v_project_id uuid; v_content_id uuid; v_source_id uuid;
    v_candidate_id uuid; v_candidate_revision integer; v_revision_fingerprint text; exact_url text;
    legacy_status text; intake_status text; candidate_status text; plan_status text;
    v_pipeline_id uuid; v_stage_id uuid; v_work_id uuid; v_plan_id uuid; v_identity_key text;
    v_credential_id uuid; revision_before bigint; revision_after bigint; v_outbox_id uuid:=gen_random_uuid();
    content_rows bigint:=0; source_rows bigint:=0; candidate_rows bigint:=0; plan_rows bigint:=0;
    affected bigint; result jsonb; result_sha text; decision text; channel text;
BEGIN
    snapshot:=migration.assert_region_talk_direct_task(requested_export_batch_id,requested_task_run_id);
    IF requested_operation_id !~ '^[a-f0-9]{64}$'
       OR requested_request_sha256 !~ '^[a-f0-9]{64}$'
       OR requested_verified_logical_sha256 !~ '^[a-f0-9]{64}$' THEN
        RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='canonical apply identity is invalid';
    END IF;
    SELECT * INTO prior FROM migration.region_talk_canonical_apply_receipt
     WHERE operation_id=requested_operation_id OR export_batch_id=requested_export_batch_id;
    IF FOUND THEN
        IF prior.operation_id<>requested_operation_id OR prior.export_batch_id<>requested_export_batch_id
           OR prior.task_run_id<>requested_task_run_id OR prior.request_sha256<>requested_request_sha256
           OR prior.verified_logical_sha256<>requested_verified_logical_sha256
           OR prior.master_instance_id<>snapshot.master_instance_id OR prior.master_epoch<>snapshot.master_epoch THEN
            RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='canonical apply replay conflicts with immutable receipt';
        END IF;
        RETURN prior.receipt;
    END IF;
    IF snapshot.state<>'complete' OR NOT snapshot.integrity_verified
       OR snapshot.verified_logical_sha256 IS DISTINCT FROM requested_verified_logical_sha256
       OR snapshot.request_sha256<>requested_request_sha256 THEN
        RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='canonical apply requires finalized exact verified snapshot';
    END IF;
    IF EXISTS(SELECT 1 FROM migration.row_accounting accounting
              WHERE accounting.export_batch_id=requested_export_batch_id
                AND (NOT accounting.cutover_ready OR accounting.quarantined_count>0)) THEN
        RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='snapshot contains quarantine and cannot become canonical';
    END IF;
    SELECT binding.credential_id INTO STRICT v_credential_id
      FROM migration.region_talk_task_credential_binding binding
     WHERE binding.task_run_id=requested_task_run_id AND binding.export_batch_id=requested_export_batch_id
       AND binding.principal=session_user ORDER BY binding.bound_at DESC LIMIT 1;
    SELECT project.project_id INTO STRICT v_project_id FROM hub.project project WHERE project.slug::text='region-talk';
    -- Serialize every canonical committer through the singleton CAS boundary.
    PERFORM pg_advisory_xact_lock(hashtextextended('my-data-hub:canonical-committer',0));
    SELECT canonical_revision INTO STRICT revision_before FROM hub.canonical_state
     WHERE singleton=true FOR UPDATE;

    -- Articles, posts, and publication candidates become shared content first.
    FOR raw IN SELECT source_row.* FROM migration.raw_record source_row
               WHERE source_row.export_batch_id=requested_export_batch_id
                 AND source_row.row_kind IN(
                    'external_publication_intake_item','processed_post_item','post_live_item',
                    'publication_candidate_item','acq_discovery_opportunity_item')
               ORDER BY source_row.source_table,source_row.source_pk LOOP
        body:=migration.region_talk_direct_body(raw.payload);
        v_content_id:=migration.region_talk_ensure_content(
            raw.raw_record_id,requested_export_batch_id,v_project_id,
            CASE WHEN raw.row_kind='external_publication_intake_item' THEN 'article'
                 WHEN raw.row_kind IN('processed_post_item','post_live_item') THEN 'post'
                 ELSE coalesce(NULLIF(body->>'content_type',''),'other') END
        );
        content_rows:=content_rows+1;
        exact_url:=NULLIF(btrim(coalesce(body->>'exact_url',body->>'canonical_url',body->>'url',
                         body->>'source_url',raw.payload->>'context_url')),'');
        legacy_status:=NULLIF(lower(btrim(coalesce(body->>'publication_status',body->>'status',body->>'state'))),'');
        INSERT INTO hub.provenance_event(
            project_id,subject_type,subject_id,event_type,actor_kind,actor_ref,source_uri,observed_at,evidence
        ) VALUES(v_project_id,'content',v_content_id,'region_talk_snapshot_canonicalized','system',
                 'region-talk-direct-pipeline',exact_url,coalesce(raw.source_updated_at,clock_timestamp()),
                 jsonb_build_object('export_batch_id',requested_export_batch_id,'raw_record_id',raw.raw_record_id,
                                    'row_kind',raw.row_kind,'payload_sha256',raw.payload_sha256,
                                    'legacy_status',legacy_status));
        IF raw.row_kind='external_publication_intake_item' AND exact_url IS NOT NULL THEN
            intake_status:=CASE legacy_status WHEN 'accepted' THEN 'accepted' WHEN 'rejected' THEN 'rejected'
                WHEN 'duplicate' THEN 'duplicate' WHEN 'quarantined' THEN 'quarantined' ELSE 'pending' END;
            INSERT INTO region_talk.external_publication_intake(
                content_id,intake_key,exact_url,status,evidence
            ) VALUES(v_content_id,'ydb:'||raw.source_table||':'||raw.source_pk,exact_url,intake_status,
                     jsonb_build_object('export_batch_id',requested_export_batch_id,'raw_record_id',raw.raw_record_id,
                                        'legacy_status',legacy_status,
                                        'status_mapping',CASE WHEN legacy_status IN('accepted','rejected','duplicate','quarantined')
                                                              THEN 'exact' ELSE 'neutral_pending' END))
            ON CONFLICT(intake_key) DO NOTHING;
        ELSIF raw.row_kind IN('processed_post_item','post_live_item') THEN
            intake_status:=CASE legacy_status WHEN 'fetched' THEN 'fetched' WHEN 'evaluated' THEN 'evaluated'
                WHEN 'accepted' THEN 'accepted' WHEN 'rejected' THEN 'rejected'
                WHEN 'terminal' THEN 'terminal' WHEN 'quarantined' THEN 'quarantined' ELSE 'pending' END;
            INSERT INTO region_talk.post_intake(
                content_id,intake_kind,exact_url,status,input_fingerprint,evidence
            ) VALUES(v_content_id,'legacy_snapshot',exact_url,intake_status,raw.payload_sha256,
                     jsonb_build_object('export_batch_id',requested_export_batch_id,'raw_record_id',raw.raw_record_id,
                                        'legacy_status',legacy_status,
                                        'status_mapping',CASE WHEN legacy_status IN(
                                          'fetched','evaluated','accepted','rejected','terminal','quarantined')
                                          THEN 'exact' ELSE 'neutral_pending' END))
            ON CONFLICT(content_id,intake_kind,input_fingerprint) DO NOTHING;
        END IF;
        IF raw.row_kind='publication_candidate_item' THEN
            candidate_status:=CASE legacy_status WHEN 'ready' THEN 'ready' WHEN 'in_review' THEN 'in_review'
                WHEN 'approved' THEN 'approved' WHEN 'rejected' THEN 'rejected'
                WHEN 'published' THEN 'published' WHEN 'revoked' THEN 'revoked' ELSE 'draft' END;
            INSERT INTO region_talk.publication_candidate(content_id,project_id,status)
            VALUES(v_content_id,v_project_id,candidate_status)
            ON CONFLICT ON CONSTRAINT publication_candidate_content_id_project_id_key DO NOTHING;
            SELECT candidate.candidate_id,candidate.current_revision INTO STRICT v_candidate_id,v_candidate_revision
              FROM region_talk.publication_candidate candidate
             WHERE candidate.content_id=v_content_id AND candidate.project_id=v_project_id FOR UPDATE;
            v_revision_fingerprint:=encode(sha256(convert_to(v_candidate_id::text||':'||raw.payload_sha256,'UTF8')),'hex');
            IF NOT EXISTS(SELECT 1 FROM region_talk.candidate_revision revision
                          WHERE revision.candidate_id=v_candidate_id
                            AND revision.revision_fingerprint=v_revision_fingerprint) THEN
                v_candidate_revision:=v_candidate_revision+1;
                INSERT INTO region_talk.candidate_revision(
                    candidate_id,revision,revision_fingerprint,text_payload,ordered_media,cta,writer_model
                ) VALUES(v_candidate_id,v_candidate_revision,v_revision_fingerprint,
                         jsonb_build_object('title',body->'title','summary',body->'summary',
                                            'body',coalesce(body->'body',body->'text',body->'content'),
                                            'legacy_status',legacy_status,'raw_record_id',raw.raw_record_id),
                         CASE WHEN jsonb_typeof(body->'ordered_media')='array' THEN body->'ordered_media'
                              WHEN jsonb_typeof(body->'media')='array' THEN body->'media' ELSE '[]'::jsonb END,
                         CASE WHEN jsonb_typeof(body->'cta')='object' THEN body->'cta' ELSE '{}'::jsonb END,
                         jsonb_build_object('imported_legacy_evidence',true,
                                            'model_result_not_invented',true,
                                            'payload_sha256',raw.payload_sha256));
                UPDATE region_talk.publication_candidate candidate
                   SET current_revision=v_candidate_revision,status=candidate_status
                 WHERE candidate.candidate_id=v_candidate_id;
            END IF;
            candidate_rows:=candidate_rows+1;
        END IF;
    END LOOP;

    -- Source discovery/frontier state is canonical and work-queue executable.
    FOR raw IN SELECT source_row.* FROM migration.raw_record source_row
               WHERE source_row.export_batch_id=requested_export_batch_id
                 AND source_row.row_kind IN(
                    'online_source_item','external_publication_source_item','source_candidate_item',
                    'source_queue_item','source_status_item','source_edge_item','acq_discovery_surface_item')
               ORDER BY source_row.source_table,source_row.source_pk LOOP
        body:=migration.region_talk_direct_body(raw.payload);
        v_source_id:=migration.region_talk_ensure_source(raw.raw_record_id,requested_export_batch_id);
        source_rows:=source_rows+1;
        legacy_status:=NULLIF(lower(btrim(coalesce(body->>'source_queue_status',body->>'queue_status',
                                            body->>'status',body->>'state'))),'');
        IF raw.row_kind='source_candidate_item' THEN
            v_identity_key:=raw.source_table||':'||raw.source_pk;
            SELECT target_id INTO v_work_id FROM migration.region_talk_canonical_identity
             WHERE identity_kind='source_candidate' AND migration.region_talk_canonical_identity.identity_key=v_identity_key;
            IF v_work_id IS NULL THEN
                v_work_id:=gen_random_uuid();
                INSERT INTO region_talk.source_candidate(
                    source_candidate_id,source_id,candidate_url,candidate_handle,discovered_by,status,evidence
                ) VALUES(v_work_id,v_source_id,
                         NULLIF(btrim(coalesce(body->>'candidate_url',body->>'url',body->>'canonical_url')),''),
                         coalesce(NULLIF(btrim(coalesce(body->>'candidate_handle',body->>'handle')),''),
                                  CASE WHEN NULLIF(btrim(coalesce(body->>'candidate_url',body->>'url',
                                       body->>'canonical_url')),'') IS NULL
                                       THEN 'legacy-ydb:'||raw.source_pk END),
                         coalesce(NULLIF(body->>'discovered_by',''),'legacy_ydb_import'),
                         CASE legacy_status WHEN 'resolved' THEN 'resolved' WHEN 'accepted' THEN 'accepted'
                              WHEN 'rejected' THEN 'rejected' WHEN 'duplicate' THEN 'duplicate'
                              WHEN 'quarantined' THEN 'quarantined' ELSE 'pending' END,
                         jsonb_build_object('export_batch_id',requested_export_batch_id,
                                            'raw_record_id',raw.raw_record_id,'legacy_status',legacy_status));
                INSERT INTO migration.region_talk_canonical_identity(
                    identity_kind,identity_key,target_table,target_id,first_export_batch_id,source_raw_record_id
                ) VALUES('source_candidate',v_identity_key,'region_talk.source_candidate',v_work_id,
                         requested_export_batch_id,raw.raw_record_id);
            END IF;
        ELSIF raw.row_kind='external_publication_source_item' THEN
            INSERT INTO region_talk.external_publication_source(source_id,registry_key,status,evidence)
            VALUES(v_source_id,'ydb:'||raw.source_table||':'||raw.source_pk,
                   CASE legacy_status WHEN 'active' THEN 'active' WHEN 'paused' THEN 'paused'
                        WHEN 'excluded' THEN 'excluded' ELSE 'unknown' END,
                   jsonb_build_object('export_batch_id',requested_export_batch_id,
                                      'raw_record_id',raw.raw_record_id,'legacy_status',legacy_status))
            ON CONFLICT(registry_key) DO NOTHING;
        ELSIF raw.row_kind='source_status_item' THEN
            v_identity_key:=raw.source_table||':'||raw.source_pk;
            IF NOT EXISTS(SELECT 1 FROM migration.region_talk_canonical_identity
                          WHERE identity_kind='source_status'
                            AND migration.region_talk_canonical_identity.identity_key=v_identity_key) THEN
                INSERT INTO region_talk.source_status(source_id,status,reason,gate_version,evidence,occurred_at)
                VALUES(v_source_id,coalesce(legacy_status,'unknown'),coalesce(body->>'reason',body->>'primary_reason'),
                       body->>'gate_version',jsonb_build_object('export_batch_id',requested_export_batch_id,
                       'raw_record_id',raw.raw_record_id),coalesce(raw.source_updated_at,clock_timestamp()))
                RETURNING source_status_id INTO v_work_id;
                INSERT INTO migration.region_talk_canonical_identity(
                    identity_kind,identity_key,target_table,target_id,first_export_batch_id,source_raw_record_id
                ) VALUES('source_status',v_identity_key,'region_talk.source_status',v_work_id,
                         requested_export_batch_id,raw.raw_record_id);
            END IF;
        ELSIF raw.row_kind='source_edge_item' THEN
            INSERT INTO region_talk.source_edge(from_source_id,edge_kind,source_ref,evidence)
            VALUES(v_source_id,coalesce(NULLIF(body->>'edge_kind',''),'legacy_discovery_edge'),
                   coalesce(body->>'to_source_ref',body->>'target_source',body->>'source_ref'),
                   jsonb_build_object('export_batch_id',requested_export_batch_id,
                                      'raw_record_id',raw.raw_record_id,'legacy_payload',body))
            ON CONFLICT DO NOTHING;
        ELSIF raw.row_kind='source_queue_item' THEN
            SELECT pipeline.pipeline_id INTO v_pipeline_id FROM orchestration.pipeline pipeline
             WHERE pipeline.workload='region-talk' AND pipeline.name='region-talk-main'
             ORDER BY pipeline.version DESC LIMIT 1;
            IF v_pipeline_id IS NULL THEN
                RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='Region Talk pipeline registration missing for source frontier';
            END IF;
            SELECT stage.stage_id INTO v_stage_id FROM orchestration.pipeline_stage stage
             WHERE stage.pipeline_id=v_pipeline_id AND stage.stage_key='source_discovery'
             ORDER BY stage.stage_version DESC LIMIT 1;
            IF v_stage_id IS NULL THEN
                RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='Region Talk source discovery stage is missing';
            END IF;
            v_identity_key:=raw.source_table||':'||raw.source_pk;
            SELECT target_id INTO v_work_id FROM migration.region_talk_canonical_identity
             WHERE identity_kind='work_item' AND migration.region_talk_canonical_identity.identity_key=v_identity_key;
            IF v_work_id IS NULL THEN
                v_work_id:=gen_random_uuid();
                INSERT INTO orchestration.work_item(
                    work_item_id,pipeline_id,stage_id,project_id,subject_type,subject_id,dedupe_key,
                    input_fingerprint,priority,payload,status,available_at
                ) VALUES(v_work_id,v_pipeline_id,v_stage_id,v_project_id,'region_talk.source',v_source_id,
                         'legacy-source:'||raw.source_pk,raw.payload_sha256,
                         coalesce(migration.region_talk_try_integer(body->>'priority'),100),
                         jsonb_build_object('legacy_payload',body,'export_batch_id',requested_export_batch_id,
                                            'publication_dispatch',false),
                         CASE legacy_status WHEN 'succeeded' THEN 'succeeded' WHEN 'completed' THEN 'succeeded'
                              WHEN 'failed_terminal' THEN 'failed_terminal' WHEN 'quarantined' THEN 'quarantined'
                              WHEN 'cancelled' THEN 'cancelled' WHEN 'failed' THEN 'failed_retryable'
                              ELSE 'pending' END,
                         coalesce(migration.region_talk_try_timestamptz(
                            coalesce(body->>'available_at',body->>'retry_at')),clock_timestamp()));
                INSERT INTO migration.region_talk_canonical_identity(
                    identity_kind,identity_key,target_table,target_id,first_export_batch_id,source_raw_record_id
                ) VALUES('work_item',v_identity_key,'orchestration.work_item',v_work_id,
                         requested_export_batch_id,raw.raw_record_id);
                INSERT INTO region_talk.source_work_projection(
                    source_id,work_item_id,priority_lane,priority_score,priority_reason,readiness_state
                ) VALUES(v_source_id,v_work_id,
                         CASE body->>'priority_lane' WHEN 'head_repair' THEN 'head_repair' WHEN 'exact' THEN 'exact'
                              WHEN 'cached' THEN 'cached' WHEN 'resolve' THEN 'resolve'
                              WHEN 'exploration' THEN 'exploration' ELSE 'normal' END,
                         coalesce(migration.region_talk_try_integer(body->>'priority_score'),0),body->>'priority_reason',
                         CASE body->>'readiness_state' WHEN 'actionable_cached' THEN 'actionable_cached'
                              WHEN 'needs_entity_resolve' THEN 'needs_entity_resolve' WHEN 'head_repair' THEN 'head_repair'
                              WHEN 'cooldown' THEN 'cooldown' WHEN 'retry' THEN 'retry' WHEN 'scan_due' THEN 'scan_due'
                              WHEN 'access_denied' THEN 'access_denied' WHEN 'not_found' THEN 'not_found'
                              WHEN 'terminal' THEN 'terminal' ELSE 'unknown' END);
            END IF;
        END IF;
    END LOOP;

    -- Candidate schedule/review history joins the same publication queue. The
    -- target channel is the product's new Region Talk channel; no dispatch row
    -- or publication attempt is created.
    FOR raw IN SELECT source_row.* FROM migration.raw_record source_row
               WHERE source_row.export_batch_id=requested_export_batch_id
                 AND source_row.row_kind IN(
                    'publication_schedule_item','external_publication_review_item',
                    'external_publication_review_state_item','external_publication_review_event_item',
                    'publication_review_state_item','publication_review_event_item')
               ORDER BY source_row.source_table,source_row.source_pk LOOP
        body:=migration.region_talk_direct_body(raw.payload);
        v_content_id:=migration.region_talk_ensure_content(raw.raw_record_id,requested_export_batch_id,v_project_id,'other');
        INSERT INTO region_talk.publication_candidate(content_id,project_id,status)
        VALUES(v_content_id,v_project_id,'draft')
        ON CONFLICT ON CONSTRAINT publication_candidate_content_id_project_id_key DO NOTHING;
        SELECT candidate.candidate_id,candidate.current_revision INTO STRICT v_candidate_id,v_candidate_revision
          FROM region_talk.publication_candidate candidate
         WHERE candidate.content_id=v_content_id AND candidate.project_id=v_project_id FOR UPDATE;
        -- A schedule or review is evidence *about* the current candidate
        -- revision; it must never manufacture a new editorial revision and
        -- thereby detach an earlier plan/review from the visible queue.  Only
        -- bootstrap revision 1 when no candidate payload exists at all.
        IF v_candidate_revision=0 THEN
            v_candidate_revision:=1;
            v_revision_fingerprint:=encode(sha256(convert_to(
                v_candidate_id::text||':legacy-schedule-review-bootstrap:'||raw.payload_sha256,'UTF8')),'hex');
            INSERT INTO region_talk.candidate_revision(
                candidate_id,revision,revision_fingerprint,text_payload,ordered_media,cta,writer_model
            ) VALUES(v_candidate_id,v_candidate_revision,v_revision_fingerprint,
                     jsonb_build_object('legacy_payload',body,'raw_record_id',raw.raw_record_id),
                     CASE WHEN jsonb_typeof(body->'ordered_media')='array' THEN body->'ordered_media' ELSE '[]'::jsonb END,
                     CASE WHEN jsonb_typeof(body->'cta')='object' THEN body->'cta' ELSE '{}'::jsonb END,
                     jsonb_build_object('imported_legacy_evidence',true,'model_result_not_invented',true,
                                        'bootstrap_without_candidate_payload',true));
            UPDATE region_talk.publication_candidate candidate SET current_revision=v_candidate_revision
             WHERE candidate.candidate_id=v_candidate_id;
        END IF;
        candidate_rows:=candidate_rows+1;
        legacy_status:=NULLIF(lower(btrim(coalesce(body->>'publication_status',body->>'status',body->>'state'))),'');
        IF raw.row_kind='publication_schedule_item' THEN
            channel:=coalesce(NULLIF(body->>'channel',''),NULLIF(body->>'target_channel',''),'region-talk-new-channel');
            plan_status:=CASE legacy_status WHEN 'queued' THEN 'queued' WHEN 'published' THEN 'published'
                         WHEN 'failed' THEN 'failed' WHEN 'cancelled' THEN 'cancelled' ELSE 'planned' END;
            v_identity_key:=raw.source_table||':'||raw.source_pk;
            SELECT target_id INTO v_plan_id FROM migration.region_talk_canonical_identity
             WHERE identity_kind='publication_plan' AND migration.region_talk_canonical_identity.identity_key=v_identity_key;
            IF v_plan_id IS NULL THEN
                v_plan_id:=gen_random_uuid();
                INSERT INTO region_talk.publication_plan(
                    publication_plan_id,candidate_id,candidate_revision,channel,idempotency_key,status,
                    scheduled_for,payload
                ) VALUES(v_plan_id,v_candidate_id,v_candidate_revision,channel,
                         'region-talk-import:'||encode(sha256(convert_to(v_identity_key,'UTF8')),'hex'),
                         plan_status,migration.region_talk_try_timestamptz(
                            coalesce(body->>'scheduled_for',body->>'publish_at')),
                         jsonb_build_object('legacy_payload',body,'export_batch_id',requested_export_batch_id,
                                            'raw_record_id',raw.raw_record_id,'publication_dispatch',false,
                                            'legacy_status',legacy_status));
                INSERT INTO migration.region_talk_canonical_identity(
                    identity_kind,identity_key,target_table,target_id,first_export_batch_id,source_raw_record_id
                ) VALUES('publication_plan',v_identity_key,'region_talk.publication_plan',v_plan_id,
                         requested_export_batch_id,raw.raw_record_id);
            END IF;
            plan_rows:=plan_rows+1;
        ELSE
            decision:=CASE lower(coalesce(body->>'decision',body->>'review_decision',''))
                      WHEN 'approve' THEN 'approve' WHEN 'approved' THEN 'approve'
                      WHEN 'reject' THEN 'reject' WHEN 'rejected' THEN 'reject'
                      WHEN 'revise' THEN 'revise' WHEN 'revoke' THEN 'revoke' ELSE NULL END;
            IF decision IS NOT NULL THEN
                v_identity_key:=raw.source_table||':'||raw.source_pk;
                IF NOT EXISTS(SELECT 1 FROM migration.region_talk_canonical_identity
                              WHERE identity_kind='review_decision'
                                AND migration.region_talk_canonical_identity.identity_key=v_identity_key) THEN
                    INSERT INTO region_talk.review_decision(
                        candidate_id,candidate_revision,decision,actor_ref,reason,evidence,occurred_at
                    ) VALUES(v_candidate_id,v_candidate_revision,decision,
                             coalesce(NULLIF(body->>'actor_ref',''),'legacy-ydb-import'),body->>'reason',
                             jsonb_build_object('export_batch_id',requested_export_batch_id,
                                                'raw_record_id',raw.raw_record_id,'legacy_payload',body),
                             coalesce(raw.source_updated_at,clock_timestamp()))
                    RETURNING review_decision_id INTO v_work_id;
                    INSERT INTO migration.region_talk_canonical_identity(
                        identity_kind,identity_key,target_table,target_id,first_export_batch_id,source_raw_record_id
                    ) VALUES('review_decision',v_identity_key,'region_talk.review_decision',v_work_id,
                             requested_export_batch_id,raw.raw_record_id);
                END IF;
            END IF;
        END IF;
    END LOOP;

    affected:=content_rows+source_rows+candidate_rows+plan_rows;
    revision_after:=hub.advance_canonical_revision(revision_before);
    INSERT INTO sync.external_outbox(
        outbox_id,aggregate_type,aggregate_id,effect_kind,idempotency_key,payload,required_revision
    ) VALUES(v_outbox_id,'region_talk_snapshot',requested_export_batch_id,
             'region_talk.snapshot.canonicalized','region-talk-snapshot:'||requested_operation_id,
             jsonb_build_object('contract_version','region-talk-canonical-apply.v3',
                                'export_batch_id',requested_export_batch_id,
                                'task_run_id',requested_task_run_id,
                                'verified_logical_sha256',requested_verified_logical_sha256,
                                'canonical_revision',revision_after,'affected_rows',affected,
                                'publication_dispatch',false),revision_after);
    result:=jsonb_build_object(
        'schema_version','region-talk-canonical-apply-receipt.v3',
        'operation_id',requested_operation_id,'export_batch_id',requested_export_batch_id,
        'task_run_id',requested_task_run_id,'verified_logical_sha256',requested_verified_logical_sha256,
        'master_instance_id',snapshot.master_instance_id,'master_epoch',snapshot.master_epoch,
        'revision_before',revision_before,'revision_after',revision_after,'affected_rows',affected,
        'content_rows',content_rows,'source_rows',source_rows,'candidate_rows',candidate_rows,
        'plan_rows',plan_rows,'outbox_id',v_outbox_id,'publication_dispatch',false,
        'note','publication dispatch remains disabled','created_at',clock_timestamp());
    result_sha:=encode(sha256(convert_to(result::text,'UTF8')),'hex');
    INSERT INTO migration.region_talk_canonical_apply_receipt(
        operation_id,export_batch_id,task_run_id,request_sha256,verified_logical_sha256,
        master_instance_id,master_epoch,credential_id,revision_before,revision_after,affected_rows,
        content_rows,source_rows,candidate_rows,plan_rows,outbox_id,receipt,receipt_sha256
    ) VALUES(requested_operation_id,requested_export_batch_id,requested_task_run_id,requested_request_sha256,
             requested_verified_logical_sha256,snapshot.master_instance_id,snapshot.master_epoch,v_credential_id,
             revision_before,revision_after,affected,content_rows,source_rows,candidate_rows,plan_rows,
             v_outbox_id,result,result_sha);
    UPDATE migration.region_talk_direct_snapshot SET canonical_applied_at=clock_timestamp()
     WHERE export_batch_id=requested_export_batch_id;
    RETURN result;
END
$$;

CREATE OR REPLACE FUNCTION migration.finalize_region_talk_direct_snapshot(
    requested_export_batch_id uuid,
    requested_task_run_id uuid,
    requested_pass_b jsonb
) RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog
AS $$
DECLARE
    snapshot migration.region_talk_direct_snapshot%ROWTYPE;
    table_row jsonb; expected_table migration.region_talk_direct_snapshot_table%ROWTYPE;
    page migration.region_talk_direct_snapshot_page%ROWTYPE;
    v_ordinal integer:=0; actual_rows bigint; integrity_rows bigint; page_rows bigint;
    landed bigint; dispositioned bigint; quarantined bigint; observed_table_sha text;
    observed_page_sha text; observed_snapshot_sha text; final_state text; result jsonb;
BEGIN
    snapshot:=migration.assert_region_talk_direct_task(requested_export_batch_id,requested_task_run_id);
    IF requested_pass_b->>'schema_version'<>'region-talk-direct-pass-b.v2'
       OR requested_pass_b->>'logical_sha256' !~ '^[a-f0-9]{64}$'
       OR jsonb_typeof(requested_pass_b->'tables')<>'array'
       OR jsonb_array_length(requested_pass_b->'tables')<>5 THEN
        RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='direct snapshot pass B receipt is invalid';
    END IF;
    IF snapshot.state IN('complete','complete_with_quarantine') THEN
        -- A retry is idempotent only for the byte-identical Pass-B contract.
        -- Do not turn a completed batch into an oracle that accepts a caller's
        -- newly forged counts or hashes merely because a receipt already exists.
        v_ordinal:=0;
        FOR table_row IN SELECT value FROM jsonb_array_elements(requested_pass_b->'tables') LOOP
            v_ordinal:=v_ordinal+1;
            SELECT * INTO STRICT expected_table
              FROM migration.region_talk_direct_snapshot_table table_contract
             WHERE table_contract.export_batch_id=requested_export_batch_id
               AND table_contract.ordinal=v_ordinal;
            IF table_row->>'source_table'<>expected_table.source_table
               OR (table_row->>'row_count')::bigint IS DISTINCT FROM expected_table.pass_b_row_count
               OR table_row->>'logical_sha256' IS DISTINCT FROM expected_table.pass_b_logical_sha256 THEN
                RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='direct snapshot replay conflicts with verified Pass B';
            END IF;
        END LOOP;
        observed_snapshot_sha:=migration.region_talk_direct_snapshot_logical_sha256(requested_export_batch_id);
        IF requested_pass_b->>'logical_sha256' IS DISTINCT FROM snapshot.pass_b_logical_sha256
           OR observed_snapshot_sha IS DISTINCT FROM snapshot.verified_logical_sha256
           OR observed_snapshot_sha IS DISTINCT FROM snapshot.pass_b_logical_sha256 THEN
            RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='direct snapshot replay conflicts with persisted evidence';
        END IF;
        IF snapshot.state='complete' THEN
            PERFORM migration.canonicalize_region_talk_direct_snapshot(
                requested_export_batch_id,requested_task_run_id,snapshot.request_sha256,
                snapshot.request_sha256,snapshot.verified_logical_sha256);
        END IF;
        RETURN snapshot.receipt;
    END IF;
    IF snapshot.state<>'landing' THEN
        RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='direct snapshot is not finalizable';
    END IF;
    FOR table_row IN SELECT value FROM jsonb_array_elements(requested_pass_b->'tables') LOOP
        v_ordinal:=v_ordinal+1;
        SELECT * INTO STRICT expected_table FROM migration.region_talk_direct_snapshot_table table_contract
         WHERE table_contract.export_batch_id=requested_export_batch_id
           AND table_contract.ordinal=v_ordinal;
        SELECT count(*),count(integrity.raw_record_id) INTO actual_rows,integrity_rows
          FROM migration.raw_record raw
          LEFT JOIN migration.region_talk_direct_raw_integrity integrity USING(raw_record_id)
         WHERE raw.export_batch_id=requested_export_batch_id
           AND raw.source_table=expected_table.source_table;
        IF actual_rows<>integrity_rows OR EXISTS(
            SELECT 1 FROM migration.raw_record raw
            JOIN migration.region_talk_direct_raw_integrity integrity USING(raw_record_id)
            WHERE raw.export_batch_id=requested_export_batch_id
              AND raw.source_table=expected_table.source_table
              AND (encode(sha256(convert_to(integrity.payload_canonical_text,'UTF8')),'hex')<>raw.payload_sha256
                   OR integrity.payload_canonical_text::jsonb<>raw.payload
                   OR integrity.row_logical_sha256<>migration.region_talk_direct_row_logical_sha256(
                        raw.source_table,raw.source_pk,raw.row_kind,raw.source_updated_at,raw.payload_sha256))
        ) THEN
            RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='persisted direct row integrity evidence is inconsistent';
        END IF;
        observed_table_sha:=migration.region_talk_direct_table_logical_sha256(
            requested_export_batch_id,expected_table.source_table);
        SELECT coalesce(sum(snapshot_page.row_count),0) INTO page_rows
          FROM migration.region_talk_direct_snapshot_page snapshot_page
         WHERE snapshot_page.export_batch_id=requested_export_batch_id
           AND snapshot_page.source_table=expected_table.source_table;
        FOR page IN SELECT snapshot_page.* FROM migration.region_talk_direct_snapshot_page snapshot_page
                    WHERE snapshot_page.export_batch_id=requested_export_batch_id
                      AND snapshot_page.source_table=expected_table.source_table
                    ORDER BY snapshot_page.page_number LOOP
            observed_page_sha:=migration.region_talk_direct_page_persisted_sha256(
                requested_export_batch_id,expected_table.source_table,page.first_source_pk,page.last_source_pk);
            IF observed_page_sha<>page.logical_sha256
               OR page.submitted_logical_sha256<>page.logical_sha256 THEN
                RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='persisted page hash differs from server-recomputed evidence';
            END IF;
        END LOOP;
        IF table_row->>'source_table'<>expected_table.source_table
           OR (table_row->>'row_count')::bigint<>actual_rows
           OR table_row->>'logical_sha256'<>observed_table_sha
           OR actual_rows<>expected_table.pass_a_row_count
           OR page_rows<>actual_rows
           OR observed_table_sha<>expected_table.pass_a_logical_sha256 THEN
            RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='pass B differs from server-recomputed evidence';
        END IF;
        UPDATE migration.region_talk_direct_snapshot_table table_contract
           SET pass_b_row_count=actual_rows,pass_b_logical_sha256=observed_table_sha
         WHERE table_contract.export_batch_id=requested_export_batch_id
           AND table_contract.source_table=expected_table.source_table;
    END LOOP;
    observed_snapshot_sha:=migration.region_talk_direct_snapshot_logical_sha256(requested_export_batch_id);
    IF observed_snapshot_sha<>snapshot.pass_a_logical_sha256
       OR observed_snapshot_sha<>requested_pass_b->>'logical_sha256' THEN
        RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='snapshot hash differs from persisted table/page/raw evidence';
    END IF;
    IF EXISTS(SELECT 1 FROM migration.row_accounting accounting
              WHERE accounting.export_batch_id=requested_export_batch_id AND NOT accounting.fully_accounted) THEN
        RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='direct snapshot has undispositioned or missing rows';
    END IF;
    SELECT count(*),count(disposition.raw_record_id),
           count(*) FILTER(WHERE disposition.disposition='quarantined')
      INTO landed,dispositioned,quarantined
      FROM migration.raw_record raw
      LEFT JOIN migration.row_disposition disposition USING(raw_record_id)
     WHERE raw.export_batch_id=requested_export_batch_id;
    final_state:=CASE WHEN quarantined=0 THEN 'complete' ELSE 'complete_with_quarantine' END;
    result:=jsonb_build_object(
        'schema_version','region-talk-direct-snapshot-receipt.v2',
        'export_batch_id',requested_export_batch_id,'task_run_id',requested_task_run_id,
        'status',final_state,'expected_row_count',landed,'landed_row_count',landed,
        'dispositioned_row_count',dispositioned,'quarantined_row_count',quarantined,
        'logical_sha256',observed_snapshot_sha,'publication_effects_enabled',false,
        'completed_at',clock_timestamp());
    UPDATE migration.region_talk_direct_snapshot direct_snapshot
       SET pass_b_logical_sha256=observed_snapshot_sha,state=final_state,receipt=result,
           completed_at=clock_timestamp(),integrity_verified=true,
           verified_logical_sha256=observed_snapshot_sha
     WHERE direct_snapshot.export_batch_id=requested_export_batch_id;
    UPDATE migration.export_batch batch
       SET status=CASE WHEN quarantined=0 THEN 'accepted' ELSE 'reconciled' END,
           logical_sha256=observed_snapshot_sha,completed_at=clock_timestamp()
     WHERE batch.export_batch_id=requested_export_batch_id;
    IF quarantined=0 THEN
        PERFORM migration.canonicalize_region_talk_direct_snapshot(
            requested_export_batch_id,requested_task_run_id,snapshot.request_sha256,
            snapshot.request_sha256,observed_snapshot_sha);
    END IF;
    RETURN result;
END
$$;

CREATE OR REPLACE VIEW region_talk.accepted_snapshot_v2 AS
SELECT snapshot.export_batch_id,snapshot.task_run_id,snapshot.master_instance_id,snapshot.master_epoch,
       snapshot.verified_logical_sha256 AS logical_sha256,snapshot.completed_at,
       canonical_apply.revision_after AS canonical_revision
FROM migration.region_talk_direct_snapshot snapshot
JOIN migration.export_batch batch USING(export_batch_id)
JOIN migration.region_talk_canonical_apply_receipt canonical_apply USING(export_batch_id)
WHERE snapshot.state='complete' AND snapshot.integrity_verified
  AND snapshot.verified_logical_sha256=snapshot.pass_a_logical_sha256
  AND batch.status='accepted'
  AND NOT EXISTS(SELECT 1 FROM migration.row_disposition disposition
                 JOIN migration.raw_record raw USING(raw_record_id)
                 WHERE raw.export_batch_id=snapshot.export_batch_id
                   AND disposition.disposition='quarantined')
ORDER BY snapshot.completed_at DESC,snapshot.export_batch_id DESC
LIMIT 1;

CREATE OR REPLACE VIEW region_talk.snapshot_inventory_v2 AS
SELECT snapshot.export_batch_id,snapshot.task_run_id,snapshot.master_epoch,snapshot.state,
       batch.expected_row_count,count(raw.raw_record_id) AS landed_row_count,
       count(disposition.raw_record_id) AS dispositioned_row_count,
       count(*) FILTER(WHERE disposition.disposition='quarantined') AS quarantined_row_count,
       coalesce(snapshot.verified_logical_sha256,snapshot.pass_a_logical_sha256) AS logical_sha256,
       snapshot.created_at,snapshot.completed_at,
       snapshot.integrity_verified,canonical_apply.revision_after AS canonical_revision
FROM migration.region_talk_direct_snapshot snapshot
JOIN migration.export_batch batch USING(export_batch_id)
LEFT JOIN migration.raw_record raw USING(export_batch_id)
LEFT JOIN migration.row_disposition disposition USING(raw_record_id)
LEFT JOIN migration.region_talk_canonical_apply_receipt canonical_apply USING(export_batch_id)
GROUP BY snapshot.export_batch_id,snapshot.task_run_id,snapshot.master_epoch,snapshot.state,
         batch.expected_row_count,snapshot.pass_a_logical_sha256,snapshot.verified_logical_sha256,
         snapshot.integrity_verified,canonical_apply.revision_after,snapshot.created_at,snapshot.completed_at;

CREATE OR REPLACE VIEW region_talk.articles_v2 AS
WITH ranked AS (
 SELECT imported.item_id,imported.title,imported.summary,imported.body_text,
        coalesce(imported.exact_url,NULLIF(body.value->>'canonical_url','')) AS exact_url,
        imported.category,
        coalesce(imported.status,body.value->>'publication_status',body.value->>'status',body.value->>'state') AS status,
        imported.source_updated_at,imported.imported_at,
        row_number() OVER(PARTITION BY coalesce(
            NULLIF(btrim(coalesce(imported.exact_url,body.value->>'canonical_url')),''),
            raw.source_table||':'||raw.source_pk)
            ORDER BY imported.source_updated_at DESC NULLS LAST,raw.source_pk,imported.item_id) AS identity_rank
 FROM region_talk.imported_content_v2 imported
 JOIN migration.raw_record raw USING(raw_record_id)
 JOIN region_talk.accepted_snapshot_v2 accepted ON accepted.export_batch_id=raw.export_batch_id
 CROSS JOIN LATERAL(SELECT migration.region_talk_direct_body(raw.payload) AS value) body
 WHERE imported.content_family='article'
)
SELECT item_id,title,summary,body_text,exact_url,category,status,source_updated_at,imported_at
FROM ranked WHERE ranked.identity_rank=1;

CREATE OR REPLACE VIEW region_talk.posts_v2 AS
WITH ranked AS (
 SELECT imported.item_id,imported.title,imported.summary,imported.body_text,
        coalesce(imported.exact_url,NULLIF(body.value->>'canonical_url','')) AS exact_url,
        imported.platform,imported.external_id,imported.category,
        coalesce(imported.status,body.value->>'publication_status',body.value->>'status',body.value->>'state') AS status,
        imported.source_updated_at,imported.imported_at,
        row_number() OVER(PARTITION BY coalesce(
            CASE WHEN imported.platform IS NOT NULL AND imported.external_id IS NOT NULL
                 THEN imported.platform||':'||imported.external_id END,
            NULLIF(btrim(coalesce(imported.exact_url,body.value->>'canonical_url')),''),
            raw.source_table||':'||raw.source_pk)
            ORDER BY imported.source_updated_at DESC NULLS LAST,raw.source_pk,imported.item_id) AS identity_rank
 FROM region_talk.imported_content_v2 imported
 JOIN migration.raw_record raw USING(raw_record_id)
 JOIN region_talk.accepted_snapshot_v2 accepted ON accepted.export_batch_id=raw.export_batch_id
 CROSS JOIN LATERAL(SELECT migration.region_talk_direct_body(raw.payload) AS value) body
 WHERE imported.content_family='post'
)
SELECT item_id,title,summary,body_text,exact_url,platform,external_id,category,status,
       source_updated_at,imported_at
FROM ranked WHERE ranked.identity_rank=1;

CREATE OR REPLACE VIEW region_talk.queue_v2 AS
WITH ranked AS (
 SELECT imported.item_id,imported.queue_family,imported.source_ref,imported.lane,
        coalesce(imported.status,body.value->>'source_queue_status',body.value->>'queue_status',
                 body.value->>'image_queue_status',body.value->>'publication_status',
                 body.value->>'status',body.value->>'state') AS status,
        imported.priority_text,imported.available_at,imported.source_updated_at,imported.imported_at,
        row_number() OVER(PARTITION BY raw.source_table,raw.source_pk
                          ORDER BY imported.source_updated_at DESC NULLS LAST,imported.item_id) AS identity_rank
 FROM region_talk.imported_queue_v2 imported
 JOIN migration.raw_record raw USING(raw_record_id)
 JOIN region_talk.accepted_snapshot_v2 accepted ON accepted.export_batch_id=raw.export_batch_id
 CROSS JOIN LATERAL(SELECT migration.region_talk_direct_body(raw.payload) AS value) body
)
SELECT item_id,queue_family,source_ref,lane,status,priority_text,available_at,
       source_updated_at,imported_at
FROM ranked WHERE ranked.identity_rank=1;

CREATE OR REPLACE VIEW region_talk.queue_summary_v2 AS
SELECT queue_family,status,count(*) AS item_count,min(imported_at) AS oldest_imported_at,
       max(source_updated_at) AS latest_source_update
FROM region_talk.queue_v2 GROUP BY queue_family,status;

CREATE OR REPLACE VIEW region_talk.publication_queue_v3 AS
SELECT candidate.candidate_id,candidate.status AS candidate_status,candidate.current_revision,
       content.content_id,content.content_type,content.title,content.summary,content.canonical_url,
       plan.publication_plan_id,plan.channel,plan.status AS plan_status,plan.scheduled_for,
       plan.payload->>'legacy_status' AS legacy_status,accepted.canonical_revision,
       candidate.updated_at
FROM region_talk.accepted_snapshot_v2 accepted
JOIN hub.content_item content
  ON content.metadata->>'region_talk_snapshot_id'=accepted.export_batch_id::text
JOIN region_talk.publication_candidate candidate USING(content_id)
LEFT JOIN region_talk.publication_plan plan
  ON plan.candidate_id=candidate.candidate_id AND plan.candidate_revision=candidate.current_revision;

CREATE OR REPLACE VIEW region_talk.publication_queue_summary_v3 AS
SELECT candidate_status,plan_status,channel,count(*) AS item_count,
       min(scheduled_for) AS earliest_scheduled_for,max(updated_at) AS latest_update
FROM region_talk.publication_queue_v3
GROUP BY candidate_status,plan_status,channel;

REVOKE ALL ON migration.region_talk_direct_raw_integrity,
    migration.region_talk_task_credential_binding,migration.region_talk_canonical_identity,
    migration.region_talk_canonical_apply_receipt FROM PUBLIC,mdh_mcp_reader,mdh_mcp_editor;
REVOKE ALL ON FUNCTION migration.region_talk_direct_logical_component(text),
    migration.region_talk_direct_logical_timestamp(timestamptz),
    migration.region_talk_direct_row_logical_sha256(text,text,text,timestamptz,text),
    migration.region_talk_direct_page_persisted_sha256(uuid,text,text,text),
    migration.region_talk_direct_table_logical_sha256(uuid,text),
    migration.region_talk_direct_snapshot_logical_sha256(uuid),
    migration.region_talk_try_timestamptz(text),migration.region_talk_try_integer(text),
    migration.region_talk_ensure_content(uuid,uuid,uuid,text),
    migration.region_talk_ensure_source(uuid,uuid),
    migration.canonicalize_region_talk_direct_snapshot(uuid,uuid,text,text,text)
    FROM PUBLIC;
GRANT EXECUTE ON FUNCTION migration.land_region_talk_direct_page(uuid,uuid,jsonb),
    migration.finalize_region_talk_direct_snapshot(uuid,uuid,jsonb),
    migration.fail_region_talk_direct_snapshot(uuid,uuid,text)
    TO mdh_region_talk_pipeline;
GRANT SELECT ON region_talk.accepted_snapshot_v2,region_talk.snapshot_inventory_v2,
    region_talk.articles_v2,region_talk.posts_v2,region_talk.queue_v2,
    region_talk.queue_summary_v2,region_talk.publication_queue_v3,
    region_talk.publication_queue_summary_v3 TO mdh_mcp_reader;

UPDATE hub.canonical_state SET schema_revision=24,updated_at=clock_timestamp() WHERE singleton=true;
