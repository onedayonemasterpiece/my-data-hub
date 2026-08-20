-- Region Talk v4: master-registered DB task authority and ordered current-state refresh.
-- 0024 remains immutable.  This migration wraps its fixed procedures and preserves
-- every accepted current-state observation while updating only deterministic newer heads.

CREATE TABLE master_control.task_credential_registration (
    credential_id       uuid PRIMARY KEY
                        REFERENCES master_control.credential_binding(credential_id) ON DELETE RESTRICT,
    principal           name NOT NULL,
    worker_kind         text NOT NULL CHECK (worker_kind='region_talk'),
    task_run_id         uuid NOT NULL,
    generation          bigint NOT NULL CHECK (generation>=1),
    master_instance_id  uuid NOT NULL,
    epoch               bigint NOT NULL CHECK (epoch>=1),
    command_sha256      text NOT NULL CHECK (command_sha256 ~ '^[a-f0-9]{64}$'),
    task_token_sha256   text NOT NULL CHECK (task_token_sha256 ~ '^[a-f0-9]{64}$'),
    registered_at       timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(worker_kind,task_run_id,generation),
    UNIQUE(principal,credential_id)
);
CREATE TRIGGER task_credential_registration_append_only
BEFORE UPDATE OR DELETE ON master_control.task_credential_registration
FOR EACH ROW EXECUTE FUNCTION hub_meta.reject_update_delete();

CREATE FUNCTION master_control.register_task_credential_binding(
    requested_credential_id uuid,
    requested_principal name,
    requested_worker_kind text,
    requested_task_run_id uuid,
    requested_generation bigint,
    requested_master_instance_id uuid,
    requested_epoch bigint,
    requested_command_sha256 text,
    requested_task_token_sha256 text
) RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog
AS $$
DECLARE
    credential master_control.credential_binding%ROWTYPE;
    registered master_control.task_credential_registration%ROWTYPE;
BEGIN
    IF requested_worker_kind<>'region_talk' OR requested_task_run_id IS NULL
       OR requested_generation<1
       OR requested_command_sha256 !~ '^[a-f0-9]{64}$'
       OR requested_task_token_sha256 !~ '^[a-f0-9]{64}$' THEN
        RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='task credential registration is invalid';
    END IF;
    SELECT * INTO STRICT credential FROM master_control.credential_binding binding
     WHERE binding.credential_id=requested_credential_id FOR UPDATE;
    IF credential.principal<>requested_principal
       OR credential.master_instance_id<>requested_master_instance_id
       OR credential.epoch<>requested_epoch
       OR credential.revoked_at IS NOT NULL
       OR credential.expires_at<=clock_timestamp() THEN
        RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='task credential registration is outside live credential binding';
    END IF;
    INSERT INTO master_control.task_credential_registration(
        credential_id,principal,worker_kind,task_run_id,generation,master_instance_id,epoch,
        command_sha256,task_token_sha256
    ) VALUES(
        requested_credential_id,requested_principal,requested_worker_kind,requested_task_run_id,
        requested_generation,requested_master_instance_id,requested_epoch,
        requested_command_sha256,requested_task_token_sha256
    ) ON CONFLICT(credential_id) DO NOTHING;
    SELECT * INTO STRICT registered FROM master_control.task_credential_registration registration
     WHERE registration.credential_id=requested_credential_id;
    IF registered.principal<>requested_principal
       OR registered.worker_kind<>requested_worker_kind
       OR registered.task_run_id<>requested_task_run_id
       OR registered.generation<>requested_generation
       OR registered.master_instance_id<>requested_master_instance_id
       OR registered.epoch<>requested_epoch
       OR registered.command_sha256<>requested_command_sha256
       OR registered.task_token_sha256<>requested_task_token_sha256 THEN
        RAISE EXCEPTION USING ERRCODE='23505',MESSAGE='credential registration conflicts with immutable task binding';
    END IF;
    RETURN jsonb_build_object(
        'registered',true,
        'credential_id',registered.credential_id,
        'principal',registered.principal,
        'worker_kind',registered.worker_kind,
        'task_run_id',registered.task_run_id,
        'generation',registered.generation,
        'master_instance_id',registered.master_instance_id,
        'epoch',registered.epoch,
        'command_sha256',registered.command_sha256,
        'task_token_sha256',registered.task_token_sha256
    );
END
$$;

CREATE FUNCTION master_control.assert_registered_task_credential(
    requested_worker_kind text,requested_task_run_id uuid
) RETURNS master_control.task_credential_registration
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog
AS $$
DECLARE
    credential master_control.credential_binding%ROWTYPE;
    registration master_control.task_credential_registration%ROWTYPE;
BEGIN
    PERFORM master_control.assert_session_write_epoch();
    SELECT * INTO STRICT credential FROM master_control.credential_binding binding
     WHERE binding.principal=session_user AND binding.revoked_at IS NULL
       AND binding.expires_at>clock_timestamp();
    SELECT * INTO STRICT registration FROM master_control.task_credential_registration task_binding
     WHERE task_binding.credential_id=credential.credential_id
       AND task_binding.principal=session_user
       AND task_binding.worker_kind=requested_worker_kind
       AND task_binding.task_run_id=requested_task_run_id;
    IF registration.master_instance_id<>credential.master_instance_id
       OR registration.epoch<>credential.epoch THEN
        RAISE EXCEPTION USING ERRCODE='42501',MESSAGE='database credential is not registered for exact task and epoch';
    END IF;
    RETURN registration;
EXCEPTION WHEN NO_DATA_FOUND THEN
    RAISE EXCEPTION USING ERRCODE='42501',MESSAGE='database credential is not registered for exact task';
END
$$;

-- Keep every accepted observation immutable; update only this explicit current head.
CREATE TABLE migration.region_talk_canonical_state_head (
    identity_kind           text NOT NULL CHECK(identity_kind IN(
                                'source_candidate','source_status','work_item',
                                'publication_plan','review_decision')),
    identity_key            text NOT NULL,
    target_table            text NOT NULL,
    target_id               uuid NOT NULL,
    source_updated_at       timestamptz,
    payload_sha256          text NOT NULL CHECK(payload_sha256 ~ '^[a-f0-9]{64}$'),
    canonical_revision      bigint NOT NULL CHECK(canonical_revision>=1),
    export_batch_id         uuid NOT NULL REFERENCES migration.region_talk_direct_snapshot(export_batch_id),
    raw_record_id           uuid NOT NULL REFERENCES migration.raw_record(raw_record_id),
    updated_at              timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(identity_kind,identity_key)
);

CREATE TABLE migration.region_talk_canonical_state_observation (
    raw_record_id           uuid PRIMARY KEY REFERENCES migration.raw_record(raw_record_id) ON DELETE RESTRICT,
    identity_kind           text NOT NULL,
    identity_key            text NOT NULL,
    target_table            text NOT NULL,
    target_id               uuid NOT NULL,
    source_updated_at       timestamptz,
    payload_sha256          text NOT NULL CHECK(payload_sha256 ~ '^[a-f0-9]{64}$'),
    canonical_revision      bigint NOT NULL CHECK(canonical_revision>=1),
    export_batch_id         uuid NOT NULL REFERENCES migration.region_talk_direct_snapshot(export_batch_id),
    disposition             text NOT NULL CHECK(disposition IN('initial','applied','replay','stale')),
    prior_payload_sha256    text CHECK(prior_payload_sha256 IS NULL OR prior_payload_sha256 ~ '^[a-f0-9]{64}$'),
    observed_at             timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TRIGGER region_talk_canonical_state_observation_append_only
BEFORE UPDATE OR DELETE ON migration.region_talk_canonical_state_observation
FOR EACH ROW EXECUTE FUNCTION hub_meta.reject_update_delete();

CREATE FUNCTION migration.region_talk_claim_canonical_state(
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
                   source_updated_at=coalesce(incoming_source_updated_at,current_head.source_updated_at),
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

CREATE FUNCTION migration.refresh_region_talk_canonical_current_state(
    requested_export_batch_id uuid,requested_canonical_revision bigint
) RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog
AS $$
DECLARE
    raw migration.raw_record%ROWTYPE; body jsonb; v_key text; v_target_id uuid;
    v_source_id uuid; v_candidate_id uuid; v_candidate_revision integer;
    v_status text; v_old_status text; v_channel text; v_apply boolean;
    applied_count bigint:=0; stale_count bigint:=0; replay_count bigint:=0;
BEGIN
    IF NOT EXISTS(
        SELECT 1 FROM migration.region_talk_canonical_apply_receipt receipt
         JOIN migration.region_talk_direct_snapshot snapshot USING(export_batch_id)
         WHERE receipt.export_batch_id=requested_export_batch_id
           AND receipt.revision_after=requested_canonical_revision
           AND snapshot.state='complete' AND snapshot.integrity_verified
    ) THEN
        RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='current-state refresh requires exact canonical receipt';
    END IF;

    FOR raw IN SELECT source_row.* FROM migration.raw_record source_row
               WHERE source_row.export_batch_id=requested_export_batch_id
                 AND source_row.row_kind IN(
                    'source_candidate_item','source_status_item','source_queue_item',
                    'publication_schedule_item','external_publication_review_item',
                    'external_publication_review_state_item','external_publication_review_event_item',
                    'publication_review_state_item','publication_review_event_item')
               ORDER BY source_row.source_table,source_row.source_pk LOOP
        body:=migration.region_talk_direct_body(raw.payload);
        v_key:=raw.source_table||':'||raw.source_pk;

        IF raw.row_kind='source_candidate_item' THEN
            SELECT identity.target_id INTO STRICT v_target_id
              FROM migration.region_talk_canonical_identity identity
             WHERE identity.identity_kind='source_candidate' AND identity.identity_key=v_key;
            v_apply:=migration.region_talk_claim_canonical_state(
                'source_candidate',v_key,'region_talk.source_candidate',v_target_id,raw.raw_record_id,
                requested_export_batch_id,raw.source_updated_at,raw.payload_sha256,requested_canonical_revision);
            IF v_apply THEN
                v_status:=CASE lower(coalesce(body->>'status',body->>'state',''))
                    WHEN 'resolved' THEN 'resolved' WHEN 'accepted' THEN 'accepted'
                    WHEN 'rejected' THEN 'rejected' WHEN 'duplicate' THEN 'duplicate'
                    WHEN 'quarantined' THEN 'quarantined' ELSE 'pending' END;
                UPDATE region_talk.source_candidate candidate
                   SET candidate_url=NULLIF(btrim(coalesce(body->>'candidate_url',body->>'url',body->>'canonical_url')),''),
                       candidate_handle=coalesce(NULLIF(btrim(coalesce(body->>'candidate_handle',body->>'handle')),''),
                           CASE WHEN NULLIF(btrim(coalesce(body->>'candidate_url',body->>'url',body->>'canonical_url')),'') IS NULL
                                THEN 'legacy-ydb:'||raw.source_pk END),
                       discovered_by=coalesce(NULLIF(body->>'discovered_by',''),'legacy_ydb_import'),
                       status=v_status,
                       evidence=candidate.evidence||jsonb_build_object(
                           'export_batch_id',requested_export_batch_id,'raw_record_id',raw.raw_record_id,
                           'legacy_status',coalesce(body->>'status',body->>'state'))
                 WHERE candidate.source_candidate_id=v_target_id;
                applied_count:=applied_count+1;
            END IF;

        ELSIF raw.row_kind='source_status_item' THEN
            v_source_id:=migration.region_talk_ensure_source(raw.raw_record_id,requested_export_batch_id);
            v_apply:=migration.region_talk_claim_canonical_state(
                'source_status',v_key,'region_talk.source',v_source_id,raw.raw_record_id,
                requested_export_batch_id,raw.source_updated_at,raw.payload_sha256,requested_canonical_revision);
            IF v_apply THEN
                v_status:=lower(coalesce(body->>'source_queue_status',body->>'queue_status',body->>'status',body->>'state','unknown'));
                INSERT INTO region_talk.source_status(source_id,status,reason,gate_version,evidence,occurred_at)
                VALUES(v_source_id,v_status,coalesce(body->>'reason',body->>'primary_reason'),body->>'gate_version',
                       jsonb_build_object('export_batch_id',requested_export_batch_id,
                                          'raw_record_id',raw.raw_record_id,'ordered_current_state',true),
                       coalesce(raw.source_updated_at,clock_timestamp()));
                UPDATE region_talk.source source
                   SET status=CASE v_status WHEN 'active' THEN 'active' WHEN 'paused' THEN 'paused'
                              WHEN 'excluded' THEN 'excluded' WHEN 'terminal' THEN 'terminal'
                              ELSE source.status END,
                       evidence=source.evidence||jsonb_build_object(
                           'current_status_raw_record_id',raw.raw_record_id,
                           'current_status_export_batch_id',requested_export_batch_id)
                 WHERE source.source_id=v_source_id;
                applied_count:=applied_count+1;
            END IF;

        ELSIF raw.row_kind='source_queue_item' THEN
            SELECT identity.target_id INTO STRICT v_target_id
              FROM migration.region_talk_canonical_identity identity
             WHERE identity.identity_kind='work_item' AND identity.identity_key=v_key;
            v_apply:=migration.region_talk_claim_canonical_state(
                'work_item',v_key,'orchestration.work_item',v_target_id,raw.raw_record_id,
                requested_export_batch_id,raw.source_updated_at,raw.payload_sha256,requested_canonical_revision);
            IF v_apply THEN
                SELECT work.status INTO STRICT v_old_status FROM orchestration.work_item work
                 WHERE work.work_item_id=v_target_id FOR UPDATE;
                v_status:=CASE lower(coalesce(body->>'source_queue_status',body->>'queue_status',body->>'status',body->>'state',''))
                    WHEN 'succeeded' THEN 'succeeded' WHEN 'completed' THEN 'succeeded'
                    WHEN 'failed_terminal' THEN 'failed_terminal' WHEN 'quarantined' THEN 'quarantined'
                    WHEN 'cancelled' THEN 'cancelled' WHEN 'failed' THEN 'failed_retryable'
                    ELSE 'pending' END;
                UPDATE orchestration.work_item work
                   SET input_fingerprint=raw.payload_sha256,
                       priority=coalesce(migration.region_talk_try_integer(body->>'priority'),100),
                       payload=jsonb_build_object('legacy_payload',body,'export_batch_id',requested_export_batch_id,
                                                  'publication_dispatch',false),
                       status=v_status,
                       available_at=coalesce(migration.region_talk_try_timestamptz(
                           coalesce(body->>'available_at',body->>'retry_at')),clock_timestamp()),
                       lease_owner=NULL,lease_token=NULL,lease_expires_at=NULL
                 WHERE work.work_item_id=v_target_id;
                INSERT INTO orchestration.work_item_event(
                    work_item_id,event_kind,from_status,to_status,actor_kind,actor_ref,reason,evidence,occurred_at
                ) VALUES(v_target_id,'legacy_snapshot_state_changed',v_old_status,v_status,'system',
                         'region-talk-direct-pipeline','ordered_current_state',
                         jsonb_build_object('export_batch_id',requested_export_batch_id,
                                            'raw_record_id',raw.raw_record_id,'payload_sha256',raw.payload_sha256),
                         coalesce(raw.source_updated_at,clock_timestamp()));
                UPDATE region_talk.source_work_projection projection
                   SET priority_lane=CASE body->>'priority_lane' WHEN 'head_repair' THEN 'head_repair'
                            WHEN 'exact' THEN 'exact' WHEN 'cached' THEN 'cached' WHEN 'resolve' THEN 'resolve'
                            WHEN 'exploration' THEN 'exploration' ELSE 'normal' END,
                       priority_score=coalesce(migration.region_talk_try_integer(body->>'priority_score'),0),
                       priority_reason=body->>'priority_reason',
                       readiness_state=CASE body->>'readiness_state' WHEN 'actionable_cached' THEN 'actionable_cached'
                            WHEN 'needs_entity_resolve' THEN 'needs_entity_resolve' WHEN 'head_repair' THEN 'head_repair'
                            WHEN 'cooldown' THEN 'cooldown' WHEN 'retry' THEN 'retry' WHEN 'scan_due' THEN 'scan_due'
                            WHEN 'access_denied' THEN 'access_denied' WHEN 'not_found' THEN 'not_found'
                            WHEN 'terminal' THEN 'terminal' ELSE 'unknown' END
                 WHERE projection.work_item_id=v_target_id;
                applied_count:=applied_count+1;
            END IF;

        ELSIF raw.row_kind='publication_schedule_item' THEN
            SELECT identity.target_id INTO STRICT v_target_id
              FROM migration.region_talk_canonical_identity identity
             WHERE identity.identity_kind='publication_plan' AND identity.identity_key=v_key;
            v_apply:=migration.region_talk_claim_canonical_state(
                'publication_plan',v_key,'region_talk.publication_plan',v_target_id,raw.raw_record_id,
                requested_export_batch_id,raw.source_updated_at,raw.payload_sha256,requested_canonical_revision);
            IF v_apply THEN
                SELECT plan.candidate_id INTO STRICT v_candidate_id FROM region_talk.publication_plan plan
                 WHERE plan.publication_plan_id=v_target_id FOR UPDATE;
                SELECT candidate.current_revision INTO STRICT v_candidate_revision
                  FROM region_talk.publication_candidate candidate WHERE candidate.candidate_id=v_candidate_id;
                v_status:=CASE lower(coalesce(body->>'publication_status',body->>'status',body->>'state',''))
                    WHEN 'queued' THEN 'queued' WHEN 'published' THEN 'published'
                    WHEN 'failed' THEN 'failed' WHEN 'cancelled' THEN 'cancelled' ELSE 'planned' END;
                v_channel:=coalesce(NULLIF(body->>'channel',''),NULLIF(body->>'target_channel',''),
                                    'region-talk-new-channel');
                UPDATE region_talk.publication_plan plan
                   SET candidate_revision=v_candidate_revision,channel=v_channel,status=v_status,
                       scheduled_for=migration.region_talk_try_timestamptz(
                           coalesce(body->>'scheduled_for',body->>'publish_at')),
                       payload=jsonb_build_object('legacy_payload',body,'export_batch_id',requested_export_batch_id,
                                                  'raw_record_id',raw.raw_record_id,'publication_dispatch',false,
                                                  'legacy_status',coalesce(body->>'publication_status',body->>'status',body->>'state'))
                 WHERE plan.publication_plan_id=v_target_id;
                applied_count:=applied_count+1;
            END IF;

        ELSE
            SELECT candidate.candidate_id,candidate.current_revision
              INTO STRICT v_candidate_id,v_candidate_revision
              FROM migration.region_talk_canonical_identity content_identity
              JOIN region_talk.publication_candidate candidate
                ON candidate.content_id=content_identity.target_id
              JOIN hub.project project ON project.project_id=candidate.project_id
             WHERE content_identity.identity_kind='content'
               AND project.slug::text='region-talk'
               AND content_identity.identity_key=CASE
                    WHEN NULLIF(btrim(coalesce(body->>'exact_url',body->>'canonical_url',body->>'url',body->>'source_url')),'') IS NOT NULL
                    THEN 'exact-url:'||NULLIF(btrim(coalesce(body->>'exact_url',body->>'canonical_url',body->>'url',body->>'source_url')),'')
                    ELSE 'source:'||raw.source_table||':'||raw.source_pk END
             ORDER BY candidate.updated_at DESC LIMIT 1;
            v_apply:=migration.region_talk_claim_canonical_state(
                'review_decision',v_key,'region_talk.publication_candidate',v_candidate_id,raw.raw_record_id,
                requested_export_batch_id,raw.source_updated_at,raw.payload_sha256,requested_canonical_revision);
            IF v_apply THEN
                v_status:=CASE lower(coalesce(body->>'decision',body->>'review_decision',''))
                    WHEN 'approve' THEN 'approve' WHEN 'approved' THEN 'approve'
                    WHEN 'reject' THEN 'reject' WHEN 'rejected' THEN 'reject'
                    WHEN 'revise' THEN 'revise' WHEN 'revoke' THEN 'revoke' ELSE NULL END;
                IF v_status IS NOT NULL THEN
                    INSERT INTO region_talk.review_decision(
                        candidate_id,candidate_revision,decision,actor_ref,reason,evidence,occurred_at
                    ) VALUES(v_candidate_id,v_candidate_revision,v_status,
                             coalesce(NULLIF(body->>'actor_ref',''),'legacy-ydb-import'),body->>'reason',
                             jsonb_build_object('export_batch_id',requested_export_batch_id,
                                                'raw_record_id',raw.raw_record_id,'legacy_payload',body,
                                                'ordered_current_state',true),
                             coalesce(raw.source_updated_at,clock_timestamp()));
                    UPDATE region_talk.publication_candidate candidate
                       SET status=CASE v_status WHEN 'approve' THEN 'approved' WHEN 'reject' THEN 'rejected'
                                      WHEN 'revise' THEN 'in_review' WHEN 'revoke' THEN 'revoked' END
                     WHERE candidate.candidate_id=v_candidate_id;
                END IF;
                applied_count:=applied_count+1;
            END IF;
        END IF;
        IF NOT v_apply THEN
            IF EXISTS(SELECT 1 FROM migration.region_talk_canonical_state_observation observation
                      WHERE observation.raw_record_id=raw.raw_record_id AND observation.disposition='stale')
            THEN stale_count:=stale_count+1; ELSE replay_count:=replay_count+1; END IF;
        END IF;
    END LOOP;
    RETURN jsonb_build_object('applied_count',applied_count,'stale_count',stale_count,
                              'replay_count',replay_count,'canonical_revision',requested_canonical_revision);
END
$$;

-- Replace first-use task binding by wrappers around the immutable v2/v3 bodies.
ALTER FUNCTION migration.begin_region_talk_direct_snapshot(jsonb)
    RENAME TO begin_region_talk_direct_snapshot_v2_unbound;
CREATE FUNCTION migration.begin_region_talk_direct_snapshot(requested_manifest jsonb)
RETURNS uuid
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog
AS $$
DECLARE requested_task_run_id uuid;
BEGIN
    BEGIN requested_task_run_id:=(requested_manifest->>'task_run_id')::uuid;
    EXCEPTION WHEN OTHERS THEN
        RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='direct snapshot task id is invalid';
    END;
    PERFORM master_control.assert_registered_task_credential('region_talk',requested_task_run_id);
    RETURN migration.begin_region_talk_direct_snapshot_v2_unbound(requested_manifest);
END
$$;

ALTER FUNCTION migration.assert_region_talk_direct_task(uuid,uuid)
    RENAME TO assert_region_talk_direct_task_v3_unbound;
CREATE FUNCTION migration.assert_region_talk_direct_task(
    requested_export_batch_id uuid,requested_task_run_id uuid
) RETURNS migration.region_talk_direct_snapshot
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog
AS $$
BEGIN
    PERFORM master_control.assert_registered_task_credential('region_talk',requested_task_run_id);
    RETURN migration.assert_region_talk_direct_task_v3_unbound(
        requested_export_batch_id,requested_task_run_id);
END
$$;

ALTER FUNCTION migration.canonicalize_region_talk_direct_snapshot(uuid,uuid,text,text,text)
    RENAME TO canonicalize_region_talk_direct_snapshot_v3;
CREATE FUNCTION migration.canonicalize_region_talk_direct_snapshot(
    requested_export_batch_id uuid,requested_task_run_id uuid,requested_operation_id text,
    requested_request_sha256 text,requested_verified_logical_sha256 text
) RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path=pg_catalog
AS $$
DECLARE receipt jsonb; revision_after bigint;
BEGIN
    receipt:=migration.canonicalize_region_talk_direct_snapshot_v3(
        requested_export_batch_id,requested_task_run_id,requested_operation_id,
        requested_request_sha256,requested_verified_logical_sha256);
    revision_after:=(receipt->>'revision_after')::bigint;
    PERFORM migration.refresh_region_talk_canonical_current_state(
        requested_export_batch_id,revision_after);
    RETURN receipt;
END
$$;

-- The product queue includes the latest immutable review evidence, not merely
-- the candidate/plan projection. Existing columns remain in their v3 order and
-- review columns are appended for CREATE OR REPLACE compatibility.
CREATE OR REPLACE VIEW region_talk.publication_queue_v3 AS
SELECT candidate.candidate_id,candidate.status AS candidate_status,candidate.current_revision,
       content.content_id,content.content_type,content.title,content.summary,content.canonical_url,
       plan.publication_plan_id,plan.channel,plan.status AS plan_status,plan.scheduled_for,
       plan.payload->>'legacy_status' AS legacy_status,accepted.canonical_revision,
       candidate.updated_at,
       review.decision AS review_decision,review.actor_ref AS review_actor_ref,
       review.reason AS review_reason,review.occurred_at AS review_occurred_at
FROM region_talk.accepted_snapshot_v2 accepted
JOIN hub.content_item content
  ON content.metadata->>'region_talk_snapshot_id'=accepted.export_batch_id::text
JOIN region_talk.publication_candidate candidate USING(content_id)
LEFT JOIN region_talk.publication_plan plan
  ON plan.candidate_id=candidate.candidate_id AND plan.candidate_revision=candidate.current_revision
LEFT JOIN LATERAL(
    SELECT decision_row.decision,decision_row.actor_ref,decision_row.reason,decision_row.occurred_at
      FROM region_talk.review_decision decision_row
     WHERE decision_row.candidate_id=candidate.candidate_id
       AND decision_row.candidate_revision=candidate.current_revision
     ORDER BY decision_row.occurred_at DESC,decision_row.review_decision_id DESC LIMIT 1
) review ON true;

REVOKE ALL ON master_control.task_credential_registration,
    migration.region_talk_canonical_state_head,
    migration.region_talk_canonical_state_observation
    FROM PUBLIC,mdh_mcp_reader,mdh_mcp_editor,mdh_region_talk_pipeline;
REVOKE EXECUTE ON FUNCTION
    migration.begin_region_talk_direct_snapshot_v2_unbound(jsonb),
    migration.assert_region_talk_direct_task_v3_unbound(uuid,uuid),
    migration.canonicalize_region_talk_direct_snapshot_v3(uuid,uuid,text,text,text),
    migration.region_talk_claim_canonical_state(text,text,text,uuid,uuid,uuid,timestamptz,text,bigint),
    migration.refresh_region_talk_canonical_current_state(uuid,bigint),
    master_control.assert_registered_task_credential(text,uuid)
    FROM PUBLIC,mdh_region_talk_pipeline;
REVOKE EXECUTE ON FUNCTION master_control.register_task_credential_binding(
    uuid,name,text,uuid,bigint,uuid,bigint,text,text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION master_control.register_task_credential_binding(
    uuid,name,text,uuid,bigint,uuid,bigint,text,text) TO mdh_owner,mdh_master_controller;
GRANT EXECUTE ON FUNCTION migration.begin_region_talk_direct_snapshot(jsonb),
    migration.land_region_talk_direct_page(uuid,uuid,jsonb),
    migration.finalize_region_talk_direct_snapshot(uuid,uuid,jsonb),
    migration.fail_region_talk_direct_snapshot(uuid,uuid,text)
    TO mdh_region_talk_pipeline;
GRANT SELECT ON region_talk.publication_queue_v3,
    region_talk.publication_queue_summary_v3 TO mdh_mcp_reader;

UPDATE hub.canonical_state SET schema_revision=25,updated_at=clock_timestamp() WHERE singleton=true;
