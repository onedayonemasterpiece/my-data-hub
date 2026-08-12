-- Gate K: immutable embedding jobs/results stay on the ACTIVE master data plane.
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
    SELECT rolsuper INTO STRICT session_is_superuser
    FROM pg_roles
    WHERE rolname = session_user;
    guarded := NOT session_is_superuser AND (
        pg_has_role(session_user, 'mdh_application', 'member')
        OR pg_has_role(session_user, 'mdh_orchestrator', 'member')
        OR pg_has_role(session_user, 'mdh_connector_intake', 'member')
        OR pg_has_role(session_user, 'mdh_mcp_editor', 'member')
        OR pg_has_role(session_user, 'mdh_migration_operator', 'member')
        OR pg_has_role(session_user, 'mdh_canonical_committer', 'member')
        OR pg_has_role(session_user, 'mdh_embedding_worker', 'member')
    );
    IF NOT guarded THEN
        RETURN;
    END IF;
    SELECT * INTO STRICT state FROM master_control.epoch_state WHERE singleton = true;
    SELECT * INTO binding
    FROM master_control.credential_binding
    WHERE principal = session_user;
    IF NOT FOUND
       OR binding.revoked_at IS NOT NULL
       OR binding.expires_at <= observed_at
       OR state.gate_state <> 'open'
       OR state.lease_until <= observed_at
       OR binding.epoch IS DISTINCT FROM state.current_epoch
       OR binding.master_instance_id IS DISTINCT FROM state.master_instance_id THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'write rejected by epoch lease gate';
    END IF;
END
$$;


CREATE TABLE search.embedding_dispatch (
    request_id          uuid NOT NULL,
    task_run_id         uuid NOT NULL,
    model_exact_id      text NOT NULL,
    input_jobs_sha256   text NOT NULL CHECK (input_jobs_sha256 ~ '^[a-f0-9]{64}$'),
    jobs                jsonb NOT NULL CHECK (octet_length(jobs::text) BETWEEN 1 AND 4194304),
    state               text NOT NULL DEFAULT 'ready'
                        CHECK (state IN ('ready','claimed','result_available','completed','failed')),
    created_at          timestamptz NOT NULL DEFAULT clock_timestamp(),
    claimed_at          timestamptz,
    PRIMARY KEY (request_id, task_run_id),
    UNIQUE (task_run_id, input_jobs_sha256)
);

CREATE TABLE search.embedding_result_landing (
    request_id          uuid NOT NULL,
    task_run_id         uuid NOT NULL,
    input_jobs_sha256   text NOT NULL CHECK (input_jobs_sha256 ~ '^[a-f0-9]{64}$'),
    manifest_sha256     text NOT NULL CHECK (manifest_sha256 ~ '^[a-f0-9]{64}$'),
    manifest            jsonb NOT NULL CHECK (octet_length(manifest::text) BETWEEN 1 AND 33554432),
    received_at         timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (request_id, task_run_id),
    UNIQUE (task_run_id, manifest_sha256),
    FOREIGN KEY (request_id, task_run_id)
        REFERENCES search.embedding_dispatch(request_id, task_run_id) ON DELETE RESTRICT
);

CREATE TRIGGER embedding_result_landing_append_only
BEFORE UPDATE OR DELETE ON search.embedding_result_landing
FOR EACH ROW EXECUTE FUNCTION hub_meta.reject_update_delete();

CREATE CONSTRAINT TRIGGER mdh_epoch_write_guard
AFTER INSERT OR UPDATE OR DELETE ON search.embedding_dispatch
DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
EXECUTE FUNCTION master_control.enforce_write_epoch();

CREATE CONSTRAINT TRIGGER mdh_epoch_write_guard
AFTER INSERT OR UPDATE OR DELETE ON search.embedding_result_landing
DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
EXECUTE FUNCTION master_control.enforce_write_epoch();

CREATE FUNCTION search.stage_embedding_dispatch(
    requested_request_id uuid,
    requested_task_run_id uuid,
    requested_model_exact_id text,
    requested_input_sha256 text,
    requested_jobs jsonb
) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog
AS $$
DECLARE existing search.embedding_dispatch%ROWTYPE;
BEGIN
    IF requested_model_exact_id IS NULL OR length(requested_model_exact_id) NOT BETWEEN 3 AND 500
       OR requested_input_sha256 !~ '^[a-f0-9]{64}$'
       OR requested_jobs->>'schema_version' <> 'embedding-jobs-batch.v1'
       OR jsonb_typeof(requested_jobs->'jobs') <> 'array'
       OR jsonb_array_length(requested_jobs->'jobs') NOT BETWEEN 1 AND 10000
       OR octet_length(requested_jobs::text) > 4194304 THEN
        RAISE EXCEPTION USING ERRCODE='22023', MESSAGE='embedding dispatch violates its bounded contract';
    END IF;
    INSERT INTO search.embedding_dispatch(
        request_id,task_run_id,model_exact_id,input_jobs_sha256,jobs
    ) VALUES (
        requested_request_id,requested_task_run_id,requested_model_exact_id,
        requested_input_sha256,requested_jobs
    ) ON CONFLICT (request_id,task_run_id) DO NOTHING;
    SELECT * INTO STRICT existing FROM search.embedding_dispatch
      WHERE request_id=requested_request_id AND task_run_id=requested_task_run_id;
    IF existing.model_exact_id <> requested_model_exact_id
       OR existing.input_jobs_sha256 <> requested_input_sha256
       OR existing.jobs <> requested_jobs THEN
        RAISE EXCEPTION USING ERRCODE='23505', MESSAGE='embedding dispatch idempotency conflict';
    END IF;
END $$;

CREATE FUNCTION search.claim_embedding_dispatch(
    requested_request_id uuid, requested_task_run_id uuid, requested_input_sha256 text
) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog
AS $$
DECLARE dispatch search.embedding_dispatch%ROWTYPE;
BEGIN
    IF NOT pg_has_role(session_user, 'mdh_embedding_worker', 'member') THEN
        RAISE EXCEPTION USING ERRCODE='42501', MESSAGE='embedding worker role required';
    END IF;
    PERFORM master_control.assert_session_write_epoch();
    SELECT * INTO STRICT dispatch FROM search.embedding_dispatch
      WHERE request_id=requested_request_id AND task_run_id=requested_task_run_id FOR UPDATE;
    IF dispatch.input_jobs_sha256 <> requested_input_sha256
       OR dispatch.state NOT IN ('ready','claimed') THEN
        RAISE EXCEPTION USING ERRCODE='55000', MESSAGE='embedding dispatch is stale or unavailable';
    END IF;
    UPDATE search.embedding_dispatch SET state='claimed',claimed_at=coalesce(claimed_at,clock_timestamp())
      WHERE request_id=requested_request_id AND task_run_id=requested_task_run_id;
    RETURN dispatch.jobs;
END $$;

CREATE FUNCTION search.submit_embedding_result(
    requested_request_id uuid,
    requested_task_run_id uuid,
    requested_input_sha256 text,
    requested_manifest_sha256 text,
    requested_manifest jsonb
) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog
AS $$
DECLARE dispatch search.embedding_dispatch%ROWTYPE;
DECLARE existing search.embedding_result_landing%ROWTYPE;
BEGIN
    IF NOT pg_has_role(session_user, 'mdh_embedding_worker', 'member') THEN
        RAISE EXCEPTION USING ERRCODE='42501', MESSAGE='embedding worker role required';
    END IF;
    PERFORM master_control.assert_session_write_epoch();
    SELECT * INTO STRICT dispatch FROM search.embedding_dispatch
      WHERE request_id=requested_request_id AND task_run_id=requested_task_run_id FOR UPDATE;
    IF dispatch.input_jobs_sha256 <> requested_input_sha256
       OR requested_manifest_sha256 !~ '^[a-f0-9]{64}$'
       OR requested_manifest->>'schema_version' <> 'embedding-artifact-manifest.v1'
       OR requested_manifest->>'run_id' <> requested_task_run_id::text
       OR requested_manifest->>'input_jobs_sha256' <> requested_input_sha256
       OR octet_length(requested_manifest::text) > 33554432 THEN
        RAISE EXCEPTION USING ERRCODE='22023', MESSAGE='embedding result violates its dispatch contract';
    END IF;
    INSERT INTO search.embedding_result_landing(
        request_id,task_run_id,input_jobs_sha256,manifest_sha256,manifest
    ) VALUES (
        requested_request_id,requested_task_run_id,requested_input_sha256,
        requested_manifest_sha256,requested_manifest
    ) ON CONFLICT (request_id,task_run_id) DO NOTHING;
    SELECT * INTO STRICT existing FROM search.embedding_result_landing
      WHERE request_id=requested_request_id AND task_run_id=requested_task_run_id;
    IF existing.input_jobs_sha256 <> requested_input_sha256
       OR existing.manifest_sha256 <> requested_manifest_sha256
       OR existing.manifest <> requested_manifest THEN
        RAISE EXCEPTION USING ERRCODE='23505', MESSAGE='embedding result idempotency conflict';
    END IF;
    UPDATE search.embedding_dispatch SET state='result_available'
      WHERE request_id=requested_request_id AND task_run_id=requested_task_run_id;
END $$;

REVOKE ALL ON search.embedding_dispatch,search.embedding_result_landing FROM PUBLIC;
REVOKE ALL ON FUNCTION search.stage_embedding_dispatch(uuid,uuid,text,text,jsonb) FROM PUBLIC;
REVOKE ALL ON FUNCTION search.claim_embedding_dispatch(uuid,uuid,text) FROM PUBLIC;
REVOKE ALL ON FUNCTION search.submit_embedding_result(uuid,uuid,text,text,jsonb) FROM PUBLIC;
GRANT USAGE ON SCHEMA search TO mdh_embedding_worker;
GRANT EXECUTE ON FUNCTION search.stage_embedding_dispatch(uuid,uuid,text,text,jsonb) TO mdh_canonical_committer;
GRANT EXECUTE ON FUNCTION search.claim_embedding_dispatch(uuid,uuid,text) TO mdh_embedding_worker;
GRANT EXECUTE ON FUNCTION search.submit_embedding_result(uuid,uuid,text,text,jsonb) TO mdh_embedding_worker;
GRANT SELECT,UPDATE ON search.embedding_dispatch TO mdh_canonical_committer;
GRANT SELECT ON search.embedding_result_landing TO mdh_canonical_committer;

UPDATE hub.canonical_state SET schema_revision=19,updated_at=clock_timestamp() WHERE singleton=true;
