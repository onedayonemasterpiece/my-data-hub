-- Preserve the immutable PostgreSQL commit timestamp in reconciliation receipts.
-- 0020 remains append-only; the fixed function is replaced only to widen its
-- named return contract with the already-stored committed_at value.
DROP FUNCTION integration.reconcile_blogger_discovery(text,text,text,uuid,bigint,bigint,text,text);

CREATE FUNCTION integration.reconcile_blogger_discovery(
    requested_operation_id text,
    requested_request_sha256 text,
    requested_plan_sha256 text,
    requested_master_instance_id uuid,
    requested_master_epoch bigint,
    requested_previous_revision bigint,
    requested_principal_id text,
    requested_client_id text
)
RETURNS TABLE (
    operation_id text,
    batch_id uuid,
    plan_sha256 text,
    affected_rows integer,
    revision_after bigint,
    committed_at timestamptz,
    duplicate boolean
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
    session_is_superuser boolean;
    receipt integration.blogger_discovery_apply_receipt%ROWTYPE;
BEGIN
    SELECT rolsuper INTO STRICT session_is_superuser FROM pg_roles WHERE rolname = session_user;
    IF session_is_superuser OR NOT pg_has_role(session_user, 'mdh_canonical_committer', 'member') THEN
        RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'blogger reconciliation requires exact canonical committer login';
    END IF;
    PERFORM master_control.assert_session_write_epoch();
    SELECT * INTO receipt FROM integration.blogger_discovery_apply_receipt
    WHERE integration.blogger_discovery_apply_receipt.operation_id = requested_operation_id;
    IF NOT FOUND THEN RETURN; END IF;
    IF receipt.request_sha256 <> requested_request_sha256
       OR receipt.plan_sha256 <> requested_plan_sha256
       OR receipt.master_instance_id <> requested_master_instance_id
       OR receipt.master_epoch <> requested_master_epoch
       OR receipt.revision_before <> requested_previous_revision
       OR receipt.principal_id <> requested_principal_id
       OR receipt.client_id <> requested_client_id THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'blogger receipt differs from exact reconciliation request';
    END IF;
    RETURN QUERY SELECT receipt.operation_id, receipt.batch_id, receipt.plan_sha256,
        receipt.affected_rows, receipt.revision_after, receipt.committed_at, true;
END
$$;

REVOKE ALL ON FUNCTION
    integration.reconcile_blogger_discovery(text,text,text,uuid,bigint,bigint,text,text)
    FROM PUBLIC;
GRANT EXECUTE ON FUNCTION
    integration.reconcile_blogger_discovery(text,text,text,uuid,bigint,bigint,text,text)
    TO mdh_canonical_committer;

UPDATE hub.canonical_state
SET schema_revision = 21, updated_at = clock_timestamp()
WHERE singleton = true;
