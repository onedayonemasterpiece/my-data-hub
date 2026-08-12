-- Append exact epoch/request identity to transactional operator receipts so a
-- control-plane acknowledgement loss can be reconciled without repeating DML.
ALTER TABLE operator_control.mcp_transaction_receipt
    ADD COLUMN request_sha256 text CHECK (request_sha256 ~ '^[a-f0-9]{64}$'),
    ADD COLUMN master_epoch bigint CHECK (master_epoch >= 1),
    ADD COLUMN master_instance_id uuid;

CREATE FUNCTION operator_control.commit_mcp_change_v2(
    requested_operation_id text,
    requested_request_sha256 text,
    expected_previous_revision bigint,
    requested_target text,
    requested_statement_kind text,
    requested_affected_rows integer,
    requested_sql_sha256 text,
    requested_parameters_sha256 text,
    requested_actor_id text,
    requested_client_id text
)
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
    session_is_superuser boolean;
    mutation operator_control.mcp_transaction_mutation%ROWTYPE;
    epoch_state master_control.epoch_state%ROWTYPE;
    next_revision bigint;
BEGIN
    SELECT rolsuper INTO STRICT session_is_superuser
    FROM pg_roles WHERE rolname = session_user;
    IF session_is_superuser OR NOT pg_has_role(session_user, 'mdh_mcp_editor', 'member') THEN
        RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'operator receipt requires exact editor login';
    END IF;
    PERFORM master_control.assert_session_write_epoch();
    SELECT * INTO STRICT epoch_state
    FROM master_control.epoch_state WHERE singleton;
    IF requested_operation_id IS NULL OR requested_operation_id !~ '^[a-f0-9]{64}$'
       OR requested_request_sha256 IS NULL OR requested_request_sha256 !~ '^[a-f0-9]{64}$'
       OR requested_target NOT IN ('hub.project', 'hub.content_item')
       OR requested_statement_kind NOT IN ('insert', 'update', 'delete')
       OR requested_affected_rows NOT BETWEEN 1 AND 1000
       OR requested_sql_sha256 IS NULL OR requested_sql_sha256 !~ '^[a-f0-9]{64}$'
       OR requested_parameters_sha256 IS NULL OR requested_parameters_sha256 !~ '^[a-f0-9]{64}$'
       OR requested_actor_id IS NULL OR length(requested_actor_id) NOT BETWEEN 1 AND 300
       OR requested_client_id IS NULL OR length(requested_client_id) NOT BETWEEN 1 AND 300
       OR epoch_state.current_epoch IS NULL OR epoch_state.master_instance_id IS NULL THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'operator receipt arguments are invalid';
    END IF;
    SELECT * INTO STRICT mutation
    FROM operator_control.mcp_transaction_mutation
    WHERE transaction_id = pg_current_xact_id();
    IF mutation.target <> requested_target
       OR mutation.statement_kind <> requested_statement_kind
       OR mutation.affected_rows <> requested_affected_rows THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'operator receipt differs from observed mutation';
    END IF;

    next_revision := hub.advance_canonical_revision(expected_previous_revision);
    INSERT INTO sync.audit_event (
        actor_id, client_id, action, outcome, subject_type, details
    ) VALUES (
        requested_actor_id, requested_client_id, 'mcp_operator_change', 'committed',
        requested_target,
        jsonb_build_object(
            'contract_version', 'mcp-operator-change.v2',
            'operation_id', requested_operation_id,
            'request_sha256', requested_request_sha256,
            'master_epoch', epoch_state.current_epoch,
            'master_instance_id', epoch_state.master_instance_id,
            'statement_kind', requested_statement_kind,
            'affected_rows', requested_affected_rows,
            'revision_before', expected_previous_revision,
            'revision_after', next_revision,
            'sql_sha256', requested_sql_sha256,
            'parameters_sha256', requested_parameters_sha256
        )
    );
    INSERT INTO sync.external_outbox (
        aggregate_type, effect_kind, idempotency_key, payload, required_revision
    ) VALUES (
        requested_target, 'canonical.operator_change',
        'mcp-operator:' || requested_operation_id,
        jsonb_build_object(
            'contract_version', 'mcp-operator-change.v2',
            'operation_id', requested_operation_id,
            'request_sha256', requested_request_sha256,
            'master_epoch', epoch_state.current_epoch,
            'master_instance_id', epoch_state.master_instance_id,
            'target', requested_target,
            'statement_kind', requested_statement_kind,
            'affected_rows', requested_affected_rows,
            'sql_sha256', requested_sql_sha256,
            'parameters_sha256', requested_parameters_sha256
        ),
        next_revision
    );
    INSERT INTO operator_control.mcp_transaction_receipt (
        transaction_id, operation_id, target, statement_kind, affected_rows,
        revision_before, revision_after, sql_sha256, parameters_sha256, actor_id, client_id,
        request_sha256, master_epoch, master_instance_id
    ) VALUES (
        pg_current_xact_id(), requested_operation_id, requested_target,
        requested_statement_kind, requested_affected_rows, expected_previous_revision,
        next_revision, requested_sql_sha256, requested_parameters_sha256,
        requested_actor_id, requested_client_id, requested_request_sha256,
        epoch_state.current_epoch, epoch_state.master_instance_id
    );
    RETURN next_revision;
END
$$;

CREATE FUNCTION operator_control.reconcile_mcp_change(
    requested_operation_id text,
    requested_request_sha256 text,
    requested_master_instance_id uuid,
    requested_master_epoch bigint,
    requested_previous_revision bigint,
    requested_actor_id text,
    requested_client_id text
)
RETURNS TABLE (
    affected_rows integer,
    revision_after bigint,
    committed_at timestamptz
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
    session_is_superuser boolean;
    receipt operator_control.mcp_transaction_receipt%ROWTYPE;
BEGIN
    SELECT rolsuper INTO STRICT session_is_superuser
    FROM pg_roles WHERE rolname = session_user;
    IF session_is_superuser OR NOT pg_has_role(session_user, 'mdh_mcp_editor', 'member') THEN
        RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'operator reconciliation requires exact editor login';
    END IF;
    PERFORM master_control.assert_session_write_epoch();
    IF requested_operation_id IS NULL OR requested_operation_id !~ '^[a-f0-9]{64}$'
       OR requested_request_sha256 IS NULL OR requested_request_sha256 !~ '^[a-f0-9]{64}$'
       OR requested_master_instance_id IS NULL OR requested_master_epoch < 1
       OR requested_previous_revision < 0
       OR requested_actor_id IS NULL OR length(requested_actor_id) NOT BETWEEN 1 AND 300
       OR requested_client_id IS NULL OR length(requested_client_id) NOT BETWEEN 1 AND 300 THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'operator reconciliation arguments are invalid';
    END IF;
    SELECT * INTO receipt
    FROM operator_control.mcp_transaction_receipt
    WHERE operation_id = requested_operation_id;
    IF NOT FOUND THEN
        RETURN;
    END IF;
    IF receipt.request_sha256 IS DISTINCT FROM requested_request_sha256
       OR receipt.master_instance_id IS DISTINCT FROM requested_master_instance_id
       OR receipt.master_epoch IS DISTINCT FROM requested_master_epoch
       OR receipt.revision_before IS DISTINCT FROM requested_previous_revision
       OR receipt.actor_id IS DISTINCT FROM requested_actor_id
       OR receipt.client_id IS DISTINCT FROM requested_client_id THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'operator receipt differs from exact reconciliation request';
    END IF;
    RETURN QUERY SELECT receipt.affected_rows, receipt.revision_after, receipt.created_at;
END
$$;

REVOKE ALL ON FUNCTION
    operator_control.commit_mcp_change_v2(text,text,bigint,text,text,integer,text,text,text,text),
    operator_control.reconcile_mcp_change(text,text,uuid,bigint,bigint,text,text)
    FROM PUBLIC;

UPDATE hub.canonical_state
SET schema_revision = 17,
    updated_at = clock_timestamp()
WHERE singleton = true;
