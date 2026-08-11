-- Exact remote MCP editor boundary.  The role may change only the two approved
-- canonical relations.  Per-transaction triggers make a same-transaction
-- canonical revision, semantic outbox operation and audit receipt mandatory.
CREATE TABLE operator_control.mcp_transaction_mutation (
    transaction_id         xid8 PRIMARY KEY,
    target                 text NOT NULL CHECK (target IN ('hub.project', 'hub.content_item')),
    statement_kind         text NOT NULL CHECK (statement_kind IN ('insert', 'update', 'delete')),
    affected_rows          integer NOT NULL CHECK (affected_rows BETWEEN 1 AND 1000),
    first_observed_at      timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE operator_control.mcp_transaction_receipt (
    transaction_id         xid8 PRIMARY KEY,
    operation_id           text NOT NULL UNIQUE CHECK (operation_id ~ '^[a-f0-9]{64}$'),
    target                 text NOT NULL CHECK (target IN ('hub.project', 'hub.content_item')),
    statement_kind         text NOT NULL CHECK (statement_kind IN ('insert', 'update', 'delete')),
    affected_rows          integer NOT NULL CHECK (affected_rows BETWEEN 1 AND 1000),
    revision_before        bigint NOT NULL CHECK (revision_before >= 0),
    revision_after         bigint NOT NULL CHECK (revision_after = revision_before + 1),
    sql_sha256             text NOT NULL CHECK (sql_sha256 ~ '^[a-f0-9]{64}$'),
    parameters_sha256      text NOT NULL CHECK (parameters_sha256 ~ '^[a-f0-9]{64}$'),
    actor_id               text NOT NULL CHECK (length(actor_id) BETWEEN 1 AND 300),
    client_id              text NOT NULL CHECK (length(client_id) BETWEEN 1 AND 300),
    created_at             timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TRIGGER mcp_transaction_receipt_append_only
BEFORE UPDATE OR DELETE ON operator_control.mcp_transaction_receipt
FOR EACH ROW EXECUTE FUNCTION hub_meta.reject_update_delete();

CREATE FUNCTION operator_control.track_mcp_canonical_mutation()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
    session_is_superuser boolean;
    exact_target text := TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME;
    exact_kind text := lower(TG_OP);
BEGIN
    SELECT rolsuper INTO STRICT session_is_superuser
    FROM pg_roles WHERE rolname = session_user;
    IF session_is_superuser OR NOT pg_has_role(session_user, 'mdh_mcp_editor', 'member') THEN
        RETURN NULL;
    END IF;
    PERFORM master_control.assert_session_write_epoch();
    INSERT INTO operator_control.mcp_transaction_mutation (
        transaction_id, target, statement_kind, affected_rows
    ) VALUES (
        pg_current_xact_id(), exact_target, exact_kind, 1
    )
    ON CONFLICT (transaction_id) DO UPDATE
    SET affected_rows = operator_control.mcp_transaction_mutation.affected_rows + 1
    WHERE operator_control.mcp_transaction_mutation.target = EXCLUDED.target
      AND operator_control.mcp_transaction_mutation.statement_kind = EXCLUDED.statement_kind
      AND operator_control.mcp_transaction_mutation.affected_rows < 1000;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING ERRCODE = '55000',
            MESSAGE = 'one operator transaction may contain only one bounded target/action';
    END IF;
    RETURN NULL;
END
$$;

CREATE FUNCTION operator_control.require_mcp_transaction_receipt()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
    session_is_superuser boolean;
BEGIN
    SELECT rolsuper INTO STRICT session_is_superuser
    FROM pg_roles WHERE rolname = session_user;
    IF session_is_superuser OR NOT pg_has_role(session_user, 'mdh_mcp_editor', 'member') THEN
        RETURN NULL;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM operator_control.mcp_transaction_receipt
        WHERE transaction_id = pg_current_xact_id()
    ) THEN
        RAISE EXCEPTION USING ERRCODE = '55000',
            MESSAGE = 'operator canonical change lacks transactional receipt/outbox';
    END IF;
    RETURN NULL;
END
$$;

CREATE FUNCTION operator_control.commit_mcp_change(
    requested_operation_id text,
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
    next_revision bigint;
BEGIN
    SELECT rolsuper INTO STRICT session_is_superuser
    FROM pg_roles WHERE rolname = session_user;
    IF session_is_superuser OR NOT pg_has_role(session_user, 'mdh_mcp_editor', 'member') THEN
        RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'operator receipt requires exact editor login';
    END IF;
    PERFORM master_control.assert_session_write_epoch();
    IF requested_operation_id IS NULL OR requested_operation_id !~ '^[a-f0-9]{64}$'
       OR requested_target NOT IN ('hub.project', 'hub.content_item')
       OR requested_statement_kind NOT IN ('insert', 'update', 'delete')
       OR requested_affected_rows NOT BETWEEN 1 AND 1000
       OR requested_sql_sha256 IS NULL OR requested_sql_sha256 !~ '^[a-f0-9]{64}$'
       OR requested_parameters_sha256 IS NULL OR requested_parameters_sha256 !~ '^[a-f0-9]{64}$'
       OR requested_actor_id IS NULL OR length(requested_actor_id) NOT BETWEEN 1 AND 300
       OR requested_client_id IS NULL OR length(requested_client_id) NOT BETWEEN 1 AND 300 THEN
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
            'operation_id', requested_operation_id,
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
            'contract_version', 'mcp-operator-change.v1',
            'operation_id', requested_operation_id,
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
        revision_before, revision_after, sql_sha256, parameters_sha256, actor_id, client_id
    ) VALUES (
        pg_current_xact_id(), requested_operation_id, requested_target,
        requested_statement_kind, requested_affected_rows, expected_previous_revision,
        next_revision, requested_sql_sha256, requested_parameters_sha256,
        requested_actor_id, requested_client_id
    );
    RETURN next_revision;
END
$$;

CREATE TRIGGER project_mcp_mutation_track
AFTER INSERT OR UPDATE OR DELETE ON hub.project
FOR EACH ROW EXECUTE FUNCTION operator_control.track_mcp_canonical_mutation();
CREATE CONSTRAINT TRIGGER project_mcp_receipt_required
AFTER INSERT OR UPDATE OR DELETE ON hub.project
DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
EXECUTE FUNCTION operator_control.require_mcp_transaction_receipt();

CREATE TRIGGER content_item_mcp_mutation_track
AFTER INSERT OR UPDATE OR DELETE ON hub.content_item
FOR EACH ROW EXECUTE FUNCTION operator_control.track_mcp_canonical_mutation();
CREATE CONSTRAINT TRIGGER content_item_mcp_receipt_required
AFTER INSERT OR UPDATE OR DELETE ON hub.content_item
DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
EXECUTE FUNCTION operator_control.require_mcp_transaction_receipt();

REVOKE ALL ON operator_control.mcp_transaction_mutation,
    operator_control.mcp_transaction_receipt FROM PUBLIC;
REVOKE ALL ON FUNCTION operator_control.track_mcp_canonical_mutation(),
    operator_control.require_mcp_transaction_receipt(),
    operator_control.commit_mcp_change(text,bigint,text,text,integer,text,text,text,text)
    FROM PUBLIC;

UPDATE hub.canonical_state
SET schema_revision = 16,
    updated_at = clock_timestamp()
WHERE singleton = true;
