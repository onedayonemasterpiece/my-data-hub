-- Master-local monotonic epochs, leased write gate, and epoch-bound credentials.
-- This schema lives only in the ACTIVE Kaggle PostgreSQL primary.  The devstand
-- control ledger keeps its own projection but cannot bypass these data-plane checks.
CREATE SCHEMA master_control;

CREATE TABLE master_control.epoch_state (
    singleton               boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    highest_epoch           bigint NOT NULL DEFAULT 0 CHECK (highest_epoch >= 0),
    current_epoch           bigint,
    master_instance_id      uuid,
    source_run_id           text,
    lease_until             timestamptz,
    gate_state              text NOT NULL DEFAULT 'closed'
                            CHECK (gate_state IN ('closed', 'open', 'draining', 'fenced')),
    reason                  text NOT NULL DEFAULT 'empty_bootstrap',
    updated_at              timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (
        (current_epoch IS NULL AND master_instance_id IS NULL AND source_run_id IS NULL AND lease_until IS NULL)
        OR
        (current_epoch IS NOT NULL AND master_instance_id IS NOT NULL AND source_run_id IS NOT NULL AND lease_until IS NOT NULL)
    ),
    CHECK (current_epoch IS NULL OR current_epoch <= highest_epoch)
);

INSERT INTO master_control.epoch_state (singleton) VALUES (true);

CREATE TABLE master_control.epoch_event (
    event_id                uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    epoch                   bigint NOT NULL CHECK (epoch >= 1),
    master_instance_id      uuid NOT NULL,
    source_run_id           text NOT NULL,
    event_kind              text NOT NULL CHECK (
                                event_kind IN ('registered', 'opened', 'renewed', 'closed', 'draining', 'fenced')
                            ),
    lease_until             timestamptz NOT NULL,
    reason                  text NOT NULL,
    occurred_at             timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX epoch_event_epoch_time_idx
    ON master_control.epoch_event (epoch, occurred_at);
CREATE TRIGGER epoch_event_append_only
BEFORE UPDATE OR DELETE ON master_control.epoch_event
FOR EACH ROW EXECUTE FUNCTION hub_meta.reject_update_delete();

CREATE TABLE master_control.credential_binding (
    credential_id           uuid PRIMARY KEY,
    principal               name NOT NULL UNIQUE,
    epoch                   bigint NOT NULL CHECK (epoch >= 1),
    master_instance_id      uuid NOT NULL,
    expires_at              timestamptz NOT NULL,
    revoked_at              timestamptz,
    revocation_reason       text,
    created_at              timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (
        (revoked_at IS NULL AND revocation_reason IS NULL)
        OR (revoked_at IS NOT NULL AND revocation_reason IS NOT NULL)
    )
);
CREATE INDEX credential_binding_epoch_idx
    ON master_control.credential_binding (epoch, expires_at)
    WHERE revoked_at IS NULL;

CREATE FUNCTION master_control.begin_epoch(
    requested_instance uuid,
    requested_run_id text,
    requested_epoch bigint,
    requested_lease_until timestamptz
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
    state master_control.epoch_state%ROWTYPE;
    observed_at timestamptz := clock_timestamp();
BEGIN
    IF requested_instance IS NULL OR requested_run_id IS NULL OR length(requested_run_id) NOT BETWEEN 1 AND 256 THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'master identity is invalid';
    END IF;
    IF requested_lease_until IS NULL OR requested_lease_until <= observed_at THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'lease deadline must be in the future';
    END IF;
    SELECT * INTO STRICT state FROM master_control.epoch_state WHERE singleton = true FOR UPDATE;
    IF state.current_epoch IS NOT NULL
       AND state.gate_state NOT IN ('fenced', 'draining')
       AND state.lease_until > observed_at THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'another master has an unexpired lease';
    END IF;
    IF requested_epoch <> state.highest_epoch + 1 THEN
        RAISE EXCEPTION USING ERRCODE = '55000',
            MESSAGE = format('epoch must advance exactly once; expected %s', state.highest_epoch + 1);
    END IF;
    UPDATE master_control.epoch_state
    SET highest_epoch = requested_epoch,
        current_epoch = requested_epoch,
        master_instance_id = requested_instance,
        source_run_id = requested_run_id,
        lease_until = requested_lease_until,
        gate_state = 'closed',
        reason = 'registered',
        updated_at = observed_at
    WHERE singleton = true;
    INSERT INTO master_control.epoch_event (
        epoch, master_instance_id, source_run_id, event_kind, lease_until, reason
    ) VALUES (
        requested_epoch, requested_instance, requested_run_id, 'registered', requested_lease_until, 'registered'
    );
END
$$;

CREATE FUNCTION master_control.open_write_gate(requested_instance uuid, requested_epoch bigint)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
    state master_control.epoch_state%ROWTYPE;
BEGIN
    SELECT * INTO STRICT state FROM master_control.epoch_state WHERE singleton = true FOR UPDATE;
    IF state.master_instance_id IS DISTINCT FROM requested_instance
       OR state.current_epoch IS DISTINCT FROM requested_epoch
       OR state.gate_state <> 'closed'
       OR state.lease_until <= clock_timestamp() THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'stale, expired, or non-closed master cannot open gate';
    END IF;
    UPDATE master_control.epoch_state
    SET gate_state = 'open', reason = 'activated', updated_at = clock_timestamp()
    WHERE singleton = true;
    INSERT INTO master_control.epoch_event (
        epoch, master_instance_id, source_run_id, event_kind, lease_until, reason
    ) VALUES (
        state.current_epoch, state.master_instance_id, state.source_run_id,
        'opened', state.lease_until, 'activated'
    );
END
$$;

CREATE FUNCTION master_control.renew_epoch(
    requested_instance uuid,
    requested_epoch bigint,
    requested_lease_until timestamptz
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
    state master_control.epoch_state%ROWTYPE;
BEGIN
    SELECT * INTO STRICT state FROM master_control.epoch_state WHERE singleton = true FOR UPDATE;
    IF state.master_instance_id IS DISTINCT FROM requested_instance
       OR state.current_epoch IS DISTINCT FROM requested_epoch
       OR state.gate_state NOT IN ('closed', 'open')
       OR state.lease_until <= clock_timestamp()
       OR requested_lease_until <= state.lease_until THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'stale master cannot renew epoch';
    END IF;
    UPDATE master_control.epoch_state
    SET lease_until = requested_lease_until, reason = 'renewed', updated_at = clock_timestamp()
    WHERE singleton = true;
    INSERT INTO master_control.epoch_event (
        epoch, master_instance_id, source_run_id, event_kind, lease_until, reason
    ) VALUES (
        state.current_epoch, state.master_instance_id, state.source_run_id,
        'renewed', requested_lease_until, 'renewed'
    );
END
$$;

CREATE FUNCTION master_control.close_write_gate(
    requested_instance uuid,
    requested_epoch bigint,
    requested_state text,
    requested_reason text
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
    state master_control.epoch_state%ROWTYPE;
BEGIN
    IF requested_state NOT IN ('closed', 'draining', 'fenced')
       OR requested_reason IS NULL OR length(requested_reason) NOT BETWEEN 1 AND 256 THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'gate close request is invalid';
    END IF;
    SELECT * INTO STRICT state FROM master_control.epoch_state WHERE singleton = true FOR UPDATE;
    IF state.master_instance_id IS DISTINCT FROM requested_instance
       OR state.current_epoch IS DISTINCT FROM requested_epoch THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'stale master cannot change gate';
    END IF;
    UPDATE master_control.epoch_state
    SET gate_state = requested_state, reason = requested_reason, updated_at = clock_timestamp()
    WHERE singleton = true;
    INSERT INTO master_control.epoch_event (
        epoch, master_instance_id, source_run_id, event_kind, lease_until, reason
    ) VALUES (
        state.current_epoch, state.master_instance_id, state.source_run_id,
        requested_state, state.lease_until, requested_reason
    );
END
$$;

CREATE FUNCTION master_control.bind_epoch_credential(
    requested_credential_id uuid,
    requested_principal name,
    requested_instance uuid,
    requested_epoch bigint,
    requested_expires_at timestamptz
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
    state master_control.epoch_state%ROWTYPE;
BEGIN
    SELECT * INTO STRICT state FROM master_control.epoch_state WHERE singleton = true FOR UPDATE;
    IF state.master_instance_id IS DISTINCT FROM requested_instance
       OR state.current_epoch IS DISTINCT FROM requested_epoch
       OR state.gate_state <> 'open'
       OR state.lease_until <= clock_timestamp()
       OR requested_expires_at <= clock_timestamp()
       OR requested_expires_at > state.lease_until THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'credential is not bounded by active epoch lease';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = requested_principal::text AND rolcanlogin) THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'credential principal is not a LOGIN role';
    END IF;
    INSERT INTO master_control.credential_binding (
        credential_id, principal, epoch, master_instance_id, expires_at
    ) VALUES (
        requested_credential_id, requested_principal, requested_epoch, requested_instance, requested_expires_at
    )
    ON CONFLICT (credential_id) DO NOTHING;
    IF NOT FOUND OR NOT EXISTS (
        SELECT 1 FROM master_control.credential_binding
        WHERE credential_id = requested_credential_id
          AND principal = requested_principal
          AND epoch = requested_epoch
          AND master_instance_id = requested_instance
          AND expires_at = requested_expires_at
          AND revoked_at IS NULL
    ) THEN
        RAISE EXCEPTION USING ERRCODE = '23505', MESSAGE = 'credential idempotency conflict';
    END IF;
END
$$;

CREATE FUNCTION master_control.revoke_epoch_credential(requested_credential_id uuid, requested_reason text)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
BEGIN
    IF requested_reason IS NULL OR length(requested_reason) NOT BETWEEN 1 AND 256 THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'revocation reason is invalid';
    END IF;
    UPDATE master_control.credential_binding
    SET revoked_at = coalesce(revoked_at, clock_timestamp()),
        revocation_reason = coalesce(revocation_reason, requested_reason)
    WHERE credential_id = requested_credential_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'credential does not exist';
    END IF;
END
$$;

-- SECURITY DEFINER is required because guarded remote roles cannot read or mutate the
-- epoch tables. session_user, unlike current_user, remains the immutable LOGIN identity.
CREATE FUNCTION master_control.assert_session_write_epoch()
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

CREATE FUNCTION master_control.enforce_write_epoch()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
BEGIN
    PERFORM master_control.assert_session_write_epoch();
    RETURN NULL;
END
$$;

-- A deferred guard re-checks clock_timestamp() at transaction commit, so a statement
-- begun before lease expiry cannot commit canonical effects after expiry/fencing.
DO $$
DECLARE
    relation record;
BEGIN
    FOR relation IN
        SELECT n.nspname AS schema_name, c.relname AS table_name
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind IN ('r', 'p')
          AND n.nspname IN (
              'hub', 'analysis', 'orchestration', 'sync', 'region_talk',
              'migration', 'joplin', 'integration', 'operator_control'
          )
        ORDER BY n.nspname, c.relname
    LOOP
        EXECUTE format(
            'CREATE CONSTRAINT TRIGGER mdh_epoch_write_guard '
            'AFTER INSERT OR UPDATE OR DELETE ON %I.%I '
            'DEFERRABLE INITIALLY DEFERRED FOR EACH ROW '
            'EXECUTE FUNCTION master_control.enforce_write_epoch()',
            relation.schema_name,
            relation.table_name
        );
    END LOOP;
END
$$;

REVOKE ALL ON SCHEMA master_control FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA master_control FROM PUBLIC;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA master_control FROM PUBLIC;
GRANT USAGE ON SCHEMA master_control TO mdh_master_controller, mdh_monitoring, mdh_checkpoint;
GRANT EXECUTE ON FUNCTION master_control.begin_epoch(uuid,text,bigint,timestamptz),
    master_control.open_write_gate(uuid,bigint),
    master_control.renew_epoch(uuid,bigint,timestamptz),
    master_control.close_write_gate(uuid,bigint,text,text),
    master_control.bind_epoch_credential(uuid,name,uuid,bigint,timestamptz),
    master_control.revoke_epoch_credential(uuid,text)
    TO mdh_master_controller;
GRANT SELECT ON master_control.epoch_state, master_control.epoch_event TO mdh_monitoring;
GRANT SELECT ON master_control.epoch_state TO mdh_checkpoint;

UPDATE hub.canonical_state
SET schema_revision = 11,
    updated_at = clock_timestamp()
WHERE singleton = true;
