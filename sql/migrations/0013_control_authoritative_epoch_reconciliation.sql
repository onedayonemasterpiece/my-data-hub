-- A restored checkpoint can legitimately lag the durable control-plane epoch
-- after one or more failed Kaggle attempts.  The owner-authoritative control
-- epoch must therefore be strictly newer, but need not be local highest + 1.
CREATE OR REPLACE FUNCTION master_control.begin_epoch(
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
    IF requested_epoch <= state.highest_epoch THEN
        RAISE EXCEPTION USING ERRCODE = '55000',
            MESSAGE = format('control epoch must be newer than restored local epoch %s', state.highest_epoch);
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

UPDATE hub.canonical_state
SET schema_revision = 13,
    updated_at = clock_timestamp()
WHERE singleton = true;
