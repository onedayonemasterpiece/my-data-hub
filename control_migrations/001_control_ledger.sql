CREATE TABLE operations (
    operation_id TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    operation_kind TEXT NOT NULL,
    intent_hash TEXT NOT NULL,
    state TEXT NOT NULL,
    identity_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE operation_log (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    operation_id TEXT NOT NULL REFERENCES operations(operation_id),
    from_state TEXT,
    to_state TEXT NOT NULL,
    recorded_at TEXT NOT NULL,
    metadata_json TEXT NOT NULL
);

CREATE TRIGGER operation_log_no_update
BEFORE UPDATE ON operation_log BEGIN SELECT RAISE(ABORT, 'operation_log is append-only'); END;
CREATE TRIGGER operation_log_no_delete
BEFORE DELETE ON operation_log BEGIN SELECT RAISE(ABORT, 'operation_log is append-only'); END;

CREATE TABLE effects (
    effect_id TEXT PRIMARY KEY,
    operation_id TEXT NOT NULL REFERENCES operations(operation_id),
    idempotency_key TEXT NOT NULL UNIQUE,
    effect_kind TEXT NOT NULL,
    exact_identity_json TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('PLANNED', 'IN_PROGRESS', 'APPLIED', 'FAILED')),
    receipt_json TEXT,
    planned_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX effects_reconcile_idx ON effects(state, planned_at);

CREATE TABLE effect_log (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    effect_id TEXT NOT NULL REFERENCES effects(effect_id),
    operation_id TEXT NOT NULL REFERENCES operations(operation_id),
    state TEXT NOT NULL,
    recorded_at TEXT NOT NULL,
    metadata_json TEXT NOT NULL
);

CREATE TRIGGER effect_log_no_update
BEFORE UPDATE ON effect_log BEGIN SELECT RAISE(ABORT, 'effect_log is append-only'); END;
CREATE TRIGGER effect_log_no_delete
BEFORE DELETE ON effect_log BEGIN SELECT RAISE(ABORT, 'effect_log is append-only'); END;

CREATE TABLE run_attempts (
    attempt_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    operation_id TEXT NOT NULL REFERENCES operations(operation_id),
    source_identity TEXT NOT NULL,
    source_version TEXT NOT NULL,
    service_instance_id TEXT NOT NULL,
    master_instance_id TEXT,
    epoch INTEGER NOT NULL CHECK (epoch >= 1),
    provider_run_ref TEXT,
    state TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(run_id, attempt_id),
    UNIQUE(service_instance_id, epoch)
);

CREATE INDEX run_attempts_operation_idx ON run_attempts(operation_id);

CREATE TABLE runtime_event_dedup (
    event_id TEXT PRIMARY KEY,
    body_sha256 TEXT NOT NULL,
    first_seen_at TEXT NOT NULL
);

CREATE TABLE runtime_events (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE REFERENCES runtime_event_dedup(event_id),
    schema_version TEXT NOT NULL,
    run_id TEXT NOT NULL,
    attempt_id TEXT NOT NULL,
    service_instance_id TEXT NOT NULL,
    source_identity TEXT NOT NULL,
    source_version TEXT NOT NULL,
    epoch INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    emitted_at TEXT NOT NULL,
    received_at TEXT NOT NULL,
    local_sequence INTEGER NOT NULL,
    body_sha256 TEXT NOT NULL,
    body_bytes INTEGER NOT NULL CHECK (body_bytes BETWEEN 2 AND 65536),
    sanitized_json TEXT NOT NULL,
    UNIQUE(run_id, attempt_id, local_sequence)
);

CREATE INDEX runtime_events_attempt_idx
ON runtime_events(run_id, attempt_id, local_sequence);

CREATE TRIGGER runtime_events_no_update
BEFORE UPDATE ON runtime_events BEGIN SELECT RAISE(ABORT, 'runtime_events is append-only'); END;
CREATE TRIGGER runtime_events_no_delete
BEFORE DELETE ON runtime_events BEGIN SELECT RAISE(ABORT, 'runtime_events is append-only'); END;

CREATE TABLE runtime_projection (
    run_id TEXT NOT NULL,
    attempt_id TEXT NOT NULL,
    latest_event_id TEXT NOT NULL,
    latest_event_type TEXT NOT NULL,
    latest_sequence INTEGER NOT NULL,
    latest_epoch INTEGER NOT NULL,
    latest_seen_at TEXT NOT NULL,
    terminal INTEGER NOT NULL DEFAULT 0 CHECK (terminal IN (0, 1)),
    PRIMARY KEY(run_id, attempt_id)
);
