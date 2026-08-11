CREATE TABLE master_acceptance_tasks (
    task_id TEXT PRIMARY KEY,
    scenario_id TEXT NOT NULL CHECK (scenario_id IN ('FM04','FM07','FM08','FM09','FM10','FM11','FM12','FM24')),
    idempotency_key TEXT NOT NULL UNIQUE,
    request_sha256 TEXT NOT NULL CHECK (length(request_sha256)=64),
    principal_id TEXT NOT NULL,
    client_id TEXT NOT NULL,
    source_revision TEXT NOT NULL CHECK (length(source_revision)=40),
    target_operation_id TEXT,
    target_run_id TEXT,
    target_attempt_id TEXT,
    target_service_instance_id TEXT,
    target_master_instance_id TEXT,
    target_epoch INTEGER CHECK (target_epoch IS NULL OR target_epoch >= 1),
    state TEXT NOT NULL CHECK (state IN ('PENDING','BOUND','CLAIMED','PASSED','FAILED')),
    timeout_seconds INTEGER NOT NULL,
    deadline_at TEXT NOT NULL,
    failure_code TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (scenario_id, task_id),
    UNIQUE (scenario_id, principal_id, client_id, idempotency_key),
    CHECK ((state='FAILED') = (failure_code IS NOT NULL)),
    CHECK ((scenario_id='FM24' AND timeout_seconds=5700)
        OR (scenario_id<>'FM24' AND timeout_seconds=1800)),
    CHECK ((target_operation_id IS NULL AND target_run_id IS NULL AND target_attempt_id IS NULL
            AND target_service_instance_id IS NULL
            AND target_master_instance_id IS NULL AND target_epoch IS NULL)
        OR (target_operation_id IS NOT NULL AND target_run_id IS NOT NULL AND target_attempt_id IS NOT NULL
            AND target_service_instance_id IS NOT NULL
            AND target_master_instance_id IS NOT NULL AND target_epoch IS NOT NULL))
);

CREATE TABLE master_acceptance_commands (
    command_id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL UNIQUE REFERENCES master_acceptance_tasks(task_id),
    scenario_id TEXT NOT NULL,
    command_kind TEXT NOT NULL CHECK (command_kind IN (
        'EMPTY_MASTER_BOOTSTRAP','CONCURRENT_ENSURE_SINGLE_RUN','CALLBACK_LOSS_RECOVERY',
        'STALE_REPLAY_REJECTION','LEASE_EXPIRY_DENIAL','OLD_EPOCH_RETURN_DENIAL',
        'CLEAN_DRAIN','SESSION_ROTATION_SOAK'
    )),
    command_sha256 TEXT NOT NULL CHECK (length(command_sha256)=64),
    state TEXT NOT NULL CHECK (state IN ('PENDING','CLAIMED','SUCCEEDED','FAILED')),
    claimed_run_id TEXT,
    claimed_attempt_id TEXT,
    claimed_epoch INTEGER CHECK (claimed_epoch IS NULL OR claimed_epoch >= 1),
    receipt_json TEXT,
    receipt_sha256 TEXT CHECK (receipt_sha256 IS NULL OR length(receipt_sha256)=64),
    claimed_at TEXT,
    completed_at TEXT,
    FOREIGN KEY (scenario_id, task_id) REFERENCES master_acceptance_tasks(scenario_id, task_id),
    CHECK ((state='PENDING') = (claimed_run_id IS NULL)),
    CHECK ((receipt_json IS NULL) = (receipt_sha256 IS NULL)),
    CHECK ((state IN ('SUCCEEDED','FAILED')) = (receipt_json IS NOT NULL))
);

CREATE TABLE master_acceptance_events (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id TEXT NOT NULL REFERENCES master_acceptance_tasks(task_id),
    event_type TEXT NOT NULL CHECK (event_type IN ('PENDING','BOUND','CLAIMED','SUCCEEDED','FAILED')),
    evidence_json TEXT NOT NULL,
    evidence_sha256 TEXT NOT NULL CHECK (length(evidence_sha256)=64),
    recorded_at TEXT NOT NULL,
    UNIQUE (task_id, event_type, evidence_sha256)
);

CREATE INDEX master_acceptance_tasks_state_idx ON master_acceptance_tasks(state, created_at);
CREATE INDEX master_acceptance_events_task_idx ON master_acceptance_events(task_id, sequence);

CREATE TRIGGER master_acceptance_events_no_update BEFORE UPDATE ON master_acceptance_events
BEGIN SELECT RAISE(ABORT, 'master_acceptance_events is append-only'); END;
CREATE TRIGGER master_acceptance_events_no_delete BEFORE DELETE ON master_acceptance_events
BEGIN SELECT RAISE(ABORT, 'master_acceptance_events is append-only'); END;
