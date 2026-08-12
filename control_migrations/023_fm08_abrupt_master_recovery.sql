CREATE TABLE master_acceptance_abrupt_recoveries (
    task_id TEXT PRIMARY KEY REFERENCES master_acceptance_tasks(task_id),
    command_id TEXT NOT NULL UNIQUE REFERENCES master_acceptance_commands(command_id),
    command_sha256 TEXT NOT NULL CHECK (length(command_sha256)=64),
    old_operation_id TEXT NOT NULL REFERENCES operations(operation_id),
    old_run_json TEXT NOT NULL,
    old_epoch INTEGER NOT NULL CHECK (old_epoch>0),
    replacement_idempotency_key TEXT NOT NULL UNIQUE,
    replacement_notebook_ref TEXT NOT NULL UNIQUE,
    termination_receipt_json TEXT,
    termination_receipt_sha256 TEXT CHECK (
        termination_receipt_sha256 IS NULL OR length(termination_receipt_sha256)=64
    ),
    replacement_operation_id TEXT REFERENCES operations(operation_id),
    replacement_run_json TEXT,
    recovery_receipt_sha256 TEXT CHECK (
        recovery_receipt_sha256 IS NULL OR length(recovery_receipt_sha256)=64
    ),
    state TEXT NOT NULL CHECK (state IN ('INTENT','TERMINATED','RECOVERING','ACTIVE')),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX master_acceptance_abrupt_recovery_state_idx
    ON master_acceptance_abrupt_recoveries(state, updated_at);
