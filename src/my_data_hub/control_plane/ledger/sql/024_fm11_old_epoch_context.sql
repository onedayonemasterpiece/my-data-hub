CREATE TABLE master_acceptance_old_epoch_contexts (
    task_id TEXT PRIMARY KEY REFERENCES master_acceptance_tasks(task_id),
    command_id TEXT NOT NULL UNIQUE REFERENCES master_acceptance_commands(command_id),
    command_sha256 TEXT NOT NULL CHECK (length(command_sha256)=64),
    old_operation_id TEXT NOT NULL REFERENCES operations(operation_id),
    old_binding_json TEXT NOT NULL,
    runtime_token_sha256 TEXT NOT NULL CHECK (length(runtime_token_sha256)=64),
    credential_handle TEXT NOT NULL UNIQUE,
    tunnel_certificate_json TEXT,
    context_sha256 TEXT CHECK (context_sha256 IS NULL OR length(context_sha256)=64),
    state TEXT NOT NULL CHECK (state IN ('INTENT','CAPTURED','RELEASED')),
    captured_at TEXT,
    expires_at TEXT NOT NULL,
    released_at TEXT,
    result_receipt_sha256 TEXT CHECK (
        result_receipt_sha256 IS NULL OR length(result_receipt_sha256)=64
    ),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX master_acceptance_old_epoch_context_state_idx
    ON master_acceptance_old_epoch_contexts(state, expires_at);
