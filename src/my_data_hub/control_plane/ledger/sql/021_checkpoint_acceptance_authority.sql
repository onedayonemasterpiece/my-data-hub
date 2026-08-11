CREATE TABLE checkpoint_acceptance_launches (
    request_id TEXT PRIMARY KEY,
    scenario_id TEXT NOT NULL CHECK (scenario_id IN ('FM05','FM14','FM15')),
    operation_id TEXT NOT NULL UNIQUE,
    task_run_id TEXT NOT NULL,
    attempt_id TEXT NOT NULL UNIQUE,
    idempotency_key TEXT NOT NULL UNIQUE,
    request_sha256 TEXT NOT NULL CHECK (length(request_sha256)=64),
    request_json TEXT NOT NULL,
    principal_id TEXT NOT NULL,
    client_id TEXT NOT NULL,
    token_sha256 TEXT NOT NULL CHECK (length(token_sha256)=64),
    expires_at TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('REQUESTED','RUNNING','LIVE_EVIDENCE_READY','BLOCKED','FAIL')),
    status_dataset_json TEXT,
    provider_run_json TEXT,
    config_sha256 TEXT NOT NULL CHECK (length(config_sha256)=64),
    cleanup_receipt_json TEXT,
    result_json TEXT,
    result_sha256 TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX checkpoint_acceptance_launch_state_idx
    ON checkpoint_acceptance_launches(state, updated_at);
