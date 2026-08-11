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

CREATE TABLE checkpoint_acceptance_events (
    request_id TEXT NOT NULL REFERENCES checkpoint_acceptance_launches(request_id),
    attempt_id TEXT NOT NULL,
    event_uid TEXT NOT NULL,
    event_type TEXT NOT NULL CHECK (event_type IN (
        'runtime.started','runtime.progress','runtime.heartbeat','runtime.failed',
        'runtime.terminal','resource.acquire','resource.release','job.result_available'
    )),
    phase TEXT,
    status TEXT,
    progress_json TEXT NOT NULL,
    body_sha256 TEXT NOT NULL CHECK (length(body_sha256)=64),
    body_json TEXT NOT NULL,
    local_sequence INTEGER NOT NULL CHECK (local_sequence >= 1),
    received_at TEXT NOT NULL,
    PRIMARY KEY (request_id, event_uid),
    UNIQUE (request_id, attempt_id, local_sequence)
);

CREATE INDEX checkpoint_acceptance_events_projection_idx
    ON checkpoint_acceptance_events(request_id, local_sequence);
