CREATE TABLE acceptance_evidence_tasks (
    task_id TEXT NOT NULL,
    scenario_id TEXT NOT NULL CHECK (scenario_id IN ('FM01','FM02','FM03','FM06','FM22','FM23')),
    idempotency_key TEXT NOT NULL,
    principal_id TEXT NOT NULL,
    client_id TEXT NOT NULL,
    request_sha256 TEXT NOT NULL CHECK (length(request_sha256) = 64),
    state TEXT NOT NULL CHECK (state IN ('CLAIMED','RUNNING','SUCCEEDED','FAILED')),
    mutation_started INTEGER NOT NULL DEFAULT 0 CHECK (mutation_started IN (0,1)),
    failure_code TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (scenario_id, task_id),
    UNIQUE (scenario_id, principal_id, client_id, idempotency_key)
);

CREATE TABLE acceptance_evidence_events (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    scenario_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    event_type TEXT NOT NULL CHECK (event_type IN (
        'CLAIMED','RUNNING','PROVIDER_DATASET','PROVIDER_NOTEBOOK','OUTPUT_READ',
        'CLEANUP','SUCCEEDED','FAILED'
    )),
    evidence_json TEXT NOT NULL,
    evidence_sha256 TEXT NOT NULL CHECK (length(evidence_sha256) = 64),
    recorded_at TEXT NOT NULL,
    FOREIGN KEY (scenario_id, task_id)
        REFERENCES acceptance_evidence_tasks(scenario_id, task_id),
    UNIQUE (scenario_id, task_id, event_type, evidence_sha256)
);

CREATE INDEX acceptance_evidence_events_task_idx
ON acceptance_evidence_events(scenario_id, task_id, sequence);

CREATE INDEX runtime_events_exact_attempt_epoch_idx
ON runtime_events(run_id, attempt_id, epoch, local_sequence);

CREATE TRIGGER acceptance_evidence_events_no_update
BEFORE UPDATE ON acceptance_evidence_events
BEGIN SELECT RAISE(ABORT, 'acceptance_evidence_events is append-only'); END;

CREATE TRIGGER acceptance_evidence_events_no_delete
BEFORE DELETE ON acceptance_evidence_events
BEGIN SELECT RAISE(ABORT, 'acceptance_evidence_events is append-only'); END;
