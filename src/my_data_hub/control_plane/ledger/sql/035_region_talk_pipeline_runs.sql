-- Metadata-only schedule and lifecycle journal for the separate private
-- Region Talk Kaggle supervisor. Canonical business rows remain in the ACTIVE
-- PostgreSQL master and credentials remain in task-private capability storage.
CREATE TABLE region_talk_pipeline_requests (
    request_id TEXT PRIMARY KEY,
    project_slug TEXT NOT NULL CHECK (project_slug = 'region-talk'),
    trigger_kind TEXT NOT NULL CHECK (trigger_kind IN ('scheduled','supervised')),
    schedule_slot TEXT NOT NULL UNIQUE,
    idempotency_key_sha256 TEXT NOT NULL UNIQUE CHECK (length(idempotency_key_sha256) = 64),
    source_revision TEXT,
    publication_dispatch INTEGER NOT NULL DEFAULT 0 CHECK (publication_dispatch = 0),
    state TEXT NOT NULL CHECK (state IN (
        'WAITING_MASTER','LAUNCHING','PENDING_ATTESTATION','ATTESTED','RUNNING',
        'TERMINAL','TIMED_OUT','FENCED','CLEANUP_PENDING','CLEANED'
    )),
    task_run_id TEXT UNIQUE,
    master_run_id TEXT,
    master_attempt_id TEXT,
    master_instance_id TEXT,
    epoch INTEGER CHECK (epoch IS NULL OR epoch >= 1),
    provider_run_ref TEXT,
    status_dataset_exact_ref TEXT,
    source_sha256 TEXT CHECK (source_sha256 IS NULL OR length(source_sha256) = 64),
    credential_id TEXT,
    credential_generation INTEGER CHECK (credential_generation IS NULL OR credential_generation >= 1),
    credential_command_sha256 TEXT CHECK (
        credential_command_sha256 IS NULL OR length(credential_command_sha256) = 64
    ),
    credential_task_token_sha256 TEXT CHECK (
        credential_task_token_sha256 IS NULL OR length(credential_task_token_sha256) = 64
    ),
    credential_expires_at TEXT,
    ssh_certificate_serial INTEGER CHECK (ssh_certificate_serial IS NULL OR ssh_certificate_serial >= 1),
    runtime_image_identity TEXT,
    runtime_image_source_commit TEXT,
    lease_owner TEXT,
    lease_until TEXT,
    timeout_at TEXT,
    terminal_status TEXT CHECK (
        terminal_status IS NULL OR terminal_status IN ('SUCCEEDED','FAILED','TIMED_OUT','EPOCH_FENCED')
    ),
    terminal_receipt_sha256 TEXT CHECK (
        terminal_receipt_sha256 IS NULL OR length(terminal_receipt_sha256) = 64
    ),
    cleanup_receipt_sha256 TEXT CHECK (
        cleanup_receipt_sha256 IS NULL OR length(cleanup_receipt_sha256) = 64
    ),
    error_code TEXT,
    requested_at TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    CHECK (
        (state = 'WAITING_MASTER' AND task_run_id IS NULL AND master_run_id IS NULL
            AND master_attempt_id IS NULL AND master_instance_id IS NULL AND epoch IS NULL)
        OR
        (state <> 'WAITING_MASTER' AND task_run_id IS NOT NULL AND master_run_id IS NOT NULL
            AND master_attempt_id IS NOT NULL AND master_instance_id IS NOT NULL AND epoch IS NOT NULL)
    ),
    CHECK (
        (credential_id IS NULL AND credential_generation IS NULL
            AND credential_command_sha256 IS NULL AND credential_task_token_sha256 IS NULL
            AND credential_expires_at IS NULL AND ssh_certificate_serial IS NULL)
        OR
        (credential_id IS NOT NULL AND credential_generation IS NOT NULL
            AND credential_command_sha256 IS NOT NULL AND credential_task_token_sha256 IS NOT NULL
            AND credential_expires_at IS NOT NULL AND ssh_certificate_serial IS NOT NULL)
    )
);

-- More than one request may wait for a master, but only one Region Talk
-- supervisor may own an exact ACTIVE epoch at a time.
CREATE UNIQUE INDEX region_talk_pipeline_single_live_run
ON region_talk_pipeline_requests(publication_dispatch)
WHERE state IN ('LAUNCHING','PENDING_ATTESTATION','ATTESTED','RUNNING');

CREATE INDEX region_talk_pipeline_scheduler_queue
ON region_talk_pipeline_requests(state, requested_at, request_id);

CREATE INDEX region_talk_pipeline_cleanup_queue
ON region_talk_pipeline_requests(state, updated_at, request_id);

CREATE TABLE region_talk_pipeline_events (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id TEXT NOT NULL REFERENCES region_talk_pipeline_requests(request_id),
    from_state TEXT,
    to_state TEXT NOT NULL,
    event_kind TEXT NOT NULL,
    event_metadata_sha256 TEXT NOT NULL CHECK (length(event_metadata_sha256) = 64),
    recorded_at TEXT NOT NULL
);

CREATE INDEX region_talk_pipeline_events_request
ON region_talk_pipeline_events(request_id, sequence);

CREATE TRIGGER region_talk_pipeline_events_no_update
BEFORE UPDATE ON region_talk_pipeline_events
BEGIN SELECT RAISE(ABORT, 'region_talk_pipeline_events is append-only'); END;

CREATE TRIGGER region_talk_pipeline_events_no_delete
BEFORE DELETE ON region_talk_pipeline_events
BEGIN SELECT RAISE(ABORT, 'region_talk_pipeline_events is append-only'); END;

CREATE TRIGGER region_talk_pipeline_requests_no_delete
BEFORE DELETE ON region_talk_pipeline_requests
BEGIN SELECT RAISE(ABORT, 'region_talk_pipeline_requests is durable metadata'); END;
