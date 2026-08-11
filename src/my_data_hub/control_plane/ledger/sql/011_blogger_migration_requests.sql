CREATE TABLE blogger_migration_requests (
    request_id TEXT PRIMARY KEY,
    operation_id TEXT NOT NULL,
    request_sha256 TEXT NOT NULL,
    request_json TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN (
        'REQUESTED','CLAIMED','IMPORT_COMMITTED','CHECKPOINT_VERIFIED','FAILED'
    )),
    claimed_run_id TEXT,
    claimed_attempt_id TEXT,
    claimed_master_instance_id TEXT,
    claimed_epoch INTEGER,
    import_receipt_json TEXT,
    checkpoint_receipt_json TEXT,
    failure_code TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(operation_id),
    CHECK ((state='REQUESTED' AND claimed_run_id IS NULL) OR state<>'REQUESTED')
);

CREATE TRIGGER blogger_migration_requests_no_delete
BEFORE DELETE ON blogger_migration_requests
BEGIN SELECT RAISE(ABORT, 'blogger_migration_requests is durable metadata'); END;
