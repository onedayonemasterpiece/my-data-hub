CREATE TABLE embedding_production_requests (
    request_id TEXT PRIMARY KEY,
    operation_id TEXT NOT NULL UNIQUE,
    idempotency_key_sha256 TEXT NOT NULL UNIQUE,
    request_sha256 TEXT NOT NULL,
    request_json TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('REQUESTED','CLAIMED','STAGE_COMMITTED','CHECKPOINT_VERIFIED','FAILED')),
    claimed_run_id TEXT,
    claimed_attempt_id TEXT,
    claimed_master_instance_id TEXT,
    claimed_epoch INTEGER,
    stage_receipt_json TEXT,
    checkpoint_receipt_json TEXT,
    failure_code TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    CHECK ((state='REQUESTED' AND claimed_run_id IS NULL) OR state<>'REQUESTED')
);
CREATE TRIGGER embedding_production_requests_no_delete
BEFORE DELETE ON embedding_production_requests
BEGIN SELECT RAISE(ABORT, 'embedding_production_requests is durable metadata'); END;
