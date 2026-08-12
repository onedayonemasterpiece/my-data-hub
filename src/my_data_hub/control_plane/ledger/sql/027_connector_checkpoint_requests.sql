CREATE TABLE connector_checkpoint_requests (
    operation_id TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    canonical_revision INTEGER NOT NULL CHECK (canonical_revision >= 1),
    master_operation_id TEXT NOT NULL REFERENCES operations(operation_id),
    master_instance_id TEXT NOT NULL,
    epoch INTEGER NOT NULL CHECK (epoch >= 1),
    state TEXT NOT NULL CHECK (state IN ('REQUESTED','CLAIMED','DURABLE_COMPLETE','FAILED')),
    checkpoint_id TEXT,
    manifest_sha256 TEXT,
    verified_at TEXT,
    failure_code TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    CHECK (
        (state = 'DURABLE_COMPLETE' AND checkpoint_id IS NOT NULL AND manifest_sha256 IS NOT NULL
            AND verified_at IS NOT NULL AND failure_code IS NULL)
        OR (state = 'FAILED' AND failure_code IS NOT NULL AND checkpoint_id IS NULL
            AND manifest_sha256 IS NULL AND verified_at IS NULL)
        OR (state IN ('REQUESTED','CLAIMED') AND checkpoint_id IS NULL
            AND manifest_sha256 IS NULL AND verified_at IS NULL AND failure_code IS NULL)
    )
);

CREATE INDEX connector_checkpoint_requests_pending_idx
    ON connector_checkpoint_requests(master_operation_id, state, created_at);
