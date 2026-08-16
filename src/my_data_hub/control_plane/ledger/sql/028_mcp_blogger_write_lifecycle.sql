-- Metadata-only owner preview/apply lifecycle for typed blogger discovery batches.
-- Canonical rows and staged payloads remain exclusively in ACTIVE-master PostgreSQL.
CREATE TABLE mcp_blogger_import_operations (
    operation_id TEXT PRIMARY KEY CHECK (length(operation_id) = 64),
    batch_id TEXT NOT NULL CHECK (length(batch_id) = 36),
    idempotency_key TEXT NOT NULL,
    principal_id TEXT NOT NULL,
    client_id TEXT NOT NULL,
    master_instance_id TEXT,
    epoch INTEGER CHECK (epoch IS NULL OR epoch >= 1),
    expected_revision INTEGER NOT NULL CHECK (expected_revision >= 0),
    request_sha256 TEXT NOT NULL CHECK (length(request_sha256) = 64),
    plan_sha256 TEXT CHECK (plan_sha256 IS NULL OR length(plan_sha256) = 64),
    state TEXT NOT NULL CHECK (state IN (
        'REQUESTED', 'WAITING_MASTER', 'PREVIEWED', 'APPLYING', 'COMMITTED_PENDING_CHECKPOINT',
        'CHECKPOINTING', 'CHECKPOINT_VERIFIED', 'DURABLE_COMPLETE', 'FAILED'
    )),
    preview_receipt TEXT,
    preview_summary_json TEXT,
    affected_rows INTEGER CHECK (affected_rows IS NULL OR affected_rows >= 0),
    committed_revision INTEGER CHECK (committed_revision IS NULL OR committed_revision >= 1),
    pre_change_checkpoint_id TEXT REFERENCES checkpoint_candidates(checkpoint_id),
    post_change_checkpoint_id TEXT REFERENCES checkpoint_candidates(checkpoint_id),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    committed_at TEXT,
    UNIQUE(principal_id, client_id, idempotency_key),
    UNIQUE(principal_id, client_id, batch_id),
    CHECK (
        (master_instance_id IS NULL AND epoch IS NULL AND pre_change_checkpoint_id IS NULL)
        OR
        (master_instance_id IS NOT NULL AND epoch IS NOT NULL AND pre_change_checkpoint_id IS NOT NULL)
    )
);

CREATE TABLE mcp_blogger_import_events (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    operation_id TEXT NOT NULL REFERENCES mcp_blogger_import_operations(operation_id),
    state TEXT NOT NULL,
    metadata_json TEXT NOT NULL,
    recorded_at TEXT NOT NULL
);

CREATE TRIGGER mcp_blogger_import_events_no_update
BEFORE UPDATE ON mcp_blogger_import_events
BEGIN SELECT RAISE(ABORT, 'mcp_blogger_import_events is append-only'); END;
CREATE TRIGGER mcp_blogger_import_events_no_delete
BEFORE DELETE ON mcp_blogger_import_events
BEGIN SELECT RAISE(ABORT, 'mcp_blogger_import_events is append-only'); END;
