CREATE TABLE mcp_write_operations (
    operation_id TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL,
    tool TEXT NOT NULL CHECK (tool IN ('data.change.preview', 'data.change.apply')),
    principal_id TEXT NOT NULL,
    client_id TEXT NOT NULL,
    master_instance_id TEXT NOT NULL,
    epoch INTEGER NOT NULL CHECK (epoch >= 1),
    expected_revision INTEGER NOT NULL CHECK (expected_revision >= 0),
    request_sha256 TEXT NOT NULL CHECK (length(request_sha256) = 64),
    state TEXT NOT NULL CHECK (state IN (
        'REQUESTED', 'PREVIEWED', 'APPLYING', 'COMMITTED_PENDING_CHECKPOINT',
        'CHECKPOINTING', 'CHECKPOINT_VERIFIED', 'DURABLE_COMPLETE', 'FAILED'
    )),
    pre_change_checkpoint_id TEXT NOT NULL REFERENCES checkpoint_candidates(checkpoint_id),
    preview_receipt TEXT,
    affected_rows INTEGER CHECK (affected_rows IS NULL OR affected_rows >= 0),
    committed_revision INTEGER CHECK (committed_revision IS NULL OR committed_revision >= 0),
    post_change_checkpoint_id TEXT REFERENCES checkpoint_candidates(checkpoint_id),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    committed_at TEXT,
    UNIQUE(principal_id, client_id, idempotency_key)
);

CREATE TABLE mcp_write_events (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    operation_id TEXT NOT NULL REFERENCES mcp_write_operations(operation_id),
    state TEXT NOT NULL,
    metadata_json TEXT NOT NULL,
    recorded_at TEXT NOT NULL
);

CREATE TRIGGER mcp_write_events_no_update
BEFORE UPDATE ON mcp_write_events BEGIN SELECT RAISE(ABORT, 'mcp_write_events is append-only'); END;
CREATE TRIGGER mcp_write_events_no_delete
BEFORE DELETE ON mcp_write_events BEGIN SELECT RAISE(ABORT, 'mcp_write_events is append-only'); END;
