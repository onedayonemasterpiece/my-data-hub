CREATE TABLE checkpoint_blob_publications (
    checkpoint_id TEXT PRIMARY KEY REFERENCES checkpoint_candidates(checkpoint_id),
    operation_id TEXT NOT NULL REFERENCES operations(operation_id),
    run_id TEXT NOT NULL,
    attempt_id TEXT NOT NULL,
    master_instance_id TEXT NOT NULL,
    service_instance_id TEXT NOT NULL,
    master_run_ref TEXT NOT NULL,
    epoch INTEGER NOT NULL CHECK (epoch > 0),
    dataset_ref TEXT NOT NULL,
    manifest_sha256 TEXT NOT NULL CHECK (length(manifest_sha256) = 64),
    source_head_generation INTEGER NOT NULL CHECK (source_head_generation >= 0),
    expected_file_count INTEGER NOT NULL CHECK (expected_file_count > 0),
    expected_total_bytes INTEGER NOT NULL CHECK (expected_total_bytes > 0),
    state TEXT NOT NULL CHECK (state IN (
        'PREPARING','UPLOADING','READY_TO_FINALIZE','FINALIZING',
        'DATASET_RESOLVED','VERIFYING','VERIFIED','PROMOTED','FAILED','QUARANTINED'
    )),
    exact_version_ref TEXT,
    expected_provider_version INTEGER CHECK (
        expected_provider_version IS NULL OR expected_provider_version > 0
    ),
    finalize_attempts INTEGER NOT NULL DEFAULT 0 CHECK (finalize_attempts BETWEEN 0 AND 3),
    verifier_run_ref TEXT,
    verifier_receipt_sha256 TEXT CHECK (
        verifier_receipt_sha256 IS NULL OR length(verifier_receipt_sha256) = 64
    ),
    failure_code TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(operation_id, manifest_sha256),
    UNIQUE(dataset_ref, checkpoint_id)
);

CREATE TABLE checkpoint_blob_upload_claims (
    claim_id TEXT PRIMARY KEY,
    checkpoint_id TEXT NOT NULL REFERENCES checkpoint_blob_publications(checkpoint_id),
    operation_id TEXT NOT NULL,
    epoch INTEGER NOT NULL CHECK (epoch > 0),
    file_name TEXT NOT NULL,
    content_length INTEGER NOT NULL CHECK (content_length > 0),
    content_type TEXT NOT NULL,
    content_sha256 TEXT NOT NULL CHECK (length(content_sha256) = 64),
    manifest_sha256 TEXT NOT NULL CHECK (length(manifest_sha256) = 64),
    intent_sha256 TEXT NOT NULL CHECK (length(intent_sha256) = 64),
    state TEXT NOT NULL CHECK (state IN (
        'PREPARING','STARTING','READY','START_AMBIGUOUS','UPLOADED','CONFLICT','CONSUMED','REVOKED'
    )),
    sealed_blob_token BLOB,
    sealed_create_url BLOB,
    expires_at TEXT NOT NULL,
    completed_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(checkpoint_id, file_name)
);

CREATE INDEX checkpoint_blob_upload_claim_state_idx
    ON checkpoint_blob_upload_claims(state, expires_at);

CREATE TABLE checkpoint_blob_upload_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    checkpoint_id TEXT NOT NULL,
    claim_id TEXT,
    event_type TEXT NOT NULL CHECK (event_type IN (
        'publication.created','blob.prepare.started','blob.prepare.claimed','blob.prepare.ready',
        'blob.prepare.ambiguous','blob.upload.completed','blob.upload.conflict',
        'dataset.finalize.started','dataset.version.resolved','verifier.started',
        'verifier.passed','publication.promoted','publication.failed','publication.quarantined'
    )),
    evidence_sha256 TEXT NOT NULL CHECK (length(evidence_sha256) = 64),
    created_at TEXT NOT NULL
);

CREATE TRIGGER checkpoint_blob_upload_events_no_update
BEFORE UPDATE ON checkpoint_blob_upload_events
BEGIN
    SELECT RAISE(ABORT, 'checkpoint blob upload events are append-only');
END;

CREATE TRIGGER checkpoint_blob_upload_events_no_delete
BEFORE DELETE ON checkpoint_blob_upload_events
BEGIN
    SELECT RAISE(ABORT, 'checkpoint blob upload events are append-only');
END;
