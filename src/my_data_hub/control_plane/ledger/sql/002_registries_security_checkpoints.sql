CREATE TABLE provider_resources (
    provider TEXT NOT NULL,
    resource_ref TEXT NOT NULL,
    resource_kind TEXT NOT NULL,
    source_identity TEXT NOT NULL,
    source_version TEXT NOT NULL,
    control_class TEXT NOT NULL,
    private INTEGER CHECK (private IN (0, 1)),
    state TEXT NOT NULL,
    metadata_json TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    PRIMARY KEY(provider, resource_ref, source_version)
);

CREATE TABLE service_epochs (
    service_kind TEXT PRIMARY KEY,
    current_epoch INTEGER NOT NULL CHECK (current_epoch >= 0),
    updated_at TEXT NOT NULL
);

CREATE TABLE services (
    service_instance_id TEXT PRIMARY KEY,
    service_kind TEXT NOT NULL,
    run_id TEXT NOT NULL,
    attempt_id TEXT NOT NULL,
    master_instance_id TEXT,
    epoch INTEGER NOT NULL CHECK (epoch >= 1),
    endpoint TEXT NOT NULL,
    protocol TEXT NOT NULL,
    tls_fingerprint TEXT,
    capabilities_json TEXT NOT NULL,
    canonical_revision INTEGER,
    schema_version TEXT,
    lease_until TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('ACTIVE', 'DRAINING', 'FENCED', 'STOPPED')),
    latest_event_id TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(service_kind, epoch)
);

CREATE INDEX services_resolve_idx ON services(service_kind, state, epoch DESC);

CREATE TABLE resource_leases (
    lease_id TEXT PRIMARY KEY,
    resource_kind TEXT NOT NULL,
    resource_ref TEXT NOT NULL,
    holder_id TEXT NOT NULL,
    epoch INTEGER NOT NULL CHECK (epoch >= 1),
    acquired_at TEXT NOT NULL,
    lease_until TEXT NOT NULL,
    released_at TEXT,
    UNIQUE(resource_kind, resource_ref, epoch)
);

CREATE INDEX resource_leases_current_idx
ON resource_leases(resource_kind, resource_ref, lease_until);

CREATE TABLE checkpoint_candidates (
    checkpoint_id TEXT PRIMARY KEY,
    operation_id TEXT NOT NULL REFERENCES operations(operation_id),
    dataset_ref TEXT NOT NULL,
    version_ref TEXT NOT NULL,
    manifest_sha256 TEXT NOT NULL,
    source_checkpoint_id TEXT REFERENCES checkpoint_candidates(checkpoint_id),
    master_instance_id TEXT NOT NULL,
    epoch INTEGER NOT NULL CHECK (epoch >= 1),
    status TEXT NOT NULL CHECK (status IN ('CANDIDATE', 'READBACK_VERIFIED', 'VERIFIED', 'FAILED')),
    failure_code TEXT,
    created_at TEXT NOT NULL,
    verified_at TEXT,
    UNIQUE(dataset_ref, version_ref)
);

CREATE TABLE checkpoint_heads (
    service_kind TEXT PRIMARY KEY,
    current_checkpoint_id TEXT REFERENCES checkpoint_candidates(checkpoint_id),
    previous_checkpoint_id TEXT REFERENCES checkpoint_candidates(checkpoint_id),
    updated_at TEXT NOT NULL,
    CHECK (current_checkpoint_id IS NULL OR current_checkpoint_id <> previous_checkpoint_id)
);

CREATE TABLE runtime_token_hashes (
    run_id TEXT NOT NULL,
    attempt_id TEXT NOT NULL,
    token_sha256 TEXT NOT NULL,
    revoked_at TEXT,
    created_at TEXT NOT NULL,
    PRIMARY KEY(run_id, attempt_id)
);

CREATE TABLE oauth_revocations (
    token_ref_sha256 TEXT PRIMARY KEY,
    client_id TEXT NOT NULL,
    principal_id TEXT,
    reason_code TEXT NOT NULL,
    revoked_at TEXT NOT NULL,
    audit_ref TEXT NOT NULL
);

CREATE TABLE audit_log (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    audit_id TEXT NOT NULL UNIQUE,
    principal_id TEXT,
    client_id TEXT,
    action TEXT NOT NULL,
    operation_id TEXT,
    epoch INTEGER,
    revision INTEGER,
    audit_ref TEXT NOT NULL,
    recorded_at TEXT NOT NULL,
    metadata_json TEXT NOT NULL
);

CREATE TRIGGER audit_log_no_update
BEFORE UPDATE ON audit_log BEGIN SELECT RAISE(ABORT, 'audit_log is append-only'); END;
CREATE TRIGGER audit_log_no_delete
BEFORE DELETE ON audit_log BEGIN SELECT RAISE(ABORT, 'audit_log is append-only'); END;
