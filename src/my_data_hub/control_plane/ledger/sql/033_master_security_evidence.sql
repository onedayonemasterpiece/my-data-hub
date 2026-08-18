CREATE TABLE master_security_evidence (
    operation_id TEXT PRIMARY KEY REFERENCES operations(operation_id),
    run_id TEXT NOT NULL,
    attempt_id TEXT NOT NULL,
    master_instance_id TEXT NOT NULL,
    epoch INTEGER NOT NULL CHECK (epoch >= 1),
    source_commit TEXT NOT NULL CHECK (length(source_commit) = 40),
    schema_version INTEGER NOT NULL CHECK (schema_version >= 1),
    canonical_revision INTEGER NOT NULL CHECK (canonical_revision >= 0),
    role_probe_count INTEGER NOT NULL CHECK (role_probe_count >= 1),
    security_probe_count INTEGER NOT NULL CHECK (security_probe_count >= 1),
    role_verification_sha256 TEXT NOT NULL CHECK (length(role_verification_sha256) = 64),
    security_test_receipt_sha256 TEXT NOT NULL CHECK (length(security_test_receipt_sha256) = 64),
    observed_at TEXT NOT NULL,
    evidence_sha256 TEXT NOT NULL UNIQUE CHECK (length(evidence_sha256) = 64),
    UNIQUE (master_instance_id, epoch)
);

CREATE TRIGGER master_security_evidence_no_update
BEFORE UPDATE ON master_security_evidence
BEGIN
    SELECT RAISE(ABORT, 'master security evidence is append-only');
END;

CREATE TRIGGER master_security_evidence_no_delete
BEFORE DELETE ON master_security_evidence
BEGIN
    SELECT RAISE(ABORT, 'master security evidence is append-only');
END;
