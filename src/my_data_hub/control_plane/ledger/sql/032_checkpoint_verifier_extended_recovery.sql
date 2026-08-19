CREATE TABLE checkpoint_verifier_extended_recovery_attempts (
    checkpoint_id TEXT NOT NULL REFERENCES checkpoint_blob_publications(checkpoint_id),
    verifier_revision_sha256 TEXT NOT NULL CHECK (length(verifier_revision_sha256) = 64),
    sequence INTEGER NOT NULL CHECK (sequence BETWEEN 1 AND 3),
    recovered_failure_code TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (checkpoint_id, verifier_revision_sha256),
    UNIQUE (checkpoint_id, sequence)
);

CREATE TRIGGER checkpoint_verifier_extended_recovery_no_update
BEFORE UPDATE ON checkpoint_verifier_extended_recovery_attempts
BEGIN
    SELECT RAISE(ABORT, 'checkpoint verifier recovery attempts are append-only');
END;

CREATE TRIGGER checkpoint_verifier_extended_recovery_no_delete
BEFORE DELETE ON checkpoint_verifier_extended_recovery_attempts
BEGIN
    SELECT RAISE(ABORT, 'checkpoint verifier recovery attempts are append-only');
END;
