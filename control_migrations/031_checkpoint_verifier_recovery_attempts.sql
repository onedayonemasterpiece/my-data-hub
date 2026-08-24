ALTER TABLE checkpoint_blob_publications
    ADD COLUMN verifier_recovery_attempts INTEGER NOT NULL DEFAULT 0
    CHECK (verifier_recovery_attempts BETWEEN 0 AND 2);
