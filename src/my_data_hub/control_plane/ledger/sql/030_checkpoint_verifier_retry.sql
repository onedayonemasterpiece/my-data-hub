ALTER TABLE checkpoint_blob_publications
    ADD COLUMN verifier_revision_sha256 TEXT
    CHECK (verifier_revision_sha256 IS NULL OR length(verifier_revision_sha256) = 64);

ALTER TABLE checkpoint_blob_publications
    ADD COLUMN verifier_attempts INTEGER NOT NULL DEFAULT 0
    CHECK (verifier_attempts BETWEEN 0 AND 3);
