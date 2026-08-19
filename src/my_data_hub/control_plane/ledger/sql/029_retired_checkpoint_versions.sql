-- Preserve the exact external version identity after a failed checkpoint
-- Dataset has been independently proven deleted.  Clearing the active
-- version claim permits the provider to recreate version 1 when no durable
-- HEAD exists, while this append-only receipt keeps the historical binding.
CREATE TABLE retired_checkpoint_versions (
    checkpoint_id TEXT PRIMARY KEY REFERENCES checkpoint_candidates(checkpoint_id),
    dataset_ref TEXT NOT NULL,
    version_ref TEXT NOT NULL,
    deletion_effect_id TEXT NOT NULL REFERENCES provider_effect_intents(effect_id),
    deletion_receipt_sha256 TEXT NOT NULL CHECK (length(deletion_receipt_sha256) = 64),
    retired_at TEXT NOT NULL
);

CREATE TRIGGER retired_checkpoint_versions_no_update
BEFORE UPDATE ON retired_checkpoint_versions
BEGIN SELECT RAISE(ABORT, 'retired_checkpoint_versions is append-only'); END;

CREATE TRIGGER retired_checkpoint_versions_no_delete
BEFORE DELETE ON retired_checkpoint_versions
BEGIN SELECT RAISE(ABORT, 'retired_checkpoint_versions is append-only'); END;
