-- Preserve the historical exact refs when an externally absent checkpoint
-- Dataset is recreated from a complete child snapshot at provider version 1.
CREATE TABLE checkpoint_dataset_incarnation_retirements (
    replacement_checkpoint_id TEXT PRIMARY KEY REFERENCES checkpoint_candidates(checkpoint_id),
    dataset_ref TEXT NOT NULL,
    source_head_checkpoint_id TEXT NOT NULL REFERENCES checkpoint_candidates(checkpoint_id),
    source_head_generation INTEGER NOT NULL CHECK (source_head_generation >= 1),
    retired_versions_json TEXT NOT NULL,
    retired_versions_sha256 TEXT NOT NULL CHECK (length(retired_versions_sha256) = 64),
    observed_absent_at TEXT NOT NULL
);

CREATE TRIGGER checkpoint_dataset_incarnation_retirements_no_update
BEFORE UPDATE ON checkpoint_dataset_incarnation_retirements
BEGIN SELECT RAISE(ABORT, 'checkpoint Dataset incarnation retirements are append-only'); END;

CREATE TRIGGER checkpoint_dataset_incarnation_retirements_no_delete
BEFORE DELETE ON checkpoint_dataset_incarnation_retirements
BEGIN SELECT RAISE(ABORT, 'checkpoint Dataset incarnation retirements are append-only'); END;
