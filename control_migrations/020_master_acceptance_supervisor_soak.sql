ALTER TABLE master_acceptance_runtime_controls ADD COLUMN armed_at TEXT;
ALTER TABLE master_acceptance_runtime_controls ADD COLUMN expires_at TEXT;
ALTER TABLE master_acceptance_runtime_controls ADD COLUMN before_boot_id TEXT;
ALTER TABLE master_acceptance_runtime_controls ADD COLUMN directive_receipt_sha256 TEXT;
ALTER TABLE master_acceptance_runtime_controls ADD COLUMN callback_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE master_acceptance_runtime_controls ADD COLUMN soak_started_monotonic_ns INTEGER;
ALTER TABLE master_acceptance_runtime_controls ADD COLUMN soak_deadline_monotonic_ns INTEGER;

CREATE TABLE master_acceptance_soak_steps (
    task_id TEXT NOT NULL REFERENCES master_acceptance_runtime_controls(task_id) ON DELETE CASCADE,
    step INTEGER NOT NULL CHECK (step BETWEEN 1 AND 12),
    state TEXT NOT NULL CHECK (state IN ('REQUESTED','COMPLETED')),
    requested_at TEXT NOT NULL,
    completed_at TEXT,
    receipt_json TEXT,
    receipt_sha256 TEXT CHECK (receipt_sha256 IS NULL OR length(receipt_sha256)=64),
    PRIMARY KEY(task_id,step),
    CHECK ((state='REQUESTED' AND completed_at IS NULL AND receipt_json IS NULL AND receipt_sha256 IS NULL)
        OR (state='COMPLETED' AND completed_at IS NOT NULL AND receipt_json IS NOT NULL AND receipt_sha256 IS NOT NULL))
);
