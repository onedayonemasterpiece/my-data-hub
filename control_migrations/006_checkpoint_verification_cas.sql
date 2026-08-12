ALTER TABLE checkpoint_heads RENAME TO checkpoint_heads_legacy;
ALTER TABLE checkpoint_candidates RENAME TO checkpoint_candidates_legacy;

CREATE TABLE checkpoint_candidates (
    checkpoint_id TEXT PRIMARY KEY,
    service_kind TEXT NOT NULL,
    operation_id TEXT NOT NULL REFERENCES operations(operation_id),
    dataset_ref TEXT NOT NULL,
    version_ref TEXT,
    manifest_sha256 TEXT NOT NULL,
    source_checkpoint_id TEXT REFERENCES checkpoint_candidates(checkpoint_id),
    source_head_generation INTEGER NOT NULL CHECK (source_head_generation >= 0),
    master_instance_id TEXT NOT NULL,
    epoch INTEGER NOT NULL CHECK (epoch >= 1),
    status TEXT NOT NULL CHECK (
        status IN (
            'CANDIDATE', 'UPLOADED', 'READBACK_VERIFIED',
            'RESTORE_VERIFIED', 'VERIFIED', 'FAILED'
        )
    ),
    failure_code TEXT,
    created_at TEXT NOT NULL,
    verified_at TEXT,
    UNIQUE(dataset_ref, version_ref)
);

CREATE TABLE checkpoint_heads (
    service_kind TEXT PRIMARY KEY,
    generation INTEGER NOT NULL CHECK (generation >= 0),
    current_checkpoint_id TEXT REFERENCES checkpoint_candidates(checkpoint_id),
    previous_checkpoint_id TEXT REFERENCES checkpoint_candidates(checkpoint_id),
    updated_at TEXT NOT NULL,
    CHECK (current_checkpoint_id IS NULL OR current_checkpoint_id <> previous_checkpoint_id)
);

INSERT INTO checkpoint_candidates (
    checkpoint_id, service_kind, operation_id, dataset_ref, version_ref,
    manifest_sha256, source_checkpoint_id, source_head_generation,
    master_instance_id, epoch, status, failure_code, created_at, verified_at
)
SELECT
    checkpoint_id,
    'postgres-master',
    operation_id,
    dataset_ref,
    version_ref,
    manifest_sha256,
    source_checkpoint_id,
    CASE WHEN source_checkpoint_id IS NULL THEN 0 ELSE 1 END,
    master_instance_id,
    epoch,
    CASE WHEN status = 'CANDIDATE' THEN 'UPLOADED' ELSE status END,
    failure_code,
    created_at,
    verified_at
FROM checkpoint_candidates_legacy;

INSERT INTO checkpoint_heads (
    service_kind, generation, current_checkpoint_id, previous_checkpoint_id, updated_at
)
SELECT
    service_kind,
    CASE WHEN current_checkpoint_id IS NULL THEN 0 ELSE 1 END,
    current_checkpoint_id,
    previous_checkpoint_id,
    updated_at
FROM checkpoint_heads_legacy;

DROP TABLE checkpoint_heads_legacy;
DROP TABLE checkpoint_candidates_legacy;
