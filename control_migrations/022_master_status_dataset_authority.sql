CREATE TABLE master_status_dataset_authorities (
    operation_id TEXT PRIMARY KEY REFERENCES operations(operation_id),
    run_id TEXT NOT NULL UNIQUE,
    attempt_id TEXT NOT NULL UNIQUE,
    token_sha256 TEXT NOT NULL CHECK (length(token_sha256)=64),
    creator_claim_until TEXT NOT NULL,
    expected_content_tree_sha256 TEXT NOT NULL CHECK (length(expected_content_tree_sha256)=64),
    resource_lease_json TEXT NOT NULL,
    status_dataset_json TEXT,
    cleanup_receipt_json TEXT,
    cleanup_claim_until TEXT,
    state TEXT NOT NULL CHECK (state IN ('CREATING','READY','CLEANING','CLEANED','AMBIGUOUS')),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX master_status_dataset_authority_state_idx
    ON master_status_dataset_authorities(state, creator_claim_until);
