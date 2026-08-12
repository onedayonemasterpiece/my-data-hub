CREATE TABLE connector_coverage_metadata (
    connector_kind TEXT PRIMARY KEY,
    contract_version TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('PENDING', 'COMPLETE', 'FAILED')),
    observed_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
