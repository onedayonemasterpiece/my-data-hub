CREATE TABLE provider_effect_intents (
    effect_id TEXT PRIMARY KEY,
    operation_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    task_id TEXT NOT NULL,
    action TEXT NOT NULL,
    provider_ref TEXT NOT NULL,
    request_sha256 TEXT NOT NULL,
    intent_json TEXT NOT NULL,
    recorded_at TEXT NOT NULL
);

CREATE TABLE provider_effect_receipts (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    effect_id TEXT NOT NULL REFERENCES provider_effect_intents(effect_id),
    receipt_json TEXT NOT NULL,
    receipt_sha256 TEXT NOT NULL,
    recorded_at TEXT NOT NULL,
    UNIQUE(effect_id, receipt_sha256)
);

CREATE TABLE provider_resource_claims (
    claim_sha256 TEXT PRIMARY KEY,
    task_id TEXT NOT NULL,
    effect_id TEXT NOT NULL REFERENCES provider_effect_intents(effect_id),
    provider_ref TEXT NOT NULL,
    resource_kind TEXT NOT NULL,
    control_class TEXT NOT NULL,
    disposable INTEGER NOT NULL CHECK (disposable IN (0, 1)),
    provider_version INTEGER NOT NULL CHECK (provider_version >= 1),
    claim_json TEXT NOT NULL,
    recorded_at TEXT NOT NULL
);

CREATE TRIGGER provider_effect_intents_no_update
BEFORE UPDATE ON provider_effect_intents BEGIN SELECT RAISE(ABORT, 'provider_effect_intents is append-only'); END;
CREATE TRIGGER provider_effect_intents_no_delete
BEFORE DELETE ON provider_effect_intents BEGIN SELECT RAISE(ABORT, 'provider_effect_intents is append-only'); END;
CREATE TRIGGER provider_effect_receipts_no_update
BEFORE UPDATE ON provider_effect_receipts BEGIN SELECT RAISE(ABORT, 'provider_effect_receipts is append-only'); END;
CREATE TRIGGER provider_effect_receipts_no_delete
BEFORE DELETE ON provider_effect_receipts BEGIN SELECT RAISE(ABORT, 'provider_effect_receipts is append-only'); END;
CREATE TRIGGER provider_resource_claims_no_update
BEFORE UPDATE ON provider_resource_claims BEGIN SELECT RAISE(ABORT, 'provider_resource_claims is append-only'); END;
CREATE TRIGGER provider_resource_claims_no_delete
BEFORE DELETE ON provider_resource_claims BEGIN SELECT RAISE(ABORT, 'provider_resource_claims is append-only'); END;
