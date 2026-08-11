ALTER TABLE blogger_migration_requests
ADD COLUMN quarantine_receipt_json TEXT;

ALTER TABLE blogger_migration_requests
ADD COLUMN quarantine_receipt_sha256 TEXT CHECK (
    quarantine_receipt_sha256 IS NULL OR length(quarantine_receipt_sha256)=64
);

CREATE TRIGGER blogger_quarantine_receipt_no_rewrite
BEFORE UPDATE OF quarantine_receipt_json,quarantine_receipt_sha256
ON blogger_migration_requests
WHEN OLD.quarantine_receipt_json IS NOT NULL
 AND (NEW.quarantine_receipt_json IS NOT OLD.quarantine_receipt_json
      OR NEW.quarantine_receipt_sha256 IS NOT OLD.quarantine_receipt_sha256)
BEGIN SELECT RAISE(ABORT, 'blogger quarantine receipt is immutable'); END;
