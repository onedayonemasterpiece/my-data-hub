# Migration procedure

## 0. Preconditions

- PostgreSQL migrations applied and backed up.
- Temporary `hub_migration` role has only staging/normalization grants.
- YDB credentials are read-only and isolated in a protected migration environment.
- Current Region Talk source code and schema commit is recorded.
- Production publication remains disabled throughout baseline/shadow import.

## 1. Inventory

Run a key/kind scan that does not stop at the orchestrator's operational row limits. Create
`inventory.json` with complete counts and hashes. Fail if pagination is incomplete or the
source database identity is unexpected.

## 2. Baseline export

Capture `watermark_start`, export in primary-key order, compute per-row payload hashes and a
file manifest, then capture `watermark_end`. The manifest records query/schema versions and
all files. Upload only to a private migration artifact location after secret scan.

## 3. Raw import

```bash
my-data-hub region-talk import-ydb-export \
  --manifest /private/export/manifest.json

my-data-hub region-talk import-ydb-export \
  --manifest /private/export/manifest.json \
  --apply
```

Importer rules:

- verify manifest and every file/row hash before a transaction;
- insert by `(export_batch_id, source_pk, payload_sha256)`;
- exact replay is a no-op;
- same batch/source PK with different hash is a conflict;
- invalid rows are retained in an error artifact and block acceptance;
- no normalization runs until raw count equals manifest count.

Before admitting a real export, run the live fixture gate against the target PostgreSQL instance:

```bash
python scripts/verify_region_talk_migration_flow.py
```

It must show `first_inserted=3`, `replay_inserted=0`, a quarantine-blocked intermediate state and a final cutover-ready reconciliation report.

## 4. Normalization

Run mapping release `region-talk-ydb-map.v1`. Each staging row becomes `normalized`, `deduplicated`, `intentionally_excluded`,
`retained_raw` or `quarantined`, with target references and reason. Mapping retries are
idempotent. Normalized writes and disposition update occur in one transaction per bounded
batch.

## 5. Reconciliation

Generate `migration-reconciliation-report.v1`, validate it against the repository
JSON Schema and execute the checks in `reconciliation.md`. Critical natural-key mismatches,
raw/manifest count drift, undispositioned rows and any remaining quarantine are blocking even
when the aggregate count equation balances.

## 6. Incremental catch-up

Repeat export for rows whose update timestamp or key/version changed after the baseline
watermark. Because timestamps alone may be imperfect, also compare primary-key/payload hash
indexes. The final catch-up runs after YDB write freeze.

## 7. Shadow cycles

Run the PostgreSQL orchestrator with external side effects disabled. Compare selected work,
gate decisions, queue counts and candidate revisions against the legacy system for several
cycles. Differences require classification: expected architecture-only, deliberate product
change with ADR, legacy defect corrected, or migration defect.

## 8. Cutover

Follow `cutover.md`. Record exact timestamps, commits, database revision, export manifests,
checks and rollback owner in a cutover receipt.
