# L02-recovery results

## Scope

- Lane: `L02-recovery`
- Requirement: `R04`
- Status: **Done**
- Base SHA: `0b6b7311081bdfecdd4f3004e5d6842a42f64253`
- Implementation head SHA: `df04b7196bf76742ef4b8064834152dd9594f99d`

## Delivered

- Backup streams PostgreSQL custom-format bytes directly into `age`; no plaintext dump
  file is created during backup. Required recipient/source/commit/retention gates fail
  closed, and the encrypted artifact is bound to a strict v2 manifest.
- Provider-neutral off-host adapter protocol performs an explicit upload and independent
  readback, suppresses adapter output, enforces timeouts, and creates evidence only after
  exact encrypted SHA-256 and byte-size agreement.
- Restore requires exact manifest and off-host-evidence validation, two explicit restore
  confirmations, a distinct isolated target ID, expected connected database name, and
  zero user relations before decrypt/restore. Restore is transactional, runs application
  database verification, does not clean a populated target, and never auto-promotes.
- Successful recovery writes a new, mode-0600, schema-valid, self-hashed recovery receipt
  containing no database URL, age identity, provider credential, or raw data.
- Operational documentation records required environment gates, the exact adapter
  interface, limitations, and invocation examples.

## Evidence and commands run

All commands ran from `/home/dev/.codex/worktrees/my-data-hub/l02-recovery`.

- `bash -n scripts/backup_postgres.sh scripts/restore_postgres.sh` — passed.
- `uv run --isolated --extra dev ruff check scripts/recovery tests/test_recovery.py` — passed.
- `uv run --isolated --extra dev python -m compileall -q src tests scripts/recovery` — passed.
- `uv run --isolated --extra dev pytest -q tests/test_recovery.py` — passed, 6 tests.
- `uv run --isolated --extra dev pytest -q` — passed, 98 tests.
- `uv run --isolated --extra dev python scripts/validate_repository.py` — passed,
  1,307 checks and zero errors.
- `git diff --check` and `git diff --cached --check` — passed.

Tests use local fake PostgreSQL/age/provider executables and temporary paths. They exercise
backup streaming, manifest tamper rejection, exact readback acceptance/rejection, fresh
isolated restore ordering, receipt generation/schema validation, and secret-field absence.
No live database, provider, or external service was mutated.

## Changed files

- `scripts/backup_postgres.sh`
- `scripts/restore_postgres.sh`
- `scripts/recovery/common.py`
- `scripts/recovery/create_manifest.py`
- `scripts/recovery/offhost_roundtrip.py`
- `scripts/recovery/validate_target.py`
- `scripts/recovery/verify_artifact.py`
- `scripts/recovery/verify_offhost_evidence.py`
- `scripts/recovery/write_receipt.py`
- `schemas/recovery-receipt.v1.schema.json`
- `examples/recovery-receipt.v1.json`
- `tests/test_recovery.py`
- `docs/operations/backup-and-recovery.md`
- `.codex/lanes/L02-recovery/RESULTS.md`

## Residual risks / required deployment proof

- This lane intentionally performed no live PostgreSQL restore or external provider
  mutation. Deployment still must prove a real compatible PostgreSQL 18 restore,
  extensions, provider privacy controls, provider adapter behavior, firewall isolation,
  measured RPO/RTO, and target destruction after receipt archival.
- Adapter executables are a privileged local trust boundary. They must independently read
  provider bytes and must not substitute the local source during readback.
- URL equality and distinct target labels are defense in depth, not network-isolation
  proof; infrastructure must enforce host/account/network separation.
- Restore necessarily materializes a mode-0600 plaintext temporary dump for `pg_restore`;
  it is protected by `umask 077` and an exit trap, but the restore host and temporary
  filesystem must be trusted and encrypted.
