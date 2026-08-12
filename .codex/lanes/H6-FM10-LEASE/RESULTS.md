# H6-FM10-LEASE results

Status: implemented as contract/production adapter; no live PASS claimed.

Owned changes:

- `src/my_data_hub/acceptance/lease_expiry_denial.py`
- `tests/acceptance/test_lease_expiry_denial.py`
- `schemas/acceptance/lease-expiry-denial-completion.v1.schema.json`
- `docs/operations/fm10-lease-expiry-denial.md`

The adapter uses fixed SQL, separate restricted operator/reader sessions,
durable exact renewal suspension, real bounded monotonic waiting, deferred and
immediate SQLSTATE-55000/INERROR assertions, explicit rollbacks, aggregate
revision/row/outbox/audit equality, stable UUID/SHA-256 receipts, and
create-once private completion reconciliation. It persists no credential, DSN,
SQL parameter, or canonical row.

Production handoff: compose with `ControlLedgerLeaseExpiryRenewal` and
`DirectoryOperatorConnectionFactory` from master-scenario commit `346ec58`,
plus `DirectoryAcceptanceObserverConnectionFactory` and a private completion
journal. The operator factory in `346ec58` was reported to its owner because it
uses obsolete `ExecutionLimits` keyword names; that must be corrected before
live use.

Validation on the lane head:

- `python -m compileall -q src tests`: PASS
- `python -m pytest -q`: PASS (956 collected; 954 passed, 2 skipped)
- focused FM10 suite: PASS (10 tests)
- `python -m ruff check .`: PASS
- `python scripts/validate_repository.py`: PASS (3,551 checks, 0 errors)
- `git diff --check`: PASS

No PostgreSQL lease was expired by these tests and no live evidence is claimed.
