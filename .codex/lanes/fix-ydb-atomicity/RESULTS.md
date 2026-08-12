# R-YDB / H5 results

Base: `b095f28c845251d1724cdc0e8bd7bfd44eb30549`

## Outcome

- **Done — transactional accounting gate.** The importer now rejects incomplete
  source accounting and any pending duplicate-decision group inside the same
  PostgreSQL transaction, before revision advancement, audit/outbox creation, or
  batch acceptance.
- **Done — rollback and replay proof.** The disposable PostgreSQL integration test
  proves a conflicting account import leaves no second batch or duplicate group,
  adds no outbox/audit effect, does not advance the canonical revision, and leaves
  the successful exact replay as a no-op.
- **Done — executed-query provenance.** Query identity is now SHA-256 over the exact
  UTF-8 bytes passed to YDB (explicitly no whitespace/case/quoting normalization).
  Import identities, receipts, and provenance use that value. The reader validates
  the exact statement/fingerprint pair immediately before execution, and a mutation
  test proves SQL drift cannot retain the prior claimed hash.

## Root cause

The database accounting tuple was gated in-transaction, but the count of pending
`migration.duplicate_group` rows was only copied into the receipt. The notebook
then rejected `receipt.accounting_complete` after the transaction had committed,
so the canonical revision and required-checkpoint outbox effect survived. Query
provenance separately used a hard-coded normalized inventory value rather than the
hash of the literal statement executed by the reader.

## Validation

- `python -m compileall -q src tests` — passed.
- `python scripts/create_notebooks.py --check` — passed; no generated notebook drift.
- `python scripts/validate_repository.py` — passed (`2780` checks, no errors).
- `python -m pytest` — passed (`399 passed, 1 skipped`).
- `MDH_RUN_DISPOSABLE_POSTGRES=1 python -m pytest -q tests/master/test_live_postgres.py`
  — passed (`1 passed`), including rollback/no-revision/no-outbox proof.
- `python -m pytest -q tests/bloggers` — passed (`9 passed`).
- `python -m ruff check` on changed Python scopes — passed.
