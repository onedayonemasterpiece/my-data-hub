# FINAL-YDB-QUARANTINE results

Base: `97c40e4e0ba28b38c69a5f028748432d3475092f`

## Delivered

- Classifies every observed YDB item before canonical writes. Strict typed rows keep
  their exact source payload; malformed, missing, unknown, non-JSON and oversized
  values become bounded `region-talk-blogger-quarantine-evidence.v1` JSON in the
  master PostgreSQL landing schema with an explicit terminal `quarantined`
  disposition.
- Preserves immutable-key conflicts without updating the original raw record. A
  deterministic conflict raw record links to the original evidence and receives
  `same_source_key_different_payload`; exact conflict replay is a receipt-stable
  no-op.
- Makes source-count mismatch, duplicate source identity, partial replay, malformed
  evidence, duplicate-account decisions, undispositioned evidence and quarantine
  fail closed. Blocked batches commit evidence as `rejected` but do not advance the
  canonical revision, emit a checkpoint outbox effect, or return a successful
  in-master stage receipt.
- Preflights account ownership before canonical writes. Conflicts are terminalized
  as `quarantined` duplicate decisions while all source rows receive a disposition.
- Keeps all payloads inside the ACTIVE master database and the existing
  `mdh_migration_operator` role. No control-plane/devstand payload surface and no
  new privilege or migration were required: the append-only `migration.raw_record`,
  one-row `migration.row_disposition`, duplicate evidence tables and existing
  accounting views already provide the required durable model.

## Fault proof

The disposable PostgreSQL test proves in one real transaction/migration stack:

- successful import and exact replay;
- same key/different payload creates immutable quarantine evidence and exact replay
  returns the same blocked receipt;
- malformed/unknown 200 KiB input and oversized typed values are durably bounded and
  quarantined;
- missing source rows produce a rejected, non-complete batch;
- all stored raw rows have exactly one disposition and all batch accounting has zero
  undispositioned rows;
- canonical revision/outbox remain unchanged by every fault;
- migration reader privileges still cannot read raw payloads and migration operator
  still cannot update them.

## Validation

- `uv run ruff check .` — passed.
- `uv run python -m compileall -q src tests` — passed.
- `uv run python scripts/validate_repository.py` — 3,085 checks, zero errors.
- `uv run python scripts/create_notebooks.py --check` — no drift.
- `uv run pytest -q` — passed (two pre-existing opt-in skips).
- `MDH_RUN_DISPOSABLE_POSTGRES=1 uv run pytest -q tests/master/test_live_postgres.py -k old_session_commit` — passed against disposable tmpfs PostgreSQL.
