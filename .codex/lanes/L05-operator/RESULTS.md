# L05-operator results

## Lane contract

- Lane: `L05-operator`
- Requirement: `R09` core restricted database operator
- Base SHA: `0b6b7311081bdfecdd4f3004e5d6842a42f64253`
- Implementation SHA: `951e8101e8fa3bd84ef45f7aa395bcc6fad92189`
- Branch: `agent/r1-infrastructure-workflow/l05-operator`

## Outcome

`R09` core is **Done for R1 disposable-schema rollout**:

- pglast AST parsing requires exactly one statement and rejects reader/editor utility,
  DDL, transaction, multi-statement, nested modifying CTE, lock, `SELECT INTO`, and
  unsafe-function paths;
- reader permits only allowlisted `SELECT` and non-executing `EXPLAIN SELECT`, requires
  physical relations to be schema-qualified, fixes `search_path` to `pg_catalog`, and
  applies read-only transaction, statement, transaction, lock, idle, row, and serialized
  byte limits with explicit truncation reasons;
- editor permits only parameter-bound `INSERT`/`UPDATE`/`DELETE` against exact table and
  column grants, with single-target enforcement and no `INSERT ON CONFLICT` expansion;
- HMAC-SHA256 preview receipts are short-lived and bind principal, session/correlation,
  normalized SQL fingerprint, parameter fingerprint, target, canonical revision, effect
  bounds/preview count, and exact backup evidence state;
- apply verifies all bindings, rechecks the fail-closed backup/restore freshness gate and
  canonical revision in the write transaction, rolls back effect mismatches, commits once,
  signs the commit receipt, and replays matching idempotency keys without another write;
- R1 allowlists admit only the explicitly declared disposable schema; production rollout
  construction always returns an empty database/function allowlist;
- no MCP registration, migration, workflow, or existing configuration file was changed.

## Evidence and commands

- `.venv/bin/ruff check src/my_data_hub/db_operator tests/test_db_operator.py`
  - passed
- `.venv/bin/pytest -q tests/test_db_operator.py`
  - passed: 27 tests
- `.venv/bin/python -m compileall -q src tests`
  - passed
- `.venv/bin/pytest -q`
  - passed: 119 tests
- `git diff --check`
  - passed

The tests cover nested modifying CTEs, multi-statements, locks, `SELECT INTO`, unsafe
functions, allowlist escapes, parameter-token compilation, editor statement/column
restrictions, production-empty policy, all freshness evidence checks, read caps and
session controls, forged/expired/cross-principal receipts, revision/effect rollback,
exact backup-state rebinding, and idempotent replay/collision behavior.

## Changed files

- `src/my_data_hub/db_operator/__init__.py`
- `src/my_data_hub/db_operator/engine.py`
- `src/my_data_hub/db_operator/errors.py`
- `src/my_data_hub/db_operator/policy.py`
- `src/my_data_hub/db_operator/receipts.py`
- `src/my_data_hub/db_operator/sql.py`
- `tests/test_db_operator.py`
- `.codex/lanes/L05-operator/RESULTS.md`

## Risks and integration notes

1. `pglast` is currently in the repository's `dev` extra rather than runtime
   dependencies. The package fails closed with `SqlRejected` when it is absent, but the
   integrator must promote `pglast>=7,<8` to runtime dependencies before enabling an
   operator process. This lane could not edit `pyproject.toml`.
2. The supplied `InMemoryIdempotencyStore` proves replay/collision semantics only within
   one process lifetime. A production MCP adapter must inject a durable receipt store
   whose record is coordinated with the database apply transaction before R1 is enabled
   remotely; no operator-receipt migration was in this lane's scope.
3. Preview executes and rolls back DML. The engine therefore enforces an R1 disposable
   schema marker. Application-schema rollout must add a dedicated safe effect estimator
   or account for non-transactional trigger/sequence effects before relaxing this gate.
4. Execution tests use a deterministic DB-API fake and the full repository suite; a real
   PostgreSQL disposable-schema canary remains an integration/deployment gate.
5. `transaction_timeout` is a PostgreSQL 17+ setting. The repository's declared Compose
   database is PostgreSQL 18; older PostgreSQL targets must remain unsupported or gain a
   separately tested compatibility policy.
