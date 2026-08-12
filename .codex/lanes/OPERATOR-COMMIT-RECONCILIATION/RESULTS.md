# Operator post-commit reconciliation results

## Scope and base

- Lane: `OPERATOR-COMMIT-RECONCILIATION`
- Branch/worktree: `agent/operator-reconcile-single-provider` /
  `operator-reconcile-single-provider`
- Exact base: `c4a9992`.
- No live deployment or production mutation was performed.

## Confirmed root cause

The restricted PostgreSQL transaction durably committed canonical DML, revision CAS, semantic
outbox, audit, and `operator_control.mcp_transaction_receipt` before returning. A process crash or
response loss before `LedgerWriteGate.record_write_result()` left the non-canonical SQLite
projection in `APPLYING`. The old PostgreSQL receipt omitted request hash, master instance, and
epoch, while status never queried PostgreSQL. Retrying DML was correctly forbidden but there was no
exact recovery path.

## Corrective contract

- Append-only PostgreSQL migration `0017` adds request hash, master epoch, and master instance to
  new immutable receipts; prior v1 rows remain intact but cannot be misrepresented as v2 evidence.
- `commit_mcp_change_v2` records those bindings in the same canonical transaction and its audit and
  semantic-outbox payloads.
- The old v1 commit function is removed from `mdh_mcp_editor`; the editor receives only v2 commit
  and exact reconciliation EXECUTE rights, never table SELECT, owner, DDL, or bypass privileges.
- `reconcile_mcp_change` is a SECURITY DEFINER, epoch-fenced, exact-match lookup by operation,
  request hash, master instance/epoch, previous revision, actor, and client. Missing receipt returns
  no row; any identity mismatch fails closed.
- `data.change.reconcile` is an internal broker operation, absent from the MCP catalog. It opens a
  read-only, short-lived operator session and carries metadata only—never caller SQL/parameters.
- `HubService` attempts reconciliation from both `data.change.status` and an exact apply retry.
  A missing canonical receipt keeps retry denied; an existing exact receipt advances SQLite without
  reissuing DML.
- `ControlLedger.reconcile_mcp_write_commit()` performs validation and projection in one SQLite
  IMMEDIATE transaction. Repeated identical receipt projection is a no-op; a different receipt or
  lifecycle state is rejected.

## Evidence

- Focused SQL/broker/gateway tests: PASS (`20` tests at the focused checkpoint).
- Crash-window test proves one `APPLYING -> COMMITTED_PENDING_CHECKPOINT` projection, status-driven
  reconciliation, metadata-only broker arguments, no `data.change.apply` session, retry denial
  after admission, and idempotent replay.
- Disposable tmpfs PostgreSQL test: PASS. It proves v2 same-transaction commit, exact reconciliation,
  mismatched request-hash denial, schema revision 17, and denial of the old v1 function privilege.
- Full suite: PASS (`838` collected; three expected opt-in skips in the ordinary non-live run).
- `python -m compileall -q src tests`: PASS.
- `ruff check .`: PASS.
- configured strict `mypy`: PASS.
- notebook drift check: PASS.
- repository validator: PASS (`3366` checks, zero errors/notes).
- `git diff --check`: PASS.
