# Operational MVP evidence — 2026-08-11

Status: **implementation in progress; operational acceptance is blocked**.

This page is an evidence index, not a claim that the one-pass completion gate is
green.  The canonical database topology remains unchanged: a writable PostgreSQL
primary may run only in the ACTIVE protected Kaggle master notebook.  The devstand
contains only the lightweight control ledger/process and no canonical rows.

## Proven in this implementation branch

- The PR-A architecture reset was independently reviewed and merged as
  `de657d63e4662e69dfb7169bc67aa65e8a9bda71`.
- The SQLite WAL control ledger, lifecycle state machine, epochs, leases,
  provider-effect journal, callback deduplication, checkpoint pointers, OAuth
  client/revocation state, and bounded audit projections are implemented.
- FakeKaggle/state-machine tests include 10,000 generated sequences, concurrent
  `ensure_master`, crash/reconciliation, callback replay, stale epoch, and
  checkpoint failure cases.
- The single concrete provider transport uses exactly `kaggle==2.2.4`; the
  repository validator forbids another Kaggle transport implementation.
- A real disposable private Kaggle Dataset was created, read back by exact
  version/hash, denied to an unauthenticated client, and deleted by an exact
  task-created claim.  See
  [the sanitized receipt](evidence/2026-08-11-operational-mvp/kaggle-private-dataset-canary.json).
- The target YDB table was inventoried with a dedicated database-scoped
  `ydb.viewer` identity. A zero-row UPDATE was denied. The live bounded snapshot
  contained 266 distinct records across 14 batches and 14 source files. No row
  payload was persisted on the devstand. See
  [the sanitized inventory](evidence/2026-08-11-operational-mvp/ydb-readonly-inventory.json).
- The control-only Compose process was killed at its host PID, restarted by
  Docker, and retained its control state. See
  [the process-recovery receipt](evidence/2026-08-11-operational-mvp/control-process-recovery.json).
- The remote MCP reader runtime now uses the same durable control ledger for
  resolution, client authorization, revocation, and audit. A standard MCP 2.0
  Streamable HTTP client proves discovery and `platform.status` with
  `master_state=ABSENT`; revoked JWTs, database environment leakage, and reader
  write-tool discovery are denied in tests.
- Operational notebook sources are deterministic and bind exact wheel/source
  hashes. PostgreSQL master fencing, typed blogger import, separate E5/BGE spaces,
  deterministic RRF, and checkpoint verification have executable unit/integration
  contracts. They have **not** yet passed the required real Kaggle matrix.

## Exact external blocker

The installed legacy Kaggle username/key can mutate private datasets and submit a
private kernel, but Kaggle rejects exact source/status/output reads for newly
created private kernels with HTTP 403. The supported `kaggle==2.2.4` OAuth flow
requires a modern access/refresh token and an interactive owner sign-in. No
`KAGGLE_API_TOKEN` or `~/.kaggle/access_token` is present. The failed task-created
kernel was deleted and its absence observed; no unknown resource was modified.

Sanitized evidence:

- [legacy-auth kernel failure and cleanup](evidence/2026-08-11-operational-mvp/kaggle-notebook-legacy-auth-failure.json)
- [modern-token blocker](evidence/2026-08-11-operational-mvp/kaggle-modern-token-blocker.json)

Owner closure command (run locally without sharing the token):

```bash
cd /home/dev/projects/my-data-hub
.venv/bin/kaggle auth login
```

Proof after sign-in:

```bash
cd /home/dev/.codex/worktrees/my-data-hub/operational-mvp
.venv/bin/python scripts/provider/real_kaggle_matrix.py notebook-canary
```

The receipt must show an exact private source version, terminal output identity,
and claim-bound cleanup before any permanent master resource is created.

## Gates not claimed

Until the blocker is closed, the following remain fail-closed and must not be
reported as complete: permanent master/checkpoint resources; 15 real run IDs;
full YDB import into the Kaggle primary; E5/BGE 100% coverage; cold restore;
direct tunnel/broker proof; owner MCP writes; public DNS/TLS/OAuth; host reboot;
implementation PR merge; and deployment of its merge commit.

Region Talk remains paused and production publication remains disabled.
