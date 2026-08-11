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
- The now-reproducible repository command repeated that real Dataset gate with a
  second task-owned private resource and exact cleanup. See
  [the schema-validated second receipt](evidence/2026-08-11-operational-mvp/kaggle-private-dataset-canary-2.json).
  Dataset canaries are real provider mutations but are not Notebook run IDs and do
  not reduce the required 15-run minimum.
- Receipt v2 then repeated the same gate from a clean exact source commit and added
  explicit gate results, counts, cleanup and blockers. See
  [the exact-commit third receipt](evidence/2026-08-11-operational-mvp/kaggle-private-dataset-canary-3.json).
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
- Nightly and weekly/manual acceptance runners now execute bounded real-provider
  inventory, MCP authentication negatives, lifecycle-receipt checks, and
  checkpoint/master/provider/embedding evaluations.  Their current
  credential-free receipt exits `78` with 14 explicit `BLOCKED` checks; it does
  not turn absent live interfaces or credentials into a pass.
- Scheduled acceptance now has schema-validated receipts, exact current/previous
  checkpoint metadata, bounded control-ledger connector coverage, production
  policy/fencing denial probes, and a durable restore/rotation consumer heartbeat.
  It submits and polls each accepted operation to a terminal state sequentially;
  verifier timeouts are capped by the remaining request budget and a late return
  is recorded as `FAILED`, not durable success. No live scheduled receipt is
  claimed because provider credentials and a deployed consumer are absent.
- The post-deploy workflow runs only trusted default-branch verifier code and
  evidence-binds an exact approved reachable merge commit without checking out
  or executing that deployed revision. It also requires
  DNS and CA-valid TLS, OAuth metadata/JWKS, Host/Origin/authentication negatives,
  a cold `ABSENT` to fenced `ACTIVE` master read, forbidden-public-port probes,
  and fresh Ed25519-signed metadata-only host evidence for the three supervised
  services, absence of local database state, process replacement and reboot
  recovery.  This is tested automation only: no public endpoint or signed live
  host receipt exists yet.
- A three-stage host-derived evidence collector and schemas now cover immutable
  image IDs, equal source/installed release tree hashes, loopback listeners,
  absence of local database state and process/reboot recovery.  GitHub's
  `devstand` Environment was observed through the GitHub API at 2026-08-11
  05:03 UTC with one custom deployment branch policy, `main`, and zero
  Environment secrets. Required-reviewer protection and live credentials have
  not been installed, so no workflow success is claimed.
- Operational notebook sources are deterministic and bind exact wheel/source
  hashes. PostgreSQL master fencing, typed blogger import, separate E5/BGE spaces,
  deterministic RRF, and checkpoint verification have executable unit/integration
  contracts. They have **not** yet passed the required real Kaggle matrix.
- The checkpoint execution path is now wired end to end in code: the master uses
  the single official adapter inside Kaggle, streams physical/WAL/logical backup
  artifacts only below `/kaggle/working`, publishes a permanent private exact
  Dataset version, performs exact numeric-version readback, launches a separate
  restore verifier Notebook, and advances metadata-only current/previous HEAD by
  compare-and-swap. A later master boots only from that exact verified HEAD.
  These statements describe tested code contracts, not a completed real checkpoint.
- The FINAL-BLOGGER command now implements the metadata-only external closure and
  the in-master bounded YDB stage. It requires an acknowledged exact 266-row import
  receipt before checkpoint publication, then a verified exact-version checkpoint,
  independent restore, cold rotation, accounting, and bounded MCP projection.
  Persistent import-receipt callback loss fences/stops the ephemeral master and
  deliberately preserves the previous HEAD. This path has not been executed live.
- The FINAL-EMBED command now validates the complete FINAL-BLOGGER receipt and exact
  pinned generated E5/BGE worker identities before accepting worker/import evidence;
  bearer input is environment-only. It remains an honest fail-closed scaffold:
  the production control request/status endpoints and MCP vector capability are not
  implemented, so even valid external credentials currently produce exit `78`
  before mutation.
- The provider-real driver can execute 16 distinct private generated-Notebook
  platform-smoke runs with exact launch fences and reconciliation. It intentionally
  emits only `SMOKE_PASS` plus `MANDATORY_OPERATIONAL_SCENARIOS_NOT_EXECUTED` and
  exit `78`; it is not the required operational master/import/embedding/restore
  scenario matrix.
- At pre-documentation integration commit
  `a7524eb39b20dfa43c0216641df635fab1d268fe`, local validation reports 3,085
  repository checks with zero errors and 655 tests pass with only the separately
  opt-in live PostgreSQL test skipped (656 collected). That disposable PostgreSQL
  18 fencing test passes when explicitly enabled. Strict type checks cover the
  high-risk protocol/manifest modules and a reachable-history secret scan passes.
  These results are code and disposable-integration evidence, not real-provider
  or deployment acceptance; exact-head hosted checks follow the final evidence
  commit.

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
cd /home/dev/.codex/worktrees/my-data-hub/operational-mvp
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

In addition to external blockers, two implementation gates remain open: the
16-run provider driver covers platform smoke only rather than the mandatory
operational scenario matrix, and production embedding request/status plus vector
MCP interfaces do not exist yet. Credentials alone cannot close those gates.

Region Talk remains paused and production publication remains disabled.
