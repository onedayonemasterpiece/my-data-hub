# Handoff to the code agent

## Goal

Turn the user-reported devstand deployment into a verified, recoverable and tested
platform; then add remote MCP, connectors, Kaggle control and restricted database
operator access. Do **not** begin with the full Region Talk data migration.

Region Talk remains the first migration workload after the reusable infrastructure gates
pass. Do not redesign the project around Region Talk's legacy backend and do not use
Kaggle as the master database.

## Required reasoning level

- Primary implementation: **high**.
- Final security/data-integrity review: **xhigh** for OAuth, PostgreSQL grants,
  operator SQL, backup/restore, Kaggle protected-resource controls and migration gates.

## Phase 0 — freeze and capture the actual deployment

1. Confirm the checkout remote/branch/commit and clean/dirty state.
2. Record OS, Docker/Compose, image digests, PostgreSQL/pgvector versions, volumes,
   listeners, firewall and service status.
3. Keep all of the following false:

   ```text
   MY_DATA_HUB_SCHEDULER_ENABLED=false
   MY_DATA_HUB_PRODUCTION_PUBLISH_ENABLED=false
   MY_DATA_HUB_MCP_WRITE_ENABLED=false
   MY_DATA_HUB_MCP_REMOTE_ENABLED=false
   ```

4. Confirm Region Talk pipeline is `paused`.
5. Run liveness/readiness, `db status`, `db verify` and repository tests.
6. Copy [`operations/first-deploy-template.md`](operations/first-deploy-template.md)
   to `docs/operations/first-deploy.md`, fill it with observed evidence and do not claim a
   check passed merely because configuration exists.

## Phase A — repository and source provenance

1. Ensure this repository is in `onedayonemasterpiece/my-data-hub` through a reviewed
   branch/PR after the initial base exists.
2. Import exact target-vision bytes from `idea-hub` commit `0c3fcf7` and update SHA-256.
3. Pin current source commits of `events-bot-new` and Region Talk.
4. Curate donor MCP and Region Talk code/docs through manifests; do not copy secrets,
   sessions, exports or unrelated runtime.
5. Reconcile target-vision conflicts through ADR.
6. Preserve new ADR-0009 through ADR-0014 and the connector/exchange schemas.

## Phase B — PostgreSQL roles, migrations and supervision

1. Use PostgreSQL 18 + pinned pgvector target.
2. Create separate owner/migrator, app, orchestrator, connector, MCP reader, MCP editor,
   migration operator, backup and monitoring roles.
3. Generate explicit grants and negative probes; new objects must not become remotely
   writable automatically.
4. Apply migrations on an empty database, verify, apply again and prove idempotency.
5. Test upgrade from the previous released schema revision.
6. Run live PostgreSQL verification scripts and archive JSON outputs.
7. Prove API/PostgreSQL/orchestrator restart after process kill and host reboot.
8. Verify only the intended TLS edge is public; PostgreSQL/API/MCP upstreams remain
   loopback/private.
9. Record the role matrix and commands without passwords.

## Phase C — backup/readback/restore gate

1. Run an encrypted local logical backup with manifest and SHA-256.
2. Upload an encrypted copy to an approved off-host target; private Kaggle is acceptable
   only as an `orchestrator_protected` backup resource.
3. Read bytes back and verify exact hash/privacy/version.
4. Restore into an isolated fresh PostgreSQL target.
5. Run migration status, database verify, extension/version, object counts and core
   invariants.
6. Destroy the restore target only after receipt archival.
7. Add backup freshness/restore status to health and make it an operator write gate.
8. Retain multiple generations. Do not expose backup contents through remote MCP.

## Phase D — automated workflows

Create/prove:

```text
.github/workflows/ci.yml
.github/workflows/devstand-deploy.yml
.github/workflows/devstand-nightly.yml
.github/workflows/kaggle-canary.yml
.github/workflows/restore-drill.yml
```

PR CI must cover static/unit/contracts, schemas/examples, clean and repeated migrations,
upgrade path, connector flow, role negative tests and disposable operator preview/apply.

Post-deploy must verify commit/images/revision/services/disabled gates/read-only MCP and a
synthetic connector replay.

Nightly must check backup, queue, connector cadence, remote auth negative cases and
read-only Kaggle inventory.

Weekly/manual must exercise isolated restore and disposable private Kaggle resources.
Each non-unit workflow emits a machine-readable receipt with run ID, commit, versions,
checks, resource IDs, hashes, cleanup and blockers.

## Phase E — remote MCP endpoint

1. Configure Yandex Cloud DNS/certificate/edge for:

   ```text
   https://mcp-datahub.kenigevents.ru/mcp
   ```

2. Expose only TCP 443; do not expose PostgreSQL or internal MCP port.
3. Port/adapt OAuth 2.1 resource/audience, Host/Origin, no-store, correlation and bounded
   response controls from the proven `events-bot-new` donor.
4. Keep development token loopback-only.
5. Start with semantic read-only tools only.
6. Test through MCP Inspector/equivalent and then ChatGPT.
7. Prove wrong/missing/expired token, audience, scope, Host and Origin fail.
8. Prove token/client revocation.
9. Archive DNS/certificate/listener IDs and non-secret receipts.

## Phase F — connector plane and first real producer

1. Add append-only `integration` migrations for connector registry, data products,
   batches, payload/artifact refs, events, watermarks, quarantine and receipts.
2. Implement `/intake/v1/batches` and receipt lookup under separate service auth.
3. Implement strict `data-connector-envelope.v1` validation, body/item limits, SHA-256,
   exact replay and conflicting-replay quarantine.
4. Implement synthetic producer with durable local spool and retry/backoff.
5. Prove platform outage/restart does not lose or duplicate a batch.
6. Normalize/commit the synthetic product and read it through MCP.
7. Register `events-bot.daily-statistics.v1` and implement a small non-sensitive daily
   aggregate connector in `events-bot-new` using its own durable outbox.
8. Monitor accepted/committed/spool/watermark/quarantine cadence.
9. Do not let the bot write shared canonical tables directly.

## Phase G — Kaggle inventory and protected control

1. Implement provider-neutral resource/operation/event registry plus Kaggle projection.
2. Inventory every visible notebook/kernel and private dataset with bounded pagination.
3. Assign unknown resources `external_read_only`; never infer control from name.
4. Register orchestrator workers/backups as `orchestrator_protected`.
5. Remote MCP may expose only minimal status for protected resources.
6. Add MCP-managed private dataset lifecycle: create, readback/hash/privacy, version,
   download and guarded delete.
7. Add MCP-managed notebook lifecycle: push/update/run/status/pull/output/delete using
   only provider-supported operations.
8. Do not expose cancel until a supported provider primitive and integration test exist.
9. Create `mcp_exchange` private TTL/hash-manifest package flow.
10. Ensure public dataset creation is absent from schemas/tools.
11. Use a separate Kaggle canary/MCP credential from orchestrator production where
    possible.
12. Prove protected resource mutation/download/delete is denied even with normal Kaggle
    write scope.

## Phase H — broad MCP database reader/editor

1. Add separate process/profile gates and OAuth scopes.
2. Implement broad bounded read query under a read-only DB role:
   - SQL AST classification;
   - allowlisted schemas/views/functions;
   - read-only transaction;
   - statement/transaction/lock/idle timeouts;
   - row/byte caps and explicit truncation;
   - no multi-statement, DML/DDL/COPY/CALL/DO/SET or unsafe functions.
3. Implement data-editor preview/apply for parameterized INSERT/UPDATE/DELETE:
   - separate DB role;
   - allowlisted targets;
   - short-lived receipt bound to principal/SQL/params/revision/effect/backup;
   - idempotency and one transaction;
   - impact tiers and pre-change checkpoint;
   - immutable audit and commit receipt.
4. Prove database grants block forbidden actions if parser/application checks are
   bypassed.
5. Start in a disposable schema. Enable selected non-critical application tables only
   after adversarial tests and restore evidence.
6. Keep DDL, roles, ownership, extension installation, server files and superuser local
   break-glass only.
7. Generic editor must not change migration accounting/cutover, provider control class,
   append-only audit/receipts or publication state.

## Phase I — migration operator tools

Implement typed tools for:

- source revision/inventory/export plan;
- bounded read-only YDB export and manifest validation;
- raw landing dry-run/apply;
- row-kind/disposition/quarantine inspection;
- versioned transformer registration and bounded partition mapping;
- expected-revision quarantine resolution;
- row/identity/queue/semantic reconciliation;
- shadow plan/run/diff;
- backup/freeze/final-delta/cutover preview;
- owner-approved cutover and rollback.

An agent may drive these tools. It may not set `cutover_ready`, erase quarantine or
bypass reconciliation with raw SQL.

## Phase J — Region Talk inventory, migration and shadow

Only after Phases B–I gates pass:

1. create protected read-only YDB migration credentials;
2. enumerate actual tables/row kinds and code references;
3. complete inventory with counts, key order, caps and semantic owner;
4. run deterministic bounded export and repeat logical hashes;
5. land every row in `migration.raw_record`;
6. implement versioned row-kind transformers;
7. preserve useful source/post/result/review/publication/history data;
8. repair queue into immutable `queue_seq` and explicit lanes;
9. reach full accounting with zero quarantine for cutover;
10. port actual Region Talk stages behind current worker contracts;
11. run at least three representative shadow cycles;
12. explain every candidate/readiness/review drift;
13. run private review canary;
14. fresh backup, freeze, final delta, cutover and rollback rehearsal;
15. keep production publishing off until separate owner approval;
16. retain YDB read-only through rollback window.

## Required final report for the infrastructure-first assignment

- commit/PR and deployed commit links;
- first-deploy receipt and open ports;
- PostgreSQL role/grant matrix and negative tests;
- migration/upgrade/idempotency evidence;
- service restart/reboot evidence;
- backup/readback/isolated restore receipt;
- CI/post-deploy/nightly/provider workflow run IDs;
- remote MCP URL, OAuth/tool/scope/negative-test evidence;
- synthetic connector and events-bot connector status;
- Kaggle inventory/control-class/canary/protected-denial evidence;
- database reader/editor disposable-schema evidence;
- exact remaining blockers before Region Talk inventory;
- explicit scheduler/publication/Region Talk pipeline states.
