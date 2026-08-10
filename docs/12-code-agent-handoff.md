# Handoff to the code agent

## Goal

Install the implemented R1 platform on the current permanent host, verify its recovery
and read-only boundaries, and then implement ADR-0015. Do **not** begin with the full
Region Talk data migration.

Region Talk remains the first migration workload after the reusable infrastructure gates
pass. Do not redesign the project around Region Talk's legacy backend and do not use
Kaggle as the master database.

Current handoff status: phases A-D have repository implementations and disposable test
evidence. Phase E has a fail-closed OAuth resource server but still needs the current
host's DNS/TLS/OAuth deployment receipt. Phase F is accepted design only. The checklists
below remain acceptance criteria; they must not be interpreted as an instruction to
discard or redo already-reviewed R1 code.

## Required reasoning level

- Primary implementation: **high**.
- Final security/data-integrity review: **xhigh** for OAuth, PostgreSQL grants,
  operator SQL, backup/restore, Kaggle protected-resource controls and migration gates.

## Phase 0 — capture the current same-host deployment

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
6. Preserve ADR-0009 through ADR-0015 and the connector/exchange schemas.

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
upgrade path, scope/backfill/policy negative tests, multi-consumer connector flow, role
negative tests and disposable operator preview/apply.

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

## Phase F — shared data scope, participation and policy

Implement ADR-0015 before connector fan-out or real Region Talk normalization:

1. Add an append-only migration for stable `orchestration.pipeline_identity` and
   `orchestration.project_pipeline`; keep existing `orchestration.pipeline` as the exact
   versioned execution definition.
2. Add catalog-object and `platform`/`project`/`pipeline`/`project_pipeline` scope registries
   with database CHECK/unique/FK constraints.
3. Add relation definitions, generic object-scope relations plus append-only relation
   events, namespaced current state plus append-only state events, policy
   definitions/decisions/evaluation receipts and object-usage events/summaries.
4. Backfill supported `actor`, `external_account`, `content_item` and `content_asset` rows.
5. Backfill `hub.project_content` into the generic relation without creating dual authority;
   keep it only as a documented compatibility/domain extension until consumers migrate.
6. Register idempotent Region Talk project, logical pipeline and exact project-pipeline scopes.
7. Give every state namespace exactly one writer and a normalized-class mapping. Do not use
   normalized class as publication authorization.
8. Implement immutable effective-policy evaluations and prove platform hard deny overrides
   local allow, including after duplicate identity remap.
9. Prove one object can have two project relations and different states in two project-pipeline
   scopes without duplication or cross-overwrite.
10. Prove usage does not create membership and work execution state does not create approval.
11. Update MCP/query views so an operator can explain relations, usage, exact/normalized state
    and effective policy independently.
12. Archive clean/upgrade/backfill/negative-test receipts. Do not claim the design implemented
    from documentation alone.

Canonical contract:
[`22-data-scope-and-pipeline-participation.md`](22-data-scope-and-pipeline-participation.md).

## Phase G — connector plane and first real producer

1. Add append-only `integration` migrations for connector registry, data products,
   batches, payload/artifact refs, events, many-to-many consumers, per-consumer applications,
   watermarks, quarantine and receipts.
2. Implement `/intake/v1/batches` and receipt lookup under separate service auth.
3. Implement strict `data-connector-envelope.v1` validation, body/item limits, SHA-256,
   exact replay and conflicting-replay quarantine.
4. Implement synthetic producer with durable local spool and retry/backoff.
5. Prove platform outage/restart does not lose or duplicate a batch.
6. Route one synthetic batch to at least two consumers/scopes; prove independent
   application status/receipt, optional-consumer isolation, replay and dedupe preserving both
   scope relations; read the results through MCP.
7. Register `events-bot.daily-statistics.v1` and implement a small non-sensitive daily
   aggregate connector in `events-bot-new` using its own durable outbox.
8. Monitor accepted plus per-consumer committed/reconciled/spool/watermark/quarantine
   cadence.
9. Do not let the bot write shared canonical tables directly.

## Phase H — Kaggle inventory and protected control

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

## Phase I — broad MCP database reader/editor

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

## Phase J — migration operator tools

Implement typed tools for:

- source revision/inventory/export plan;
- bounded read-only YDB export and manifest validation;
- raw landing dry-run/apply;
- row-kind/disposition/quarantine inspection;
- versioned transformer registration and bounded partition mapping;
- expected-revision quarantine resolution;
- row/identity/scope/queue/semantic reconciliation;
- shadow plan/run/diff;
- backup/freeze/final-delta/cutover preview;
- owner-approved cutover and rollback.

An agent may drive these tools. It may not set `cutover_ready`, erase quarantine or
bypass reconciliation with raw SQL.

## Phase K — Region Talk inventory, migration and shadow

Only after Phases B–J gates pass:

1. create protected read-only YDB migration credentials;
2. enumerate actual tables/row kinds and code references;
3. complete inventory with counts, key order, caps and semantic owner;
4. run deterministic bounded export and repeat logical hashes;
5. attest `project:region-talk` and exact Region Talk project-pipeline scope on every export
   batch, then land every row in `migration.raw_record`;
6. implement versioned row-kind transformers that atomically write target, target refs,
   provenance, required Region Talk relation/scoped state/usage and disposition;
7. preserve useful source/post/result/review/publication/history data;
8. repair queue into immutable `queue_seq` and explicit lanes;
9. reach full accounting and scope completeness with zero quarantine for cutover; prove
   normalized/deduplicated shared targets retain Region Talk relation and duplicate groups
   preserve union aliases/provenance/scopes;
10. port actual Region Talk stages behind current worker contracts;
11. run at least three representative shadow cycles;
12. explain every candidate/readiness/review/state/policy drift;
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
- scope/policy migration, backfill and negative-test receipts;
- connector multi-consumer application receipts;
- exact remaining blockers before Region Talk inventory;
- explicit scheduler/publication/Region Talk pipeline states.
