# Infrastructure-first plan after the first devstand deployment

Status: `ACCEPTED IMPLEMENTATION PLAN`
Date: 2026-08-10
Related decisions: ADR-0009, ADR-0013, ADR-0014, ADR-0015

## 1. What to do first

The first task after putting the repository on the devstand is **not** the Region Talk
YDB migration. The first task is to turn the host into a measurable, recoverable and
testable platform baseline.

The required order is:

```text
freeze dangerous capabilities
→ capture deployment facts
→ verify PostgreSQL and roles
→ prove backup and isolated restore
→ establish CI and scheduled runtime tests
→ expose remote read-only MCP
→ implement and backfill the shared scope/policy foundation
→ prove one synthetic multi-consumer data connector
→ prove Kaggle sandbox control
→ prove bounded database-operator access
→ start Region Talk inventory and migration
```

This keeps infrastructure failures separate from migration mapping failures.

## 2. Freeze the system before verification

The following values remain false until their specific gate is accepted:

```text
MY_DATA_HUB_SCHEDULER_ENABLED=false
MY_DATA_HUB_PRODUCTION_PUBLISH_ENABLED=false
MY_DATA_HUB_MCP_WRITE_ENABLED=false
MY_DATA_HUB_MCP_REMOTE_ENABLED=false
```

The Region Talk pipeline remains `paused`. No YDB export credentials are installed on
the normal application service. No Kaggle mutation credentials are made available to
the orchestrator or remote MCP before the provider sandbox test.

## 3. Capture a deployment baseline

Create `docs/operations/first-deploy.md` from
[`operations/first-deploy-template.md`](operations/first-deploy-template.md) and record,
without secrets:

- checked-out Git commit and whether the tree is clean;
- operating system and kernel;
- Docker/Compose or native PostgreSQL versions;
- exact container image digests;
- PostgreSQL major, locale/collation and pgvector version;
- active environment profile and instance ID;
- service/process status and restart policy;
- current database migration list and canonical schema revision;
- mounted volumes, artifact paths and backup paths;
- open listening sockets and firewall exposure;
- liveness/readiness results;
- known deviations from the repository deployment contract.

Suggested evidence commands:

```bash
git rev-parse HEAD
git status --short --branch
docker compose config
docker compose ps
docker compose images
make status
make verify
curl --fail --silent http://127.0.0.1:8080/health/live
curl --fail --silent http://127.0.0.1:8080/health/ready
```

The receipt must distinguish **observed** facts from intended configuration. A service
being present in Compose is not evidence that it survives a host reboot.

## 4. Verify PostgreSQL before adding workloads

### 4.1 Clean-database repeatability

On an isolated test database using the same PostgreSQL 18/pgvector image:

1. apply all migrations;
2. run `db verify`;
3. apply migrations again and prove no drift;
4. run repository validation and PostgreSQL integration scripts;
5. destroy and recreate the test database;
6. repeat the sequence.

Expected gates:

- every migration checksum matches the repository;
- the Region Talk pipeline is registered once and remains `paused`;
- after ADR-0015 implementation, stable Region Talk project/pipeline/project-pipeline scopes
  are registered once and resolve unambiguously;
- catalog-object and `hub.project_content` compatibility backfills are idempotent and do not
  create duplicate active relations;
- no application service needs schema-owner privileges;
- an invalid or modified historical migration fails closed.

### 4.2 Split database roles

The initial single Compose role is a bootstrap convenience, not the final operator
model. Create and test separate roles for:

- migration/schema owner;
- application runtime;
- orchestrator/committer;
- connector intake;
- MCP data reader;
- MCP data editor;
- migration operator;
- backup/restore;
- monitoring.

No remote MCP role receives superuser, schema ownership, `CREATEDB`, `CREATEROLE`,
`BYPASSRLS`, extension installation or access to server files.

### 4.3 Service supervision

Prove all of the following:

- PostgreSQL starts before dependent services;
- API/orchestrator restart after process failure;
- the host reboot restores PostgreSQL, API and the disabled/plan-only orchestrator;
- only one active scheduler identity can claim due work;
- publication remains disabled after reboot and redeploy;
- PostgreSQL and internal application ports are not internet-facing.

## 5. Prove recovery before broad writes

Run a real backup and a real isolated restore. Merely producing a dump is insufficient.

Minimum evidence:

1. create a logical backup with manifest and SHA-256;
2. copy it to an off-host encrypted target;
3. read the uploaded bytes back and verify the hash;
4. restore into a fresh PostgreSQL instance/database;
5. run migration status, `db verify` and representative integrity queries;
6. record duration, versions, object counts and failures;
7. destroy the restore target after receipt archival.

Until this passes, remote MCP is read-only and connector batches may be accepted only
into a disposable/synthetic environment.

## 6. Establish automated workflow layers

The repository needs four distinct workflows rather than one oversized CI job:

| Layer | Trigger | Purpose |
|---|---|---|
| Pull request | every change | static, unit, contract, migration and ephemeral PostgreSQL gates |
| Devstand post-deploy | explicit deployment | liveness, readiness, revision, disabled gates and connector smoke |
| Nightly | schedule | backup freshness, queue health, remote MCP negative tests and provider inventory |
| Weekly provider canary | schedule/manual | disposable Kaggle and restore lifecycle tests |

The complete matrix and acceptance rules are in
[`19-test-first-rollout.md`](19-test-first-rollout.md).

## 7. First useful end-to-end flow

Before importing Region Talk, implement the ADR-0015 scope/policy backbone and one
synthetic connector with the same delivery mechanics intended for `events-bot-new`:

```text
fixture producer
→ durable local spool
→ HTTPS intake
→ one accepted immutable batch receipt
→ server-attested routing to at least two project/pipeline consumers
→ independent batch_application records
→ deterministic normalizers and canonical commits
→ explicit target relations plus append-only usage
→ independent per-consumer receipts/reconciliation
→ query relation, usage, state and effective policy through read-only MCP
→ exact replay without duplicate target relation or consumer application
```

This proves authentication, idempotency, outage retry, schema versioning, provenance,
consumer isolation, scope preservation, monitoring and MCP visibility without risking
existing accumulated data. A producer scope hint remains diagnostic; authoritative scope
comes from the server-side consumer registry.

After the synthetic flow passes, register `events-bot.daily-statistics.v1` and send a
non-sensitive daily aggregate as the first real producer. The connector architecture is
specified in [`16-data-connectors.md`](16-data-connectors.md), and the scope contract is
[`22-data-scope-and-pipeline-participation.md`](22-data-scope-and-pipeline-participation.md).

## 8. Remote MCP order

Publish `https://mcp-datahub.kenigevents.ru/mcp` in stages:

1. liveness at the edge;
2. OAuth metadata/resource validation;
3. semantic read-only tools;
4. Kaggle inventory read-only;
5. data-reader profile;
6. MCP-managed Kaggle mutation tools;
7. data-editor profile in a disposable schema;
8. application-schema data-editor after backup and negative-test gates;
9. migration-operator tools;
10. no production publication tool until a separate acceptance decision.

A scope must not merely make a dangerous tool return an error. The process should omit
that tool from discovery when the profile is disabled.

## 9. When Region Talk may start

Region Talk inventory may begin after the platform can prove:

- clean and repeatable PostgreSQL migrations;
- successful backup/readback/restore;
- CI and devstand workflows are green;
- remote MCP read-only access works;
- the ADR-0015 schema/backfill is applied and stable Region Talk scopes resolve exactly once;
- independent scope relations, namespaced state and platform hard-deny precedence pass
  negative tests;
- connector idempotency, offline retry and multi-consumer isolation work;
- Kaggle protected-resource boundaries work;
- operator writes are auditable and bounded;
- the pipeline remains paused and production publication is disabled.

The read-only export must attest the Region Talk origin scope in a new immutable contract
version. It may then proceed in parallel with porting row-kind transformers. Real target
normalization remains blocked until raw batch scope, target-relation writes and
scope-completeness reconciliation are active. Cutover remains blocked by full accounting,
quarantine, shadow, policy, backup and rollback gates.

## 10. Infrastructure-first code-agent assignment

The executable handoff covers the staged pre-migration releases R1–R4. It must not perform
full Region Talk migration or cutover. Its combined evidence must include:

- deployment receipt;
- role/grant matrix and negative tests;
- backup plus isolated restore receipt;
- updated GitHub Actions workflows;
- remote read-only MCP at the accepted hostname;
- ADR-0015 append-only migration/backfill plus relation/state/usage/policy negative tests;
- synthetic multi-consumer connector round trip and outage/replay evidence;
- Kaggle read inventory plus protected-resource authorization tests;
- exact remaining blockers before enabling mutation profiles or Region Talk normalization.

The recommended model level for this task is **high reasoning**. Use **xhigh** for the
review pass over authorization, database grants, backup/restore, scope/policy invariants,
deduplication relation preservation and negative provider controls, not for routine Compose
or CI wiring.
