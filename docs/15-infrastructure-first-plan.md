# Infrastructure-first plan after the first devstand deployment

Status: `ACCEPTED IMPLEMENTATION PLAN`
Date: 2026-08-09
Related decisions: ADR-0009, ADR-0013, ADR-0014

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
→ prove one synthetic data connector
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

Before importing Region Talk, implement one synthetic connector with the same delivery
mechanics intended for `events-bot-new`:

```text
fixture producer
→ durable local spool
→ HTTPS intake
→ connector batch receipt
→ validation and staging
→ deterministic normalizer
→ canonical commit
→ query through read-only MCP
→ reconciliation receipt
```

This proves authentication, idempotency, outage retry, schema versioning, provenance,
monitoring and MCP visibility without risking existing accumulated data.

After the synthetic flow passes, register `events-bot.daily_statistics.v1` and send a
non-sensitive daily aggregate as the first real producer. The connector architecture is
specified in [`16-data-connectors.md`](16-data-connectors.md).

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
- connector idempotency and offline retry work;
- Kaggle protected-resource boundaries work;
- operator writes are auditable and bounded;
- the pipeline remains paused and production publication is disabled.

The inventory/export step is read-only and may then proceed in parallel with porting
row-kind transformers. Cutover remains blocked by the existing full-accounting,
quarantine, shadow and rollback gates.

## 10. First code-agent assignment

The next code-agent task should implement and prove **R1 Infrastructure and Workflow**, not
perform the full Region Talk migration. Its output must include:

- deployment receipt;
- role/grant matrix and negative tests;
- backup plus isolated restore receipt;
- updated GitHub Actions workflows;
- remote read-only MCP at the accepted hostname;
- synthetic connector round trip and outage/replay evidence;
- Kaggle read inventory plus protected-resource authorization tests;
- exact remaining blockers before enabling mutation profiles.

The recommended model level for this task is **high reasoning**. Use **xhigh** only for a
review pass over authorization, database grants, backup/restore and negative provider
controls, not for routine Compose or CI wiring.
