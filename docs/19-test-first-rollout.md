# Test-first infrastructure and workflow rollout

Status: `R1 TEST AUTOMATION IMPLEMENTED / SAME-HOST AND ADR-0015 RUNTIME PROOF PENDING`
Date: 2026-08-10
Related decisions: ADR-0014, ADR-0015

## 1. Principle

The first production-quality result is not “Region Talk data was copied”. It is:

> the platform can prove that a bounded input is accepted once, independently applied to
> its declared project/pipeline consumers, queried safely, backed up, restored and protected
> against unauthorized provider/database mutation or cross-scope state/policy corruption.

Migration starts only after those reusable platform paths and ADR-0015 scope invariants are
independently green.

The R1 repository and disposable PostgreSQL automation exists. The ADR-0015
scope/participation cases added below are target tests and are not claimed as implemented;
same-host, off-host recovery and provider receipts remain runtime gates.

## 2. Test environments

| Environment | Purpose | Data policy | Destructive operations |
|---|---|---|---|
| local unit/contract | pure models, parsers, policies | fixtures only | yes, disposable |
| CI PostgreSQL | migrations/repositories/integration | synthetic fixtures | yes, disposable |
| devstand sandbox | deployed services and auth | synthetic + approved non-sensitive canary | only in sandbox schemas/resources |
| Kaggle canary | provider lifecycle | private disposable resources | yes, MCP-managed only |
| devstand canonical | real platform state | canonical data | gated operator/migration tools only |
| restore target | recovery proof | encrypted backup copy | isolated, destroyed after receipt |

No test may use an orchestrator-protected production notebook or backup dataset as a
disposable fixture.

## 3. Pull-request workflow

Run on every pull request and protected-branch change.

### 3.1 Static and repository gates

- repository structural validator;
- Ruff/lint and Python compilation;
- dependency lock/advisory checks where available;
- JSON Schema validation and example validation;
- deterministic notebook generation drift;
- Markdown link and ADR reference checks;
- secret scanning;
- Compose and workflow syntax validation.

### 3.2 Unit and contract gates

- domain and policy unit tests;
- scope-registry CHECK/unique/FK and idempotent compatibility-backfill tests;
- separation tests for relation vs usage vs scoped state vs policy decision;
- namespaced-state writer and normalized-class mapping tests;
- effective-policy tests proving platform hard deny overrides local/project/pipeline allow;
- pending-side-effect tests proving a new deny or traversed relationship revision invalidates
  a prior allow receipt before provider dispatch;
- connector envelope/idempotency and multi-consumer routing/application tests;
- MCP tool discovery by profile/scope;
- SQL parser/allowlist/impact classification tests;
- Kaggle control-class policy tests;
- artifact, traversal, hash and size tests;
- migration accounting/cutover policy tests.

### 3.3 Ephemeral PostgreSQL gates

Against the pinned PostgreSQL 18 + pgvector image:

1. apply migrations on an empty database;
2. run bootstrap verification;
3. apply migrations again and prove idempotency;
4. run repository integration tests;
5. run scope/catalog-object/project-content backfill and upgrade-path verification;
6. prove one canonical object has two project-pipeline relations and different namespaced
   states without duplication or cross-overwrite;
7. prove usage does not imply membership, workflow approval does not imply policy allow and
   platform hard deny cannot be weakened by a narrower allow;
8. prove a new applicable deny invalidates a pending allow receipt before provider dispatch;
9. run Region Talk synthetic landing/replay/quarantine/reconciliation with mandatory batch
   scope and target-relation completeness;
10. run connector accept/replay/conflict/correction with one batch and at least two
    independent consumer applications;
11. create role matrix and execute positive/negative grant probes;
12. run operator preview/apply in a disposable schema and prove protected objects remain
    immutable through the generic editor;
13. destroy the database.

Migration changes must also be tested from the previous released schema revision, not
only from empty state.

## 4. Devstand post-deploy workflow

Triggered explicitly after deployment and before marking a commit active.

Checks:

- deployed commit and image digests match the release input;
- PostgreSQL migration revision matches repository head;
- API liveness and readiness;
- orchestrator is alive but scheduler/publication gates have expected values;
- Region Talk remains paused unless the release explicitly changes it;
- remote MCP endpoint presents the expected certificate and OAuth resource;
- read-only MCP can execute health and one bounded query;
- write/operator/Kaggle mutation tools are absent unless that release enables them;
- stable logical pipeline and required scope identities resolve exactly once;
- synthetic connector batch is accepted once and reaches at least two independently tracked
  consumer applications/scopes;
- MCP can explain relation, usage, exact/normalized state and effective policy separately;
- exact replay returns the original batch and per-consumer receipts without duplicate active
  relations;
- service restart and one controlled process failure recover automatically;
- no internal port is internet-facing;
- a deployment receipt is archived.

A failed post-deploy job triggers rollback to the previous image/commit; it must not
continue into migration or provider canaries.

## 5. Nightly workflow

Run against the devstand with non-destructive scopes.

### Database and orchestrator

- readiness, schema revision and connection budget;
- work queue age, expired leases and retry/dead-letter counts;
- publication and protected pipeline gate state;
- slow/blocked transaction indicators;
- disk/volume headroom;
- latest canonical revision and commit activity sanity;
- ambiguous/missing required scope and cross-scope state-writer anomaly counts;
- active policy decisions with invalid/superseded references or missing evaluation receipts.

### Backup

- latest local and off-host generation age;
- manifest/hash/readback verification;
- encryption and retention state;
- last successful restore-drill age;
- alert if the operator write gate would currently be closed.

### Connectors

- one synthetic batch, at least two consumer applications and exact replay;
- expected daily connector lateness/spool health;
- accepted-to-committed lag per required consumer, not only per batch;
- missing/duplicate target-scope relation and unmatched consumer-application counts;
- conflict/quarantine/schema-version/routing anomalies.

### Remote MCP security

- no-auth, wrong audience, wrong origin/host and expired-token rejection;
- scope/tool discovery matrix;
- response/row/body/time limits;
- protected table/resource mutation denial;
- audit/correlation receipt presence.

### Kaggle inventory

- read-only list/status using bounded pagination;
- control-class reconciliation;
- protected resource remains protected;
- no mutation in the nightly read-only job.

## 6. Weekly provider canary

Use a separate canary principal and uniquely tagged private resources.

### Kaggle dataset canary

1. create a private `mcp_managed` dataset with a small fixture;
2. verify provider privacy and file hashes by readback;
3. create a second version;
4. download and verify exact content;
5. delete only the canary resource;
6. prove a protected dataset rejects the same version/download/delete tools.

### Kaggle notebook canary

1. create/push a small private MCP-managed notebook;
2. attach only the canary dataset;
3. run and observe status transitions;
4. retrieve and hash output;
5. reconcile operation receipts;
6. delete the canary notebook;
7. prove protected notebook source/output/run/delete are denied.

If the provider does not expose a reliable cancellation primitive, do not test or
advertise cancellation. Timeouts end local polling and mark reconciliation required;
they do not assert that provider execution stopped.

### Restore canary

At least weekly, restore the newest accepted backup into an isolated PostgreSQL target
and run:

- migration/status verification;
- extension/version verification;
- core object counts;
- referential/invariant queries, including scope registry, relation, usage, state and policy;
- compatibility-backfill and Region Talk scope-completeness counters;
- representative FTS/vector setup checks;
- outbox/receipt consistency checks;
- synthetic read-only MCP query against the restored target.

The restore target is never promoted automatically.

## 7. Manual release/cutover workflows

### 7.1 Enable remote MCP read-only

Required evidence:

- DNS and TLS active;
- OAuth resource/audience validated;
- Host/Origin and negative auth tests green;
- operator/mutation tools absent;
- audit logs and revocation tested.

### 7.2 Enable Kaggle mutation profile

Required evidence:

- weekly canary green;
- separate/approved provider credential;
- registry/control classes active;
- protected-resource negative tests green;
- public dataset creation impossible;
- ambiguous-outcome reconciliation tested.

### 7.3 Enable database reader/editor

Reader requires grant and exfiltration/timeout negative tests. Editor additionally
requires:

- preview/apply receipts;
- database-layer grants;
- recent backup and restore evidence;
- disposable-schema positive tests;
- protected-object negative tests;
- rollback/idempotency tests;
- explicit allowlist of application targets.

### 7.4 Region Talk migration

Only after all prior gates:

- inventory/export is read-only and the export receipt attests stable Region Talk scopes;
- raw landing is reversible, fully accounted and inherits Region Talk origin through batch;
- mapping is partitioned/idempotent and normalized/deduplicated targets receive the required
  Region Talk relation in the same canonical transaction;
- deduplication into a pre-existing shared object preserves Region Talk membership/reference
  without falsely assigning `originated_in`;
- scope-completeness, identity and row-accounting blockers are all zero;
- quarantine remains visible;
- agent tools cannot falsify reconciliation, policy evaluation or scope relations;
- shadow/canary/backup/rollback gates are enforced;
- publication remains separately disabled and fails closed on missing policy evidence.

## 8. Suggested GitHub Actions layout

```text
.github/workflows/
  ci.yml                       # PR static/unit/contracts/ephemeral PostgreSQL
  devstand-deploy.yml          # protected manual deploy + post-deploy smoke
  devstand-nightly.yml         # non-destructive runtime checks
  kaggle-canary.yml            # weekly/manual disposable provider lifecycle
  restore-drill.yml            # weekly/manual isolated restore
  region-talk-migration.yml    # manual protected environment, later
```

Use protected GitHub environments for devstand mutation, Kaggle canary and migration.
A schedule can start a workflow, but production mutation still requires the appropriate
environment, credential and runtime gate.

## 9. Evidence format

Every non-unit workflow emits a machine-readable receipt containing:

```text
contract version
workflow/run ID
trigger and actor
repository commit
container/dependency versions
instance/environment identity
started/finished timestamps
checks with expected/observed/outcome
affected synthetic resource IDs
batch and per-consumer application IDs
project/pipeline scope IDs and policy-evaluation refs where applicable
artifact hashes
cleanup outcome
remaining blockers
```

Receipts contain no tokens, connection strings, raw personal data or plaintext backups.
A green GitHub status without the receipt is insufficient for a release or cutover gate.

## 10. Incident classification

Keep failures separate:

- `CODE_CONTRACT` — tests/schema/parser/policy;
- `DB_MIGRATION` — migration/repository/grant/invariant;
- `DATA_SCOPE_POLICY` — missing/ambiguous scope, relation loss, state overwrite or policy
  precedence/evaluation failure;
- `DEPLOYMENT` — image/service/restart/routing;
- `AUTHORIZATION` — OAuth/scope/role/control-class;
- `CONNECTOR_DELIVERY` — spool/intake/replay/normalization;
- `BACKUP_RECOVERY` — dump/readback/restore;
- `PROVIDER_KAGGLE` — API/CLI/resource lifecycle;
- `REGION_TALK_MAPPING` — source/mapping/reconciliation;
- `PUBLICATION` — external side effects.

A provider outage must not be reported as Region Talk data loss, and a mapping mismatch
must not be hidden by rerunning an infrastructure workflow.

## 11. Initial acceptance checklist

The platform is ready for Region Talk inventory when all are true:

- [ ] deployed commit and runtime evidence are recorded;
- [ ] PostgreSQL migrations pass twice on empty and upgrade-path databases;
- [ ] database roles and negative grants are proven;
- [ ] local plus off-host backup is hash-verified;
- [ ] isolated restore succeeds;
- [ ] CI, post-deploy and nightly workflows are green;
- [ ] remote read-only MCP works through OAuth/TLS;
- [ ] ADR-0015 migrations/backfill are idempotent and stable Region Talk scopes resolve once;
- [ ] relation/usage/state/policy separation and platform hard-deny precedence are proven;
- [ ] pending publication cannot use an allow receipt invalidated by newer policy/relations;
- [ ] synthetic connector survives replay/outage and isolates at least two consumer applications;
- [ ] Kaggle inventory classifies every resource;
- [ ] protected Kaggle resources reject mutation;
- [ ] one disposable MCP-managed notebook and dataset lifecycle passes;
- [ ] operator reader/editor pass in a disposable schema;
- [ ] Region Talk fixture has zero raw-without-batch-scope and zero normalized/deduplicated
  target-without-relation counters;
- [ ] scheduler/publication remain disabled and Region Talk paused.
