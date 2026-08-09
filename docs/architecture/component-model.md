# Component model

## 1. Canonical PostgreSQL

Owns normalized identities, project membership, content, provenance, analysis metadata,
work state, Region Talk state, connector/provider registry, MCP receipts, outbox, audit
and Joplin links. Schemas are separated by responsibility, not deployment:

| Schema | Responsibility |
|---|---|
| `hub` | shared projects, actors, accounts, content, identities, relations and provenance |
| `analysis` | model registry, immutable results and vector projections |
| `orchestration` | pipelines, stages, work, runs, leases, artifacts and transitions |
| `sync` | sessions, commands/changesets, receipts, conflicts and outbox |
| `region_talk` | product-specific source, gate, review and publication projections |
| `migration` | raw YDB import, mappings, exceptions and reconciliation evidence |
| `joplin` | note/notebook links, sync cursors and conflicts |
| future `integration` | connector batches, provider resources/operations and receipts |

PostgreSQL is supervised on the devstand. Kaggle and artifacts do not provide an
alternate canonical head.

## 2. Orchestrator / canonical committer

The orchestrator:

- selects bounded work according to priorities, leases and backpressure;
- executes pull connectors and normalizer/committer stages;
- creates exact notebook input manifests;
- launches/observes protected local/Kaggle workers;
- validates schema, hashes, run identity and expected input revision;
- records result acceptance, rejection or quarantine atomically;
- emits transactional outbox events for external effects;
- measures product, connector and queue health.

It does not embed every worker implementation and cannot wake its own stopped host.

## 3. Connector intake

Receives authenticated versioned batches, verifies idempotency/schema/hash/size, records
a durable acceptance receipt and stages normalization. It does not run expensive model
work synchronously and does not let producers write shared canonical tables.

## 4. Notebook workers

Workers are stateless with respect to canonical data. They may cache models within a run,
but durable output is a strict hash-addressed result envelope. Separate heavy models
remain separate workers.

## 5. MCP server profiles

- semantic product/orchestration tools;
- broad bounded data reader;
- preview/apply data editor;
- typed migration operator;
- Kaggle control-class-aware operator.

The remote endpoint is HTTPS/OAuth. PostgreSQL roles and provider registry remain the
primary target authorization boundaries.

## 6. Kaggle provider adapter

Reconciles account resources into the local registry and executes only operations allowed
by control class. Orchestrator-protected resources are status-only through remote MCP;
MCP-managed resources support tested provider lifecycle; exchange datasets are private,
TTL-bound and hash-manifested.

## 7. Joplin bridge

A Windows-local bridge/plugin uses the supported Joplin Data/Plugin API, converts selected
note changes into semantic commands/connector batches and stores stable links. Android
participates through normal Joplin synchronization; no service writes Joplin SQLite.

## 8. Artifact and backup storage

Private storage, initially including private Kaggle Datasets where practical, stores:

- immutable notebook result bundles;
- large run evidence;
- encrypted PostgreSQL backups/checkpoints;
- private exchange packages;
- manifests and hashes.

An artifact is not canonical because its version is latest. PostgreSQL receipts and
verified manifests establish acceptance.
