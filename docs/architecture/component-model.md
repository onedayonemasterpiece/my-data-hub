# Component model

## 1. Canonical PostgreSQL

Owns normalized identities, project membership, content, provenance, analysis metadata,
work state, Region Talk state, MCP command receipts, outbox, audit and Joplin links.
Schemas are separated by responsibility, not by deployment:

| Schema | Responsibility |
|---|---|
| `hub` | shared projects, actors, accounts, content, identities, relations and provenance |
| `analysis` | model registry, immutable analysis results and vector projections |
| `orchestration` | pipelines, stages, work items, runs, leases, artifacts and transitions |
| `sync` | sessions, commands/changesets, operations, receipts, conflicts and outbox |
| `region_talk` | product-specific source, gate, review and publication projections |
| `migration` | raw YDB import, mappings, exceptions and reconciliation evidence |
| `joplin` | note/notebook links, sync cursors and conflict records |

## 2. Orchestrator

The orchestrator is a policy engine and canonical result committer. It:

- selects bounded work according to priorities, leases and backpressure;
- creates exact input manifests;
- launches or observes local/Kaggle workers;
- validates schema, hashes, run identity and expected input revision;
- records result acceptance, rejection or quarantine atomically;
- emits transactional outbox events for external effects;
- measures the product funnel and queue health.

It does not embed all worker implementation in a single process.

## 3. Notebook workers

Workers are stateless with respect to canonical data. They may cache models within a run,
but their durable output is only a signed/hash-addressed result envelope. Separate heavy
models remain separate workers (for example E5, BGE-M3 and image diagnostics).

## 4. MCP server

MCP exposes bounded search/resources and semantic commands. The transport adapter is not
the domain service: tool handlers call application services that are also testable from
CLI and orchestration code.

## 5. Joplin bridge

A Windows-local bridge or plugin uses the supported Joplin Data/Plugin API, converts
selected note changes into semantic commands and stores stable cross-system links. Android
participates through normal Joplin synchronization; no service writes Joplin's SQLite file.

## 6. Artifact and backup storage

Private object-like storage (initially private Kaggle Datasets where practical) stores:

- immutable notebook result bundles;
- large run evidence;
- encrypted PostgreSQL backups/checkpoints;
- manifests and hashes.

An artifact is not canonical merely because its version is numerically latest. PostgreSQL
receipts and verified manifests establish acceptance.
