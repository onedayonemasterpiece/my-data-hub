# Data ownership and write paths

## Canonical ownership

PostgreSQL is the only canonical relational state. Shared entities are not copied into a
project-specific schema merely because a pipeline uses them.

- `hub.content_item` owns the publication/post/video identity and compact content record.
- `hub.project_content` expresses membership in Region Talk or another project.
- `analysis.result` owns immutable model evidence.
- `orchestration.work_item` owns current work state; `work_item_event` owns history.
- `region_talk.*` owns Region Talk-specific policy decisions and operational projections.
- `migration.*` owns source-preservation and reconciliation evidence only.

## Permitted writers

| Writer | Allowed path |
|---|---|
| Local application service | SQL transaction through repository/UoW plus outbox |
| Remote MCP client | semantic command with scope, idempotency key and expected revision |
| Kaggle notebook | immutable result artifact only |
| Orchestrator | validated canonical result application and work transitions |
| YDB migration | read-only export followed by importer transaction |
| Joplin bridge | semantic note-link/content command; no direct SQL or Joplin DB mutation |

## Prohibited writes

- worker notebooks directly changing canonical PostgreSQL tables;
- arbitrary `UPDATE` or `DELETE` through MCP;
- source-specific services bypassing outbox for external side effects;
- using GitHub JSON, Kaggle latest or a Joplin note as a competing source of truth;
- writing back to YDB after migration freeze.

## Identity

New internal objects use globally unique UUIDs generated before insertion. External
identities use typed unique keys: canonical URL, platform/external ID, DOI, Telegram entity
ID or other namespace-qualified identifiers. Deduplication may map a provisional UUID to a
canonical UUID, but the alias is retained permanently.

## Derived data

FTS, embeddings, scores and aggregate metrics are derived from versioned inputs. They are
never merged by “last result wins”. A result identity includes at least input fingerprint,
model/policy identifier and version. Recalculation creates a new immutable result and moves
the current projection explicitly.
