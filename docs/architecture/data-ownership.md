# Data ownership and write paths

## Canonical ownership

PostgreSQL is the only canonical relational state. Shared entities are not copied into a
project-specific schema merely because a pipeline uses them.

- `hub.content_item` owns publication/post/video identity and compact content.
- `hub.project_content` expresses membership in Region Talk or another project.
- `analysis.result` owns immutable model evidence.
- `orchestration.work_item` owns current work state; `work_item_event` owns history.
- `region_talk.*` owns Region Talk-specific policy and operational projections.
- `migration.*` owns source preservation and reconciliation evidence only.
- future `integration.*` owns connector/provider intake, registry, receipts and quarantine.
- operator audit/preview/apply evidence is append-only and not owned by the editor role.

## Permitted writers

| Writer | Allowed path |
|---|---|
| Local application service | repository/UoW SQL transaction plus outbox |
| Remote semantic MCP | typed command with scope, idempotency and expected revision |
| Remote data reader | read-only transaction under restricted role |
| Remote data editor | preview/apply DML under allowlisted restricted role and impact gates |
| Migration operator | typed landing/mapping/reconciliation/cutover procedures |
| Data connector | HTTPS batch intake or own integration landing objects only |
| Kaggle notebook | immutable result artifact only |
| Orchestrator | validated canonical result application and work transitions |
| YDB migration | read-only export followed by importer transaction |
| Joplin bridge | semantic note-link/content command; no Joplin DB mutation |

## Prohibited writes

- notebooks directly changing canonical PostgreSQL tables;
- connectors writing shared canonical domain tables;
- default semantic MCP executing generic SQL;
- remote MCP receiving database owner/superuser, DDL, role or extension rights;
- generic editor changing protected migration accounting/cutover, append-only audit,
  backup manifests, provider protection or publication receipts;
- source-specific services bypassing outbox for external side effects;
- using GitHub JSON, Kaggle latest or a Joplin note as competing source of truth;
- writing back to YDB after migration freeze;
- treating a backup as authorization for an otherwise forbidden mutation.

## PostgreSQL role principle

Database grants are the primary enforcement boundary. Application-level parsing,
allowlists, scopes, preview receipts and limits are additional controls. New schemas and
tables receive no implicit remote editor rights. Structural changes remain local
break-glass administration.

## Identity

New internal objects use globally unique UUIDs generated before insertion. External
identities use typed unique keys: canonical URL, platform/external ID, DOI, Telegram
entity ID or other namespace-qualified identifiers. Deduplication may map a provisional
UUID to a canonical UUID, but the alias is retained permanently.

Connector batches, provider operations and exchange packages also have stable internal
UUIDs plus provider/producer idempotency identities. Exact replay with a different hash
is a conflict, not a second valid version.

## Derived data

FTS, embeddings, scores and aggregate metrics are derived from versioned inputs. They
are never merged by “last result wins”. A result identity includes input fingerprint,
model/policy identifier and version. Recalculation creates a new immutable result and
moves the current projection explicitly.

Periodic statistics corrections use append-and-supersede rather than rewriting accepted
source evidence.
