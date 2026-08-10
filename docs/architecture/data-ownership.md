# Data ownership and write paths

## Canonical ownership

PostgreSQL is the only canonical relational state. Shared entities are not copied into a
project-specific schema merely because a pipeline uses them.

- `hub.catalog_object` (target under ADR-0015) registers shareable root identity;
  `hub.actor`, `external_account`, `content_item` and `content_asset` own typed data.
- `hub.relation_definition`, `hub.object_scope_relation` and append-only relation events
  own generic project/pipeline relation. Existing `hub.project_content` is a
  content-specific compatibility/domain extension, not the universal membership source.
- `hub.object_scope_state` owns reusable namespaced scoped state; each namespace has one
  declared writer and append-only state history.
- `hub.policy_decision` owns global/project/pipeline authorization decisions and their
  evidence; immutable policy evaluations record the exact decision set used by an action.
- `analysis.result` owns immutable model evidence.
- `orchestration.work_item` owns current execution state; `work_item_event` owns its
  history. `orchestration.object_usage_event` owns facts of object participation.
- `region_talk.*` owns rich Region Talk-specific projections; it does not own platform-wide
  policy or generic object membership.
- `migration.*` owns source preservation and reconciliation evidence only.
- future `integration.*` owns connector/provider intake, consumer routing, per-consumer
  application receipts and quarantine.
- operator audit/preview/apply evidence is append-only and not owned by the editor role.

## Permitted writers

| Writer | Allowed path |
|---|---|
| Local application service | repository/UoW SQL transaction plus outbox |
| Remote semantic MCP | typed command with scope, idempotency and expected revision |
| Remote data reader | read-only transaction under restricted role |
| Remote data editor | preview/apply DML under allowlisted restricted role and impact gates |
| Migration operator | typed landing/mapping/reconciliation/cutover; target + scope relation + disposition + provenance in one transaction |
| Data connector | HTTPS batch intake or own integration landing objects only; no authoritative scope assignment |
| Kaggle notebook | immutable result artifact only |
| Orchestrator | validated canonical result application, scoped state/usage and work transitions |
| YDB migration | read-only export followed by importer transaction |
| Joplin bridge | semantic note-link/content command; no Joplin DB mutation |

## Prohibited writes

- notebooks directly changing canonical PostgreSQL tables;
- connectors writing shared canonical domain tables or self-assigning project/platform scope;
- default semantic MCP executing generic SQL;
- remote MCP receiving database owner/superuser, DDL, role or extension rights;
- generic editor changing protected migration accounting/cutover, append-only audit,
  backup manifests, provider protection or publication receipts;
- source-specific services bypassing outbox for external side effects;
- inferring membership/policy from work status, schema name, latest run or free-form metadata;
- letting a local allow override an applicable platform-wide hard deny;
- mutating one pipeline's scoped state when another pipeline processes the same object;
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
UUID to a canonical UUID, but the alias is retained permanently. Dedupe also preserves
the set union of project/pipeline relations, state histories, applicable policy decisions and
provenance; a winning UUID does not imply a winning project. Conflicting current states in
the same namespace/scope require an explicit merge rule or a blocking conflict; they are
never resolved by last-write-wins. Closing one scope relation does not delete the shared
object or other scopes; retirement/erasure follows a separate lifecycle/retention policy.

Connector batches, provider operations and exchange packages also have stable internal
UUIDs plus provider/producer idempotency identities. Exact replay with a different hash
is a conflict, not a second valid version.

## Derived data

FTS, embeddings, scores and aggregate metrics are derived from versioned inputs. They
are never merged by “last result wins”. A result identity includes input fingerprint,
model/policy identifier and version. Recalculation creates a new immutable result and
moves the current projection explicitly. Scope-sensitive results include exact
project/pipeline evaluation scope in identity; scope-neutral results may be reused without
copying.

Periodic statistics corrections use append-and-supersede rather than rewriting accepted
source evidence.
