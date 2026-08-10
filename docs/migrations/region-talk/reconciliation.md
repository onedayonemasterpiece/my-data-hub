# Migration reconciliation

## Required equations

For each export batch and row kind:

```text
expected = raw
raw = normalized + deduplicated + intentionally_excluded + retained_raw + quarantined
undispositioned = 0
conflicting_duplicate_source_pk = 0
unexplained_missing_target = 0
raw_without_region_talk_batch_scope = 0
normalized_target_without_region_talk_relation = 0
deduplicated_target_without_region_talk_relation = 0
region_talk_projection_without_scoped_shared_root = 0
scope_relation_without_raw_or_provenance_evidence_during_migration = 0
work_or_usage_with_ambiguous_project_pipeline_scope = 0
```

Two deliberately different predicates are reported:

```text
fully_accounted = (expected = raw) AND (undispositioned = 0)
scope_complete  = all six scope counters above are zero
cutover_ready   = fully_accounted AND (quarantined = 0) AND scope_complete
```

A quarantined row is preserved and therefore counts as **accounted**, but it is not accepted
as successfully migrated. Any `quarantined > 0`, `undispositioned > 0`, raw/manifest count mismatch or
scope-completeness failure creates a blocking finding and makes the reconciliation report
fail. A row may leave quarantine only through an explicit, versioned remapping or an
owner-approved terminal non-quarantine disposition with evidence. Quarantine cannot hide a
failed parser or defer an unknown row until after cutover.

The current machine-readable v1 contract remains at
[`schemas/migration-reconciliation-report.v1.schema.json`](../../../schemas/migration-reconciliation-report.v1.schema.json),
with a passing example in
[`examples/contracts/migration-reconciliation-report.v1.example.json`](../../../examples/contracts/migration-reconciliation-report.v1.example.json).
It proves the bootstrap accounting mechanism only. Before live migration, add a new
immutable schema version containing explicit scope counters/findings; do not add unknown
fields to v1 or call a v1-only report scope-complete.

## Identity reconciliation

Count comparison alone is insufficient. Produce sorted/hashable sets for:

- canonical URLs and normalized URLs;
- Telegram/VK/external platform IDs;
- source fingerprints and aliases;
- DOI or publication external identity;
- post/source legacy PK -> canonical UUID mapping;
- publication candidate revision fingerprints.

For each duplicate group, record whether PostgreSQL intentionally merged source rows and
show all aliases, provenance and the union of platform/project/pipeline relations. A
deduplicated pre-existing target must visibly gain Region Talk relation without being marked
as newly originated in Region Talk.

## Scope reconciliation

Prove from direct keys/FKs and reproducible queries, not schema-name inference:

- every export batch resolves `project:region-talk`;
- every raw row resolves the same origin through its batch;
- every normalized/deduplicated shared target has the mapping-required Region Talk relation;
- intentionally excluded/retained/quarantined rows retain origin lineage without fictitious
  membership;
- every Region Talk projection with a shared root resolves that scoped root;
- every migrated/replayed work or usage event has exact Region Talk project-pipeline scope;
- no pipeline-local state overwrites another scope;
- global hard deny/blacklist remains effective in Region Talk and cannot be weakened locally.

For each target reference, report relation kind, relation revision, provenance/raw row and
scoped-state namespace. Missing scope evidence is not an informational warning.

## Semantic reconciliation

For representative and all currently actionable rows, compare:

- source externality/geo/topic/status verdict;
- exact/manual post queue readiness;
- processed/live/terminal post state;
- E5/BGE presence, input hash and model/contract;
- fused text eligibility and reason/gate version;
- image readiness, per-image verdict and terminal reason;
- candidate readiness, exact text/URL/media fingerprint;
- review decision and publication receipt.

## Queue reconciliation

Create a breakdown by stage and status, including age percentiles and retry state. A single
aggregate “pending” count is not evidence because state taxonomies may map differently.
Report exact namespaced state and normalized class per project-pipeline scope; execution work
state is reported separately from domain/policy state.

## Referential integrity

- no Region Talk source row without a shared actor/account or explicit unresolved link;
- no post evaluation without a content item;
- no embedding without analysis metadata and matching dimensions;
- no candidate revision without content/project and gate evidence;
- no review decision without exact revision;
- no delivery receipt without outbox/publication record;
- no normalized target lacking a source staging/provenance link during migration;
- no normalized/deduplicated shared target lacking its Region Talk relation;
- no Region Talk projection whose shared root lacks that relation;
- no work/usage row with ambiguous logical pipeline or project-pipeline scope.

## Reproducibility

The reconciliation bundle contains SQL query texts, database schema revision, code commit,
export batch IDs, Region Talk scope IDs/keys, policy/scope contract versions, result hashes
and a machine-readable report. Re-running it on the same
snapshot must produce the same findings.
