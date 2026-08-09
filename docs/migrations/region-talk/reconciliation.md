# Migration reconciliation

## Required equations

For each export batch and row kind:

```text
expected = raw
raw = normalized + deduplicated + intentionally_excluded + retained_raw + quarantined
undispositioned = 0
conflicting_duplicate_source_pk = 0
unexplained_missing_target = 0
```

Two deliberately different predicates are reported:

```text
fully_accounted = (expected = raw) AND (undispositioned = 0)
cutover_ready   = fully_accounted AND (quarantined = 0)
```

A quarantined row is preserved and therefore counts as **accounted**, but it is not accepted
as successfully migrated. Any `quarantined > 0`, `undispositioned > 0` or raw/manifest count
mismatch creates a blocking finding and makes the reconciliation report fail. A row may leave
quarantine only through an explicit, versioned remapping or an owner-approved terminal
non-quarantine disposition with evidence. Quarantine cannot hide a failed parser or defer an
unknown row until after cutover.

The machine-readable contract is
[`schemas/migration-reconciliation-report.v1.schema.json`](../../../schemas/migration-reconciliation-report.v1.schema.json),
with a passing example in
[`examples/contracts/migration-reconciliation-report.v1.example.json`](../../../examples/contracts/migration-reconciliation-report.v1.example.json).

## Identity reconciliation

Count comparison alone is insufficient. Produce sorted/hashable sets for:

- canonical URLs and normalized URLs;
- Telegram/VK/external platform IDs;
- source fingerprints and aliases;
- DOI or publication external identity;
- post/source legacy PK -> canonical UUID mapping;
- publication candidate revision fingerprints.

For each duplicate group, record whether PostgreSQL intentionally merged source rows and
show all aliases.

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

## Referential integrity

- no Region Talk source row without a shared actor/account or explicit unresolved link;
- no post evaluation without a content item;
- no embedding without analysis metadata and matching dimensions;
- no candidate revision without content/project and gate evidence;
- no review decision without exact revision;
- no delivery receipt without outbox/publication record;
- no normalized target lacking a source staging/provenance link during migration.

## Reproducibility

The reconciliation bundle contains SQL query texts, database schema revision, code commit,
export batch IDs, result hashes and a machine-readable report. Re-running it on the same
snapshot must produce the same findings.
