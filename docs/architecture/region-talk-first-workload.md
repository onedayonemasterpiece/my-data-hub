# Region Talk as the first my-data-hub workload

## Decision

Region Talk is not merely an example integration. It is the first production workload and
the migration that proves the architecture.

## What moves

The default is **complete migration**, not a curated subset:

- source queue/status/candidates/edges and onboarding profiles/evidence;
- discovered posts, canonical URLs, platform IDs and live/processed state;
- candidate memory and duplicate guards;
- E5/BGE/text enrichment evidence and model identities;
- image queue/results and media evidence;
- publication candidates, exact revision fingerprints and review/publication state;
- external-publication intake/source records;
- Telegram entity/link caches that remain operationally necessary;
- run/provenance evidence available in YDB;
- all other rows retained in raw staging until mapped or explicitly declared obsolete.

## What does not move as architecture

- YDB queues, leases and broad-read workarounds;
- SQLite as a new canonical store;
- Supabase as a second product-state database;
- mutable “latest Kaggle Dataset” semantics;
- direct worker writes.

## Behaviour preservation

The current Region Talk product semantics remain migration invariants:

- external/nonregional source distinction;
- exact URL and duplicate protection;
- source and post discovery as separate contours;
- dual-vector evidence and explicit fusion;
- text gate before expensive image work;
- media evidence preserved through invalidation/restoration;
- final verifier after image readiness;
- manual review bound to exact candidate revision;
- no production publication before approved canary and exact receipt.

## Shared versus workload-specific data

Authors, outlets, accounts, content, assets and analysis results live in shared schemas.
Region Talk-specific eligibility, queues, review and publication projections live in
`region_talk`. Another regional project can attach the same content without copying it.

## Data scope contract

Before real normalization, the database must contain stable identities for:

```text
project:region-talk
pipeline:region-talk.main
project-pipeline:region-talk:region-talk.main
```

Every YDB export batch declares Region Talk as origin project scope, and all raw rows
inherit it. Every `normalized` or `deduplicated` shared actor/account/content/asset target
gets an explicit Region Talk relation in the same transaction as disposition, target refs
and provenance. A pre-existing deduplicated object gains the relation without being falsely
marked `originated_in`.

`intentionally_excluded`, `retained_raw` and `quarantined` rows remain attributable to
Region Talk through batch lineage, but are not given fictitious active membership. Work
and usage records resolve the exact Region Talk project-pipeline scope. Scope relation,
workflow state, usage and publication policy remain independent.

See [`../22-data-scope-and-pipeline-participation.md`](../22-data-scope-and-pipeline-participation.md).

## Proof required before cutover

- every exported YDB row has a disposition and resolves Region Talk batch scope;
- every normalized/deduplicated shared target has the required Region Talk relation;
- source/content identities reconcile by natural keys, not only counts;
- duplicate groups show one canonical object plus union aliases, provenance and scopes;
- duplicate guard equivalence is demonstrated;
- representative current queue cohorts produce equivalent gate decisions;
- at least one full private canary reaches review and publication receipt;
- rollback restores the frozen YDB read path without losing post-freeze commands.
