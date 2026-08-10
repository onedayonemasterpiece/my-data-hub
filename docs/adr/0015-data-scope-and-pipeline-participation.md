# ADR-0015: Project/pipeline scope, participation and policy are first-class data

- Status: Accepted
- Date: 2026-08-10

## Context

The bootstrap shares authors, accounts, content and assets across workloads, but the only
explicit project-membership relation is `hub.project_content`. Other reusable objects can be
used by several projects or pipelines without a universal relation that records that fact.
Operational `orchestration.work_item.status`, workload-specific statuses and global business
policy are also easy to confuse.

This is insufficient for a multi-pipeline data hub. The same actor, account, content item or
asset may:

- belong to more than one project;
- be observed or processed by more than one pipeline;
- have different workflow states in different project/pipeline contexts;
- be subject to a platform-wide policy such as a publication deny/blacklist;
- be deduplicated to an existing canonical object while retaining every project's relation
  and source lineage.

Region Talk makes the gap blocking: every YDB row must be attributable to the Region Talk
migration, and every normalized or deduplicated shared target must retain an explicit Region
Talk association.

## Decision

1. Introduce stable logical pipeline identity separately from immutable/versioned pipeline
   definitions. A pipeline may be associated with several projects.
2. Introduce a first-class scope registry with four contexts:
   `platform`, `project`, `pipeline` and `project_pipeline`.
3. Register shareable canonical roots as catalog objects and relate them to scopes instead of
   copying the object or adding ad-hoc project columns to every table.
4. Keep five concerns separate:
   - entity lifecycle;
   - persistent project/scope relation;
   - scoped workflow state;
   - append-only pipeline usage;
   - scoped policy decision and its effective evaluation.
5. Exact state remains namespaced. A small normalized state class supports cross-pipeline
   reporting but is not sufficient authorization for publication or other side effects.
6. Global/project/pipeline policy decisions are append-only, reasoned and versioned. The
   policy definition declares how applicable scopes combine. For publication eligibility,
   any applicable hard deny overrides a local allow; a narrower scope may tighten but cannot
   weaken a platform-wide blacklist. A side-effect receipt is reusable only while its exact
   object/relationship/decision input fingerprint remains current.
7. `orchestration.work_item.status` remains execution state only. It does not prove project
   membership, domain approval, publication eligibility or durable pipeline participation.
8. A connector batch is accepted once but may have several independently tracked consumers.
   Server-side routing creates one application record per project/pipeline consumer; producer
   hints are not authoritative scope assignments.
9. Region Talk migration uses stable Region Talk project and project-pipeline scopes:
   - every export batch declares Region Talk as its origin scope;
   - raw, excluded, retained and quarantined rows inherit that origin through the batch;
   - every `normalized` or `deduplicated` shared target receives the required Region Talk
     relation in the same canonical transaction as target/disposition/provenance writes;
   - deduplication never removes a project relation;
   - an intentionally excluded row keeps migration lineage but need not become an active
     project member.
10. Existing `hub.project_content` and workload status columns are compatibility/domain
    projections during migration. They are not the universal cross-project model and may not
    become competing sources of truth.

The detailed contract is in
[`../22-data-scope-and-pipeline-participation.md`](../22-data-scope-and-pipeline-participation.md).

## Consequences

- One canonical object can safely participate in many workloads without duplication.
- Pipeline-specific states no longer overwrite one another.
- Platform-wide approval/deny policy is explicit, explainable and auditable.
- Every decision and side effect can cite exact scope, state and policy revisions; a new
  applicable deny invalidates a previously pending allow receipt.
- Region Talk reconciliation gains scope-completeness gates in addition to row accounting and
  identity reconciliation.
- The implementation requires an append-only schema migration, backfill, compatibility
  views/adapters, repository changes and negative tests before the real YDB normalization.

## Rejected alternatives

- **One universal `status` column.** It conflates lifecycle, workflow and authorization and
  cannot represent independent states in several pipelines.
- **`project_id`/`pipeline_id` on every physical table.** It duplicates routing metadata,
  cannot express many-to-many use cleanly and creates drift between child rows and parents.
- **Infer scope from schema name, work items or provenance text.** Inference is ambiguous,
  especially after deduplication, retention or pipeline-version changes.
- **Copy an object per project/pipeline.** This breaks shared identity and causes conflicting
  edits, aliases and policy decisions.
- **Let a local allow override a global blacklist.** That makes platform governance unsafe and
  non-explainable.
