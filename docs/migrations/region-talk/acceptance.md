# Region Talk migration acceptance

The migration is accepted only when all mandatory conditions are evidenced.

## Data

- [ ] Live PostgreSQL fixture gate `scripts/verify_region_talk_migration_flow.py` passes.
- [ ] Complete source kind inventory exists.
- [ ] Export manifest/hash verification passes.
- [ ] Raw imported count equals exported count.
- [ ] Every row has a disposition and resolves an attested Region Talk batch origin scope.
- [ ] `quarantined = 0` for the baseline and final-delta batches.
- [ ] Every row-kind accounting row has `cutover_ready = true`.
- [ ] The bootstrap fixture report validates against the retained v1 schema and has
  `passed = true`.
- [ ] The live versioned reconciliation report validates against a new immutable
  scope-aware schema and has `scope_complete = true`; v1-only accounting is insufficient.
- [ ] Every normalized/deduplicated shared target has the mapping-required Region Talk relation.
- [ ] Every Region Talk projection resolves a scoped shared root or an explicit unresolved record.
- [ ] Intentionally excluded/retained/quarantined rows preserve origin lineage without false membership.
- [ ] Natural-key identity reconciliation passes.
- [ ] Duplicate groups preserve one canonical object plus union aliases, provenance and scopes.
- [ ] Critical relationships and candidate fingerprints reconcile.
- [ ] Unknown/unmapped rows are zero or individually accepted with rationale.

## Pipeline

- [ ] Exact URL intake works.
- [ ] Source and post discovery remain separate and observable.
- [ ] Work/usage resolves the exact Region Talk logical pipeline and project-pipeline scope.
- [ ] Exact namespaced state is independent from work execution status and other pipelines.
- [ ] E5 and BGE worker outputs are accepted idempotently.
- [ ] Fusion/text eligibility uses one versioned contract.
- [ ] Effective publication policy records exact decision IDs, scope, policy version and
  current input fingerprint; stale pending allow is rejected before provider dispatch.
- [ ] Image evidence and restoration semantics are preserved.
- [ ] Final verifier and exact candidate revision work.
- [ ] Review decision binds the exact revision.
- [ ] Private publication canary has an exact receipt and retry proof.

## Platform

- [ ] MCP read/write scopes and limits tested.
- [ ] Workers cannot write canonical tables directly.
- [ ] Platform-wide hard deny/blacklist overrides Region Talk local allow in negative tests.
- [ ] Scope relation, usage, state and policy cannot be substituted for one another.
- [ ] Backup/restore drill passes.
- [ ] Devstand auto-start and health checks work.
- [ ] Publication kill switch works.
- [ ] Rollback package and post-cutover command export tested.

## Retirement

- [ ] Rollback window closed explicitly.
- [ ] YDB writer credentials removed.
- [ ] Frozen export and source manifest retained privately.
- [ ] Donor code/docs provenance recorded.
