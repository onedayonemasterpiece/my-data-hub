# Region Talk migration acceptance

The migration is accepted only when all mandatory conditions are evidenced.

## Data

- [ ] Live PostgreSQL fixture gate `scripts/verify_region_talk_migration_flow.py` passes.
- [ ] Complete source kind inventory exists.
- [ ] Export manifest/hash verification passes.
- [ ] Raw imported count equals exported count.
- [ ] Every row has a disposition.
- [ ] `quarantined = 0` for the baseline and final-delta batches.
- [ ] Every row-kind accounting row has `cutover_ready = true`.
- [ ] The machine-readable reconciliation report validates against its v1 schema and has `passed = true`.
- [ ] Natural-key identity reconciliation passes.
- [ ] Critical relationships and candidate fingerprints reconcile.
- [ ] Unknown/unmapped rows are zero or individually accepted with rationale.

## Pipeline

- [ ] Exact URL intake works.
- [ ] Source and post discovery remain separate and observable.
- [ ] E5 and BGE worker outputs are accepted idempotently.
- [ ] Fusion/text eligibility uses one versioned contract.
- [ ] Image evidence and restoration semantics are preserved.
- [ ] Final verifier and exact candidate revision work.
- [ ] Review decision binds the exact revision.
- [ ] Private publication canary has an exact receipt and retry proof.

## Platform

- [ ] MCP read/write scopes and limits tested.
- [ ] Workers cannot write canonical tables directly.
- [ ] Backup/restore drill passes.
- [ ] Devstand auto-start and health checks work.
- [ ] Publication kill switch works.
- [ ] Rollback package and post-cutover command export tested.

## Retirement

- [ ] Rollback window closed explicitly.
- [ ] YDB writer credentials removed.
- [ ] Frozen export and source manifest retained privately.
- [ ] Donor code/docs provenance recorded.
