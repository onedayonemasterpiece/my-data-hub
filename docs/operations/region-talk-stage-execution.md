# Region Talk post-import stage execution

## Implemented boundary

The private Region Talk Notebook now has a typed post-import supervisor contract. After an
accepted canonical snapshot, the integration caller must invoke
`RegionTalkPostImportSupervisor.execute_after_import(task_run_id, export_batch_id)` before it
can report terminal success. The supervisor can use only the fixed function
`migration.execute_region_talk_post_import_stages(uuid, uuid, jsonb)`; it has no generic SQL or
table-write interface.

The fixed DAG is deterministic:

1. `canonical_import`
2. parallel `e5_embedding` and `bge_m3_embedding`
3. `vector_fusion`
4. `image_scoring`
5. `final_verifier`
6. `writer`
7. `review_queue`

Each stage declares its contract version, dependencies, maximum attempts, and timeout. Every
attempt has a typed status (`SUCCEEDED`, `WAITING_WORK`, `SKIPPED_BLOCKED`,
`FAILED_RETRYABLE`, or `FAILED_TERMINAL`) and immutable input/output/receipt fingerprints.
Stage-run and work-item identities are UUIDv5 values over the exact task, snapshot, candidate
revision, stage, and input fingerprint.

## Queue policy

- A migrated legacy candidate whose canonical status records prior selection may enter the
  operator review queue with basis `LEGACY_SELECTED`. This is preservation of imported review
  intent, not a new model verdict.
- A future candidate may enter with basis `CURRENT_EVIDENCE` only when every E5, BGE-M3,
  fusion, image, final-verifier, and writer input is exact and current.
- Missing, stale, or retryable evidence creates a typed bounded work request. Dependencies keep
  downstream stages blocked; retry exhaustion becomes terminal rather than cycling forever.
- Publication and notification dispatch are false in requests, work items, queue rows, and
  receipts. No publication attempt is created.

## Heavy-worker state

The required E5, BGE-M3, image, final-verifier, and writer notebooks no longer contain ambiguous
`NotImplementedError` shells. They validate the exact stage work-item contract and return
`HEAVY_RUNTIME_NOT_ATTACHED` until a verified heavyweight implementation is installed. E5 and
BGE-M3 identify the existing repository pins:

- `intfloat/multilingual-e5-base@d128750597153bb5987e10b1c3493a34e5a4502a`
- `BAAI/bge-m3@5617a9f61b028005a4858fdac845db406aefb181`

This contract-ready state is intentionally still `production_ready=false`.

## Residual blockers

1. No real E5 semantic-bank, BGE-M3 semantic-bank, image diagnostic, final verifier, or writer
   execution receipt exists for this Region Talk stage path. Their queued work is not PASS.
2. The image/final-verifier/writer donor model revisions and shadow-equivalence evidence remain
   unavailable, so those notebooks report an explicit retryable unavailable result.
3. Migration 0026 conservatively reports heavyweight evidence as missing. Exact evidence import
   and stale/current reconciliation must be implemented before a future candidate can use
   `CURRENT_EVIDENCE` in production.
4. The parent integration must place the supervisor call immediately after
   `DirectSnapshotRunner.run()` and require its typed receipt before terminal success. This lane
   intentionally does not edit the shared direct-pipeline integration point.
5. No live YDB/PostgreSQL/Kaggle run was performed by this lane; row counts, queue counts, and
   operational readiness remain unclaimed.
