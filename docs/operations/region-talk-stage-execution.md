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

Migration 0027 adds fixed task/ACTIVE-epoch-bound claim, result landing, and status functions.
`RegionTalkStageDispatcher` is the separate lightweight controller for those functions. It:

1. atomically claims only a dependency-ready work item from the accepted snapshot;
2. verifies the database-derived UUIDv5 effect identity and fixed stage policy;
3. persists the exact claim and launch metadata before the provider effect;
4. observes before launching the stage's exact private Notebook through its one injected adapter;
5. validates the generic Notebook result envelope, subject/input/output hashes, model/runtime
   identity, and bounded result metadata;
6. lands the result through `migration.submit_region_talk_stage_result`; and
7. re-runs PREPARE/COMMIT so exact-current evidence advances the next dependency.

Response loss replays the byte-equivalent result submission. A restart observes the same effect
instead of launching a duplicate. An expired lease remains `WAITING_WORK`; a late result is not
submitted and the database owns the next bounded attempt. `EMPTY` and `WAITING_DEPENDENCY` are
also nonterminal. Only an explicit database `COMPLETE` receipt can complete stage execution.

The required E5, BGE-M3, vector-fusion, image, final-verifier, and writer notebooks contain no
ambiguous `NotImplementedError` shells. They validate the exact execution payload. Vector fusion
executes the repository's deterministic `fuse_vector_evidence` transform using two verified
upstream metadata receipts. Other stages execute only through an explicitly attached private
runtime with an exact producer identity; without one they return the retryable
`HEAVY_RUNTIME_NOT_ATTACHED` failure. E5 and BGE-M3 retain the repository pins:

- `intfloat/multilingual-e5-base@d128750597153bb5987e10b1c3493a34e5a4502a`
- `BAAI/bge-m3@5617a9f61b028005a4858fdac845db406aefb181`

All templates intentionally remain `production_ready=false`. Scheduling, publication, and
notification remain disabled until a real private run supplies the required receipts.

## Residual blockers

1. No real Region Talk semantic-bank E5/BGE, image diagnostic, final verifier, or writer result
   exists. Their queued work is not PASS. Without E5/BGE results the executable vector-fusion
   stage correctly remains dependency-blocked.
2. The semantic-bank runtime assets and the image/final-verifier/writer donor implementations,
   exact revisions, and shadow-equivalence receipts are not attached. Those workers therefore
   produce retryable failures rather than current evidence.
3. The parent integration must inject the production Kaggle stage adapter and call the dispatcher
   while a stage receipt is `WAITING_WORK`; it must not map that state to cycle `COMPLETE` or
   terminal `SUCCEEDED`.
4. No live YDB/PostgreSQL/Kaggle run was performed here. The schedule remains off, and row counts,
   provider-run identities, checkpoint evidence, and operational readiness remain unclaimed.
