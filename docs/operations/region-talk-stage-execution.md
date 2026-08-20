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

Migration 0028 splits the heavy-worker path so canonical content cannot enter the lightweight
control plane:

1. the private supervisor claims only a bounded metadata receipt through
   `claim_region_talk_stage_work_metadata`;
2. central creates the deterministic child task credential from that receipt and may journal
   only task/work/effect/dispatch identities, stage/input hashes, attempts, timeouts and provider
   references;
3. the supervisor binds the separately registered child credential through
   `bind_region_talk_stage_worker`;
4. `RegionTalkStageDispatcher` observes or launches the exact private stage Notebook through its
   one injected adapter, with deterministic worker, dispatch and effect UUIDs;
5. inside that private Notebook, `PostgresStageWorkerFunctions` calls
   `fetch_region_talk_stage_work_payload` using the child task credential, executes the transform,
   and lands the exact result through `submit_region_talk_stage_worker_result`; and
6. the supervisor re-runs PREPARE/COMMIT so exact-current landed evidence advances dependencies.

The central launch model and mode-0600 dispatch journal contain no payload, input data, content
text, raw lease, database URL or credential token. Receipt hashes and deterministic identities
are validated before the provider boundary. Response-loss restart observes the same dispatch
instead of launching a duplicate. Missing attached model/media/editorial runtime lands
`FAILED_RETRYABLE`, never `SUCCEEDED`. An expired lease remains `WAITING_WORK`; the database owns
the next bounded attempt. `EMPTY` and `WAITING_DEPENDENCY` are also nonterminal. Only an explicit
database `COMPLETE` receipt can complete stage execution; database `FAILED` becomes terminal
failure rather than success.

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
3. The metadata-only dispatcher and child credential command/registration handshake are
   implemented, but the concrete private Kaggle stage adapter, supervisor callbacks, and
   successive credential-generation rotation are not yet assembled into the central lifespan.
   Until those are complete the schedule must remain off and no heavy worker is production-ready.
4. Migration 0029 now supplies exact successive-generation binding/fencing because a child
   credential lasts at most four minutes while fixed stage timeouts range from five to twenty
   minutes. The remaining adapter must checkpoint, bind generation N+1, prove the replacement
   direct session, activate it, and only then revoke N; TTL relaxation or an expired-generation
   success is forbidden.
5. No live YDB/PostgreSQL/Kaggle run was performed here. The schedule remains off, and row counts,
   provider-run identities, checkpoint evidence, and operational readiness remain unclaimed.
