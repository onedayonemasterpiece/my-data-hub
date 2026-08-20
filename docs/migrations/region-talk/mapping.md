# Initial YDB-to-PostgreSQL mapping

## Strategy

Every source row is inserted first into raw staging. Normalization then runs as an
idempotent mapping release identified by `mapping_version`. A source row may produce several
normalized records. The staging row stores its disposition and links to target identities.

Shared identities are preferred over Region Talk copies. Region-specific status/evidence
references those shared identities. Every mapping declares separately:

- raw origin scope inherited from the Region Talk export batch;
- target relation (`member`, `managed`, `referenced` or none);
- exact project/pipeline scoped-state namespace, when applicable;
- usage event produced by migrated/replayed operational history, when evidence permits.

`normalized` and `deduplicated` shared targets receive their required Region Talk relation
in the same transaction as target refs, provenance and disposition. Deduplication to an
existing object adds the relation but does not falsely claim `originated_in`.

## Initial mapping registry

| YDB row kind | Primary target(s) | Notes |
|---|---|---|
| `online_source_item` | `hub.actor`, `hub.external_account`, `region_talk.source` | actor/outlet and platform account deduped by typed identity |
| `source_candidate_item` | `region_talk.source_candidate`, `hub.provenance_event` | retain proposed identity and discovery evidence |
| `source_edge_item` | `region_talk.source_edge`, `hub.provenance_event` | typed source-to-source relation and discovery method |
| `comment_link_item` | `region_talk.discovery_observation` | comment/link evidence; attach content/source when resolved |
| `telegram_entity_cache_item` | `region_talk.telegram_entity_cache` | operational cache with expiry and external identity |
| `source_queue_item` | `orchestration.work_item`, `region_talk.source_work_projection` | map state/retry/priority; source PK retained as legacy key |
| `source_status_item` | `region_talk.source_status`, `orchestration.work_item_event` | current status plus reconstructed event where evidence permits |
| `source_onboarding_profile_item` | `region_talk.source_profile` | versioned current profile and policy identity |
| `source_onboarding_evidence_item` | `region_talk.source_profile_evidence` | immutable evidence; never flatten into profile only |
| `post_link_queue_item` | `hub.content_identity`, `orchestration.work_item`, `region_talk.post_intake` | exact/manual URL lane |
| `post_live_item` | `hub.content_item`, `hub.content_identity`, target `hub.object_scope_relation`, compatibility `hub.project_content` | current compact post record plus Region Talk relation |
| `processed_post_item` | `region_talk.post_evaluation`, `hub.provenance_event` | processing/gate state and reasons |
| `text_vector_enrichment_item` | `analysis.result`, `analysis.embedding`, `region_talk.text_evidence` | split metadata/result/vector; dedupe by input/model identity |
| `image_queue_item` | `hub.content_asset`, `orchestration.work_item`, `region_talk.image_evaluation` | preserve ordered media and all prior evidence |
| `candidate_memory_item` | `region_talk.candidate_memory`, `hub.content_identity` | durable duplicate/editorial memory |
| `publication_candidate_item` | `region_talk.publication_candidate`, `region_talk.candidate_revision`, review/publication tables | exact revision is immutable; current projection points to it |
| `external_publication_source_item` | `hub.actor`, `hub.external_account`, `region_talk.external_publication_source` | outlet scope and affiliation remain separate attributes |
| `external_publication_intake_item` | `hub.content_item`, `region_talk.external_publication_intake`, `hub.provenance_event` | preserve request/import identity and candidate/excluded/unresolved disposition |
| unknown | `migration.raw_record`, `migration.row_disposition` | retained as `retained_raw` or `quarantined`; blocks retirement until an explicit accepted disposition |

## Scope effects by target class

| Target class | Required scope effect | Notes |
|---|---|---|
| `hub.actor` / `hub.external_account` | Region Talk `member` or `referenced` according to mapping | a deduped pre-existing actor keeps its old scopes and gains Region Talk |
| `hub.content_item` / `hub.content_asset` | Region Talk `member`/`referenced` | content-specific `hub.project_content` remains compatibility/domain extension during migration |
| `analysis.result` / embedding | exact evaluation scope when result semantics are project/pipeline-sensitive | scope-neutral evidence may be reused without copying |
| `orchestration.work_item` / usage | exact Region Talk `project_pipeline` scope | work execution status is not project membership |
| `region_talk.*` projection | shared root must already resolve Region Talk relation | schema name alone is not scope evidence |
| intentionally excluded raw row | batch origin only unless mapping explicitly creates a referenced object | no fictitious active membership |
| retained/quarantined raw row | batch origin only | no fictitious target |

## Legacy keys and aliases

Each normalized object may carry several source identities through `hub.external_account`,
`hub.content_identity`, `hub.entity_alias` and `migration.legacy_identity_map`. The
original YDB PK is never repurposed as the new primary key. This permits deduplication without breaking traceability. Duplicate-group reconciliation
proves one canonical UUID plus the union of aliases, provenance and all project/pipeline
relations; selecting a canonical UUID never drops Region Talk context.

## Embeddings

Embedding rows are split into metadata and dimension-specific vector tables. Region Talk
currently needs at least 768-dimensional E5 and 1024-dimensional BGE-M3/Qwen-class vectors.
A unique result identity includes content hash, model ID, encoder contract and dimensions.
If a payload is malformed or model dimensions do not match, metadata remains staged and the
vector enters quarantine; it is not truncated/padded.

## Current-state versus history

A YDB row that contains only current state cannot manufacture a full event history. Import
creates a `migration_snapshot` event with source timestamps and raw-row link. Existing true
history/receipts are imported as events where available. New PostgreSQL operation is fully
append-audited from cutover onward. Imported current state is written under an exact
namespaced Region Talk scope and mapped to a normalized cross-pipeline class; it is not
collapsed into `orchestration.work_item.status` or a platform policy decision.

## Direct canonical apply v3 coverage

The append-only 0024 mapper makes the business-critical subset executable after the
snapshot integrity gate:

| Legacy family | Canonical v3 effect |
|---|---|
| external articles and live/processed posts | exact-URL/source-identity dedupe into `hub.content_item`, `hub.content_identity`, Region Talk project membership, provenance, external-publication/post intake |
| source candidates/status/edges/frontier | canonical `region_talk.source` identity, candidate/status/edge evidence, plus paused-but-executable `orchestration.work_item` and `source_work_projection` rows |
| publication candidates | one candidate per canonical content/project and an immutable revision keyed by candidate plus exact payload hash |
| publication schedule | `region_talk.publication_plan` for the Region Talk new-channel target, with `publication_dispatch=false` and no attempt |
| review state/events | `review_decision` only when the exact legacy decision is approve/reject/revise/revoke; unrecognized values remain evidence and are not guessed |

The field aliases accepted by this mapper include `canonical_url`,
`source_queue_status`/`queue_status`, `image_queue_status`, and
`publication_status`; the original payload is still the authoritative evidence.
Statuses outside a target table's enum are preserved verbatim in JSON evidence and use
an explicitly neutral import lifecycle state. This neutral state is not a reconstructed
model verdict or a claim that missing legacy history occurred.

Metric snapshots, heartbeats, model request/budget records, embeddings, image diagnostic
history, and other unsupported historical kinds remain terminally `retained_raw` (or
`quarantined` when malformed). They are losslessly migrated and accounted, but are not
claimed as canonical executable semantics in v3. A later append-only mapper with a proven
target contract is required to promote them.

## Ordered current state across accepted snapshots

Migration 0025 retains `migration.raw_record` and an immutable canonical-state
observation for every supported current-state source row. A separate explicit head is
allowed to move only when the incoming source timestamp is not older and the accepted
canonical revision advances; the exact payload hash makes replay and conflicting state
observable. This updates existing source candidates, source status/history, work items
and their events, publication plans, and review decisions rather than silently keeping
the first snapshot forever. Source status and review/work transitions append history;
mutable projection tables update in place while continuing to reference the immutable
raw row and export batch.

Migration 0026 makes exact-payload replay monotonic: replay may refresh the accepted
revision/raw pointer, but it retains the greatest observed non-null source timestamp.
A later changed payload with an older timestamp is therefore recorded as `stale` and
cannot overwrite the head. The first `source_status_item` also updates the mutable
`region_talk.source.status` projection while reusing the immutable status row already
created by 0024.

The same migration registers the versioned `region-talk-main` definition and all fixed
stages during database bootstrap. The pipeline remains `paused`; its
`publication_dispatch` stage is disabled. Missing heavy post-import evidence is emitted
only through the fixed task-bound `execute_region_talk_post_import_stages` contract as
durable typed work. Review queue rows and work payloads constrain both publication and
notification dispatch to `false`.

The database task authority is also no longer established by the first page. The master
registers the generated credential against the exact task and generation before worker
handoff, and every direct-snapshot procedure verifies that registration for the LOGIN.

## Verified stage dispatch and non-regressing replay

Migration 0027 makes identical-payload replay an immutable observation only: it cannot
move the current revision, batch, raw-record pointer, source clock, or projection.
Changed stale observations remain denied.

The database now selects eligible heavy-stage work through a fixed claim function and
binds the lease and deterministic effect identity to the registered task and ACTIVE
epoch. Only bounded, server-hash-verified metadata enters the immutable result landing;
exact matching landed success is the sole source for `CURRENT` preparation evidence.
Provider-specific launch state, arbitrary SQL, raw model artifacts, publication, and
notification are outside these functions.

The canonical publication queue includes an exact-current publication plan or durable
post-import review row once per candidate revision. Rows from stale batches/revisions
are excluded and publication remains disabled.

## Private worker payload split

Migration 0028 removes business payloads and raw lease material from the supervised
control path. The supervisor claims only an immutable metadata receipt containing task,
batch, stage, work/effect/dispatch identities, policy, timestamps, and hashes. The full
bounded execution payload and raw lease remain in PostgreSQL.

The master registers a separate deterministic worker task and credential, then the
supervisor binds that exact credential, generation, master instance, and epoch to one
dispatch. Only that worker LOGIN can fetch the payload directly from PostgreSQL and land
the exact result through the fixed direct-submit function. Wrong supervisor/worker task,
credential, generation, epoch, effect, binding hash, attempt, or lease fails closed. The
supervisor status receipt exposes only identities, counts/status, and result hashes.
Legacy 0027 payload-returning claim/submit/status functions are revoked from the pipeline
role; publication and notification remain disabled.

## Long-running worker credential rotation

Migration 0029 keeps the deterministic worker task, dispatch, effect, work item, attempt,
input fingerprint, and database lease stable while allowing the supervisor to bind the
next separately registered short-lived credential generation. Binding history is
append-only; the highest exact generation is `ACTIVE` and every prior generation is
`FENCED` by the fetch/submit functions.

Rotation requires the current binding hash, exact next generation, current supervisor
authority, a live child credential for the same worker task/master/epoch, and the original
work/effect/dispatch identity. Exact response-loss replay returns the same rotation
receipt. Prior or cross-worker LOGINs cannot fetch or submit after rotation. No payload,
raw lease, task token, database URL, or publication effect enters the rotation receipt.

## Dependency-ready DAG materialization

Migration 0030 makes every PREPARE/reprepare a deterministic database-side DAG
reconciliation. E5 and BGE work appears only with an ACTIVE-revision runtime pin;
vector fusion appears only after both exact score receipts validate; image, verifier,
and writer work appears in order only after each exact predecessor is current. The
private input row and work UUID are immutable and replaying PREPARE creates no duplicate.

Vector input is `region-talk-vector-fusion-input.v1`; every sorted score row identifies
its E5/BGE stage and immutable result SHA. Image work is not executable from historical
queue/frame-score rows or a public URL. It requires a current candidate-associated
`region-talk-media-artifact-manifest.v1` whose task-private object reference, media ID,
normalized source URL, byte size, content type, artifact SHA, and acquisition receipt
cross-check the accepted `hub.content_asset`. Missing verified media leaves image work
unmaterialized rather than fabricating availability.

Successful evidence is also stage-specific. The database recomputes the canonical
result-metadata SHA, requires the output SHA to equal it, validates bounded typed metrics,
and matches an append-only owner/master-registered runtime pin for the exact canonical
revision and ACTIVE epoch. Runtime pin generations supersede only the exact current
receipt; workers cannot register or choose pins. Arbitrary producers, empty generic
success, forged result hashes, and stale pin/result combinations fail closed.
