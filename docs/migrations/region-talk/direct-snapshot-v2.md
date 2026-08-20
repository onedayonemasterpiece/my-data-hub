# Region Talk direct snapshot v2

## Boundary

The v2 transfer is executed by the separate Region Talk Kaggle pipeline. It reads
YDB with a read-only identity and connects directly to the one ACTIVE PostgreSQL
master with a short-lived `mdh_region_talk_pipeline` credential. Source rows are
never written to a devstand file, control-plane database, callback, Dataset, or MCP
argument.

Migration 0025 requires the master credential reconciler to register that LOGIN's
exact `credential_id`, principal, `worker_kind=region_talk`, task UUID, generation,
master instance/epoch, command hash, and task-token hash in an append-only PostgreSQL
binding **before** the private credential is handed to the worker. The LOGIN cannot
choose a new task UUID on first use: begin, page, finalize, failure and canonical apply
all resolve the immutable registration for `session_user` and reject any other task.

The closed source scope is:

1. `acq_discovery_opportunities` (`dedupe_key`);
2. `acq_discovery_runs` (`run_uid`);
3. `acq_discovery_surfaces` (`external_id`);
4. `region_talk_compact_state_kv` (`pk`, with **explicit** `kind` column);
5. `region_talk_external_blogger_evidence` (`record_id`).

The 2026-08-19 read-only inventory observed 58,554 rows in these five tables. That
number is evidence, not a coded acceptance constant. Every run computes counts and
hashes again. The compact table had 58,046 rows. The blogger registry had 266 source
records; the separate reviewed blogger import resolved those to 263 canonical actors.
The thousands of `source_candidate_item`, `source_queue_item`, `source_status_item`
and `source_edge_item` records are a discovery frontier, not thousands of confirmed
bloggers.

## Two-pass algorithm

Pass A performs keyset scans in the fixed table order. It retains only row counts,
kind counts and ordered logical hashes in memory. `kind` for compact-state rows comes
from the source column and is never inferred from a `pk` prefix. The manifest binds
the task UUID, ACTIVE master instance/epoch, exact five-table inventory, request hash,
and `publication_effects_enabled=false`.

Pass B scans the same source again and sends pages of at most 500 rows directly to
`migration.land_region_talk_direct_page`. The master checks:

- exact epoch-bound pipeline login and task identity;
- contiguous pages and strictly increasing primary keys;
- bounded JSON/page sizes and server-recomputed payload hashes;
- the row logical hash reconstructed from the exact UTF-8 byte-length framing of
  `source_table`, `source_pk`, `row_kind`, the fixed-microsecond UTC source timestamp,
  and `payload_sha256`;
- the page hash reconstructed from those row hashes, rather than the caller's page
  receipt;
- exact replay or explicit idempotency conflict;
- table/kind membership in Pass A;
- raw row and terminal-disposition accounting.

Finalization recomputes every payload, row, page, table, and final snapshot digest from
the persisted raw and integrity relations, then compares that evidence independently
with Pass A and Pass B. A same-count payload mutation or a forged row/page/Pass-B hash
therefore fails closed. Drift, an expired/fenced epoch, a missing disposition, or an
identity conflict also fails closed.
Valid unknown kinds remain `retained_raw`; malformed rows are `quarantined`. Nothing
is silently dropped.

## Typed projections

The v3 landing mapper creates fixed projections only for the core rows that the same
release can canonicalize: articles, posts, publication candidates, discovery
opportunities/surfaces, source registry/frontier, schedules, and reviews. Historical
run snapshots, metrics, cursor/feedback/delivery history, image/model diagnostics, and
LLM request/budget records remain losslessly `retained_raw` and terminally accounted;
the older shadow tables are not treated as canonical support for those kinds.

Migration 0024 adds the canonical apply boundary. Only an integrity-verified,
non-quarantined `complete` snapshot belonging to the same task credential and ACTIVE
epoch may enter it. The apply transaction projects supported core rows into
`hub.content_item`, content identities/project membership, `region_talk.source`,
source candidates/status/edges, `orchestration.work_item`, publication candidates and
immutable revisions, review decisions, and publication plans. It advances the global
canonical revision exactly once and writes the semantic outbox item and immutable
apply receipt in that same transaction. Exact replay returns that receipt; a replay
whose batch, task, request, epoch, Pass-B table/count/hash contract, or verified hash
differs is rejected before the stored receipt can be returned.

Legacy values that do not fit a current constrained lifecycle enum are not presented as
model conclusions. Their exact value remains in provenance/evidence while the new
workflow receives a documented neutral import state (`pending`, `draft`, `planned`, or
`unknown`) needed to make the queue executable. Review decisions are created only for
an exact recognized approve/reject/revise/revoke value. No publication attempt is
created and publication dispatch remains off.

For repeated accepted snapshots, 0025 separates immutable observation history from an
explicit current-state head. Source candidate/status, source work item, publication
plan, and review state use `(source_table, source_pk)` identity plus source timestamp,
payload hash, and canonical revision ordering. A newer changed payload updates the
existing canonical current object and appends its status/work/review evidence; an exact
payload replay is a no-op, and an older observed timestamp cannot overwrite the head.
Raw snapshot rows and every applied/replay/stale decision remain append-only evidence.

Migration 0026 closes two fresh-master gaps. Database bootstrap itself installs the
paused `region-talk-main` pipeline and its fixed stages, so a `source_queue_item` can be
canonicalized without an out-of-band Python registry call. Exact-payload replay uses a
monotonic source timestamp, later stale changed payloads remain evidence-only, and the
first source-status observation updates the source projection immediately. The
publication stage stays disabled and every imported work payload carries
`publication_dispatch=false`.

After canonical apply, the same exact registered task credential may call the single
fixed `migration.execute_region_talk_post_import_stages(uuid,uuid,jsonb)` seam. PREPARE
binds a deterministic UUIDv5 stage run to the latest accepted batch and returns only
canonical candidates plus honest `MISSING` heavy-evidence states. COMMIT validates
complete typed outcomes and the fixed DAG, persists append-only stage receipts and
candidate outcomes, forms the bounded review queue, and creates deterministic
`orchestration.work_item` requests for missing evidence. Request identity excludes the
transport-only `requested_at`, so response-loss replay is stable. Caller-selected SQL,
table names, stages, publication, and notification effects are not accepted.

The full snapshot does not create blogger actors. Blogger evidence rows reuse the
dedicated identity map/profile when present and otherwise remain raw pending that
reviewed path. This preserves the 266-to-263 decision instead of duplicating it.

MCP readers are restricted to `snapshot_inventory_v2`, `articles_v2`, `posts_v2`,
and the canonical publication queue views. `region_talk.queue.list/summary` read
publication candidates/revisions/plans/reviews, with `status` and `channel` filters;
the legacy raw work-family queue projections are not exposed under those product tool
names. Typed rows
are selected only from the latest integrity-verified, canonical-applied `complete`
snapshot whose export batch is `accepted`; failed, landing, quarantined, older, and
duplicate source identities are excluded deterministically. Readers receive neither `migration.raw_record` nor a
generic SQL capability. Article/post list and search omit bodies; exact get returns
the typed body field. The only outbox row is the internal canonical-commit signal;
publication outbox effects and publication attempts are not created and stay disabled
until an explicit later activation gate.

## Honest readiness

The code and schema define the transfer and reconciliation boundary. Operational
cutover is not proven until a supervised Kaggle run produces a complete receipt on
the current ACTIVE epoch, all dynamic source counts/hashes reconcile, typed readback
passes, and the new canonical state is included in a verified checkpoint.
