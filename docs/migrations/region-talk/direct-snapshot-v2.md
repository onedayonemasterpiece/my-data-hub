# Region Talk direct snapshot v2

## Boundary

The v2 transfer is executed by the separate Region Talk Kaggle pipeline. It reads
YDB with a read-only identity and connects directly to the one ACTIVE PostgreSQL
master with a short-lived `mdh_region_talk_pipeline` credential. Source rows are
never written to a devstand file, control-plane database, callback, Dataset, or MCP
argument.

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
- exact replay or explicit idempotency conflict;
- table/kind membership in Pass A;
- raw row and terminal-disposition accounting.

Finalization compares Pass A, Pass B, and landed PostgreSQL counts per table. Drift,
an expired/fenced epoch, a missing disposition, or an identity conflict fails closed.
Valid unknown kinds remain `retained_raw`; malformed rows are `quarantined`. Nothing
is silently dropped.

## Typed projections

The mapper creates fixed projections for articles, posts, publication candidates,
discovery opportunities/runs/surfaces, the source frontier, schedule/review/cursor
state, and LLM request/budget idempotency. Valid kinds without an approved semantic
mapper remain losslessly raw and terminally accounted until a later append-only
mapping release.

The full snapshot does not create blogger actors. Blogger evidence rows reuse the
dedicated identity map/profile when present and otherwise remain raw pending that
reviewed path. This preserves the 266-to-263 decision instead of duplicating it.

MCP readers are restricted to `snapshot_inventory_v2`, `articles_v2`, `posts_v2`,
`queue_v2`, and `queue_summary_v2`. They receive neither `migration.raw_record` nor a
generic SQL capability. Article/post list and search omit bodies; exact get returns
the typed body field. Publication attempts/outbox effects are not created by this
migration and stay disabled until an explicit later activation gate.

## Honest readiness

The code and schema define the transfer and reconciliation boundary. Operational
cutover is not proven until a supervised Kaggle run produces a complete receipt on
the current ACTIVE epoch, all dynamic source counts/hashes reconcile, typed readback
passes, and the new canonical state is included in a verified checkpoint.
