# Bounded Region Talk blogger workload

This workload is **not** a Region Talk cutover. It reads only
`region_talk_external_blogger_evidence` from YDB in one snapshot transaction,
ordered by `record_id`, and streams the exact 27-column rows directly to the
ACTIVE Kaggle PostgreSQL master. Devstand keeps only sanitized counters and
hashes; it never persists source rows.

A provider-side preflight performs two ordered scans and emits only the detached
`region-talk-ydb-source-read-receipt.v1` metadata contract. The ACTIVE master
rechecks its hash evidence, then compares a second direct scan inside the import
transaction. Row accounting is dynamic and bound to that receipt; the historical
inventory below is observed evidence, not a required production count.

The read principal must have only database-scoped `ydb.viewer`. A zero-row
UPDATE denial is required before SELECT. Each source row receives exactly one
terminal disposition. Ambiguous actor kind is retained as `unknown`, never
silently coerced to `person`; repeated public account identities require an
explicit duplicate group/decision in PostgreSQL.

Live read-only inventory on 2026-08-11 observed 266 rows, 266 IDs, 14 batches and
14 source-file hashes. Full extraction/import/checkpoint remains incomplete
until an ACTIVE master and direct tunnel exist.
