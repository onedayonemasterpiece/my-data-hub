# FINAL-BLOGGER production closure

The production path has two read-only authorities and never materializes a row
artifact on the devstand. First, an owner runs the provider preflight with the
dedicated database-scoped `ydb.viewer` service account. The preflight proves the
zero-row write probe returns either the SDK's exact `UNAUTHORIZED` status or the
current serverless Query Service's exact structured `ABORTED` wrapper containing
the table-bound `AccessDenied` issues, and performs two non-overlapping ordered
`QuerySnapshotReadOnly` scans. Source values remain
in process only. Its sole output is a mode-0600 detached receipt containing
bounded counts, set/logical hashes, timestamps, source/query/schema identities,
source revision, the viewer binding hash, and deterministic snapshot/batch
identity. It contains no YDB row, token, URL credential, PostgreSQL DSN, or
business value.

```bash
chmod 600 /run/my-data-hub/ydb-viewer-token /run/my-data-hub/ydb-access-bindings.json
python3 scripts/provider/read_only_ydb_blogger_export.py \
  --endpoint "$MY_DATA_HUB_YDB_ENDPOINT" \
  --reader-service-account-id "$YDB_READER_SERVICE_ACCOUNT_ID" \
  --iam-token-file /run/my-data-hub/ydb-viewer-token \
  --source-revision "$SOURCE_COMMIT_SHA" \
  --access-bindings-json /run/my-data-hub/ydb-access-bindings.json \
  --receipt /run/my-data-hub/blogger-ydb-source-read-receipt.json
```

The receipt is metadata-only, but it is not proof of a later import. Transfer it
through the protected operator workflow, never a row export, and start closure:

```bash
python3 scripts/bloggers/run_final_closure.py run \
  --idempotency-key final-blogger-20260812-01 \
  --project-id "$PROJECT_ID" \
  --source-read-receipt /run/my-data-hub/blogger-ydb-source-read-receipt.json \
  --receipt /run/my-data-hub/final-blogger-closure.json
```

The control client is pinned to `http://127.0.0.1:8080`; it does not transmit a
reusable control credential and cannot target a remote host. The MCP client is
pinned to the owner-approved `https://mcp-datahub.kenigevents.ru/mcp` audience.
The command fails before a control mutation when the central Kaggle credential
preflight fails, and it will not create a request without the exact detached
source receipt.

The importer executes only inside the matching ACTIVE Kaggle master. The master
uses the dedicated viewer-only YDB credential from Kaggle User Secrets, repeats
the denial probe, then performs a fresh hash-only ordered scan and reconciles it
to the provider receipt. `MY_DATA_HUB_YDB_DATABASE` must equal the pinned source
database path, and `MY_DATA_HUB_YDB_READER_SERVICE_ACCOUNT_ID` must equal the
receipt's reviewed viewer principal. It opens a second direct ordered scan and
streams those
rows into the one PostgreSQL importer transaction. Before commit, the importer
compares the dynamic row count, distinct record count, record-id set hash,
logical hash, and source-file count with the preceding master scan. Any provider
receipt mismatch, changed scan, incomplete accounting, or lease/credential fault
rolls back and leaves canonical current/previous state unchanged. Counts are
bounded by the contract but are never assumed to be a historical fixed value.

No PostgreSQL URL is handed to another Notebook. The stage creates a five-minute,
one-connection `mdh_migration_operator` LOGIN bound to the current epoch and
drops it on every outcome. YDB denial/read requests are capped at 10/30 seconds;
PostgreSQL enforces a 180-second transaction timeout; admission requires 300
seconds of remaining active runtime and at least 270 seconds on the lease.
Current `STOPPED`/zero-RCU source state remains an external blocker. Code or a
metadata receipt alone is not evidence that a live read or import occurred.

`COMMITTED_PENDING_CHECKPOINT` is not success. The master immediately enters its
normal drain/checkpoint path. A final receipt is emitted only after exact private
checkpoint publication/readback, independent isolated restore verification and
HEAD promotion; M1's durable rotation consumer cold-boots that HEAD; and bounded
MCP `bloggers.migration.accounting` plus `bloggers.statistics` agree on revision,
hashes, zero pending/quarantined/undispositioned rows, and the exact canonical
actor count in the import receipt. The source row count is taken from the exact source receipt; explicit same-person
resolutions can correctly make the canonical actor count smaller than that dynamic
source count.
The same closure then walks the complete bounded `bloggers.list` cursor and
proves representative `bloggers.get`, `bloggers.provenance`, and exact/FTS
`bloggers.search` results without writing returned rows to its receipt. Only
then is status `DURABLE_COMPLETE`.

The control plane records a bounded `region-talk-ydb-bloggers-v1` connector
coverage heartbeat only after that exact import checkpoint is VERIFIED. It
contains state, contract version and timestamp only; it is not derived from or
accompanied by business rows.

The devstand stores request, run, count, hash, revision and checkpoint identities
only. It never receives blogger rows, YDB credentials, a PostgreSQL DSN, PGDATA,
or checkpoint bytes. Receipts are canonical JSON, bounded to 256 KiB, and mode
0600. Unresolved-free imports without duplicate decisions retain the compatible
v2/v1 receipt pair. A resolved duplicate replay uses the append-only
`schemas/region-talk-ydb-bloggers-import-receipt.v3.schema.json` and
`schemas/blogger-closure-receipt.v2.schema.json` pair; the earlier schemas are
unchanged.

## Duplicate quarantine and replay

Shared normalized account identities never merge during the first import. The
whole batch remains non-canonical, every claimant is terminally quarantined, and
the master stores a durable duplicate group plus immutable member evidence. A
replay may proceed only with a complete
`region-talk-blogger-duplicate-resolution-envelope.v1`. The first request becomes
terminal `FAILED` with the exact code `BloggerMigrationQuarantined`; its rejected
PostgreSQL batch yields a bounded metadata-only
`region-talk-ydb-bloggers-quarantine-receipt.v1`, not a successful checkpoint.
The callback persists that receipt and SHA-256 against the exact request, run,
attempt, master instance and epoch before status exposes sanitized
`quarantine_evidence`, `duplicate_review`, and `duplicate_review_inputs`. Raw YDB
rows never cross the control plane.

The mode-0600 envelope contains decisions and provenance metadata, never YDB row
payloads. It binds `authorization_id`, authorizer and authorization time to the
source request/operation/request SHA-256, deterministic export batch, project,
snapshot, source revision and pinned query hash. Each sorted decision binds the
exact identity hash and member-record-id set to one reviewed canonical source row
and actor UUID. The control plane accepts it only on a **new ACTIVE ensure
operation**, after the source request is terminally quarantined and its receipt is
immutable. Every decision must cover exactly one persisted identity group and its
complete sorted member set; the canonical actor must be the existing actor when
present, otherwise the deterministic projection for the chosen member. The v2
migration request SHA-256 covers the complete envelope; the
import receipt covers that request hash. Partial, stale, inconsistent,
wrong-authorizer, future-dated, changed-source, or changed-account decisions fail
closed and leave prior quarantine evidence effective.

Run the replay with a fresh idempotency key:

```bash
chmod 600 /run/my-data-hub/blogger-duplicate-resolution-envelope.json
python3 scripts/bloggers/run_final_closure.py run \
  --idempotency-key final-blogger-resolve-20260811-01 \
  --project-id "$PROJECT_ID" \
  --source-read-receipt /run/my-data-hub/blogger-ydb-source-read-receipt.json \
  --duplicate-resolution-envelope /run/my-data-hub/blogger-duplicate-resolution-envelope.json \
  --receipt /run/my-data-hub/final-blogger-closure.json
```

Use
`examples/bloggers/region-talk-blogger-duplicate-resolution-envelope.v1.example.json`
only as a shape reference. Copy exact source bindings from the loopback request
status. These facts support human review but never synthesize an owner decision;
inspect durable duplicate evidence through the bounded operator profile. Never copy raw export rows into the envelope or the control ledger.

A valid exact replay writes one append-only batch replay, one append-only
resolution per group, and one append-only effective disposition per raw row in
the same canonical transaction. The selected source row is `normalized`; other
rows explicitly targeting that actor are `deduplicated`. Raw payloads and their
first quarantine dispositions are never updated. Exact later retries reuse the
stored revision, hashes, actor/account counts, and effective dispositions, so no
second checkpoint request or canonical revision is created.

If the import-receipt response is lost, the master fences and discards that
unacknowledged ephemeral attempt instead of promoting it. Re-running the same
authorized resolution against the immutable quarantine receipt deterministically
recreates the same append-only resolution. If the receipt was acknowledged but
the response was lost, the runtime endpoint returns the exact stored receipt on
retry; it cannot downgrade `IMPORT_COMMITTED` to `FAILED`.


Both blogger and embedding request admission use one SQLite `BEGIN IMMEDIATE`
compare-and-set transaction for ACTIVE operation/service identity, current epoch,
unexpired lease, and insertion. Embedding additionally binds the exact VERIFIED
checkpoint HEAD and canonical revision. An exact existing request remains
replayable after drain; a new request racing drain/rotation is rejected, and a
request admitted immediately before terminal drain is reconciled to
`ADMISSION_RUNTIME_TERMINAL_BEFORE_CLAIM` rather than remaining stranded.
