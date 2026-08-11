# FINAL-BLOGGER production closure

Run `python3 scripts/bloggers/run_final_closure.py run ...` only on the protected
devstand operator account. The control client is pinned to
`http://127.0.0.1:8080`; it does not transmit a reusable control credential and
cannot target a remote host. The MCP client is pinned to the owner-approved
`https://mcp-datahub.kenigevents.ru/mcp` audience before it attaches the operator
token. The command is fail closed and returns `78` before any control-plane
request when a modern Kaggle token is absent.

The command first calls `POST /control/v1/master/ensure`. It then stores one
secret-free, exact request bound to that ensure operation. The request is claimed
only by the matching ACTIVE run/attempt/master/epoch. The stage is default-off;
ordinary master runs never access YDB.

The importer executes **inside the ACTIVE Kaggle master**. `MY_DATA_HUB_YDB_ENDPOINT`,
`MY_DATA_HUB_YDB_DATABASE`, and the dedicated viewer-only
`YDB_ACCESS_TOKEN_CREDENTIALS` are supplied through Kaggle User Secrets. No
PostgreSQL URL is handed to another notebook. The stage creates a five-minute,
one-connection `mdh_migration_operator` LOGIN bound to the current epoch, proves
that a zero-row YDB UPDATE returns the SDK's exact `UNAUTHORIZED` status, streams
the exact ordered snapshot, requires 266 distinct rows, and commits the importer
transaction. YDB denial/read requests are capped at 10/30 seconds and PostgreSQL
enforces a 180-second transaction timeout; admission requires 300 seconds of
remaining active runtime and at least 270 seconds on the current lease. It drops
the LOGIN in all outcomes.

`COMMITTED_PENDING_CHECKPOINT` is not success. The master immediately enters its
normal drain/checkpoint path. A final receipt is emitted only after exact private
checkpoint publication/readback, independent isolated restore verification and
HEAD promotion; M1's durable rotation consumer cold-boots that HEAD; and bounded
MCP `bloggers.migration.accounting` plus `bloggers.statistics` agree on revision,
hashes, zero pending/quarantined/undispositioned rows, and the exact canonical
actor count in the import receipt. The source row count remains exactly 266;
explicit same-person resolutions can correctly make the canonical actor count
smaller than 266.
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
PostgreSQL batch is nevertheless published through the normal verified-checkpoint
shutdown path. A replay is not admitted until that exact source operation owns the
current verified HEAD.

The mode-0600 envelope contains decisions and provenance metadata, never YDB row
payloads. It binds `authorization_id`, authorizer and authorization time to the
source request/operation/request SHA-256, deterministic export batch, project,
snapshot, source revision and pinned query hash. Each sorted decision binds the
exact identity hash and member-record-id set to one reviewed canonical source row
and actor UUID. The control plane accepts it only on a **new ACTIVE ensure
operation**, after the source request is terminally quarantined and its checkpoint
is current. The v2 migration request SHA-256 covers the complete envelope; the
import receipt covers that request hash. Partial, stale, inconsistent,
wrong-authorizer, future-dated, changed-source, or changed-account decisions fail
closed and leave prior quarantine evidence effective.

Run the replay with a fresh idempotency key:

```bash
chmod 600 /run/my-data-hub/blogger-duplicate-resolution-envelope.json
python3 scripts/bloggers/run_final_closure.py run \
  --idempotency-key final-blogger-resolve-20260811-01 \
  --project-id "$PROJECT_ID" \
  --snapshot-at 2026-08-09T00:00:00Z \
  --source-revision "$SOURCE_COMMIT_SHA" \
  --duplicate-resolution-envelope /run/my-data-hub/blogger-duplicate-resolution-envelope.json \
  --receipt /run/my-data-hub/final-blogger-closure.json
```

Use
`examples/bloggers/region-talk-blogger-duplicate-resolution-envelope.v1.example.json`
only as a shape reference. Copy exact source bindings from the loopback request
status; inspect durable duplicate evidence through the bounded operator profile.
Never copy raw export rows into the envelope or the control ledger.

A valid exact replay writes one append-only batch replay, one append-only
resolution per group, and one append-only effective disposition per raw row in
the same canonical transaction. The selected source row is `normalized`; other
rows explicitly targeting that actor are `deduplicated`. Raw payloads and their
first quarantine dispositions are never updated. Exact later retries reuse the
stored revision, hashes, actor/account counts, and effective dispositions, so no
second checkpoint request or canonical revision is created.

If the import-receipt response is lost, the master fences and discards that
unacknowledged ephemeral attempt instead of promoting it. Re-running the same
authorized resolution against the prior verified quarantine HEAD deterministically
recreates the same append-only resolution. If the receipt was acknowledged but
the response was lost, the runtime endpoint returns the exact stored receipt on
retry; it cannot downgrade `IMPORT_COMMITTED` to `FAILED`.
