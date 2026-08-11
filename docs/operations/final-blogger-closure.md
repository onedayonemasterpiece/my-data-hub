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
PostgreSQL URL is handed to another notebook. The stage creates a four-minute,
one-connection `mdh_migration_operator` LOGIN bound to the current epoch, proves
that a zero-row YDB UPDATE returns the SDK's exact `UNAUTHORIZED` status, streams
the exact ordered snapshot, requires 266 distinct rows, and commits the importer
transaction. It drops the LOGIN in all outcomes.

`COMMITTED_PENDING_CHECKPOINT` is not success. The master immediately enters its
normal drain/checkpoint path. A final receipt is emitted only after exact private
checkpoint publication/readback, independent isolated restore verification and
HEAD promotion; M1's durable rotation consumer cold-boots that HEAD; and bounded
MCP `bloggers.migration.accounting` plus `bloggers.statistics` agree on revision,
hashes, zero pending/quarantined/undispositioned rows, and 266 canonical bloggers.
Only then is status `DURABLE_COMPLETE`.

The devstand stores request, run, count, hash, revision and checkpoint identities
only. It never receives blogger rows, YDB credentials, a PostgreSQL DSN, PGDATA,
or checkpoint bytes. Receipts are canonical JSON, bounded to 256 KiB, and mode
0600. Schemas are `schemas/region-talk-ydb-bloggers-import-receipt.v2.schema.json`
and `schemas/blogger-closure-receipt.v1.schema.json`.
