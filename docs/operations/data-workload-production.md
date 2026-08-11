# H6 production data-workload executor

`my_data_hub.acceptance.data_production` adapts the resumable H6 core to the
existing H5, H3, checkpoint/master, blogger-accounting, and H1 MCP interfaces.
`scripts/provider/data_workload_evidence.py` is the stable Kaggle Notebook/CLI
entry point. It exchanges bounded metadata only and cannot claim live `PASS`.
Even `EVIDENCE_READY` has `live_evidence=false` and
`outer_reconciliation_required=true`; the outer operational driver must match the
exact provider run/output before it may accept FM16–FM19 or FM21.

## Invocation contract

```bash
python scripts/provider/data_workload_evidence.py \
  --plan /kaggle/working/data-plan.json \
  --production-config /kaggle/working/data-production.json \
  --state /kaggle/working/data-state.json \
  --output /kaggle/working/data-receipt.json \
  [--owner-envelope /kaggle/working/duplicate-resolution.json]
```

The config and receipt schemas are
`schemas/provider/data-workload-production-config.v1.schema.json` and
`schemas/provider/data-workload-production-receipt.v1.schema.json`. State and
inner evidence continue to use the `operational-data-workload-state.v1` and
`operational-data-evidence.v1` contracts; the CLI plan validates against
`schemas/provider/operational-data-workload-plan.v1.schema.json`. Matching
examples are under `examples/provider/`.

Credentials are read only from `MY_DATA_HUB_DATA_CONTROL_TOKEN`,
`MY_DATA_HUB_DATA_MCP_READER_TOKEN`, and
`MY_DATA_HUB_DATA_MCP_OPERATOR_TOKEN` (the names may be overridden by CLI
options). They never enter state, output, exceptions, or URLs. The control token
is optional only for the exact in-master `http://127.0.0.1:8080` endpoint. MCP
must be credential-free HTTPS `/mcp`; redirects are rejected.

The owner duplicate-resolution envelope must validate as
`region-talk-blogger-duplicate-resolution-envelope.v1`, be a regular non-symlink
file with exact mode `0600`, and bind every persisted duplicate-review hash. The
runner stops at `AWAITING_OWNER_AUTHORIZATION` until it is supplied. Source
record IDs in the full envelope are never copied into H6 state or receipts.

## Mutation/replay rules

- H5/H3 request UUIDs are deterministically persisted before POST. Their true
  request hashes come from the exact versioned request models/server response;
  a retry must return the same hash or fails terminally.
- FM17 persists its idempotency hash before requesting rotation, then persists
  the server-assigned operation ID and polls `operation.get`. A lost response
  resumes by replaying the same idempotency identity.
- H1 preview returns the deterministic operation ID and signed receipt. State
  stores the operation ID and only the receipt hash before apply. After restart,
  preview is replayed and must reproduce both before apply is allowed.
- Every poll is bounded by a 1–60 second interval and a 60–43000 second deadline.
  Ambiguous apply responses become resumable `FAIL`; cleanup continues by
  operation status and ends only after the delete checkpoint plus zero-row
  delete preview.
- The only SQL is the fixed parameterized `hub.project` fixture insert/delete.
  No generic SQL argument, local PostgreSQL, PGDATA, YDB rows, blogger fields,
  vectors, DSN, or provider mutation is exposed.

## Current fail-closed H5 handoff

The existing H5 `FAILED / BloggerMigrationQuarantined` status contains only the
failure code. Production replay therefore stops with
`FM16_H5_QUARANTINE_PROJECTION_UNAVAILABLE`; it does **not** infer hashes from an
owner envelope or fabricate accounting. To unblock the serial H5 status lane,
`GET /control/v1/blogger-closure/requests/{request_id}` must add these two
metadata objects for that exact terminal failure:

1. `quarantine_evidence`, validating as `BloggerQuarantineEvidence`: exact
   request/request hash, source operation, export batch, failure code, all 266
   raw/dispositioned counts, positive quarantined count, zero undispositioned,
   logical/record-set/outcome hashes, and equal positive duplicate-group/pending
   counts.
2. `duplicate_review`, validating as `DuplicateReviewEvidence`: the same batch,
   request, operation and request hash; equal group/pending counts; SHA-256 of
   the sorted identity set, sorted member-record-id set, and bounded review
   projection.

These fields contain no source payload columns or decisions. They must be read
from durable H5 quarantine/review evidence. Until both validate and cross-bind,
no v2 request is sent. No default reader/catalog expansion is requested by this
lane.
