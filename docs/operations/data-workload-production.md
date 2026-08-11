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

## Implemented fail-closed H5 handoff

H5 now derives a bounded metadata-only `BloggerQuarantineReceipt` from the
durably committed rejected import and exposes three sanitized objects from
`GET /control/v1/blogger-closure/requests/{request_id}` for the exact
`FAILED / BloggerMigrationQuarantined` request:

1. `quarantine_evidence`, validating as `BloggerQuarantineEvidence`: exact
   request/request hash, source operation, export batch, failure code, all 266
   raw/dispositioned counts, positive quarantined count, zero undispositioned,
   logical/record-set/outcome hashes, and equal positive duplicate-group/pending
   counts.
2. `duplicate_review`, validating as `DuplicateReviewEvidence`: the same batch,
   request, operation and request hash; equal group/pending counts; SHA-256 of
   the sorted identity set, sorted member-record-id set, and bounded review
   projection.
3. `duplicate_review_inputs`: the sorted identity groups, record IDs, projected
   actor IDs and optional existing actor IDs used to prepare the owner envelope.

These fields contain no source payload columns or decisions. The gateway parses
the first two projections and the state machine cross-binds them before returning
`AWAITING_OWNER_AUTHORIZATION`. The mode-0600 envelope decisions must cover the
exact third projection; H5 revalidates that coverage at atomic v2 admission.
The accepted v2 response and subsequent status must retain the exact H5 request
SHA-256 and `REQUESTED` state. The former projection blocker is therefore closed.

This is interface evidence only: no real provider run, owner decision, H5 import,
checkpoint, or outer reconciliation was executed, so `live_evidence` remains
false and production acceptance remains blocked pending those live steps.

## Operational matrix driver integration

The operational driver consumes this protocol through the optional
`data_workload` object in
`MY_DATA_HUB_OPERATIONAL_EVIDENCE_DRIVER_JSON`. Its four fields are absolute
owner-fixed paths: `plan_path`, `production_config_path`, `state_path`, and the
optional `owner_envelope_path`. The plan and production config must be bounded
regular non-symlink files. The driver requires the plan matrix ID/source commit
to equal its signed launch request, requires the dedicated production control,
reader and operator credentials, and persists a mode-0600 initial state before
starting the CLI. A launch-fenced resume without that state is BLOCKED/0 and
never recreates an action.

One shared state produces the exact ordered FM16, FM17, FM18, FM19 and FM21
bundle. The driver additionally proves that FM18/FM19 share one request ID and
carry distinct worker task IDs. It does not treat the bundle as PASS: each
requirement becomes a separate task-run-bound acceptance Notebook, followed by
independent outer output reconciliation and exact claim cleanup.

At the owner authorization boundary the CLI is invoked without an absent
optional envelope so it can safely reach and persist the quarantine/review
pause. The matrix records only the exact resumable
`FM16_AWAITING_OWNER_AUTHORIZATION` blocker, stops before FM17, and on the next
run reuses the same matrix launch and state. The envelope is admitted only when
the file exists and the production loader verifies owner UID, non-symlink
regular type, exact mode 0600, size, schema, and persisted review bindings.
There is no generated or default duplicate decision.

Any ordinary BLOCKED receipt is allowed through the operational driver only
while the state is still `INITIAL` with zero mutations. A capability loss,
deadline, malformed receipt, or transport ambiguity after a persisted action
phase is an operational FAIL, never rewritten as BLOCKED/0. No live execution
has been performed by this repository change.
