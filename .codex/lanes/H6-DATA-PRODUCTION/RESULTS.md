# H6-DATA-PRODUCTION results

- Lane: `H6-DATA-PRODUCTION`
- Requirements: `FM16`, `FM17`, `FM18`, `FM19`, `FM21` production gateway/runner only
- Base SHA: `c4a9992a9ed1a21bb8d1310f0480754c0fd037a9`
- Verified implementation SHA: `00b2845f1c005ed30452a0ccdecadda2677a541b`
- Final lane HEAD: the documentation-only commit containing this report

## Delivered

- Added `ControlPlaneDataWorkloadGateway`, bounded HTTPS control and streamable-HTTP MCP clients, crash-safe mode-0600 state storage, mode-0600 owner-envelope loading, production config/receipt types, and a bounded production runner.
- Added stable CLI/Notebook entry `scripts/provider/data_workload_evidence.py` with plan/config/state/output paths and optional owner duplicate-resolution envelope.
- Corrected the owned core so H5/H3 deterministic request UUIDs are persisted before POST, exact model/server request hashes are stored after acceptance, and ambiguous requests replay the same versioned request with exact hash comparison.
- Corrected FM17 to persist its idempotency hash before mutation and retain the server-assigned rotation operation ID/request hash for `operation.get` resume.
- Corrected FM21 to use real H1 preview semantics: exact parameterized insert preview affects one row, operation ID and only the signed-receipt hash persist before apply, insert/delete each require a durable post-change checkpoint, and final delete preview must affect zero rows. Preview replay reconstructs the transient signed receipt after restart and must match persisted evidence.
- FM18/FM19 use one H3 request containing both pinned worker assets and split only the terminal model-specific hashes into inner evidence.
- Production receipts always set `live_evidence=false` and `outer_reconciliation_required=true`; this lane has no `PASS` value or authority.
- Added plan/config/receipt/state JSON schemas and examples plus operational documentation.

## H5 handoff closure (integration follow-up)

Integrated H5 now exposes durable `quarantine_evidence`, `duplicate_review`, and
`duplicate_review_inputs` from the exact `BloggerQuarantineReceipt`. An
integration-focused test feeds those real receipt projections through
`ControlPlaneDataWorkloadGateway.observe_blogger`, binds a mode-0600 envelope to
the exact identity/member inputs, verifies H5's replay-source matcher, and proves
the v2 request acceptance/status retain the exact server request SHA-256 and
`REQUESTED` state. `FM16_H5_QUARANTINE_PROJECTION_UNAVAILABLE` is no longer the
interface blocker.

The implemented status supplies:

1. `quarantine_evidence` validating as `BloggerQuarantineEvidence`: exact request/request hash/source operation/export batch/failure code; raw, dispositioned, undispositioned and quarantined accounting for all 266 source records; logical, record-ID-set and canonical-outcome SHA-256; positive duplicate-group count equal to pending count.
2. `duplicate_review` validating as `DuplicateReviewEvidence`: the same batch/request/operation/request hash and group counts, plus SHA-256 of the sorted identity set, sorted member-record-ID set and bounded review projection.

It additionally supplies deterministic `duplicate_review_inputs` for the owner;
all projections contain no source payload columns or decisions. The gateway and
state machine cross-bind the evidence before returning
`AWAITING_OWNER_AUTHORIZATION`. Full decisions remain only in a regular
current-user-owned mode-0600 envelope and are sent solely in the H5 v2 request.

The production config also deliberately requires distinct exact ACTIVE-master operation UUIDs for H5 v1 and v2. The outer orchestration lane must source-pin/persist those pre-existing operations; this gateway does not silently call `master.ensure` behind the core's mutation journal.

## Evidence and commands

All commands ran in `/home/dev/.codex/worktrees/my-data-hub/h6-data-production` using the existing operational MVP virtual environment.

- `python -m compileall -q src tests scripts/provider/data_workload_evidence.py` — PASS
- `ruff check .` — PASS
- `ruff format --check ...` — PASS after formatting
- `pytest -q tests/acceptance/test_data_workloads.py tests/acceptance/test_data_production.py` — PASS (`14 passed`)
- `python scripts/validate_repository.py` — PASS (`3396` checks, zero errors/notes)
- `pytest -q` — PASS (full suite; two existing skips and only two existing `jsonschema.RefResolver` deprecation warnings)
- `git diff --check` — PASS
- CLI fail-closed smoke with a mode-0644 owner envelope — exit `2`, bounded mode-0600 receipt with `outcome=BLOCKED`, `live_evidence=false`, blocker `FM16_OWNER_ENVELOPE_PERMISSIONS_INVALID`; no token was required/read before this pre-mutation denial.

Focused tests cover server/model request-hash fidelity, exact ambiguous replay, missing H5 projection denial, server-assigned FM17 identity, one shared two-model H3 request, fixed-only FM21 SQL/preview receipt binding, mode-0600 owner authorization, crash-safe state resume, and committed schema/example validation.

## Risks / integration boundary

- No real production mutation was attempted. Fake dependencies exercise transitions but cannot produce live acceptance.
- The H5 projection interface is implemented, but no real owner-authorized replay
  or provider run was executed in this follow-up. Live FM16 and downstream
  evidence remain blocked on production execution and outer reconciliation.
- The outer driver must independently bind `EVIDENCE_READY` to the exact provider run/output. Inner evidence alone is not PASS.
- No operational driver/matrix, control/MCP implementation, adapter/catalog/app/ledger/migration, master notebook core, provider gateway, canonical SQL migration, deploy file, local PostgreSQL, raw business row, vector, DSN or credential was added or modified.

## Changed files

- `.codex/lanes/H6-DATA-PRODUCTION/RESULTS.md`
- `docs/operations/data-workload-production.md`
- `docs/operations/operational-data-workload-core.md`
- `examples/provider/data-workload-production-config.v1.example.json`
- `examples/provider/data-workload-production-receipt.v1.example.json`
- `examples/provider/operational-data-workload-plan.v1.example.json`
- `examples/provider/operational-data-workload-state.v1.example.json`
- `schemas/provider/data-workload-production-config.v1.schema.json`
- `schemas/provider/data-workload-production-receipt.v1.schema.json`
- `schemas/provider/operational-data-workload-plan.v1.schema.json`
- `schemas/provider/operational-data-workload-state.v1.schema.json`
- `scripts/provider/data_workload_evidence.py`
- `src/my_data_hub/acceptance/__init__.py`
- `src/my_data_hub/acceptance/data_production.py`
- `src/my_data_hub/acceptance/data_workloads.py`
- `tests/acceptance/test_data_production.py`
- `tests/acceptance/test_data_workloads.py`
