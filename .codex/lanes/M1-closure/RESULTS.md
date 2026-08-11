# M1-closure results

- Lane: `M1-closure` — Stage N scheduled acceptance metadata/status/probe closure.
- Base SHA: `c02c2f17ae85fad01b5a2e1b80cec8aa5979f681`.
- Validated implementation head SHA: `9d6a7dc0fccb2564a29582d06dce4ab0cd14e8fe`.
- Outcome: implemented and validated; live mutation/deployment was intentionally not run.

## Delivered

- Hard-coded scheduled MCP audience host `mcp-datahub.kenigevents.ru`; only canonical HTTPS
  `/mcp` with implicit port 443 and no userinfo, query, or fragment is admitted before either
  bearer is sent.
- Existing reader catalog remains unchanged. New connector/probe/request contracts are visible
  only to an owner/operator identity.
- `checkpoint.status` now returns metadata-only exact current/previous checkpoint IDs, numeric
  version refs, manifest hashes, verification times, and HEAD generation.
- Append-only control migration 009 adds bounded connector heartbeat metadata only; no connector
  payloads or business rows are stored.
- Current/previous isolated-restore and forced-rotation requests validate exact generation,
  exact version and timeout bounds, then remain `BLOCKED` because no production operation
  consumer exists. They deliberately do not enqueue immortal `REQUESTED` rows on scheduled
  runs; a future consumer must add a durable claim/execute contract first.
- Stale-epoch and protected-resource probes validate bindings but explicitly remain `BLOCKED`;
  they do not synthesize denial evidence without a guaranteed non-mutating route through the
  real admission/policy path.
- Scheduled receipts have a strict schema/example and distinguish runner
  `source_commit_sha` from observed `deployed_commit_sha`.

## Validation evidence

- `python3 -m compileall -q src tests scripts` — PASS.
- `uv run --no-project --with '.[dev]' python scripts/create_notebooks.py --check` — PASS,
  zero drift.
- `uv run --no-project --with '.[dev]' python scripts/validate_repository.py` — PASS,
  2,897 checks and zero errors.
- `uv run --no-project --with ruff ruff check ...` for every touched Python file — PASS.
- `uv run --no-project --with '.[dev]' pytest -q tests/provider/test_scheduled_acceptance.py
  tests/provider/test_scheduled_control_interfaces.py tests/control/test_ledger_master.py
  tests/mcp/test_dynamic_contracts.py` — PASS.
- `uv run --no-project --with-editable '.[dev]' pytest -q` — PASS (two pre-existing skips).
- `git diff --check` — PASS.
- Mirrored migration SHA-256:
  `b2f25fc0860873eae6034534904e6447f525e83645aa06ef0e46cd5fc8c593fa` for both copies.

## Remaining external blockers / risks

- `checkpoint_restore_smoke` has no devstand operation consumer wired to the real isolated
  Kaggle verifier launch/reconciliation path.
- `forced_master_rotation` has no devstand operation consumer wired to checkpoint, drain/fence,
  and next-master orchestration.
- No safe non-mutating adapter currently exercises stale-epoch or protected-resource denial via
  the same production admission path; receipts remain blocked rather than overclaiming.
- A runtime/control callback must populate connector heartbeat metadata before live connector
  coverage can pass.
- No Kaggle resource mutation, master deployment, or live MCP action was performed in this lane.

## Changed files

- `.github/workflows/nightly.yml`
- `.github/workflows/provider-real.yml`
- `control_migrations/009_connector_coverage_metadata.sql`
- `examples/contracts/scheduled-acceptance-receipt.v1.example.json`
- `schemas/scheduled-acceptance-receipt.v1.schema.json`
- `scripts/provider/scheduled_acceptance.py`
- `src/my_data_hub/control_plane/adapters.py`
- `src/my_data_hub/control_plane/ledger/sql/009_connector_coverage_metadata.sql`
- `src/my_data_hub/control_plane/ledger/store.py`
- `src/my_data_hub/mcp/catalog.py`
- `src/my_data_hub/mcp/service.py`
- `tests/control/test_ledger_master.py`
- `tests/provider/test_scheduled_acceptance.py`
- `tests/provider/test_scheduled_control_interfaces.py`
- `.codex/lanes/M1-closure/RESULTS.md`
