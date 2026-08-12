# H6-DATA-CORE results

- Lane: `H6-DATA-CORE`
- Requirements: `FM16`, `FM17`, `FM18`, `FM19`, `FM21` data-workload core only
- Base SHA: `ed95ee2f9503650c08bb6bb56d1444fe46414cb8`
- Verified implementation SHA: `511b8da6e4b929a792aede0e17dc6c460fbcba2e`
- Final lane HEAD: the documentation-only commit containing this report

## Delivered

- Added a frozen Pydantic metadata contract plus async gateway/state-store protocols and a one-transition-per-call resumable state machine.
- FM16 persists deterministic H5 request identities, proves a losslessly accounted v1 duplicate quarantine, stops at owner authorization, binds the authorization to exact review hashes, and requires a v2 verified checkpoint with explicit deduplicated dispositions.
- FM17 captures complete accounting, persists restore identity before submission, requires a new master instance/run and higher epoch, and compares exact accounting/revision before and after restore.
- FM18/FM19 submit one deterministic H3 request, require two distinct pinned E5/BGE task results, and split their terminal evidence hashes without storing vectors.
- FM21 restricts execution to a named SQL-free `hub.project` fixture adapter and sequences insert preview/apply/checkpoint, delete preview/apply/checkpoint, and final zero-row preview. Operation identity is persisted before each apply.
- Ambiguous mutations yield typed, resumable `FAIL` and resume via observation/status rather than repeat apply. No result or schema contains a live `PASS`; the terminal bundle is fixed to `EVIDENCE_READY` and `live_evidence=false`.
- Added append-only state/evidence JSON schemas, examples, operational documentation, and focused protocol tests.

## Evidence and commands

All commands ran in `/home/dev/.codex/worktrees/my-data-hub/h6-data-core` using the existing operational MVP virtual environment.

- `python -m compileall -q src tests` — PASS
- `ruff check .` — PASS
- `pytest -q tests/acceptance/test_data_workloads.py` — PASS (`5 passed`)
- `python scripts/validate_repository.py` — PASS (`3279` checks, zero errors/notes)
- `pytest -q` — PASS (full suite; two existing skips; only two existing `jsonschema.RefResolver` deprecation warnings)
- `git diff --check` — PASS

Focused tests prove the full deterministic path, the explicit owner stop/mismatch boundary, persistence-before-mutation ordering, ambiguity resume without reapply, model/schema example validity, and inability to construct fake live-PASS evidence.

## Risks / integration boundary

- This lane intentionally does not provide the exact Kaggle Notebook adapter or the H6 driver. FM20/evidence lanes must implement `DataWorkloadGateway` against the existing H1/H3/H5 control contracts and persist `DataWorkloadState` in an approved metadata location.
- Fake gateways validate orchestration only and can never establish production PASS. A source-pinned live evidence runner must independently verify and sign the resulting hashes.
- Raw rows, vectors, secrets, DSNs, SQL, bearer receipts, control ledgers, migrations, deploy code, and canonical tables were not added or modified.

## Changed files

- `.codex/lanes/H6-DATA-CORE/RESULTS.md`
- `docs/operations/operational-data-workload-core.md`
- `examples/provider/operational-data-evidence.v1.example.json`
- `examples/provider/operational-data-workload-state.v1.example.json`
- `schemas/provider/operational-data-evidence.v1.schema.json`
- `schemas/provider/operational-data-workload-state.v1.schema.json`
- `src/my_data_hub/acceptance/__init__.py`
- `src/my_data_hub/acceptance/data_workloads.py`
- `tests/acceptance/test_data_workloads.py`
