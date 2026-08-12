# H6-DRIVER-TRUST-CLOSURE results

## Scope

- Requirement IDs: FM08, FM10, FM11, FM24; operational carrier/reconciliation trust boundary.
- Original lane base: `85321972d30cb39a82a3436406e7f9a90d333674`.
- Final integration base after prerequisite rebases: `162328a`.
- Code/docs head before this results-only commit: `5bf0d67`.
- Effort/risk: max-review security/schema/auth boundary; no live mutation was run.

## Delivered

- Hard-pinned the regular checked-in `operational_kaggle_driver.py`; removed workflow/CLI arbitrary argv and local Kaggle credential/adapter use.
- Added append-only `EXECUTE -> RECONCILE -> CLEANUP` requests. RECONCILE re-reads exact claim/run/output metadata through the same deployed control authority and the matrix compares every locator/hash.
- Wired FM08/FM10/FM11/FM24 to exact owner-only `acceptance.scenario.request/status`, ACTIVE `target_operation_id`, bounded terminal polling, full typed `MasterAcceptanceReceipt`, receipt SHA, runtime-attested carrier source, numeric provider run identity, and output receipt hashes.
- Derived fixed assertions only from typed evidence. Added exact terminal master lifecycle observations for FM08 restart/recovery and FM11 old/new epoch rotation.
- Moved the second provider `clean_rotation` gate from single-epoch FM24 to FM11; FM24 retains only its 60–90 minute soak gate with heartbeat/read/checkpoint/recovery evidence.
- Removed the legacy generic self-authored PASS path. Existing FM13/FM20 action paths now fail after mutation when no independently reconcilable control output receipt exists.
- Updated v2 schemas, exact matrix example, focused tests, workflow assertions, and operations documentation.

## Honest remaining production risk

The integrated ledger status projection currently emits the exact carrier launch identity but leaves `output_file_name`, `output_file_sha256`, `output_tree_sha256`, and `output_receipt_sha256` null unless an official terminal provider-output observation is persisted. The driver requires all four. Therefore a real FM08/FM10/FM11/FM24 action with the current null projection terminates as `FAIL` after mutation, never PASS/BLOCKED. Closing live PASS requires the control-owned official output observation; this lane does not fabricate it or add a second adapter.

FM08 additionally requires terminal `master.status` to expose a distinct recovery provider run. A same-run terminal projection fails the lifecycle contract.

## Validation

Commands run from the isolated lane worktree:

- `python -m compileall -q src tests scripts` — PASS.
- `python scripts/validate_repository.py` — PASS, 3,653 checks, zero errors/notes.
- `ruff check .` — PASS.
- `pytest -q tests/provider/test_operational_kaggle_driver.py tests/provider/test_operational_kaggle_matrix.py` — PASS, 96 tests.
- `pytest -q` — PASS (1,027 collected; two environment-gated skips observed).
- `git diff --check` — PASS.

## Changed files

- `.github/workflows/provider-real.yml`
- `scripts/provider/operational_kaggle_driver.py`
- `scripts/provider/operational_kaggle_matrix.py`
- `schemas/provider/operational-kaggle-driver-request.v2.schema.json`
- `schemas/provider/operational-kaggle-driver-result.v2.schema.json`
- `examples/provider/operational-kaggle-matrix-plan.v1.example.json`
- `tests/provider/test_operational_kaggle_driver.py`
- `tests/provider/test_operational_kaggle_matrix.py`
- `docs/operations/operational-kaggle-matrix.md`
- `.codex/lanes/H6-DRIVER-TRUST-CLOSURE/RESULTS.md`
