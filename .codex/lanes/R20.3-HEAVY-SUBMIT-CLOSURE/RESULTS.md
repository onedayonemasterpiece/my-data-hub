# R20.3 Heavy Submit Closure — Results

## Scope and revisions

- Lane: `R20.3-HEAVY-SUBMIT-CLOSURE`
- Base SHA: `defe48be3ecf5ded840ea0b0268590449ddcfd8f`
- Implementation SHA: `36e6e8508aec7b3bd3413b26b98634d2cb037999`
- Writable scope used: append-only migration 0033, Region Talk heavy/dispatch contract code, focused unit/disposable-PostgreSQL tests, this results file.
- Forbidden production assembly, central launcher, KPA/provider/deploy, and migrations 0001–0032 were not edited.

## Closed defects

1. Heavy `image_scoring`, `final_verifier`, and `writer` successes can now persist their exact typed private result digest as the public metadata artifact. Migration 0033 retains the frozen v9/v10 dependency and semantic checks by normalizing only the artifact field for the metadata-only predecessor, while the actual stored result remains bound to the exact private digest.
2. The combined boundary no longer accepts a plain self-hashed object as a heavy result. Python uses `HeavyStagePrivateResult` plus the exact discriminated R20 result model and checks locally derivable metrics. PostgreSQL independently validates the closed per-stage result shape, canonical result hash, false publication/notification flags, current rich-input and revision provenance, fact/source/media/upstream bindings, runtime pin and producer, and an exact server-derived guard-metric object before delegating to the existing credential/lease/idempotency submit path.
3. The prior function is renamed append-only; no historical migration was changed. Exact replay remains authoritative and forged private replay cannot replace the append-only artifact.

## Evidence

- `MDH_RUN_DISPOSABLE_POSTGRES=1 uv run --extra dev pytest -q tests/region_talk/test_snapshot_integrity_postgres.py -x`
  - PASS.
  - Uses a disposable tmpfs PostgreSQL and the real migration chain through schema revision 33.
  - For each of image -> final -> writer: real metadata claim, child credential registration/binding, private sparse/rich fetch, exact combined success, typed-field-removal forgery rejection, guard-pin-metric forgery rejection, enrichment forgery rejection, and byte-for-byte exact replay.
- `uv run --extra dev pytest -q tests/region_talk`
  - PASS: all Region Talk tests; one environment-gated disposable test skipped in the non-disposable invocation.
- `uv run --extra dev pytest -q tests/region_talk/test_heavy_contracts.py tests/region_talk/test_stage_dispatch.py`
  - PASS: 30 tests.
- `uv run --extra dev ruff check src/my_data_hub/workloads/region_talk/heavy_wiring.py src/my_data_hub/workloads/region_talk/stage_dispatch.py tests/region_talk/test_heavy_contracts.py tests/region_talk/test_snapshot_integrity_postgres.py`
  - PASS.
- `uv run --extra dev python -m compileall -q src tests`
  - PASS.
- `uv run --extra dev python scripts/validate_repository.py`
  - PASS: `4681` checks, zero errors/notes.
- `uv run --extra dev pytest -q`
  - Code/test collection completed; 15 unrelated provider-upload tests failed solely because the shared filesystem had about 583 MiB free and the intentionally hard `MIN_UPLOAD_DISK_RESERVE_BYTES` admission guard rejected staging. All failures have the same `provider upload staging disk reserve would be violated` cause, outside this lane; no provider behavior was modified.
- `uv run --extra dev python scripts/verify_region_talk_migration_flow.py`
  - Not independently runnable without `MY_DATA_HUB_DATABASE_URL`; the stronger disposable-PostgreSQL end-to-end test above applied and exercised the full migration chain.

## Changed files

- `sql/migrations/0033_region_talk_heavy_artifact_result_validation.sql`
- `src/my_data_hub/workloads/region_talk/heavy_wiring.py`
- `src/my_data_hub/workloads/region_talk/stage_dispatch.py`
- `tests/region_talk/test_heavy_contracts.py`
- `tests/region_talk/test_snapshot_integrity_postgres.py`
- `.codex/lanes/R20.3-HEAVY-SUBMIT-CLOSURE/RESULTS.md`

## Residual risks / blockers

- This closure does not attach external image/model/editorial provider capabilities. Missing reviewed assets/providers must still yield retryable failure through the existing R20 runtime contracts; 0033 does not fabricate success.
- The full-suite disk-reserve failures require free space or the test's injected disk-usage seam; lowering the production reserve was explicitly not done.
