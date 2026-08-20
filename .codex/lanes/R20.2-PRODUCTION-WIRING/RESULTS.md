# R20.2 Production Wiring Results

## Scope

- Lane: `R20.2-PRODUCTION-WIRING`
- Requirement IDs: `R20.2-0032`, `R20.2-H1-NORMALIZED-MEDIA-AUTHORITY`, `R20.2-H2-TWO-HASH-AUTHORITY`, `R20.2-PRIVATE-RUNTIME`, `R20.2-FAIL-CLOSED-ASSETS`
- Base SHA: `d46366bbb6bdedb34afd5fb40c842f475ea74ac8`
- Implementation head SHA: `37d0c131edb63dafbf6339d676d3fbbf04df978d`

## Delivered

1. Append-only migration 0032 adds hidden heavy-evidence and heavy-result tables, owner/master-only evidence registration, private worker rich-input fetch, and atomic private-result plus bounded public-metadata submit.
2. A v2 acquisition receipt projection hashes the normalized URL, binds the immutable 0031 receipt through `legacy_receipt_sha256`, and rechecks ACTIVE epoch, current candidate, accepted snapshot, and current asset columns. The 0031 row is not modified.
3. Sparse database `work_input_fingerprint` remains dispatch authority. The server computes a distinct `enrichment_sha256` over the closed rich input, and the rich `input_fingerprint` binds both. Python and SQL validate all three rather than comparing incompatible preimages.
4. Image/final/writer execution validates the private receipt and closed R20 input, invokes only injected deterministic runtimes, validates typed results against input, derives frozen 0030/0031 guard metrics, and stores the typed result privately before the guarded normal submit.
5. The old direct worker submit is no longer executable by the pipeline role. Publication and notification remain hardcoded false. Missing evidence, runtime factory, or a non-production-ready asset manifest yields `FAILED_RETRYABLE`; no synthetic success is created.
6. The frozen 0031 DAG receipt remains parseable only through a legacy sparse bridge; heavy execution requires corrected v2 authority.

## Evidence and commands

- `MDH_RUN_DISPOSABLE_POSTGRES=1 uv run --extra dev pytest -q tests/region_talk/test_snapshot_integrity_postgres.py` — **passed**. This applies all migrations through 0032 in tmpfs PostgreSQL and proves raw mixed-case/query URL versus normalized URL, v2 receipt binding, registered current evidence, and DB sparse-work to Pydantic rich-input materialization.
- `uv run --extra dev pytest -q tests/region_talk` — **passed** (`1 skipped`).
- `uv run --extra dev pytest -q tests/region_talk/test_heavy_contracts.py tests/region_talk/test_heavy_runtime_wiring_v11.py tests/region_talk/test_stage_dispatch.py` — **passed**.
- `uv run --extra dev pytest -q tests/control/test_provider_chunked_uploads.py tests/control/test_mcp_operator_provider.py::test_chunked_upload_finalize_uses_single_adapter_and_durable_manifest` — **passed** in isolation.
- `uv run --extra dev ruff check ...` over all changed Python/tests — **passed**.
- `uv run --extra dev python -m compileall -q src tests` — **passed**.
- `uv run --extra dev python scripts/validate_repository.py` — **passed**, 4,898 checks, zero errors.
- Full `uv run --extra dev pytest -q` reached 100%; 15 unrelated provider-upload tests failed only because the shared filesystem fell below the deliberate 512 MiB staging reserve during the full run. The exact 15 tests pass together in isolation once free space recovers. No upload behavior or limits were changed.

## Changed files

- `sql/migrations/0032_region_talk_heavy_runtime_wiring.sql`
- `src/my_data_hub/workloads/region_talk/heavy_contracts.py`
- `src/my_data_hub/workloads/region_talk/heavy_dag_bridge.py`
- `src/my_data_hub/workloads/region_talk/heavy_wiring.py`
- `src/my_data_hub/workloads/region_talk/notebook_stages.py`
- `src/my_data_hub/workloads/region_talk/stage_dispatch.py`
- `tests/region_talk/test_heavy_contracts.py`
- `tests/region_talk/test_heavy_runtime_wiring_v11.py`
- `tests/region_talk/test_snapshot_integrity_postgres.py`
- `.codex/lanes/R20.2-PRODUCTION-WIRING/RESULTS.md`

## Honest residual blockers / risks

- `assets/heavy-runtime-assets.v1.json` remains `production_ready=false`: exact offline wheels/model hashes and immutable remote-model revisions/smoke receipt are not yet materialized. Therefore production heavy execution correctly remains retryable.
- The authoritative fact/source/profile pack must be registered from current canonical evidence by the master/controller. 0032 provides the closed, idempotent, ACTIVE-epoch-fenced registration and rejects fabricated/stale shapes; it does not invent absent facts.
- No live Google/model/media capability was available in this lane, so no production-ready claim is made.
