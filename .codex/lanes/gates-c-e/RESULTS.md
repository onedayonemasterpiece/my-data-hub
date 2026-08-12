# Lane gates-c-e Results

## Status
committed

## Requirement IDs
- C1 — exact durable resource lease helper payloads
- C2 — secret redaction across serialized runtime event surfaces, with adversarial coverage
- E1 — deterministic per-notebook execution pin contract
- E2 — generated private/fail-closed notebooks and zero generator drift

## Branch
`agent/gates-c-e`

## Worktree
`/home/dev/.codex/worktrees/my-data-hub/gates-c-e`

## Base SHA
`6b1cebdd1e81541669b66f63e6369905c58dcc11`

## Head SHA
Implementation head: `b170e3670424402adf13d878319f08199958dd42`.

The following report-only commit contains this file; the implementation SHA above is the exact reviewed/tested code head and avoids a self-referential commit hash.

## Outcome / evidence
- Added `DurableResourceLease` with the exact owner-task fields consumed by the coordinator: `lease_id`, `resource_kind`, `resource_ref`, `holder_id`, `lease_until`, and fencing `epoch`.
- `acquire_resource`, `renew_resource`, and `release_resource` now emit the lease under `data.resource`; renew also emits its exact requested deadline at `data.lease_until`. Incomplete, naive-deadline, and cross-task payloads fail closed.
- Runtime identities, phase, status, nested data/metrics and their keys, artifact kinds/locators, URL user-info/query/fragment credentials, labelled credentials, JWTs, PEM private keys, and the known per-run secret are redacted before body sizing, JSONL persistence, delivery, or replay. Redaction retains fixed event schema and safe diagnostics plus `redacted_fields` counts.
- Operational notebooks `01`–`06` declare a complete `my-data-hub-notebook-execution-pins/v1` contract and fail before install/execution unless a SHA-256-bound launch manifest exactly matches the observed CPython patch, immutable Kaggle image digest, numeric input Dataset versions, private flag, task-wheel/source hashes, output contract, model revision, resource class, and cleanup/retention policy.
- All generated metadata remains `is_private=true`, `production_ready=false`, and `activation_prerequisites_satisfied=false` until launch prerequisites and real evidence exist.
- The generator owns byte-identical E5/BGE packaged worker assets after parent-approved scope expansion. SHA-256 evidence:
  - E5 notebook and asset: `a8105f3f67fc3127e62d605352870f5345b9858d6a32ff070cf0c71086fbd8e7`
  - BGE-M3 notebook and asset: `7e1eb74b1b312e390f98843616af87ca5873057883ad793d4294d0f2bbffd1f8`

## Files changed
- `scripts/create_notebooks.py`
- `notebooks/README.md`
- `notebooks/01-platform-runtime-smoke/{worker.ipynb,kernel-metadata.example.json}`
- `notebooks/02-postgres-master/{worker.ipynb,kernel-metadata.example.json}`
- `notebooks/03-checkpoint-verifier-restore-smoke/{worker.ipynb,kernel-metadata.example.json}`
- `notebooks/04-region-talk-ydb-bloggers-importer/{worker.ipynb,kernel-metadata.example.json}`
- `notebooks/05-e5-blogger-embedding-worker/{worker.ipynb,kernel-metadata.example.json}`
- `notebooks/06-bge-m3-blogger-embedding-worker/{worker.ipynb,kernel-metadata.example.json}`
- `src/my_data_hub/embeddings/assets/{e5-worker.json,bge-worker.json}` (authorized exact generated copies only)
- `src/my_data_hub/runtime_sdk/{__init__.py,client.py,events.py,sanitize.py}`
- `tests/runtime/test_runtime_sdk.py`
- `tests/test_notebooks.py`
- `.codex/lanes/gates-c-e/RESULTS.md`

## Commands run
- `uv venv .venv`
- `uv pip install --python .venv/bin/python -e '.[dev]'`
- `.venv/bin/pytest -q tests/runtime/test_runtime_sdk.py`
- `.venv/bin/python scripts/create_notebooks.py`
- `.venv/bin/pytest -q tests/test_notebooks.py tests/runtime/test_runtime_sdk.py`
- `.venv/bin/ruff check src/my_data_hub/runtime_sdk scripts/create_notebooks.py tests/runtime/test_runtime_sdk.py tests/test_notebooks.py`
- `.venv/bin/python scripts/create_notebooks.py --check`
- `.venv/bin/python -m compileall -q src tests`
- `.venv/bin/python scripts/validate_repository.py`
- `.venv/bin/pytest -q`
- `.venv/bin/ruff check .`
- `.venv/bin/pytest -q tests/runtime/test_runtime_sdk.py tests/test_notebooks.py tests/embeddings/test_master_stage_live.py`
- `sha256sum` comparison of generated E5/BGE notebook and packaged asset pairs
- `.venv/bin/pytest --collect-only` (`1079 tests collected`)
- `git diff --check`

## Tests / verification
- Focused runtime/notebook/packaged-asset suite: PASS (`33` tests).
- Full repository suite: PASS, exit 0 (`1079` collected; expected skips and two pre-existing `RefResolver` deprecation warnings).
- Repository validation: PASS (`3714` checks, zero errors).
- Compileall: PASS.
- Full Ruff: PASS.
- Generator `--check`: PASS with `drift: []`.
- `git diff --check`: PASS.

An earlier full-suite run correctly exposed that the packaged E5/BGE assets were stale after notebook regeneration. Work stopped for scope confirmation; the parent authorized only those two exact generated copies plus drift coverage. After generator ownership was added, the full suite passed.

## Risks / blockers
- No blocker.
- No live Kaggle execution was claimed. Provider-observed image identities and numeric Dataset versions are intentionally launch-time pins; absent or inconsistent bindings fail before package installation. The notebooks remain non-production-ready until those bindings and real receipts exist.
- The resource helper API intentionally replaces the old incomplete `(resource_kind, resource_ref, ...)` form. Repository search found no callers of those helper methods; donor-envelope compatibility remains covered separately.
- Heuristic credential-label redaction may suppress a diagnostic field whose key is named like a credential. This is the deliberate confidentiality-first behavior; `redacted_fields` preserves the fact that data was removed.

## Merge notes
- Cherry-pick implementation commit `b170e3670424402adf13d878319f08199958dd42`, then the subsequent report-only commit.
- Do not resolve generated notebook conflicts by hand. Re-run `scripts/create_notebooks.py`, then require `--check` with zero drift.
- The E5/BGE packaged asset JSON files must remain byte-identical to their generated notebooks.
