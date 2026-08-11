# Lane checkpoint_runtime_wiring Results

## Status
committed

## Requirement IDs
- R-CHECKPOINT-ADAPTER-EXACT-DOWNLOAD
- R-CHECKPOINT-REMOTE-METADATA-JOURNAL
- R-CHECKPOINT-PROVIDER-RUNTIME
- R-CHECKPOINT-INDEPENDENT-VERIFIER
- R-CHECKPOINT-EXACT-HEAD-BOOT

## Branch
agent/operational-mvp/checkpoint-runtime-wiring

## Worktree
`/home/dev/.codex/worktrees/my-data-hub/checkpoint-runtime-wiring`

## Base SHA
`deb4674fc46b04c609935dd920b479cbd866b9c5`

## Head SHA
`bab25368db44eaa2dc7fb0661590d09d18efe15f` (pre-amend content commit; final commit is the branch HEAD).

## Files changed
- `src/my_data_hub/checkpoints/kaggle_runtime.py`
- `src/my_data_hub/providers/kaggle/adapter.py`
- `src/my_data_hub/providers/kaggle/contracts.py`
- `src/my_data_hub/providers/kaggle/control_journal.py`
- `src/my_data_hub/providers/kaggle/__init__.py`
- `notebooks/templates/checkpoint_verifier/runtime.py`
- `notebooks/03-checkpoint-verifier-restore-smoke/worker.ipynb`
- `notebooks/03-checkpoint-verifier-restore-smoke/kernel-metadata.example.json`
- `tests/provider/test_kaggle_adapter.py`
- `tests/provider/test_checkpoint_runtime_wiring.py`
- `tests/provider/test_remote_kaggle_journal.py`
- `tests/master/test_checkpoint_verifier_notebook_runtime.py`
- `.codex/lanes/checkpoint_runtime_wiring/RESULTS.md`

## Commands run
- `python scripts/create_notebooks.py`
- `python scripts/create_notebooks.py --check`
- `python scripts/validate_repository.py`
- `ruff check .`
- `python -m compileall -q src tests scripts`
- `pytest -q -rs`
- Focused provider/checkpoint/verifier pytest selections during implementation.

All Python commands used `/home/dev/.codex/worktrees/my-data-hub/operational-mvp/.venv`.

## Tests / verification
- Repository validator: 2,833 checks, zero errors.
- Notebook generator drift check: zero drift.
- Ruff: pass.
- Compileall: pass.
- Full pytest: pass; one expected environment-gated disposable PostgreSQL test skipped.
- Focused tests prove destination-preserving numeric dataset download, numeric run-output request with before/after latest-source fencing, streaming directory upload, exact nested HEAD resolution, boot readback ID/hash recheck, authenticated runtime headers, metadata-only remote journal/claim lookup, master-compatible create-and-publish composite, child-verifier authorization separation, isolated restore invocation, and typed receipt binding.

## Risks
- The remote HTTP endpoints and ledger lookup are a separate root-owned integration and must implement the exact paths/payloads/runtime-header authorization reported to the integrator.
- Real Kaggle upload/readback/verifier and real PostgreSQL restore remain external acceptance gates; this lane used contract fakes and the repository restore-verifier unit harness.
- Kaggle 2.2.4 internally resolves kernel output by current slug even when given a numeric ref. The adapter submits the numeric ref and verifies the exact current source/kernel identity before and after download; any concurrent source advance fails closed.
- The independent restore receipt is a typed verifier output/control-plane attestation. It is not inserted into the already immutable candidate bytes, which would change the manifest after the restore it attests.

## Merge notes
- Cherry-pick the lane commit.
- Root must wire `build_runtime_checkpoint_coordinator_from_environment(identity=..., attempt_id=..., postgres_bin=...)` into notebook entrypoint construction.
- Required factory env contract was sent to the integrator; remote HEAD is nested exact metadata and boot must use `resolve_head()` plus `exact_head_readback()`, never dataset latest.
- `providers.kaggle.__init__` uses lazy master-runtime exports to avoid the lifecycle branch's package import cycle while preserving its public API.
