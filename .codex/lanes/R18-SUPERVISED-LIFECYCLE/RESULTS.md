# R18-SUPERVISED-LIFECYCLE results

## Scope

- Base SHA: `771d68cd7841b02524aeeaa25eb6db737b8f3124`
- Implementation SHAs: `edf5cdd` (`fix(region-talk): supervise rotated worker lifecycle`) and
  fail-closed follow-up `3882438` (`fix(region-talk): keep provider-only completion retryable`).
- Final documentation commit SHA: reported in the parent handoff because this file is part of
  that commit.
- Live mutation: none.
- Schedule, publication, notification: unchanged and disabled.

## Requirement disposition

1. **Done — rotated supervisor cleanup.** After private-authority activation, the exact
   replacement `RegionTalkAccessBinding` is persisted in SQLite with exact replay semantics.
   Restart cleanup now targets the current generation; the explicit terminal outcome survives
   `CLEANUP_PENDING` to `CLEANED`.
2. **Done — fresh stage access after startup.** Generated stage code completes source/image,
   offline manifest/wheel validation and installation, posts metadata attestation, then forces an
   N-to-N+1 credential rotation before the first tunnel or PostgreSQL connection. Later fetch and
   submit checkpoints rotate again near expiry.
3. **Done — terminal delivery and central reaping.** The child retries one immutable terminal
   callback. The central adapter persists the exact Kaggle run identity, observes provider
   COMPLETE/FAILED/deadline states through the single injected adapter, persists a bounded
   metadata-only retryable terminal proof before effects (never provider-only success), and idempotently revokes authority plus deletes
   the exact protected Notebook and Dataset after callback loss, startup failure, or timeout.
4. **Done — reviewer A16 status distinction.** `RegionTalkCycleResult` and the bounded supervisor
   retain accepted-snapshot and stage receipt hashes. `IMPORT_COMPLETE_WAITING_STAGES`,
   `RETRYABLE`, and `IMPORT_FAILED` are distinct from `SUCCEEDED`; bounded idle/waiting no longer
   becomes success. The control lifecycle may terminalize solely to perform cleanup, while the
   metadata-only explicit outcome remains in durable `error_code` through `CLEANED`.

## Evidence

- `uv run pytest -q tests/region_talk --maxfail=10`
  - `112 passed, 1 skipped`
  - skipped test is the existing opt-in disposable PostgreSQL test; no live database was used.
- `uv run ruff check` on all changed Python/test files
  - `All checks passed!`
- `uv run python -m compileall -q src tests`
  - exit 0.
- `uv run python scripts/validate_repository.py`
  - `ok: true`, `checks: 4558`, no errors or notes.
- `git diff --check`
  - exit 0.

Focused tests cover rotation/projection replay, restart cleanup of generation N+1, explicit
waiting-stage receipt preservation, forced pre-DB rotation ordering, exact terminal callback
retry generation, provider COMPLETE/FAILED/timeout reconciliation, restart replay without
duplicate deletes, authority revocation, and forbidden-byte journal scanning.

## Changed files

- `src/my_data_hub/control_plane/app.py`
- `src/my_data_hub/workloads/region_talk/central_launcher.py`
- `src/my_data_hub/workloads/region_talk/direct_pipeline.py`
- `src/my_data_hub/workloads/region_talk/pipeline_contracts.py`
- `src/my_data_hub/workloads/region_talk/pipeline_runtime.py`
- `src/my_data_hub/workloads/region_talk/production_assembly.py`
- `tests/region_talk/test_direct_pipeline.py`
- `tests/region_talk/test_long_run_authority.py`
- `tests/region_talk/test_pipeline_core.py`
- `tests/region_talk/test_stage_dispatch.py`
- `docs/operations/region-talk-stage-execution.md`
- `.codex/lanes/R18-SUPERVISED-LIFECYCLE/RESULTS.md`

## Residual risks and blockers

- No live Kaggle/YDB/PostgreSQL canary was run. Provider-run identities, timing, and remote
  cleanup are proven with exact adapter-contract fakes, not claimed as live evidence.
- The opt-in disposable PostgreSQL suite was not enabled in this lane.
- Heavy model/media/editorial runtime readiness remains owned by R16/R19. This lane does not
  claim E5/BGE/image/final-verifier/writer production evidence or alter their assets.
- Provider `UNKNOWN` before the exact deadline remains nonterminal by design; the next lifespan
  poll observes it again, and the deadline path eventually produces the retryable cleanup proof.
