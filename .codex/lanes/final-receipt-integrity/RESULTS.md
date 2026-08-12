# Lane final-receipt-integrity Results

## Status
committed

## Requirement IDs
- FRI-1
- FRI-2
- FRI-3
- FRI-4
- FRI-5
- FRI-6

## Branch
`agent/operational-mvp/final-receipt-integrity`

## Worktree
`/home/dev/.codex/worktrees/my-data-hub/final-receipt-integrity`

## Base SHA
`6b1cebdd1e81541669b66f63e6369905c58dcc11`

## Head SHA
Implementation commit: `b82e253aa7fd49e391c30d5cc6e03c9c83817c7a`.
This results file is added by the immediately following metadata-only commit; use
the branch tip reported in the handoff when integrating both commits.

## Files changed
- `schemas/operational-mvp-acceptance-receipt.v1.schema.json`
- `examples/contracts/operational-mvp-acceptance-receipt.v1.example.json`
- `scripts/validate_repository.py`
- `tests/test_operational_mvp_acceptance_receipt.py`
- `docs/operations/operational-mvp-acceptance-receipt.md`
- `docs/operations/evidence/2026-08-11-operational-mvp/operational-mvp-acceptance-blocked.json`
- `.codex/lanes/final-receipt-integrity/RESULTS.md`

## Evidence
- `COMPLETE` is conditional on observed-live scope, exact checkout/merge/deployed/post-deploy commit identity, clean deployment state, Gates A-N exactly once with PASS evidence, zero blockers, qualifying 24-scenario/15-run/15-kernel and lifecycle counts, exact matrix/scenario reconciliation, full blogger accounting, and E5/BGE-M3 coverage.
- Required implementation-review, deployment, post-deploy, security, data-integrity, and real-matrix artifacts are source-commit-bound and SHA-256 verified from local bytes.
- `BLOCKED` requires `completion_criteria_met: false` and precise INTERNAL/EXTERNAL blockers with affected gates/scenarios and closure proof.
- The committed example remains synthetic and BLOCKED. The observed current receipt remains BLOCKED with zero qualifying operational-matrix runs/scenarios and now records the internal central-checkpoint path blocker rather than inventing live evidence.
- The repository validator no longer hard-codes BLOCKED; it applies semantic consistency and exact-head validation before accepting COMPLETE.

## Commands run
- `uv sync --extra dev`
- `.venv/bin/ruff check scripts/validate_repository.py tests/test_operational_mvp_acceptance_receipt.py`
- `.venv/bin/pytest -q tests/test_operational_mvp_acceptance_receipt.py`
- `.venv/bin/python scripts/validate_repository.py`
- `.venv/bin/python -m compileall -q src tests`
- `.venv/bin/pytest -q`
- `git diff --check`

## Tests / verification
- Focused acceptance-receipt tests: PASS, `6 passed`.
- Ruff on changed Python files: PASS.
- Repository validator: PASS, `3737` checks, `0` errors.
- Compileall: PASS.
- Full pytest first run: PASS to `100%`, exit `0` (before the final defensive invalid-matrix parsing guard; the focused suite and repository validator passed after that guard).
- Final full-suite retry reached `100%` but the runner exited `1` because the shared root filesystem became full (`OSError: [Errno 28] No space left on device`) while pytest wrote its cache. The lane's own generated `.venv` and caches were removed afterward. Integration should rerun full pytest when shared disk headroom is restored.

## Risks
- No live deployment, provider, security, or data-integrity evidence was created or claimed in this lane; the committed operational receipt intentionally remains BLOCKED.
- Required COMPLETE evidence is intentionally limited to locally readable content-addressed files. External URLs can be supplementary but cannot close an offline-verification gate by themselves.
- Full-suite final-head confirmation remains an integration rerun because of shared-disk exhaustion; all focused and repository gates passed at the implementation head.

## Merge notes
Cherry-pick the implementation commit and this results-only follow-up commit. The schema retains version `v1` while strengthening its conditional contract; both committed receipt instances were migrated to the stronger shape.
