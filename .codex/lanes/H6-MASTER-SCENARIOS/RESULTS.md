# Lane H6-MASTER-SCENARIOS Results

## Status
committed

The fixed durable protocol/core and the H4 post-expiry recovery are committed.
The ordinary-production scenario effects are **Partial**, not represented as
live evidence and not represented as complete.

## Requirement IDs
- FM04 — Partial: exact preboot claim/binding and empty-proof receipt contract; real fixed empty-row probe missing.
- FM07 — Partial: exact preboot claim/binding and 20-observation receipt contract; control-side 20-call executor/inventory proof missing.
- FM08 — Partial: exact callback/restart receipt contract; supervisor restart/process-boot identity and one-shot transport suppression missing.
- FM09 — Partial: exact replay/stale/state-invariance contract; ACKed body replay and bounded state snapshot action missing.
- FM10 — Partial: requires >=60s, H1 operator operation/receipt hash, stable denial, rollback-only and revision invariance; fixed H1 denial action missing.
- FM11 — Partial: requires DRAINING, old/new operations, verified handoff and renew/register/write/tunnel denials; cross-epoch probe executor missing.
- FM12 — Partial: requires gate close, exact checkpoint readback/restore/HEAD/STOPPED; post-terminal control finalizer missing.
- FM24 — Partial: enforces 3600–5400 monotonic seconds and positive session/lease/tunnel/stale counts; active-loop soak controller missing.
- H4-R1 — Done: ABSENT trigger after expired tunnel lease terminally fails/fences and permits the next epoch without overlap.

FM05/FM14/FM15 remain owned by `my_data_hub.checkpoints.acceptance`; this lane
did not call or widen their 900-second operation budget.

## Branch
`agent/h6-master-scenarios`

## Worktree
`/home/dev/.codex/worktrees/my-data-hub/h6-master-scenarios`

## Base SHA
`1b12fb450424b99f46120b35267b74dda9626f5b`

## Head SHA
Implementation commits:

- `f5626547d3a4cf1b3944653f821c8a1ac8a781eb` — fixed lifecycle protocol/core
- `3c59cfe7d849d2973bc4f16740a42b62f778055a` — H4 expired-trigger recovery

The RESULTS-only commit follows these implementation commits.

## Files changed
- `control_migrations/016_master_lifecycle_acceptance.sql` and mirrored packaged migration
- `src/my_data_hub/acceptance/master_lifecycle.py` and acceptance exports
- `src/my_data_hub/control_plane/ledger/store.py`
- `src/my_data_hub/control_plane/runtime.py`
- `src/my_data_hub/control_plane/app.py`
- `src/my_data_hub/master_runtime/notebook_entrypoint.py`
- `src/my_data_hub/orchestrator/master/coordinator.py`
- acceptance schemas/example/operations documentation
- focused acceptance/control tests

## Commands run
- `python -m compileall -q src tests`
- `ruff check src tests`
- `python scripts/validate_repository.py`
- `python -m pytest -q`
- focused acceptance/control test suites throughout implementation
- `git diff --check`

The integration worktree virtual environment was used with `PYTHONPATH=src`.

## Tests / verification
- Full exact-head suite: **847 passed, 2 expected skips** (849 collected).
- Repository validation: **3392 checks, 0 errors**.
- Ruff: all checks passed.
- Compileall: passed.
- Focused master acceptance + ledger suite: passed.
- H4 regression proves: provider trigger response loss before mutation, exact ABSENT after 301 seconds, restart, effect FAILED, operation FAILED, attempt FENCED, runtime token revoked, replacement epoch 2, no tunnel-authority overlap.
- No live Kaggle scenario was executed or claimed by this lane.

## Risks

### Production effects handoff (Partial)

A truthful ordinary-production `MasterAcceptanceRuntimeEffects` factory cannot
be assembled from the current primitives. The seam executes inside ACTIVE while
several actions are necessarily preboot, supervisor-side, cross-epoch, or
post-terminal. The follow-up cross-area lane must add:

1. a control acceptance executor for FM04/FM07/FM08/FM11/FM12;
2. runtime-context factory created after RuntimeClient, DatabaseGate, tunnel,
   checkpoint and session state exist;
3. durable process-start UUID plus authenticated supervisor restart;
4. one-shot callback suppression, ACKed-body replay and bounded state hash;
5. fixed H1 lease-expiry DML denial operation/receipt (real SQLSTATE is currently
   generic `55000`; do not fabricate `MDH_EPOCH_LEASE_EXPIRED`);
6. old-runtime renew/register/write/tunnel probes;
7. post-terminal FM12 finalization from terminal output/checkpoint ledger;
8. active-loop FM24 counters and a defined stale-session reconnect probe.

`ControlPlaneMasterRuntime.complete_master_acceptance()` is the scoped exact
completion path intended for cross-epoch/post-terminal control execution. The
ordinary notebook leaves effects disabled unless an explicit task-owned
implementation is injected, so scaffold tests cannot terminalize a live PASS.

### H4 recovery boundary

The persisted `effects.updated_at` claim time is the immutable lease origin used
for expiry. Terminal fencing occurs only after provider reconciliation returns
exact `ABSENT` and the broker TTL has elapsed. A provider run that is FOUND or
AMBIGUOUS is not fenced by this recovery. At/after expiry, even a delayed provider
start cannot acquire the dead same epoch, so advancing only after durable
FAILED/FENCED state preserves the single-writer invariant.

## Merge notes
- Cherry-pick implementation commits in order: `f562654`, then `3c59cfe`, then the RESULTS commit.
- Migration 016 was reserved for this lane; do not renumber canonical PostgreSQL migration 0016.
- Shared MCP catalog/service/provider gateway and operational matrix driver were intentionally not edited.
- A separate cross-area production-effects lane is required before FM04/FM07/FM08/FM09/FM10/FM11/FM12/FM24 can leave BLOCKED.
