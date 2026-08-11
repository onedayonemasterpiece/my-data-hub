# C1 Gate K fencing results

## Scope

- Requirement: **C1** — maintain ACTIVE lease/control reachability throughout long Gate K provider work; route canonical embedding imports through fresh non-superuser, epoch-bound `mdh_canonical_committer` credentials; reject commits after expiry/fencing.
- Base SHA: `4916d166e7df80ab676c619a8e2eae7d0ada7b8b`
- Implementation head SHA: `cd68280d7bd64b6a3d55fbc2b53e3c91e5da75e3`
- Branch: `lane/c1-gate-k-fencing`

## Delivered

- Added a dedicated Gate K lease-maintenance thread using separate owner connections. It polls the tunnel, emits authenticated ACTIVE heartbeats, renews the PostgreSQL epoch gate only after control acknowledgement, and fences at the 15-second safety margin or on control/database reachability failure.
- Added foreground lease guards around provider mutations, status polling, output downloads, every canonical import, and final coverage reads. A simulated worker run remains guarded for longer than the original 120-second lease.
- Removed canonical imports from the owner/controller connection. Each artifact now receives a fresh `NOSUPERUSER` epoch-bound LOGIN in `mdh_canonical_committer`, connects as immutable `session_user`, validates it is not superuser, commits through migration 0011's deferred guard, then revokes and drops the LOGIN in `finally`.
- Tracked and dropped reader/operator broker LOGINs before checkpoint publication. Cleanup failure fences and stops the runtime; it never reopens writes.
- Accepted the exact H1 integration contract: only an exact ACTIVE activation response may request `reader` or `reader,operator`; operator maps solely to `mdh_mcp_editor`; both are delivered in one bounded TLS-loopback envelope. Default remains reader-only.
- Added pure fencing and opt-in live PostgreSQL proofs for canonical-committer expiry, forced fencing, and stale-session commit rejection.

## Validation evidence

- `uv run ruff check src/my_data_hub/embeddings/master_stage.py src/my_data_hub/master_runtime/notebook_entrypoint.py tests/embeddings/test_master_stage_live.py tests/master/test_notebook_entrypoint.py tests/master/test_fencing.py tests/master/test_live_postgres.py` — passed.
- `uv run pytest -q tests/embeddings/test_master_stage_live.py tests/master/test_notebook_entrypoint.py tests/master/test_fencing.py tests/master/test_live_postgres.py` — passed; 39 passed and 1 expected opt-in disposable-PostgreSQL skip.
- `uv run python scripts/validate_repository.py` — passed, 3,181 checks and zero errors.
- `uv run python scripts/create_notebooks.py --check` — passed, zero drift.
- `uv run python -m compileall -q src tests scripts` — passed.
- `uv run pytest -q` — passed at implementation head; 100%, with the repository's 2 expected opt-in skips.
- `git diff --check` — passed.

## Risks and integration dependencies

- The live deferred-trigger proof remains opt-in (`MDH_RUN_DISPOSABLE_POSTGRES=1` plus Docker); default CI exercises the same expiry/fence state model and all credential/lifecycle wiring.
- H1's control-side activation/session-registration changes must be merged with this branch so `credential_roles` and optional operator envelopes agree end to end.
- H4 also edits `notebook_entrypoint.py`; integrate H4 after C1 and preserve the lease maintainer, fresh committer factory, activation roles, and pre-checkpoint credential cleanup.

## Changed files

- `src/my_data_hub/embeddings/master_stage.py`
- `src/my_data_hub/master_runtime/notebook_entrypoint.py`
- `tests/embeddings/test_master_stage_live.py`
- `tests/master/test_fencing.py`
- `tests/master/test_live_postgres.py`
- `tests/master/test_notebook_entrypoint.py`
- `.codex/lanes/C1/RESULTS.md`
