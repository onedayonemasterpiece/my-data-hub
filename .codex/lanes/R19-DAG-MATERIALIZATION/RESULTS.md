# R19-DAG-MATERIALIZATION results

## Scope

- Requirement IDs: B03, B04, B05, B06.
- Base SHA: `771d68cd7841b02524aeeaa25eb6db737b8f3124`.
- Implementation SHA: `94bb25b`.
- Final lane SHA: recorded by the RESULTS-only commit containing this file.
- No live database or provider state was read or mutated.

## Delivered

- Append-only migration 0030 and schema revision 30.
- Deterministic PREPARE/reprepare materialization of E5/BGE, vector fusion,
  image scoring, final verifier, and writer work as dependencies become current.
- Immutable private stage inputs and UUIDv5 work identities with replay/no-duplicate
  validation.
- Exact `region-talk-vector-fusion-input.v1` score rows bound to the current E5 and
  BGE immutable result SHAs.
- Image execution only from a current candidate-associated, task-readable,
  acquisition-verified private artifact manifest; legacy media queue/evaluation
  rows do not make input available.
- Append-only owner/master runtime-pin registry with ACTIVE epoch/canonical revision
  binding, server-derived generations, exact replay, and controlled supersession.
- Direct submit verification of the canonical metadata/result hash, exact registered
  producer/runtime pin, stage-specific schemas, and bounded metrics. Empty generic
  success, arbitrary producer, and arbitrary result SHA are rejected.
- Publication and notification remain false; internal tables/helpers remain revoked
  from MCP and pipeline roles. Existing task/epoch/lease/worker-generation checks remain
  in the wrapped private worker path.

## Evidence

- Disposable PostgreSQL proof:
  `MDH_RUN_DISPOSABLE_POSTGRES=1 .../.venv/bin/pytest -q tests/region_talk/test_snapshot_integrity_postgres.py -x`
  -> `1 passed`.
  The proof migrates a fresh PostgreSQL instance, registers exact runtime pins, rejects
  a forged successful direct result, validates the private worker binding/rotation path,
  materializes all six stage inputs over repeated PREPARE cycles, verifies the exact
  vector score input and honest available image manifest, observes `6/6` unique work
  inputs, and finishes with all evidence `CURRENT` and stage receipt `COMPLETE`.
- Static/focused SQL contracts:
  `pytest -q tests/region_talk/test_dag_materialization_v9.py tests/region_talk/test_snapshot_current_state_v6.py tests/region_talk/test_private_stage_payload_v7.py tests/region_talk/test_stage_worker_rotation_v8.py`
  -> `19 passed`.
- Ruff:
  `ruff check tests/region_talk/test_snapshot_integrity_postgres.py tests/region_talk/test_dag_materialization_v9.py`
  -> passed.
- Compile:
  `python -m compileall -q tests/region_talk/test_snapshot_integrity_postgres.py tests/region_talk/test_dag_materialization_v9.py`
  -> passed.
- Repository validator:
  `python scripts/validate_repository.py`
  -> `4561` checks, `0` errors.
- `git diff --check` -> passed.

## Changed files

- `sql/migrations/0030_region_talk_dag_materialization.sql`
- `tests/region_talk/test_dag_materialization_v9.py`
- `tests/region_talk/test_snapshot_integrity_postgres.py`
- `docs/migrations/region-talk/mapping.md`
- `docs/migrations/region-talk/direct-snapshot-v2.md`
- `.codex/lanes/R19-DAG-MATERIALIZATION/RESULTS.md`

## Integration notes and risks

- R16 runtime discovery must consume the private `input_data.runtime_pin`, verify its
  receipt/pin preimages, and emit the exact server-computed `producer_exact_id` plus the
  cross-bound metric identity fields. The R16 owner confirmed alignment during this lane.
- Final-verifier and writer work is materialized only after exact immutable predecessor
  receipts. R20 may further enrich their private content/fact/source packs in its own
  migration/runtime lane; it must preserve the 0030 identity, pin, hash, and publication
  fencing rather than weaken them.
