# H6 terminal carrier reconciliation — results

## Lane

- Lane ID: `H6-TERMINAL-CARRIER-RECONCILIATION`
- Requirement IDs: `FM08`, `FM10`, `FM11`, `FM24`
- Initial base SHA: `2ca00fa`
- Final prerequisite base SHA: `b8b09fd`
- Implementation head before this evidence-only commit: `d2eb270da114040a383dbbe5fb1b232220f70336`
- Implementation commits: `059a10d`, `d2eb270`

## Outcome

- FM10 uses only the validated durable `LeaseExpiryEvidence` control receipt and the
  control-owned, runtime-source-attested active-master carrier. It requires the exact
  `credentials_invalidated` proof and rejects terminal-output fields because the master
  remains ACTIVE.
- FM11 requires the validated `OldEpochEvidence`, exact distinct old/new operation,
  provider-run, and kernel identities, consecutive epochs, registry resolution to the
  replacement, and the persisted real `my-data-hub-master-terminal.json` hash quartet.
  It supplies the second `clean_rotation` lifecycle gate.
- FM24 uses only the validated durable `RotationSoakEvidence` and source-attested
  active-master carrier. It binds the exact heartbeat/read/checkpoint/recovery receipts
  and rejects terminal-output fields; it does not fabricate a provider rotation.
- FM08 remains honestly pre-action `BLOCKED` with `mutations_started=0`. The current
  `CallbackLossEvidence` lacks exact terminated/recovery provider run identities, and
  both driver and matrix reject attempting to derive an abrupt-master PASS from it.
- Reconciliation remains through the single control authority. This lane adds no local
  Kaggle adapter/token path, arbitrary driver argv, generic self-authored PASS, or live
  provider mutation.

## Validation

All commands ran in the isolated `h6-terminal-carrier` worktree with
`PYTHONPATH=src` where applicable and the repository virtual environment.

- Focused tests:
  - `pytest -q tests/provider/test_operational_kaggle_driver.py tests/provider/test_operational_kaggle_matrix.py`
  - PASS: 97 tests.
- Focused lint:
  - `ruff check scripts/provider/operational_kaggle_driver.py scripts/provider/operational_kaggle_matrix.py tests/provider/test_operational_kaggle_driver.py tests/provider/test_operational_kaggle_matrix.py`
  - PASS.
- Compile gate:
  - `python -m compileall -q src tests`
  - PASS.
- Repository/schema/notebook validation:
  - `python scripts/validate_repository.py`
  - PASS: 3,657 checks, zero errors, zero notes.
- Full lint:
  - `ruff check .`
  - PASS.
- Full test gate:
  - `pytest -q`
  - PASS (two existing `jsonschema.RefResolver` deprecation warnings only).
- Patch hygiene:
  - `git diff --check b8b09fd..HEAD`
  - PASS.

## Changed files

- `scripts/provider/operational_kaggle_driver.py`
- `scripts/provider/operational_kaggle_matrix.py`
- `schemas/provider/operational-kaggle-driver-request.v2.schema.json`
- `schemas/provider/operational-kaggle-driver-result.v2.schema.json`
- `schemas/provider/operational-kaggle-scenario-receipt.v1.schema.json`
- `tests/provider/test_operational_kaggle_driver.py`
- `tests/provider/test_operational_kaggle_matrix.py`
- `docs/operations/operational-kaggle-matrix.md`
- `.codex/lanes/H6-TERMINAL-CARRIER-RECONCILIATION/RESULTS.md`

## Residual risk/blocker

FM08 cannot PASS until the production durable receipt exposes exact, distinct old
terminated-master and recovery-master provider run/kernel identities. This lane does
not weaken that contract or mutate before the missing evidence exists.
