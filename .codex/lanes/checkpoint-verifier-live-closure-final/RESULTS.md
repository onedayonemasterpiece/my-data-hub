# CHECKPOINT-VERIFIER-LIVE-CLOSURE-FINAL results

## Scope

- Requirement: C1 production checkpoint verifier live closure.
- Base SHA: `35866f9b9d1e46e0f47c340d5816d1b18a678f80`.
- Effort/risk: extra-high security and provider-evidence lane.
- Authorized scope exception: `src/my_data_hub/control_plane/runtime.py` and its focused runtime wiring test were changed only to project `KaggleCheckpointVerifierAssets` from already-verified `KaggleMasterLaunchAssets` plus the durable deterministic asset claim/effect/receipt.

## Delivered evidence

- One injected central `KaggleProviderAdapter` pushes a private, internet-disabled verifier Notebook with immutable image digest and `original` pinning.
- The push attaches exactly the numeric runtime asset Dataset and exact numeric checkpoint Dataset.
- Rendered bootstrap recursively discovers normalized Kaggle mounts within file-count/per-file/total-byte bounds, rejects symlinks, ambiguous hash matches, mixed mounts, and mismatched checkpoint manifests, and installs the hash-pinned wheel/PostgreSQL runtime.
- The isolated restore validates PostgreSQL 18, `vector`/`pgcrypto`/`citext`/`pg_trgm`, exact append-only repository migration history, singleton/constraint invariants, a cosine vector query, and bounded allowlisted reads.
- Canonical receipt v2 is strict, metadata-only, and binds provider numeric run, executable source, selected receipt, selected output tree, execution pins, image/source commit, both numeric inputs, checkpoint manifest/tree, and live DB observations.
- Real `KaggleProviderAdapter` fake-API tests prove exact SaveKernel metadata and reject missing/wrong evidence without a GetKernel dependency after a complete SaveKernel response.
- Production projection rejects absent/stale/mismatched master asset authority and uses the same adapter instance; no caller numeric ref is accepted.

## Validation

- `uv run pytest -q tests/provider/test_checkpoint_runtime_wiring.py tests/master/test_checkpoint_verifier_notebook_runtime.py tests/master/test_physical_restore.py tests/control/test_control_runtime_wiring.py -k 'checkpoint or verifier or production_builder'` — PASS (`30 passed`).
- `uv run python scripts/validate_repository.py` — PASS (`4352` checks, no errors/notes).
- `uv run python -m compileall -q src tests` — PASS.
- `uv run ruff check .` — PASS.
- `uv run pytest -q` — PASS (full suite; two pre-existing `jsonschema.RefResolver` deprecation warnings).
- `git diff --check` — PASS.

## Risks / residuals

- No live provider mutation was performed in this lane, as required. A real Kaggle run is still needed for live operational evidence; tests prove the official adapter contract but do not fabricate readiness.
- The older standalone checkpoint acceptance runtime constructs verifier assets without the new exact runtime fields and therefore now fails closed if invoked through that legacy path. Production broker/acceptance assembly is wired to the verified asset projection; migrating that standalone deployment contract is outside this lane's authorized runtime wiring exception.

## Changed files

- `src/my_data_hub/checkpoints/kaggle_runtime.py`
- `src/my_data_hub/checkpoints/restore_probe.py`
- `src/my_data_hub/checkpoints/verifier.py`
- `src/my_data_hub/checkpoints/verifier_runtime.py`
- `src/my_data_hub/control_plane/runtime.py` (authorized minimal projection)
- `schemas/checkpoint-restore-verified-receipt.v2.schema.json`
- `docs/operations/checkpoint-verifier-live-closure.md`
- `tests/control/test_control_runtime_wiring.py` (authorized focused wiring test)
- `tests/master/test_checkpoint_verifier_notebook_runtime.py`
- `tests/master/test_physical_restore.py`
- `tests/provider/test_checkpoint_runtime_wiring.py`
- `.codex/lanes/checkpoint-verifier-live-closure-final/RESULTS.md`

## Head SHA

Populated after commit.
