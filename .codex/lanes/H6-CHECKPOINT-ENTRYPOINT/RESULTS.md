# H6-CHECKPOINT-ENTRYPOINT results

## Implemented

- Added the fixed Kaggle-runtime-only invocation
  `scripts/provider/checkpoint_acceptance_evidence.py --config ... --output ...`
  for exactly FM05, FM14 and FM15.
- Added strict production config and operational result Pydantic contracts,
  JSON Schemas and sanitized examples. Config is mode `0600`, at most 256 KiB,
  contains metadata identities only and restricts every path below
  `/kaggle/working`. Exact numeric protected template/verifier Dataset refs and
  resource-claim hashes bind the launch assets without exposing their bytes.
- Added `build_production_checkpoint_acceptance_runtime`, which performs a
  modern runtime-token/checkpoint-HEAD authorization preflight before local
  journal creation or provider mutation, verifies exact template/verifier
  hashes, and admits only the official Kaggle adapter plus remote journal and
  checkpoint registry as live evidence.
- Bound replay to the same config hash, operation/task/candidate/provider effect
  IDs and fixed mode-0600 local control journal. The absolute operation deadline
  remains 900 seconds.
- Added bounded metadata-only `operational-result.json` outcomes:
  `LIVE_EVIDENCE_READY`/0, pre-mutation `BLOCKED`/78, or ambiguity-safe
  `FAIL`/1. READY embeds the exact durable receipt/hash and provider locator but
  deliberately does not claim matrix PASS.

## Evidence status

No live Kaggle execution is claimed. Automated tests use injected runtimes and
therefore prove only config/result, classification, and entrypoint contracts.

## Gates

- `python -m compileall -q src tests scripts/provider/checkpoint_acceptance_evidence.py`: PASS.
- `python scripts/validate_repository.py`: PASS, 3,491 checks, zero errors.
- `ruff check .`: PASS.
- `mypy`: PASS, 5 source files.
- `pytest`: PASS, 907 passed / 2 skipped.

Disposable PostgreSQL validation is pending as a follow-up gate; this lane has
no PostgreSQL migration or database-runtime changes.
