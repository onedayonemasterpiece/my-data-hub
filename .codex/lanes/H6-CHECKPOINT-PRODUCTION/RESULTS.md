# H6-CHECKPOINT-PRODUCTION results

## Implemented

- Production `CheckpointAcceptanceJournal` on existing `ControlLedger`
  operations/effects; no migration.
- Fixed owner-bound `KaggleTaskOwnedCheckpointEffects` using one
  `KaggleProviderAdapter` and an existing local or remote checkpoint registry.
- FM05 empty package validation, exact private upload/readback, independent
  restore verifier and exact generation CAS/replay.
- FM14 deterministic task-owned corruption/readback rejection without HEAD
  mutation.
- FM15 fixed verifier continuation and strict failed-run receipt contract, plus
  an exact pre-mutation capability blocker because the current adapter cannot
  download output from a failed exact run.
- Deterministic provider intent IDs, reconcile-before-mutate behavior, bounded
  metadata receipts, exact task/operation/commit/resource bindings, absolute
  900-second budget propagation, and third-attempt terminal state.

The only new wire schema is the bounded metadata-only FM15 failed-verifier
receipt. The runtime binding remains owner-local provider configuration and is
deliberately not a remote caller payload.

## Evidence

Focused automated tests cover durable restart/replay, terminal receipt replay,
three-attempt terminal failure, conflicting identity rejection, injected
classification, and pre-mutation deadline rejection.

No live Kaggle result is asserted by this lane. A real run additionally needs
owner-configured official Kaggle credentials, remote control credentials, an
exact verified empty PostgreSQL 18 checkpoint template, and the reviewed
restore verifier source under `/kaggle/working`.

FM15 cannot honestly produce live evidence with the current adapter. Its
minimal missing seam is
`KaggleProviderAdapter.download_exact_failed_run_output_file`, bounded to one
top-level receipt and fenced to the exact numeric run before and after the
official Kaggle output call. The existing complete-run-only output method is
not reused or weakened.

## Gates

- `python -m compileall -q src tests`: PASS.
- `python scripts/validate_repository.py`: PASS, 3,392 checks, zero errors.
- `pytest`: PASS, 840 passed / 3 skipped.
- `ruff check .`: PASS.
- `mypy`: PASS.
- Disposable PostgreSQL 18.4 CI-equivalent bootstrap: PASS through role
  bootstrap, all migrations 0001..0016, `verify_postgres_bootstrap.py`, and
  `my-data-hub db verify`; canonical revision remained zero. The disposable
  container/network were removed with volumes afterwards.

The root Compose API image itself could not be used for the migration command:
its existing Dockerfile runs `pip install .` before copying the force-included
`sql/migrations` directory. This lane did not edit deployment/container files;
the same disposable PostgreSQL gate completed through the repository's exact CI
host-CLI sequence instead.
