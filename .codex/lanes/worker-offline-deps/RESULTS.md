# Lane worker-offline-deps Results

## Status

Code complete; local deterministic gates pass. No provider mutation or provider-side
smoke was performed in this lane, so embedding worker admission intentionally remains
closed until the central adapter produces a verified receipt.

## Requirement closure

- **R-D1 — Done (artifact contract):** the canonical v170-bound lock selects exact
  FlagEmbedding 1.4.0, ir-datasets 0.5.11, psycopg 3.3.4 and matching CPython 3.12
  manylinux x86-64 psycopg-binary 3.3.4 wheels by PyPI file URL and SHA-256. The
  immutable v170 image digest supplies the large pinned ML dependency layer; the
  provider smoke recursively verifies its installed transitive versions and specifiers.
  The builder accepts only the exact private wheelhouse inventory and commits no wheel
  bytes.
- **R-D2 — Done:** the bundle contains a canonical dependency manifest, every exact
  overlay wheel, and the credential-free smoke runner. The builder and independent
  verifier reject missing, extra, symlinked, oversized or hash-mismatched files. E5/BGE
  generated assets hash-check and install each wheel separately with
  `--no-index --no-deps` before project imports, then compare the receipt's distribution
  versions with the running image.
- **R-D3 — Done (fail-closed handoff):** no normal build, PREPARE or devstand install
  pulls the approximately 9 GB official image. A bounded disposable-private-Kaggle
  runner emits an `imports_passed` observation only. It cannot self-author PASS. The
  final receipt schema binds the canonical observation hash, exact numeric provider run,
  official image/source/Python, dependency/project/all wheel hashes, distribution
  versions, private status, internet disabled and central-adapter verification. E5/BGE
  reject launch unless central execution pins deliver the exact receipt hash. Production
  central receipt assembly/durable admission wiring is deliberately left for Gate K.

## Branch / base

- Branch: `agent/operational-mvp/worker-offline-deps`
- Worktree: `/home/dev/.codex/worktrees/my-data-hub/worker-offline-deps`
- Base: `90ce252b75f385ce46bc3f2ecb5418967afc747c`
- Head: this lane's delivered commit (see merge handoff)

## Reproducible source contract

- Official Kaggle CPU v170 OCI identity:
  `gcr.io/kaggle-images/python@sha256:c1fa4de30bc268e601e6dcddb6ceb2519b9adde3527dbbfb05e6bdfbbbdcd1a2`
- Official image source commit: `fc61d5cda7da39530055bae9bd0e92865f995cd9`.
- Official release inventory is the source for the image-provided ML stack. PyPI release
  JSON/core metadata is the source for overlay filenames, hashes and requirements.
- `scripts/provider/assets/embedding-worker-wheel-lock.v1.json` is canonical JSON with
  exact `files.pythonhosted.org` URLs and SHA-256 digests; it contains no index range,
  `latest`, sdist or runtime resolver instruction.
- Psycopg uses the upstream matching-version binary-install contract: psycopg 3.3.4 and
  psycopg-binary 3.3.4 for CPython 3.12/manylinux2014 x86-64.

## Changed areas

- Provider asset builder/verifier and exact wheel lock.
- Credential-free provider smoke observation runner.
- Generated E5/BGE Notebook assets and generator admission/install logic.
- Master bundle plus smoke observation/receipt schemas and synthetic bundle example.
- Focused bundle/notebook tests and operations documentation.

Forbidden control-plane, central launcher, acceptance checkpoint, adapter and auth files
were not changed.

## Verification

- `uv run --extra dev ruff check ...` — passed.
- `uv run --extra dev pytest -q tests/provider/test_build_master_assets.py tests/test_notebooks.py`
  — `20 passed`.
- `.venv/bin/python scripts/validate_repository.py` — `3996` checks, zero errors.
- `.venv/bin/python -m compileall -q src tests scripts/provider scripts/create_notebooks.py`
  — passed.
- `.venv/bin/pytest -q` — passed (`1226` collected; `1223` passed, `3` skipped).

## Explicit non-evidence / risks

- No official Kaggle image was pulled locally: the host had only about 2.4 GiB free and
  the registry manifest was approximately 9.1 GB compressed. Local deterministic
  verification is not represented as exact-image import proof.
- No provider Notebook was launched and no final smoke receipt exists yet. The runner's
  observation is not accepted as a PASS receipt.
- Existing exact-revision model retrieval remains a separate network-enabled worker
  behavior. This lane closes dependency installation offline; it does not package model
  snapshots.
- No raw wheels, model bytes, credentials, private data or fabricated smoke evidence are
  committed.

## Merge notes

This lane is based before the unrelated `4e77449` stale-claim integration. Files are
disjoint by owner report. After cherry-pick, Gate K must use the existing single central
Kaggle adapter to run the private/no-internet smoke, validate the observation against its
schema and provider launch facts, durably write the final canonical receipt, and supply
its exact hash/path through E5/BGE execution pins. Until then, embedding launch failure is
the intended safe state.
