# H6-FM15-FAILED-OUTPUT results

## Implemented

- Added `KaggleProviderAdapter.download_exact_failed_run_output_file` using
  only the injected official Kaggle 2.2.4 API.
- Added typed `KaggleKernelFailureOutputIdentity` with terminal `FAILED`, raw
  provider status limited to `failed|error`, exact run identity, receipt hash,
  and selective output hash.
- Fenced source/run/status before and after the official `kernels_output` call;
  an anchored top-level pattern, 64-KiB receipt cap, one-file result and exact
  post-download status equality are mandatory.
- Modelled the pinned SDK's response log honestly: when supplied, it writes
  `<kernel-slug>.log` independently of the anchored file pattern. The
  post-download residual is capped at 1 MiB, removed before return, and any
  other path or overrun deletes the destination and fails closed.
- Closed FM15 consumption: exact run/candidate/version/source commit/manifest
  hash/content hash/fixed failure code are required before registry rejection.
  Cancellation, generic failure, missing output, stale output and infrastructure
  failure remain failed acceptance attempts with HEAD unchanged.
- Corrected the FM15 disposable Notebook control class to `mcp_managed`;
  `mcp_exchange` remains Dataset-only by provider policy.

## Evidence status

No live Kaggle execution is claimed. Focused fake/API tests exercise the exact
official SDK call shape, unconditional log behavior, anchored filtering,
pre/post status fence and negative statuses. They are contract evidence only.

## Gates

- Focused Ruff over all lane-owned Python files: PASS.
- `python -m compileall -q src tests`: PASS.
- `python scripts/validate_repository.py`: PASS, 3,468 checks, zero errors.
- `pytest`: PASS, 888 passed / 2 skipped.
- `mypy`: PASS, 5 source files.
- Disposable PostgreSQL 18.4 CI-equivalent bootstrap: PASS through role
  bootstrap, all migrations 0001..0017, `verify_postgres_bootstrap.py`, and
  `my-data-hub db verify`; canonical revision remained zero. The disposable
  container/network and volumes were removed afterwards.

Full `ruff check .` is blocked only by the inherited `RUF022` ordering in
`src/my_data_hub/acceptance/__init__.py` at base `4845381`. That unrelated file
is outside this lane and is already fixed at integration commit `a546ec8`;
focused Ruff for every Python file changed here passes.

The root Compose API image itself cannot run the migration command because its
existing Dockerfile runs `pip install .` before copying the force-included
`sql/migrations` directory. This lane does not own deployment/container files;
the same disposable PostgreSQL gate passed through the repository's exact CI
host-CLI sequence instead.
