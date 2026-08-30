# Issue #31 real-media acceptance lane

## Scope

This lane adds synthetic, provider-offline acceptance coverage only. It does not
access production, private audio, device credentials, provider credentials, or
the affected real session. The tests use locally generated silent AAC/M4A files,
the real `ffmpeg`/`ffprobe` media boundary, the real v2 store and worker, and the
real `AggregateGeminiInference` response parser with a bounded scripted
requester and fake limiter.

## Proven invariants

- A seven-chunk, `1,207,620 ms` manifest is backed by physical AAC/M4A files and
  processed per source chunk rather than as one aggregate request.
- A schema-valid but duration-implausible result, parseable or malformed
  `MAX_TOKENS`, an unknown finish reason, gap/overlap evidence, and an incomplete
  receipt set all retain every source file and keep content, publication, purge
  authorization, physical purge, and the legacy client purge gate closed.
- Failed attempts retain bounded finish/source/range evidence. A process loss
  after a failed attempt receipt but before retry-policy persistence is fenced
  after lease expiry and makes no hidden provider replay.
- Restarts after accepted segment receipt, content receipt, summary receipt,
  publication readback, purge authorization, and physical purge reuse successful
  provider/publication receipts.
- Exact GitHub readback without content verification cannot authorize deletion.
- A complete seven-segment flow creates contiguous content verification, makes
  exactly seven segment calls plus one summary call, publishes only afterward,
  durably authorizes purge separately, and only then physically removes source
  and normalized audio.
- A physical purge failure retains source files and keeps both new and frozen
  Android deletion gates closed.

Every fail-closed case asserts real `Path.exists()`/`Path.is_file()` evidence
before processing and after failure. The success case asserts physical absence
only after the durable content/publication/authorization chain completes.

## Validation evidence

- `.venv/bin/pytest -o addopts='' -q tests/voice_intake_v2/test_issue31_acceptance.py`
  — PASS, `17 passed in 8.16s`.
- `.venv/bin/pytest -o addopts='' -q tests/voice_intake_v2`
  — PASS, `112 passed in 9.07s`.
- `.venv/bin/ruff check .` — PASS.
- `.venv/bin/python -m compileall -q src tests scripts` — PASS.
- `.venv/bin/mypy --config-file pyproject.toml src/my_data_hub/voice_intake_v2`
  — PASS, `11 source files`.
- `.venv/bin/mypy --config-file pyproject.toml` — PASS, configured broad target,
  `26 source files` (mypy `1.20.2`).
- `.venv/bin/python scripts/validate_repository.py` — PASS, `4,581` checks,
  zero errors and zero notes.
- `.venv/bin/python scripts/scan_tracked_secrets.py` — PASS.
- `.venv/bin/pytest -o addopts='' -q tests/test_secret_scan.py` — PASS,
  `5 passed in 0.24s`.

## Commits

- Acceptance implementation: `1cfe900`
- This evidence document: recorded by its containing commit.
