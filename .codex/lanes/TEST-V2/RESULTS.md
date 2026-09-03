# TEST-V2 lane results

## Scope

- Lane: `TEST-V2`
- Requirements: `R01` (preserve `/voice-intake/v1`) and `R08` (mandatory v2 validation matrix)
- Base SHA: `f72f1122afc0587fa14469ed1a42cd83ae49e355`
- Implementation head SHA: `2772de48da890b7437055004dec69e792b3f1478`
- Branch: `agent/voice-intake-v2/test-v2`
- Worktree: `/home/dev/.codex/worktrees/my-data-hub/voice-intake-v2-tests`

## Result

### R01 — Done

The v1 implementation was not edited. The focused regression run includes the complete
legacy v1 suite (`tests/voice_intake/test_voice_intake.py` and
`tests/voice_intake/test_usage_fallback.py`), including WAV upload and complete/readback
behaviour. All v1 tests pass alongside v2.

### R08 — Done for repository validation; live acceptance remains integrator-owned

Closed the discovered safety/correctness gaps and added regression coverage:

- unverified `receiving`, failed, and reconciliation-required sessions retain their
  recoverable audio past the seven-day TTL; TTL never authorizes pre-readback deletion;
- server audio purge is fail-closed, checks that both chunk and normalized directories
  are absent, and cannot set `server_audio_purged=true` after a failed deletion;
- exact chunk repeats remain idempotent after complete and process restart, while changed
  receipts remain conflicts;
- complete validates the wall-time accounting equation with the existing 2-second timing
  tolerance;
- strict mypy now checks every v2 source module; all resulting real annotations and the
  v2 terminology-resolver override boundary were corrected;
- real ffmpeg/ffprobe tests create AAC-LC M4A, accept mono 16 kHz, reject invalid bytes,
  and reject wrong channel/sample-rate media without skips;
- two real transport chunks flow into one aggregate transcription and one summary, with
  two distinct durable request UIDs and no `countTokens` call;
- a durable transcript survives a summary failure and the explicit retry does not replay
  transcription; GitHub retry likewise does not replay either provider stage;
- quota pre-send performs zero physical provider POSTs; provider 429 and timeout each
  perform exactly one POST with no hidden retry and correct limiter/fencing evidence;
- upload traversal/oversize/temp cleanup, concat-path rejection, ffmpeg timeout/kill/temp
  cleanup, lease fencing, restart, manifest conflict, and safe-log assertions pass;
- deployment assertions prove the committed image installs and verifies ffmpeg+ffprobe,
  only control-plane receives the dedicated v2 spool bind, root remains read-only, and
  installer source enforces non-symlink/private owner mode 0700 and emits the spool env.

## Evidence

Host media tools used by the real fixture tests:

- `ffmpeg version 7.0.2-static`
- `ffprobe version 7.0.2-static`

Commands executed from the lane worktree:

1. Focused matrix:
   `pytest -q tests/voice_intake_v2 tests/voice_intake tests/test_control_plane_deployment.py`
   — PASS, 86 tests.
2. Full repository suite at implementation head:
   `pytest`
   — PASS, `1693 passed, 4 skipped, 2 warnings` in 144.10s. The skips are existing
   environment-gated repository tests, not v2 media skips.
3. `ruff check .` — PASS.
4. `mypy` — PASS, `26 source files` checked under strict mode.
5. `python -m compileall -q src tests` — PASS.
6. `python scripts/validate_repository.py` — PASS, `4809` checks and zero errors.
7. `git diff --check` — PASS.

## Changed files

- `pyproject.toml`
- `src/my_data_hub/voice_intake_v2/api.py`
- `src/my_data_hub/voice_intake_v2/contracts.py`
- `src/my_data_hub/voice_intake_v2/inference.py`
- `src/my_data_hub/voice_intake_v2/markdown.py`
- `src/my_data_hub/voice_intake_v2/publisher.py`
- `src/my_data_hub/voice_intake_v2/runtime.py`
- `src/my_data_hub/voice_intake_v2/store.py`
- `tests/test_control_plane_deployment.py`
- `tests/voice_intake_v2/test_api.py`
- `tests/voice_intake_v2/test_inference.py`
- `tests/voice_intake_v2/test_media.py`
- `tests/voice_intake_v2/test_publisher.py`
- `tests/voice_intake_v2/test_store.py`
- `tests/voice_intake_v2/test_worker.py`
- `.codex/lanes/TEST-V2/RESULTS.md`

## Risks and remaining acceptance work

- Live deploy, real Android-compatible multi-M4A upload, real Gemini two-POST receipts,
  IdeaHub atomic commit/current-main readback, and server-spool purge proof require the
  integrator's production acceptance lane; this lane does not claim them.
- The handoff says `vad` must be null for `continuous_v1`, while the frozen Pydantic
  contract currently allows optional provenance metadata in that mode. The user prompt
  only says nullable, so this lane did not silently tighten the API. Documentation and
  contract wording must be reconciled by the integrator.
- Recoverable sessions are intentionally not TTL-deleted before verified publication.
  This prevents data loss but means abandoned recordings require an explicit future
  owner-approved retention/cleanup policy rather than an unsafe automatic reaper.
