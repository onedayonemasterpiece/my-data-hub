# Issue 31 compatibility lane results

## Scope and revisions

- Lane ID: `issue31-compatibility`
- Requirements: R07, R09
- Base SHA: `9006791e5e3df7adf4021b0c44b702335846bff6`
- Implementation SHA: `b9548582519eafff73b722368e2f24725d9551b4`
- Documentation hardening SHA: `bb21ce28c0b1f19c497ca9a4b296ba59e3628cbd`

Writable scope used:

- `src/my_data_hub/voice_intake_v2/api.py`
- `src/my_data_hub/voice_intake_v2/markdown.py`
- `src/my_data_hub/voice_intake_v2/publisher.py`
- `tests/voice_intake_v2/test_api.py`
- `tests/voice_intake_v2/test_markdown.py`
- `tests/voice_intake_v2/test_publisher.py`
- `docs/operations/record-idea-hub-voice-intake-v2.md`
- `docs/handoffs/record-idea-hub-android-1.1-api-contract.md`
- `docs/operations/voice-v2-content-verification.md`
- this evidence file

Forbidden store, contract, inference and worker files were not edited. No
production/private audio, transcript, terminology, session data, credential,
Authorization value or API key was accessed or added.

## Delivered invariants

1. Capabilities retain the legacy scalar hint but add the authoritative
   `N source chunks + 1 post-coverage summary` topology and truthful retention/
   client-deletion policy.
2. The renderer and publisher both reject a projection without a syntactically
   valid content-verification receipt, a per-chunk descriptor, and a consistent
   ordered source manifest. Rejection happens before rendering or GitHub I/O.
3. `Полная расшифровка` is emitted only after that guard. The publication
   carries bounded source-segment provenance and the content receipt; it no
   longer claims aggregate transcription or readback-based retention.
4. The nullable legacy aggregate request UID remains present and is truthfully
   `null` for per-source-chunk transcription. Existing state strings and
   historical response fields remain unchanged.
5. The stale direct-purge API regression now proves `purge_not_authorized` and
   checks real source-file bytes still exist. Exact upload replay is idempotent
   and cannot resurrect or remove audio.
6. A GitHub publication receipt contains only publication facts. Tests prove an
   absent content receipt starts no GitHub operation; exact readback exposes no
   purge authority.
7. The Android handoff requires `client_audio_purge_allowed=true` for new
   clients and documents that the frozen legacy terminal triplet is withheld
   until verified physical deletion. GitHub readback alone is insufficient.
8. The design note maps every deletion precondition to durable evidence and
   includes migration, restart/failure, canary and fail-closed rollback matrices.
   Historical aggregate evidence is explicitly non-authoritative after #31.

## Verification evidence

Commands ran from
`/home/dev/.codex/worktrees/my-data-hub/issue31-compatibility`.

- `uv run --extra dev pytest -q tests/voice_intake_v2/test_api.py tests/voice_intake_v2/test_markdown.py tests/voice_intake_v2/test_publisher.py -o addopts=''`
  - PASS: `24 passed`
- `uv run --extra dev pytest -q tests/voice_intake_v2 -o addopts=''`
  - PASS: `93 passed`
- `uv run ruff check <six owned Python files>`
  - PASS
- `uv run mypy <three owned source files>`
  - PASS: no issues
- `uv run python -m compileall -q src tests`
  - PASS
- `uv run python scripts/validate_repository.py`
  - PASS: `4580` checks, zero errors
- `uv run python scripts/scan_tracked_secrets.py`
  - PASS
- `git diff --check`
  - PASS

An informational `ruff format --check` reported that six pre-existing compact-
style files would be reformatted. Formatting is not a configured repository
gate; broad formatting churn was intentionally not mixed into this safety fix.
Ruff lint, type checking and repository validation are green.

## Integration notes and residual risk

- Durability is established by the store before it constructs
  `PublicationProjection`; the renderer performs a second shape/topology guard
  but does not and cannot query SQLite independently. Keep the store projection
  gate and receipt triggers in the integrated diff.
- `typical_gemini_requests=2` remains solely for frozen one-chunk compatibility.
  New consumers must use the additive formula; status counters are dynamic.
- This lane did not run the complete repository suite or production canary.
  Those remain integration/deployment gates owned by the root integrator.
