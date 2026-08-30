# issue31-ledger results

## Scope

- Lane: `issue31-ledger`
- Requirements: R01, R02, R08, R10, R11
- Base SHA: `edc382f93157376532abc008a16440be652d0070`
- Implementation SHA: `8c0e89ce3ac9a0ef2fbf40db0451181aba540105`
- Writable files used:
  - `src/my_data_hub/voice_intake_v2/store.py`
  - `tests/voice_intake_v2/test_store.py`
  - `.codex/lanes/issue31-ledger/RESULTS.md`

No production session, private audio, transcript, terminology, device credential, or
API credential was accessed or added to the repository.

## Delivered invariants

1. Physical audio deletion requires an immutable content-verification receipt, an
   immutable purge-authorization receipt tied to the verified publication commit,
   and a durable audio-purge receipt recording filesystem absence.
2. GitHub publication readback alone sets only `publication_verified`; it cannot set
   `content_verified`, `purge_authorized`, or `audio_purged`, and cannot delete audio.
3. `publication_verified`, `content_verified`, `purge_authorized`, and `audio_purged`
   are independent durable facts. SQLite triggers reject forged terminal transitions
   that do not have the corresponding receipt chain.
4. Per-chunk provider attempts are immutable and idempotent. They retain source hash,
   source and coverage ranges, bounded finish metadata, and only accepted transcript
   hashes/content; failed attempts cannot carry transcript content.
5. Content verification deterministically assembles accepted receipts in chunk order
   and rejects missing chunks, non-contiguous source ranges, range/hash mismatch,
   incomplete coverage, or a non-`STOP` accepted finish reason.
6. Summary and publication projections require a durable content-verification receipt.
7. The migration is transactionally idempotent. Legacy publication/deletion facts are
   preserved as historical facts but never promoted into content verification or purge
   authorization. Previously deleted legacy rows are marked
   `legacy_unverified_purge`; the audit exposes bounded aggregate counters only.
8. Frozen status aliases fail closed: publication and physical purge facts come from
   the separated ledger fields, not from GitHub readback as deletion authority.

## Test evidence

Commands were run in
`/home/dev/.codex/worktrees/my-data-hub/issue31-ledger`.

- `uv run --extra dev pytest -q tests/voice_intake_v2/test_store.py -o addopts=''`
  - PASS: `14 passed`
  - Includes real synthetic chunk files and asserts they remain after GitHub-only
    readback, forged state, incomplete/MAX_TOKENS coverage, and filesystem deletion
    failure. The authorized success/crash path asserts physical files are absent only
    after the receipt chain is complete.
- `uv run ruff check src/my_data_hub/voice_intake_v2/store.py tests/voice_intake_v2/test_store.py`
  - PASS
- `uv run mypy src/my_data_hub/voice_intake_v2/store.py`
  - PASS: no issues
- `uv run python -m compileall -q src/my_data_hub/voice_intake_v2/store.py tests/voice_intake_v2/test_store.py`
  - PASS
- `uv run python scripts/validate_repository.py`
  - PASS: `4555` checks, zero errors
- `.venv/bin/python scripts/scan_tracked_secrets.py`
  - PASS
- `git diff --check`
  - PASS

An informational pre-integration run of the complete Voice v2 directory produced
`52 passed, 9 failed`. Those failures are expected dependency-boundary failures: the
base worker still uses the legacy aggregate-transcription lifecycle, and one base API
test directly invokes purge without authorization. This lane did not edit its forbidden
worker/API files. The orchestration and compatibility lanes must replace those call
sites before final full-suite validation.

## Migration and recovery evidence

- Opening the same migrated SQLite ledger twice creates one migration marker and does
  not duplicate or change receipts.
- A synthetic 1,005-row legacy ledger exercises the bounded 1,000-row metadata audit;
  `PRAGMA integrity_check` returns `ok`.
- Immutable receipt triggers reject update/delete mutation.
- A retry after a crash between physical deletion and receipt finalization completes
  the purge receipt without replaying provider inference or publication.

## Integration risks and required follow-up

- The orchestration lane must call per-segment receipt persistence, content verification,
  explicit purge authorization, physical purge, and terminal finalization in order.
- The compatibility lane must update the old direct-purge API fixture and expose the new
  status fields while preserving frozen Android behavior.
- The final integrator must rerun all Voice tests and the full suite after dependent lanes
  are merged; the isolated ledger branch is intentionally fail closed against the old
  aggregate worker.
