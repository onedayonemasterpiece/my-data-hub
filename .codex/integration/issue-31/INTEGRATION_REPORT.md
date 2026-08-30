# Issue 31 integration report

## Source authority

- Authoritative deployed source lineage: `7efd017e7fc65373e8063857c52afa1458b38197`.
- Production image observed before rollout: tag
  `my-data-hub-control-plane:7efd017e7fc65373e8063857c52afa1458b38197`,
  image `sha256:de8f8dd97a1c0c280d6ed7e878e74dab85edeafeced63f6f14c28421d6426721`.
- Read-only installed-package comparison: all 11 tracked Voice Intake v2 Python
  files match the deployed source commit.
- PR base: `feat/record-idea-hub-voice-intake-v2` at
  `804ec60d5da12790bd9ae9b016270e417169eff8`; stale `main` was not used to
  reconstruct Voice Intake v2.

## Integrated safety invariants

1. Physical purge is impossible without immutable content-verification,
   publication, purge-authorization, and audio-purge receipts bound together.
2. GitHub exact/current-main readback is only publication evidence and cannot
   authorize deletion.
3. Each original source chunk has an independent bounded provider attempt and
   accepted receipt containing source/input hashes, source and coverage ranges,
   finish reason, schema version, provider request identity, and bounded usage.
4. Only exact `STOP`, complete range coverage, and independently recomputed
   duration-normalized plausibility can create an accepted/content receipt.
5. `MAX_TOKENS`, malformed or short schema-valid data, missing/unknown finish,
   ambiguity, missing chunks, range gaps/overlaps, and corrupted evidence fail
   closed with source audio retained.
6. The aggregate transcript is deterministic from ordered accepted receipts;
   summary and full-transcript publication are blocked until full coverage.
7. Publication verification, content verification, purge authorization, and
   physical purge are separate durable states. Old Android aliases remain false
   until verified physical deletion.
8. Successful durable segment/content/summary/publication receipts are reused
   after restart. Genuinely in-flight provider ambiguity is fenced for explicit
   reconciliation rather than replayed.
9. Existing ledgers migrate additively and idempotently. Historical GitHub
   publication does not backfill content verification; metadata-only audit rows
   contain bounded counts and no identifiers or content.
10. Schema-failure logs contain only bounded shape diagnostics and no session
    identifier, raw model response, request body, credentials, audio, transcript,
    summary, or terminology.

## Validation at integrated source

- Real-media Issue 31 acceptance: `17 passed` in the lane run; final Voice v2
  suite after review fixes: `114 passed`.
- Full repository: `1770 passed, 4 skipped`, with two pre-existing
  `jsonschema.RefResolver` deprecation warnings.
- Migration/secret/store targeted gate: `26 passed`.
- `python -m compileall -q src tests scripts`: PASS.
- `ruff check .`: PASS.
- configured `mypy`: PASS, 27 source files.
- `scripts/validate_repository.py`: PASS, 4,853 checks, zero errors/notes.
- `scripts/scan_tracked_secrets.py`: PASS.
- `git diff --check`: PASS.
- Independent checklist review: no remaining critical/high source defect.

The long-session acceptance uses seven physical synthetic AAC/M4A files totaling
1,207,620 ms, real ffmpeg/ffprobe, the real media/store/worker/parser boundaries,
and an offline bounded scripted provider. It covers retained-audio and complete
N+1 success paths without production/private data.

## Rollout and rollback gate

Production has not been reloaded by this report. The immutable current image and
unit remain the rollback target until the PR and CI pass and an explicit
production-change confirmation is received.

Before rollout:

1. snapshot the current user unit, exact Compose inputs, image ID, Voice v2
   SQLite database including WAL/SHM, and spool file manifest without reading or
   printing private content;
2. prepare the new immutable release/image without changing `current`, systemd,
   containers, routes, or secrets;
3. validate the image with disposable fail-closed and complete-flow canaries;
4. install/reload only after the explicit production confirmation.

Rollback restores the attested old unit/Compose inputs and immutable image, but
never downgrades the migrated ledger and never deletes or rewrites receipts,
unfinished server audio, or client state. Forward recovery resumes from durable
receipts. Production deployment, public probes, and the live synthetic long
canary remain explicit post-PR gates rather than being fabricated as completed.
