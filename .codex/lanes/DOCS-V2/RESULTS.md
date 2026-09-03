# Lane DOCS-V2 results

## Status

Committed; ready for integration.

## Requirement IDs

- R09 — authoritative Android 1.1 API v2 handoff.
- R11 — v2 operations, evidence and v2-only rollback contract.

## Ownership

Writable scope was limited to:

- `docs/handoffs/record-idea-hub-android-1.1-api-contract.md`
- `docs/operations/record-idea-hub-voice-intake-v2.md`
- `.codex/lanes/DOCS-V2/RESULTS.md`

No source, test, deploy, configuration or existing-document file was edited.

## Branch and revisions

- Branch: `agent/voice-intake-v2/docs-v2`
- Base SHA: `491b2ba55b8c7ec30fbcc97a9839ad874fbdeba0`
- Documentation head SHA: `27c1b37fd1185a000e3165bb538da4c22253a802`
- Final lane tip: the documentation-only commit containing this RESULTS file;
  resolve with `git rev-parse agent/voice-intake-v2/docs-v2`.

## Delivered contract

The Android handoff normatively freezes:

- authenticated v2 capabilities, create, M4A chunk upload, complete and status
  routes;
- request/response schemas, all six upload metadata headers, the full durable
  state enum, typed error envelope/codes, immutable-metadata conflicts and
  exact-repeat behavior;
- durable create/upload/complete receipts, container/process restart behavior,
  retry boundaries, post-send ambiguity handling and purge only after exact-
  commit/current-main readback;
- zero provider calls on create/upload and exactly one aggregate transcription
  plus one summary physical `generateContent` POST on the ordinary <=20-minute
  successful path;
- the battery-aware non-neural Android boundary: AudioRecord 16 kHz mono,
  conservative adaptive gate + fixed-point WebRTC VAD, fail-open continuous
  capture, distinct manual/automatic pause semantics, hardware-preferred AAC,
  no default app-owned partial WakeLock, bounded WorkManager activity and
  3–5-minute recorded-audio segments;
- the rule that every SyncWorker pass starts with idempotent v2 session create;
- a short implementation brief for the Android agent without editing the
  mobile repository.

The operations runbook freezes the minimal existing-control-plane spool,
durable stage/lease flow, provider accounting, publication/readback/purge
gates, deployment and acceptance evidence checklist, unchanged v1 boundary,
and v2-only rollback that preserves v1 and unfinished spool state.

All deployment SHA, image digest, live session, limiter/provider UID, IdeaHub
commit/readback and purge fields are explicit `PENDING INTEGRATOR ... EVIDENCE`
placeholders. No reported SHA, tag, branch, healthy container or synthetic
receipt was represented as live proof.

## Commands and evidence

- `git status --short --branch`
- `git rev-parse HEAD`
- `sed -n ... pasted-text.txt`
- `git diff --check` — PASS.
- Contract assertion script via `uv run python` — PASS, 25 required contract
  markers found across both documents.
- `uv run python scripts/validate_repository.py` — PASS: 4,511 checks, zero
  errors and zero notes.
- `uv run python scripts/scan_tracked_secrets.py` — PASS.
- `git diff --cached --check` — PASS before documentation commit.

The first local attempt used a nonexistent `python` executable; it performed no
validation. The same checks were rerun successfully with `uv run python`.

## Risks and integration notes

- The error codes and complete-202 acknowledgement are a normative draft
  because the source request specified their semantics but not every field
  name. The integrator confirmed that the v1-style safe error envelope and the
  documented acknowledgement are authoritative; implementation should conform
  to the handoff unless an objective constraint is identified and reconciled.
- The intended v2 production URL is documented, but no route availability,
  deployed SHA, request UID, IdeaHub commit or purge is claimed. Those remain
  integration/live acceptance work.
- This lane did not run source unit tests because it changed documentation only;
  repository validation and tracked-secret scanning passed.
- Cherry-pick documentation commit `27c1b37fd1185a000e3165bb538da4c22253a802`,
  then the following RESULTS-only commit. Do not drop implementation changes
  from other lanes when reconciling contract field names.
