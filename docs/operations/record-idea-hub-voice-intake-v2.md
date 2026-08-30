# Record Idea Hub voice intake v2 operations

This runbook deploys and operates the additive, durable
`/voice-intake/v2` pipeline for Android 1.1. It does not replace or change
`/voice-intake/v1`; v1 remains the compatibility path for the currently
installed APK. The client-facing contract is frozen in
[`../handoffs/record-idea-hub-android-1.1-api-contract.md`](../handoffs/record-idea-hub-android-1.1-api-contract.md).
The authoritative deletion proof chain, migration behavior and fail-closed
rollback matrix are in
[`voice-v2-content-verification.md`](voice-v2-content-verification.md).

## Non-negotiable boundaries

- Extend the existing devstand control-plane only. Do not create another
  backend, Fly application, PostgreSQL database, Redis service or queue.
- Retain the read-only container root and a single-device Bearer token on every
  v2 route.
- Use one bounded spool worker with a durable lease/CAS per stage.
- Keep `gemini-3.1-flash-lite` and only explicitly registered Flash-Lite
  models.
- A successful recording uses one bounded transcription request for each
  source chunk, then one summary request only after durable contiguous
  coverage is verified. Already receipted successful chunks are never replayed.
- The configurable safety limit is at least 60 minutes. Twenty minutes is the
  priority acceptance duration, not a hard limit.
- Never log credentials, audio bytes, transcript text or summary text.

## Evidence fields

Fill these only from actual Git/image/runtime/GitHub readback evidence during
integration. A healthy container or mutable image tag is not sufficient.
The populated rows below are retained as pre-#31 rollout history. Aggregate
transcription and GitHub/purge readbacks in that history are not acceptance
evidence for the content-verification design and must not authorize deletion.

| Evidence | Verified value |
|---|---|
| reconciled authoritative v1 base SHA | `491b2ba55b8c7ec30fbcc97a9839ad874fbdeba0` |
| v2 deployed source SHA | `455f5a836eba29544c5f533f3f173f7639107914` |
| prior deployed image digest | `sha256:d3cbaa197f1d1b8b9e6180ca733af7428625d693c1908144a29834610dc01af4` |
| new deployed image digest and source attestation | `sha256:56c09e25940ab43defac8e2289e3be4b09ee5d868c3e178f46a1445bcd248a3c`, source/release `455f5a836eba29544c5f533f3f173f7639107914` |
| public v2 URL readback | authenticated `GET https://mcp-datahub.kenigevents.ru/voice-intake/v2/capabilities` = `200`, `status=ready`, `api_version=2.0`; the additive request topology must report `one_per_source_chunk` plus one post-coverage summary; unauthenticated = `401` |
| v1 live WAV regression receipt | session `voice-20260828-163612-28516afe`, transcription UID `514ccbb2-8c13-4ea8-adf7-19b5737ea5f0`, publication/readback `ed070f0fafd89f885b3e9aeba972f396a846abe1` |
| v2 live session ID | `voice-20260828-163102-f5645802`; two independent AAC-LC/M4A receipts survived a full control-plane restart and replayed as duplicates |
| transcription request UID / physical POST | `289c6883-db01-4c43-b705-38319b852395`; one durable aggregate transcription result |
| summary request UID / physical POST | `37cd6bc0-2cbe-46ae-9d19-ca972d0f2d1f`; one durable text-summary result |
| limiter reserve/sent/finalize receipts | both UIDs completed; transcription `reserved_tpm=266`, `actual_tpm=3045`; summary `reserved_tpm=23156`, `actual_tpm=3321`; shared limiter contract `google_ai_project_model_atomic_v1` |
| IdeaHub publication commit and exact/current-main readback | atomic four-file commit `fb142c92ff15b8bfaf22ae9e4983a83e273c9d36`; exact and then-current `main` blob `35fcb109d5ea43f674c1cb9b38a82b7b9002ef0f`; disposable v1/v2 sessions closed by follow-up `54e5a26f856c4eebdccf7a8c3edcfcc01e9259de` |
| historical pre-#31 purge readback | **not content-verification evidence**; GitHub readback and legacy purge flags must not be reused as authority for any new deletion |

Do not turn a reported checkpoint such as `68bb5bf...`, a branch head, Draft PR
number or image label into verified evidence without reconciling actual
container/image/source contents and ancestry.

## Reconcile deployed v1 before rollout

The production Android client was observed uploading without calling v1
session creation. After a backend restart, its first upload received
`voice_session_terminology_not_initialized`; a manual create then allowed the
upload and publication. Before building v2:

1. Fresh-read the running container/image/source and locate the implementation
   that emitted the observed error.
2. Compare that exact tree with GitHub and the reported deployed branch.
3. Preserve useful live fixes and make their authoritative ancestry auditable
   in GitHub; do not treat an image tag as content evidence.
4. Run complete v1 regression tests before and after the v2 merge.
5. Keep v1 request, response, state and error semantics unchanged.

V2 avoids this volatile-session failure by durably committing create metadata
and terminology/context identity before responding. Every Android 1.1
`SyncWorker` pass repeats the idempotent v2 create before upload.

## Durable spool

Mount a dedicated writable host directory into the existing control-plane at
`/voice-intake-v2`. It is temporary operational state, not canonical business
data:

```text
/voice-intake-v2/
  voice-intake-v2.sqlite3
  voice-intake-v2.sqlite3-wal
  voice-intake-v2.sqlite3-shm
  sessions/<session_id>/
    chunks/<zero-padded-index>.m4a
    normalized/<zero-padded-index>-<source-sha256>.mp3
    transcript.json
    summary.json
```

Use SQLite WAL or a simpler existing durable primitive with equivalent atomic
receipts. Required properties:

- host directory mode `0700`; regular files `0600`;
- read-only container root remains enabled;
- bounded request body and bounded total session size/duration;
- default aggregate transport bound 64 MiB, enforced atomically in the ledger
  independently of the per-chunk bound;
- upload writes a same-filesystem temp file, calls `fsync`, atomically renames,
  then durably records the receipt before returning success;
- hashes are computed from actual body bytes; session IDs and indices are
  validated before path construction, with no path traversal;
- ffprobe validates independent MP4/M4A, AAC-LC, mono, 16 kHz and duration;
- one bounded worker uses durable lease/CAS transitions so restart or duplicate
  wake-up cannot run the same stage concurrently;
- startup recovers expired leases and unfinished durable states;
- active/recent receipt TTL is at least seven days;
- audio and normalized derivatives remain until a durable content-verification
  receipt proves full ordered coverage, exact/current publication readback is
  durable, a separate purge-authorization receipt binds both facts, and
  filesystem absence is recorded in an audio-purge receipt;
- small non-audio reconciliation receipts may remain for idempotency/audit;
- no audio is added to Git, backups, logs or crash artifacts.

The upload route performs no transcription and no provider call. Restart after
create/upload/complete must preserve immutable metadata, receipts, close
manifest, terminology identity and completed inference artifacts.

## Worker stages and accounting

The frozen durable states are:

```text
receiving -> queued -> normalizing -> transcribing -> summarizing
          -> publishing -> verifying -> published_verified
```

The worker may enter `waiting_quota`, `retryable_error`, or
`reconciliation_required`. Every transition and lease must be committed before
external work begins or before success is exposed.

### Normalize each source chunk

Revalidate every M4A and its durable source receipt. Normalize each independent
source chunk to its own bounded MP3: mono, 16 kHz, 32 kbit/s. Clean temporary
files on error. Bind the exact source SHA, normalized-input SHA, source time
range and coverage range into that chunk's inference receipt.

### Transcribe by bounded source chunk

Perform exactly one structured audio request for each source chunk that lacks a
successful immutable receipt. Exact `STOP`, schema validation, full source
range coverage and bounded plausibility evidence are mandatory. `MAX_TOKENS`,
missing/unknown finish reason, malformed or short schema-valid output,
ambiguous outcome, missing receipt, source mismatch and any gap/overlap fail
closed with every audio file retained. A restart reuses accepted receipts and
does not replay their Gemini calls.

Assemble the complete transcript deterministically from accepted receipts in
chunk-index order. Persist a separate content-verification receipt containing
the source-manifest hash, ordered segment-receipt hash, transcript hash and
full contiguous range. A transcript-shaped JSON value without this receipt is
not complete content.

### Summarize once

Perform one text-only Flash-Lite request over the content-verified deterministic
transcript using the existing detailed structured schema. Atomically persist
`summary.json` before publication. Summary is forbidden while coverage is
incomplete; a summary retry must never repeat a receipted segment.

For each physical provider call retain the existing shared-limiter ordering:

```text
preflight
-> request-specific reserve
-> limiter-selected key
-> mark_sent
-> exactly one provider POST
-> finalize
```

Transcription reserve is `recorded_audio_seconds * 32`, not wall duration and
not full model TPM. Pre-send quota denial makes zero provider POSTs. After
`mark_sent`, there is no hidden retry: timeout or other ambiguous outcome
enters `reconciliation_required`. Provider 429 consumes exactly the one sent
attempt and is reported through the limiter. Summary retry repeats only the
summary; GitHub retry repeats neither inference stage.

## Completion, publication and purge

Complete verifies a contiguous manifest against durable receipts, including
SHA and timeline, then durably commits close plus queued job before returning
HTTP `202`. Repeating the same manifest is idempotent; changing it is a `409`.

Reuse the verified IdeaHub publisher. One atomic non-force update to
`idea-hub/main` creates:

```text
inbox/voice/YYYY/MM/<session_id>.md
registry/sessions/YYYY/MM/<session_id>.md
registry/intake-sessions.yaml
inbox/voice/README.md
```

The source packet records `client_version`,
`api_contract: voice-intake-v2`, capture policy, audio format, wall/manual/
recorded/auto-silence durations and VAD provenance when present. It stores the
transcript and summary, never audio. It may label a section
`Полная расшифровка` only when the independent content-verification receipt is
valid. Publication success requires both exact-commit and current-main
readback, but that receipt proves only publication durability. Purge requires
the content receipt, publication receipt and a separate durable authorization;
`server_audio_purged=true` is set only after physical absence is verified.

If publication outcome is unknown, reconcile by deterministic `session_id`;
do not create a second provider request or silently publish a divergent packet.

## Reverse proxy and deployment

Add only `/voice-intake/v2/` to the existing reverse proxy and existing
loopback control-plane listener. Retain `/voice-intake/v1/` unchanged. Require
TLS and Bearer authentication, bound request bodies/timeouts to the server
contract, and disable request/body logging. Do not expose `/internal/` or a
catch-all control-plane route.

Build, attest and deploy the existing control-plane through its established
deployment mechanism. Record the old image digest, new content digest, exact
Git SHA and container source/hash readback. Provision the private spool volume
before starting the new image. A rebuild/restart or healthy status is not
acceptance evidence by itself.

## Acceptance sequence

Run all current repository gates plus the following tests:

1. Reconcile deployed drift and prove the built v1 source ancestry.
2. Exercise durable create, exact repeat, immutable-metadata conflict, process
   restart and container restart.
3. Prove upload before create returns typed `409` with zero provider calls and
   that terminology/context is not lost after restart.
4. Test valid and invalid independent M4A, actual-byte SHA,
   receipt idempotency/conflict, oversized body, path traversal, ffprobe/ffmpeg
   timeout and temp cleanup.
5. Test complete manifest contiguity, missing indices, exact repeat and changed
   manifest conflict.
6. Test lease/CAS exclusion, expired-lease recovery and the seven-day minimum
   TTL behavior.
7. Assert N uploads make zero Gemini calls; processing makes at most one
   successful transcription POST per source chunk; full coverage is durable
   before exactly one summary POST; successful total is `N + 1`.
8. Assert restart, summary and GitHub retries do not repeat successful segment
   transcription; quota
   pre-send makes zero POST; provider 429/timeout makes one POST with no hidden
   retry; reserve uses recorded audio duration.
9. Assert exact/current GitHub readback alone cannot purge. Require the durable
   content-verification and purge-authorization receipts, then verify real
   source-file absence before exposing the legacy terminal purge flag.
10. Run full v1 regression, including a short live WAV smoke after rollout.
11. Verify no secret, audio, transcript or summary appears in logs.

After green CI, run a live v2 multi-M4A fixture through the public route and
capture safe evidence for:

- zero provider calls during all uploads;
- one successful immutable transcription receipt per source chunk, including
  source hash, range, exact finish reason and bounded coverage evidence;
- the single summary request UID/POST and its limiter receipt;
- exactly `N + 1` successful provider POSTs for N source chunks;
- atomic IdeaHub commit plus exact/current-main readback;
- a separate content-verification and purge-authorization receipt before
  `server_audio_purged=true`, plus verified absent server audio;
- all related containers healthy on the attested image;
- the unchanged v1 live WAV smoke.

No new APK is required for this server acceptance; the Android 1.1 build starts
only from the frozen, deployed and read-backed contract.

Exclude or close a disposable IdeaHub session only with a follow-up commit;
never rewrite history. Fill the evidence table above from these readbacks.

The 2026-08-28 rollout followed that rule: publication commits were retained,
and follow-up commit `54e5a26f856c4eebdccf7a8c3edcfcc01e9259de`
atomically marked both disposable v1/v2 sessions `excluded` /
`closed_with_exception`, reconciled their ledger counts, and updated the
chronological README. The current-main readback confirmed all six follow-up
paths.

## Android operational boundary

The server accepts `continuous_v1` as well as
`voice_activity_auto_pause_v1`. The recommended Android 1.1 chain is an
adaptive conservative energy gate followed by native fixed-point WebRTC VAD,
failing open to continuous capture. No neural VAD runtime is required. VAD
metadata is provenance, not trusted quality evidence.

Automatic silence keeps the microphone listening and pauses encoder/file
output; manual pause stops the microphone. Prefer hardware AAC when available.
Do not add an application-owned partial wake lock by default: first use the
foreground microphone service/media-capture lock, and add a fallback only
after physical A/B evidence. Avoid frequent WorkManager polling; target a
durable segment every 3–5 minutes of recorded audio or at manual pause/finish.
Every worker pass begins with idempotent v2 create.

## V2-only rollback

1. Disable v2 intake and remove/disable **only** the `/voice-intake/v2/`
   reverse-proxy route.
2. Restore the prior attested control-plane image while retaining the unchanged
   `/voice-intake/v1/` route and behavior.
3. Preserve the entire unfinished v2 spool read-only/in place for diagnosis and
   later forward recovery; do not purge it as part of rollback.
4. Verify v1 authenticated health and a short v1 WAV flow.
5. Record rollback image/source readback and the state of every unfinished v2
   session without logging payload data.

Rollback never deletes v1, rewrites IdeaHub history, or discards unfinished
v2 evidence.
