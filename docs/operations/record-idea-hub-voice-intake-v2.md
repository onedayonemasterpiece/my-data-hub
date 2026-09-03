# Record Idea Hub voice intake v2 operations

This runbook deploys and operates the additive, durable
`/voice-intake/v2` pipeline for Android 1.1. It does not replace or change
`/voice-intake/v1`; v1 remains the compatibility path for the currently
installed APK. The client-facing contract is frozen in
[`../handoffs/record-idea-hub-android-1.1-api-contract.md`](../handoffs/record-idea-hub-android-1.1-api-contract.md).

## Non-negotiable boundaries

- Extend the existing devstand control-plane only. Do not create another
  backend, Fly application, PostgreSQL database, Redis service or queue.
- Retain the read-only container root and a single-device Bearer token on every
  v2 route.
- Use one bounded spool worker with a durable lease/CAS per stage.
- Keep `gemini-3.1-flash-lite` and only explicitly registered Flash-Lite
  models.
- A normal successful recording up to approximately 20 minutes uses exactly
  two physical Gemini `generateContent` POSTs: one aggregate transcription and
  one summary.
- The configurable safety limit is at least 60 minutes. Twenty minutes is the
  priority acceptance duration, not a hard limit.
- Never log credentials, audio bytes, transcript text or summary text.

## Evidence fields

Fill these only from actual Git/image/runtime/GitHub readback evidence during
integration. A healthy container or mutable image tag is not sufficient.

| Evidence | Verified value |
|---|---|
| reconciled authoritative v1 base SHA | `491b2ba55b8c7ec30fbcc97a9839ad874fbdeba0` |
| v2 deployed source SHA | `455f5a836eba29544c5f533f3f173f7639107914` |
| prior deployed image digest | `sha256:d3cbaa197f1d1b8b9e6180ca733af7428625d693c1908144a29834610dc01af4` |
| new deployed image digest and source attestation | `sha256:56c09e25940ab43defac8e2289e3be4b09ee5d868c3e178f46a1445bcd248a3c`, source/release `455f5a836eba29544c5f533f3f173f7639107914` |
| public v2 URL readback | authenticated `GET https://mcp-datahub.kenigevents.ru/voice-intake/v2/capabilities` = `200`, `status=ready`, `api_version=2.0`, `typical_gemini_requests=2`; unauthenticated = `401` |
| v1 live WAV regression receipt | session `voice-20260828-163612-28516afe`, transcription UID `514ccbb2-8c13-4ea8-adf7-19b5737ea5f0`, publication/readback `ed070f0fafd89f885b3e9aeba972f396a846abe1` |
| v2 live session ID | `voice-20260828-163102-f5645802`; two independent AAC-LC/M4A receipts survived a full control-plane restart and replayed as duplicates |
| transcription request UID / physical POST | `289c6883-db01-4c43-b705-38319b852395`; one durable aggregate transcription result |
| summary request UID / physical POST | `37cd6bc0-2cbe-46ae-9d19-ca972d0f2d1f`; one durable text-summary result |
| limiter reserve/sent/finalize receipts | both UIDs completed; transcription `reserved_tpm=266`, `actual_tpm=3045`; summary `reserved_tpm=23156`, `actual_tpm=3321`; shared limiter contract `google_ai_project_model_atomic_v1` |
| IdeaHub publication commit and exact/current-main readback | atomic four-file commit `fb142c92ff15b8bfaf22ae9e4983a83e273c9d36`; exact and then-current `main` blob `35fcb109d5ea43f674c1cb9b38a82b7b9002ef0f`; disposable v1/v2 sessions closed by follow-up `54e5a26f856c4eebdccf7a8c3edcfcc01e9259de` |
| server audio purge readback | status `published_verified`, `github_verified=true`, `server_audio_purged=true`; spool retains only `transcript.json` and `summary.json` for the acceptance session |

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
    normalized/session.mp3
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
- audio and `normalized/session.mp3` are deleted only after successful exact
  publication and current-main readback;
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

### Normalize

Revalidate every M4A. Run bounded, timeout-controlled ffmpeg concat/normalize
to a single MP3: mono, 16 kHz, 32 kbit/s. Clean temporary files on error.
Priority sessions up to 20 minutes are normalized into one provider audio
input. Longer sessions split only when genuine provider TPM/body constraints
require it.

### Transcribe once

Perform one structured, complete-transcript audio request and atomically
persist `transcript.json` before setting `transcription_complete=true` or
entering summary. Do not issue per-chunk requests, `countTokens`, session model
probes, or fallback POSTs.

### Summarize once

Perform one text-only Flash-Lite request over the durable transcript using the
existing detailed structured schema. Atomically persist `summary.json` before
publication. A summary retry must never repeat a completed transcription.

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
HTTP `202`. Audio ranges must be exactly contiguous. Adjacent wall-clock
ranges tolerate at most 50 ms overlap to cover one-frame Android wall-clock
jitter around blocking capture reads; larger overlaps remain invalid.
Repeating the same manifest is idempotent; changing it is a `409`.

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
transcript and summary, never audio. Publication success requires both
exact-commit and current-main readback. Only then purge chunk audio and the
normalized derivative, set `server_audio_purged=true`, and expose
`published_verified`.

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
7. Assert N uploads make zero Gemini calls; complete makes exactly one
   transcription POST; transcript is durable before exactly one summary POST;
   successful total is exactly two.
8. Assert summary and GitHub retries do not repeat transcription; quota
   pre-send makes zero POST; provider 429/timeout makes one POST with no hidden
   retry; reserve uses recorded audio duration.
9. Assert purge cannot happen before GitHub readback and does happen after both
   exact/current readbacks.
10. Run full v1 regression, including a short live WAV smoke after rollout.
11. Verify no secret, audio, transcript or summary appears in logs.

After green CI, run a live v2 multi-M4A fixture through the public route and
capture safe evidence for:

- zero provider calls during all uploads;
- the single transcription request UID/POST and its limiter receipt;
- the single summary request UID/POST and its limiter receipt;
- exactly two physical provider POSTs total;
- atomic IdeaHub commit plus exact/current-main readback;
- `server_audio_purged=true` and absent server audio;
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
