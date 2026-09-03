# Record Idea Hub Android 1.1 — Voice Intake API v2 contract

Status: **authoritative Android handoff for API 2.0**. Earlier Voice Intake v2
prompts are superseded by this document. This contract is additive: Android
1.1 uses `/voice-intake/v2`; the installed Android 1.0 client continues to use
the unchanged `/voice-intake/v1` contract.

## Endpoint and authentication

Intended production base URL:

```text
https://mcp-datahub.kenigevents.ru/voice-intake/v2
```

Every route requires the current single-device credential:

```http
Authorization: Bearer <device-token>
```

Every JSON success and error response contains `"api_version":"2.0"`.
Requests and responses use UTF-8 JSON unless the upload route says otherwise.
The client must use a stable `session_id` matching
`voice-YYYYMMDD-HHMMSS-xxxxxxxx`; it must never derive a server path from user
input.

The deployed image/source SHA and live-readback receipt are operational
evidence, not contract constants:

- deployed my-data-hub SHA: `455f5a836eba29544c5f533f3f173f7639107914`
- deployed image digest:
  `sha256:56c09e25940ab43defac8e2289e3be4b09ee5d868c3e178f46a1445bcd248a3c`
- live v2 acceptance session/commit:
  `voice-20260828-163102-f5645802` /
  `fb142c92ff15b8bfaf22ae9e4983a83e273c9d36`; exact and current-main
  readback succeeded before audio purge; disposable-session closure is
  follow-up commit `54e5a26f856c4eebdccf7a8c3edcfcc01e9259de`

These values were filled only from immutable image/source and live GitHub
readback evidence; a branch name, mutable image tag, healthy container, or
unverified reported SHA would not be sufficient.

## Routes

### Capabilities

```http
GET /voice-intake/v2/capabilities
```

This endpoint performs no Google request.

```json
{
  "api_version": "2.0",
  "status": "ready",
  "accepted_audio": [{
    "container": "mp4",
    "codec": "aac_lc",
    "mime_type": "audio/mp4",
    "sample_rate_hz": 16000,
    "channels": 1,
    "target_bitrate_bps": 32000
  }],
  "capture_policies": [
    "continuous_v1",
    "voice_activity_auto_pause_v1"
  ],
  "typical_gemini_requests": 2,
  "max_session_seconds": 3600,
  "max_session_bytes": 67108864,
  "server_audio_persistence": "temporary_until_github_readback"
}
```

`max_session_seconds` is configurable but must never be configured below
3,600 seconds. Twenty minutes is the priority exactly-two-request acceptance
case, not an API duration limit. `max_session_bytes` is the server's aggregate
admission bound across all transport chunks; Android must retain local files
when that bound is rejected and must not keep uploading unchanged data.

### Create or re-open a durable session

Every `SyncWorker` pass begins with this idempotent request, including passes
that are resuming uploads after process or device restart:

```http
POST /voice-intake/v2/sessions
Content-Type: application/json
```

```json
{
  "session_id": "voice-20260828-123456-abcdef12",
  "started_at": "2026-08-28T12:34:56+02:00",
  "timezone": "Europe/Kaliningrad",
  "device_label": "Samsung SM-G998B",
  "client_version": "1.1.0",
  "capture_policy": "voice_activity_auto_pause_v1",
  "audio_format": {
    "container": "mp4",
    "codec": "aac_lc",
    "mime_type": "audio/mp4",
    "sample_rate_hz": 16000,
    "channels": 1,
    "target_bitrate_bps": 32000
  },
  "vad": {
    "engine": "webrtc_vad",
    "engine_version": "pinned-by-client",
    "mode": 1,
    "frame_ms": 30,
    "config_version": "vad-auto-pause-efficient-v1"
  }
}
```

`vad` is required for `voice_activity_auto_pause_v1` and may be omitted or
`null` for `continuous_v1`; when a continuous-mode client still supplies it,
the server retains it only as provenance. All request fields are immutable
session metadata. The server durably commits the session and its
terminology/context identity before
responding. An exact repeat is successful and has `duplicate:true`; the same
`session_id` with any changed immutable value is a typed `409` conflict. The
receipt survives process and container restart.

Successful response:

```json
{
  "api_version": "2.0",
  "session_id": "voice-20260828-123456-abcdef12",
  "state": "receiving",
  "recording_finished": false,
  "duplicate": false
}
```

The returned state can be later than `receiving` when reopening an existing
session. A client must not try to reset it.

### Upload an independent M4A segment

```http
PUT /voice-intake/v2/sessions/{session_id}/chunks/{chunk_index}
Content-Type: audio/mp4
X-Chunk-SHA256: <lowercase hex SHA-256 of the exact body bytes>
X-Chunk-Duration-Ms: <positive integer>
X-Audio-Start-Ms: <non-negative integer>
X-Audio-End-Ms: <positive integer>
X-Wall-Start-Ms: <non-negative integer>
X-Wall-End-Ms: <positive integer>
```

`chunk_index` is zero-based. Each body is an independently playable MP4/M4A
file containing AAC-LC, mono, 16 kHz audio. Header names are HTTP
case-insensitive, but Android should emit the spelling above. The server hashes
the actual bytes and validates the container, codec, channel count, sample
rate, duration and bounded body before issuing a receipt.

```json
{
  "api_version": "2.0",
  "session_id": "voice-20260828-123456-abcdef12",
  "chunk_index": 0,
  "accepted": true,
  "duplicate": false,
  "sha256": "<64 lowercase hex characters>",
  "duration_ms": 240000,
  "size_bytes": 960000,
  "chunks_received": 1,
  "bytes_received": 960000
}
```

Success means temp write, `fsync`, atomic rename and durable ledger receipt
have completed. Retrying the same index with the same body SHA and identical
duration/audio/wall metadata returns the same receipt with `duplicate:true`.
A changed SHA or metadata at an existing index returns a typed `409` conflict.
Upload before durable create returns typed `409` and makes **zero** provider
calls. Every upload, including a duplicate, makes **zero** Gemini calls and
never returns a synthetic transcript.

Adjacent audio ranges remain exactly contiguous. Adjacent wall-clock ranges
may overlap by at most 50 ms (less than two 30 ms capture frames) because
Android derives them from wall-clock samples around blocking `AudioRecord`
reads. A larger overlap is an invalid complete manifest. This bounded clock
jitter never changes audio order, duration, hashes, or provider input.

### Finish recording

```http
POST /voice-intake/v2/sessions/{session_id}/complete
Content-Type: application/json
```

```json
{
  "ended_at": "2026-08-28T12:54:56+02:00",
  "wall_elapsed_ms": 1200000,
  "manual_pause_ms": 60000,
  "recorded_audio_ms": 420000,
  "auto_silence_skipped_ms": 720000,
  "chunk_count": 2,
  "chunks": [
    {
      "chunk_index": 0,
      "sha256": "<sha256-0>",
      "duration_ms": 240000,
      "audio_start_ms": 0,
      "audio_end_ms": 240000,
      "wall_start_ms": 0,
      "wall_end_ms": 600000
    },
    {
      "chunk_index": 1,
      "sha256": "<sha256-1>",
      "duration_ms": 180000,
      "audio_start_ms": 240000,
      "audio_end_ms": 420000,
      "wall_start_ms": 600000,
      "wall_end_ms": 1200000
    }
  ]
}
```

The manifest must contain exactly `chunk_count` entries with contiguous
indices `0..chunk_count-1`. Every SHA, duration and timeline value must match
its durable upload receipt. The server permits only its documented bounded
codec-padding tolerance; the client must not depend on that tolerance to repair
its own inconsistent timeline. `audio_end_ms - audio_start_ms` matches the
declared recorded duration, audio positions are contiguous, wall positions are
monotonic, and
`recorded_audio_ms + manual_pause_ms + auto_silence_skipped_ms` matches
`wall_elapsed_ms`. Missing indices return typed `409`.

The server durably closes the session and enqueues the bounded worker job before
returning `202`; the HTTP connection does not remain open for inference:

```json
{
  "api_version": "2.0",
  "session_id": "voice-20260828-123456-abcdef12",
  "state": "queued",
  "recording_finished": true,
  "duplicate": false
}
```

An exact repeat of the complete manifest is safe and returns the durable
current state with `duplicate:true`. A changed complete manifest is a typed
`409` conflict.

### Poll durable status

```http
GET /voice-intake/v2/sessions/{session_id}
```

Response schema:

```json
{
  "api_version": "2.0",
  "session_id": "voice-20260828-123456-abcdef12",
  "state": "transcribing",
  "recording_finished": true,
  "chunks_expected": 2,
  "chunks_received": 2,
  "bytes_received": 1680000,
  "recorded_audio_ms": 420000,
  "auto_silence_skipped_ms": 720000,
  "inference_batches_total": 2,
  "inference_batches_completed": 0,
  "gemini_requests_total": 2,
  "gemini_requests_completed": 0,
  "transcription_complete": false,
  "summary_complete": false,
  "github_verified": false,
  "server_audio_purged": false,
  "github_url": null,
  "github_commit_sha": null,
  "retryable": false,
  "retry_at": null,
  "error_code": null,
  "reconciliation_required": false
}
```

Nullable fields are present as JSON `null`; clients must not infer completion
from a missing field. The frozen state enum is:

```text
receiving
queued
normalizing
transcribing
summarizing
publishing
verifying
waiting_quota
retryable_error
reconciliation_required
published_verified
```

Only `published_verified` with `github_verified:true` and
`server_audio_purged:true` is terminal success. `reconciliation_required` is a
terminal manual-reconciliation condition, not permission to silently resend a
provider call. Other non-success states remain durable across restart.

## Error contract

The error envelope is stable:

```json
{
  "api_version": "2.0",
  "detail": {
    "code": "session_not_created",
    "retryable": false,
    "retry_after_seconds": null,
    "reconciliation_required": false
  }
}
```

The client branches on `detail.code`, never on prose. The minimum frozen code
set and HTTP mapping is:

| HTTP | `detail.code` | Meaning / client action |
|---:|---|---|
| 401 | `device_token_required`, `device_token_invalid` | Stop; configuration/authentication must be repaired. |
| 409 | `session_not_created` | Call idempotent create, then retry upload. No provider call occurred. |
| 409 | `session_metadata_conflict` | Do not overwrite; surface reconciliation. |
| 409 | `chunk_conflict` | Same index differs from its durable receipt; reconcile local ledger. |
| 409 | `complete_manifest_conflict` | Session was already closed with another manifest. |
| 409 | `chunks_missing` | Upload the missing contiguous indices, then repeat complete. |
| 409 | `complete_manifest_mismatch` | The close manifest differs from durable chunk receipts; reconcile locally. |
| 409 | `chunk_sha256_mismatch`, `session_not_receiving` | Do not overwrite server state; reconcile the local segment/session. |
| 413 | `request_too_large`, `session_size_limit_exceeded`, `session_audio_limit_exceeded` | Do not retry unchanged data. |
| 415 | `audio_content_type_invalid` | Encode the advertised capabilities format. |
| 422 | `session_invalid`, `chunk_metadata_invalid`, `audio_invalid`, `audio_probe_invalid`, `audio_container_invalid`, `audio_codec_invalid`, `audio_format_invalid`, `audio_duration_invalid`, `audio_duration_mismatch`, `complete_manifest_invalid`, `complete_time_invalid` | Correct local request/data before retry. |
| 503 | `voice_intake_disabled`, `voice_intake_v2_disabled`, `voice_intake_v2_worker_unavailable`, `spool_unavailable`, `terminology_resolver_unavailable`, `terminology_unavailable` | Preserve local files and retry only if `retryable:true`. |

An implementation may add more typed codes without changing these meanings.
It must never label a post-send ambiguous provider result as an ordinary
retryable error.

Gemini and GitHub work is asynchronous, so quota/provider/publication failures
are reported by `GET .../sessions/{session_id}` through `state`, `error_code`,
`retryable`, `retry_at`, and `reconciliation_required`; they are not fictional
HTTP 429/502/504 responses from `POST .../complete`. Pre-send quota codes are
`quota_exhausted_rpm`, `quota_exhausted_tpm`, or `quota_exhausted_rpd` and may
enter `waiting_quota`. Known sent failures include `provider_429` and
`provider_rejected_request`. `provider_timeout`, `provider_network_error`,
limiter-finalization ambiguity, and ambiguous GitHub outcomes require
reconciliation rather than a hidden retry.

## Retry and polling rules

1. Retain every local segment until status is `published_verified` and the
   server reports `server_audio_purged:true`.
2. Each `SyncWorker` pass first repeats `POST /v2/sessions`, then uploads only
   segments whose matching durable receipt is absent locally.
3. Retry create/upload/complete idempotently after connection loss. Query
   status instead of guessing whether complete advanced.
4. Honor `retry_at` and `retry_after_seconds`. Do not implement frequent
   WorkManager polling or upload loops; use bounded backoff and network
   constraints.
5. A quota or pre-send failure can resume safely. Once a request is marked
   sent, the server performs no hidden provider retry. An ambiguous outcome
   becomes `reconciliation_required`.
6. Summary retry never repeats a durable completed transcription. GitHub retry
   never repeats transcription or summary.

## Exactly two physical Gemini requests

For an ordinary successful session up to approximately 20 minutes, regardless
of transport-segment count, manual pauses or automatically skipped silence:

1. create and all uploads: zero Gemini requests;
2. complete queues work and returns: zero synchronous Gemini requests;
3. worker concatenates/normalizes all verified audio and performs exactly one
   Flash-Lite `generateContent` audio POST for the complete transcript;
4. after durably storing that transcript, it performs exactly one text-only
   Flash-Lite `generateContent` POST for the detailed structured summary;
5. publication, exact-commit/current-main readback and purge: zero Gemini
   requests.

Thus the happy path is exactly **two physical `generateContent` POSTs**. No
per-chunk calls, `countTokens`, model probe, or fallback POST is allowed. A
longer session may split only when provider TPM/body constraints genuinely
require it; automatically skipped silence reduces recorded duration and TPM,
not the normal request count.

## Battery-aware capture boundary

The backend does not perform VAD and does not treat client VAD metadata as
proof of audio quality. API v2 is deliberately not coupled to Silero, ONNX
Runtime, NNAPI or any neural runtime.

The recommended Android 1.1 detector is:

```text
adaptive conservative energy gate
-> native fixed-point WebRTC VAD
-> fail-open continuous capture on any detector failure
```

Required mobile semantics:

- capture from Android `AudioRecord` as 16 kHz mono before AAC-LC encoding;
- `voice_activity_auto_pause_v1`: automatic silence keeps the microphone
  listening while pausing encoder/file output;
- manual pause stops the microphone;
- `continuous_v1` is valid, including fail-open fallback, and correct audio
  must not be rejected merely because VAD was bypassed;
- VAD fields are provenance only; pin `engine_version` and `config_version` in
  the client release;
- no continuous neural inference is required or recommended for Android 1.1;
- prefer a hardware-accelerated AAC-LC encoder when the device reports one;
- rely first on the foreground microphone service and media-capture wake lock;
  do not own a manual partial `WakeLock` by default unless physical A/B testing
  demonstrates it is required;
- target one durable transport segment per 3–5 minutes of actually recorded
  audio, or at manual pause/finish, to reduce radio wakeups;
- animation and other UI behavior are outside the backend contract;
- avoid frequent WorkManager polling/upload loops.

Detector failure therefore loses only silence suppression, never the recording:
switch to continuous capture and describe it with `capture_policy`.

## Publication and deletion boundary

The server publishes one atomic `idea-hub/main` commit containing the source
packet, detail registry record and intake-session registry update. The packet
contains the client version, `api_contract: voice-intake-v2`, capture policy,
audio format, wall/manual/recorded/auto-silence durations, optional VAD
provenance, complete transcript and structured summary—never audio.

Server audio and normalized derivatives are purged only after both exact-commit
and current-main GitHub readback succeed. A small non-audio reconciliation
receipt may remain. Android deletes its local audio only after observing the
verified/purged terminal status.

## Short implementation brief for the Android agent

> In `onedayonemasterpiece/record-idea-hub`, implement Android 1.1 against the
> frozen Voice Intake API 2.0 contract in this file. Keep durable local
> recording/segment/receipt state; use AudioRecord 16 kHz mono, hardware-
> preferred AAC-LC/M4A at 32 kbit/s, the conservative energy-gate + native
> fixed-point WebRTC VAD with fail-open continuous capture, and no continuous
> neural inference. Make every SyncWorker pass begin with idempotent session
> create, upload durable 3–5-minute recorded-audio segments without frequent
> polling, send complete asynchronously, and retain local audio until status is
> both GitHub-verified and server-audio-purged. Do not add an app-owned partial
> WakeLock without physical A/B evidence. Preserve manual-pause versus
> auto-silence microphone semantics and prove restart/idempotency behavior on a
> physical device. Do not change the server contract.
