# CORE-V2 results

## Scope

- Lane: `CORE-V2`
- Requirement IDs: `R02-R06`
- Base SHA: `491b2ba55b8c7ec30fbcc97a9839ad874fbdeba0`
- Implementation SHA: `12d58ace7e3c1bded2119fb25234efb18a719ad5`
- Branch: `agent/voice-intake-v2/core-v2`
- Effort: high; the durable ledger, authentication, provider accounting and restart ambiguity boundaries received negative-path tests.

## Delivered

- Added authenticated `/voice-intake/v2` capabilities, durable create, bounded M4A upload,
  asynchronous complete and frozen status contracts. Every produced v2 JSON response and
  typed error envelope carries `api_version: 2.0`; typed errors carry code, retryability,
  retry timing and reconciliation markers.
- Added a private SQLite WAL ledger and session spool with `0700` directories, `0600`
  database/artifact files, full synchronous durability, temp+fsync+atomic rename upload,
  immutable-metadata idempotency, SHA and timeline conflict detection, explicit same-complete
  retry, one lease/CAS worker, restart recovery and a configurable seven-day-or-longer TTL.
- Upload-before-create is a typed `409` and does not call media or inference providers.
  Upload performs bounded actual-byte SHA and bounded ffprobe validation for MP4/M4A,
  AAC-LC, mono, 16 kHz and duration. Conflict cleanup cannot leave the newly finalized
  unreferenced M4A.
- Complete durably verifies contiguous receipts/manifests and queues before returning HTTP
  202. No inference runs in the request. The worker revalidates every file, performs one
  bounded ffmpeg concat/normalization to mono 16 kHz 32 kbit/s MP3, then invokes exactly
  one aggregate transcription stage and one text-only summary stage.
- Added a physical requester-based Gemini Flash-Lite adapter. Each stage performs shared
  limiter preflight/reserve/key selection/mark-sent, one and only one generateContent POST,
  and finalization. Transcription reserves `ceil(recorded_audio_seconds * 32)` rather than
  wall time or full model TPM. Summary fails before send rather than under-reserving.
  Quota denial before send creates zero provider POST; marked-sent transport ambiguity is
  fenced and never silently retried; known 429 is reported and requires an explicit retry.
- Transcript and summary JSON, distinct physical request UIDs and limiter receipts are
  atomically durable. Retry derives the next stage from durable artifacts, so summary or
  publication retry cannot repeat transcription.
- Exposed a frozen `PublicationProjection` containing Android provenance, timing, ordered
  transport chunk receipts, model, pinned terminology snapshot, transcript, summary,
  request UIDs and limiter receipts. Worker order is publish/readback -> persist verified
  GitHub receipt -> purge audio/normalized -> final `published_verified`.
- Added `attach_configured_voice_intake_v2()` to construct the real store/media/inference/
  single-worker runtime while composing, not replacing, the existing FastAPI lifespan.
  The generic unconfigured route attachment fails closed when v2 is enabled without a
  worker. The compose control-plane gains one dedicated writable spool mount while keeping
  the read-only root and adding no service, Redis or PostgreSQL.
- Preserved v1 modules and their request/response/state behavior; only the existing runtime
  assembly receives the parallel v2 router.

## Verification

Commands executed against implementation SHA `12d58ac`:

```text
uv run --extra dev pytest -q tests/voice_intake tests/voice_intake_v2
33 passed

uv run --extra dev pytest -q
PASS at 100%; four existing environment-gated skips and two existing
jsonschema.RefResolver deprecation warnings observed

uv run --extra dev ruff check \
  src/my_data_hub/voice_intake_v2 src/my_data_hub/voice_intake/runtime.py \
  tests/voice_intake_v2
All checks passed!

uv run --extra dev mypy
Success: no issues found in 15 source files

uv run --extra dev python scripts/validate_repository.py
4751 checks, zero errors/notes

uv run --extra dev python -m compileall -q src tests
PASS

git diff --check
PASS
```

Focused evidence includes real ffmpeg/ffprobe AAC-LC/M4A validation and normalization,
durable reopen/idempotency/conflict, worker lease/TTL/ambiguous restart fencing, catch-all
route ordering, lifespan composition, zero-provider upload, exact two physical requester
POSTs, `audio/mpeg`, exact 32-token/second reserve, distinct durable request UIDs and purge
only after verified publication receipt persistence.

## Integration dependency / risks

- The separately owned PUBLISH-V2 lane must implement `RuntimePublisher` and root integration
  must switch production assembly from generic `attach_voice_intake_v2_routes()` to
  `attach_configured_voice_intake_v2(..., publisher=...)`. Until then an enabled v2 fails
  closed with `voice_intake_v2_worker_unavailable`; it never advertises false readiness.
- The deploy lane owns adding ffmpeg/ffprobe to the image, creating the host spool directory,
  its `0700` ownership, edge routing and live rollout. Compose uses internal
  `MY_DATA_HUB_VOICE_INTAKE_V2_SPOOL=/voice-intake-v2` and host mount source
  `MY_DATA_HUB_VOICE_V2_SPOOL_DIR`.
- No live Gemini, GitHub, deployment, credential or production audio operation was performed
  in this isolated implementation lane. Live exactly-two-POST and purge evidence remains an
  integration/deploy acceptance responsibility.

## Changed files

- `.codex/lanes/CORE-V2/RESULTS.md`
- `compose.control-plane.yaml`
- `src/my_data_hub/voice_intake/runtime.py`
- `src/my_data_hub/voice_intake_v2/{__init__,api,contracts,inference,media,runtime,settings,store,worker}.py`
- `tests/voice_intake_v2/{__init__,conftest,test_api,test_inference,test_media,test_store,test_worker}.py`

