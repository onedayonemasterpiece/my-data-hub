# 2026-09-03 Voice Intake v2 wall-clock overlap

Status: recovered; server correction deployed and Android 1.1.0-rc3 published;
device installation pending

## Impact

Voice session `voice-20260903-204457-368264f8` uploaded all three durable M4A
segments (458,310 ms total) but did not reach `complete`, transcription, or
IdeaHub publication. The source audio remained present in the private server
spool and on the phone.

## Evidence and root cause

- The durable ledger contained contiguous audio ranges `0..180000`,
  `180000..360000`, and `360000..458310` with all three hashes and files.
- The first two wall-clock ranges met at `180758` and `180751`, a 7 ms
  overlap. Android rejected its own completion manifest before making the
  HTTP request; repeated retries therefore stopped after the idempotent
  session-create call.
- The overlap is capture-clock jitter: each 30 ms audio frame is timestamped
  around a blocking `AudioRecord` read. It does not overlap or omit audio.
- A simultaneous showcase deployment repeatedly restarted the shared
  control-plane between 18:46 and 19:11 UTC. This exposed the retry path but
  did not delete any durable voice receipt.

## Correction and regression contract

- Accept at most 50 ms overlap between adjacent wall-clock ranges while
  retaining exact contiguous audio ranges and exact receipt matching.
- Reject 51 ms and larger wall overlaps.
- Android and server must use the same 50 ms boundary.
- Before closure, recover the affected session to `published_verified`, verify
  exact/current-main GitHub readback, and confirm server audio purge.
- A future deployment procedure must avoid repeatedly restarting the shared
  voice control-plane for showcase-only iteration.

## Release evidence

- Server commit, immutable release and control-plane image:
  `4265967302c696174454df845c6224d7555b6faf`; it is reachable from
  `origin/main`. After deployment all five compose services reported healthy
  and the authenticated runtime exported the 50 ms boundary.
- Recovery completion returned HTTP 202 and `queued`. The worker then advanced
  through `summarizing`, `publishing`, and `published_verified` without a
  retryable or reconciliation error.
- Final accounting: 3/3 chunks, 1,866,404 bytes, 458,310 ms recorded audio,
  two completed Gemini requests with distinct durable request UIDs,
  transcription complete, and summary complete.
- IdeaHub packet:
  <https://github.com/onedayonemasterpiece/idea-hub/blob/main/inbox/voice/2026/09/voice-20260903-204457-368264f8.md>
  at commit `7c811c04e12068f42ce4d87588ece00b66da84be`. Authenticated GitHub Contents
  readback succeeded both at that commit and at `main`; the publication commit
  was exactly the current `idea-hub/main` head at verification time.
- Ledger and filesystem readback: `github_verified=1`,
  `server_audio_purged=1`, `retryable=0`, no error or reconciliation flag, and
  neither the chunk nor normalized-audio directory remained.
- Android correction merged to `record-idea-hub/main` as
  `35fb24ee20ef2845824032793dedb60aee7ea6fa`. Main CI run
  <https://github.com/onedayonemasterpiece/record-idea-hub/actions/runs/33797105016>
  passed lint, unit tests, and APK assembly. Artifact
  `record-idea-hub-1.1-rc3-apk` (ID `9909691659`) contains
  `record-idea-hub-1.1.0-rc3-debug.apk`, SHA-256
  `051aa06be3140f3f7efcd4a877f4f81d7170cdfaff9674b9fceb8dd4d042e6fa`.

The affected session is fully recovered. Closure of the client-prevention
rollout still requires installing RC3 on the approved Android device; the host
had no connected ADB device during recovery, so no installation claim is made.
