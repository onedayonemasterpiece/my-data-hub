# 2026-09-03 Voice Intake v2 wall-clock overlap

Status: mitigated; permanent Android rollout pending

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

To be filled with the server commit/image, Android commit/artifact, affected
session GitHub commit/url, terminal status, and purge readback.
