# Voice intake runtime

This package adds the bounded Record Idea Hub HTTP surface to the existing
lightweight my-data-hub control process. The Android phone remains the durable
owner of audio, transcripts and retries until GitHub readback succeeds.

The server never persists audio. It performs only three bounded operations:
transcribe a WAV chunk through Gemini Flash-Lite and the shared limiter,
synthesize the ordered transcript, and atomically publish the resulting source
packet/session registry entry to `onedayonemasterpiece/idea-hub`.

At session creation the runtime resolves the current `idea-hub/main` commit,
reads the bounded `config/voice-terminology.yaml` card at that exact commit,
and pins the commit/blob snapshot for every transcription and the final
synthesis. The bounded pin is mirrored into the existing control-ledger volume,
so the same session ID cannot silently re-pin after a container restart. There
is no global TTL or stale fallback; failure to load a current card fails session
creation, and missing or invalid durable pin state fails closed.
Successful publication records the exact card provenance and also regenerates
the stable chronological `inbox/voice/README.md` index and returns a
`/blob/main/...` navigation URL rather than trapping the Android client on an
immutable commit snapshot.
