# Voice intake runtime

This package adds the bounded Record Idea Hub HTTP surface to the existing
lightweight my-data-hub control process. The Android phone remains the durable
owner of audio, transcripts and retries until GitHub readback succeeds.

The server never persists audio. It performs only three bounded operations:
transcribe a WAV chunk through Gemini Flash-Lite and the shared limiter,
synthesize the ordered transcript, and atomically publish the resulting source
packet/session registry entry to `onedayonemasterpiece/idea-hub`.

Before transcription and synthesis, the runtime reads the bounded versioned
`config/voice-terminology.yaml` card from `idea-hub/main` and injects it into
both prompts. Successful publication also regenerates the stable chronological
`inbox/voice/README.md` index and returns a `/blob/main/...` navigation URL
rather than trapping the Android client on an immutable commit snapshot.
