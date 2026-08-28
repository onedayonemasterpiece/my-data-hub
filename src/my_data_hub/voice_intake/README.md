# Voice intake runtime

This package adds the bounded Record Idea Hub HTTP surface to the existing
lightweight my-data-hub control process. The Android phone remains the durable
owner of audio, transcripts and retries until GitHub readback succeeds.

The server never persists audio. It performs only three bounded operations:
transcribe a WAV chunk through Gemini Flash-Lite and the shared limiter,
synthesize the ordered transcript, and atomically publish the resulting source
packet/session registry entry to `onedayonemasterpiece/idea-hub`.
