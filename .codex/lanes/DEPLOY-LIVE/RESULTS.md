# DEPLOY-LIVE results

Status: **completed** on 2026-08-28 UTC.

## Deployment attestation

- Reconciled v1 base: `491b2ba55b8c7ec30fbcc97a9839ad874fbdeba0`.
- Deployed v2 source/release: `455f5a836eba29544c5f533f3f173f7639107914`.
- Prior image: `sha256:d3cbaa197f1d1b8b9e6180ca733af7428625d693c1908144a29834610dc01af4`.
- Deployed image: `sha256:56c09e25940ab43defac8e2289e3be4b09ee5d868c3e178f46a1445bcd248a3c`.
- Control-plane, OAuth server and remote MCP all read back the new immutable
  image and reached `healthy`.
- The container root remained read-only. The sole v2 spool bind is writable,
  mode `0700`, and the application uses mode `0600` regular files.
- Deployed `ffmpeg` and `ffprobe` both report version `7.1.5-0+deb13u1`.
- The v2-only nginx block passed `nginx -t`; authenticated public capability
  readback returned `200/ready/2.0`, while unauthenticated readback returned
  `401`. The v1 block was retained unchanged.

## Live v2 acceptance

- Session: `voice-20260828-163102-f5645802`.
- Two independently playable AAC-LC, mono, 16 kHz M4A segments were durably
  accepted. Before `complete`, status showed zero completed Gemini requests
  and zero completed inference batches.
- A full user-service restart recreated all three containers. Exact session
  create and both uploads then returned durable duplicate receipts with the
  same two chunks and byte count.
- Completion returned `202/queued`; terminal status was
  `published_verified`, with two of two Gemini requests completed.
- Aggregate transcription UID:
  `289c6883-db01-4c43-b705-38319b852395`.
- Text summary UID: `37cd6bc0-2cbe-46ae-9d19-ca972d0f2d1f`.
- The two durable results and two distinct limiter receipts are the two
  physical successful provider POSTs. Transcription reserved 266 TPM from
  recorded duration; summary reserved 23156 TPM. Both finalized under
  `google_ai_project_model_atomic_v1`.
- IdeaHub atomic publication commit:
  `fb142c92ff15b8bfaf22ae9e4983a83e273c9d36`, with exactly the source packet,
  detail record, intake registry and `inbox/voice/README.md`.
- Exact-commit and then-current-main source blob were identical:
  `35fcb109d5ea43f674c1cb9b38a82b7b9002ef0f`.
- The published packet read back the pinned terminology commit
  `fed582853d894c5482744981d12d650de4bd3ca7`, blob
  `8802bf33b5c0ff125cc2bf56607e9bcdcaaca69e`, status `current`.
- Only after readback, status reported `server_audio_purged=true`; the session
  directory retained the two small JSON reconciliation artifacts and no M4A
  or normalized MP3.

## V1 regression and closure

- Short live v1 WAV session: `voice-20260828-163612-28516afe`.
- Transcription UID: `514ccbb2-8c13-4ea8-adf7-19b5737ea5f0`.
- V1 publication/readback commit:
  `ed070f0fafd89f885b3e9aeba972f396a846abe1`.
- Follow-up IdeaHub commit
  `54e5a26f856c4eebdccf7a8c3edcfcc01e9259de` retained both live publication
  commits, marked the disposable v1/v2 packets excluded, reconciled their
  registry counts, closed both details, and updated the README. Current-main
  readback confirmed the six changed paths.
- A safe-log scan found neither the device token nor fixture speech text in
  control-plane logs. No audio, transcript, summary, credential or secret is
  stored in this evidence file.

## Rollback receipt

Rollback remains v2-only: remove the v2 nginx location, disable v2, restore
the recorded prior image, retain the unfinished spool for diagnosis, and
re-run v1 authenticated health plus WAV smoke. No rollback was required.
