# Lane AUDIT-DRIFT Results

## Status

completed-read-only

## Requirement IDs

- R00

## Authoritative base

- Deployed and remote v1 commit:
  `491b2ba55b8c7ec30fbcc97a9839ad874fbdeba0`.
- Git tree: `5c7d7ccd27b04822e28b2ca91bd4c64d8688ed14`.
- Immutable deployed image ID:
  `sha256:d3cbaa197f1d1b8b9e6180ca733af7428625d693c1908144a29834610dc01af4`.
- Draft PR #29 head is the same exact commit; PR #30 is stacked on that branch.

## Readback evidence

- The live `current` release points to the exact `491b2ba...` release.
- All 1,088 tracked release files match the Git commit: 0 missing, 0 changed.
- All 394 Git-controlled files copied into the runtime image match the Git
  commit by SHA-256: 0 missing, 0 changed.
- All three healthy control-plane containers use the same immutable image ID.
- `origin/feat/record-idea-hub-voice-proxy` equals the deployed commit.

## Ancestry

The history is linear with no divergence:

`68bb5bf -> 5475ae6 -> df308ed -> 99bc060 -> 491b2ba`.

The reported `68bb5bf` checkpoint is four commits behind the deployed source.
Starting v2 there would lose current terminology/index readback, session pinning,
restart durability, and the deployed Android first-chunk compatibility fix.

## Protected v1 surfaces

- `src/my_data_hub/voice_intake/api.py`
- `src/my_data_hub/voice_intake/github.py`
- `src/my_data_hub/voice_intake/markdown.py`
- existing v1 contract tests

V1 retains its deployed first-chunk compatibility behavior. V2 deliberately has
a separate durable ledger and rejects upload-before-create with a typed 409.

## Commands / verification

Read-only Git, Docker, release-tree and image-content hash inspection; fresh
remote and PR readback. No runtime state was mutated and no credentials, audio,
transcripts or summaries were inspected.

## Risks

- Image tags alone remain insufficient; deployment closure must repeat immutable
  image-ID and source-content readback.
- Rebasing v2 onto main or `68bb5bf` would lose the authoritative PR #29 stack.
