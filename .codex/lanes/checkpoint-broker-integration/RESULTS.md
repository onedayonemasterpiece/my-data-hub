# Checkpoint broker integration results

Status: implemented; no live provider mutation performed.

## Base and ownership

- Exact base: `5589629b3449998cdc1459855f2bbabe19927378`.
- Isolated branch/worktree: `agent/operational-mvp/checkpoint-broker-integration`.
- Integration advanced independently; this lane was intentionally not rebased.

## Seam map and root cause

Normal master split-upload was already present in `checkpoints/brokered_upload.py`, but the
runtime coordinator did not register its candidate before the first prepare. Acceptance
already had task auth, source attestation, remote registry, owner-fixed assets, status
Dataset, and a credential-free evidence Notebook launcher. It was deliberately excluded
by `_broker_runtime_authority`, and the Notebook factory ended in the named broker blocker.
The surviving legacy effects required a second in-Notebook Kaggle client.

## Implemented contract

- Normal master registers the immutable candidate before any blob prepare.
- Checkpoint-bound acceptance authority derives from the persisted task launch,
  source-attested provider run, fixed scenario/config, expiry, and candidate manifest.
- Notebook bytes go by direct signed HTTPS PUT; control receives metadata only. Tokens and
  signed URLs stay sealed in the private broker ledger and are absent from public evidence.
- Restart skips exact completed blobs rather than requesting a second URL.
- FM05 uses the normal checkpoint Dataset, exact next version, independent verifier,
  readback/restore proof, and CAS promotion.
- FM14 permits only the fixed base archive same-size hash mismatch, records the expected
  rejection, and leaves HEAD unchanged.
- FM15 uses the same central adapter for its owner-fixed verifier, proves the exact expected
  failed output, retains typed bounded evidence, and leaves HEAD unchanged.
- Production assembly enables the launcher only with the broker and rejects a distinct
  FM05 Dataset. Migration 026 adds explicit authority/scenario and typed verifier evidence.

## Gates

- `uv run python -m compileall -q src tests`: PASS.
- Focused pytest (broker, API, acceptance runtime, launcher): PASS (34 tests).
- Ruff on all touched Python files: PASS.
- Full suite: PASS after updating the contiguous migration expectation (two discovered
  integration regressions fixed); no live mutation attempted.

## Collision note

`src/my_data_hub/control_plane/app.py` is also edited by Gate K on the newer integration
head. Integrate this broker commit first, then reconcile Gate K manually as coordinated.
