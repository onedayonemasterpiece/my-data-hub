# CHECKPOINT-IMAGE-ATTESTATION results

## Scope

- Requirements: R-C1, R-C2, R-C3.
- Base SHA: `90ce252b75f385ce46bc3f2ecb5418967afc747c`.
- Implementation SHA: `4175b037fc28c80da2907b982f77a6551e8c3fea`.
- Branch: `agent/checkpoint-image-attestation`.
- Worktree: `/home/dev/.codex/worktrees/my-data-hub/checkpoint-image-attestation`.
- No provider, cloud, deploy, database or credential mutation was performed.

## Requirement closure

- **R-C1 — Done.** FM05/FM14/FM15 use the existing central
  `KaggleProviderAdapter.push_private_notebook_pending_runtime_attestation` path with
  one immutable `docker_image` digest and required
  `docker_image_pinning_type: original` in both the typed provider intent and adapter call.
- **R-C2 — Done.** The strict deployment/runtime contract now requires immutable image
  digest, original pinning, image source commit and CPython series. A credential-free,
  hash-bound execution-pins file accompanies the status input. Before copying checkpoint
  assets or starting the evidence entrypoint, rendered source verifies its exact pin
  document, `/etc/git_commit`, Python series, exact numeric Dataset refs, and exact attached
  Dataset mount set. The synchronous source-attestation callback carries only source/pins/
  image/Dataset metadata; no Kaggle credential or checkpoint bytes enter callbacks/status.
- **R-C3 — Done.** Parameterized FM05/FM14/FM15 regression runs the rendered source and
  typed intent through a real `KaggleProviderAdapter` with a fake provider API. It proves
  omission of the image contract fails before provider mutation and the complete contract
  produces exact private Kaggle metadata.

## Changed files

- `src/my_data_hub/acceptance/checkpoint_launcher.py`
- `tests/acceptance/test_checkpoint_launcher.py`
- `tests/control/test_checkpoint_acceptance_authority.py`
- `schemas/checkpoint-acceptance-deployment.v1.schema.json`
- `examples/provider/checkpoint-acceptance-deployment.v1.example.json`
- `docs/operations/checkpoint-acceptance-production.md`
- `.codex/lanes/checkpoint-image-attestation/RESULTS.md`

## Commands and evidence

- `python -m compileall -q src tests` — PASS.
- Focused/adjacent pytest covering checkpoint launcher/authority/entrypoint, central Kaggle
  adapter, operational driver and matrix — PASS (148 tests collected/executed).
- Full `python -m pytest -q` — PASS; three opt-in skips and two pre-existing
  `jsonschema.RefResolver` deprecation warnings.
- `python scripts/validate_repository.py` — PASS: 3,990 checks, zero errors/notes.
- Focused Ruff — PASS.
- `git diff --check` — PASS.

## Integration notes / risks

- The deployment document is intentionally strict: existing private deployment files must
  be updated with the reviewed digest, `original`, source commit and Python series before
  acceptance assembly; old files fail validation rather than launching unattested code.
- Root owns integration-time control-plane wiring and must populate these four provenance
  fields from the reviewed exact runtime asset contract without weakening the schema.
- This closes internal launch-contract validation only. It does not claim a live provider
  run, source-attestation callback, brokered checkpoint publication, or scenario PASS.
