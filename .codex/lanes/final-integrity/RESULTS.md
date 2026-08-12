# Final-integrity lane results

## Scope

- Lane ID: `final-integrity`
- Base SHA: `21f9160d7e986dc53e8b19560866964b97c7704e`
- Implementation SHA: `723e5cb0f50d6d32d4c695f4c41448c16717ca29`
- Branch: `agent/operational-mvp/final-integrity`
- Status: complete; no synthetic `COMPLETE` receipt was created and the checked-in
  operational receipt remains honestly `BLOCKED`.

## Delivered evidence

- Added a semantic JSON schema and content-bearing example for implementation
  review, deployment, post-deploy, security, data-integrity, and typed Gate
  evidence.
- Required every final receipt evidence index entry to declare its authoritative
  schema, Gate scope, and requirement scope.
- Bound Gates A-N to their requirement-specific evidence classes. Gate L accepts
  only a schema-valid `CONNECTOR_DURABILITY` receipt in `DURABLE_COMPLETE` state;
  generic dummy/review evidence cannot close it.
- Required all Gate-closing evidence to be local, hash-verifiable, schema-valid
  JSON whose semantic content agrees with the index.
- Verified `reviewed_head_commit` is an actual parent/ancestor of a real Git merge
  commit and bound required successful GitHub-hosted `contracts` and
  `postgres-integration` checks to that exact reviewed head.
- Bound deployment and post-deploy evidence to exact clean deployed/verified
  commits.
- Added semantic support for `provider-real` receipts only on exact runner labels
  `[self-hosted, linux, my-data-hub-devstand]`, plus a pure workflow boundary
  validator that rejects static MCP/data/Kaggle credential variables or secrets
  and requires the private rotating OAuth credential-file preflight for all four
  profiles. Wiring that helper into repository validation awaits the separately
  owned OAuth workflow change because the mandated lane base still has the legacy
  GitHub-hosted/static-token job.
- Allowed only the exact optional `connector-intake` Compose service and tested a
  default-off, read-only, no-port, no-database/PGDATA/data-plane boundary. The
  integration branch has overlapping newer connector assertions, so the
  integrator should preserve its exact service contract while merging the
  semantic receipt changes.

## Commands and outcomes

- `.venv/bin/pytest -q tests/test_operational_mvp_acceptance_receipt.py`
  - PASS: 15 tests after the owner-runner OAuth follow-up.
- `.venv/bin/ruff check scripts/validate_repository.py tests/test_operational_mvp_acceptance_receipt.py`
  - PASS.
- `.venv/bin/python -m compileall -q src tests`
  - PASS.
- `.venv/bin/python scripts/validate_repository.py`
  - PASS: `ok: true`, 4,043 checks, no errors or notes.
- `.venv/bin/pytest -q`
  - PASS: exit 0 across the full 1,167-test collection; three pre-existing skips
    were displayed and only existing `jsonschema.RefResolver` deprecation
    warnings were emitted.
- `git diff --check`
  - PASS.

## Changed files

- `docs/operations/operational-mvp-acceptance-receipt.md`
- `examples/contracts/operational-mvp-evidence.v1.example.json`
- `schemas/operational-mvp-acceptance-receipt.v1.schema.json`
- `schemas/operational-mvp-evidence.v1.schema.json`
- `scripts/validate_repository.py`
- `tests/test_operational_mvp_acceptance_receipt.py`
- `.codex/lanes/final-integrity/RESULTS.md` (this metadata-only follow-up)

## Risks and integration notes

- Integration advanced after this lane's mandated exact base. The validator's
  Compose section therefore conflicts textually with the integration branch's
  independently added `connector-intake` assertions. Do not discard the newer
  integration service/environment contract; manually retain it while bringing
  over semantic evidence/provenance validation and the pure boundary tests.
- The provider-real OAuth workflow is separately owned. Root must call
  `validate_provider_real_workflow_auth_boundary` from workflow validation when
  reconciling that workflow SHA; doing so on this exact base would intentionally
  fail its legacy job.
- Hosted-check records are schema- and commit-bound repository evidence; the
  offline validator does not contact GitHub. Receipt production remains
  responsible for obtaining genuine hosted run observations.
- No broker, connector implementation, embeddings, YDB, app, or deploy workflow
  file was modified.
