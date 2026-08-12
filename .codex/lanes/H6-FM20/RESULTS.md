# H6-FM20 Results

## Status

Committed implementation; no live reboot, Kaggle run, MCP mutation, credential use, or production PASS was attempted or claimed.

## Requirement IDs

- `FM20` — signed host reboot evidence, remote MCP cold start, bounded blogger search, and exact evidence Notebook binding.
- `FM20-EVIDENCE-V2` — append-only signed before/after boot identities and independently expected source/image identities.

## Branch / worktree

- Branch: `agent/operational-mvp/h6-fm20`
- Worktree: `/home/dev/.codex/worktrees/my-data-hub/h6-fm20`
- Base SHA: `fbed2abe2146981810ef3378b9535ddd8e4f1068`
- Tested implementation head SHA: `2ef05c88e6a72fc0d8a54427efec1546f032470f`
- The RESULTS commit is a documentation-only successor; use the branch tip reported in the handoff as the final head SHA.
- Effort/risk: extra-high because this lane crosses signed evidence, protected workflow inputs, production master mutation, resume reconciliation, and non-disclosure boundaries.

## Outcome

### Deployment evidence v2

- The existing owner-side three-stage collector now emits `my-data-hub-deployment-evidence.v2`.
- The signed receipt contains distinct `before_boot_id_sha256` and `after_boot_id_sha256` values derived from the private staged state and post-reboot host observation.
- The shared verifier preserves historical v1 validation and adds v2-only validation for:
  - fresh Ed25519 signature and exact key ID;
  - exact deployed commit and owner source identity;
  - independently expected source-tree SHA-256 equal to both observed source and installed release manifests;
  - independently expected per-service immutable image IDs;
  - distinct before/after boot IDs;
  - exact systemd unit, enabled/active autostart, linger and recovered service set;
  - process replacement, timestamp ordering and DB-free/listener invariants.
- The trusted post-deploy CLI/workflow now accepts v2 only and receives expected source/image identities from protected Environment variables. It still executes verifier code from the trusted default branch, not the target SHA.
- Append-only v2 deployment and post-deploy schemas/examples were added; v1 contracts remain unchanged for historical receipts.

### FM20 driver path

- The keyed evidence-claims document now supports FM20 and embeds one strict `operational-kaggle-fm20-evidence.v1` bundle. The existing provider-real workflow variable therefore carries the non-secret claim, public key, sanitized signed receipt, expected identities and bounded query without adding reader scopes.
- Before mutation, the driver requires:
  - a modern Kaggle token and exact reader/operator/provider credentials/tool catalogs;
  - an owner-issued FM20 evidence Notebook claim;
  - a successful direct `provider.resources.read` reconciliation of task, task-run, provider run/kernel/source and claim fingerprint;
  - valid fresh deployment-evidence v2;
  - exact reader `master.status == ABSENT` with no stale identity fields.
- The only mutation is existing operator `master.ensure`. The driver requires a new, non-duplicate, exact UUID `REQUESTED` receipt with the production intent and then polls reader status for a bounded transition to an exact ACTIVE instance/epoch/revision.
- The final `bloggers.search` uses `limit: 1`; PASS requires exactly one item, no continuation cursor, and response epoch/revision equal to the ACTIVE status. Query text and result rows are never written to driver receipts; only a count and response hash are retained.
- `resume_only` requires the prior ensure UUID in the FM20 claim, first reconciles it through `operation.get` as `ensure_master`, and never calls `master.ensure` again.
- Missing/untrusted pre-action evidence stays BLOCKED with `mutations_started: 0`. Any ambiguous ensure response or post-action observation/search failure is FAIL with `mutations_started: 1`.
- MCP never initiates a host reboot and the default reader catalog is unchanged.

## Commands and evidence

All commands ran in the isolated worktree against implementation head `2ef05c88e6a72fc0d8a54427efec1546f032470f` using the integration worktree's synchronized dev environment.

```text
python -m pytest -q \
  tests/test_post_deploy_acceptance.py \
  tests/test_deployment_evidence_collector.py \
  tests/provider/test_operational_kaggle_driver.py
PASS: 74 tests collected/passed

python scripts/validate_repository.py
PASS: 3243 checks, 0 errors, 0 notes

python -m compileall -q src tests scripts
PASS

python -m pytest -q
PASS: 776 collected; 774 passed, 2 expected environment-gated skips
(two pre-existing jsonschema RefResolver deprecation warnings)

ruff check \
  scripts/provider/operational_kaggle_driver.py \
  scripts/verify_post_deploy.py \
  src/my_data_hub/control_plane/deployment_evidence.py \
  tests/provider/test_operational_kaggle_driver.py \
  tests/test_deployment_evidence_collector.py \
  tests/test_post_deploy_acceptance.py
PASS

python scripts/create_notebooks.py --check
PASS: no drift

python scripts/scan_tracked_secrets.py
PASS

python scripts/provider/operational_kaggle_driver.py --help
PASS: direct-script import path resolves the shared verifier

git diff --check
PASS
```

One full-suite attempt first failed because an external provider-schema `$ref` was not registered by the repository's generic example validator. Root cause was confirmed from the resolver stack. The claims wrapper was restored to a locally resolvable schema-version gate while the dedicated FM20 schema retains the exact v2 reference and is tested with an explicit offline registry. The focused and full suites then passed.

## Risks / live prerequisites

- No real v2 signed host receipt, evidence key, exact expected release hash/image map, FM20 Notebook claim, Kaggle run or production MCP call was available. FM20 remains live-evidence BLOCKED until those inputs are supplied and the exact run passes.
- The existing `master.ensure` contract has no matrix task argument. The driver compensates by requiring exact ABSENT state, a non-duplicate first receipt, and a claim-bound UUID on resume; any contradictory response fails rather than starting/restarting blindly.
- The protected post-deploy Environment must configure `MY_DATA_HUB_EXPECTED_DEPLOY_SOURCE_TREE_SHA256` and `MY_DATA_HUB_EXPECTED_DEPLOY_SERVICE_IMAGE_IDS_JSON`. The provider acceptance claim document must independently contain matching FM20 expectations and an authentic public key/receipt.
- The bounded query must be chosen so the deployed canonical blogger dataset returns one row. An empty result is FAIL after ensure, not synthetic PASS.
- Historical v1 receipts remain verifiable but are intentionally insufficient for FM20 because they do not expose the signed before/after boot pair.

## Changed files

- `.github/workflows/post-deploy.yml`
- `docs/operations/deployment-evidence.md`
- `docs/operations/operational-kaggle-matrix.md`
- `examples/contracts/deployment-evidence.v2.example.json`
- `examples/contracts/post-deploy-verification.v2.example.json`
- `examples/provider/operational-kaggle-evidence-claims.v1.example.json`
- `examples/provider/operational-kaggle-fm20-evidence.v1.example.json`
- `schemas/deployment-evidence.v2.schema.json`
- `schemas/post-deploy-verification.v2.schema.json`
- `schemas/provider/operational-kaggle-evidence-claims.v1.schema.json`
- `schemas/provider/operational-kaggle-fm20-evidence.v1.schema.json`
- `scripts/provider/operational_kaggle_driver.py`
- `scripts/validate_repository.py`
- `scripts/verify_post_deploy.py`
- `src/my_data_hub/control_plane/deployment_evidence.py`
- `tests/provider/test_operational_kaggle_driver.py`
- `tests/test_deployment_evidence_collector.py`
- `tests/test_post_deploy_acceptance.py`
- `.codex/lanes/H6-FM20/RESULTS.md`

## Integration notes

Cherry-pick both implementation commits and the RESULTS commit in order. Do not mark FM20 PASS from unit tests or schema-valid examples; only the matrix runner's independent reconciliation of a real exact provider run can do that.
