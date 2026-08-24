# Lane R01 Results

## Status
committed

## Requirement IDs
- R01

## Branch
integration/provider-run-product

## Worktree
/home/dev/.codex/worktrees/my-data-hub/deploy-oauth-region-talk

## Base SHA
4e981982538f27f17ab08d06776079d9c7f05420

## Head SHA
Recorded by the integration commit containing this report.

## Files changed
- src/my_data_hub/control_plane/adapters.py
- tests/control/test_mcp_operator_provider.py

## Commands run
- `.venv/bin/pytest -q tests/control/test_mcp_operator_provider.py::test_single_provider_gateway_uses_exact_claims_and_metadata_only_ledger`
- `.venv/bin/pytest -q tests/control/test_mcp_operator_provider.py`
- `.venv/bin/python -m compileall -q src tests`

## Tests / verification
The regression failed before the fix because the gateway intent hash included
`dataset_inputs`, while the concrete Kaggle adapter recomputed the exact hash from
provider-facing `dataset_sources` only. It passes after removing control-only claim
metadata from the adapter intent while retaining claim authorization in the gateway.

## Risks
None to provider input authorization: exact claims are still resolved and policy-checked
before the intent is built; the provider adapter receives only numeric provider sources.

## Merge notes
This is the integration branch; no separate cherry-pick is needed.
