# Lane R03 Results

## Status
committed

## Requirement IDs
- R03

## Branch
integration/provider-run-product

## Worktree
/home/dev/.codex/worktrees/my-data-hub/deploy-oauth-region-talk

## Base SHA
9fe48c0

## Head SHA
Recorded by the integration commit containing this report.

## Files changed
- src/my_data_hub/mcp/provider_schemas.py
- src/my_data_hub/mcp/server.py
- src/my_data_hub/control_plane/adapters.py
- tests/control/test_mcp_operator_provider.py
- tests/mcp/test_dynamic_contracts.py

## Commands run
- targeted control/MCP/provider suites
- full repository gates listed in R02

## Tests / verification
`provider.resources.read` now returns live exact run status. Run requests declare up to
32 top-level outputs with per-file 8 MiB caps. `provider.resources.list` exposes those
claim-bound declarations and live run state; `provider.resources.download` returns
verified base64 chunks capped at 128 KiB after the exact terminal run/output fence.

## Risks
Outputs must be declared before launch and must be top-level files. This is deliberate:
it avoids an unbounded provider output-tree download/list operation.

## Merge notes
No database migration or canonical-data path is involved.
