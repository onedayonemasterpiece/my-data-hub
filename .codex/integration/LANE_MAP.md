# Provider Run Product Lane Map

```yaml
mode: serial_integrator
repo: /home/dev/projects/my-data-hub
base_ref: 4e981982538f27f17ab08d06776079d9c7f05420
base_branch: main
integration_branch: integration/provider-run-product
global_constraints:
  - PostgreSQL remains Kaggle-master-only
  - remote MCP may mutate only private mcp_managed resources
  - no secrets, provider output bytes, or canonical data in the control ledger
  - exact claims, idempotency, and fenced provider effects remain enforced
verification_owner: root
stop_conditions:
  - ambiguous provider mutation cannot be reconciled
  - public resource or unbounded output exposure
  - existing protected/orchestrator Kaggle lifecycle changes
lanes:
  - id: R01
    role: worker
    requirement_ids: [R01]
    target: synchronize gateway and adapter notebook effect intent
    depends_on: []
    execution_mode: serial
    branch: integration/provider-run-product
    worktree: /home/dev/.codex/worktrees/my-data-hub/deploy-oauth-region-talk
    writable_files: [src/my_data_hub/control_plane/adapters.py, tests/control/test_mcp_operator_provider.py]
    forbidden_files: [migrations, exact imported source]
    expected_output: regression test and restored provider run
    verification_scope: full_local
    status: in_progress
  - id: R02
    role: planner_then_integrator
    requirement_ids: [R02]
    target: bounded runtime network and accelerator contract
    depends_on: [R01]
    execution_mode: serial_after_dependency
    branch: integration/provider-run-product
    worktree: /home/dev/.codex/worktrees/my-data-hub/deploy-oauth-region-talk
    writable_files: [src/my_data_hub/mcp/provider_schemas.py, src/my_data_hub/control_plane/adapters.py, src/my_data_hub/providers/kaggle/adapter.py, tests]
    forbidden_files: [migrations, exact imported source]
    expected_output: closed options propagated to exact Kaggle metadata
    verification_scope: full_local
    status: planned
  - id: R03
    role: planner_then_integrator
    requirement_ids: [R03]
    target: claim-bound notebook status and bounded output retrieval
    depends_on: [R01]
    execution_mode: serial_after_dependency
    branch: integration/provider-run-product
    worktree: /home/dev/.codex/worktrees/my-data-hub/deploy-oauth-region-talk
    writable_files: [src/my_data_hub/mcp, src/my_data_hub/control_plane, src/my_data_hub/providers/kaggle, tests]
    forbidden_files: [migrations, exact imported source]
    expected_output: polling plus bounded output list/download through MCP
    verification_scope: full_local
    status: planned
```

Native read-only subagent startup was attempted twice for R02 and failed before task
execution because the required `private_events` MCP initialization timed out. Per the
external-tool research gate, no third blind retry is made; the coupled investigation
continues serially in the integration lane.
