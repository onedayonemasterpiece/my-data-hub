# Failure recovery lane map

```yaml
mode: read_only_parallel_then_serial_integrator
repo: onedayonemasterpiece/my-data-hub
base_ref: ce2b4ccf6a296e91d8390403bb13b6ae056be9ac
base_branch: main
integration_branch: integration/chatgpt-provider-recovery
global_constraints:
  - preserve failed YouTube resource as regression evidence
  - never expose Kaggle or OAuth credentials
  - no canonical data-plane dependency
  - all writes and deployment remain serial
verification_owner: root
stop_conditions:
  - no root cause evidence
  - provider mutation becomes ambiguous
lanes:
  - id: R01
    role: planner
    requirement_ids: [R01]
    target: ChatGPT-facing MCP schema refresh boundary
    depends_on: []
    execution_mode: parallel
    branch: null
    worktree: shared-read-only
    writable_files: []
    forbidden_files: ['*']
    expected_output: observed cache boundary and remediation
    verification_scope: inspection_only
    effort: high
    status: completed
  - id: R02
    role: planner
    requirement_ids: [R02]
    target: normalize deleted Kaggle readback failures
    depends_on: []
    execution_mode: parallel
    branch: null
    worktree: shared-read-only
    writable_files: []
    forbidden_files: ['*']
    expected_output: root cause and regression-test target
    verification_scope: inspection_only
    effort: high
    status: completed
  - id: R03
    role: planner
    requirement_ids: [R03]
    target: terminal notebook status projection
    depends_on: []
    execution_mode: parallel
    branch: null
    worktree: shared-read-only
    writable_files: []
    forbidden_files: ['*']
    expected_output: root cause and regression-test target
    verification_scope: inspection_only
    effort: high
    status: completed
  - id: INTEGRATE
    role: merge_reviewer
    requirement_ids: []
    target: serial implementation, tests, PR, deploy, live verification
    depends_on: [R01, R02, R03]
    execution_mode: serial_after_dependency
    branch: integration/chatgpt-provider-recovery
    worktree: /home/dev/.codex/worktrees/my-data-hub/deploy-oauth-region-talk
    writable_files:
      - src/my_data_hub/providers/kaggle/adapter.py
      - src/my_data_hub/control_plane/adapters.py
      - tests/provider/test_kaggle_adapter.py
      - tests/control/test_mcp_operator_provider.py
      - .codex/integration/**
    forbidden_files:
      - docs/source-material/**
      - architecture/migrations/**
    expected_output: tested and deployed closure or explicit external cache handoff
    verification_scope: full_local
    effort: high
    status: local_validation_complete
```

## Integration evidence

- R01: running MCP server exposes the corrected action schema; the ChatGPT workspace app
  snapshot remains stale and must be refreshed/re-approved in ChatGPT Action control.
- R02: explicit delete now records exact durable absence; absent resources short-circuit
  read/list/download, and raw provider reads are normalized to redacted domain errors.
- R03: notebook read/list now project exact terminal Kaggle status monotonically into the
  control ledger.
- `python -m compileall -q src tests`: passed.
- `python scripts/validate_repository.py`: 4,599 checks, zero errors.
- `pytest`: 1,542 passed, 4 skipped.
