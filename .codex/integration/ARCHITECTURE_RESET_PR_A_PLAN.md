# PR-A architecture-reset execution plan

```yaml
mode: serial_integrator
repo: onedayonemasterpiece/my-data-hub
base_ref: bcc02df1f980ac6eefcd305d71cef94817033d70
base_branch: main
integration_branch: integration/architecture-reset-pr-a
global_constraints:
  - never run INSTALL_MY_DATA_HUB_SAME_HOST
  - never start or create local production PostgreSQL or PGDATA
  - never apply master migrations on devstand
  - never enable same-host autostart
  - do not change DNS, VPN, port 443, Region Talk runtime, or remote MCP writes
  - do not modify the exact imported source research file
verification_owner: root
stop_conditions:
  - exact source contradicts the reset package
  - a requested correction requires a real Kaggle master or later-phase implementation
  - host evidence would require a destructive operation without explicit approval
lanes:
  - id: PR-A-01
    role: integrator
    requirement_ids: [R01, R02, R03, R04, R05, R06, R07, R08, R09, R10]
    target: architecture reset, deployment safety, documentation and semantic tests
    depends_on: [exact_source_audit, host_read_only_evidence]
    execution_mode: serial_after_dependency
    branch: integration/architecture-reset-pr-a
    worktree: /home/dev/.codex/worktrees/my-data-hub/architecture-reset-pr-a
    writable_files:
      - AGENTS.md
      - README.md
      - PROJECT_STATUS.md
      - architecture/**
      - deploy/**
      - docs/**
      - scripts/validate_repository.py
      - tests/**
      - compose*.yaml
      - .codex/**
    forbidden_files:
      - docs/source-material/idea-hub/idea-20260809-content-platform-current-design.md
      - sql/migrations/**
      - production secrets
    expected_output: one corrective PR-A with host receipt, ADR, invariants, hard-disabled old installer, control-plane-only contract, drift tests, and preservation map
    verification_scope: full_local
    status: implementation_complete_validation_green
  - id: PR-A-REVIEW
    role: reviewer
    requirement_ids: [DOD_REVIEW]
    target: independent maximum-available architecture/security closure review
    depends_on: [PR-A-01]
    execution_mode: serial_after_dependency
    branch: read_only
    worktree: /home/dev/.codex/worktrees/my-data-hub/architecture-reset-pr-a
    writable_files: []
    forbidden_files: ['**']
    expected_output: exact-head Critical/High findings and merge verdict
    verification_scope: full_local
    status: third_review_blocker_remediated_pending_exact_head_rereview
```

## Requirement map

| ID | Requirement | Done when |
|---|---|---|
| R01 | Read-only host evidence | Receipt truthfully distinguishes staging, empty artifacts, and deployed runtime |
| R02 | Incident and authority | Incident plus corrective ADR preserve history and restore exact-source authority |
| R03 | Machine-readable invariants | Validator enforces topology and deployment facts |
| R04 | Supersede drift | Listed docs and dependent ADRs are consistent |
| R05 | Disable local production PostgreSQL | Old token hard-fails; production/control plane contains no PostgreSQL/PGDATA/backup/migrations |
| R06 | Restore topology docs | Devstand, Kaggle master, checkpoints and direct data plane agree |
| R07 | Restore implementation sequence | PR-B begins reusable runtime/FakeKaggle; Region Talk remains later |
| R08 | Architecture tests | Semantic tests cover authority, profiles, ABSENT state and safety flags |
| R09 | Preservation map | Every prior work class is kept, rebound, superseded, test-only, or deferred |
| R10 | Git delivery | CI, independent review, PR, merge and final evidence |
