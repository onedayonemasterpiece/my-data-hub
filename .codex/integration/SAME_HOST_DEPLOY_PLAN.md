# Same-host deployment and documentation integration plan

```yaml
mode: serial_integrator
repo: onedayonemasterpiece/my-data-hub
base_ref: cbea2a43ffa430e0d2c82b82db6198d30c362f65
base_branch: main
integration_branch: integration/same-host-deploy-docs
global_constraints:
  - current host is the permanent execution host
  - preserve fail-closed write/publication/Kaggle defaults
  - never extract the supplied ZIP over the repository
  - do not restart existing services without an explicit reviewed command
verification_owner: root
stop_conditions:
  - documentation archive contains unsafe paths or credentials
  - deployment would overwrite an unrelated proxy route
  - required production secrets are absent
lanes:
  - id: R01
    role: integrator
    requirement_ids: [R01]
    target: permanent same-host supervision and autostart
    depends_on: [server_runtime_audit, deployment_code_map]
    execution_mode: serial_after_dependency
    branch: integration/same-host-deploy-docs
    worktree: /home/dev/.codex/worktrees/my-data-hub/same-host-deploy-docs
    writable_files: [deploy/systemd, scripts, docs/operations, .github/workflows]
    forbidden_files: [sql/migrations]
    expected_output: same-host install/runbook and runtime evidence
    verification_scope: full_local
    status: prepared_install_pending_explicit_confirmation
    evidence:
      - exact release 3f9c1f0266ace518e94e6096ac1b8b86a153c06e staged read-only
      - PREPARE left current pointer, root environment hashes and disabled/inactive unit unchanged
      - permanent PostgreSQL volume, migrations and autostart not started
  - id: R02
    role: integrator
    requirement_ids: [R02]
    target: expose MCP under a safe HTTPS subpath on the current server
    depends_on: [R01, server_runtime_audit, deployment_code_map]
    execution_mode: serial_after_dependency
    branch: integration/same-host-deploy-docs
    worktree: /home/dev/.codex/worktrees/my-data-hub/same-host-deploy-docs
    writable_files: [deploy, docs/operations, src/my_data_hub/mcp, tests]
    forbidden_files: [sql/migrations]
    expected_output: proxy route plus bounded MCP path configuration
    verification_scope: full_local
    status: edge_plan_complete_live_change_blocked
    blockers:
      - production OAuth issuer/JWKS is not configured
      - DNS/TLS/Xray-nginx SNI cutover requires separate disruptive-change confirmation
  - id: R03
    role: integrator
    requirement_ids: [R03]
    target: semantic merge of supplied data-scope documentation archive
    depends_on: [archive_docs_audit]
    execution_mode: serial_after_dependency
    branch: integration/same-host-deploy-docs
    worktree: /home/dev/.codex/worktrees/my-data-hub/same-host-deploy-docs
    writable_files: [docs, schemas, examples, tests, scripts/validate_repository.py]
    forbidden_files: [sql/migrations, src]
    expected_output: reviewed documentation merge without losing R1 changes
    verification_scope: full_local
    status: completed
    evidence:
      - archive and internal manifest checksums verified
      - 28 modified and 2 new documentation files merged semantically
      - R1 implementation claims preserved; ADR-0015 runtime remains explicitly pending
```
