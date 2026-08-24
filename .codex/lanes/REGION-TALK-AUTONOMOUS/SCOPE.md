# Region Talk autonomous migration and pipeline — execution matrix

## Fanout decision

The work spans source migration, PostgreSQL contracts, a separate Kaggle workload,
control-plane registration/scheduling, MCP surfaces, and live acceptance. Discovery is
parallel and read-only. Writable work is split only where file ownership is disjoint;
shared MCP/control/deploy assembly remains serial in the integration worktree.

## Requirements

| ID | Requirement | Primary lane | Dependencies | Done when |
|---|---|---|---|---|
| R01 | Inventory and losslessly migrate the complete Region Talk source, not only the curated blogger list | `region-talk-data` | exact source inventory | every source row is transactionally accounted for as normalized, deduplicated, intentionally excluded, retained raw, or quarantined |
| R02 | Migrate bloggers, articles, selected posts, candidates, and publication-queue state into the shared canonical PostgreSQL | `region-talk-data` | R01 | typed PostgreSQL projections and reconciliation receipts match the source inventory without fabricated counts |
| R03 | Run Region Talk as a separate Kaggle Notebook workload, not inside the devstand orchestrator or master process | `region-talk-pipeline` | R01/R02 contracts | independently versioned private Notebook launches against the ACTIVE master data plane |
| R04 | Keep the devstand orchestrator lightweight and general | `region-talk-pipeline` | R03 | control stores only metadata/receipts and never Region Talk business rows or PGDATA |
| R05 | Register, fence, retry, time out, receipt, and periodically schedule the workload through the shared orchestrator | `region-talk-pipeline` | R03 | durable request/run state survives restart, binds exact epoch/task, and revokes credentials on terminal/expiry |
| R06 | Expose bounded typed MCP reads and controlled operations for Region Talk articles/posts/queue/pipeline without generic SQL | `region-talk-mcp` | R02/R05 | role/scope/catalog tests plus live read/status proof pass; provider-only semantics are unchanged |
| R07 | Perform the first migration and pipeline run under supervision, restore temporary YDB capacity, and prove one ACTIVE master | `integration-live` | all code lanes | source is restored to STOPPED/0, one ACTIVE master remains, queue/status reads and checkpoint evidence are observed |

## Lane map

```yaml
mode: read_only_parallel_then_serial_integration
repo: /home/dev/.codex/worktrees/my-data-hub/operational-mvp
base_ref: 1f9f08367e2414b538b2b3dfa77bb7693a26d57a
base_branch: integration/operational-mvp
integration_branch: integration/operational-mvp
global_constraints:
  - one writable ACTIVE PostgreSQL primary, only in the Kaggle master Notebook
  - no canonical business rows, PGDATA, provider credentials, or worker secrets on devstand
  - no generic SQL and no publication side effect during the supervised first run
  - YDB capacity changes are temporary and must be restored to the observed STOPPED/0 state
verification_owner: /root
stop_conditions:
  - conflicting ACTIVE master epoch
  - source mutation or inability to restore YDB state
  - unverified provider effect ambiguity
  - migration accounting mismatch or undispositioned row
lanes:
  - id: region-talk-data
    role: worker
    requirement_ids: [R01, R02]
    execution_mode: serial_after_read_only_map
    writable_files: [sql/migrations/0023_*, src/my_data_hub/workloads/region_talk/*, tests/region_talk/*, .codex/lanes/REGION-TALK-DATA/*]
    verification_scope: full_local
    status: planned
  - id: region-talk-pipeline
    role: worker
    requirement_ids: [R03, R04, R05]
    execution_mode: serial_after_read_only_map
    writable_files: [new Region Talk runtime/launcher modules, generated Notebook assets, focused tests, lane evidence]
    verification_scope: full_local
    status: planned
  - id: region-talk-mcp
    role: worker
    requirement_ids: [R06]
    execution_mode: serial_after_dependency
    writable_files: [shared MCP/catalog/service/broker/OAuth/deploy files after data and pipeline contracts stabilize]
    verification_scope: full_local
    status: planned
  - id: integration-live
    role: merge_reviewer
    requirement_ids: [R07]
    execution_mode: serial_after_dependency
    writable_files: [integration conflict resolution, exact assets, live evidence only]
    verification_scope: full_local_and_live
    status: planned
```
