# Issue 31 execution matrix and lane map

## Source authority

- Deployed/source base: `7efd017e7fc65373e8063857c52afa1458b38197`.
- PR base: `origin/feat/record-idea-hub-voice-intake-v2` at `804ec60d5da12790bd9ae9b016270e417169eff8`.
- PR target is the existing Voice v2 stacked branch, not stale `main`.
- Private sessions, audio, transcripts, terminology and credentials are forbidden in all lanes.

## Requirement matrix

| ID | Requirement | Primary lane | Dependency | Done when |
|---|---|---|---|---|
| R01 | Durable content-verification receipt gates purge | ledger | none | Store cannot delete without a passed receipt |
| R02 | GitHub readback alone cannot authorize deletion | ledger | R01 | Direct purge is rejected and files remain |
| R03 | Bounded per-source-chunk transcription | inference | none | One successful immutable receipt per chunk |
| R04 | Persist finish reason, source hash, range and coverage | inference | none | Receipt is bounded, durable and contains no raw logs |
| R05 | MAX_TOKENS, short valid, unknown finish, missing/gap/overlap/ambiguity fail closed | inference | R03-R04 | Typed failures never become verified receipts |
| R06 | Deterministic ordered assembly; summary only after coverage | orchestration | ledger+inference | Aggregate hash is derived from ordered verified receipts |
| R07 | No `Полная расшифровка` before verification | compatibility | orchestration | Renderer refuses an unverified projection |
| R08 | Separate publication/content/authorization/purge states | ledger | none | Independent durable fields/receipts and transitions |
| R09 | Old Android remains fail closed | compatibility | ledger+orchestration | Legacy terminal triplet is false until physical purge |
| R10 | Idempotent legacy-ledger migrations | ledger | none | Two migrations are a no-op; legacy GitHub rows stay unverified |
| R11 | Metadata-only legacy audit | ledger | R10 | Bounded aggregate counts only; no IDs/content |
| R12 | Separate auditable purge authorization | orchestration | R01,R02,R06 | Authorization references content and publication receipts |
| T01 | 20+ min short-valid retention with real files | acceptance | orchestration | All source files exist; no summary/full publication/purge |
| T02 | Parseable/malformed MAX_TOKENS and finish matrix | inference | R05 | All cases fail closed |
| T03 | Missing/gap/overlap/hash/range coverage matrix | acceptance | ledger | All cases retain physical files |
| T04 | Crash/restart after every durable stage | acceptance | orchestration | No successful provider call repeats |
| T05 | Exact GitHub readback with incomplete content | acceptance | R02 | Publication may be true; purge remains impossible |
| T06 | Complete multi-chunk flow | acceptance | all | Content -> publication -> authorization -> verified deletion |
| T07 | Filesystem purge failure recovery | acceptance | R12 | Retry performs purge only; inference/publication not repeated |
| V01 | Targeted and full tests | integrator | all | Green, with skips reported |
| V02 | Migration/schema/repository validation | integrator | ledger | Green |
| V03 | Secret scan | integrator | all | Green; no forbidden artifacts tracked |
| V04 | Dedicated commits and issue-linked PR | integrator | V01-V03 | PR open against stacked Voice v2 branch |
| V05 | Deployment/canary/rollback evidence | integrator | PR+review | Only gated deployment; blockers explicit |

## Dependency graph

```text
ledger ─────┐
            ├─> orchestration ─> compatibility ─> acceptance ─> review/integration
inference ──┘
```

## Lane map

```yaml
mode: worktree_worker_then_serial_integrator
repo: onedayonemasterpiece/my-data-hub
base_ref: 7efd017e7fc65373e8063857c52afa1458b38197
base_branch: integration/voice-v2-schema-recovery
integration_branch: integration/issue-31-content-verification-stacked
global_constraints:
  - fail closed; false negatives retain audio
  - never access or emit production/private content or credentials
  - never use the affected real session in tests
  - preserve v1 and frozen Android 1.1 state strings
verification_owner: root integrator
stop_conditions:
  - any deletion path without durable content and purge-authorization receipts
  - any hidden retry after an ambiguous provider outcome
  - any private artifact in git diff or logs
lanes:
  - id: issue31-ledger
    role: worker
    requirement_ids: [R01, R02, R08, R10, R11]
    execution_mode: parallel
    branch: agent/issue-31-content-verification/ledger
    worktree: /home/dev/.codex/worktrees/my-data-hub/issue31-ledger
    writable_files: [src/my_data_hub/voice_intake_v2/store.py, tests/voice_intake_v2/test_store.py, .codex/lanes/issue31-ledger/RESULTS.md]
    forbidden_files: [src/my_data_hub/voice_intake_v2/inference.py, src/my_data_hub/voice_intake_v2/worker.py]
    verification_scope: targeted
    status: planned
  - id: issue31-inference
    role: worker
    requirement_ids: [R03, R04, R05, T02]
    execution_mode: parallel
    branch: agent/issue-31-content-verification/inference
    worktree: /home/dev/.codex/worktrees/my-data-hub/issue31-inference
    writable_files: [src/my_data_hub/voice_intake_v2/contracts.py, src/my_data_hub/voice_intake_v2/inference.py, tests/voice_intake_v2/test_inference.py, .codex/lanes/issue31-inference/RESULTS.md]
    forbidden_files: [src/my_data_hub/voice_intake_v2/store.py, src/my_data_hub/voice_intake_v2/worker.py]
    verification_scope: targeted
    status: planned
  - id: issue31-orchestration
    role: worker
    requirement_ids: [R06, R12]
    depends_on: [issue31-ledger, issue31-inference]
    execution_mode: serial_after_dependency
    branch: agent/issue-31-content-verification/orchestration
    worktree: /home/dev/.codex/worktrees/my-data-hub/issue31-orchestration
    writable_files: [src/my_data_hub/voice_intake_v2/worker.py, tests/voice_intake_v2/test_worker.py, .codex/lanes/issue31-orchestration/RESULTS.md]
    verification_scope: targeted
    status: planned
  - id: issue31-compatibility
    role: worker
    requirement_ids: [R07, R09]
    depends_on: [issue31-orchestration]
    execution_mode: serial_after_dependency
    branch: agent/issue-31-content-verification/compatibility
    worktree: /home/dev/.codex/worktrees/my-data-hub/issue31-compatibility
    writable_files: [src/my_data_hub/voice_intake_v2/api.py, src/my_data_hub/voice_intake_v2/markdown.py, src/my_data_hub/voice_intake_v2/publisher.py, tests/voice_intake_v2/test_api.py, tests/voice_intake_v2/test_markdown.py, tests/voice_intake_v2/test_publisher.py, docs/operations/record-idea-hub-voice-intake-v2.md, docs/handoffs/record-idea-hub-android-1.1-api-contract.md, .codex/lanes/issue31-compatibility/RESULTS.md]
    verification_scope: targeted
    status: planned
  - id: issue31-acceptance
    role: worker
    requirement_ids: [T01, T03, T04, T05, T06, T07]
    depends_on: [issue31-compatibility]
    execution_mode: serial_after_dependency
    branch: agent/issue-31-content-verification/acceptance
    worktree: /home/dev/.codex/worktrees/my-data-hub/issue31-acceptance
    writable_files: [tests/voice_intake_v2, .codex/lanes/issue31-acceptance/RESULTS.md]
    verification_scope: full_local
    status: planned
  - id: issue31-review
    role: reviewer
    requirement_ids: [V01, V02, V03, V04, V05]
    depends_on: [issue31-acceptance]
    execution_mode: serial_after_dependency
    verification_scope: inspection_only
    status: planned
```

The richer runner's maximum reasoning tier is warranted for ledger, orchestration,
acceptance and final review because a false positive can destroy user data. Available
workers use their highest fixed review/implementation effort.
