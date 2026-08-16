# Lane BLOGGER-MCP-PHASE-C Results

## Status

committed; repository-validated, not live-deployed

## Requirement IDs

- C01 public typed discovery intake
- C02 blogger preview/apply/status/reconciliation
- C03 cold-master continuation
- C04 bounded sanitized reads
- C05 reader/unified/operator profile separation
- C06 deploy/OAuth/docs/gates
- CR1 old-epoch immutable apply reconciliation from the current ACTIVE epoch
- CR2 real verified-checkpoint request/receipt lifecycle
- CR3 authoritative structural schema plus mandatory semantic validation
- CR4 exact committed apply replay without a second canonical broker effect
- CR5 successor-epoch checkpoint authority without rewriting the immutable writer receipt
- CR6 expired exact durable replay with signature and ledger binding preserved
- CR7 immutable PostgreSQL `committed_at` reconciliation
- CR8 coordinator/ledger agreement for later request-bound checkpoint revisions

## Branch / base

- Branch: `agent/mcp-r03/blogger-mcp-phase-c`
- Worktree: `/home/dev/.codex/worktrees/my-data-hub/blogger-mcp-phase-c`
- Base SHA: `7530f24`

## Commits

- `05ecab4` — initial typed blogger MCP implementation.
- `5cd4899` — initial lane evidence.
- `b508033` — reviewer follow-up: epoch reconciliation, checkpoint request/receipt,
  authoritative schema loading, and exact apply replay.
- `dfd06c1` — reviewer follow-up evidence.
- Final successor-authority/timestamp implementation and evidence commits are recorded
  in the handoff after this file is committed.

## Implemented evidence

- Discovery ingress loads the checked-in
  `schemas/blogger-discovery-batch.v1.schema.json` (including its packaged wheel copy)
  before the mandatory Pydantic semantic validator. An invalid URI accepted by the
  generated Pydantic schema is rejected by the authoritative schema.
- Exact successful `bloggers.import.apply` replay returns the durable stored result in
  `COMMITTED_PENDING_CHECKPOINT`, `CHECKPOINTING`, `CHECKPOINT_VERIFIED`, and
  `DURABLE_COMPLETE`; conflicting identity is rejected and no second PostgreSQL broker
  request is made.
- The current ACTIVE canonical-committer session can reconcile an exact immutable
  APPLYING receipt created by an older fenced epoch. The old receipt's master/epoch,
  request, plan, batch, principal, and client remain bound.
- A canonical blogger commit creates a deterministic task-bound verified-checkpoint
  request in the shared control ledger. Status reads never invent `CHECKPOINTING`:
  that state appears only after the ACTIVE notebook claims the request, and terminal
  durability requires its exact current VERIFIED checkpoint receipt.
- Operator control-plane assembly exposes the master notebook's checkpoint-request
  claim endpoint even when the optional connector-intake service is disabled. Unified
  and provider-only profiles still have no canonical blogger write authority.
- Provider-only/upload catalogs retain their existing semantics; unified/default reader
  profiles still expose no `data.query` and no blogger writes.
- The immutable canonical write receipt remains bound to its original writer epoch,
  while post-change durability accepts only the exact deterministic checkpoint request
  claimed by the current ACTIVE successor and its request-bound current VERIFIED HEAD
  at the committed revision. An unrelated same-revision checkpoint cannot close it.
- Exact committed replay remains available after the 300-second preview admission TTL.
  The signed body, immutable operation/batch/request/plan/principal/client binding, and
  stored durable state are still mandatory; expired new effects and forged receipts do
  not pass and no second broker effect occurs.
- Append-only migration `0021_blogger_reconcile_committed_at.sql` widens the fixed
  reconciliation function with the already-stored commit timestamp. The PostgreSQL
  facade and broker return that value instead of fabricating a control-plane time.
- Blogger durability now follows the shared checkpoint coordinator's monotonic
  containment rule: the exact deterministic request-bound current VERIFIED HEAD may
  protect a revision later than the requested commit. A revision-14 checkpoint closes
  a revision-13 request; revision 12 and unrelated operation/request HEADs do not.

## Commands run

- `python3 -m compileall -q src tests`
- focused Phase-C, broker, control-runtime, brokered-checkpoint, deployment, and blogger
  contract pytest sets
- `uv run pytest -q`
- `uv run ruff check .`
- `uv run mypy`
- `uv run python scripts/validate_repository.py`
- `uv run python scripts/create_notebooks.py --check`
- `MDH_RUN_DISPOSABLE_POSTGRES=1 uv run pytest -q tests/bloggers/test_discovery_postgres_live.py`

## Verification

- Full suite: 1,453 collected, 4 skipped, 0 failed.
- Repository validator: `ok=true`, 4,569 checks, no errors or notes.
- Ruff: all checks passed.
- Configured mypy set: no issues.
- Notebook generator: no drift.
- Disposable PostgreSQL test passed and proves reconciliation returns the exact
  `committed_at` stored in `integration.blogger_discovery_apply_receipt`. The opt-in test
  uses an ephemeral local container only; no production/live proof is claimed.

## Remaining live blockers / non-claims

- This lane performed no live deployment, OAuth grant, Kaggle run, public MCP call,
  production PostgreSQL mutation, or real checkpoint rotation.
- Artifact-backed discovery remains blocked until the dedicated verified provider
  materializer produces its live receipt; inline typed rows do not have that blocker.
- Deployment still requires the approved operator profile and fresh OAuth authorization.
