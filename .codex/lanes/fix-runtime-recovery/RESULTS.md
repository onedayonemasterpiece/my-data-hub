# Lane fix-runtime-recovery Results

## Status

Committed.

## Scope

- Requirement IDs: R-H2, R-H3
- Branch: `agent/operational-mvp/fix-runtime-recovery`
- Worktree: `/home/dev/.codex/worktrees/my-data-hub/fix-runtime-recovery`
- Base SHA: `751febf21477cfc6b4fae720c5797756204db05d`
- Implementation commit: `055b0097fc7d8b9a187adb27ab48e309d76bd8b9`

## Delivered behavior

- `RuntimeClient` automatically replays its durable JSONL spool at process construction
  and before every new callback, including heartbeat and terminal callbacks. Replay and
  new delivery are serialized so local sequence order is preserved across the heartbeat
  and main threads.
- Checkpoint publication and terminal-delivery retries are separate stages. Once a
  checkpoint is durably promoted, lost `checkpoint.verified` or terminal acknowledgments
  only flush the existing callback spool; they cannot invoke the checkpoint coordinator
  or consume another fenced epoch.
- The Notebook lifetime is anchored to a monotonic timestamp captured at `main()` entry,
  before configuration, durable HEAD resolution and boot. Boot therefore consumes the
  configured process budget rather than resetting it.
- The real Kaggle notebook push timeout is `42,300` seconds, leaving a declared 900-second
  hard-cutoff reserve below the 12-hour cap. Process runtime plus checkpoint reserve is
  contract-checked against that timeout. These are conservative declared budgets, not
  claims derived from a measured provider run.
- The PostgreSQL master notebook and kernel metadata were regenerated from the updated
  generator and carry the exact `42,300`-second timeout.

## Validation

- Focused runtime/master/provider tests: passed (`23` tests).
- Full `pytest -q`: passed (`1` opt-in test skipped).
- `python -m compileall -q src tests`: passed.
- `ruff check src tests scripts/create_notebooks.py`: passed.
- `scripts/create_notebooks.py --check`: no drift.
- Repository validator: `2,852` checks, zero errors, `ok: true`.
- `git diff --check`: passed.

Focused fault tests cover restart replay, heartbeat-triggered replay ordering, both
verified-callback and terminal-callback acknowledgment loss, no second checkpoint after
promotion, publication-only checkpoint retry, boot-time deadline charging, and the exact
timeout passed to the provider adapter.

## Risks and evidence boundary

- No live Kaggle run was performed. The timeout/reserve values are configuration and
  validation contracts only; provider startup variance and observed shutdown duration
  remain deployment evidence to collect.
- The spool is durable on the Notebook filesystem and supports process recovery where
  that filesystem survives. It does not claim durability across destruction of the
  Kaggle runtime itself.
