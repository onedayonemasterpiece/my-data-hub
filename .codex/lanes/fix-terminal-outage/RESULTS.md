# Lane fix-terminal-outage Results

## Status

Committed and clean.

## Identity

- Finding: critical F3 terminal callback outage follow-up
- Base SHA: `259acdc`
- Branch: `agent/operational-mvp/fix-terminal-outage`
- Implementation commits:
  - `c07a17dfac42e72208ca1fe2e48e4b99a8d00c37`
  - `114084ce9a25ce67607d1e2399df81f39685b728`

## Delivered behavior

- Terminal callback delivery keeps its existing bounded retries.
- If those retries exhaust after durable checkpoint promotion, the runtime re-reads and
  validates the exact canonical terminal artifact: regular non-symlink file, mode `0600`,
  at most 256 KiB, typed identity/event/checkpoint contract, canonical encoding, and the
  exact promoted checkpoint ID, current ID and manifest hash.
- Only with that recovery evidence intact does the master stop the tunnel and PostgreSQL
  cleanly and return success, allowing Kaggle to expose the exact private output tree to
  the lifecycle recovery projector.
- Pending callbacks remain pending in the durable local spool. No delivery ACK or control
  callback success is fabricated.
- The active loop now raises a dedicated `CallbackLeaseClosingError` when callback loss
  closes the write lease. After checkpoint shutdown succeeds, `run_master` suppresses
  only that exact expected closure and only after independently revalidating the exact
  terminal artifact against the returned durable checkpoint receipt. Unrelated active
  tunnel/database/runtime errors still propagate and make the provider run fail.
- A missing/invalid terminal artifact still fails closed and leaves processes unstopped.
  Checkpoint publication failure behavior is unchanged, and terminal retries never invoke
  a second checkpoint publication.

## Validation

- Focused master/runtime/provider suite: `33` tests passed.
- Full `pytest -q`: passed with one opt-in test skipped.
- `python -m compileall -q src tests`: passed.
- `ruff check src tests scripts`: passed.
- `scripts/create_notebooks.py --check`: no drift.
- Repository validator: `2,878` checks, zero errors, `ok: true`.
- `git diff --check`: passed.

The persistent-outage test uses the real `RuntimeClient` and offline transport. It proves
that all four lifecycle event bodies remain queued with no delivered records, the exact
terminal artifact exists, only one checkpoint was published, both processes stopped, and
the checkpoint coordinator returned cleanly. A paired negative test proves that the same
outage without exact terminal output remains failed and unstopped.

A run-master-level parameterized test drives the full boot/activation/active/checkpoint
path. It proves callback lease closure returns zero only with exact terminal recovery,
while an unrelated active-loop failure is re-raised after the same durable checkpoint.
