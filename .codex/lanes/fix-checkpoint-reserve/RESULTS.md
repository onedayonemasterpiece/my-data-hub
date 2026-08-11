# Lane fix-checkpoint-reserve Results

## Status

Committed.

## Scope

- Finding: F3-R
- Base SHA: `eef876166ee097b128794d909aeb5ecca5a15c54`
- Branch: `agent/operational-mvp/fix-checkpoint-reserve`
- Implementation commit: `0af4284f8e22b375c054c5be5d9c8a3b34035262`

## Delivered behavior

- The checkpoint reserve is now exactly 10,800 seconds, divided into two
  independently admitted 5,400-second publication-attempt allocations. A first
  attempt or retry is not started unless its complete allocation remains.
- Admission is explicitly conservative. It does not claim that an already-started
  third-party provider call can be cancelled at the absolute process deadline.
- A 60-second transition guard separates the ACTIVE deadline from the checkpoint
  reserve. Readiness is rejected before `service.ready` when boot consumed the
  ACTIVE window, and the fixed deadline is checked again before local write-gate
  activation. Failure fences/stops the booted runtime rather than opening writes.
- The active loop uses a bounded final sleep and cannot knowingly begin another
  heartbeat cycle after its deadline.
- After durable checkpoint promotion, the master writes canonical, typed, atomic,
  fsync-backed mode-`0600` recovery evidence to
  `/kaggle/working/my-data-hub-master-terminal.json` before callback-delivery
  failure can raise. The bounded file contains the exact four RuntimeEvent bodies
  from the durable spool and the exact checkpoint/identity binding agreed with the
  lifecycle recovery consumer.
- The runtime spool exposes ACKed event bodies without reconstructing them, so the
  recovery artifact contains the exact callback payload dictionaries.

## Validation

- Focused master/runtime/provider tests: passed (`29` tests).
- Full `pytest -q`: passed (`1` opt-in test skipped).
- `python -m compileall -q src tests`: passed.
- `ruff check src tests scripts`: passed.
- `scripts/create_notebooks.py --check`: no drift; the generated master Notebook did
  not require regeneration because its provider timeout/source template is unchanged.
- Repository validator: `2,866` checks, zero errors, `ok: true`.
- `git diff --check`: passed.

Fault tests prove exact-minimum reserve validation, boot-time readiness refusal before
activation, first-attempt refusal before drain when 5,400 seconds are unavailable,
second-attempt refusal when another full allocation is absent, exact-bound admission,
and terminal recovery output persistence before a lost terminal acknowledgment raises.

## Integration dependency and evidence boundary

- The 5,400-second allocation depends on the integration owner aligning the checkpoint
  archive/provider/verifier sequential stage maxima to fit one attempt after the F1/H1
  checkpoint-runtime work. This lane intentionally did not edit
  `checkpoints/kaggle_runtime.py`, which is owned by that lane. Crash-safe stage
  resumption is correctness behavior, not a timing guarantee.
- No live Kaggle run or timing measurement was performed. The values above are declared
  admission contracts, not observed provider performance evidence.

