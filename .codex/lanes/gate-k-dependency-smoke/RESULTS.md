# Gate K central dependency smoke

- Added one-adapter, deterministic, disposable private/offline Kaggle dependency smoke coordinator.
- The central process alone converts the exact `imports_passed` observation into a canonical v1 verified receipt; workers receive the receipt and hash in the private task status Dataset and execution pins.
- State and receipt are secret-free, atomic, fsync'd, and mode 0600. Cleanup is claim-bound and replayed after restart/response loss.
- Control lazy reconciliation runs smoke only after the exact master asset claim and keeps embedding admission closed until the matching receipt exists.
- No live provider mutation was performed in this lane.

## One-shot callable

The deploy process can construct exactly one `KaggleProviderAdapter.from_environment(journal=ControlLedgerKaggleJournal(ledger))`, then instantiate `CentralDependencySmoke` with the exact numeric built asset Dataset ref and private absolute state/receipt paths and call `run_once()` on the bounded control reconciliation interval. This is the same callable used by `create_app`; it never creates a second client and never places credentials in a Notebook. A standalone disposable-input provisioning CLI was not added because the central adapter currently has no claim-bound transactional composite that can prove both input-Dataset and Notebook absence after response loss; fabricating that guarantee would exceed this closure.
