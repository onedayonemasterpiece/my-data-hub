# Acceptance evidence control plane

The operational Kaggle matrix uses a metadata-only acceptance ledger. It does
not make the devstand a data plane and does not change the default MCP reader
profile.

## Provider-operator tools

| Tool | Exact purpose |
|---|---|
| `provider.acceptance.dataset.lifecycle` | FM01/FM22 private disposable Dataset create, version, exact readback, and inline cleanup. |
| `provider.acceptance.notebook.lifecycle` | FM01/FM02/FM03/FM06/FM22/FM23 private disposable evidence Notebook source push, numeric run, bounded terminal polling, and selective output fingerprint. The Notebook remains available with `cleanup_state=PENDING`. |
| `provider.acceptance.claim.get` | Read the exact metadata-only result for `(scenario_id, task_id)`. |
| `provider.acceptance.claim.cleanup` | Delete an evidence Notebook only after the caller supplies the exact durable resource claim, numeric run reference, and output-read receipt. |

The lifecycle tools use the single injected `KaggleMCPProviderGateway` and its
single `KaggleProviderAdapter`. Every provider effect has a deterministic
effect identity. The acceptance task is committed as `CLAIMED`, then
`RUNNING`, before the first effect. A lost response is replayed through the
existing effect journal and exact provider readback. An outcome that cannot be
reconciled is terminal `FAILED`; it is never reported as `BLOCKED` after a
mutation may have started.

When one scenario needs both Dataset and evidence Notebook lifecycles (FM01
and FM22), the caller derives distinct deterministic `task_id` values for the
`dataset` and `notebook` subtasks. The ledger key remains
`(scenario_id, task_id)`: a completed subtask therefore cannot mask the other,
while reuse of either task ID with a different exact request is rejected.

Notebook output selection is restricted to one top-level file and a caller
supplied byte limit. The output is compared with the expected SHA-256 and is
then discarded. Only numeric provider version/kernel/run identity,
fingerprints, hashes, and cleanup receipts enter the control ledger. Source,
Dataset file content, raw Notebook output, credentials, and provider logs do
not.

## Runtime event history

`runtime.events.history` is an `acceptance:probe` operator tool. Its exact key
is `(run_id, attempt_id, epoch)`, its limit is capped at 200, and its result
contains envelope metadata only. In particular, it never returns the stored
sanitized event JSON. This supports FM03 without exposing callback payloads or
canonical business bytes.

## Cleanup order

The outer operational matrix runner must reconcile the terminal provider run
and download `operational-result.json` before calling
`provider.acceptance.claim.cleanup`. Cleanup is idempotent: a replay returns
the existing append-only cleanup receipt rather than issuing another logical
delete.
