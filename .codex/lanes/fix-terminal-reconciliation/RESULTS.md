# Terminal reconciliation lane results

- Base: `eef876166ee097b128794d909aeb5ecca5a15c54`
- Branch: `agent/operational-mvp/fix-terminal-reconciliation`
- Scope: exact Kaggle master terminal observation/recovery and terminal callback response-loss idempotency.

## Delivered

- Added a provider-neutral terminal query/evidence contract and a production Kaggle bridge that:
  - observes the exact numeric launched run and configured source identity/version;
  - downloads through the existing `KaggleProviderAdapter` exact-run output-tree fence;
  - bounds the downloaded tree (256 files/64 MiB) and terminal record (256 KiB);
  - accepts only canonical `my-data-hub-master-terminal.v1` JSON;
  - binds run, attempt, service, master, source, epoch, checkpoint and output-tree identities;
  - rejects missing, stale, mismatched or noncanonical output without choosing a generic/latest result.
- Reconciliation now resumes `ACTIVE`, `DRAINING`, `CHECKPOINTING`, and the partial
  `STOPPED` operation / `DRAINING` service state. It requires the exact durable
  `VERIFIED` checkpoint candidate and current HEAD before replaying the exact four-event
  shutdown chain. Fully stopped services leave the startup reconciliation set, so output
  is downloaded once.
- Exact successful output no longer overrides an unknown/nonterminal provider status.
  Success requires exact provider `COMPLETE` plus exact output and durable HEAD evidence.
- Reordered callback deduplication so an exact terminal body replay under its matching
  former per-attempt token is acknowledged after token revocation. Wrong tokens, altered
  bodies and altered run identities remain denied. The coordinator returns that exact
  terminal duplicate without a second lifecycle projection.

## Producer contract / integration

Consumer path and schema match F3 commit `259acdc`:

- `/kaggle/working/my-data-hub-master-terminal.json`
- `my-data-hub-master-terminal.v1`
- canonical JSON, mode `0600`, maximum 256 KiB
- exact identity fields plus current checkpoint `{checkpoint_id, manifest_sha256,
  current_checkpoint_id}`
- four full RuntimeEvent bodies in order: `runtime.draining`, `checkpoint.started`,
  `checkpoint.verified`, `runtime.terminal`, with the exact phases/status/data emitted by
  the F3 producer.

Integration must include F3 `259acdc` before exercising real output recovery. The focused
`store.py` hunks may conflict with the H1 claim/commit lane; preserve both the
`incomplete_operations("ensure_master")` startup selection and the duplicate-before-revoked
token ordering.

## Validation

- `pytest -q tests/control/test_ledger_master.py tests/provider/test_master_runtime_bridge.py`: pass (26 tests)
- `pytest -q`: pass (two existing skips)
- `python -m compileall -q src tests`: pass
- `python scripts/validate_repository.py`: pass (`2867` checks, zero errors)
- `ruff check .`: pass
- `git diff --check`: pass

Focused integration tests cover all-callback loss across restart, partial recovery from
`DRAINING`/`CHECKPOINTING`/`STOPPED`, exact-once output download, stale output denial, and
commit-then-lost HTTP response replay draining the durable RuntimeClient spool.

## Critical selective-output follow-up

- The production bridge no longer calls broad `download_exact_run_output_tree`. It uses
  the same official adapter through `download_exact_run_output_file`, which passes the
  anchored pattern `^my\-data\-hub\-master\-terminal\.json$` to Kaggle
  `kernels_output`, fences the exact numeric run/source before and after, and requires the
  final destination to contain exactly one top-level regular non-symlink file no larger
  than 256 KiB. Pinned Kaggle 2.2.4 also writes `<kernel-slug>.log` independently of the
  pattern; the adapter permits only that deterministic extra path, bounds it to 1 MiB,
  and unlinks it before returning. Any other path, missing receipt, oversized receipt/log,
  or API that ignores the pattern fails closed; there is no broad-output fallback.
  Kaggle 2.2.4 buffers the exact matched receipt before the post-download 256 KiB check;
  this is a recorded SDK residual, bounded by the producer contract and rejected locally
  before parsing. A realistic nonempty-provider-log test covers the pinned behavior.
- Before projecting recovered lifecycle state, the coordinator writes an idempotent
  append-only audit receipt containing only provider status, exact identities,
  checkpoint/output hashes, and the four event IDs/body hashes. Runtime event bodies are
  not copied into the control ledger. A focused failure-injection test proves the audit
  is durable before the first projection and exact retry does not duplicate it.
- Persistent callback-outage completion is closed by the coordinated F3 final tip
  `bbceb2faa9d419df05acc30446f3d6ea706cae84` (implementation `114084c`): `run_master`
  suppresses only the dedicated callback lease-closure exception, and only after helper
  success plus exact artifact/receipt revalidation. The spool truthfully remains queued
  and unacknowledged; unrelated active errors still propagate. Run-master-level tests
  cover both paths, its full suite passes with one skip, and its validator reports
  2,878 checks with zero errors.

Follow-up validation: focused provider/control tests pass (37 tests); full `pytest -q`
passes with one remaining opt-in skip when pinned Kaggle 2.2.4 is installed; compileall,
repository validation (2,867 checks), full Ruff, and `git diff --check` pass.
