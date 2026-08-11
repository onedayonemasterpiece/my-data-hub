# Production checkpoint acceptance adapters

`my_data_hub.checkpoints.acceptance_runtime` connects the fixed FM05, FM14 and
FM15 coordinator to the existing durable control ledger, checkpoint registry,
and the repository's single `KaggleProviderAdapter`. It does not add a control
endpoint, migration, generic upload operation, or fault selector.

## Fixed binding

Create one `CheckpointAcceptanceRuntimeBinding` for one scenario. The owner
configuration fixes the operation UUID, task-run UUID, 40-hex source revision,
exact owned Dataset and Notebook refs, start time, verified empty-checkpoint
template, working directory, and verifier source. Public effect methods accept
only the resulting `CheckpointAcceptanceIntent`; callers cannot pass refs,
package bytes, paths, corruption bytes, commands, or failure modes.

Both package paths must be real absolute paths below `/kaggle/working`. The
template must be a verified PostgreSQL 18 checkpoint with canonical revision
zero and zero canonical rows. Missing or invalid assets fail before a provider
mutation with `CHECKPOINT_ACCEPTANCE_TEMPLATE_INVALID` or
`CHECKPOINT_ACCEPTANCE_EMPTY_TEMPLATE_REQUIRED`.

The binding start plus 900 seconds is the absolute operation deadline. Every
adapter retry budget is reduced to the remaining time, Notebook execution gets
the remaining bounded timeout, and polling gets a remaining-time
`PollPolicy`. An expired or future start fails closed.

## Durable journal

`ControlLedgerCheckpointAcceptanceJournal` uses existing general operations and
effects, with no SQL migration:

- the exact typed intent is committed as `INTENT_COMMITTED` before effects;
- deterministic stage effects retain bounded typed stage receipts;
- deterministic failure effects make the third failed attempt terminal;
- a deterministic terminal effect retains the exact final receipt before the
  operation becomes `DURABLE_COMPLETE`;
- conflicting operation, idempotency, intent, stage ordering, or receipt replay
  is rejected.

A process restart can reconstruct the intent, completed stages, attempt count,
terminal failure, and final receipt from the ledger alone.

## Provider lifecycle

- **FM05:** materialize the task candidate from the fixed empty template, add
  it to the registry, reconcile/create or version the protected private
  checkpoint Dataset, download its exact numeric version, run the independent
  restore verifier Notebook, mark the verification gates, and promote only by
  the committed HEAD generation CAS. Promotion replay accepts only the exact
  `generation+1/current=candidate/previous=old-current` tuple.
- **FM14:** copy the fixed template and flip byte zero of
  `physical/base.tar.gz` after writing its manifest. Upload the private
  task-owned disposable Dataset through a persisted provider effect, download
  exact version 1, prove manifest hash differs from the read byte hash, reject
  the registry candidate, and never call promotion.
- **FM15:** upload and exactly read back the disposable candidate, then run the
  fixed restore-failure verifier. `download_exact_failed_run_output_file`
  accepts only raw provider status `failed` or `error`, fences the exact source
  and status before and after the official SDK output call, and admits only the
  strict `checkpoint-acceptance-fm15-failure.v1` receipt. That receipt binds
  run, candidate, exact Dataset version, source commit, manifest hash, content
  hash and fixed failure code. Missing/mismatched receipts and cancellation or
  unrelated failure statuses remain failed acceptance attempts; only the exact
  receipt permits candidate rejection. Promotion is never called.

Dataset and Notebook mutations call the adapter's reconcile operation before a
create/push. Provider effect IDs and request hashes are deterministic and the
adapter journal commits intent before mutation. A response lost after a Kaggle
effect therefore resumes the same physical resource/run rather than launching
another one. Negative scenarios keep current and previous HEAD unchanged.

## Evidence classification

The runtime reports `evidence_class=live` only when the concrete adapter is the
repository `KaggleProviderAdapter` backed by the official `kaggle` `KaggleApi`.
Injected/fake APIs remain `injected` and can produce only `CONTRACT_PASS`.
Checked-in tests are contract evidence, not Kaggle evidence. No live receipt is
claimed without the official adapter receipts, exact readbacks, verifier run,
and durable registry/ledger state.

The FM15 failed-verifier metadata contract is defined by
`schemas/checkpoint-acceptance-fm15-failure.v1.schema.json` with a sanitized
example under `examples/contracts/`. It contains identities and hashes/status
only, never checkpoint or business bytes.

The pinned `kaggle==2.2.4` SDK applies `file_pattern` to output files but writes
a supplied response log as `<kernel-slug>.log` independently of that anchored
pattern. This creates an unavoidable post-download residual of the receipt (at
most 64 KiB) plus that log (at most 1 MiB). The adapter rejects an oversized log
and any third path, removes the log before return, and deletes the entire
destination on failure. It never requests or accepts a broad output tree.

## Kaggle evidence entrypoint

The task-owned evidence Notebook invokes exactly:

```bash
python scripts/provider/checkpoint_acceptance_evidence.py \
  --config /kaggle/working/checkpoint-acceptance-config.json \
  --output /kaggle/working/operational-result.json
```

The owner matrix host must launch and independently reconcile that Notebook; it
must not run this command on the host. The config, output, template, verifier,
working directory and local metadata journal are all restricted to descendants
of `/kaggle/working`. The config must be a regular non-symlink file with mode
`0600` and size at most 256 KiB. Its strict v1 contract binds one scenario,
operation/task/source/start identity, owner Dataset and Notebook refs, exact
numeric protected template Dataset version/claim plus manifest/content hashes,
the task-owned evidence Notebook ref, exact numeric protected verifier Dataset
version/claim/path/hash and isolated verifier Notebook ref for FM05 or FM15,
and run/attempt/master/epoch control identity. The launch port validates the
protected input claims before launch; the config hash lets the outer status
path prove that the Notebook consumed that same binding. The config contains no
token, Kaggle credential, PostgreSQL URL, checkpoint bytes or verifier bytes.

`MY_DATA_HUB_RUN_SECRET` is the only accepted control credential. Kaggle
credentials use the official SDK's environment/User Secret discovery. There is
no CLI/config credential override. Before creating the local acceptance journal
or permitting any adapter/provider mutation, the factory validates the modern
runtime token by resolving the bounded remote checkpoint HEAD. It then verifies
the template and verifier assets and requires the exact official
`KaggleProviderAdapter`, Kaggle `KaggleApi`, remote provider journal and remote
checkpoint registry types. An injected adapter can never emit live evidence.

The fixed local journal is
`<working_directory>/checkpoint-acceptance-control.sqlite3`, mode `0600`. A
rerun with the same config reopens it and uses the same operation, task,
candidate and provider effect IDs. Provider effects additionally reconcile
against the remote journal/registry. The absolute deadline remains exactly 900
seconds from `started_at`; a new deadline cannot be selected during replay.

The bounded output schema is
`schemas/checkpoint-acceptance-operational-result.v1.schema.json`:

- `LIVE_EVIDENCE_READY` (exit 0) embeds the exact durable `LIVE_PASS` receipt,
  its SHA-256, flattened HEAD/intent/candidate/stage identities and the owner
  evidence/verifier Notebook, protected-input claim and produced exact-version
  locator. This is intermediate evidence, not a
  matrix `PASS`; the outer driver must validate and bind it to its planned task.
- `BLOCKED` (exit 78) is allowed only for a named missing/auth/interface/asset
  capability before mutation and has `mutations_started=0`.
- `FAIL` (exit 1) is mandatory after journal/action entry or whenever mutation
  status may be ambiguous. It never includes exception text, credentials or
  business bytes and conservatively reports `mutations_started=1`.

Config and sanitized result examples live under `examples/provider/`.
