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
- **FM15:** upload a valid private task-owned disposable candidate, prove exact
  readback, push/reconcile the fixed isolated verifier source bound to the exact
  Dataset version, require its terminal failure, reject the candidate, and
  never call promotion.

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
