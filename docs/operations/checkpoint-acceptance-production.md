# Production checkpoint acceptance adapters

`my_data_hub.checkpoints.acceptance_runtime` connects the fixed FM05, FM14 and
FM15 coordinator to the durable control ledger, checkpoint registry, and the
repository's single control-owned `KaggleProviderAdapter`. Control migration
021 adds only task authority/launch metadata; it adds no generic upload or
fault selector.

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
and dedicated request/task/attempt control identity. The authority carries
`acceptance:operate` and deliberately has no master instance or epoch. The
launch port validates the
protected input claims before launch; the config hash lets the outer status
path prove that the Notebook consumed that same binding. The config contains no
token, Kaggle credential, PostgreSQL URL, checkpoint bytes or verifier bytes.

The control process creates a unique private disposable status Dataset before
the Notebook push. Its exact version contains only bounded `kaggle_run.json`
(`run_id`, `attempt_id`, kind, Notebook, credential-free callback URL,
one-time token, the exact bounded Notebook resource lease and the execution-pins
hash), the fixed bootstrap helper, and a credential-free `execution-pins.json`.
The ledger stores only the token hash and exact Dataset claim/hashes. The
Notebook verifies both files, exports `MY_DATA_HUB_RUN_SECRET` locally and then
uses the existing Bearer/header transport with redacted JSONL fallback. It
never receives a manually provisioned callback root User Secret. The status
Dataset is deleted through its exact disposable claim after a reconciled
terminal run; ambiguous launch response retains it rather than risking deletion
under an unknown live run. Control acquires that task/notebook lease before the
push and idempotently releases the exact lease epoch only with cleanup.

The fixed source adapts the canonical events-bot envelopes through
`RuntimeClient`: stable `event_uid` values cover `kernel_started`,
`preflight_ok`, periodic `alive`, `report_written`, resource acquire/release and
terminal/failure. Alive progress includes the phase, monotonic elapsed seconds,
heartbeat count and completed-step count. The append-only control event ledger
rejects changed-body UID/sequence replay and projects bounded typed phase,
progress, event counts and receipt hashes into acceptance status. A completed
provider run is not ready evidence until report, resource release and terminal
events have all been durably observed. Authentication remains only in Bearer
and dedicated task headers; callback JSON and
`kaggle_status_events.jsonl` contain no token.

The deployment runtime input is fail-closed on one immutable Kaggle image
digest, `docker_image_pinning_type: original`, the image source commit and the
CPython series. The central adapter binds those fields and the exact numeric
private Dataset inputs to the push intent and generated Kaggle metadata. Before
copying a template or starting the checkpoint entrypoint, the rendered script
verifies the hashed execution-pins document, the observed `/etc/git_commit`, the
running Python series, and that `/kaggle/input` exposes exactly the pinned
Dataset slugs as regular directories. Its synchronous `kernel_started` event
carries only the executed source hash and sanitized pin/image/Dataset metadata;
the control ledger must accept the expected source hash before the checkpoint
broker authorizes any mutation.

Checkpoint acceptance Notebooks receive **no Kaggle credential or User Secret**.
The deployment contract requires `brokered_checkpoint_upload: true` and rejects
the former `kaggle_secret_bindings` shape. The rendered source contains no
`kaggle_secrets`, `KAGGLE_USERNAME`, `KAGGLE_KEY`, or `KAGGLE_API_TOKEN`.

FM05/FM14/FM15 use the same brokered direct-upload control service as the normal
master: metadata goes to control, the one central adapter creates per-file signed
PUT capabilities and retains blob tokens, and checkpoint bytes flow
Notebook→Kaggle. The task-bound authority comes only from the persisted launch,
source-attested evidence run, fixed scenario/config and absolute expiry; it does
not impersonate a master epoch. The Notebook-side factory never constructs an
account-authenticated adapter. Injected fake-adapter tests remain contract-only
and can never emit live evidence.

FM05 must name the configured normal checkpoint Dataset and advances its exact
next version only after the central independent verifier passes. FM14 and FM15
remain disposable Datasets and leave HEAD unchanged. FM14 permits only the fixed
same-size `physical/base.tar.gz` hash mismatch. FM15 launches its fixed verifier
through the central adapter and validates the bounded output of the exact failed
run. Broker publication status retains typed verifier run/source/output/receipt
hashes and exact completed-file metadata so a restarted evidence Notebook skips
already uploaded files without asking for another signed URL.

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

## Production opt-in

`MY_DATA_HUB_CHECKPOINT_ACCEPTANCE_DEPLOYMENT_FILE` points to one private,
bounded `my-data-hub-checkpoint-acceptance-deployment.v1` document containing
only exact provider refs/version claims/hashes, immutable runtime image
provenance and the literal brokered-upload requirement.
The schema and example are
`schemas/checkpoint-acceptance-deployment.v1.schema.json` and
`examples/provider/checkpoint-acceptance-deployment.v1.example.json`.

The installer exposes the unified tools only for the operator profile when
`MY_DATA_HUB_ENABLE_ACCEPTANCE_SCENARIOS` equals
`I_ACKNOWLEDGE_PROTECTED_ACCEPTANCE_EFFECTS`. It adds `acceptance:operate` and
enables the same single control gateway for control and remote MCP. FM08 remains
honestly blocked: control-process callback recovery is not the required abrupt
master termination with distinct old/recovery provider runs.
