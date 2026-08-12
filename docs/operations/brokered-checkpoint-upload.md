# Brokered direct checkpoint upload

The PostgreSQL master never receives a Kaggle account credential and never creates a
second Kaggle SDK client. Checkpoint publication is split between the existing central
lifecycle adapter and the active, epoch-fenced master:

1. The master creates the fixed physical, WAL, logical, verification and manifest files
   below `/kaggle/working` and re-hashes every file immediately before upload.
2. The task-bound TLS client sends only the operation/run/epoch, file name, size, content
   type, file SHA-256 and manifest SHA-256 to control.
3. Control verifies the exact active service lease and immutable checkpoint candidate,
   persists a `STARTING` effect, and asks the one central Kaggle adapter to start a blob
   upload.
4. Control AES-GCM seals the opaque blob token and signed URL in the private mode-`0600`
   ledger. Only the short-lived HTTPS URL is returned once to the exact master.
5. The master streams the file directly to Kaggle storage. The devstand never accepts a
   checkpoint body.
6. After all exact completion metadata is durable, the central adapter finalizes one
   private Dataset version. A lost provider response is reconciled by numeric version,
   canonical per-file descriptions and hashes; no blind second version is created.
7. A separate pinned verifier Notebook restores the exact numeric version, validates
   PostgreSQL 18, extensions, schema and read/vector probes, and returns metadata only.
8. Control CAS-promotes the candidate only after the verifier receipt passes. The former
   current checkpoint becomes previous. Any mismatch leaves HEAD unchanged and marks the
   candidate failed or quarantined.

The runtime coordinator registers the immutable candidate before requesting its first
blob grant. Publication status exposes only completed file name/size/SHA-256 metadata;
after a restart the Notebook skips an exact completed file instead of trying to reissue
its one-time signed URL.

## Deterministic restart boundaries

The mode-`0600` SQLite ledger, not process memory, is the restart journal. Focused tests
close the production boundaries with a newly constructed `ControlLedger`, broker service
and runtime provider over the same on-disk ledger:

- If the process disappears after one or several PUT completions were durably accepted,
  the new runtime reads the exact `completed_files` identities and skips them. It neither
  requests another blob grant nor repeats the PUT. Public status and the database file
  contain no plaintext URL, signature or blob token; the encrypted token remains only in
  the protected ledger until Dataset resolution.
- `FINALIZING` persists the expected numeric Dataset version before the provider effect.
  Loss of the finalize response is recovered by exact version and file-description
  reconciliation in a fresh service; finalization is not issued twice.
- `VERIFIED` persists the bounded, secret-scanned typed verifier receipt, its SHA-256,
  provider run reference and exact Dataset ref before HEAD CAS. A fresh service uses that
  evidence without launching another verifier and advances HEAD once.
- If the process disappears after the HEAD transaction commits but before the broker
  journal records `PROMOTED`, the durable registry recognizes the exact candidate at
  generation `source_head_generation + 1`. It reconciles the journal without a second
  generation advance. Any different generation/current identity remains a hard conflict.

Failure tests start with both an empty HEAD and an existing verified HEAD. Dataset or
verifier failure leaves current and previous exactly unchanged; no failure path promotes
a candidate or rewrites the prior pair.

## Task-bound acceptance

The FM05/FM14/FM15 evidence Notebook uses the same metadata client and central adapter.
Its authority is checkpoint-bound and derives from the persisted acceptance launch,
source-attested provider run, fixed deployment config and expiry. FM05 shares the normal
checkpoint Dataset and follows exact-version verifier→readback/restore proof→CAS
promotion ordering. FM14/FM15 create disposable private Datasets and never promote HEAD;
their expected rejection codes are terminal acceptance evidence rather than a successful
normal publication. Typed verifier evidence is bounded and secret-scanned before it is
retained in the private ledger/public metadata projection.

## Security boundaries

- `KAGGLE_USERNAME`, `KAGGLE_KEY`, token files, Kaggle CLI and kagglehub are forbidden in
  the master runtime. Startup rejects ambient credential variables or files.
- The signed URL is a temporary bearer capability. It is never included in exceptions,
  logs, status, callbacks, MCP, status Datasets or receipts, and is deleted after an exact
  completion.
- The blob token never leaves the central adapter/ledger and is deleted after the exact
  Dataset version is resolved.
- The broker accepts only the six names defined by the checkpoint manifest contract and
  at most 20 GiB total. Request bodies are capped at 256 KiB and cannot contain bytes.
- A provider blob-start response loss is quarantined because the official API offers no
  exact lookup for an orphaned blob token. Dataset-version response loss is recoverable
  through exact version reconciliation, bounded to three attempts.

## Operator checks

The installer creates the broker key automatically. Verify it without printing it:

```bash
test "$(stat -c '%a:%s' "$MY_DATA_HUB_CHECKPOINT_UPLOAD_BROKER_KEY_FILE")" = "600:32"
docker compose -f compose.control-plane.yaml config >/dev/null
```

The metadata-only publication projection is available only to the exact runtime at
`GET /internal/checkpoints/{checkpoint_id}/publication`. A successful terminal record
must have `state=PROMOTED`, an exact numeric `exact_version_ref`, verifier run reference
and verifier receipt SHA-256. The raw URL and token are never part of this projection.

## Disposable live canary

The live canary must use the same deployed control endpoints and central credential as a
normal master. It creates a random opaque bundle, uploads it through broker claims,
verifies the exact private numeric Dataset version in a separate Kaggle Notebook, and
deletes the canary Dataset and both Notebooks. The receipt records only provider/run IDs,
numeric version, sizes, hashes, operation IDs, cleanup hashes and secret-scan status.
Until that receipt exists, this implementation is code-complete but not operationally
accepted.

The executable implementation is `scripts/provider/broker_live_canary.py`. It preserves
the production split rather than testing a convenient local upload:

1. The central process constructs exactly one `KaggleProviderAdapter` from the pinned
   official SDK and starts one brokered Dataset blob.
2. A private disposable producer Notebook is pushed without a Kaggle credential. Its
   source contains only the short-lived signed `create_url`; it deterministically creates
   the opaque 4 KiB canary and performs the direct HTTPS `PUT`. The blob token never
   leaves the central process.
3. Central finalization uses `BrokeredDatasetFile` and the same canonical description
   fields as the deployed checkpoint broker (`operation_id`, `master_run_ref`, `epoch`,
   manifest/file SHA-256, and total bytes). It must reconcile private Dataset version 1
   exactly before continuing.
4. A second credential-free Notebook is attached to the exact numeric Dataset input. It
   independently verifies the byte count and SHA-256 and emits bounded status JSON.
5. Claim-bound deletion removes the verifier Notebook, Dataset, and producer Notebook.
   A final paginated owned-inventory read must prove all three exact refs absent.

The runner uses an atomic, mode-`0600`, metadata-only custom-state file, following the
status-file pattern used by the established Kaggle runners. The state and public receipt
reject `create_url` and `blob_token`; neither stores a capability. A small central
readback of this disposable canary is used only to derive the adapter's exact deletion
fingerprint. Production checkpoint bytes still travel only Notebook → signed Kaggle blob
and never through the devstand.

Run only from a clean deployed commit and keep all mutable evidence outside the checkout:

```bash
set -a
. /path/to/private/provider.env
set +a
uv run --no-project --with-editable . --with kaggle==2.2.4 \
  python scripts/provider/broker_live_canary.py \
  --ledger /private/evidence/broker-live-canary.sqlite \
  --state /private/evidence/broker-live-canary-state.json \
  --receipt /private/evidence/broker-live-canary-receipt.json
```

No `--fake`, CLI, `kagglehub`, direct second client, or skip-cleanup mode exists. Missing
credentials exit with code 78 before a mutation. Unit-test adapters produce only a
`SIMULATED` receipt; the receipt model/schema permit `PASS` only for `observed` + `live`
execution with provider mutations, all three cleanup receipts, and inventory absence.
The checked-in example is explicitly `NOT_RUN` and cannot be presented as live evidence.
