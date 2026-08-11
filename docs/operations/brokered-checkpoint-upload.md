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
