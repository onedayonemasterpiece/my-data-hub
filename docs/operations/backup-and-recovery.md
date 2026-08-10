# Master checkpoint and recovery contract

Status: `TOOLING PRESERVED / KAGGLE-MASTER REBINDING DEFERRED`

Production durability belongs to the Kaggle master Notebook and private Kaggle Datasets,
not a devstand backup timer.

Required generations:

- current verified checkpoint;
- previous verified checkpoint;
- less frequent portable encrypted logical backup.

Promotion chain:

```text
consistent master backup -> manifest -> private exact dataset version
-> readback/hash -> isolated restore smoke -> atomic HEAD advance
```

Receipts include source instance/run/epoch, PostgreSQL major, extension versions,
schema/canonical revision, counts, hashes, dataset ref/version, readback and restore result.
A failed candidate never replaces current/previous. Drain closes new writes and finishes
transactions before checkpointing; an expired epoch cannot reopen the DB gate.

Existing backup/restore scripts and schemas remain reusable tooling. They are not wired to
the devstand production installer and must be adapted/tested inside the master Notebook
phase. No production checkpoint or restore was executed by PR-A.

## Preserved detailed contract — bound by ADR-0016

The detailed material below is retained where topology-neutral. Any reference to a database, role, committer, backup or connector application is executed inside/against the latest ACTIVE Kaggle master; devstand execution claims are superseded.

## Purpose

Backups protect recovery from operator error, software defect and host/storage loss. They
are not an authorization mechanism and do not justify exposing a database owner or
unbounded remote writes.

## Backup layers

1. PostgreSQL logical backup (`pg_dump` custom format) for portable recovery.
2. Optional physical/base backup or WAL continuity when measured RPO requires it.
3. Encrypted master-runtime generation with manifest/hash.
4. Encrypted private off-host copy, initially a protected private Kaggle Dataset or
   another approved target.
5. Schema/migration code in GitHub.
6. Separate artifact/connector/provider/operator receipts and hashes.

Kaggle backup datasets use `orchestrator_protected`. Remote MCP exposes freshness/status
only, never dump files, version/delete/download.

## Implemented recovery tools

The repository tools are deliberately provider-neutral and fail closed:

- `scripts/backup_postgres.sh` streams `pg_dump --format=custom` directly into `age`.
  The accepted local artifact is `*.dump.age`; the script never creates a plaintext dump
  file. It writes a mode-0600 v2 manifest bound to the encrypted artifact SHA-256 and
  byte size.
- `scripts/recovery/offhost_roundtrip.py` calls an explicit upload adapter followed by an
  independent readback adapter. It accepts evidence only when the read-back file has the
  exact manifest SHA-256 and byte size. Adapter output is discarded so a provider error
  cannot accidentally place credentials in logs or receipts.
- `scripts/restore_postgres.sh` accepts the historical two backup arguments plus an
  optional third off-host evidence argument. It decrypts only after the manifest,
  off-host evidence, isolated-target identity, connected database name, and zero-user-
  relation freshness check pass. Restore uses `--exit-on-error --single-transaction`,
  executes `my-data-hub db verify`, and writes a recovery receipt only after success.

These tools do not provision, promote, or destroy a PostgreSQL server. Provisioning and
destruction remain explicit operator/infrastructure actions. In particular, a successful
receipt sets `automatic_promotion=false` and is evidence for a later decision, not
permission to mutate production.

### Backup invocation

Keep the database URL and age identity in Kaggle User Secrets/short-lived master secret injection, not shell history.
The age recipient is public key material, but its value is represented in the manifest
only by a SHA-256 fingerprint.

```bash
export MY_DATA_HUB_BACKUP_DATABASE_URL='<from secret store>'
export MY_DATA_HUB_BACKUP_AGE_RECIPIENT='age1...'
export MY_DATA_HUB_BACKUP_SOURCE_INSTANCE='production-primary'
export MY_DATA_HUB_BACKUP_SOURCE_ENVIRONMENT='production'
export MY_DATA_HUB_BACKUP_ROOT='/var/lib/my-data-hub/backups'
scripts/backup_postgres.sh
```

Required backup gates are the database URL, age recipient, non-secret source instance
and environment labels, `pg_dump`, `age`, and a Git commit digest (normally discovered
from the checkout). A missing gate, an invalid retention value, or either side of the
`pg_dump | age` pipeline failing produces no accepted manifest.

### Exact off-host adapter contract

An adapter is an absolute path to one executable, not a shell expression. The operator
may use a provider-specific wrapper whose credentials come from the provider's normal
secret mechanism. The orchestrator invokes it with no arguments and the following
environment contract:

| Variable | Upload | Readback |
| --- | --- | --- |
| `MDH_RECOVERY_ACTION` | `upload` | `readback` |
| `MDH_RECOVERY_PROVIDER` | non-secret provider label | same |
| `MDH_RECOVERY_OBJECT_LOCATOR` | credential-free immutable locator | same |
| `MDH_RECOVERY_SOURCE_PATH` | encrypted artifact path | empty |
| `MDH_RECOVERY_DESTINATION_PATH` | empty | new protected temporary path to create |

The upload adapter must synchronously finish the upload and return zero. The readback
adapter must download provider bytes into `MDH_RECOVERY_DESTINATION_PATH` and return
zero. It must not trust or copy the local source path. The orchestrator hashes that
readback itself; a provider-reported checksum alone is insufficient.

```bash
export MY_DATA_HUB_OFFHOST_UPLOAD_CONFIRM='UPLOAD_ENCRYPTED_BACKUP'
export MY_DATA_HUB_OFFHOST_PRIVATE_CONFIRM='PRIVATE_ENCRYPTED_STORAGE'
export MY_DATA_HUB_OFFHOST_IS_REMOTE='OFF_HOST_PRIVATE_STORAGE'
export MY_DATA_HUB_OFFHOST_UPLOAD_ADAPTER='/opt/my-data-hub/bin/provider-upload'
export MY_DATA_HUB_OFFHOST_READBACK_ADAPTER='/opt/my-data-hub/bin/provider-readback'
python3 scripts/recovery/offhost_roundtrip.py \
  --artifact "$artifact" --manifest "$manifest" \
  --provider approved-private-store \
  --object-locator 's3://private-bucket/immutable/object.dump.age' \
  --evidence "$evidence"
```

The three confirmations are intentional: running a repository test cannot silently make
an external mutation. The locator must contain no credentials, query string, or fragment.
Failed upload, timeout, missing readback, byte-size mismatch, or SHA-256 mismatch produces
no accepted evidence file.

### Isolated restore invocation

Provision a new compatible database first. Do not set the restore URL to the canonical
database. The script rejects a URL equal to a configured canonical/backup URL, but that
string comparison is defense in depth rather than proof of network isolation; the
operator remains responsible for firewall, account, and host isolation.

```bash
export MY_DATA_HUB_RESTORE_CONFIRM='RESTORE_MY_DATA_HUB'
export MY_DATA_HUB_RESTORE_ISOLATED_CONFIRM='ISOLATED_FRESH_TARGET'
export MY_DATA_HUB_RESTORE_DATABASE_URL='<fresh target URL from secret store>'
export MY_DATA_HUB_RESTORE_TARGET_ID='recovery-drill-20260809'
export MY_DATA_HUB_RESTORE_EXPECTED_DATABASE='my_data_hub_recovery'
export MY_DATA_HUB_RESTORE_AGE_IDENTITY_FILE='/run/secrets/backup-age-identity'
export MY_DATA_HUB_RECOVERY_RECEIPT='/var/lib/my-data-hub/receipts/recovery-20260809.json'
scripts/restore_postgres.sh "$artifact" "$manifest" "$evidence"
```

The receipt path must be new. The identity path must be a regular non-symlink file. The
target ID must differ from the source instance recorded in the manifest. The database
must have zero non-system relations: this script never uses `--clean` to make a populated
target appear fresh. Decrypted bytes exist only in a mode-0600 temporary file for the
duration of `pg_restore` and are removed by an exit trap.

### Receipt contract

Successful drills emit `schemas/recovery-receipt.v1.schema.json`; a non-production
example is `examples/recovery-receipt.v1.json`. The receipt binds:

- encrypted artifact and manifest hashes;
- source instance/environment and Git revision, without a database URL;
- credential-free off-host locator, upload/readback hashes and evidence self-hash;
- isolated target label/database, zero-relations precheck and both restore/verifier
  outcomes;
- a canonical JSON self-hash calculated with `receipt_sha256` omitted.

No failure receipt is presented as success. Preserve process logs separately when a
drill fails, correct the failure, use a fresh target, and rerun the complete drill.

## Manifest

Every accepted backup records:

- backup ID and timestamps;
- source instance/environment;
- repository commit supplied by the deployment;
- dump tool version/options;
- encrypted artifact hash, size and age recipient fingerprint.

Off-host locator/readback evidence and isolated-target verification belong to the
separate recovery evidence and recovery receipt, not to the backup manifest. Canonical
revision, object counts, extension versions, locale/collation and plaintext hashes are
not recorded by the current manifest and must not be inferred from it.

## Cadence and operator gates

Initial policy before broad MCP writes:

- frequent master checkpoint cadence based on measured write volume;
- at least daily verified private-dataset generation and periodic portable logical backup;
- pre-change checkpoint for bulk/high-impact database operation;
- multiple retained generations;
- at least weekly isolated restore drill during rollout;
- backup and restore freshness exposed as a machine gate.

The prior one-day provisional RPO is only an upper-bound bootstrap goal and is not
sufficient as the sole protection once broad remote writes are enabled. Measure dump,
readback and restore duration, then set enforceable RPO/RTO.

## Backup rules

- encrypt before off-host upload;
- keep encryption key outside the artifact/provider;
- never place plaintext dump in GitHub, exchange package or logs;
- verify exact uploaded bytes by provider readback and SHA-256;
- verify provider privacy after create/version;
- retain more than one generation and more than one storage location;
- do not auto-delete old versions until dependency/restore evidence permits;
- test restore, not only dump creation.

## Isolated restore drill

1. provision a fresh isolated PostgreSQL target with compatible version/extensions;
2. download/read back and verify encrypted artifact hash;
3. decrypt in protected local storage;
4. restore without overwriting canonical production;
5. run migration status and `db verify`;
6. verify extension/version/locale metadata;
7. compare representative object counts and critical invariants;
8. verify connector receipts, outbox/provider receipts and audit history;
9. execute one read-only MCP query against the restored target;
10. record duration/outcome and destroy the target after receipt archival.

A restore target is never promoted automatically.

## Operator write gate

Before data-editor apply, verify:

- newest accepted master/private-checkpoint backup age;
- readback/hash status;
- last restore-drill outcome/age;
- schema revision compatibility;
- whether impact tier requires a fresh pre-change checkpoint;
- whether a newer unprotected high-impact operation exists.

Failed/stale gate makes the remote operator read-only. Break-glass bypass requires a
local incident procedure and explicit evidence; it is not a normal MCP parameter.

## Recovery procedure

1. stop canonical writers and external dispatchers;
2. preserve current failed state/artifacts/logs;
3. select exact accepted generation and verify manifest/hash;
4. restore into isolated target first;
5. run integrity and receipt/outbox reconciliation;
6. decide whether to promote restored target or perform targeted repair;
7. replay unapplied semantic commands/connector batches idempotently;
8. rebuild derived indexes;
9. resume services with scheduler/publication gates reviewed explicitly;
10. record incident and new canonical revision.

## Required monitoring

- age of local and off-host accepted generation;
- upload/readback/hash failures;
- generation count/retention risk;
- restore-drill age/outcome/duration;
- encryption/key-access health without logging keys;
- disk space;
- operator gate open/closed and reason.
