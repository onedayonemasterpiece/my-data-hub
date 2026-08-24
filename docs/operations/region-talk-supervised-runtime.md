# Region Talk supervised runtime gate

Region Talk remains paused by default and publication dispatch remains hard-set
to `false`. This document describes code-level admission only; it is not live
Kaggle, YDB, PostgreSQL, row-count, or checkpoint evidence.

## YDB viewer delivery

Set these non-secret control-plane values before enabling the assembly:

- `MY_DATA_HUB_REGION_TALK_YDB_ENDPOINT` — credential-free `grpc://` or
  `grpcs://` endpoint;
- `MY_DATA_HUB_REGION_TALK_YDB_DATABASE` — absolute YDB database path;
- `MY_DATA_HUB_REGION_TALK_YDB_VIEWER_SECRET_LABEL` — label of the reviewed
  Kaggle User Secret containing the read-only service-account JSON.
- `MY_DATA_HUB_MASTER_YDB_DEPENDENCY_MANIFEST_SHA256` — SHA-256 of the exact
  canonical 14-wheel offline YDB dependency manifest from the reviewed master
  asset bundle.

An owner must pre-provision the private Notebook
`<owner>/mdh-region-talk-supervisor` and attach that User Secret in its Kaggle
Settings before enabling the supervised assembly. The launcher versions this
stable, orchestrator-protected Notebook rather than creating a task-specific
Notebook, so the reviewed secret attachment is retained; task cleanup never
deletes it. The generated bootstrap resolves
the value through `kaggle_secrets.UserSecretsClient`, writes it once to a mode
`0600` file in `/kaggle/working`, and points
`YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS` at that file. The secret value is
never placed in the status Dataset, SQLite journal, provider intent, receipt,
Notebook source, or log message. Only its label and the credential-free
endpoint/database are launch pins. This matches the official Kaggle
`UserSecretsClient` attached-secret contract and the YDB SDK's documented
environment credential selection.

The supervisor also requires the project wheel, dependency manifest, and all
14 locked CPython 3.12/manylinux wheels to resolve unambiguously from the same
private asset Dataset. It SHA-verifies every file, installs each wheel with
`--no-index --no-deps`, and verifies installed versions plus `ydb==3.31.2`
before importing the application. Missing or mismatched closure assets fail
closed; neither Kaggle image preinstallation nor Internet access is a fallback.

## Response loss and restart

The central launcher journal commits each original provider intent—including
its original `requested_at`—before the Dataset create, Notebook push, or exact
cleanup delete. A restarted launcher first performs an exact read-only provider
reconciliation and reconstructs the central receipt/claim. For the stable
Notebook, the journal also pins the pre-effect provider version: a retry either
proves the exact next version and source or proves that the prior version is
still current before one push. Any other provider state is ambiguous and stops.
Cleanup retains an exact terminal receipt for response-loss replay.

## Long-run transport rotation

The Notebook checks the task-bound credential expiry at cycle, pass, page,
landing, begin, and finalize boundaries. Rotation creates and proves the next
tunnel/PostgreSQL session, posts the exact activation with retry, then closes
the prior connection and tunnel. `DirectSnapshotRunner` retains the current
pass, table, primary-key cursor, and page number, so transport replacement does
not restart or skip a page. Activation persists the revocation mailbox and
idempotently revokes the certificate before recording the new active
generation.

## Read-only status and terminal replay

`region_talk.pipeline.status` reads only the local metadata SQLite ledger. It
does not require the provider/write gateway and does not resolve or wake the
Kaggle master. The generated terminal callback retries the same serialized
receipt, and the control endpoint accepts an exact already-terminal or cleaned
replay while rejecting a different terminal receipt.
