# Backup and recovery

## Purpose

Backups protect recovery from operator error, software defect and host/storage loss. They
are not an authorization mechanism and do not justify exposing a database owner or
unbounded remote writes.

## Backup layers

1. PostgreSQL logical backup (`pg_dump` custom format) for portable recovery.
2. Optional physical/base backup or WAL continuity when measured RPO requires it.
3. Encrypted local generation with manifest/hash.
4. Encrypted private off-host copy, initially a protected private Kaggle Dataset or
   another approved target.
5. Schema/migration code in GitHub.
6. Separate artifact/connector/provider/operator receipts and hashes.

Kaggle backup datasets use `orchestrator_protected`. Remote MCP exposes freshness/status
only, never dump files, version/delete/download.

## Manifest

Every accepted backup records:

- backup ID and timestamps;
- source instance/environment;
- repository commit and canonical/schema revision;
- PostgreSQL major, extension versions and locale/collation;
- dump tool version/options;
- plaintext-before-encryption hash where safely retained;
- encrypted artifact hash, size and encryption metadata without key;
- local/off-host locator identity;
- upload/readback verification;
- retention class and parent generation;
- restore compatibility notes.

## Cadence and operator gates

Initial policy before broad MCP writes:

- frequent local backup/snapshot cadence based on measured write volume;
- at least daily encrypted off-host generation;
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

- newest accepted local/off-host backup age;
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
