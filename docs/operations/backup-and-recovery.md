# Backup and recovery

## Backup layers

1. PostgreSQL logical backup (`pg_dump` custom format) for portable recovery.
2. Optional physical/base backup once continuous operation justifies it.
3. Encrypted private off-host copy, initially a private Kaggle Dataset or another approved
   storage target.
4. Schema/migration code in GitHub.
5. Separate artifact manifests and hashes.

## Rules

- encrypt before upload;
- never place a plaintext dump in a GitHub artifact;
- include PostgreSQL major, extension versions, locale/collation, schema revision and commit;
- verify exact uploaded bytes by readback and SHA-256;
- retain more than one generation;
- test restore into an isolated instance on schedule.

## Recovery objectives

Initial provisional objectives, to be measured rather than promised:

- RPO: at most one day before Region Talk publication is enabled; tighter after volume data;
- RTO: one operator session for database restore and service verification.

Outbox/provider receipts are included in backups so recovery cannot repeat already completed
external effects.
