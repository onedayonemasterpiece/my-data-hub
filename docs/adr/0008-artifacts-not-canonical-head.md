# ADR-0008: Backups and artifacts are not canonical head

- Status: Accepted
- Date: 2026-08-09

## Decision

Private Kaggle Datasets, GitHub artifacts and object storage may contain
encrypted backups, checkpoints and run bundles, but “latest artifact version”
does not define canonical state. PostgreSQL state plus verified receipts and
artifact manifests determine acceptance.

## Consequences

Every backup path needs exact hash verification and restore drills. Orphan
uploads are non-authoritative and can be collected under a retention policy.
