# ADR-0006: Ordinary notebooks are isolated compute workers

- Status: Accepted; master exception clarified by ADR-0016
- Date: 2026-08-09

Ordinary model/processing notebooks consume exact manifests and return immutable typed
results; they receive no canonical write credentials. The explicitly designated Kaggle
master Notebook is a separate database-runtime role: it hosts the single ACTIVE writable
PostgreSQL-primary behind lease, fencing and DB-gate controls.

Retries of ordinary workers cannot directly corrupt canonical state. Runtime artifacts are
replayable and attributable to exact source/model/input identities.
