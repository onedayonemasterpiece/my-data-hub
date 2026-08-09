# ADR-0006: Notebooks are isolated compute workers

- Status: Accepted
- Date: 2026-08-09

## Decision

Kaggle/local notebooks consume exact input manifests and return immutable,
schema-validated result bundles. They do not receive canonical PostgreSQL write
credentials and do not advance state. Candidate/E5, BGE-M3, ImageDiagnostic
and Writer remain independent workers.

## Consequences

Retries and model-runtime failures cannot directly corrupt canonical state.
The orchestrator owns acceptance, idempotency and domain transitions. Worker
artifacts are replayable and attributable to exact code/model/input revisions.
