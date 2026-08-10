# Agent operating contract

## Authority order

1. Explicit owner decisions.
2. Exact imported source research, currently
   `docs/source-material/idea-hub/idea-20260809-content-platform-current-design.md`.
3. Corrective ADR-0016.
4. `architecture/invariants.yaml` and append-only migrations/JSON schemas.
5. Derived documentation, code, tests and historical implementations.

The derived overview [`docs/00-source-of-truth.md`](docs/00-source-of-truth.md) explains
this order but does not outrank the exact imported source.

`my-data-hub` is the final name; `content-platform` is its historical alias. Never edit
the exact imported source to make derived decisions appear consistent.

## Hard invariants

- PostgreSQL is the only canonical server-side database engine.
- At most one writable PostgreSQL-primary is ACTIVE, and it runs only in the Kaggle
  master Notebook.
- Private Kaggle Datasets store current and previous verified checkpoints; they are not
  a live database.
- The devstand is a lightweight control plane and contains no production PostgreSQL,
  PGDATA or canonical business data.
- Internal workers/connectors resolve an ACTIVE epoch and then use the direct master
  data plane with short-lived role-bound credentials.
- The default MCP exposes no generic SQL. A separate bounded operator profile never
  receives owner/superuser/DDL/BYPASSRLS/server-file rights.
- Ordinary workers emit typed results; only the designated master Notebook is a database
  runtime. Every canonical write and required semantic outbox operation share one
  PostgreSQL transaction.
- Data connectors use versioned idempotent contracts and dedicated master landing, never
  shared canonical tables directly.
- Only one canonical committer may advance a revision; expired epochs are fenced.
- Kaggle resources marked orchestrator-protected remain status-only through remote MCP.
- Region Talk remains paused until the ordered master lifecycle gates pass.
- Never fabricate migrations, row counts, hashes, IDs, model versions or readiness.
- Never expose credentials, production data, decrypted exports or personal sessions.

## Definition of done

- `python -m compileall src tests` and `pytest` pass.
- Repository/schema/notebook validation passes.
- Migrations remain append-only and contiguous.
- Architecture tests reject production local PostgreSQL/PGDATA paths.
- New pipeline stages define retry, timeout, terminal result and receipt contracts.
- Documentation reports observed evidence and blockers rather than scaffold as proof.
