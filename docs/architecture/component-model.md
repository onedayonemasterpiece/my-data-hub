# Component model

- **Control plane (devstand):** stable control/MCP surface, lifecycle adapter, operation
  ledger, registry, callbacks, leases/fencing, checkpoint metadata, security/audit.
- **Master Notebook (Kaggle):** single ACTIVE writable PostgreSQL, canonical state,
  restricted roles, queues, FTS/pgvector, DB gate/watchdog and checkpoint agent.
- **Checkpoint Datasets (private Kaggle):** current/previous verified generations and
  portable logical backup.
- **Workers/model services:** typed immutable compute/results; direct epoch-bound data plane.
- **Connectors:** durable spool, ensure/resolve, dedicated landing, idempotent receipt.
- **Joplin bridge:** supported APIs only; never its internal database.
