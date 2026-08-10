# Target architecture

## Topology

```text
external agent -> stable devstand MCP/control gateway -> ensure/resolve ACTIVE master
worker/connector -> ensure/resolve -> short-lived epoch credential -> direct data plane

private Kaggle Dataset checkpoints -> restore -> Kaggle master Notebook
Kaggle master Notebook -> verified checkpoint/readback/restore -> Dataset HEAD advance
```

### Devstand lightweight control plane

Runs stable control/status/callback surfaces, Kaggle lifecycle adapter, durable
operation/idempotency ledger, service/capability registry, leases, fencing epochs,
checkpoint metadata, security and audit. It contains no production PostgreSQL, PGDATA,
canonical business catalog, vector index, Region Talk rows or local master backup.
`master=ABSENT` is healthy; data operations are unavailable or return an operation ID.

### Kaggle master Notebook

At most one ACTIVE writable PostgreSQL-primary. It contains PostgreSQL/pgvector/FTS,
canonical catalog and pipeline state, restricted roles, transactional queues, MCP data
access, DB gate, lease watchdog, checkpoint agent and service heartbeat. It restores and
migrates before opening the write gate. An expired or older epoch is fenced.

### Private Kaggle Datasets

Store at least current and previous verified checkpoint generations plus a portable
logical backup. Promotion is: consistent backup -> manifest -> private exact version ->
readback/hash -> isolated restore smoke -> atomic HEAD advance. Failed candidates never
replace verified generations.

## Lifecycle

```text
ABSENT -> REQUESTED -> STARTING -> RESTORING -> REGISTERING -> ACTIVE
ACTIVE -> DRAINING -> CHECKPOINTING -> STOPPED
FAILED | FENCED | CHECKPOINT_FAILED | ORPHANED
```

Every provider side effect follows a durable transition. `ensure_master` is idempotent;
parallel callers create at most one physical run. Registry resolution returns only the
latest ACTIVE epoch. Credentials are short-lived and epoch-bound.

## Data integrity

Canonical transactions, semantic outbox, connector application and revision advancement
occur in the master PostgreSQL. Ordinary workers return typed immutable results. Connector
roles can write only connector-specific landing/stored contracts. External publication
requires an exact approved revision and idempotency identity.

## PR-A boundary

PR-A implements the DB-free ABSENT health contract and removes unsafe deployment paths.
It does not implement lifecycle, real Kaggle calls, master Notebook, dynamic MCP data
access or Region Talk migration. See [the ordered roadmap](roadmap-architecture-reset.md).
