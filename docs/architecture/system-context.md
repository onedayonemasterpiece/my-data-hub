# System context

```mermaid
flowchart LR
  Agent[External agent] --> MCP[Stable devstand MCP/control]
  Worker[Kaggle worker or connector] --> Control[ensure/resolve service]
  MCP --> Control
  Control --> Master[Kaggle master Notebook: single writable PostgreSQL]
  Worker -->|short-lived epoch-bound direct data plane| Master
  Master --> Checkpoints[Private current/previous verified checkpoint Datasets]
```

The devstand stores operational metadata only. It has no production PostgreSQL, PGDATA or
canonical business rows. When the master is ABSENT, control health remains ready and data
operations wait/fail closed.
