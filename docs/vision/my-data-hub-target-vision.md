# my-data-hub target vision

This is a derived overview. It cannot override the exact imported source research or
ADR-0016.

`my-data-hub` unifies canonical content, pipeline state, semantic search and controlled
agent access in PostgreSQL. In an active session, the only writable primary runs in a
Kaggle master Notebook. Private Kaggle Datasets retain current and previous verified
checkpoints. The devstand remains a lightweight stable control/MCP plane with lifecycle,
registry, leases/fencing, callbacks, checkpoint metadata, security and audit—never
canonical business data or PGDATA.

Workers and connectors resolve the latest ACTIVE epoch and use short-lived direct data-plane
access. External agents use stable devstand MCP. PostgreSQL performs FTS/vector retrieval;
E5/BGE remain separate model services.

The rollout is FakeKaggle-first, then real provider smoke, then master Notebook PoC, then
dynamic MCP/connectors, models, durability and bounded canary. Region Talk migration is
last among these gates and remains paused.
