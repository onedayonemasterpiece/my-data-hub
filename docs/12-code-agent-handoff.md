# Code-agent handoff after PR-A

Read in order: owner decisions, exact imported source, ADR-0016,
`architecture/invariants.yaml`, then derived docs/code.

## Do not do

- do not run the legacy same-host INSTALL;
- do not create local PostgreSQL/PGDATA or apply master migrations on devstand;
- do not start DNS/VPN/443, remote MCP writes, a real master Notebook or Region Talk;
- do not edit the exact imported source or applied migrations.

## Next task

Start reusable donor runtime compatibility, then the FakeKaggle orchestrator core with
deterministic clock, lifecycle transitions, idempotent operations, leases, fencing,
registry and property tests. Real Kaggle and master PostgreSQL come only after that.

Preserve migrations/roles/connector/MCP/recovery/Region Talk contracts, but bind them to
the future ACTIVE Kaggle master. The devstand ledger is operational metadata only.
