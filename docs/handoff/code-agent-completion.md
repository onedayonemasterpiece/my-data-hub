# Historical implementation handoff

The local-devstand PostgreSQL availability conclusion in the previous handoff is superseded
by ADR-0016 after an owner-confirmed architecture drift. Its schema, migrations, roles,
connector, MCP and recovery artifacts remain useful but must be rebound to the Kaggle master.
See the incident record, invariants and preservation map before reusing any deployment claim.
