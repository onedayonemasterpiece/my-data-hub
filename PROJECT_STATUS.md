# Project status

Status: `PR-A ARCHITECTURE RESET / LOCAL MASTER PATH REMOVED`

## Owner decision

One writable PostgreSQL-primary may be ACTIVE only in the Kaggle master Notebook. Private
Kaggle Datasets retain current/previous verified checkpoints. DevCoveer is the lightweight
control plane and must not host production PostgreSQL, PGDATA or canonical business data.

## Corrected in PR-A

- exact-source authority restored and ADR-0016 accepted;
- ADR-0009/local-runtime clauses superseded;
- production same-host Compose/systemd/workflows removed;
- legacy install token hard-disabled;
- database-free control-plane readiness supports `master=ABSENT`;
- disposable CI/local PostgreSQL uses tmpfs and no named volume;
- semantic architecture validator/tests added;
- Region Talk, publication and remote MCP writes remain disabled.

## Host evidence

The rejected INSTALL was not run. No my-data-hub containers or relevant listeners were
observed; the legacy user unit was disabled/inactive. An empty, unattached named volume
created during prior validation does exist and is disclosed in the host receipt. It
contains no initialized PGDATA and was not deleted without destructive-operation approval.
Prepared releases/secrets are also residue, not deployed runtime.

## Not implemented by PR-A

- reusable donor/runtime compatibility;
- FakeKaggle state machine and durable lifecycle ledger;
- real Kaggle adapter or master Notebook;
- checkpoint dataset promotion/readback/restore;
- dynamic MCP/connectors against ACTIVE master;
- DNS/VPN/443 or remote MCP writes;
- Region Talk inventory/migration.

Next task after merge is the reusable runtime/FakeKaggle sequence in
[`docs/roadmap-architecture-reset.md`](docs/roadmap-architecture-reset.md), not local DB
deployment.
