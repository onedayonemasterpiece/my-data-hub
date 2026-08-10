# Devstand deployment: lightweight control plane

Status: `CONTRACT READY / NOT INSTALLED BY PR-A`

DevCoveer is the permanent lightweight control-plane host. It must not run production
PostgreSQL, PGDATA, master migrations, canonical committer or master backup.

`compose.control-plane.yaml` defines one loopback-only DB-free process. Its readiness is
healthy at `master_state=ABSENT`; its data methods fail closed. The lifecycle adapter,
durable ledger and stable MCP gateway are later implementation phases and are not falsely
claimed by this placeholder contract.

The legacy same-host token is permanently disabled. The replacement installer has a new
explicit token and may enable only `my-data-hub-control-plane.service`; PR-A does not run
it. No DNS/VPN/443 change is part of this phase.

Disposable database work uses root `compose.yaml` only, tmpfs only, and ends with
`docker compose down -v`.
