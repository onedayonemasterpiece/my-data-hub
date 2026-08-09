# Devstand deployment and auto-start

The development host is the initial production host. Deployment should use Docker Compose
or equivalent supervised systemd units; manual shells are not production process managers.

## Host layout

```text
/opt/my-data-hub/repo        checked-out release
/var/lib/my-data-hub/postgres Docker volume or dedicated data directory
/var/lib/my-data-hub/artifacts private bounded cache
/etc/my-data-hub/env         root-readable environment file
/var/backups/my-data-hub     encrypted local staging only
```

## Deployment sequence

1. Create a dedicated OS user and firewall rules.
2. Install Docker/Compose and pin tested image digests.
3. Store secrets outside the repository with restrictive permissions.
4. Start PostgreSQL and run migrations with the owner role.
5. Run `db verify`, backup and restore preflight.
6. Start MCP bound privately; expose only through TLS/auth reverse proxy when needed.
7. Start orchestrator with publication disabled.
8. Run Region Talk shadow cycle and health probes.
9. Configure restart policy and host reboot test.
10. Record image digests, commit and migration revision in a deployment receipt.

## Required services

- PostgreSQL: `restart: unless-stopped`, local/private network only.
- Orchestrator: one active scheduler identity; DB advisory/lease fencing.
- MCP: can have multiple stateless HTTP instances after tool idempotency is proven.
- Dispatchers: independently pausable; publication dispatcher disabled until canary.

## Reverse proxy

The proxy terminates TLS, limits request body/rate and forwards verified identity. MCP still
validates scopes, origin/host and per-tool limits; the proxy is not the sole authorization
check.
