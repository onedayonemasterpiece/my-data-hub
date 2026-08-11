# Deployment profiles after PR-A

## Production: lightweight devstand control plane

`compose.control-plane.yaml` contains one database-free control/status service. It is
loopback-only, read-only, has no database URL, PostgreSQL service, PGDATA, named volume,
master migration, local committer or master backup. Readiness returns 200 while
`master_state=ABSENT`; data operations fail closed.

`deploy/control-plane/install.sh` has separate PREPARE/INSTALL tokens. The INSTALL token
is code only and was not executed by PR-A. It may enable only the control-plane unit after
separate owner approval.

## Forbidden legacy profile

`deploy/same-host/install.sh INSTALL_MY_DATA_HUB_SAME_HOST` exits 78 before prerequisites
or filesystem operations. DB-coupled same-host Compose/systemd/workflows were removed.

## Disposable integration

Root `compose.yaml` is explicitly test-only: PostgreSQL 18 is pinned, uses tmpfs, restart
is disabled, and no named volume exists. Cleanup is always `docker compose down -v`.
Production and control-plane validators reject any reachability from installers to this
profile.

The Yandex Cloud DNS/TLS edge is provisioned as documented in `operations/yandex-edge-deployment.md`, but the MCP/OAuth application backends are not deployed and last returned `502`. Master/checkpoint acceptance and remote MCP writes remain gated; edge existence is not application acceptance.
