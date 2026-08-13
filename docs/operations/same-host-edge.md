# Stable MCP edge on DevCoveer

Status: `OWNER-APPROVED / LOCAL TLS CUTOVER PENDING`

ADR-0019 makes the existing DevCoveer VPN nginx the canonical TCP/443 edge. MCP, OAuth and the callback control plane remain loopback-only. The exact rendering, TLS, VPN, DNS, reboot, rollback and protected-resource gates are defined in `../../deploy/local-edge/README.md` and the open incident `../incidents/2026-08-13-yandex-edge-architecture-drift.md`.

Do not change DNS or delete cloud resources until every pre-cutover gate passes. Do not hand-edit generated VPN configuration.
