# 2026-08-13 — Yandex edge architecture drift

- Status: open
- Severity: architecture/cost
- Owner decision: public orchestration and ingress run on DevCoveer

## Impact

`mcp-datahub.kenigevents.ru` and `identity.kenigevents.ru` currently resolve to a paid
Yandex ALB/VM/NAT/tunnel edge even though the MCP, OAuth and control services run locally.
The service remains available, but the deployment contradicts the intended personal-tool
architecture and incurs unnecessary cloud cost.

## Root cause

Commit `0bd128c1149663a4d04bad4cd0539f0d91fb9687` made an isolated Yandex TLS gateway a
canonical deployment path. This conflicted with the higher-level invariant that the stable
external MCP is on the devstand and with the owner's intended cost/availability trade-off.

## Protected resources

The shared `kenigevents.ru` DNS zone, buckets `kenigevents`, `kenigevents.ru`, `kgd80.ru`,
both static CDN resources and their certificates, the entire `kenigevents-email-prod`
folder, Postbox/mail identities and service accounts, mail DNS, Identity Hub and YDB are
not part of this incident and must remain unchanged.

## Closure gates

1. Local trusted SAN TLS and renewal are proven without changing OAuth identity.
2. MCP/OAuth and real VPN clients pass pre-DNS tests against DevCoveer.
3. Both A records change together; existing ChatGPT/OpenCode authorization and MCP calls pass.
4. Process restart and host reboot recover ingress and applications.
5. The Yandex edge is stopped and service independence is proven through a rollback soak.
6. Only the exact task-owned edge graph is deleted; protected inventories match baseline.
7. The final state contains no chargeable `my-data-hub public-edge` VM/ALB/NAT/disk/IP.

Until every gate passes, this incident remains open and destructive cloud cleanup is forbidden.
