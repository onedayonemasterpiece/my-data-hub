# 2026-08-13 — Yandex edge architecture drift

- Status: mitigated; exact cloud teardown complete, host-reboot evidence pending
- Severity: architecture/cost
- Owner decision: public orchestration and ingress run on DevCoveer

## Impact

`mcp-datahub.kenigevents.ru` and `identity.kenigevents.ru` resolved to a paid Yandex
ALB/VM/NAT/tunnel edge even though the MCP, OAuth and control services ran locally. The
service remained available, but the deployment contradicted the intended personal-tool
architecture and incurred unnecessary cloud cost.

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

The owner-authorized cost cleanup proceeded after DNS propagation, local TLS/VPN checks,
process recovery and provider stop tests proved independence. The remaining reboot gate keeps
the incident open but does not justify retaining the already-independent paid edge.

## 2026-08-13 mitigation evidence

- DevCoveer now owns public TCP `443`; exact MCP/OAuth SNI is terminated locally and
  unknown/no-SNI traffic remains on the existing Reality path.
- Both public A records resolve to `188.227.84.107`. The protected-resource, OAuth and
  OIDC metadata hashes are unchanged and public MCP no longer has the `ycalb` header.
- A fresh locally issued SAN certificate is active and its Certbot DNS-01 renewal dry-run
  succeeded. OAuth signing material, client registrations, issuer, resource and JWKS are
  unchanged.
- Public Erlang EPMD/distribution exposure was removed by moving MTProto off host networking;
  only its intended `1443/tcp` is published.
- A real local Xray client passed the VLESS Reality path after updating the incompatible
  `www.microsoft.com` camouflage target to `www.bing.com` for Xray 26.3.27. Existing client
  subscriptions must be refreshed for that VPN-only setting change.
- The task-owned Yandex ALB and VM were stopped first. MCP/OAuth remained available from the
  local address after each stop. The exact labelled edge graph was then deleted: ALB/router/
  backend/target, VM and auto-delete disk, reserved edge IP, task VPC/subnet/route/NAT/security
  groups, tunnel Lockbox/service account, temporary overlap certificate and restricted tunnel
  SSH key. No task edge ID remains in the default folder.
- The shared DNS zone and the two public A records remain, now pointing to DevCoveer. All three
  protected static buckets, both static CDN resources, the four unrelated site certificates,
  mail records and the entire `kenigevents-email-prod` folder remain present. Static website,
  bucket-object, MX/DMARC/DKIM and MCP/OAuth probes remained healthy after deletion.
- During certificate staging, the old edge key was emitted by the provider CLI despite file
  output arguments. The old certificate was treated as compromised: a fresh independent local
  certificate was issued, activated on both ALB and DevCoveer during overlap, the old certificate
  and local copy were deleted, and the temporary overlap certificate was deleted with the ALB.

The cost-removal and protected-inventory gates are complete. Final incident closure still
requires one owner-authorized host reboot and post-boot ingress/VPN/OAuth verification; the
current session cannot perform that privileged host action. No bucket, CDN, shared DNS zone,
Postbox/mail resource, Identity Hub or unrelated YDB resource is eligible for cleanup.
