# ADR-0019: Public MCP edge runs on DevCoveer

- Status: Accepted
- Date: 2026-08-13
- Supersedes: ADR-0013 only where it selected a Yandex Cloud edge

## Context

The remote MCP/OAuth applications already run on DevCoveer and bind only loopback. A
Yandex ALB, private VM, NAT and reverse SSH tunnel were nevertheless provisioned as the
mandatory public ingress. The owner clarified that this is a personal information-work
tool and the orchestrator/public edge must run on the same machine. The extra cloud path
adds cost and complexity without the required availability benefit.

## Decision

DevCoveer is the only runtime host for the lightweight control plane, MCP, OAuth and their
TLS ingress. Cloud DNS may continue to host the shared zone, but the two public A records
point to DevCoveer's stable IPv4. The public authorities remain exactly:

- `https://mcp-datahub.kenigevents.ru/mcp`;
- `https://identity.kenigevents.ru`.

The existing local VPN edge owns TCP/443 and separates the two HTTPS authorities from
Xray/Trojan by SNI. Unknown/no-SNI remains on the VPN camouflage path. Application and
VPN backend ports stay loopback-only.

The OAuth signing key, ledger, issuer/resource, client IDs, redirect URIs and connector
names are preserved. A raw-IP MCP URL is forbidden.

Yandex VM, ALB, NAT, reserved edge address, private edge network, tunnel secret/service
account and the edge-only certificate are retired after local TLS, DNS, OAuth/MCP, VPN,
reboot and renewal acceptance. The shared DNS zone, all Object Storage buckets, CDN/static
site resources, mail/Postbox infrastructure, Identity Hub, YDB and unrelated resources
are outside this retirement.

## Consequences

The machine and its internet connection are a deliberate single point of availability.
There is no cloud compute fallback after the rollback window. External monitoring,
automatic certificate renewal and encrypted off-host backups of the OAuth signing material
and control ledger are required operational controls.
