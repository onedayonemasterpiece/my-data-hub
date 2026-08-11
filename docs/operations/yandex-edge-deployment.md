# Yandex Cloud public edge

Status: `PUBLIC TLS EDGE PROVISIONED / APPLICATION BACKENDS PARTIAL`

The existing DevCoveer host remains the lightweight control plane and keeps every
my-data-hub listener on loopback. Its unrelated VPN/Xray ownership of public ports 80/443
is not changed. The public edge is a dedicated Yandex Cloud network with:

- a public Application Load Balancer listener on TCP 443 only;
- one Certificate Manager certificate for `mcp-datahub.kenigevents.ru` and
  `identity.kenigevents.ru`;
- a private, no-public-IP Ubuntu edge VM running only nginx and a restricted autossh
  client;
- an outbound NAT route for that VM;
- three fixed edge-local forwards to DevCoveer loopback ports 8080, 8765 and 8780;
- a task-specific Lockbox private key readable only by the edge VM service account;
- no PostgreSQL, PGDATA, business rows, checkpoint bytes or master credentials.

The ALB preserves the exact public authority. Edge nginx rejects an unknown Host, routes
`/internal/*` on the MCP authority only to the control callback plane, routes other MCP
requests to the resource server, and routes the identity authority to the OAuth server.
Forwarded headers are cleared because the tunnel peer is not configured as a trusted
client-address proxy. The edge itself has bounded request/concurrency controls.

## Observed certificate request

On 2026-08-11 UTC, the owner-authorized Cloud DNS/Certificate Manager operation created:

- Certificate ID: `fpq4k7654ilgdo1fuvma`
- Name: `my-data-hub-public-edge`
- Exact domains: the two public authorities above
- Initial state: `VALIDATING`; observed `ISSUED` at 2026-08-11T07:20:45.928Z
- Cloud DNS zone: `dnsbhbtvj0l1lf8jpefb`

The two `_acme-challenge` CNAME records point to
`fpq4k7654ilgdo1fuvma.cm.yandexcloud.net.`. This is certificate-validation evidence only;
it is not, by itself, endpoint or deployment acceptance.

## Observed edge provisioning

On 2026-08-11 UTC the owner-authorized provisioning command created a dedicated
task-labelled VPC/NAT/private VM target and public ALB. The stable observed public address
is `158.160.187.150`; both public names resolve to it. The VM has only private address
`10.210.0.10`, the ALB target is healthy, and hostname-verified TLS succeeds for both
authorities with the issued certificate.

The restricted tunnel was verified without moving application data: a public request to
`https://mcp-datahub.kenigevents.ru/internal/runtime/events` traversed ALB, nginx and the
fixed loopback tunnel and returned the control plane's bounded `master_absent` JSON. This
proves the control callback route, not an operational master. The current MCP and OAuth
routes return `502` because the merge-commit `remote-mcp` and `oauth-server` processes and
their production secrets have not been installed on DevCoveer. Therefore public endpoint,
OAuth, ChatGPT, process-recovery and reboot acceptance remain blocked and must not be
reported as successful.

The private edge VM was then restarted through Compute Cloud. The ALB-to-nginx-to-tunnel
path recovered automatically and returned the same bounded control-plane response on the
fifth 5-second probe. This proves edge/tunnel reboot recovery only; it is not the required
DevCoveer three-process/systemd reboot receipt.

The initial clean-image diagnostic also proved an ordering constraint: cloud-init
`write_files` runs before a user declared in the same cloud-config is guaranteed to exist.
The public DevCoveer host pin is consequently written `root:root`/`0644`; using the future
`mdh-edge` group made the network stage fail and left the tunnel without its host pin.

Rollback of only this pre-provision step is:

```bash
yc dns zone delete-records --id dnsbhbtvj0l1lf8jpefb \
  --record '_acme-challenge.identity.kenigevents.ru. 300 CNAME fpq4k7654ilgdo1fuvma.cm.yandexcloud.net.' \
  --record '_acme-challenge.mcp-datahub.kenigevents.ru. 300 CNAME fpq4k7654ilgdo1fuvma.cm.yandexcloud.net.'
yc certificate-manager certificate delete --id fpq4k7654ilgdo1fuvma
```

Do not use that rollback after a live listener depends on the certificate.

## Secret and provisioning gates

`create_tunnel_identity.sh` is run on DevCoveer only after reviewing the pinned SSH host
key. It generates the key in `/dev/shm`, sends the private value to Lockbox through stdin,
adds only the public key to `~/.ssh/authorized_keys`, and deletes the tmpfs private file.
The authorized key permits only `direct-tcpip` destinations 127.0.0.1:8080, :8765 and
:8780; it denies agent/X11/PTY/user-rc use. Unknown keys/resources are never deleted.

`provision.sh` requires an exact action token plus the issued certificate ID, Lockbox
secret ID and a one-line pinned DevCoveer host key. It creates or reconciles only
`project=my-data-hub,scope=public-edge` resources. The VM cloud-init contains the Lockbox
ID, never the private key; the VM fetches the value with its metadata IAM token.
The EC2-compatible metadata endpoint remains enabled only because Ubuntu cloud-init uses
that datasource for non-secret user-data; EC2 IAM-token delivery is disabled. The separate
GCE-compatible token endpoint is enabled for the VM's scoped service-account token.

```bash
export MY_DATA_HUB_YC_EDGE_CERTIFICATE_ID=fpq4k7654ilgdo1fuvma
export MY_DATA_HUB_YC_EDGE_TUNNEL_SECRET_ID='<task Lockbox secret id>'
export MY_DATA_HUB_DEVSTAND_KNOWN_HOST_FILE="$HOME/.local/state/my-data-hub-yandex-edge/devstand-known-host"
deploy/yandex-edge/provision.sh PROVISION_MY_DATA_HUB_YANDEX_EDGE
```

Actual DNS A records are added only after a reserved IPv4, ALB, private target and TLS
listener exist. A green certificate or health check does not waive the implementation
merge, three-process devstand install, OAuth owner configuration, Host/Origin negatives,
process-kill/reboot receipt or MCP cold-start acceptance.
