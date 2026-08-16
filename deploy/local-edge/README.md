# DevCoveer local public edge

The canonical public MCP and OAuth applications run on DevCoveer and remain bound to
loopback (`127.0.0.1:8765`, `:8780`, and the bounded callback plane on `:8080`). The
existing VPN nginx is the only public TCP/443 owner and must route by TLS SNI:

- `mcp-datahub.kenigevents.ru` and `identity.kenigevents.ru` -> a loopback TLS HTTP
  listener with exact Host admission;
- the exact Reality SNI and every unknown/no-SNI connection -> loopback Xray Reality;
- an explicitly enabled Trojan SNI -> its loopback listener.

The MCP host routes `/internal/*` only to `127.0.0.1:8080` and other requests to
`127.0.0.1:8765`. The identity host routes only to `127.0.0.1:8780`. The proxy must clear
`Forwarded` and every `X-Forwarded-*` header, retain the existing one-megabyte request cap,
bounded timeouts/rates, and never log OAuth query strings. Ports 8080/8765/8780 and the
Xray backend remain loopback-only.

## Activation gate

This repository intentionally does not overwrite the independently managed VPN renderer.
Before DNS changes, its source generator and tests must implement the contract above;
hand-editing a generated nginx file is forbidden. A trusted locally readable SAN
certificate for both public names, automated renewal, `nginx -t`, Xray config validation,
`curl --resolve` MCP/OAuth checks, and a real VPN-client regression are mandatory.

The stable public authorities remain unchanged. Do not regenerate OAuth client IDs,
signing keys, the control ledger, connector names, issuer or resource URL.

## Active host implementation

The host renderer now implements the contract: exact MCP/OAuth SNI goes to local TLS
`127.0.0.1:8444`, default/no-SNI remains on Reality `127.0.0.1:10443`, and application
upstreams stay loopback-only. A local Certbot DNS-01 certificate is renewed by an owner
user-systemd timer. OAuth access logs omit query strings. MTProto uses a Docker bridge and
publishes only `1443/tcp`, so Erlang EPMD/distribution ports are not exposed by host networking.
