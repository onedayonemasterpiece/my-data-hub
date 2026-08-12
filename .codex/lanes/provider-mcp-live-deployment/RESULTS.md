# Provider MCP local deployment evidence

## Observed deployment

- Exact deployed release: `77b737fb58955a418d51d45093bef8c30280dc50`.
- The user unit is enabled and active.
- Exact-release control plane, OAuth server, and remote MCP containers are healthy.
- Loopback listeners `127.0.0.1:{8080,8765,8780}` answer their readiness/metadata probes.
- Control readiness is honest provider-only: provider gateway ready, master absent, data plane not ready.
- OAuth authorization-server metadata advertises CIMD and PKCE/public-client behavior.

The accompanying JSON stores only bounded non-secret status and the SHA-256 of the private installer receipt. It does not copy boot IDs, credentials, tokens, or account identity.

## External public-edge blocker

Both the edge VM and ALB were observed `STOPPED`. Exact start calls were attempted with the current user profile and the impersonatable project service accounts, and Yandex Cloud returned `PermissionDenied`. Consequently the public HTTPS routes time out even though all three local services are healthy. Closing this requires an external principal with permission to start the two existing resources; it is not an application-code failure.

## Scope honesty

This evidence proves a local production-style provider-only deployment, not public ChatGPT connectivity, ACTIVE master, canonical data-plane readiness, or the full 24-scenario matrix.
