# Provider MCP live deployment evidence

## Observed deployment

- Exact deployed release: `77b737fb58955a418d51d45093bef8c30280dc50`.
- The user unit is enabled and active.
- Exact-release control plane, OAuth server, and remote MCP containers are healthy.
- Loopback listeners `127.0.0.1:{8080,8765,8780}` answer their readiness/metadata probes.
- Control readiness is honest provider-only: provider gateway ready, master absent, data plane not ready.
- OAuth authorization-server metadata advertises CIMD, PKCE, public-client authentication, and the exact provider scopes.

The accompanying JSON stores only bounded non-secret status, response hashes, and the SHA-256 of the private installer receipt. It does not copy boot IDs, credentials, tokens, account identity, or metadata bodies.

## Public edge

The existing edge VM and ALB were started. The original edge security-group reference rule did not admit the ALB health traffic; an exact failing test and the live target proved the root cause. The reviewed fix admits only TCP/8080 from the dedicated private `10.210.0.0/24` ALB/backend subnet. After applying it, the target was `HEALTHY` and both canonical HTTPS metadata routes returned 200. The unauthenticated MCP resource itself correctly returned 401.

Temporary start/diagnostic IAM grants, public NAT, SSH key material, and diagnostic ingress were removed. The final edge VM remains private and the rule set contains only the expected seven bounded rules.

## Usable surface and scope honesty

The public MCP/OAuth backend is now ready for ChatGPT application connection at `https://mcp-datahub.kenigevents.ru/mcp`. A real private Kaggle create/list/chunk-download/version/delete lifecycle was independently recorded in the MCP batch canary lane.

This evidence does not claim ACTIVE master, canonical data-plane readiness, or the full 24-scenario matrix. Those remain separate work after the provider-only surface.
