# Bounded full ChatGPT deployment

`INSTALL_MY_DATA_HUB_BOUNDED_FULL` deploys the owner-facing ChatGPT surface that combines
the lightweight provider control plane, Google YouTube analysis and the isolated Showcase
runtime. It exists for this exact capability set and does not enable the canonical database
operator profile or Kaggle master runtime.

The MCP resource scopes are exactly:

```text
platform:read,provider:read,provider:write,youtube:analyze,showcase:read,showcase:write
```

The profile keeps `ProviderOnlyWriteGate` for provider mutations, so only private
`mcp_managed` and `mcp_exchange` resources are writable while the canonical master is
`ABSENT`. YouTube is read-only but quota-consuming and uses the dedicated shared limiter.
Showcase runs in a separate unprivileged container with repository-scoped read/write deploy
keys and its own gateway token. The profile never grants `data:write`, `master:ensure`,
`region-talk:operate`, `acceptance:operate`, DDL or generic SQL.

Required private host inputs are the existing provider/OAuth environment, a Google-only
environment containing the limiter and candidate API-key variables, and the Showcase
gateway/deploy-key/runtime files described by `compose.showcase.yaml`. The installer verifies
that Kaggle credentials cannot enter the remote MCP process and that the final protected
resource metadata exactly matches the six scopes above.

Install from a clean reviewed commit:

```bash
MY_DATA_HUB_APPROVED_CONTROL_COMMIT="$(git rev-parse HEAD)" \
  deploy/control-plane/install.sh INSTALL_MY_DATA_HUB_BOUNDED_FULL
```

Successful readiness requires the provider control gateway, OAuth and remote MCP endpoints,
the Showcase runtime and static edge, and exact live OAuth scope readback. The systemd unit
keeps all five containers in the same rollback/restart lifecycle.
