
# IdeaHub Showcase: production runtime

## Ownership boundary

`onedayonemasterpiece/idea-hub` is the curated source of cards,
audience views, source references and visual decisions. It does not
execute Astro, publish files, store active secret links or expose MCP
tools.

`onedayonemasterpiece/my-data-hub` owns the complete display runtime:

```text
MCP client
  -> OAuth remote-mcp edge (Python only; no GitHub/publisher credentials)
  -> authenticated loopback showcase gateway
  -> isolated showcase-runtime (GitHub source + Astro + publisher + private state)
```

The standard `remote-mcp` image stays Python-only. Node, the read-only
source credential and publication credentials exist only in
`showcase-runtime`.

## One-time host inputs

Create `/srv/my-data-hub/showcase/gateway.key` as a distinct random
token and `/srv/my-data-hub/showcase/runtime.env` from
`deploy/showcase-runtime/runtime.env.example`. Both files must be
regular, owner-only `0600` files readable by runtime UID/GID `65532`.
Never commit their contents or pass the runtime env to `remote-mcp`.

Set the host deployment env:

```dotenv
MY_DATA_HUB_SHOWCASE_GATEWAY_TOKEN_FILE=/srv/my-data-hub/showcase/gateway.key
MY_DATA_HUB_SHOWCASE_RUNTIME_ENV_FILE=/srv/my-data-hub/showcase/runtime.env
MY_DATA_HUB_SHOWCASE_RUNTIME_PORT=8790
MY_DATA_HUB_SHOWCASE_IMAGE=my-data-hub-showcase:local
MY_DATA_HUB_SHOWCASE_MEMORY_LIMIT=512m
MY_DATA_HUB_SHOWCASE_CPU_LIMIT=1.00
MY_DATA_HUB_SHOWCASE_STATE_VOLUME=my-data-hub-showcase-state
MY_DATA_HUB_MCP_SCOPES_WITH_SHOWCASE=<existing-owner-scopes>,showcase:read,showcase:write
```

The OAuth owner/operator client receives `showcase:read` and
`showcase:write`. A reader explicitly granted `showcase:read` may call
only `showcase.list`, which returns masked URLs.
`showcase.get_link` returns the full secret URL and therefore requires
owner/operator scope `showcase:write`, despite being annotated as a
read-only operation.

## Build and start

```bash
docker compose               -f compose.control-plane.yaml               -f compose.showcase.yaml               --env-file /srv/my-data-hub/control-plane.env               build showcase-runtime
docker compose               -f compose.control-plane.yaml               -f compose.showcase.yaml               --env-file /srv/my-data-hub/control-plane.env               up -d showcase-runtime remote-mcp
curl --fail --silent http://127.0.0.1:8790/health/ready
```

Do not expose port `8790` in Nginx, a tunnel, firewall rule or Docker
published-port rule.

## Live acceptance

Use unique idempotency keys containing the exact `idea-hub` source
commit:

```text
showcase.create_view(view_id="main", idempotency_key="create:main:<source-sha>")
showcase.get_link(view_id="main")
showcase.rebuild(view_id="main", idempotency_key="rebuild:main:<source-sha>")
showcase.get_link(view_id="main")              # URL unchanged
showcase.rotate_link(view_id="main", idempotency_key="rotate:main:<source-sha>:1")
showcase.get_link(view_id="main")              # new URL
verify old URL is unavailable
repeat rotate with the same key                 # duplicate, no third URL
```

Leave the new rotated main URL active. Verify catalog and detail
pages plus `noindex`, `X-Robots-Tag` and `Referrer-Policy`. Save only
a masked URL and hashes in the deployment receipt; retrieve the full
URL through `showcase.get_link`.

## Rollback

Stop only `showcase-runtime`, restart `remote-mcp` without the overlay
and remove showcase scopes. Preserve the named state volume until
every active URL has been explicitly revoked or migrated. This runtime
requires neither PostgreSQL nor Kaggle.
