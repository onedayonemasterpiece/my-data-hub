
# IdeaHub Showcase: production runtime

## Evidence status

This document specifies the deployment boundary; it does not prove a deployment. On
2026-09-04, live ChatGPT `showcase.list` returned `503 OAuth token request failed` and
its discovered surface showed only six older methods. Source code, an image build, or
compose configuration is not live acceptance. Do not claim the service ready until the
receipts in [showcase-runtime-v2-verification.md](showcase-runtime-v2-verification.md)
are complete for the deployed exact main successor.

The runtime image installs the HTTP client used by the gateway, and the read-only
static edge writes all Nginx temporary files only to its private `/tmp` tmpfs.
The immutable renderer template is copied into the runtime's private `/work`
tmpfs and made owner-writable there before Astro links its dependencies. The
builder consumes that copy through `MY_DATA_HUB_SHOWCASE_SITE_ROOT`.
The read-only Git source uses a blobless sparse checkout of the configured
Showcase subtree so unrelated repository assets cannot exhaust the runtime tmpfs.
Astro telemetry is disabled in the non-login runtime account so builds never
attempt to persist user configuration outside the private tmpfs.
Astro and Vite caches live under the writable runtime copy's `.cache` directory,
never under the immutable `node_modules` symlink.
`TMPDIR=/work` keeps the transient Git checkout, renderer cache, and Astro output
on the same private filesystem, preserving Astro's atomic asset renames.
Rotation recovery invokes the publisher's explicit `view_id`/`slug` contract,
including keyword-only production publishers.

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
  -> loopback-only showcase-static (read-only generated files)
  -> existing TLS edge for ideas.kenigevents.ru
```

The standard `remote-mcp` image stays Python-only. Node, the read-only
source credential and publication credentials exist only in
`showcase-runtime`.

## One-time host inputs

Create one distinct random gateway token and store two identical `0600` copies because
the edge and runtime intentionally use different UIDs: an edge copy owned by the service
user/UID 1000 and a runtime copy owned by UID/GID `65532`. Create runtime.env from
`deploy/showcase-runtime/runtime.env.example`. Never commit their contents or pass the
runtime env, GitHub deploy key, or renderer configuration to `remote-mcp`.

Production source access uses a repository-scoped GitHub deploy key registered read-only
on `onedayonemasterpiece/idea-hub`. Mount its private half only into showcase-runtime and
pin GitHub SSH host keys. A distinct write-enabled deploy key may be mounted only as
`MY_DATA_HUB_SHOWCASE_GITHUB_WRITE_SSH_KEY_FILE`; it is repo/ref/root bounded by the
runtime and is never a broad owner credential. Do not reuse the owner's broad CLI token.

Set the host deployment env:

```dotenv
MY_DATA_HUB_SHOWCASE_EDGE_GATEWAY_TOKEN_FILE=/srv/my-data-hub/showcase/gateway-edge.key
MY_DATA_HUB_SHOWCASE_RUNTIME_GATEWAY_TOKEN_FILE=/srv/my-data-hub/showcase/gateway-runtime.key
MY_DATA_HUB_SHOWCASE_GITHUB_SSH_KEY_FILE=/srv/my-data-hub/showcase/idea-hub-deploy-key
MY_DATA_HUB_SHOWCASE_GITHUB_WRITE_SSH_KEY_FILE=/srv/my-data-hub/showcase/idea-hub-write-key
MY_DATA_HUB_SHOWCASE_GITHUB_KNOWN_HOSTS_FILE=/srv/my-data-hub/showcase/github-known-hosts
MY_DATA_HUB_SHOWCASE_PUBLIC_DIR=/srv/my-data-hub/showcase/public
MY_DATA_HUB_SHOWCASE_STATE_DIR=/srv/my-data-hub/showcase/state
MY_DATA_HUB_SHOWCASE_RUNTIME_ENV_FILE=/srv/my-data-hub/showcase/runtime.env
MY_DATA_HUB_SHOWCASE_RUNTIME_PORT=8790
MY_DATA_HUB_SHOWCASE_IMAGE=my-data-hub-showcase:local
MY_DATA_HUB_SHOWCASE_MEMORY_LIMIT=512m
MY_DATA_HUB_SHOWCASE_CPU_LIMIT=1.00
MY_DATA_HUB_MCP_SCOPES_WITH_SHOWCASE=<existing-owner-scopes>,showcase:read,showcase:write
```

The OAuth owner/operator client receives `showcase:read` and
`showcase:write`. A reader explicitly granted `showcase:read` may call
only `showcase.list`, which returns masked URLs.
`showcase.get_link` returns the full secret URL and therefore requires
owner/operator scope `showcase:write`, despite being annotated as a
read-only operation.

The protected-resource document at
`/.well-known/oauth-protected-resource/mcp` must advertise both Showcase
scopes whenever the runtime is enabled. This document and `tools/list` must be
built from the same env-backed dependency graph; otherwise OAuth clients such
as ChatGPT keep requesting their previously known scope set and hide the
Showcase tools even though the backend is live. After adding scopes, verify the
document before reconnecting the client account (a catalog refresh alone does
not enlarge an existing OAuth grant). To avoid a discovery deadlock for an
already connected ChatGPT app, `tools/list` advertises enabled Showcase action
schemas to the existing unified owner/operator grant identified by
`provider:write`. This does not grant execution: each call remains denied until
the exact Showcase scope is authorized, and reader grants remain unable to see
the new actions. After deployment, refresh the app catalog, invoke a Showcase
action to complete incremental authorization if prompted, then refresh once
more and verify `showcase.get_source` and `showcase.apply` alongside the existing actions in the ChatGPT app UI.

## Build and start

```bash
docker compose               -f compose.control-plane.yaml               -f compose.showcase.yaml               --env-file /srv/my-data-hub/control-plane.env               build showcase-runtime
docker compose               -f compose.control-plane.yaml               -f compose.showcase.yaml               --env-file /srv/my-data-hub/control-plane.env               up -d showcase-runtime showcase-static remote-mcp
curl --fail --silent http://127.0.0.1:8790/health/ready
curl --fail --silent http://127.0.0.1:8791/healthz
```

Do not expose port `8790` in Nginx, a tunnel, firewall rule or Docker
published-port rule.

## Deploy, discovery, and readback acceptance

Before accepting this runtime, deploy an exact committed main successor and record its
commit/image identity. Verify the protected-resource document advertises both Showcase
scopes, OAuth token issuance succeeds, and `tools/list` exposes all eight tools:
`list`, `get_source`, `apply`, `rebuild`, `create_view`, `get_link`, `rotate_link`, and
`revoke_link`. Then execute remote `get_source` and `apply`; schema registration alone
is insufficient.

Run the complete A–H content-only checklist in
[showcase-runtime-v2-verification.md](showcase-runtime-v2-verification.md). In
particular, use `apply(dry_run=true)` for no-mutation validation, retain exact
source/public-page/link readbacks, and keep the existing URL for ordinary updates.
Create a disposable view via create-aware `apply(..., expected_source_revision=absent)`
and clean it up with `revoke_link`; do not rotate a partner link to demonstrate the
constructor.

Publication is not atomic with the source commit. A source commit followed by publish
failure must return `applied_not_published` (or an equally explicit status), retain the
source revision, and be recoverable with idempotent `rebuild`.

## Rollback

For content rollback, preserve the pre-change bundle and re-apply it against the then
current revision with `publish=true`; the stable link remains unchanged. For runtime
rollback, stop only `showcase-runtime` and `showcase-static`, restart `remote-mcp`
without the overlay, and remove Showcase scopes. Preserve the named state volume until
every active URL has been explicitly revoked or migrated. This runtime requires neither
PostgreSQL nor Kaggle.
