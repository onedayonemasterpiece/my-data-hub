# IdeaHub Showcase in my-data-hub

## Product contract

IdeaHub Showcase is a deterministic mobile-first Astro projection of curated,
publication-safe records from the private `idea-hub` repository. A normal rebuild keeps
the existing high-entropy URL. Link rotation and revocation are explicit owner actions.

The product deliberately has two layers:

1. `idea-hub/showcase/` owns the curated items and view manifests.
2. `my-data-hub` validates, renders, publishes and remembers active links.

## Routine after the one-time deployment

```text
edit or add showcase/items/*.yaml and showcase/views/*.yaml in idea-hub
→ call showcase.rebuild(view_id)
→ receive status, immutable source revision, build receipt and current URL
```

No code agent is required for ordinary content changes, rebuilding an existing surface,
retrieving a forgotten link, creating a surface from a new view manifest, rotating a URL,
or revoking a URL.

A code agent is required only when the schema, visual components, MCP contract or hosting
infrastructure changes.

## MCP contract

The standalone contract server in `my_data_hub.showcase.mcp_server` exposes:

- `showcase.list` — list registered surfaces;
- `showcase.get_link(view_id)` — return the current full link and last receipt;
- `showcase.rebuild(view_id)` — validate, build, publish and return the same link;
- `showcase.create_view(view_id, publish=true)` — register a source view with its own link;
- `showcase.rotate_link(view_id)` — build under a new slug and revoke the old prefix;
- `showcase.revoke_link(view_id)` — remove or disable the current prefix.

The standard `my-data-hub` MCP server exposes the same six tools when
`MY_DATA_HUB_SHOWCASE_ENABLED=true` and the authenticated owner/operator token carries
`showcase:read` and `showcase:write`. They therefore use the existing OAuth boundary,
security metadata and audit path. `my-data-hub-showcase-mcp` remains only a local stdio
entry point for focused contract testing.

## Runtime inputs

| Variable | Purpose |
|---|---|
| `MY_DATA_HUB_SHOWCASE_ENABLED` | Enables the six tools in the standard my-data-hub MCP catalog. |
| `MY_DATA_HUB_SHOWCASE_GITHUB_TOKEN` | Read-only token for private `idea-hub` source files. |
| `MY_DATA_HUB_SHOWCASE_GITHUB_REPOSITORY` | Defaults to `onedayonemasterpiece/idea-hub`. |
| `MY_DATA_HUB_SHOWCASE_GITHUB_REF` | Defaults to `main`; every build resolves it to one exact commit. |
| `MY_DATA_HUB_SHOWCASE_GITHUB_ROOT` | Defaults to `showcase`. |
| `MY_DATA_HUB_SHOWCASE_ORIGIN` | Public origin, for example `https://ideas.kenigevents.ru`. |
| `MY_DATA_HUB_SHOWCASE_STATE_PATH` | Persistent private JSON state containing slugs and receipts. |
| `MY_DATA_HUB_SHOWCASE_SITE_ROOT` | Path to the bundled `showcase-site`. |
| `MY_DATA_HUB_SHOWCASE_PUBLISH_COMMAND_JSON` | JSON argv template for checked artifact publication. |
| `MY_DATA_HUB_SHOWCASE_REVOKE_COMMAND_JSON` | JSON argv template for removing or disabling a prefix. |

For development, set `MY_DATA_HUB_SHOWCASE_SOURCE_ROOT` and
`MY_DATA_HUB_SHOWCASE_LOCAL_PUBLISH_ROOT`; no GitHub or bucket credentials are then used.

Publisher command templates are arrays, not shell strings. Supported placeholders are
`{source}`, `{prefix}`, `{slug}` and `{view_id}`. Example shape:

```json
["aws", "s3", "sync", "{source}", "s3://example-bucket/{prefix}", "--delete"]
```

The deployment command must additionally apply the headers described by
`showcase-headers.json` to HTML objects. The final infrastructure smoke verifies
`X-Robots-Tag`, `Referrer-Policy`, root isolation, old-link revocation and URL readback.

## Local development

```bash
cd showcase-site
npm install
cd ..

export MY_DATA_HUB_SHOWCASE_SOURCE_ROOT="$PWD/tests/showcase/fixtures"
export MY_DATA_HUB_SHOWCASE_SITE_ROOT="$PWD/showcase-site"
export MY_DATA_HUB_SHOWCASE_STATE_PATH="$PWD/.tmp/showcase-state.json"
export MY_DATA_HUB_SHOWCASE_LOCAL_PUBLISH_ROOT="$PWD/.tmp/public"
export MY_DATA_HUB_SHOWCASE_ORIGIN="https://ideas.example.test"

python -m my_data_hub.showcase.cli rebuild main
python -m my_data_hub.showcase.cli get-link main
python -m my_data_hub.showcase.cli create-view lecturers-guides
python -m my_data_hub.showcase.cli rotate-link lecturers-guides
```

## Safety properties

- only records marked `publish_state: ready` are accepted;
- a record cannot exceed the view's visibility ceiling;
- source files are read from one exact Git commit per build;
- the renderer receives only sanitized JSON and cannot read private documents;
- generated HTML is checked for `noindex`, `noarchive`, `no-referrer` and forbidden
  internal markers;
- a normal rebuild reuses the stored slug;
- rotation publishes the new prefix before revoking the old one;
- state writes are atomic and stored with owner-only filesystem permissions;
- production publication is a deployment-provided argv command, executed without a shell.
