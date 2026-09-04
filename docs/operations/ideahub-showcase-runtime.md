# IdeaHub Showcase: production runtime

## Evidence status

This document specifies deployment, not proof of a particular deployment. The earlier
2026-09-04 live acceptance is recorded in
[showcase-runtime-v2-verification.md](showcase-runtime-v2-verification.md).
The product/MCP changes in PR #38 require a new coordinated release and live smoke;
old receipts do not certify the new client schema, PNG renderer or runtime settings.
Follow [the bounded release handoff](../handoffs/showcase-product-mcp-deploy-20260904.md).

The runtime image installs the HTTP client used by the gateway, and the read-only
static edge writes all Nginx temporary files only to its private `/tmp` tmpfs.
Control-plane release preparation makes the archived release tree read/traverse-only
for all runtime UIDs before removing every write bit. This is required because Compose
bind-mounts `deploy/showcase-runtime/nginx.conf` into the unprivileged static edge;
leaving the archive at the installer's `umask 077` permissions causes that edge to
restart with `Permission denied` even though the release itself is immutable.
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
MY_DATA_HUB_SHOWCASE_GATEWAY_TIMEOUT_SECONDS=240
MY_DATA_HUB_SHOWCASE_IMAGE=my-data-hub-showcase:local
MY_DATA_HUB_SHOWCASE_MEMORY_LIMIT=512m
MY_DATA_HUB_SHOWCASE_CPU_LIMIT=1.00
MY_DATA_HUB_MCP_SCOPES_WITH_SHOWCASE=<existing-owner-scopes>,showcase:read,showcase:write
```

Set the actual runtime env `MY_DATA_HUB_SHOWCASE_MAX_REQUEST_BYTES=262144` and the
actual edge env `MY_DATA_HUB_SHOWCASE_GATEWAY_TIMEOUT_SECONDS=240`. Old explicit
values (65536/45/180) override new defaults and must be inspected, not assumed updated.
Arguments remain capped at 128 KiB UTF-8 inside the 256 KiB authenticated envelope.
Authorized Showcase source/build calls get a scoped 300-second MCP admission budget;
unrelated calls keep their existing limit. Align the outer proxy/operator client budget
with it. A client timeout does not prove that the source write or build stopped: always
read source/link and reuse the original idempotency key for an identical retry.

Deploy the edge and isolated runtime from the same tested revision. The new edge forwards
`mode`, so running it against an old runtime is not an accepted mixed-version state.
The runtime image includes pinned Sharp and DejaVu fonts for static PNGs. Build the actual
container and exercise a representative catalog under its memory/CPU limits; unconstrained
CI builds do not prove a 512 MiB production memory budget. No font files go into public
source or downloadable receipts.

The OAuth owner/operator client receives `showcase:read` and
`showcase:write`. A reader explicitly granted `showcase:read` may call
`showcase.list` (masked URLs) and `showcase.get_source` (curated source, including drafts).
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
docker compose \
  -f compose.control-plane.yaml -f compose.showcase.yaml \
  --env-file /srv/my-data-hub/control-plane.env build showcase-runtime
docker compose \
  -f compose.control-plane.yaml -f compose.showcase.yaml \
  --env-file /srv/my-data-hub/control-plane.env up -d showcase-runtime showcase-static remote-mcp
curl --fail --silent http://127.0.0.1:8790/health/ready
curl --fail --silent http://127.0.0.1:8791/healthz
```

Do not expose port `8790` in Nginx, a tunnel, firewall rule or Docker
published-port rule.

The public static edge must send `X-Robots-Tag` with the complete private-surface
policy (`noindex`, `nofollow`, `noarchive`, `nosnippet`, `noimageindex`), plus
`Referrer-Policy: no-referrer` and the repository CSP. Acceptance checks the live
response rather than only the checked-in Nginx template.
At the required `390x844` viewport, long audience/status badges wrap inside the
card body; the document must not acquire horizontal overflow.

## Deploy, discovery, and readback acceptance

Before accepting this runtime, deploy an exact committed main successor and record its
commit/image identity. Verify the protected-resource document advertises both Showcase
scopes, OAuth token issuance succeeds, and `tools/list` exposes all eight tools:
`list`, `get_source`, `apply`, `rebuild`, `create_view`, `get_link`, `rotate_link`, and
`revoke_link`. Then execute remote `get_source` and `apply`; schema registration alone
is insufficient.

Use the current [constructor contract](../ideahub-showcase.md) and the existing
`scripts/showcase_live_closure.py`. The runner leaves main content/link unchanged,
previews main, then creates one disposable view with an existing card ID and one new
card definition through `create_view(mode=preview|publish)`. It verifies duplicate create,
CAS update with a stable URL, rotation retry and revocation. It also checks public security
headers and the mobile browser. Hidden irrelevant filters are allowed; rendered filters
must work. Native share payload is tested, not delivery to a real messaging app.

The runner creates source and links on production and therefore requires the owner's
explicit integration/live acceptance task. It is not an ordinary unit test. Its required
env is `MY_DATA_HUB_MCP_CANARY_ENDPOINT`, `MY_DATA_HUB_MCP_OAUTH_CREDENTIAL_FILE`,
`SHOWCASE_MAIN_LINK_FILE` (absolute private 0600 output), plus the release image/commit
metadata and a safe optional `SHOWCASE_LIVE_RUN_ID`. Do not print the credentials or full
URLs. Keep the sanitized receipt under `SHOWCASE_LIVE_RECEIPT`.

After testing, revoke only disposable links. Remove only the receipt's disposable source
paths if cleanup is approved, never the reused canonical card. On a lost response inspect
that same temporary view before recovery; `manual_recovery_required` is not a clean PASS.
Rebuild existing partner views after the runtime upgrade through MCP, preserving URLs and
source content; editorial changes are a separate owner-approved content operation.

Publication is not atomic with the source commit. A source commit followed by publish
failure must return `applied_not_published` (or an equally explicit status), retain the
source revision, and be recoverable with idempotent `rebuild`.

## Rollback

For runtime rollback restore the previous tested edge/runtime image pair and configuration,
preserving private state, gateway/OAuth grants and existing static pages. Do not revoke
partner links or delete the named state volume to undo an application release.
For content rollback, preserve the pre-change bundle, review concurrent changes and apply
only the intended correction against the current revision with `mode=publish`; shared-card
protection remains active. Source already committed during a partial publish must be
accounted for explicitly. This runtime requires neither PostgreSQL nor Kaggle.

The Compose gateway timeout fallback is 240 seconds, matching the Python gateway default.
Explicit deployed environment values still take precedence and must be checked at rollout.

## PR #38 integration gate — 2026-09-04 UTC

**Integrated, not deployed; live acceptance is not closed.** PR #38 was squash-merged
as `27a187a28f1cdbfe27951f985933d702984060a1`. Its tree exactly matches tested PR tip
`cc1857d6385c68d82f1efb0601c4fcd0305cc5a6`. The integration fixes reject incomplete
constructors before publication and align the Compose timeout fallback to 240 seconds.

| Request | Status / evidence |
| --- | --- |
| R1 — fresh integration and tests | Done. Fresh main, PR comments/diff/CI reviewed; exact-tip CI green; squash merge preserves the final tree without transitional workflows. |
| R2 — protected baseline | Done. Private 0700/0600 backups contain container inspection/config, state/public archives, active-link hashes and raw YAML SHA-256/Git blob hashes for main and pharma-business-ai. Source revision `c4a234cbd6031ac03562793e02553f01d0e4330d`; no cross-version serialized-default comparison. |
| R3 — paired rollout | Blocked pending an authorized CLI acceptance credential. No services/config switched. Actual old runtime request limit remains 65536 and edge timeout remains 180; required new values are **not yet deployed**. Existing external MCP proxy read/send budgets are 300 seconds. |
| R4 — fresh authorized schemas/preview | Partial. Connected ChatGPT MCP `get_source(main)` succeeds. Existing CLI operator credential returns HTTP 401 `invalid_scope` during edge initialization, not a master checkpoint failure. Cached ChatGPT apply schema rejects the source contact href and requires an idempotency key before runtime; this is not a successful preview. Fresh new-schema discovery/preview remain pending. |
| R5 — partner rebuilds | Pending. Neither partner surface was rebuilt/rotated/revoked or editorially changed. |
| R6 — disposable live acceptance | Blocked on credential. Runner and additional negatives have not run; no disposable source/link was created, so no cleanup is claimed. |
| R7 — public browser acceptance | Pending rollout. Real Chromium fixture acceptance passed at all three requested sizes; it is not post-deploy public acceptance. No partner messages, native OS sharing, or messenger delivery/readback performed. |
| R8 — release evidence/rollback | Partial. Baseline and coordinated rollback images retained; no rollback needed because deployment was held. Final deployed/live receipts and owner reconnect checks remain open. |

Validation at the tested tip:

- `SHOWCASE_BROWSER=1 pytest -q tests/showcase`: initial 88 passed; after the
  constructor regression, all **89 Showcase tests** also pass within the full run.
- Full suite: **1806 passed, 4 skipped** (non-Showcase live prerequisites).
  An initial local invariant failure was caused by an archived duplicate Compose tree
  under artifacts; the archive was relocated outside the test worktree, without
  weakening the invariant. Complete rerun is green.
- Repository validator, tracked-secret scan, compileall, Ruff, mypy and notebook drift
  check pass. GitHub Actions runs `33927462668` (contracts/PostgreSQL) and
  `33927462659` / `33927459947` (Showcase/browser/build) pass at the exact PR tip.
- Clean archive Docker builds passed for both images. Isolated renderer ran as
  UID 65532, read-only, network disabled, 1 CPU, 512 MiB memory/no extra swap and
  256 MiB work tmpfs. Main: 32 cards, 33 HTML/PNG, 24.09 s; pharma: 6 cards,
  7 HTML/PNG, 21.42 s. All 40 PNGs have PNG signatures and 1200×630 dimensions.
  These were isolated output builds, **not partner publication**.

Built, **not deployed**, image IDs (tagged with tested tip `cc1857d6…`):

- edge: `sha256:da6119f1251f1538c1a68f916ae8d9c421f4e561b86d40c5a2e8494ae5d7ef31`
- renderer: `sha256:2c278efb0c0c0988d86eab89ec909de95d1b688df1a1289507070a9857434026`

Still deployed from `622031f36f937a3015e17f361a1088b6864a3f03`, all five services healthy:

- edge: `sha256:af7cac55eb0b82fe6590a388c201080e3c7395506026e690bcb4089dd20b020f`
- renderer: `sha256:d7ce1e8b0c863e4854dee24db15292369e0a292210ec225409a0a2ae146b0066`

Dependency audit is **not zero**: npm reports Astro/Sharp high and esbuild low
advisories. No automatic dependency upgrade was applied. The Sharp advisory concerns
untrusted image decoders; this renderer feeds only its own XML-escaped SVG, not
uploaded GIF/TIFF/VIPS input. This bounded observation does not certify the entire
package dependency graph. See [the Sharp maintainer advisory](https://github.com/lovell/sharp/security/advisories/GHSA-f88m-g3jw-g9cj).
Retain the audit output for security follow-up rather than describing it as clean.

Local evidence: `artifacts/pr38-release-20260904/` in the primary repository,
with private material under its protected `private/` directory (never commit).
Only reproducible build caches and two obsolete non-running image tags were removed
for disk space; current rollback images, volumes, state and pages were preserved.

Next gate: supply an existing credential with `showcase:read showcase:write`, or
owner-authorize a separate temporary OAuth grant through the existing flow. Do not
widen/revoke the owner's current grant or copy its rotating refresh token into a new
file. Then revalidate current source/config, execute the coordinated rollout and
complete R4–R8. Owner refresh/reconnect and a fresh owner-client tools/list plus
keyless preview remain **pending**, not already achieved by the server work.
