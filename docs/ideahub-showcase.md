# IdeaHub Showcase in my-data-hub

## Status and authority

This is the canonical product and MCP contract for IdeaHub Showcase. It records an
owner-approved constructor target; it is **not** production-acceptance evidence.
At this decision point, `origin/main` was
`a6eb589e88a4219f6f19cfcf2154a31fe9523e35`. That source tree contains
`get_source`, `apply`, sharing, `capability_type`, and CSP changes, but source code
or a deployment scaffold is not proof that a remote service is live.

On 2026-09-04 a live ChatGPT call to `showcase.list` returned `503 OAuth token
request failed`. The discovered ChatGPT surface showed only six older methods.
OAuth, discovery, deployment, and remote readback are therefore **CURRENT NOT
ACCEPTED** until the receipts in
[`operations/showcase-runtime-v2-verification.md`](operations/showcase-runtime-v2-verification.md)
exist.

## Product goal and ordinary cycle

IdeaHub Showcase is a constructor for mobile-first partner showcases. The ordinary
content cycle contains no Codex:

```text
model reads IdeaHub and relevant voice material through an available read-only source
→ proposes card set and cuts
→ owner approves
→ model invokes Showcase MCP
→ MCP mutates source, validates, builds, publishes, and returns receipts/link
→ browser/mobile check
```

Codex is required only to change or repair the constructor itself. This release does
not add a general IdeaHub search API: IdeaHub reading is an external read-only input
(GitHub/IdeaHub connector), and the Showcase API begins with the approved manifest.
Acceptance must demonstrate that source and publication proceed without Codex after
approval.

`idea-hub/showcase/` remains the curated source of manifests and cards.
`my-data-hub` owns bounded source mutation, validation, rendering, publication,
private active-link state, and receipts.

## MVP MCP surface

The MVP has exactly these eight methods:

- `showcase.list`
- `showcase.get_source`
- `showcase.apply`
- `showcase.rebuild`
- `showcase.create_view`
- `showcase.get_link`
- `showcase.rotate_link`
- `showcase.revoke_link`

Do not add a `showcase.validate` method: `showcase.apply(dry_run=true)` is the
validation preview. `create_view` remains for backward compatibility and an operator
flow where source already exists; it is not the primary constructor flow.

### Required create-aware `apply` change

The only missing constructor contract change is create-aware `showcase.apply`:

- `expected_source_revision` accepts either an exact current revision for update or
  the reserved value `absent` for CAS-create.
- With `absent`, the operation succeeds only when the view truly does not exist;
  collision fails closed. It requires a non-empty complete `view` and all items
  referenced by `view.item_ids`.
- `dry_run=true` validates the complete proposed bundle and reports changed paths and
  buildability; it writes no source and creates no link.
- `dry_run=false, publish=false` writes the source draft only.
- `dry_run=false, publish=true` writes source and, when no registry/link exists,
  internally creates a stable secret slug, builds, publishes, and returns URL and
  receipts.
- An exact-revision update keeps its existing slug. Only `rotate_link` changes URL.

A source commit and publication are not one atomic transaction. If source commit
succeeds but publication fails, the return must explicitly report
`applied_not_published` (or an equivalent unambiguous state), preserve the committed
revision, and allow an idempotent `rebuild`; it must not report publication success.

### Flows

Existing view:

```text
get_source
→ owner-approved patch
→ apply(dry_run=true)
→ apply(dry_run=false, publish=true)
→ get_source + get_link readback
```

The link remains stable.

New view:

```text
owner-approved complete manifest
→ apply(expected_source_revision=absent, dry_run=true)
→ apply(expected_source_revision=absent, dry_run=false, publish=true)
→ get_source + get_link
```

This returns a new stable secret link.

## Manifest and presentation rules

- Canonical card ordering is only `view.item_ids`.
- `capability_type` is `technical`, `product`, or `business`. It is required for new
  or modified partner views; legacy `null` remains readable only until migration.
- Category, audience, maturity, and effort remain independent dimensions.
- Production contact CTA is Telegram `@confidentmax`,
  `https://t.me/confidentmax`.
- Sharing appears beneath each card, on its detail page, and at the bottom of the
  index. Web Share uses a short text, URL, and business-card asset where supported;
  fallback copies link/text. General and card-specific OG/share assets are static.

## Runtime inputs and safety

Existing runtime variables and deployment isolation remain documented in
[`operations/ideahub-showcase-runtime.md`](operations/ideahub-showcase-runtime.md).
The renderer must read one exact source revision, accept only ready records within the
view visibility ceiling, receive only sanitized publication JSON, and publish through
a shell-free deployment-provided argv contract. Normal rebuild reuses the stored slug;
rotation publishes the new prefix before revoking the old one.
