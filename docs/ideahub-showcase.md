# IdeaHub Showcase in my-data-hub

## Status and authority

Canonical product/MCP contract. The 2026-09-04 product simplification is implemented
in PR #38; code/CI acceptance is not a production deployment. Integration and live
acceptance are a separate owner-started task:
[`handoffs/showcase-product-mcp-deploy-20260904.md`](handoffs/showcase-product-mcp-deploy-20260904.md).
The earlier runtime's historical live acceptance remains in
[`operations/showcase-runtime-v2-verification.md`](operations/showcase-runtime-v2-verification.md).
It does not certify the new schemas, renderer or deployment configuration.

## Product and ordinary cycle

A mobile-first curated page helps a partner understand a working task, the result,
required inputs, actual readiness and how to contact the owner. The ordinary cycle is:

```text
read relevant IdeaHub/voice material → propose a useful card selection → owner approval
→ Showcase MCP preview → save/publish → source/link/browser readback
```

No code agent or direct Git edits are needed for ordinary content changes. IdeaHub
reading remains an external read-only input, not a new search service in Showcase.
`idea-hub/showcase/` owns curated manifests/cards. `my-data-hub` owns bounded source
mutation, validation, Astro rendering, publication, private link state and receipts.
No new CMS, database, editor, user account system or general-purpose page builder.

## Exactly eight tools

| Method | Purpose |
| --- | --- |
| `showcase.list` | List registered surfaces with masked URLs. |
| `showcase.get_source` | Read a view, its ordered cards and source revision, including drafts. |
| `showcase.create_view` | Create a NEW view from a manifest, reusing existing card IDs. |
| `showcase.apply` | Update an existing view with compare-and-swap revision protection. |
| `showcase.rebuild` | Build/publish the current saved source without changing its URL. |
| `showcase.get_link` | Obtain the exact active secret link and last build receipt. |
| `showcase.rotate_link` | Explicitly change the URL, then revoke the old URL. |
| `showcase.revoke_link` | Explicitly withdraw a published link. |

There is no ninth `validate` tool. Creation and update accept one preferred `mode`:
`preview` (default with a manifest), `save`, or `publish`.

### Create from existing cards

Use IDs returned by `get_source` or found in curated source; the placeholders below
must be replaced with actual IDs. Do not resend their full definitions.

```json
{
  "view_id": "partner-tasks",
  "view": {
    "title": "Задачи вашей команды",
    "subtitle": "Подборка сценариев с понятным результатом.",
    "item_ids": ["existing-card-id", "another-existing-id"],
    "filters": []
  },
  "mode": "preview"
}
```

Call `showcase.create_view` with this payload. No `expected_source_revision`, magic
`absent` value, `view.id`, colours or link registration are required. For publication,
repeat with `mode: "publish"` and a unique `idempotency_key` of 8–200 characters
(letters, digits, `.`, `_`, `:`, `-`). Return the result URL, then read it back.

A new card must appear in both `view.item_ids` and `items`, with its complete semantic
definition. The tool input schema requires `capability_type`:
`technical`, `product`, or `business`. Its `publish_state` defaults to `draft`; set
`ready` only after review. Do not invent facts, prices, readiness or contacts to pass
validation. Use `save` when content is not ready.

### Update without changing the URL

Read `get_source(view_id)` first. Copy its `source_revision` into
`expected_source_revision`. Use `apply` with `mode=preview`, then `save` or `publish`.
Pass `view` only when editing view-level fields; it is a complete view, not a JSON
merge patch. Pass full definitions only for new/changed cards, not all unchanged cards.
`view.item_ids` alone controls order and inclusion. A no-op does not create a commit.

Existing IDs reference shared canonical cards. Creating a view never overwrites an
existing card with different content. Updating a card used by another view is also
rejected (`SHARED_ITEM`). To adapt it for an audience, create a new card ID and replace
that ID in this view. Unmodified legacy cards without `capability_type` remain readable
and reusable, with a warning; authoring new/changed cards requires the type.

### Modes, results and retries

| Mode/state | Meaning |
| --- | --- |
| `preview` / `status=dry_run` | Validate and show changed paths. No source/link/journal write and no key required. |
| `save` / `status=applied` | Save the source draft only. It does not create a public link. |
| `publish` / `status=published` | Save, exact-readback, build and publish. URL and build receipt returned. |
| `applied_not_verified` | A commit exists, but exact source readback failed. Preserve `new_source_revision`, read source before retrying. |
| `applied_not_published` | Source saved; publication failed. Preserve revision and retry `rebuild`, not creation under a new ID. |

Preview reports `validation.valid`, `publication_ready`, publication errors,
`build_checked=false`, `buildable=null`. It **does not run Astro** and does not claim a
working build just because `package.json` exists. Successful publication reports a
checked build. Draft and visibility gates are enforced again at the publication boundary.

Use a new idempotency key for a new write, and the original key only for an identical
retry. Preview never consumes the key. A lost response does not imply that the operation
stopped. Read source/link before retrying. A replay of a partial result remains a replay;
use `rebuild` to recover publication. Source commits and publication are not one atomic
transaction. CAS conflicts require a fresh read and review, not a forced overwrite.

### Errors and bounded requests

Domain errors contain `code`, `field`, `message`, `next_action`, without provider
credentials, raw stack traces or secret URLs. For example:

```json
{
  "code": "ITEM_NOT_FOUND",
  "field": "view.item_ids[1]",
  "message": "The view references items not included in the source or proposed bundle.",
  "next_action": "Correct the item ID or supply its complete definition in items."
}
```

Important codes: `VIEW_EXISTS`, `REVISION_CONFLICT`, `ITEM_ID_CONFLICT`, `SHARED_ITEM`,
`CAPABILITY_TYPE_REQUIRED`, `ITEM_NOT_READY`, `VISIBILITY_EXCEEDED`, `INVALID_MODE`,
`IDEMPOTENCY_REQUIRED`, `IDEMPOTENCY_CONFLICT`, `REQUEST_TOO_LARGE`, `INVALID_FIELD`.
The gateway preserves allowlisted diagnostics. Infrastructure failures are not disguised
as missing input fields.

Arguments are limited to **128 KiB UTF-8**, the authenticated runtime envelope to
**256 KiB**, and a view to 100 cards. Reuse IDs to avoid large payloads. For a large new
collection, save a bounded draft and add further cards in bounded CAS updates before
publication. A hundred IDs does not guarantee room for a hundred full definitions.

### Legacy compatibility

`apply(expected_source_revision="absent", dry_run=..., publish=...)` remains supported,
as does `create_view` without a manifest for registering already existing source.
These are compatibility forms, not the recommended creation path. Do not combine
`mode` with either old flag. Old `apply(publish=true)` without `dry_run=false` remains
a preview, preserving its previous safety default.

## Cards and presentation

Use existing fields rather than a second card schema:

- `title`: the reader's working task; `summary`: concrete short deliverable.
- `benefit`: what changes in the team's work, shown on the list card.
- `requirements`: minimum inputs and meaningful limits; `available`: what was actually checked.

List cards show a canonical order number, task/result, small audience and one readiness
label. Details expose inputs and verified capabilities. Readiness is rendered consistently:
`implemented` → «Работает», `prototype` → «Прототип», `designed` → «Спроектировано»,
`concept` → «Идея». A prototype is not automatically declared pilot-ready. Category,
audience, capability type, maturity and effort remain separate source dimensions.
An unexplained effort badge is not displayed as a proxy for price or lead time.

Search is a compact sticky field. Extra filters open on focus or explicit interaction;
only dimensions with multiple values in this view are rendered. `view.filters` defaults
to `audience`, `category`, `maturity`; `[]` removes extra filters. `capabilityType` is
opt-in for audiences that benefit from the internal classification.

`contacts` accepts up to six explicit HTTPS or `tel:` contacts. An empty list falls back
to the compatible singular `contact`; its known default is Telegram `@confidentmax` (`https://t.me/confidentmax`).
Do not invent a telephone/MAX account. No contact URL is silently rerouted.

The small «Интересно» toggle stores IDs locally per view. It does not transmit a lead or
perform analytics. A visible, copyable message contains the selected card URLs; the
visitor decides whether and where to send it. Storage denial leaves in-page toggling
usable with an explicit warning. No selection is required to contact the owner.

Each share control uses the corresponding card URL, not the index URL. Static 1200×630
PNG/OG assets use only curated partner-safe content. Web Share uses a prepared PNG when
supported, then text/link; unavailable clipboard falls back to a selectable field.
Cancellation never silently copies. Native app delivery and its link-preview cache are
separate device/provider checks, not proven by stubbing `navigator.share` in CI.

Secret links, noindex, no-referrer and CSP are preserved. A secret URL is not authentication;
any recipient with it can open/forward it. Never publish internal source documents or
put full active URLs, credentials or actual partner screenshots into public Git receipts.

## Verification and deployment

```bash
python -m pip install -e '.[dev]'
npm ci --prefix showcase-site
pytest -q tests/showcase
python -m pip install playwright==1.57.0
python -m playwright install --with-deps chromium
SHOWCASE_BROWSER=1 pytest -q tests/showcase
```

Real Chromium tests render/publish temporary fixture source through the constructor,
then exercise 360×800, 390×844 and 1440×900. They cover search/reset, details, PNG/OG,
interest persistence, blocked storage, share cancellation/copy fallback, expanded menus
and a same-link update. Only OS sharing/clipboard interfaces are stubbed. GitHub Actions
stores screenshots/JUnit and a local publication receipt; these are not live partner pages.

`scripts/showcase_live_closure.py` is the existing **separate live** runner. It now leaves
main content untouched, uses preview on main, and proves creation/reuse/new-card/update/
retry/rotation/revocation on disposable source. It records cleanup paths without deleting
shared cards. A transport failure requires explicit recovery, not a fabricated PASS.

Deployment topology and operational safeguards:
[`operations/ideahub-showcase-runtime.md`](operations/ideahub-showcase-runtime.md).

### Incomplete constructor safety

`create_view` with nonempty `items` but no `view` fails with `VIEW_REQUIRED`
without source writes, link registration, publication, or idempotency journal changes.
Legacy registration applies only when no view, items, mode, or dry-run is supplied.
