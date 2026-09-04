# IdeaHub Showcase runtime verification

## CURRENT NOT ACCEPTED

Repository checks and historical workflow output are not live evidence. As of
2026-09-04, the production Showcase runtime still has no completed A–H receipt.

The original live ChatGPT call returned `503 OAuth token request failed` and exposed
only six older methods. A bounded GitHub-hosted closure then tested every existing
repository credential name without logging or persisting any secret:

| Run | Trigger | Result |
|---|---|---|
| `33889785417` | same-repository pull request | `MCP_TOKEN_MISSING` |
| `33890805899` | same-repository pull request, canary fallback | `MCP_TOKEN_MISSING` |
| `33890998236` | same-repository pull request, full known MCP-token fallback chain | `MCP_TOKEN_MISSING` |
| `33891684997` | trusted direct push, full known MCP/data-token fallback chain | `MCP_TOKEN_MISSING` |

All four attempts failed before opening an MCP session. They made no source, link,
publisher, or public-page mutation; no disposable view was created. Run
`33891684997` uploaded a sanitized failure receipt with
`main_rollback_completed=false`, `disposable_revoked=true`, and
`source_cleanup_required=null`.

This is consistent with the repository's production credential contract:
`scripts/provider/devstand_acceptance_controller.py` forbids static MCP bearers and
requires the private host-side `MY_DATA_HUB_MCP_OAUTH_CREDENTIAL_FILE` available on
DevCoveer. A GitHub-hosted runner cannot substitute for that credential boundary.
Owner-hosted/self-hosted GitHub Actions runners remain prohibited. Therefore the
remaining closure must run directly on DevCoveer through its authorized execution
surface, not through GitHub Actions.

Do not mark this runtime accepted until all receipts below are collected from the
deployed exact main successor.

## Required end-to-end evidence

A. OAuth succeeds; `tools/list` exposes all eight Showcase methods; `get_source` and
`apply` execute remotely.

B. Update existing `main` through MCP only: change one disposable test string,
ordering, or CTA; prove dry-run has no mutation; publish; read exact source; verify
public page changed and URL did not.

C. Create a disposable partner view of two or three cards through create-aware
`apply(expected_source_revision=absent)`; obtain its new URL; rotate with a repeated
idempotency key, prove no third URL, revoke, and clean up its source YAML.

D. At 390x844, prove one column and no horizontal overflow; category, capability,
readiness, and search filters change the count.

E. Verify sharing beneath a card, on detail, and at index bottom; verify Web Share
payload or fallback; production CTA resolves to `@confidentmax`.

F. Verify CSP has no executable inline script and no script `unsafe-inline`; capture
no console or network errors.

G. Preserve a receipt containing exact source revision, artifact/tree hash, file and
HTML counts, and stable slug; readback must match the submitted payload.

H. Roll back without another method: retain the pre-change bundle, re-apply it at the
current revision, publish it, and prove the link is unchanged.

For every evidence item retain timestamp, deployed commit/image identity, masked URL
where appropriate, request/result receipts, and failure evidence. A source commit plus
a failed publish must be labelled `applied_not_published` (or equivalent), never as a
successful publication; retry safely with `rebuild`.
