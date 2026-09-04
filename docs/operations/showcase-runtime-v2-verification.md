# IdeaHub Showcase runtime verification

## CURRENT NOT ACCEPTED

Repository checks and historical workflow output are not live evidence. As of
2026-09-04, live ChatGPT `showcase.list` returned `503 OAuth token request failed`,
and the discovered surface showed only six older methods. Do not mark this runtime
accepted until all receipts below are collected from the deployed exact main successor.

## Required end-to-end evidence

A. OAuth succeeds; `tools/list` exposes all eight Showcase methods; `get_source` and
`apply` execute remotely.

B. Update existing `main` through MCP only: change one disposable test string,
ordering, or CTA; prove dry-run has no mutation; publish; read exact source; verify
public page changed and URL did not.

C. Create a disposable partner view of two or three cards through create-aware
`apply(expected_source_revision=absent)`; obtain its new URL; revoke and clean it up.

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
