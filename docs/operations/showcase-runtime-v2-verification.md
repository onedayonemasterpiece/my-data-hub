# IdeaHub Showcase runtime verification

## ACCEPTED — 2026-09-04

The A–H live acceptance completed on DevCoveer through the production OAuth MCP
endpoint. The sanitized durable receipt is
[`evidence/2026-09-04-ideahub-showcase-live-closure.json`](evidence/2026-09-04-ideahub-showcase-live-closure.json).
The full secret URL is deliberately absent from Git, receipts, logs, and issue text.

Accepted runtime identity:

- live run: `devcoveer-20260904T175724Z`;
- deployed `my-data-hub` commit: `bb9b2cdd317d1fc7162505abaf02c3b8de8fa278`;
- control image: `sha256:af7cac55eb0b82fe6590a388c201080e3c7395506026e690bcb4089dd20b020f`;
- Showcase image: `sha256:d7ce1e8b0c863e4854dee24db15292369e0a292210ec225409a0a2ae146b0066`;
- final IdeaHub source/readback revision after cleanup and continuation:
  `cd6ec63f34fa05449ee53b1eb181c28c87d9ce06`;
- final tree hash: `dc546771f5532e893ef751cd4a12933340bc8f0450ef60da6d35ff68b37eae26`;
- build: 71 files, including 33 HTML files.

## A–H result

| Gate | Result | Live evidence |
|---|---|---|
| A | PASS | OAuth refresh succeeded; all eight Showcase methods were invoked against production. |
| B | PASS | `main` dry-run made no mutation; temporary content was applied, published, and read back at the same URL. |
| C | PASS | Disposable two-card view returned 200, rotated, rejected the old URL with 404, repeated its idempotency key without a third URL, and was revoked with 404. |
| D | PASS | Playwright at 390×844 found one column, no horizontal overflow, and changing category/capability/maturity/search counts. |
| E | PASS | 33 index share controls, detail sharing, Web Share payload, and `@confidentmax` CTA passed. |
| F | PASS | No console/network errors or executable inline scripts; CSP `script-src` has no `unsafe-inline`; the complete private robots policy and `no-referrer` passed. |
| G | PASS | Source revision, masked URL, slug SHA-256, tree hash, file/HTML counts, commit, and image identities are retained in the sanitized receipt. |
| H | PASS | The exact pre-change bundle hash was restored and the original `main` URL remained unchanged. |

The disposable source was removed in IdeaHub commit
`9ec74fbb0d2b601722074746dfb1a34883fe74b0`. Its created and rotated URLs were
already revoked before deletion. A post-cleanup `showcase.create_view(main)`
idempotency readback plus explicit `showcase.rebuild(main)` republished the final
IdeaHub revision while retaining the existing secret URL.

## Release and adjacent-surface regression evidence

- `python -m compileall src tests`: PASS;
- repository/schema/layout/security validation: PASS, 4,769 checks;
- generated notebook drift check: PASS, no drift;
- full Ruff check: PASS;
- full Pytest suite: PASS (five expected skips);
- all five deployed health/metadata endpoints: HTTP 200/204 as specified;
- live `platform.status`: PASS;
- live catalog: 26 tools, retaining the provider resource/upload surfaces and
  `youtube.video.analyze` alongside the eight Showcase tools;
- the pre-existing `voice-v2-hotfix.conf` systemd drop-in remained active during
  the exact-commit deployment.

## Regression contract

Future releases must repeat the A–H checks when changing the Showcase gateway,
renderer, OAuth scopes, source writer, link state, static edge, or publication
timeouts. A source commit plus a failed publish is `applied_not_published`, never
publication success; read source/link state before a retry. Keep the public URL out
of durable evidence and use only a host-side rotating OAuth credential.

## Historical blocker (closed)

The earlier GitHub-hosted runs `33889785417`, `33890805899`, `33890998236`, and
`33891684997` ended with `MCP_TOKEN_MISSING`. This correctly demonstrated that a
GitHub-hosted runner cannot replace DevCoveer's host-side OAuth credential. No
self-hosted GitHub Actions runner was added; final acceptance ran directly on
DevCoveer as required.
