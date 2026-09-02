# IdeaHub Showcase Astro renderer

A deterministic static renderer. It receives one already-sanitized JSON bundle through
`SHOWCASE_INPUT`, builds pages below `/v/<secret-slug>/`, and writes a checked artifact.

```bash
SHOWCASE_INPUT=/absolute/showcase.json \
SHOWCASE_SLUG=URL_SAFE_SECRET_AT_LEAST_20_CHARS \
SHOWCASE_ORIGIN=https://ideas.kenigevents.ru \
SHOWCASE_OUT_DIR=/absolute/output \
npm run test:build
```

The renderer does not read private IdeaHub documents. Sanitization and publication policy
are enforced by the Python control layer before this process starts.
