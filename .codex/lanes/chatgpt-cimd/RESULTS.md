# Lane results: chatgpt-cimd

## Scope

- Base integration SHA: `e1ed23c7d0663c87a18fd74e5345a8ac613be722`.
- Bounded ChatGPT CIMD public-client support for the provider-only MCP profile.
- No DCR, client secret, live OAuth mutation, restart, or deployment.

## Outcome

- Added exact ChatGPT-origin metadata fetching with redirect, timeout, body-size,
  content-type, client-ID, redirect-URI, grant, authentication-method, and secret-material
  checks.
- Added bounded in-memory nonsecret metadata caching; malformed/error responses are not
  cached.
- Advertised CIMD only when explicitly enabled, while retaining public method `none`,
  S256 PKCE, exact resource binding, refresh rotation, and predefined static clients.
- Enabled the exact provider-only CIMD scopes through the deployment override and removed
  the callback-known-in-advance requirement.

## Validation

- `python -m compileall -q src tests` — passed.
- OAuth/deployment focused suite — passed: 102 tests.
- `python -m pytest -q` — passed; two existing `jsonschema.RefResolver`
  deprecation warnings only.
- `python scripts/validate_repository.py` — passed: 4,324 checks, zero errors or
  notes.
- `python scripts/create_notebooks.py --check` — passed with zero drift.
- Ruff, installer Bash syntax, and `git diff --check` — passed.
