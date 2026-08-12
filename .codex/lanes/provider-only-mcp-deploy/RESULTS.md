# Lane results: provider-only-mcp-deploy

## Scope

- Base SHA: `1e699908fefd6c25e1bb13aa77a987c507f0d2e6`.
- Adds an explicit provider-only MCP deployment path without weakening the default/full
  install.
- No live deployment, restart, OAuth mutation, or provider mutation was performed.

## Outcome

- Added `INSTALL_MY_DATA_HUB_PROVIDER_MCP`, a runtime Compose override, exact-commit
  gate, private-file checks, Compose preflight, systemd autostart, rollback, and loopback
  readiness receipt validation.
- Removed inherited master assets, tunnel broker, checkpoint key, master sessions/TLS,
  embeddings, connectors, and acceptance requirements from this profile.
- Reused one central `KaggleProviderAdapter` and the existing control-ledger Kaggle journal;
  no second adapter or Kaggle credential copy is created.
- Added a canonical-data-independent provider permit restricted to private MCP-controlled
  resources while master is `ABSENT`. It cannot authorize data, master, migration,
  checkpoint, blogger, generic write, or acceptance-scenario operations.
- Restricted the runtime catalog and OAuth protected-resource metadata to the provider-only
  tools/scopes, with exact `provider:write` and an authenticated central gateway.
- Added static-bearer rejection and bounded mode-0600 write-gate/gateway token checks.
  Secrets are neither logged nor copied to receipts/artifacts.
- Added nonblocking static OAuth client inspection. A usable bounded client ID is reported;
  otherwise the receipt reports `CHATGPT_OAUTH_CLIENT_CONFIGURATION_REQUIRED` for the
  exact callback configuration step.

## Deployment prerequisites and exact command

See `docs/operations/provider-only-mcp-deploy.md`. After review, the exact command is:

```bash
MY_DATA_HUB_APPROVED_CONTROL_COMMIT="$(git rev-parse HEAD)" \
  deploy/control-plane/install.sh INSTALL_MY_DATA_HUB_PROVIDER_MCP
```

Required inputs are Docker Compose with `!override` support, user lingering, the exact
approved clean commit/image, private provider/MCP/OAuth environments, and the existing
OAuth/write-gate/gateway secrets. Master, PostgreSQL, tunnel, checkpoint, connector, and
acceptance assets are deliberately irrelevant.

## Validation

- `python -m compileall -q src tests` — passed.
- Focused provider/deployment and unchanged-default tests — passed (60 tests).
- `python -m pytest -q` — passed; two existing `jsonschema.RefResolver`
  deprecation warnings only.
- `python scripts/validate_repository.py` — passed: 4,286 checks, zero errors or
  notes.
- `python scripts/create_notebooks.py --check` — passed with zero drift.
- Ruff on all changed Python files, `bash -n deploy/control-plane/install.sh`, and
  `git diff --check` — passed.

## Integration note

The concurrent provider batch lane adds `provider.resources.list` and
`provider.resources.download`. This lane already names both in its provider-only control and
MCP allowlists; after the batch catalog is integrated they become visible without widening
any unrelated authority.
