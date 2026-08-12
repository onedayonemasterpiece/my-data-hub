# Single provider authority and exchange cleanup results

## Scope and base

- Lane: `SINGLE-PROVIDER-AUTHORITY`.
- Branch/worktree: `agent/operator-reconcile-single-provider` /
  `operator-reconcile-single-provider`.
- Exact starting base: `c4a9992`; this commit follows the H1 reconciliation commits
  `bcaa026` and `9087df4`.
- No live deployment, production provider operation, or production mutation was performed.

## Corrective production contract

- The control-plane production builder returns the one `KaggleProviderAdapter` instance used by
  master control, and `create_app()` injects that exact instance into the bounded MCP provider
  gateway. Tests assert object identity rather than merely equal configuration.
- Remote MCP no longer constructs a Kaggle adapter or journal. It categorically rejects every
  `KAGGLE_*` environment key and, only in explicitly enabled operator mode, sends exact semantic
  provider requests to `/internal/mcp-provider/invoke`.
- The internal request uses a separate mode-private service token. It carries bounded arguments
  and OAuth-derived principal metadata but never the user's OAuth token ID, Kaggle token, database
  URL, provider bytes, or caller SQL. Control rechecks the fixed tool allowlist, scope, token
  lifetime, envelope bounds, and credential-shaped fields before calling the sole authority.
- Production Compose remains reader-only and provider-gateway-off by default. The acknowledged,
  post-security-gate installer override mounts the gateway token into control and remote, enables
  the control gateway, and deliberately mounts no provider environment into remote MCP.
- Provider create/version/run/read/delete MCP discovery now uses distinct closed Pydantic input
  models. The exchange manifest is typed and closed. Notebook Dataset inputs require exact
  `resource_ref`, positive numeric version, claim SHA-256, and allowed control class; the schema
  cannot express `latest`, a raw source slug, or arbitrary payload keys.

## Expired exchange retention cleanup

- Exchange reads and versions remain denied at or after expiry.
- Only the exact authenticated creator may delete the claim-bound disposable resource after
  expiry. Recipients and unrelated principals remain denied.
- Delete intent is durable before the provider effect. A terminal exact receipt is returned on
  replay without duplicate provider DML; an already-absent exact task claim reconciles to
  `ALREADY_APPLIED`.
- The stable response includes `mcp-exchange-cleanup-retention.v1`, package and manifest identity,
  expiry, the seven-day maximum TTL, operation/effect/claim identities, absent state, and a
  canonical receipt SHA-256. It contains no exchange payload bytes or instructions.

## Evidence

- Focused control/MCP/provider/deployment tests: PASS (`104` tests at the focused checkpoint).
- Full ordinary suite: PASS (`842 passed`, `3` expected opt-in skips).
- Disposable tmpfs PostgreSQL migration and duplicate-replay tests: PASS (`2 passed`).
- `ruff check .`: PASS.
- Configured strict `mypy`: PASS (five configured source files).
- `python -m compileall -q src tests`: PASS.
- `bash -n deploy/control-plane/install.sh`: PASS.
- Notebook drift check: PASS.
- Repository validator: PASS (`3372` checks, zero errors/notes).
- `git diff --check`: PASS.
