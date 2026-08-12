# Provider run input-claim hardening results

## Scope and base

- Lane: `PROVIDER-RUN-INPUT-CLAIMS`
- Isolated branch/worktree: `agent/provider-run-input-claims` / `provider-run-input-claims`
- Exact base: atomic master-admission commit `460d17ae863686da82aaddabcc07e592125ab64d`
  (whose original integration base was `ed95ee2f9503650c08bb6bb56d1444fe46414cb8`).
- Scope: the remote MCP Kaggle provider gateway's notebook-run input authorization and focused
  gateway tests. No scheduler/matrix runner, protected orchestrator adapter path, migration, live
  Kaggle call, deployment, or production mutation was performed.

## Root cause and reproduced exploit

`KaggleMCPProviderGateway._run()` accepted arbitrary caller-provided `dataset_sources` strings and
passed them to the Kaggle adapter configured with the provider account credential. The adapter
accepts both unversioned slugs and exact versions, so an MCP operator could ask the credentialed
notebook to attach an unregistered, `orchestrator_protected`, external, or latest Dataset without a
durable control-plane authorization decision.

A focused regression first reproduced the issue: the old run payload containing
`owner/orchestrator-checkpoints` reached `push_private_notebook()`. It now fails at the exact
gateway contract before any adapter call.

## Implemented contract

- Replaced remote run `dataset_sources` with bounded `dataset_inputs` (maximum 16).
- Every item has exactly four fields: `resource_ref`, positive integer `provider_version`,
  `claim_sha256`, and `control_class`.
- Only `mcp_managed` and `mcp_exchange` Dataset claims are admissible. Protected, external,
  unknown, malformed, duplicate, unregistered, and non-numeric/latest inputs fail closed.
- Every input must match both the immutable task-resource claim and its exact control-ledger
  provider projection: Kaggle provider, Dataset kind, private visibility, source task identity,
  numeric source version, and control class.
- `mcp_managed` inputs require the same task ID, the same Kaggle namespace owner as the target
  notebook, and the same authenticated creating principal. Dataset registrations now persist only
  this minimal authorization subject metadata; no credentials or file content are stored.
- `mcp_exchange` inputs reuse the signed exchange manifest authorization: intended recipient and
  unexpired TTL are checked at the moment of the run.
- Only after all checks does the gateway render `owner/slug/<numeric-version>` values for the
  Kaggle adapter. The normalized exact claims are also bound into the idempotent provider intent.
- The trusted internal Kaggle adapter and orchestrator-protected lifecycle paths are unchanged;
  this is solely the remote MCP gateway boundary.

The exact acceptance-scenario request/controller is not present at this lane's required base. The
existing operational evidence driver only discovers the provider tool name and does not construct
a run request. Any H6 controller that invokes `provider.resources.run` must use the exact
`dataset_inputs` object above; there is intentionally no compatibility path for raw slugs.

## Test coverage

`tests/control/test_mcp_operator_provider.py` proves:

- a registered managed input reaches the adapter only as `owner/mcp-data/1`;
- a legacy raw slug is rejected before adapter invocation;
- latest/string versions, mismatched numeric versions, unknown claims, and protected, external,
  or unknown control classes are rejected before adapter invocation;
- managed claims cannot cross task, Kaggle namespace owner, or authenticated creator;
- exchange input succeeds for the intended recipient before expiry and reaches the adapter as an
  exact numeric ref;
- the same exchange input is denied to a non-recipient and after TTL expiry, without another
  adapter call;
- provider content remains absent from the metadata-only control ledger.

## Validation evidence

All commands ran in the isolated worktree against this implementation.

- focused provider/operator gateway suite — PASS (`6 passed`).
- `pytest` — PASS (`804 passed`, three expected opt-in skips, two pre-existing
  `jsonschema.RefResolver` deprecation warnings).
- `python -m compileall -q src tests` — PASS.
- `ruff check .` — PASS.
- configured `mypy --config-file pyproject.toml` target set — PASS (five source files).
- `python scripts/create_notebooks.py --check` — PASS, zero drift.
- `python scripts/validate_repository.py` — PASS (`3251` checks, zero errors/notes).
- `git diff --check` — PASS.

No disposable PostgreSQL or live Kaggle run was required: this lane changes neither PostgreSQL SQL
nor the production provider. Its security property is specifically that denied input never reaches
the credentialed adapter.
