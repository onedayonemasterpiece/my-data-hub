# H1/H2 operational closure results

## Scope and base

- Lane: `H1-OPERATIONAL`
- Isolated branch/worktree: `agent/h1-operational` / `h1-operational`
- Exact base: `0b86000cf2a0adaf15a99feae44fb823474d5bb7`
- Core implementation: `ada8ed69a2d9c541671729dd5c4334c228283fb3`
- Requirements: H1 production operator MCP activation and exact PostgreSQL mutation
  boundary; H2 production provider wiring and `mcp_exchange` confidentiality/expiry
  mutation boundary.

## Done

### Bounded canonical operator transaction

- Added append-only migration `0016_mcp_operator_transaction_boundary.sql`.
- Granted `mdh_mcp_editor` column-level INSERT/UPDATE and table DELETE only for
  `hub.project` and `hub.content_item`; it has no canonical-state UPDATE, owner,
  superuser, DDL, role-admin, BYPASSRLS or server-file authority.
- Added editor-only transaction tracking and a deferred commit guard. Direct table DML
  cannot commit without a same-transaction receipt.
- The receipt function rechecks the ACTIVE epoch, compares/advances the canonical
  revision once, records `sync.audit_event`, emits the idempotent semantic
  `sync.external_outbox` operation, and records an immutable transaction receipt.
- The MCP broker now refuses zero-row/over-limit apply, calls that function only for
  apply, and returns the new canonical revision. Preview rolls all data-plane effects
  back. SQL/parameter fingerprints contain no raw parameter values.
- The SQL AST policy now repeats the exact column allowlist and fingerprints normalized
  SQL, rejecting generated/search/revision/audit timestamp columns.

### Explicit default-off production activation

- Base `compose.control-plane.yaml` remains unchanged and literal reader-only (`false`)
  for both MCP writes and operator credential issuance.
- Added explicit `INSTALL_MY_DATA_HUB_CONTROL_PLANE_OPERATOR`; normal install remains
  reader-only and removes the generated override from the systemd command.
- Operator install requires the exact acknowledgement token, exact approved commit,
  mode-private write-gate key, and a valid maximum-24-hour HMAC receipt binding the
  release commit, verified checkpoint revision, role verification hash and security-test
  receipt hash.
- Added `scripts/operator_profile_gate.py` for deterministic issue/verify of that receipt.
- The release-specific generated Compose override enables control-plane operator
  credentials and remote MCP write/operator settings, mounts only the write-gate key,
  and injects an exact explicit scope catalog.
- A separate private provider environment permits exactly one modern
  `KAGGLE_API_TOKEN`; database, YDB, master runtime and OAuth credential crossover is
  rejected. Remote runtime still constructs the repository's single official
  `KaggleProviderAdapter` transport implementation.

### Exchange confidentiality and expiry

- `mcp_exchange` create/version requires the exact v1 manifest, authenticated creator,
  exact ref/version, current TTL, disposable cleanup intent and all declared file
  hashes before the provider adapter is called.
- Confidential exchange files must be non-executable ASCII-armored age ciphertext.
- The canonical manifest is included in the private Dataset version.
- The control ledger stores access metadata/receipts only, never payload or instruction
  bytes. Read is recipient- and TTL-gated; version/delete is creator-gated.

## Validation evidence

All commands ran in `/home/dev/.codex/worktrees/my-data-hub/h1-operational` against the
committed implementation.

- `python -m compileall -q src tests` — passed.
- `ruff check .` — passed.
- configured `mypy` target set — passed (`5` source files).
- `python scripts/create_notebooks.py --check` — passed, zero drift.
- `python scripts/validate_repository.py` — passed (`3237` checks, zero errors/notes).
- `pytest -q` — passed; `776` collected, two opt-in skips, two pre-existing
  `jsonschema.RefResolver` deprecation warnings.
- `MDH_RUN_DISPOSABLE_POSTGRES=1 pytest -q tests/master/test_live_postgres.py` — passed
  against tmpfs `pgvector/pgvector:0.8.6-pg18-bookworm`. It proved direct editor DML
  without receipt is rejected at commit, and a valid one-row transaction produces one
  revision, audit event and semantic outbox operation under the exact restricted role.
- `bash -n deploy/control-plane/install.sh` and `git diff --check` — passed.

## Honest operational boundary

No live deployment or provider mutation was performed by this lane. Enabling the profile
still requires an owner-issued real gate receipt, fresh verified checkpoint evidence,
role/security proof, OAuth client scopes and a real modern Kaggle token. The repository
contract is runnable and fail-closed; those external credentials/evidence remain live
acceptance prerequisites, not claimed proof.
