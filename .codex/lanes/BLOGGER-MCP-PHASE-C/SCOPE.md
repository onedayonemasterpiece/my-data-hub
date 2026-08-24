# Blogger MCP Phase C scope and conflict audit

## Baseline

- Branch: `agent/mcp-r03/blogger-mcp-phase-c`
- Worktree: `/home/dev/.codex/worktrees/my-data-hub/blogger-mcp-phase-c`
- Exact base: `7530f24`
- Baseline status: clean

## Requirement matrix

| ID | Requirement | Primary files | Dependency / conflict control |
|---|---|---|---|
| C01 | Public closed `submit_discovery_batch` ingress with structural and official semantic validation | `mcp/catalog.py`, `mcp/server.py`, `mcp/service.py`, `mcp/postgres_broker.py`, blogger contract tests | Reuse Phase-B connector envelope and ACTIVE-master landing; no payload in SQLite |
| C02 | Typed blogger import preview/apply/status plus apply reconciliation | `control_plane/adapters.py`, `mcp/service.py`, `mcp/postgres_broker.py`, lifecycle tests | Reuse migration 0020 and control migration 028; no caller SQL |
| C03 | Cold-master continuation without terminal business failure | `mcp/service.py`, `control_plane/adapters.py`, tests | Persist only operation identity/hashes before `ensure_master`; resume same operation |
| C04 | Bounded sanitized blogger reads | `mcp/server.py`, `mcp/postgres_broker.py`, tests | Query fixed `hub.bloggers_v1` only; project scope and keyset cursor |
| C05 | Reader/unified/operator profile separation | `mcp/catalog.py`, `mcp/server.py`, `mcp/runtime.py`, `config.py`, deploy/OAuth tests | Provider-only catalog stays byte-for-byte equivalent; unified excludes writes and generic SQL |
| C06 | Deployment/OAuth/docs and regression gates | `deploy/control-plane/install.sh`, focused tests, docs, `RESULTS.md` | No live deployment, provider mutation, root mutation, or non-disposable database mutation |

## Writable scope

- Shared MCP composition: catalog, server, service, PostgreSQL broker, runtime and contracts if a narrow protocol seam is required.
- Control-plane adapters only for Phase-B lifecycle projection/gating; no business-row storage.
- Configuration and installer scope strings/profile wiring.
- New or directly related MCP/control/deployment tests and derived documentation.
- This lane's `.codex/lanes/BLOGGER-MCP-PHASE-C/*` evidence.

## Explicit non-goals / forbidden mutation

- No edits to provider schemas, provider gateway semantics, upload storage or Kaggle provider modules.
- No edit to Phase-B migrations 0020/028 or role grants unless a test proves the integrated contract itself is defective.
- No generic SQL in reader/unified or new blogger tools.
- No canonical blogger rows, artifact contents, PostgreSQL URLs, credentials or decrypted data in the devstand ledger.
- No live Kaggle/provider/root/deployment action. PostgreSQL writes are limited to disposable tests.

## Serial integration order

1. Freeze catalogs/profile scopes and closed public models.
2. Wire fixed reader/intake/import broker dispatch.
3. Wire lifecycle gate, reconciliation and cold continuation.
4. Wire runtime/deploy/OAuth profile scopes without changing provider-only behavior.
5. Run focused tests, repository/schema gates and full test suite; document observed blockers only.

## Known integration risks

- Provider-only and unified profiles share `server.py`, `runtime.py`, `config.py` and the installer. Changes must preserve the exact provider-only set and live upload schemas.
- Phase-B artifact handoff requires a separately authenticated materializer after provider verification. Public artifact claim admission can be wired here, but fabricating a provider-to-materializer success path is forbidden.
- Master activation already issues `reader`, `connector` and `canonical_committer` short-lived credentials; Phase C must consume those existing roles rather than invent a second credential mechanism.
