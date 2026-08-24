# REGION-TALK-MCP results

## Scope

- Lane: `REGION-TALK-MCP`
- Requirement: `R06`
- Base SHA: `1068d103ec261a37dd31e1f6d11265e1e238c168`
- Head SHA (implementation commit): `252380539cfb392944ca2a3e3f1d1df8586eaf68`; the branch tip adds only this finalized receipt

## Delivered

- Added bounded typed MCP tools with no SQL parameter:
  - `region_talk.inventory`
  - `region_talk.articles.list|get|search`
  - `region_talk.posts.list|get|search`
  - `region_talk.queue.list|summary`
  - `region_talk.pipeline.status|run`
- Added `region-talk:read` reader/unified scope and `region-talk:operate` operator scope.
- Kept `PROVIDER_ONLY_TOOLS` unchanged and excluded `pipeline.run` from unified/provider profiles.
- Added closed Pydantic request contracts, opaque bounded cursors, row/byte caps, UUID IDs, fixed supervised mode, immutable source revision, idempotency key, and hard `publication_dispatch=false`.
- Added fixed `RegionTalkReader` broker dispatch; it does not accept generic SQL or raw payload access.
- Added metadata-only status routing through the control reader, so status never resolves or wakes the master.
- Added `RegionTalkPipelineController` seam. `pipeline.run` is advertised only when a controller is injected and its enable flag is true.
- Preserved OAuth `client_id` through status/run paths; OpenCode and ChatGPT remain distinct clients and need independent grants for the new scopes.

## Evidence

Passed:

```text
PYTHONPATH=src:. .../.venv/bin/python -m pytest -q tests/mcp
132 passed

PYTHONPATH=src:. .../.venv/bin/python -m pytest -q \
  tests/oauth_server tests/test_mcp_sdk_v2_contract.py tests/test_oauth_negative_canaries.py
116 passed

PYTHONPATH=src:. .../.venv/bin/python -m ruff check <changed Python files>
All checks passed!

python3 -m compileall -q src tests
git diff --check
passed
```

Full suite:

```text
PYTHONPATH=src:. .../.venv/bin/python -m pytest -q
1 failed, remainder passed (3 skipped)
```

The only failure is the intentionally unmodified shared integration verifier:
`tests/test_remote_mcp_verifier.py::test_verifier_reader_catalog_is_the_exact_15_runtime_read_tools`.
It imports the old exact 15-tool set from `scripts/verify_remote_mcp.py`. That script and scheduled
acceptance catalog are outside this lane's writable scope. Root integration acknowledged it will add
the ten Region Talk read/status tools after cherry-pick rather than weakening the reader-role invariant.

## Integration requirements / risks

1. Root must inject a real `RegionTalkPipelineController`, explicitly enable run advertising, and add
   `region_talk.pipeline.status` to the local control reader. Until then `pipeline.run` is hidden and
   status fails closed if no control reader exists.
2. Root must update `scripts/verify_remote_mcp.py` and scheduled acceptance exact reader catalogs.
3. Unified deploy scope must add `region-talk:read`; operator deploy scope must add
   `region-talk:operate`. OpenCode and ChatGPT must reauthorize independently because they are
   separate OAuth clients; no client ID or grant is shared.
4. The data lane must provide `my_data_hub.workloads.region_talk.reader.RegionTalkReader` with the
   agreed fixed method names. The MCP broker imports it lazily.
5. No provider tool implementation, provider-only allowlist, upload contract, SQL migration, deploy,
   control ledger, pipeline asset, master runtime, or tunnel code was changed in this lane.

## Changed files

- `src/my_data_hub/config.py`
- `src/my_data_hub/mcp/catalog.py`
- `src/my_data_hub/mcp/postgres_broker.py`
- `src/my_data_hub/mcp/region_talk_schemas.py`
- `src/my_data_hub/mcp/server.py`
- `src/my_data_hub/mcp/service.py`
- `tests/mcp/test_region_talk_contracts.py`
- `tests/mcp/test_remote_runtime.py`
- `.codex/lanes/REGION-TALK-MCP/RESULTS.md`
