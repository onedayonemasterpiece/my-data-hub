# Stage 1A results

Validated in `/tmp/my-data-hub-dataset-loop-20260901`:

```text
UV_CACHE_DIR=/tmp/uv-cache /usr/local/bin/uv run --extra dev pytest integrations/devcoveer/openai-codex-mcp/tests/test_dataset_loop_stage1a.py -q
# 7 passed
UV_CACHE_DIR=/tmp/uv-cache /usr/local/bin/uv run --extra dev python -m py_compile integrations/devcoveer/openai-codex-mcp/dataset_loop/*.py integrations/devcoveer/openai-codex-mcp/tests/test_dataset_loop_stage1a.py
UV_CACHE_DIR=/tmp/uv-cache /usr/local/bin/uv run --extra dev ruff check integrations/devcoveer/openai-codex-mcp/dataset_loop integrations/devcoveer/openai-codex-mcp/tests/test_dataset_loop_stage1a.py
# All checks passed!
git diff --check
# passed
git diff -- integrations/devcoveer/openai-codex-mcp | grep -En -- '-----BEGIN (RSA|OPENSSH|EC) PRIVATE KEY-----|AKIA[0-9A-Z]{16}'
# no matches
```

Stage 1A includes only baseline provenance, immutable remote-dataset resolution,
frozen manifests, catalog/probe gating, durable controls, and initial private artifacts.

## Explicitly not done (later stages)

- Runner and OpenCode session execution, terminal polling, cancellation and receipts.
- Isolated run workspaces, `research_data.py`, Git mutation/push/readback validation.
- Council scheduling/execution and advisory schema-gap processing.
- NVIDIA audit job execution/retries/concurrency suppression.
- Bridge schemas/dispatch polish, installer, and live registration/install/readback.
