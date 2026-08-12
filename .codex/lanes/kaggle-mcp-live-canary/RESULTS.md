# Kaggle MCP live batch canary

- Integration source: `61161f9de8c9a25d7f7a7e9e44636dd7be424b0e`
- Execution window: `2026-08-12T06:26:50Z`–`2026-08-12T06:27:28Z`
- Evidence class: live provider mutation with cleanup; this is not an operational-MVP completion claim.
- Central authority: one repository `KaggleProviderAdapter` authenticated from the owner-mode-0600 control-plane provider environment; no Kaggle credential was passed to an MCP/runtime client.
- Disposable private Dataset: `zigomaro/mdh-mcp-batch-20260812062650`
- Protected receipt: `/home/dev/.local/state/my-data-hub-control-plane/canaries/mcp-batch-live-20260812062650/receipt.json`, mode `0600`, SHA-256 `4d2671faf9cefbf411319222ed11f186085c5656df9b8e18b4c596466b00d27d`.

Observed exact lifecycle:

1. Created version 1 with three mixed text/binary files.
2. Listed the exact three-file manifest and reconciled its package hash.
3. Downloaded and hash-verified two binary files (65,536 and 38,912 bytes).
4. Created version 2 with a 49,152-byte replacement binary file.
5. Listed and chunk-downloaded version 2, verifying its exact whole-file hash.
6. Deleted the task-owned Dataset through the durable claim path.
7. Reconciled provider inventory absence.

Terminal result: `PASS`. No provider resource from this canary remains. No raw test bytes or credentials are recorded in the repository.
