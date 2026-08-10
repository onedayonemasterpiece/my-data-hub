# ADR-0014: Test-first rollout sequence

- Status: Accepted; sequence superseded and restated by ADR-0016
- Date: 2026-08-09

Infrastructure and negative tests still precede Region Talk, but the earlier local-database
sequence was architecture drift. The binding order is: PR-A safety; donor compatibility;
FakeKaggle lifecycle; runtime SDK; real provider smoke; master Notebook PoC; dynamic
MCP/connectors; models; durability/canary; Region Talk last.

Every phase retains explicit receipts and authorization/fencing/recovery negatives. A later
happy path cannot waive an earlier gate.
