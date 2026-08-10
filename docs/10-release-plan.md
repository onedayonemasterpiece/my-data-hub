# Release plan after architecture reset

PR-A is architecture/safety only. Each following item is a separate reviewed PR:

1. reusable donor runtime baseline with exact commits/blobs and compatibility tests;
2. orchestrator core with FakeKaggle, deterministic clock, state machine, idempotency,
   leases/fencing/registry and property tests;
3. generic runtime event/callback/heartbeat SDK;
4. real private Kaggle dataset/notebook lifecycle smoke and cleanup receipts;
5. PostgreSQL master Notebook PoC with restore, roles/migrations, DB gate, direct access,
   checkpoint/readback/restore and rotation;
6. MCP/connectors against dynamic ACTIVE master;
7. E5 service;
8. BGE service;
9. durability/recovery/WAL PoC;
10. bounded canary;
11. Region Talk inventory and migration.

No later happy path waives an earlier fencing, checkpoint, scope or recovery gate. DNS,
remote MCP and model services do not begin in PR-A.
