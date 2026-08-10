# Executable roadmap after PR-A

1. Pin and test reusable donor runtime blobs/commits.
2. Build orchestrator core with deterministic clock, FakeKaggle, lifecycle state machine,
   operation idempotency, leases, fencing, service registry and property tests.
3. Add the generic runtime callback/heartbeat/output-evidence SDK.
4. Prove a real private Kaggle dataset/notebook lifecycle with cleanup receipts.
5. Prove the PostgreSQL master Notebook: restore, roles/migrations, DB gate, lease
   watchdog, ready registration, direct connection, checkpoint/readback/restore and rotation.
6. Bind MCP and a synthetic connector to ACTIVE master resolution and epoch credentials.
7. Add E5, then BGE, as separately resolved services.
8. Run durability/recovery/WAL PoC and a bounded canary.
9. Only then inventory and migrate Region Talk.

Each step is a separate PR. PR-A implements none of steps 1–9.
