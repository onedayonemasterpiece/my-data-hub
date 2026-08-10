# Observability

Control-plane health reports lifecycle/operation IDs, master state, latest epoch, lease,
service registry freshness, callbacks/output reconciliation, checkpoint HEAD metadata,
provider failures and dangerous gates. `master=ABSENT` is healthy control status, while
data-plane readiness is false.

Master telemetry reports PostgreSQL connections/locks/disk, DB gate, lease watchdog,
schema/canonical revision, queues/outbox, role denials and checkpoint progress. Checkpoint
telemetry includes private Dataset exact version, hash/readback, restore smoke and current/
previous generation age.

Connector telemetry retains accepted/replay/conflict/quarantine, producer spool and
accepted-to-committed latency. MCP telemetry binds calls/denials/receipts to principal,
master instance/epoch/revision, scope and limits. Credentials and full payloads are never
logged. Region Talk and publication metrics stay inactive until their later gate.
