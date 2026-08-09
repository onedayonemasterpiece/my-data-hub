# ADR-0009: Canonical PostgreSQL is supervised and Kaggle is not a database failover

- Status: Accepted
- Date: 2026-08-09

## Context

The first deployment runs on one devstand that is also the initial production host.
Connectors, MCP clients and notebook workers need a predictable canonical endpoint.
An earlier idea was to let the orchestrator start a Kaggle notebook containing the
"master database" when PostgreSQL is stopped.

That creates a circular dependency: if the host, PostgreSQL and orchestrator are down,
the orchestrator cannot make the decision or start the recovery job. A Kaggle Dataset
or notebook also does not provide the transactional, continuously available PostgreSQL
service required by the canonical committer.

## Decision

The canonical PostgreSQL instance remains on the devstand and is operated as a
supervised, normally always-on service with restart-on-failure and restart-on-boot.

Kaggle may contain only:

- compute workers;
- immutable result bundles;
- encrypted logical backups and checkpoints;
- controlled exchange packages.

Kaggle never becomes a writable canonical database and never advances the canonical
revision.

When the canonical endpoint is unavailable:

1. a push connector writes the batch to its own durable local spool/outbox;
2. it retries the same idempotency key with bounded exponential backoff and jitter;
3. pull connectors remain due in PostgreSQL and resume after service recovery;
4. no producer silently drops, rewrites or redirects canonical writes to Kaggle.

A future external availability controller may start a stopped VM through Yandex Cloud
with a narrowly scoped service account. That controller is outside the orchestrator and
is not required for the first release.

## Consequences

- PostgreSQL availability, restart and restore are infrastructure responsibilities.
- Connector correctness does not depend on a separate "is the master database awake?"
  preflight request; the idempotent intake request is the availability probe.
- Backup copies improve recovery but do not grant permission for unsafe writes.
- A stopped devstand means delayed ingestion, not an alternate canonical head.
