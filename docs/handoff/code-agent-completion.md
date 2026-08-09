# Code-agent completion task

Repository: `onedayonemasterpiece/my-data-hub`
Target host: existing devstand, also the initial production runtime.

## Goal

Verify and harden the deployed platform before Region Talk migration. PostgreSQL is the
supervised canonical head; Kaggle is compute/private artifacts, not a database failover.
Add connectors, remote MCP, Kaggle resource control and restricted database operator
access through the accepted contracts.

Primary reasoning: **high**. Final authorization/recovery review: **xhigh**.

## Required work

1. Read ADR-0009–ADR-0014 and `docs/15-infrastructure-first-plan.md`.
2. Capture actual commit/images/services/ports/versions in
   `docs/operations/first-deploy.md`, created from
   [`../operations/first-deploy-template.md`](../operations/first-deploy-template.md); keep
   scheduler/publication/remote writes off and Region Talk paused.
3. Split PostgreSQL roles; prove clean/upgrade migrations and negative grants.
4. Prove process/reboot auto-start, encrypted local/off-host backup, readback and isolated
   restore.
5. Implement PR, post-deploy, nightly, Kaggle canary and restore workflows with receipts.
6. Publish read-only OAuth MCP at `https://mcp-datahub.kenigevents.ru/mcp` through Yandex
   DNS/TLS/private upstream and prove negative auth/Host/Origin cases.
7. Implement connector registry/intake/receipt/quarantine, synthetic outage/replay flow and
   `events-bot.daily-statistics.v1` producer.
8. Implement Kaggle inventory/control classes; prove protected resources cannot be
   mutated and disposable MCP-managed private notebook/dataset lifecycle works.
9. Implement broad bounded DB reader and preview/apply editor under restricted roles,
   first in a disposable schema with backup/audit/impact gates.
10. Implement typed migration-operator tools.
11. Only then perform Region Talk read-only inventory/export, full accounting, mapping,
    shadow/private canary, backup/rollback and controlled cutover.
12. Keep production publication disabled until separate owner approval.

## Acceptance

Return commit/PR, deployment receipt, role/grant probes, workflow run IDs, backup/restore,
remote MCP, connector, Kaggle protected/control and operator canary evidence. Do not mark
Region Talk complete on green CI alone; prove data accounting, behavior, idempotency,
exact revision, private canary and rollback.
