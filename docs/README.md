# Documentation index

## Authority and product

- [`00-source-of-truth.md`](00-source-of-truth.md)
- [`01-project-charter.md`](01-project-charter.md)
- [`vision/my-data-hub-target-vision.md`](vision/my-data-hub-target-vision.md)
- accepted decisions in [`adr/`](adr/)

## Baseline architecture

- [`02-target-architecture.md`](02-target-architecture.md)
- [`03-data-model.md`](03-data-model.md)
- [`04-orchestrator.md`](04-orchestrator.md)
- [`05-mcp.md`](05-mcp.md)
- [`06-notebooks.md`](06-notebooks.md)
- [`07-joplin-integration.md`](07-joplin-integration.md)
- [`08-security.md`](08-security.md)
- [`09-observability.md`](09-observability.md)

Supporting architecture documents:

- [`architecture/system-context.md`](architecture/system-context.md)
- [`architecture/component-model.md`](architecture/component-model.md)
- [`architecture/data-ownership.md`](architecture/data-ownership.md)
- [`architecture/orchestration.md`](architecture/orchestration.md)
- [`architecture/notebook-contract.md`](architecture/notebook-contract.md)
- [`architecture/mcp.md`](architecture/mcp.md)
- [`architecture/region-talk-first-workload.md`](architecture/region-talk-first-workload.md)
- [`architecture/joplin-integration.md`](architecture/joplin-integration.md)
- [`architecture/security.md`](architecture/security.md)

## Infrastructure-first supplement

- [`15-infrastructure-first-plan.md`](15-infrastructure-first-plan.md) — what to do
  first on the deployed devstand.
- [`16-data-connectors.md`](16-data-connectors.md) — push/pull/artifact/direct-landing
  connector contract, receipts and the events-bot daily-statistics example.
- [`17-kaggle-control-plane.md`](17-kaggle-control-plane.md) — protected,
  MCP-managed, exchange and external read-only resources.
- [`18-mcp-operator-and-database-access.md`](18-mcp-operator-and-database-access.md) —
  broad bounded reads, preview/apply DML, backup and migration gates.
- [`19-test-first-rollout.md`](19-test-first-rollout.md) — PR, post-deploy, nightly,
  provider and restore workflows.
- [`20-remote-mcp-endpoint.md`](20-remote-mcp-endpoint.md) —
  `mcp-datahub.kenigevents.ru`, Yandex edge, OAuth and ChatGPT acceptance.
- [`21-infrastructure-addendum-delivery.md`](21-infrastructure-addendum-delivery.md) — scope, accepted decisions, validation and implementation boundary.

Associated decisions:

- [`adr/0009-canonical-postgres-availability.md`](adr/0009-canonical-postgres-availability.md)
- [`adr/0010-data-connector-ingress-contract.md`](adr/0010-data-connector-ingress-contract.md)
- [`adr/0011-kaggle-resource-control-classes.md`](adr/0011-kaggle-resource-control-classes.md)
- [`adr/0012-mcp-database-operator-profiles.md`](adr/0012-mcp-database-operator-profiles.md)
- [`adr/0013-remote-mcp-endpoint.md`](adr/0013-remote-mcp-endpoint.md)
- [`adr/0014-test-first-infrastructure-rollout.md`](adr/0014-test-first-infrastructure-rollout.md)

Machine-readable design contracts:

- [`../schemas/data-connector-envelope.v1.schema.json`](../schemas/data-connector-envelope.v1.schema.json)
- [`../schemas/kaggle-exchange-manifest.v1.schema.json`](../schemas/kaggle-exchange-manifest.v1.schema.json)

## Region Talk migration

The complete migration package is under [`migrations/region-talk/`](migrations/region-talk/).
It separates read-only export, raw preservation, normalized import, reconciliation,
shadow operation, cutover and YDB retirement.

Region Talk remains the first migration workload, but the infrastructure-first gates
must pass before heavy import/cutover work.

## Operations

- [`operations/local-development.md`](operations/local-development.md)
- [`operations/devstand-deployment.md`](operations/devstand-deployment.md)
- [`operations/first-deploy-template.md`](operations/first-deploy-template.md) — observed-facts receipt template for the first devstand verification.
- [`operations/backup-and-recovery.md`](operations/backup-and-recovery.md)
- [`operations/observability.md`](operations/observability.md)
- [`operations/secrets.md`](operations/secrets.md)

## Release, delivery and handoff

- [`10-release-plan.md`](10-release-plan.md)
- [`11-deployment.md`](11-deployment.md)
- [`12-code-agent-handoff.md`](12-code-agent-handoff.md)
- [`14-bootstrap-delivery.md`](14-bootstrap-delivery.md)
- [`handoff/code-agent-completion.md`](handoff/code-agent-completion.md)
- [`roadmap.md`](roadmap.md)

## External references

- [`13-external-references.md`](13-external-references.md)
- repository-level [`../BOOTSTRAP_VALIDATION.md`](../BOOTSTRAP_VALIDATION.md)
