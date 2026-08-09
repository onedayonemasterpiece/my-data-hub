# Documentation index

## Product and architecture

- `vision/my-data-hub-target-vision.md`
- `architecture/system-context.md`
- `architecture/component-model.md`
- `architecture/data-ownership.md`
- `architecture/orchestration.md`
- `architecture/notebook-contract.md`
- `architecture/mcp.md`
- `architecture/region-talk-first-workload.md`
- `architecture/joplin-integration.md`
- `architecture/security.md`

## Decisions

Accepted decisions live in `adr/`. An ADR changes status only in a dedicated commit
that also updates affected contracts.

## Region Talk migration

The complete migration package is under `migrations/region-talk/`. It deliberately
separates read-only export, raw preservation, normalized import, reconciliation,
shadow operation, cutover and YDB retirement.

## Operations

- `operations/local-development.md`
- `operations/devstand-deployment.md`
- `operations/backup-and-recovery.md`
- `operations/observability.md`
- `operations/secrets.md`

## Delivery and handoff

- `14-bootstrap-delivery.md` records what this bootstrap actually implements and proves.
- `12-code-agent-handoff.md` is the executable environment/cutover sequence.
- `handoff/code-agent-completion.md` is the compact completion contract.

All three assume this architecture and do not ask the code agent to redesign the system.

## External implementation references

- `13-external-references.md`
- repository-level `../BOOTSTRAP_VALIDATION.md`
