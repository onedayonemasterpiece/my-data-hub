# ADR-0011: Kaggle resources have registry-enforced control classes

- Status: Accepted
- Date: 2026-08-09
- Amended: 2026-08-25
- Research-workflow refinement: [`../23-kaggle-research-workflow.md`](../23-kaggle-research-workflow.md)

## Context

The remote MCP should make Kaggle useful as an alternate agent, research runtime and
file-transfer lane, while the orchestrator must retain exclusive control over jobs and
Datasets that are part of canonical pipeline execution. Provider ownership alone does
not express that boundary.

The original decision treated `external_read_only` as metadata/status-only. Durable
research requires a more precise separation: the authority to mutate or delete a
provider resource is not the same as the right to inspect or use data that Kaggle already
allows the authenticated owner to access.

## Decision

Every Kaggle resource discovered in the configured owner inventory, adopted by the
platform, or selected as an exact research input is represented by a bounded devstand
control-plane projection with immutable provider identity, origin, control class and
observed provider version/fingerprint. A public search result need not be persisted until
it is selected or otherwise material to a workflow.

Control classes determine mutation and deletion authority:

- `orchestrator_protected` — created or adopted by the orchestrator; MCP exposes only
  bounded existence and execution/freshness status;
- `mcp_managed` — created or explicitly adopted through MCP; MCP may perform the
  provider-supported lifecycle under scopes, ownership checks and leases;
- `mcp_exchange` — a private, TTL-bound Dataset used for hashed file/code/document
  transfer; it is never canonical state;
- `external_read_only` — public, owner-visible or explicitly granted provider resource
  that has not been adopted; mutation and deletion are forbidden.

Data access is evaluated separately for:

- metadata visibility;
- file-manifest visibility;
- content read;
- use as a Notebook input.

Therefore `external_read_only` does not itself forbid legitimate provider-authorized
reading. A public Dataset, an owner-private Dataset, or a Dataset covered by an explicit
provider/access grant may be inspected, read and attached to a research Notebook when the
provider, license/terms and research access policy permit it. Adoption is required to
mutate the resource, not merely to consume it read-only.

`orchestrator_protected` remains an explicit final deny for source, Dataset contents,
outputs and use as a user research input. Its status-only boundary is not relaxed by
provider ownership or a broad MCP scope.

Resource names and prefixes are audit hints, not authorization. A resource cannot change
control class implicitly because it was renamed, rediscovered or used by a Notebook.
Authorization combines the authenticated subject, MCP scope, research ownership or
grant, provider rights, control class and the separate data-access decision.

Provider-only research orchestration is lightweight operational control-plane work. It
must operate with `master_state=ABSENT` and does not require an ACTIVE Kaggle PostgreSQL
master, `master.ensure`, a master session or `data.query`.

All Datasets created by `my-data-hub` remain private. Public Dataset creation is absent
from the MCP tool surface. Orchestrator backup/checkpoint Datasets are
`orchestrator_protected` and cannot be downloaded, attached, versioned or deleted through
the remote MCP profile. Competition rules or license terms that require affirmative
acceptance are never accepted automatically.

## Consequences

- An agent can inventory account resources while learning only minimal status for
  protected workloads.
- Public and owner-private Datasets can serve as provider-native research inputs without
  copying them merely to manufacture an internal claim.
- MCP-created Notebooks and Datasets remain fully manageable without allowing an agent
  to interfere with orchestrated runs.
- Exchange Datasets provide a controlled bridge between ChatGPT, Notebooks and code
  agents, with manifests, hashes, recipients and retention.
- Research identities, exact input pins, source revisions, runs and output provenance are
  operational metadata in the existing SQLite-WAL `ControlLedger`, not canonical
  business data.
- Provider operations not proven by the pinned official client and live acceptance,
  including cancellation where no supported primitive is established, are not
  advertised.
