# ADR-0011: Kaggle resources have registry-enforced control classes

- Status: Accepted
- Date: 2026-08-09

## Context

The remote MCP should make Kaggle useful as an alternate agent and file-transfer lane,
while the orchestrator must retain exclusive control over jobs and datasets that are
part of canonical pipeline execution. Provider ownership alone does not express this
boundary.

## Decision

Every discovered Kaggle notebook/kernel and private dataset is represented in a local
registry with an immutable provider reference, origin, control class and current
provider fingerprint.

Control classes are:

- `orchestrator_protected` — created/adopted by the orchestrator; MCP exposes only
  bounded existence and execution/freshness status;
- `mcp_managed` — created or explicitly adopted through MCP; MCP may perform the
  provider-supported lifecycle under scopes and leases;
- `mcp_exchange` — a private, TTL-bound dataset used for hashed file/code/document
  transfer; it is never canonical state;
- `external_read_only` — discovered in the account but not adopted; metadata/status
  only and no mutation.

Resource names and prefixes are audit hints, not authorization. Authorization comes
from the PostgreSQL registry and current principal. A resource cannot change control
class implicitly because it was renamed or rediscovered.

All datasets created by `my-data-hub` are private. Public dataset creation is absent
from the MCP tool surface. Orchestrator backup/checkpoint datasets are
`orchestrator_protected` and cannot be downloaded, versioned or deleted through the
remote MCP profile.

## Consequences

- An agent can inventory all account notebooks while learning only minimal status for
  protected workloads.
- MCP-created notebooks and datasets remain fully manageable without allowing an agent
  to interfere with orchestrated runs.
- Exchange datasets provide a controlled bridge between ChatGPT, notebooks and code
  agents, with manifests, hashes, recipients and retention.
- Provider operations not proven by an integration test, including cancellation where
  the official client has no supported primitive, are not advertised.
