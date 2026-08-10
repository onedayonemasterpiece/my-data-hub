# Kaggle control plane

Status: `CORE ARCHITECTURE / REAL PROVIDER IMPLEMENTATION DEFERRED`

Kaggle has two distinct roles:

1. the master Notebook hosts the single ACTIVE writable PostgreSQL-primary;
2. private Datasets hold current/previous verified checkpoints and controlled artifacts.

The devstand adapter owns lifecycle operations, service registry, leases/fencing, callbacks,
checkpoint metadata and recovery coordination. Internal clients resolve a service then use
the direct data plane. Provider status alone is insufficient; exact output/callback evidence
must reconcile completion.

Resource control classes remain: `orchestrator_protected`, `mcp_managed`, `mcp_exchange`,
and `external_read_only`. Names never grant authorization. Master and checkpoint resources
are orchestrator-protected. Public creation is absent.

PR-A makes no provider call. Next are donor compatibility and FakeKaggle; real lifecycle and
master Notebook are later PRs.

## Preserved detailed contract — bound by ADR-0016

The detailed material below is retained where topology-neutral. Any reference to a database, role, committer, backup or connector application is executed inside/against the latest ACTIVE Kaggle master; devstand execution claims are superseded.

Status: `R1 POLICY/REGISTRY CONTRACT IMPLEMENTED / PROVIDER ADAPTER BLOCKED`
Date: 2026-08-09
Related decision: ADR-0011
Exchange contract: [`../schemas/kaggle-exchange-manifest.v1.schema.json`](../schemas/kaggle-exchange-manifest.v1.schema.json)

Implemented in R1: provider-neutral control-class models, bounded pagination,
unknown-resource classification, immutable protected-class database enforcement,
fingerprint/lease mutation policy, private exchange validation, private lifecycle
receipt contracts, protected-resource negative tests, and a minimal remote status tool.
No concrete Kaggle create/readback/delete adapter, account inventory, resource ID, or
cleanup receipt is claimed; the provider workflow fails closed until those primitives
and dedicated credentials exist.

## 1. Role of Kaggle

Kaggle is the runtime/provider plane for:

- the single fenced PostgreSQL master Notebook;
- intermittent CPU/GPU worker and model-service notebooks;
- private current/previous verified checkpoint Datasets;
- immutable notebook outputs/evidence and private exchange packages.

The devstand control ledger stores provider registry, ownership/control class, desired
operation, leases, receipts and accepted runtime identity. Canonical business data remains
in the ACTIVE master PostgreSQL; the control ledger is not a second business database.

## 2. Required account inventory

The MCP adapter may list every notebook/kernel visible to the configured Kaggle account
and every private dataset in the account. Discovery alone grants no mutation rights.

For every provider resource, the platform resolves:

```text
provider_ref
provider_kind
owner/account
origin
control_class
privacy
current version/fingerprint
status
last observed time
registry state
```

A provider resource not present in the local registry is created as
`external_read_only`. It is not inferred to be MCP-managed from its name.

## 3. Control classes

### 3.1 `orchestrator_protected`

Used for Region Talk and other pipeline resources created or adopted by the
orchestrator.

Remote MCP may expose only:

- stable provider reference;
- workload/pipeline identity;
- coarse state: queued/running/completed/failed/unknown;
- started/finished/last-observed timestamps;
- output acceptance state and health flags;
- protected control class.

Remote MCP must not expose or permit:

- notebook source or generated input manifest;
- output download;
- attached dataset contents;
- provider credentials;
- push/update/rerun/cancel/delete;
- dataset create/version/delete/download;
- control-class change.

The orchestrator is the only normal controller. A local break-glass operator may act
outside MCP under a documented incident procedure.

### 3.2 `mcp_managed`

Used for notebooks and private datasets explicitly created through MCP or adopted by an
operator.

Under the correct scopes, MCP may provide the provider-supported lifecycle:

- create/push/update notebook;
- launch/run and observe status;
- pull source/metadata;
- retrieve output;
- create private dataset;
- create a new private dataset version;
- download files/metadata;
- delete after confirmation and retention checks.

Every mutation requires a local lease, exact provider reference, expected provider
fingerprint/version, idempotency key and audit receipt.

### 3.3 `mcp_exchange`

A specialized private dataset for passing files, documents or code among ChatGPT,
notebooks and code agents.

It is always:

- private;
- manifest-driven;
- hash-verified;
- TTL-bound;
- scoped to declared recipients/purpose;
- free of secrets and production database dumps unless separately encrypted and
  explicitly authorized;
- non-canonical.

### 3.4 `external_read_only`

A resource discovered in the account but not yet adopted.

MCP may read bounded public/provider metadata and status. It cannot read private file
contents or mutate the resource. Adoption is a separate operator command with expected
fingerprint and an explicit target control class.

## 4. Capability matrix

| Capability | orchestrator protected | MCP managed | MCP exchange | external read-only |
|---|---:|---:|---:|---:|
| List existence | yes | yes | yes | yes |
| Coarse execution/status | yes | yes | n/a | yes |
| Read notebook source | no | yes | n/a | no |
| Read output | no | yes | n/a | no |
| Push/update/run | no | yes | n/a | no |
| Cancel/stop | no | only if provider adapter proves support | n/a | no |
| Delete notebook | no | yes, guarded | n/a | no |
| List dataset metadata | minimal | yes | yes | minimal |
| Download dataset files | no | yes | yes, recipient-scoped | no |
| Create/version dataset | no | yes, private only | yes, private only | no |
| Delete dataset | no | yes, guarded | yes after TTL/receipt | no |
| Change control class | no | explicit operator adoption only | no | explicit operator adoption only |

The official provider client is the compatibility boundary. A tool is not advertised
just because a provider web UI appears to support it. In particular, cancellation is
absent until an integration test proves a supported API/CLI operation and its ambiguous
outcome handling.

## 5. Registry model

A future append-only migration should add provider-neutral integration tables, with a
Kaggle projection. Provisional fields:

### `integration.provider_resource`

- internal resource UUID;
- provider (`kaggle`);
- kind (`notebook`, `dataset`);
- immutable provider owner/slug reference;
- origin (`orchestrator`, `mcp`, `external`, `migration`);
- control class;
- project/pipeline association;
- privacy attestation;
- expected/current provider fingerprint;
- discovered, created and last-observed timestamps;
- lifecycle state and deletion tombstone;
- policy revision.

### `integration.provider_operation`

- operation UUID and idempotency key;
- principal and scope;
- resource reference and expected fingerprint;
- requested action and bounded arguments;
- lease/fencing token;
- provider request/result fingerprints;
- status, retry class and unknown-outcome flag;
- receipt/artifact references;
- timestamps and correlation ID.

### `integration.provider_event`

Append-only observed provider status and audit evidence. Provider polling updates a
current projection but never erases event history.

## 6. Notebook lifecycle through MCP

A safe MCP-managed notebook flow:

```text
create draft specification
→ validate metadata and private inputs
→ reserve provider slug in registry
→ acquire control lease
→ push exact source/metadata
→ record provider fingerprint
→ run
→ poll bounded status
→ retrieve output into private artifact intake
→ verify manifest/hash/size
→ release lease and write receipt
```

A rerun is a new operation, not an untracked click. The local registry records which
source fingerprint and dataset versions were used.

If the provider response is ambiguous, the operation becomes `unknown_outcome`; the
adapter first reconciles current provider state before retrying a destructive/create
request.

## 7. Dataset lifecycle through MCP

All `my-data-hub` dataset creation is private. The MCP surface does not accept a
`public=true` parameter and never forwards a public-creation flag.

Safe lifecycle:

```text
prepare local manifest and files
→ scan names/types/sizes/secrets
→ calculate file and package hashes
→ create private dataset or version
→ read back provider metadata/files
→ verify privacy and hashes
→ record version receipt
```

Do not delete old versions automatically by default. Retention/deletion requires an
explicit policy, expected current version and evidence that no active notebook,
receipt, backup or exchange recipient depends on the version.

## 8. Exchange packages

### 8.1 Use cases

- ChatGPT creates a code/document package for a code agent;
- a code agent places generated output for a notebook;
- a notebook publishes non-canonical evidence for review;
- an operator passes a larger binary artifact where the MCP response limit is
  unsuitable.

### 8.2 Package contract

Each package follows
[`kaggle-exchange-manifest.v1.schema.json`](../schemas/kaggle-exchange-manifest.v1.schema.json)
and declares:

- package UUID and manifest version;
- private Kaggle dataset reference and version;
- creator principal and intended recipients;
- purpose and target project/workload;
- sensitivity classification;
- creation and expiry timestamps;
- every file path, media type, size and SHA-256;
- optional execution/import instructions;
- parent package or source commit where applicable.

### 8.3 Trust rules

- No package is trusted because it came from the same Kaggle account.
- The recipient verifies the manifest and every file hash.
- Paths are relative, normalized and traversal-free.
- Archives are scanned before extraction.
- Secrets, cookies, tokens and plaintext production dumps are prohibited.
- Sensitive allowed data is encrypted client-side before upload; the key travels through
  a different secret channel.
- Exchange content cannot directly update canonical tables; it enters through artifact
  intake, a connector, notebook result validation or a reviewed code change.
- Expired packages are tombstoned and deleted only after receipt/dependency checks.

A sample manifest is at
[`../examples/contracts/kaggle-exchange-manifest.v1.example.json`](../examples/contracts/kaggle-exchange-manifest.v1.example.json).

`manifest_sha256` is calculated over RFC 8785 canonical UTF-8 JSON with the
`manifest_sha256` field omitted. Dataset readback must reproduce the same manifest hash
and every declared file hash before the package is acknowledged.

## 9. Backups on Kaggle

Backup datasets use `orchestrator_protected`, not `mcp_exchange` or `mcp_managed`.
Remote MCP exposes only:

- backup/checkpoint existence;
- last successful timestamp;
- generation count;
- readback/hash verification state;
- restore-drill freshness;
- health/incident flags.

It does not expose dump files or permit version/delete/download. The master checkpoint
agent uses a separate credential path. Current and previous verified private Dataset
generations are mandatory; a less frequent portable encrypted logical backup may also be
kept in another approved private target for account-wide loss.

## 10. Proposed MCP tools

### Read inventory

- `kaggle.notebook.list`
- `kaggle.notebook.get_status`
- `kaggle.dataset.list`
- `kaggle.dataset.get_status`
- `kaggle.operation.get`

Results include `control_class` and omit fields forbidden by that class.

### MCP-managed notebook operations

- `kaggle.notebook.create_draft`
- `kaggle.notebook.push_and_run`
- `kaggle.notebook.pull`
- `kaggle.notebook.get_output`
- `kaggle.notebook.delete`

### MCP-managed/private dataset operations

- `kaggle.dataset.create_private`
- `kaggle.dataset.create_version`
- `kaggle.dataset.download`
- `kaggle.dataset.delete`

### Exchange

- `kaggle.exchange.create`
- `kaggle.exchange.add_file`
- `kaggle.exchange.finalize`
- `kaggle.exchange.get_manifest`
- `kaggle.exchange.acknowledge`
- `kaggle.exchange.expire`

### Registry/adoption

- `kaggle.resource.adopt`
- `kaggle.resource.release`

Adoption/release are operator-only, revision-bound and never permitted for
`orchestrator_protected` through the remote profile.

## 11. Authentication and provider credentials

Kaggle credentials stay server-side and are never returned by MCP. Separate provider
identities are preferred for:

- orchestrator-owned production workers/backups;
- MCP-managed sandbox resources;
- provider canary tests.

Where separate identities are initially impractical, the local registry and scopes
still enforce control classes, but this is treated as weaker isolation and recorded as
a risk until provider-account separation is implemented.

## 12. Mandatory authorization and integration tests

1. inventory lists all visible resources and assigns unknown resources read-only;
2. protected notebook status is visible but source/output/mutation is denied;
3. protected dataset minimal status is visible but download/version/delete is denied;
4. an MCP-managed private dataset can be created, read back, versioned and cleaned up;
5. public dataset creation is impossible through schema/tool discovery;
6. an MCP-managed notebook can be pushed, run, observed and its output verified;
7. one principal cannot mutate another principal's active resource lease;
8. stale provider fingerprint blocks mutation;
9. ambiguous provider outcome is reconciled before retry;
10. exchange file tamper, traversal, secret finding and expired package are rejected;
11. backup resources remain protected even for a principal with normal Kaggle write
    scopes;
12. orchestrator-created resources cannot be reclassified through rename or rediscovery.
