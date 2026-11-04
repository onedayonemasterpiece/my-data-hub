# kaggle-runtime: provenance and initial executable scope

Date: 2026-11-04. This record covers local source under `implementation/kaggle-runtime`; it is not evidence of deployment or a successful Kaggle run.

## Product decision and scope

The owner requires one maintained implementation shared by My Data Hub, RKB, EventsBot and DAEP. When the eligible free-model pool is exhausted, the executor must be stopped and a fresh executor launched. A fresh executor is not evidence of restored provider quota. Credential inheritance is empty by default; only explicitly required scoped credentials may be supplied. Database authority and durability follow RKB; backups are outside this implementation slice.

The owner of a monitoring or coding job notices interrupted work when the executor exhausts its models. The concrete decisions are stop-before-replace and continuously current OpenCode. The concrete version-selection defect is that official `releases/latest` returned v1.19.5 while the repository also published v2 releases. The shared policy boundary addresses these decisions without adding a service, database, scheduler, retry loop or product schema.

Instructions consulted: [devserver-simplicity at f588486fbc66354b3567fa9b0d539f4c9e0b782a](https://github.com/onedayonemasterpiece/codex-devcoveer-mcp/blob/f588486fbc66354b3567fa9b0d539f4c9e0b782a/skills/devserver-simplicity/SKILL.md).

## Exact source provenance

| Implemented part | Source and exact scope |
|---|---|
| `codec._unique_object` | Adapted from the identically named function at RKB `0b7590628dcb29a270727f3920607084dde30720`, `src/regional_knowledge/kaggle_protocol.py`, lines 441–448. |
| `codec._reject_constant` | Adapted from the identically named RKB function at the same SHA, lines 450–451. |
| `codec.decode_object` | Extracted generic byte/UTF-8/object checks and strict JSON parsing from RKB `decode_envelope`, starting line 454. Domain dispatch, Pydantic models, routes and `PROTOCOL_VERSION=2` are deliberately absent. Caller supplies the byte limit and keeps its own envelope validation. |
| `codec.encode_object` | New generic encoding boundary inspired by RKB `encode_envelope` at lines 495–499, without Pydantic or domain round-trip validation. It rejects key coercion and non-finite output. |
| `codec._finite_float` | Small extension: reject exponent overflow such as `1e999`; `json.loads(parse_constant=...)` alone does not catch this path to infinity. |
| `releases.py` | New pure policy for the owner's freshness requirement and observed v1/v2 release selection problem. It is not copied from a previously working updater. |
| `rotation.py` | New pure transition policy implementing the owner's stop→fresh decision, preserving unknown effect outcomes and exact attempt generation. It is not a provider implementation. |
| `providers.py` | Structural Python `Protocol` for future adapters and an explicit credential selector; no network, shell, Kaggle or model operations. |

RKB source: [kaggle_protocol.py at the exact commit](https://github.com/onedayonemasterpiece/regional-knowledge-base/blob/0b7590628dcb29a270727f3920607084dde30720/src/regional_knowledge/kaggle_protocol.py#L441). Git blob SHA: `2a86ce7f8411925bcf1ee6206cfc793fe642fe23`. The whole source file was read before extracting the generic portion.

Planning evidence: [MDH architecture at 77a62c53c881120164f525a226ec296dcb3c77b3](https://github.com/onedayonemasterpiece/my-data-hub/blob/77a62c53c881120164f525a226ec296dcb3c77b3/docs/architecture/regional-monitoring-shared-kaggle-runtime-20261104.md), with the later owner decisions above taking precedence. DAEP lifecycle requirements were inspected at [d3559d731bb74fbb1262114b9aad15687bb54701](https://github.com/onedayonemasterpiece/my-daep/blob/d3559d731bb74fbb1262114b9aad15687bb54701/docs/implementation-task.md); no DAEP runtime code was claimed or copied.

## Real behavior of this slice

- The codec parses one bounded UTF-8 JSON object, rejecting duplicate keys at every depth, malformed JSON, non-object frames, NaN/Infinity and float overflow. It imposes no new wire format and does not authenticate, authorize or deduplicate repeated frames.
- Release selection uses numeric stable `major.minor.patch` tags, with an optional `v` prefix. It excludes branches, malformed tags, drafts and prereleases; it reports newer major lines without automatically migrating the chosen line. Feed adapters must enumerate the relevant official release pages rather than trust a single `latest` result.
- Freshness is a pure assessment from an actual installed version, a selected candidate, an explicit observation timestamp and a caller-supplied maximum age. Missing facts remain unknown; old discovery remains stale. No default freshness SLA has been invented.
- An existing attempt pin remains unchanged when a new candidate appears. The pin contains the exact executable version and executable SHA-256. This is a binary digest, not the digest of a download archive. Attestation comparison accepts observations supplied by a future launch adapter; it does not independently prove which server process is running.
- Rotation begins at `STOP_REQUIRED`. Persisting `requested()` before the external effect moves it to `STOP_UNKNOWN`; replacement cannot start until the exact old executor/generation is observed stopped. Start also becomes unknown before its external effect. Reconciliation distinguishes an unknown start, a confirmed absent start and an identified fresh executor. A stale or duplicate observation cannot advance the current operation. The operation ID must be retained when retrying a confirmed absent launch.
- There is no model-backoff loop in place of rotation. `COMPLETE` means a replacement executor was observed, not that a provider granted quota. A controller must measure subsequent model availability separately.
- `credential_environment` selects only explicitly named scoped keys and returns an empty mapping by default. It is a credential subset, not a sanitizer for an environment that is subsequently merged with `os.environ`. The future process adapter must pass an explicit environment and isolate OpenCode auth/config directories to prevent credential-file inheritance.

## Concrete next integration boundaries

Not yet implemented: durable operation ledger, controller loop, real Kaggle stop/status/launch and operation reconciliation, WSS sessions/routing/authentication, compatibility adapters for existing RKB messages, checkpoint/restore, database master management, resource-authority client, release fetching/install/promotion/canary/rollback, actual process attestation, model calls, isolated worker process environment and product adapters.

Provider reconciliation must use the exact provider execution identity and operation key. Merely observing that a kernel slug exists or that a stop request was accepted is insufficient to claim the old execution stopped. These are provider-adapter obligations; this package currently exposes only the interface and decision primitives.

The complete release lifecycle still needs a deliberate v1/v2 migration policy, scheduled discovery, artifact verification, compatibility canary, promotion for new attempts only, rollback and a maximum-age/offline policy. The current policy code does not download or upgrade OpenCode, and passing its tests does not establish a production rollout.

## Added-element justification

| Element | Concrete decision/failure | Existing donor boundary and why extraction is needed |
|---|---|---|
| Caller-provided byte cap and duplicate/non-finite rejection | Owner requests strict generic JSON extraction | RKB implements these checks inside domain dispatch; the reusable portion needs no domain dependency. |
| Chosen major, newer-major signal, freshness and immutable attempt pin | Owner requires current OpenCode; latest v1 label coexists with v2 | No executable updater exists in the inspected my-daep tree. Pinning alone would become stale. |
| Stop/start UNKNOWN phases and generation correlation | Owner requires stop old before fresh; lost responses must not create overlap | This slice is a pure decision layer; durable ledger and provider observation remain explicit missing adapters. |
| Known-absent versus unknown launch observation | Unknown cannot authorize another launch; definite absence must allow progress | A bare `None` could not distinguish these outcomes. |
| Explicit scoped credential selector | Owner forbids inherited OpenCode API keys by default | No worker process adapter is present yet; source starts from an empty credential set. |

## Validation

Command, from `implementation/kaggle-runtime`:

```sh
PYTHONPATH=src python -m unittest discover -s tests -v
```

Local result: **16 tests passed**. The suite covers malformed/oversize/duplicate JSON fields, UTF-8 byte caps, v1 latest versus v2 stable selection, numeric ordering and branch/draft/prerelease rejection, stale or missing discovery, immutable attempt pins, actual-version mismatch, unknown/stale stop evidence, ordered replacement and replay, unknown versus absent start, stale start identities and default empty credentials.

The package declares Python >=3.11 and no runtime dependencies. Local unit tests use only stdlib and do not require a package install. The checked-in GitHub workflow runs the same command on Python 3.11, 3.12 and 3.13; those CI runs have not been observed as part of this local implementation.

Budget: **320 physical production Python lines + 173 test lines = 493 lines** at handoff. Metadata/workflow and this evidence document are separate. No remote changes, deployment, Kaggle launches, model calls or runtime upgrades were performed by this implementation subtask.
