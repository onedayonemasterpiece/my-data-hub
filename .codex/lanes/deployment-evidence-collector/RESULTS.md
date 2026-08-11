# M2 closure: deployment evidence collector and post-deploy trust boundary

## Scope

- Branch: `agent/operational-mvp/deployment-evidence-collector`
- Exact base: `c02c2f17ae85fad01b5a2e1b80cec8aa5979f681`
- Execution: serial isolated lane because collector, schemas, verifier projection and procedure share one exact receipt contract.
- Live actions: none. No container was killed, no host rebooted, no key generated, no evidence signed, no endpoint contacted and no workflow dispatched.

Reviewer findings expanded the originally isolated producer scope. The lane therefore makes
narrow required changes to `scripts/verify_post_deploy.py`, `.github/workflows/post-deploy.yml`
and their focused tests. Integration must reconcile those files with the later integration
head; it must not discard the trust-boundary changes below.

## Requirement closure

| Requirement | Status | Evidence |
|---|---|---|
| Host-side collector/signer/procedure | Done | Three explicit commands exercise one real process kill, prepare a separate reboot, then observe post-reboot recovery and sign. The collector has no reboot command and accepts no asserted fact fields. |
| Exact checkout/source/release | Done | GitHub source identity and clean commit are derived from Git; `current` must target `releases/<commit>`; a canonical tracked path/mode/content SHA-256 manifest must match the read-only installed release. |
| Immutable image binding | Done | Docker inspection requires three exact healthy Compose services, exact commit tag resolution, `unless-stopped`, and one shared immutable `sha256:` image ID. The signed receipt includes each service-to-image binding; the verifier validates the exact map and single immutable ID. |
| DB-free devstand | Done | Procfs and all Docker container configurations are checked for PostgreSQL; controlled roots and project volume names are checked for PGDATA; host/container/runtime environment key inventories reject PGDATA/libpq/database URLs without recording values. |
| Listener/service/unit state | Done | Container inspection derives the exact three loopback bindings and no my-data-hub public binding. User systemd active/enabled state, exact unit release/env binding and `Linger=yes` are observed. |
| Process replacement | Done | The host PID is re-observed immediately before `SIGKILL`; recovery must become healthy inside a bounded wait with a different sanitized process reference. No manual Docker start is used. |
| Reboot/autostart | Done | Private staged state binds the recovered process and pre-reboot boot ID. Signing requires a later kernel boot ID/time, active/enabled unit and exact three healthy services. |
| External canonical signing | Done | A separately provisioned mode-`0600`, owner-only, non-symlink Ed25519 private key is loaded only at final signing. Canonical compact sorted JSON excluding `signature` is signed; key material and command output are never emitted. |
| JSON contracts/examples | Done | Strict schemas and synthetic examples exist for staged state, raw signed deployment evidence and the sanitized `my-data-hub-post-deploy-verification.v1` report. The report schema matches the actual verifier projection, not a duplicate raw signature. |
| Validator registration | Done | Repository validation registers all three examples/schemas and the new classified deployment script. A real `verify_all`-shaped object is schema-tested. |
| Canonical public trust | Done | MCP is pinned to `https://mcp-datahub.kenigevents.ru/mcp`; OAuth issuer is pinned to `https://identity.kenigevents.ru`; alternate endpoint/issuer tests deny. Committed provider metadata now matches runtime `/authorize`, `/token` and public JWKS paths. |
| Complete public port probing | Done | One DNS result set is reused; every resolved global A/AAAA address is probed on every forbidden port, while TLS CA/hostname validation still connects by the canonical hostname. |
| Workflow code/secret trust | Done | Only trusted default-branch verifier code is installed/run. The deployed SHA must equal `MY_DATA_HUB_APPROVED_DEPLOY_COMMIT`, be a reachable merge commit, and is never checked out/executed. Reader/evidence inputs exist only on the verification step. |
| Deployment procedure | Done | `docs/operations/deployment-evidence.md` documents prerequisites, explicit disruptive authorization, external key handling, process/reboot/sign order, dispatch wiring and evidence limitations; devstand deployment links it. |

## Files and integration wiring

Main producer files:

- `src/my_data_hub/control_plane/deployment_evidence.py`
- `deploy/control-plane/collect_deployment_evidence.py`
- `schemas/deployment-evidence-state.v1.schema.json`
- `schemas/deployment-evidence.v1.schema.json`
- `schemas/post-deploy-verification.v1.schema.json`
- matching examples under `examples/contracts/`
- `docs/operations/deployment-evidence.md`

Required repository configuration:

1. `MY_DATA_HUB_APPROVED_DEPLOY_COMMIT`: exact approved default-branch merge SHA;
2. `MY_DATA_HUB_DEPLOY_EVIDENCE_PUBLIC_KEY_PEM`: public half of the external evidence key;
3. `MY_DATA_HUB_MCP_READER_TOKEN`: bounded reader/status token;
4. workflow dispatch key ID must match the signer key ID;
5. collection must run from the exact clean checkout matching the installed `current` release.

The raw signed receipt is sanitized but remains distinct from the uploaded post-deploy report,
which contains only its verified projection and evidence hash. The synthetic examples contain
placeholder hashes/signatures and are not deployment proof.

## Validation

- Focused collector/post-deploy/MCP contract tests: PASS (40 tests in the final focused command).
- Full suite: PASS after final rerun (575 collected; two pre-existing skips).
- `uv run --extra dev python -m compileall -q src tests`: PASS.
- `uv run --extra dev ruff check src tests scripts deploy/control-plane`: PASS.
- `uv run --extra dev python scripts/validate_repository.py`: PASS (`ok: true`, 2,935 checks).
- `git diff --check`: PASS.

These results prove local implementation and contract behavior only. A real signed receipt
must come from the documented authorized host procedure after the merged commit is installed.
