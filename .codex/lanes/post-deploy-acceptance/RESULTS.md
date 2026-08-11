# M2 post-deploy acceptance automation results

## Scope and base

- Lane: `agent/operational-mvp/post-deploy-acceptance`
- Exact base: `a0622426a335f74808c6c1ccd7d7581cc880333a`
- Owned changes: post-deploy workflow, remote/post-deploy verifier scripts, focused tests, and the direct `cryptography` dependency bound.
- No VPN, DNS, deployment, process-kill, reboot, or other live action was performed.

## Implemented acceptance gates

- **Exact deploy/source identity:** checks out the requested 40-character commit, compares local `HEAD`, binds the same commit to `platform.status`, binds `GITHUB_REPOSITORY` to the requested source identity, and verifies both fields in signed host evidence.
- **DNS/TLS:** requires an exact HTTPS `/mcp` endpoint on implicit port 443, a globally routable DNS answer, normal CA/hostname certificate validation, and TLS 1.2 or 1.3.
- **Host/Origin/auth negatives:** requires wrong Host and wrong Origin to return 403 and missing/invalid bearer credentials to return 401 with an OAuth challenge.
- **OAuth publication:** validates protected-resource metadata, exact authorization-server endpoints, and a bounded public-only RSA/RS256 JWKS.
- **Control and cold data path:** requires the exact read-only MCP catalog, healthy control status and `master=ABSENT`, starts a cold ensure through a bounded `data.query`, observes its durable operation in control status, waits boundedly for an ACTIVE fenced master, and completes a one-row bounded canonical read.
- **Public network boundary:** actively probes that PostgreSQL and all control-loopback ports are not publicly reachable.
- **Host-local boundary and recovery:** requires fresh Ed25519-signed, sanitized evidence for exact running control services, absence of database process/PGDATA/database environment, exact public and loopback listener inventories, process replacement after a process-kill test, and systemd reboot/autostart recovery.
- **Secret safety:** the verifier never includes the bearer token or raw host receipt in results or error output. The uploaded workflow artifact contains only the sanitized verification result and its workflow receipt/hash.

## Required integration wiring

The deployment/host acceptance producer must supply one JSON receipt with schema version `my-data-hub-deployment-evidence.v1`. It must use the exact field contract enforced by `validate_deployment_evidence`, canonical JSON serialization (`sort_keys=true`, compact separators, UTF-8) excluding the `signature` field, and an Ed25519 signature encoded as unpadded base64url. Host/process/boot identities are SHA-256 references only; raw hostnames, environment values, credentials, tokens, private keys, database URLs, or production data must never be included.

Configure:

1. repository variable `MY_DATA_HUB_DEPLOY_EVIDENCE_PUBLIC_KEY_PEM` with the trusted Ed25519 public key;
2. dispatch input `deployment_evidence_key_id` matching that key;
3. dispatch input `deployment_evidence_receipt` containing the fresh sanitized signed receipt;
4. secret `MY_DATA_HUB_MCP_READER_TOKEN` with only the read/status scopes needed by the exact public reader catalog;
5. the expected commit must describe a deployment whose first observed master state is `ABSENT` (the acceptance run intentionally exercises cold activation).

The deployment evidence producer is outside this lane. Until it emits the exact signed contract and the trusted public key is configured, the workflow fails closed rather than asserting host-local state from a public endpoint.

## Validation

- `uv run --extra dev pytest -q tests/test_remote_mcp_verifier.py tests/test_post_deploy_acceptance.py` — PASS (14 tests).
- `uv run --extra dev python -m compileall -q src tests scripts` — PASS.
- `uv run --extra dev ruff check src tests scripts` — PASS.
- `uv run --extra dev pytest -q` — PASS (full suite; two pre-existing skips).
- `uv run --extra dev python scripts/validate_repository.py` — PASS (`ok: true`, 2,887 checks).
- `git diff --check` — PASS.

These are local automated validations only. They are not evidence of a deployment, successful public reachability, a real process-kill/reboot, or ChatGPT interoperability.
