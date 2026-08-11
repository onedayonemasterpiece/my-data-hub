# Signed devstand deployment evidence procedure

Status: `IMPLEMENTED / NOT EXECUTED`

This procedure produces the host-local input required by the post-deploy workflow. It is
not a deployment receipt until every real observation succeeds and the final document is
signed. No example in the repository is live evidence.

The collector is
`deploy/control-plane/collect_deployment_evidence.py`. It never accepts booleans, service
states, listener lists, image identifiers, process identifiers, boot identifiers, source
hashes or database-absence claims on the command line. It derives them from the exact Git
checkout, immutable installed release, procfs, Docker and the enabled user systemd unit.
All subprocesses are bounded and their output is omitted from errors.

## What the collector proves

For one exact clean GitHub checkout and its matching installed release, it fails closed
unless all of these facts are observed:

- the clean checkout commit equals the basename and target of the installed `current`
  release;
- the GitHub `origin` supplies the source identity;
- a canonical SHA-256 manifest over every tracked path, executable mode and file content is
  identical for the source checkout and installed read-only release;
- `control-plane`, `oauth-server` and `remote-mcp` are healthy Compose services with
  `unless-stopped`, their exact loopback bindings are `8080`, `8780` and `8765`, and all
  three running containers use the same immutable Docker `sha256:` image ID resolved from
  the exact commit tag;
- the user unit is active and enabled, is bound to that release and Compose environment,
  and the service user has lingering enabled;
- no PostgreSQL/postmaster process or configured container exists, no `PG_VERSION` exists
  below the controlled runtime/release roots, no project PostgreSQL/PGDATA Docker volume is
  inventoried, and neither host/container/runtime environment keys contain `PGDATA`, libpq
  connection variables or a database URL;
- an exact observed container process is killed with `SIGKILL`, becomes healthy again with
  a different hashed process reference, and remains the same recovered process when the
  reboot gate is prepared;
- a later collection sees a different kernel boot ID, a kernel boot time after the
  prepared gate, the enabled/active unit and the exact three healthy services.

Only hashes of host, boot and process identity are recorded. Environment values, command
output, hostnames, raw process arguments, credentials, private keys, database URLs and
production data are never written to state or receipt files.

## Preconditions

Run from the exact clean checkout used by `INSTALL_MY_DATA_HUB_CONTROL_PLANE`. The
installed `current` symlink, runtime Compose environment, containers and user unit must
already exist. Use a Python 3.12 environment with the checked-out project installed; for a
checkout with `uv`, prefix the commands below with `uv run --extra dev`.

Create or provision a dedicated Ed25519 evidence key outside the release. This is distinct
from the OAuth token-signing key. The private file must be owned by the service user, be a
regular non-symlink file, and have mode `0600`; it is never copied into the repository or
receipt. For example, under an operator-approved private state path:

```bash
umask 077
openssl genpkey -algorithm ED25519 \
  -out "$HOME/.local/state/my-data-hub-control-plane/secrets/deployment-evidence-ed25519.pem"
openssl pkey \
  -in "$HOME/.local/state/my-data-hub-control-plane/secrets/deployment-evidence-ed25519.pem" \
  -pubout \
  -out "$HOME/.local/state/my-data-hub-control-plane/deployment-evidence-public.pem"
```

Store only the public PEM in the GitHub repository variable
`MY_DATA_HUB_DEPLOY_EVIDENCE_PUBLIC_KEY_PEM`. Set the repository variable
`MY_DATA_HUB_APPROVED_DEPLOY_COMMIT` to the exact reviewed default-branch merge commit
before dispatching post-deploy acceptance. Rotate the key ID deliberately and supply the
same non-secret ID to collection and workflow dispatch.

## Controlled process failure

This step deliberately kills one live container process. It requires an explicit
maintenance window and owner authorization. The collector does not stop, start or restart
the service through Docker; it signals the immediately re-observed host PID so the Docker
restart policy, rather than a manual start, must recover it.

```bash
python deploy/control-plane/collect_deployment_evidence.py \
  EXERCISE_PROCESS_KILL \
  --source-root "$PWD" \
  --target-service remote-mcp \
  --timeout-seconds 120
```

Success writes a mode-`0600` sanitized state file below the private control runtime. A
failure writes no passing receipt.

## Prepare and perform the separate reboot

Immediately before the authorized host reboot, bind the recovered process and current boot
to the staged state:

```bash
python deploy/control-plane/collect_deployment_evidence.py \
  PREPARE_REBOOT \
  --source-root "$PWD"
```

The collector intentionally has no reboot command. After the preparation succeeds, the
operator performs the separately authorized host reboot using the host's normal procedure.
Do not edit the state file, change the deployed release or rebuild/retag the image between
these steps.

## Collect and sign after reboot

After login-independent systemd/Docker reconciliation is healthy, run:

```bash
python deploy/control-plane/collect_deployment_evidence.py \
  SIGN_DEPLOYMENT_EVIDENCE \
  --source-root "$PWD" \
  --signing-key-file "$HOME/.local/state/my-data-hub-control-plane/secrets/deployment-evidence-ed25519.pem" \
  --key-id devstand-evidence-2026-08 \
  --output "$HOME/.local/state/my-data-hub-control-plane/deployment-evidence.v1.json" \
  --ttl-seconds 3600
```

The output directory must be private. The collector refuses the signing step if the boot
ID did not change, observations are stale or out of order, the release/source hash differs,
a running container differs from the one immutable image ID, the key is unsafe, or any
DB-free/service/listener/unit gate fails.

The receipt and its precursor are validated by
`schemas/deployment-evidence.v1.schema.json` and
`schemas/deployment-evidence-state.v1.schema.json`. The committed examples contain only
synthetic hashes and a placeholder signature; they cannot authenticate a deployment.

## Dispatch post-deploy acceptance

Dispatch `.github/workflows/post-deploy.yml` from the repository default branch only. The
workflow runs verifier code from a trusted default-branch checkout, requires the requested
deployed commit to equal `MY_DATA_HUB_APPROVED_DEPLOY_COMMIT`, requires it to be a merge
commit reachable from that branch, and scopes reader/evidence inputs only to the verifier
step. Its MCP resource and authorization-server origins are fixed owner contracts, not
workflow inputs.

Supply the fresh signed JSON as `deployment_evidence_receipt`, its key ID as
`deployment_evidence_key_id`, and the exact deployed merge SHA as `expected_commit`. The
remote verifier independently checks the signature, exact source/commit, equal
source/release SHA-256 manifests, one immutable image ID for all three services, recovery
ordering and every other receipt field before performing public acceptance.

Do not describe a successful local command, schema-valid example or generated unsigned
state as deployment proof. Archive only the signed receipt and the sanitized post-deploy
workflow receipt; never archive the private key or environment files.
