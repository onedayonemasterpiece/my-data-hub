#!/usr/bin/env python3
from __future__ import annotations

import base64
import os
import pathlib
import subprocess
import sys

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import deploy as impl  # noqa: E402


def robust_run(
    argv: list[str],
    *,
    cwd: pathlib.Path | None = None,
    env: dict[str, str] | None = None,
    timeout: int = 120,
    check: bool = True,
    host_gh: bool = False,
    input_text: str | None = None,
) -> impl.Completed:
    command_env = os.environ.copy()
    if env:
        command_env.update(env)
    if host_gh:
        command_env.pop("GH_TOKEN", None)
        command_env.pop("GITHUB_TOKEN", None)
    try:
        result = subprocess.run(
            argv,
            cwd=cwd,
            env=command_env,
            input=input_text,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=timeout,
            check=False,
        )
    except Exception as exc:
        raise impl.DeployError(
            f"command could not execute: {argv[0]}: {type(exc).__name__}"
        ) from exc
    output = result.stdout
    if not check and result.returncode != 0 and not output:
        output = f"__EXIT_{result.returncode}__\n"
    if check and result.returncode != 0:
        raise impl.DeployError(
            f"command failed ({result.returncode}): {' '.join(argv[:4])}\n"
            f"{impl.safe_text(result.stdout)}"
        )
    return impl.Completed(argv, output)


def write_service_env(
    port: int, kaggle_owner: str, venv: pathlib.Path
) -> dict[str, str]:
    secret_root = impl.STATE_ROOT / "secrets"
    provider_root = secret_root / "provider"
    artifact_root = impl.STATE_ROOT / "artifacts"
    for path in (impl.STATE_ROOT, secret_root, provider_root, artifact_root):
        path.mkdir(parents=True, exist_ok=True)
        path.chmod(0o700)
    credential_key = impl.ensure_secret(
        secret_root / "credential-encryption.key",
        lambda: base64.urlsafe_b64encode(os.urandom(32)).decode(),
    )
    artifact_key = impl.ensure_secret(
        secret_root / "artifact-signing.key", lambda: impl.secrets.token_urlsafe(64)
    )
    callback_key = impl.ensure_secret(
        secret_root / "callback-signing.key", lambda: impl.secrets.token_urlsafe(64)
    )
    values = {
        "DATASET_LOOP_ENVIRONMENT": "staging",
        "DATASET_LOOP_AUTH_MODE": "owner_pct",
        "DATASET_LOOP_PUBLIC_BASE_URL": impl.PUBLIC_BASE_URL,
        "DATASET_LOOP_DATABASE_PATH": str(impl.STATE_ROOT / "control.sqlite3"),
        "DATASET_LOOP_SECRET_STORE_PATH": str(provider_root),
        "DATASET_LOOP_ARTIFACT_ROOT": str(artifact_root),
        "DATASET_LOOP_CREDENTIAL_ENCRYPTION_KEY": credential_key,
        "DATASET_LOOP_ARTIFACT_SIGNING_KEY": artifact_key,
        "DATASET_LOOP_CALLBACK_SIGNING_KEY": callback_key,
        "DATASET_LOOP_WORKER_TOKEN_SIGNING_KEY": callback_key,
        "DATASET_LOOP_KAGGLE_LIVE_ENABLED": "true",
        "DATASET_LOOP_NATIVE_DATASET_LOOP_ENABLED": "false",
        "DATASET_LOOP_KAGGLE_CAPABILITY_MODE": "stable_slot_pool",
        "DATASET_LOOP_KAGGLE_TEMPLATE_SLUG": "dataset-loop-worker",
        "DATASET_LOOP_KAGGLE_SLOT_IDS": "s01,s02",
        "DATASET_LOOP_KAGGLE_KERNEL_OWNER": kaggle_owner,
        "DATASET_LOOP_MAX_CONCURRENCY": "2",
        "DATASET_LOOP_OIDC_ISSUER": impl.OAUTH_ISSUER,
        "DATASET_LOOP_OIDC_JWKS_URL": f"{impl.OAUTH_ISSUER}/.well-known/jwks.json",
        "DATASET_LOOP_OIDC_CLIENT_ID": "dataset-loop-mcp",
        "DATASET_LOOP_OIDC_REVOCATION_ENDPOINT": f"{impl.OAUTH_ISSUER}/revoke",
        "DATASET_LOOP_POLL_INTERVAL_SECONDS": "15",
        "DATASET_LOOP_SUPERVISOR_ENABLED": "true",
        "DATASET_LOOP_DEPLOYED_PORT": str(port),
        "PATH": f"{venv / 'bin'}:{os.environ.get('PATH', '')}",
    }
    impl.atomic_private_text(
        impl.STATE_ROOT / "service.env",
        "\n".join(f"{key}={value}" for key, value in sorted(values.items())) + "\n",
    )
    return values


def commit_success() -> str:
    robust_run(["git", "diff", "--check"])
    robust_run(["git", "add", "-A"])
    changed = subprocess.run(["git", "diff", "--cached", "--quiet"]).returncode != 0
    if changed:
        robust_run(["git", "config", "user.name", "github-actions[bot]"])
        robust_run(
            [
                "git",
                "config",
                "user.email",
                "41898282+github-actions[bot]@users.noreply.github.com",
            ]
        )
        robust_run(
            ["git", "commit", "-m", "ops: deploy Dataset Loop MCP on DevCoveer"]
        )
        robust_run(["git", "pull", "--rebase", "origin", "main"])
        robust_run(["git", "push", "origin", "HEAD:main"])
    head = robust_run(["git", "rev-parse", "HEAD"]).stdout.strip()
    remote = robust_run(
        ["git", "ls-remote", "origin", "refs/heads/main"]
    ).stdout.split()[0]
    if head != remote:
        raise impl.DeployError("my-data-hub main remote readback mismatch")
    return head


impl.run = robust_run
impl.write_service_env = write_service_env
impl.git_commit_success = commit_success

exit_code = impl.main()
runner_temp = pathlib.Path(os.environ.get("RUNNER_TEMP", "/tmp"))
for source, target in (
    (
        impl.EVIDENCE_DIR / "deployment-failure.json",
        runner_temp / "dataset-loop-deployment-failure.json",
    ),
    (
        impl.EVIDENCE_DIR / "deployment-result.json",
        runner_temp / "dataset-loop-deployment-result.json",
    ),
):
    if source.is_file():
        target.write_bytes(source.read_bytes())
raise SystemExit(exit_code)
