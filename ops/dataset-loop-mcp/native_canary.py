#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import os
import pathlib
import re
import shutil
import stat
import subprocess
import time
import urllib.error
import urllib.request
from contextlib import suppress
from datetime import UTC, datetime
from typing import Any

import jwt

PUBLIC_BASE_URL = "https://mcp-dataset-loop.kenigevents.ru"
PUBLIC_MCP_URL = f"{PUBLIC_BASE_URL}/mcp"
TARGET_REPOSITORY = "onedayonemasterpiece/idea-hub"
TARGET_DATASET_ID = "ru-national-projects-plan-fact-cases"
INVOCATION = f"расширь датасет {TARGET_DATASET_ID} --lightweight --max-iterations 1"
STATE_ROOT = pathlib.Path.home() / ".local/state/dataset-loop-mcp"
RELEASE_ROOT = pathlib.Path.home() / ".local/share/dataset-loop-mcp/current"
SERVICE_NAME = "dataset-loop-mcp.service"
EVIDENCE = pathlib.Path("ops/dataset-loop-mcp/evidence/native-canary.json")
MARKER = pathlib.Path("ops/dataset-loop-mcp/native-canary.completed.json")


class NativeError(RuntimeError):
    pass


def safe(value: str, limit: int = 8000) -> str:
    value = re.sub(r"dlpct_[A-Fa-f0-9]+\.[A-Za-z0-9._~-]+", "[REDACTED]", value)
    value = re.sub(r"gh[pousr]_[A-Za-z0-9_]+", "[REDACTED]", value)
    value = re.sub(r"github_pat_[A-Za-z0-9_]+", "[REDACTED]", value)
    return value[-limit:]


def run(
    argv: list[str],
    *,
    check: bool = True,
    timeout: int = 120,
    host_gh: bool = False,
) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    if host_gh:
        env.pop("GH_TOKEN", None)
        env.pop("GITHUB_TOKEN", None)
    result = subprocess.run(
        argv,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=timeout,
        check=False,
        env=env,
    )
    if check and result.returncode != 0:
        raise NativeError(f"command failed ({result.returncode}): {' '.join(argv[:4])}\n{safe(result.stdout)}")
    return result


def host_gh_json(args: list[str]) -> Any:
    value = run(["gh", *args], host_gh=True, timeout=120).stdout
    try:
        return json.loads(value)
    except json.JSONDecodeError as exc:
        raise NativeError("host gh returned invalid JSON") from exc


def connection() -> dict[str, Any]:
    path = STATE_ROOT / "owner-connection.json"
    if not path.is_file() or path.is_symlink() or stat.S_IMODE(path.stat().st_mode) != 0o600:
        raise NativeError("owner connection file is absent or unsafe")
    value = json.loads(path.read_text())
    if value.get("mcp_url") != PUBLIC_MCP_URL or not str(value.get("authorization", "")).startswith("Bearer dlpct_"):
        raise NativeError("owner connection file is invalid")
    return value


def api(conn: dict[str, Any], method: str, path: str, payload: dict[str, Any] | None = None) -> Any:
    body = None if payload is None else json.dumps(payload).encode()
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": str(conn["authorization"]),
    }
    request = urllib.request.Request(PUBLIC_BASE_URL + path, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=90) as response:
            raw = response.read()
    except urllib.error.HTTPError as exc:
        message = exc.read(8192).decode(errors="replace")
        raise NativeError(f"API {method} {path} failed: HTTP {exc.code}: {safe(message)}") from None
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise NativeError(f"API {method} {path} returned non-JSON") from exc


def settings_fields() -> set[str]:
    script = "from dataset_loop_mcp.config import Settings; print('\\n'.join(sorted(Settings.model_fields)))"
    result = run([str(RELEASE_ROOT / "venv/bin/python"), "-c", script])
    return {line.strip() for line in result.stdout.splitlines() if line.strip()}


def candidate_private_files() -> tuple[list[pathlib.Path], list[pathlib.Path]]:
    roots = [
        pathlib.Path.home() / ".local/state/my-data-hub-control-plane",
        pathlib.Path.home() / ".local/state",
        pathlib.Path.home() / ".config",
        pathlib.Path.home() / ".local/share",
    ]
    pems: list[pathlib.Path] = []
    envs: list[pathlib.Path] = []
    for root in roots:
        if not root.exists():
            continue
        for path in root.rglob("*"):
            try:
                if not path.is_file() or path.is_symlink() or path.stat().st_size > 2_000_000:
                    continue
                mode = stat.S_IMODE(path.stat().st_mode)
            except OSError:
                continue
            lower = path.name.casefold()
            private_github_key = path.suffix.casefold() == ".pem" or (
                "github" in lower and ("app" in lower or "key" in lower)
            )
            if private_github_key and not mode & 0o077:
                pems.append(path.resolve())
            private_config = path.suffix.casefold() in {
                ".env",
                ".conf",
                ".ini",
                ".toml",
                ".yaml",
                ".yml",
            }
            if private_config and not mode & 0o004:
                envs.append(path.resolve())
    return sorted(set(pems)), sorted(set(envs))


def discover_github_app() -> dict[str, Any]:
    pems, envs = candidate_private_files()
    app_ids: set[int] = set()
    installation_ids: set[int] = set()
    client_ids: set[str] = set()
    patterns = {
        "app": re.compile(r"^(?:[A-Z0-9_]*GITHUB[A-Z0-9_]*APP_ID)\s*=\s*([0-9]+)\s*$"),
        "installation": re.compile(r"^(?:[A-Z0-9_]*GITHUB[A-Z0-9_]*INSTALLATION_ID)\s*=\s*([0-9]+)\s*$"),
        "client": re.compile(r"^(?:[A-Z0-9_]*GITHUB[A-Z0-9_]*CLIENT_ID)\s*=\s*([A-Za-z0-9_-]+)\s*$"),
    }
    lines = [f"{key}={value}" for key, value in os.environ.items()]
    for path in envs:
        with suppress(OSError):
            lines.extend(path.read_text(errors="ignore").splitlines())
    for raw in lines:
        line = raw.strip()
        if match := patterns["app"].match(line):
            app_ids.add(int(match.group(1)))
        if match := patterns["installation"].match(line):
            installation_ids.add(int(match.group(1)))
        if match := patterns["client"].match(line):
            client_ids.add(match.group(1))
    now = int(time.time())
    for pem in pems:
        try:
            private_key = pem.read_text()
        except OSError:
            continue
        if "PRIVATE KEY" not in private_key:
            continue
        for app_id in sorted(app_ids):
            app_jwt = jwt.encode(
                {"iat": now - 60, "exp": now + 480, "iss": str(app_id)},
                private_key,
                algorithm="RS256",
            )
            headers = {
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {app_jwt}",
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": "dataset-loop-native-canary",
            }
            request = urllib.request.Request("https://api.github.com/app/installations", headers=headers)
            try:
                with urllib.request.urlopen(request, timeout=20) as response:
                    installations = json.loads(response.read())
            except Exception:
                continue
            for item in installations if isinstance(installations, list) else []:
                installation_id = int(item.get("id") or 0)
                if not installation_id or (installation_ids and installation_id not in installation_ids):
                    continue
                payload = json.dumps({"repositories": ["idea-hub"], "permissions": {"contents": "write"}}).encode()
                token_request = urllib.request.Request(
                    f"https://api.github.com/app/installations/{installation_id}/access_tokens",
                    data=payload,
                    headers={**headers, "Content-Type": "application/json"},
                    method="POST",
                )
                try:
                    with urllib.request.urlopen(token_request, timeout=20) as response:
                        minted = json.loads(response.read())
                except Exception:
                    continue
                token = str(minted.get("token") or "")
                repositories = {
                    str(value.get("name")) for value in minted.get("repositories") or [] if isinstance(value, dict)
                }
                if token and (not repositories or "idea-hub" in repositories):
                    token = ""
                    return {
                        "app_id": app_id,
                        "client_id": next(iter(sorted(client_ids)), None),
                        "installation_id": installation_id,
                        "private_key_file": str(pem),
                        "private_key_sha256": hashlib.sha256(pem.read_bytes()).hexdigest(),
                        "idea_hub_repository_limited_token_minted": True,
                    }
    raise NativeError("no existing DevCoveer GitHub App can mint an idea-hub contents:write installation token")


def acquire_pack_and_opencode() -> dict[str, Any]:
    cache = STATE_ROOT / "native-cache"
    cache.mkdir(parents=True, exist_ok=True)
    cache.chmod(0o700)
    pack = cache / "dataset-loop-pack-0.1.0-alpha.2.tar.gz"
    if not pack.is_file():
        run(
            [
                "gh",
                "release",
                "download",
                "dataset-loop-pack-v0.1.0-alpha.2",
                "--repo",
                "onedayonemasterpiece/dataset-loop-mcp",
                "--pattern",
                pack.name,
                "--dir",
                str(cache),
            ],
            host_gh=True,
            timeout=300,
        )
    opencode = shutil.which("opencode")
    if not opencode:
        raise NativeError("OpenCode binary is absent on DevCoveer")
    opencode_path = pathlib.Path(opencode).resolve()
    version = run([str(opencode_path), "--version"]).stdout.strip().splitlines()[-1].removeprefix("v")
    archive = cache / f"opencode-linux-x64-{version}.tar.gz"
    if not archive.is_file():
        request = urllib.request.Request(
            f"https://github.com/anomalyco/opencode/releases/download/v{version}/opencode-linux-x64.tar.gz",
            headers={"User-Agent": "dataset-loop-native-canary"},
        )
        with urllib.request.urlopen(request, timeout=120) as response, archive.open("wb") as stream:
            shutil.copyfileobj(response, stream)
    run(["tar", "-tzf", str(archive)])
    return {
        "pack_path": str(pack),
        "pack_sha256": hashlib.sha256(pack.read_bytes()).hexdigest(),
        "opencode_path": str(opencode_path),
        "opencode_version": version,
        "opencode_binary_sha256": hashlib.sha256(opencode_path.read_bytes()).hexdigest(),
        "opencode_archive_path": str(archive),
        "opencode_archive_sha256": hashlib.sha256(archive.read_bytes()).hexdigest(),
    }


def update_env(fields: set[str], app: dict[str, Any], native: dict[str, Any], enabled: bool) -> list[str]:
    env_path = STATE_ROOT / "service.env"
    backup = STATE_ROOT / "service.env.before-native-canary"
    if not backup.exists():
        shutil.copy2(env_path, backup)
        backup.chmod(0o600)
    values = {
        "github_app_id": app["app_id"],
        "github_app_client_id": app.get("client_id"),
        "github_app_private_key_file": app["private_key_file"],
        "opencode_version": native["opencode_version"],
        "opencode_sha256": native["opencode_binary_sha256"],
        "opencode_binary_path": native["opencode_path"],
        "opencode_binary_sha256": native["opencode_binary_sha256"],
        "opencode_archive_path": native["opencode_archive_path"],
        "opencode_archive_sha256": native["opencode_archive_sha256"],
        "native_pack_archive_path": native["pack_path"],
        "native_pack_path": native["pack_path"],
        "native_pack_sha256": native["pack_sha256"],
        "dataset_repository_root": str(STATE_ROOT / "native-repositories"),
        "native_dataset_loop_enabled": enabled,
    }
    existing = env_path.read_text().splitlines()
    mapping = {line.split("=", 1)[0]: index for index, line in enumerate(existing) if "=" in line}
    configured: list[str] = []
    for field, raw_value in values.items():
        if field not in fields or raw_value in {None, ""}:
            continue
        value = str(raw_value).lower() if isinstance(raw_value, bool) else str(raw_value)
        key = "DATASET_LOOP_" + field.upper()
        line = f"{key}={value}"
        if key in mapping:
            existing[mapping[key]] = line
        else:
            mapping[key] = len(existing)
            existing.append(line)
        configured.append(field)
    env_path.write_text("\n".join(existing) + "\n")
    env_path.chmod(0o600)
    run(["systemctl", "--user", "restart", SERVICE_NAME])
    return sorted(configured)


def run_payload(conn: dict[str, Any], app: dict[str, Any], base_sha: str) -> dict[str, Any]:
    profiles = api(conn, "GET", "/v1/kaggle/profiles")
    if not isinstance(profiles, list) or not profiles:
        raise NativeError("no Kaggle profile for native canary")
    profile_id = str(profiles[0]["id"])
    openapi = api(conn, "GET", "/openapi.json")
    schema = openapi.get("components", {}).get("schemas", {}).get("RunCreate", {})
    properties = schema.get("properties", {})
    required = set(schema.get("required") or [])
    semantic = {
        "workload_type": "native_dataset_loop",
        "duration_minutes": 30,
        "kaggle_profile_id": profile_id,
        "profile_id": profile_id,
        "dataset_repository": TARGET_REPOSITORY,
        "repository": TARGET_REPOSITORY,
        "github_repository": TARGET_REPOSITORY,
        "base_sha": base_sha,
        "dataset_base_sha": base_sha,
        "repository_base_sha": base_sha,
        "github_installation_id": app["installation_id"],
        "installation_id": app["installation_id"],
        "invocation": INVOCATION,
        "dataset_loop_invocation": INVOCATION,
        "command": INVOCATION,
    }
    payload = {name: value for name, value in semantic.items() if name in properties}
    payload.setdefault("workload_type", "native_dataset_loop")
    payload.setdefault("duration_minutes", 30)
    unresolved = required - set(payload)
    if unresolved:
        raise NativeError(f"native RunCreate has unresolved required fields: {sorted(unresolved)}")
    return payload


def wait_run(conn: dict[str, Any], run_id: str, timeout: int = 3600) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    latest: dict[str, Any] = {}
    while time.monotonic() < deadline:
        latest = api(conn, "GET", f"/v1/runs/{run_id}")
        if latest.get("state") in {"succeeded", "failed", "cancelled", "stale"}:
            return latest
        time.sleep(20)
    raise NativeError(f"native canary timeout, last state={latest.get('state')!r}")


def safe_run(value: dict[str, Any]) -> dict[str, Any]:
    keys = (
        "id",
        "workload_type",
        "duration_minutes",
        "state",
        "provider_session_id",
        "slot_id",
        "kernel_id",
        "notebook_revision",
        "manifest_sha256",
        "lease_generation",
        "branch",
        "git_head",
        "checkpoint",
        "created_at",
        "updated_at",
    )
    return {key: value.get(key) for key in keys}


def write_result(value: dict[str, Any]) -> None:
    EVIDENCE.parent.mkdir(parents=True, exist_ok=True)
    EVIDENCE.write_text(json.dumps(value, indent=2, ensure_ascii=False, sort_keys=True) + "\n")
    MARKER.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "status": value["status"],
                "native_dataset_loop_enabled": value.get("native_dataset_loop_enabled", False),
                "public_mcp_url": PUBLIC_MCP_URL,
                "evidence": str(EVIDENCE),
                "workflow_run_id": int(os.environ.get("GITHUB_RUN_ID", "0")),
            },
            indent=2,
            sort_keys=True,
        )
        + "\n"
    )


def main() -> int:
    started = datetime.now(UTC).isoformat()
    try:
        completion = json.loads(pathlib.Path("ops/dataset-loop-mcp/deployment.completed.json").read_text())
        if completion.get("status") != "PUBLIC_INFRASTRUCTURE_READY":
            raise NativeError("public infrastructure deployment is not ready")
        conn = connection()
        fields = settings_fields()
        app = discover_github_app()
        native = acquire_pack_and_opencode()
        base_sha = run(
            ["gh", "api", f"repos/{TARGET_REPOSITORY}/branches/main", "--jq", ".commit.sha"],
            host_gh=True,
        ).stdout.strip()
        if not re.fullmatch(r"[0-9a-f]{40}", base_sha):
            raise NativeError("idea-hub exact base SHA is unavailable")
        configured = update_env(fields, app, native, False)
        payload = run_payload(conn, app, base_sha)
        configured = update_env(fields, app, native, True)
        started_run = api(conn, "POST", "/v1/runs", payload)
        if not isinstance(started_run, dict) or not started_run.get("id"):
            raise NativeError("native start_run returned invalid data")
        terminal = wait_run(conn, str(started_run["id"]))
        if terminal.get("state") != "succeeded":
            raise NativeError(f"native canary failed: {safe_run(terminal)}")
        branch = str(terminal.get("branch") or "")
        git_head = str(terminal.get("git_head") or "")
        if not branch.startswith("dataset-loop/run/") or not re.fullmatch(r"[0-9a-f]{40}", git_head):
            raise NativeError("native canary has no exact run branch/head")
        remote = run(
            ["gh", "api", f"repos/{TARGET_REPOSITORY}/git/ref/heads/{branch}", "--jq", ".object.sha"],
            host_gh=True,
        ).stdout.strip()
        if remote != git_head:
            raise NativeError("native run branch remote readback differs from run head")
        artifacts = api(conn, "GET", f"/v1/runs/{terminal['id']}/artifacts")
        artifacts_invalid = (
            not isinstance(artifacts, list)
            or not artifacts
            or any(not item.get("verified_readback") for item in artifacts)
        )
        if artifacts_invalid:
            raise NativeError("native artifacts are absent or lack verified readback")
        names = {str(item.get("name") or "").casefold() for item in artifacts}
        if not any("session" in name and "bundle" in name for name in names):
            raise NativeError("native canary lacks the OpenCode session bundle")
        journal = run(["journalctl", "--user", "-u", SERVICE_NAME, "--no-pager", "-n", "2000"], check=False).stdout
        bearer = str(conn["authorization"])
        if bearer in journal or bearer.removeprefix("Bearer ") in journal:
            raise NativeError("owner bearer leaked into service journal")
        result = {
            "schema_version": 1,
            "status": "NATIVE_DATASET_LOOP_READY",
            "started_at": started,
            "completed_at": datetime.now(UTC).isoformat(),
            "public_mcp_url": PUBLIC_MCP_URL,
            "target_repository": TARGET_REPOSITORY,
            "target_base_sha": base_sha,
            "dataset_id": TARGET_DATASET_ID,
            "invocation": INVOCATION,
            "github_app": {key: value for key, value in app.items() if key != "private_key_file"},
            "configured_settings": configured,
            "run": safe_run(terminal),
            "remote_git_readback_verified": True,
            "artifact_count": len(artifacts),
            "session_bundle_present": True,
            "native_dataset_loop_enabled": True,
        }
        write_result(result)
        return 0
    except Exception as exc:
        env_path = STATE_ROOT / "service.env"
        backup = STATE_ROOT / "service.env.before-native-canary"
        if backup.is_file():
            shutil.copy2(backup, env_path)
        elif env_path.is_file():
            text = re.sub(
                r"(?m)^DATASET_LOOP_NATIVE_DATASET_LOOP_ENABLED=.*$",
                "DATASET_LOOP_NATIVE_DATASET_LOOP_ENABLED=false",
                env_path.read_text(),
            )
            env_path.write_text(text)
        if env_path.is_file():
            env_path.chmod(0o600)
            run(["systemctl", "--user", "restart", SERVICE_NAME], check=False)
        result = {
            "schema_version": 1,
            "status": "NATIVE_BLOCKED",
            "started_at": started,
            "failed_at": datetime.now(UTC).isoformat(),
            "public_mcp_url": PUBLIC_MCP_URL,
            "error_type": type(exc).__name__,
            "error": safe(str(exc)),
            "native_dataset_loop_enabled": False,
        }
        write_result(result)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
