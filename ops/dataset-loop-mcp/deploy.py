#!/usr/bin/env python3
from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import secrets
import shutil
import socket
import stat
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

RELEASE_REPOSITORY = "onedayonemasterpiece/dataset-loop-mcp"
RELEASE_TAG = "v0.1.0-alpha.3"
PUBLIC_BASE_URL = "https://mcp-dataset-loop.kenigevents.ru"
PUBLIC_MCP_URL = f"{PUBLIC_BASE_URL}/mcp"
OAUTH_ISSUER = "https://identity.kenigevents.ru"
EXISTING_MCP_URLS = (
    "https://mcp.kenigevents.ru/mcp",
    "https://mcp-datahub.kenigevents.ru/mcp",
)
STATE_ROOT = Path.home() / ".local/state/dataset-loop-mcp"
INSTALL_ROOT = Path.home() / ".local/share/dataset-loop-mcp"
SERVICE_NAME = "dataset-loop-mcp.service"
OWNER_CONNECTION = STATE_ROOT / "owner-connection.json"
PUBLIC_KEY = Path("ops/dataset-loop-mcp/owner-delivery-public.pem")
EVIDENCE_DIR = Path("ops/dataset-loop-mcp/evidence")
RUNTIME_DIR = Path("ops/dataset-loop-mcp/runtime")


class DeployError(RuntimeError):
    pass


@dataclass(frozen=True)
class Completed:
    command: list[str]
    stdout: str


def utcnow() -> str:
    return datetime.now(UTC).isoformat()


def safe_text(value: str, limit: int = 12_000) -> str:
    patterns = (
        r"dlpct_[A-Fa-f0-9]+\.[A-Za-z0-9._~-]+",
        r"dlclaim_[A-Fa-f0-9]+\.[A-Za-z0-9._~-]+",
        r"github_pat_[A-Za-z0-9_]+",
        r"gh[pousr]_[A-Za-z0-9_]+",
        r"(?i)authorization:\s*bearer\s+\S+",
        r"(?i)\"provider_credential\"\s*:\s*\"[^\"]+\"",
    )
    result = value
    for pattern in patterns:
        result = re.sub(pattern, "[REDACTED]", result)
    return result[-limit:]


def run(
    argv: list[str],
    *,
    cwd: Path | None = None,
    env: dict[str, str] | None = None,
    timeout: int = 120,
    check: bool = True,
    host_gh: bool = False,
    input_text: str | None = None,
) -> Completed:
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
        raise DeployError(f"command could not execute: {argv[0]}: {type(exc).__name__}") from exc
    if check and result.returncode != 0:
        raise DeployError(
            f"command failed ({result.returncode}): {' '.join(argv[:4])}\n{safe_text(result.stdout)}"
        )
    return Completed(argv, result.stdout)


def host_gh_json(args: list[str]) -> Any:
    result = run(["gh", *args], host_gh=True, timeout=120)
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise DeployError(f"host gh returned invalid JSON for {args[:3]}") from exc


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def atomic_private_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.parent.chmod(0o700)
    if path.exists() and path.is_symlink():
        raise DeployError(f"refusing symlink: {path}")
    temp = path.with_name(path.name + ".new")
    descriptor = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write(value)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp, path)
        path.chmod(0o600)
    finally:
        temp.unlink(missing_ok=True)


def choose_port(preferred: int | None = None) -> int:
    candidates = ([preferred] if preferred else []) + list(range(8791, 8811))
    for port in candidates:
        if port is None:
            continue
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.bind(("127.0.0.1", port))
        except OSError:
            if preferred == port and service_active():
                return port
        else:
            return port
        finally:
            sock.close()
    raise DeployError("no free loopback port in 8791..8810")


def service_active() -> bool:
    return run(
        ["systemctl", "--user", "is-active", "--quiet", SERVICE_NAME],
        check=False,
        timeout=20,
    ).stdout == ""


def release_metadata() -> dict[str, Any]:
    ref = host_gh_json(["api", f"repos/{RELEASE_REPOSITORY}/git/ref/tags/{RELEASE_TAG}"])
    if ref.get("object", {}).get("type") != "tag":
        raise DeployError("alpha.3 release tag is absent or lightweight")
    tag_object_sha = str(ref["object"]["sha"])
    tag = host_gh_json(["api", f"repos/{RELEASE_REPOSITORY}/git/tags/{tag_object_sha}"])
    if tag.get("object", {}).get("type") != "commit":
        raise DeployError("alpha.3 annotated tag does not peel to a commit")
    release = host_gh_json(["api", f"repos/{RELEASE_REPOSITORY}/releases/tags/{RELEASE_TAG}"])
    if release.get("prerelease") is not True or release.get("draft") is not False:
        raise DeployError("alpha.3 GitHub Release is not a published prerelease")
    assets = {item.get("name"): item for item in release.get("assets", [])}
    required = {
        "dataset_loop_mcp-0.1.0a3-py3-none-any.whl",
        "dataset_loop_mcp-0.1.0a3.tar.gz",
        "SHA256SUMS",
    }
    if not required <= set(assets):
        raise DeployError(f"alpha.3 Release assets missing: {sorted(required - set(assets))}")
    return {
        "tag_object_sha": tag_object_sha,
        "peeled_commit_sha": str(tag["object"]["sha"]),
        "release_id": int(release["id"]),
        "assets": assets,
    }


def download_release(temp: Path, metadata: dict[str, Any]) -> dict[str, Any]:
    destination = temp / "release"
    destination.mkdir(parents=True)
    run(
        [
            "gh",
            "release",
            "download",
            RELEASE_TAG,
            "--repo",
            RELEASE_REPOSITORY,
            "--dir",
            str(destination),
        ],
        host_gh=True,
        timeout=300,
    )
    sums = destination / "SHA256SUMS"
    run(["sha256sum", "-c", sums.name], cwd=destination)
    readback: dict[str, Any] = {}
    for path in sorted(destination.iterdir()):
        if not path.is_file():
            continue
        remote = metadata["assets"].get(path.name)
        if remote is None or int(remote.get("size") or -1) != path.stat().st_size:
            raise DeployError(f"release asset size readback failed: {path.name}")
        digest = sha256(path)
        remote_digest = remote.get("digest")
        if remote_digest and remote_digest != f"sha256:{digest}":
            raise DeployError(f"release asset API digest mismatch: {path.name}")
        readback[path.name] = {
            "id": int(remote["id"]),
            "bytes": path.stat().st_size,
            "sha256": digest,
            "digest": remote_digest or f"sha256:{digest}",
        }
    return {"directory": destination, "assets": readback}


def install_release(release_dir: Path, metadata: dict[str, Any]) -> tuple[Path, Path]:
    release_root = INSTALL_ROOT / "releases" / RELEASE_TAG
    source = release_root / "source"
    venv = release_root / "venv"
    release_root.mkdir(parents=True, exist_ok=True)
    release_root.chmod(0o700)
    shutil.rmtree(source, ignore_errors=True)
    shutil.rmtree(venv, ignore_errors=True)
    source.mkdir()
    run(
        [
            "tar",
            "-xzf",
            str(release_dir / "dataset_loop_mcp-0.1.0a3.tar.gz"),
            "-C",
            str(source),
            "--strip-components=1",
        ]
    )
    run([sys.executable, "-m", "venv", str(venv)], timeout=180)
    run(
        [
            str(venv / "bin/pip"),
            "install",
            "--disable-pip-version-check",
            str(release_dir / "dataset_loop_mcp-0.1.0a3-py3-none-any.whl"),
        ],
        timeout=600,
    )
    run([str(venv / "bin/dataset-loop-mcp"), "--help"])
    current = INSTALL_ROOT / "current"
    current.parent.mkdir(parents=True, exist_ok=True)
    new = current.with_name("current.new")
    new.unlink(missing_ok=True)
    new.symlink_to(release_root)
    os.replace(new, current)
    atomic_private_text(STATE_ROOT / "release.json", json.dumps({
        "tag": RELEASE_TAG,
        "tag_object_sha": metadata["tag_object_sha"],
        "release_commit_sha": metadata["peeled_commit_sha"],
        "release_id": metadata["release_id"],
        "installed_at": utcnow(),
    }, indent=2, sort_keys=True) + "\n")
    return source, venv


def load_kaggle_credential() -> tuple[str, str]:
    candidates = []
    configured = os.environ.get("KAGGLE_CONFIG_DIR")
    if configured:
        candidates.append(Path(configured) / "kaggle.json")
    candidates.append(Path.home() / ".kaggle/kaggle.json")
    for path in candidates:
        if not path.is_file() or path.is_symlink():
            continue
        try:
            payload = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError):
            continue
        username = str(payload.get("username") or "").strip()
        key = str(payload.get("key") or "").strip()
        if username and key:
            if stat.S_IMODE(path.stat().st_mode) & 0o077:
                path.chmod(0o600)
            return username, json.dumps({"username": username, "key": key}, separators=(",", ":"))
    username = os.environ.get("KAGGLE_USERNAME", "").strip()
    key = os.environ.get("KAGGLE_KEY", "").strip()
    if username and key:
        return username, json.dumps({"username": username, "key": key}, separators=(",", ":"))
    raise DeployError("managed Kaggle credential is absent on DevCoveer")


def ensure_secret(path: Path, generator: callable) -> str:
    if not path.exists():
        atomic_private_text(path, str(generator()) + "\n")
    if path.is_symlink() or stat.S_IMODE(path.stat().st_mode) != 0o600:
        raise DeployError(f"unsafe service secret: {path}")
    value = path.read_text().strip()
    if not value:
        raise DeployError(f"empty service secret: {path}")
    return value


def write_service_env(port: int, kaggle_owner: str, venv: Path) -> dict[str, str]:
    from cryptography.fernet import Fernet

    secret_root = STATE_ROOT / "secrets"
    provider_root = secret_root / "provider"
    artifact_root = STATE_ROOT / "artifacts"
    for path in (STATE_ROOT, secret_root, provider_root, artifact_root):
        path.mkdir(parents=True, exist_ok=True)
        path.chmod(0o700)
    credential_key = ensure_secret(secret_root / "credential-encryption.key", lambda: Fernet.generate_key().decode())
    artifact_key = ensure_secret(secret_root / "artifact-signing.key", lambda: secrets.token_urlsafe(64))
    callback_key = ensure_secret(secret_root / "callback-signing.key", lambda: secrets.token_urlsafe(64))
    values = {
        "DATASET_LOOP_ENVIRONMENT": "staging",
        "DATASET_LOOP_AUTH_MODE": "owner_pct",
        "DATASET_LOOP_PUBLIC_BASE_URL": PUBLIC_BASE_URL,
        "DATASET_LOOP_DATABASE_PATH": str(STATE_ROOT / "control.sqlite3"),
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
        "DATASET_LOOP_OIDC_ISSUER": OAUTH_ISSUER,
        "DATASET_LOOP_OIDC_JWKS_URL": f"{OAUTH_ISSUER}/.well-known/jwks.json",
        "DATASET_LOOP_OIDC_CLIENT_ID": "dataset-loop-mcp",
        "DATASET_LOOP_OIDC_REVOCATION_ENDPOINT": f"{OAUTH_ISSUER}/revoke",
        "DATASET_LOOP_POLL_INTERVAL_SECONDS": "15",
        "DATASET_LOOP_SUPERVISOR_ENABLED": "true",
        "DATASET_LOOP_DEPLOYED_PORT": str(port),
        "PATH": f"{venv / 'bin'}:{os.environ.get('PATH', '')}",
    }
    atomic_private_text(
        STATE_ROOT / "service.env",
        "\n".join(f"{key}={value}" for key, value in sorted(values.items())) + "\n",
    )
    return values


def migrate(source: Path, venv: Path, env: dict[str, str]) -> None:
    run([str(venv / "bin/alembic"), "-c", "alembic.ini", "upgrade", "head"], cwd=source, env=env)


def install_service(source: Path, venv: Path, port: int) -> None:
    unit_root = Path.home() / ".config/systemd/user"
    unit_root.mkdir(parents=True, exist_ok=True)
    unit = unit_root / SERVICE_NAME
    unit.write_text(
        "\n".join(
            (
                "[Unit]",
                "Description=Dataset Loop MCP 0.1.0-alpha.3",
                "After=network-online.target",
                "Wants=network-online.target",
                "",
                "[Service]",
                "Type=simple",
                f"WorkingDirectory={source}",
                f"EnvironmentFile={STATE_ROOT / 'service.env'}",
                f"ExecStart={venv / 'bin/uvicorn'} dataset_loop_mcp.main:app --host 127.0.0.1 --port {port} --proxy-headers",
                "Restart=on-failure",
                "RestartSec=5",
                "NoNewPrivileges=true",
                "PrivateTmp=true",
                "UMask=0077",
                "",
                "[Install]",
                "WantedBy=default.target",
                "",
            )
        )
    )
    run(["systemctl", "--user", "daemon-reload"])
    run(["systemctl", "--user", "enable", "--now", SERVICE_NAME])
    wait_local_health(port)


def wait_local_health(port: int, timeout: int = 90) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    error = ""
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=5) as response:
                value = json.loads(response.read())
                if value.get("status") == "ok":
                    return value
        except Exception as exc:
            error = type(exc).__name__
        time.sleep(2)
    journal = run(
        ["journalctl", "--user", "-u", SERVICE_NAME, "--no-pager", "-n", "200"],
        check=False,
    ).stdout
    raise DeployError(f"local health did not become ready ({error})\n{safe_text(journal)}")


def bootstrap_owner(venv: Path, env: dict[str, str]) -> dict[str, Any]:
    if not OWNER_CONNECTION.exists():
        run(
            [
                str(venv / "bin/dataset-loop-mcp"),
                "bootstrap-owner",
                "--tenant",
                "owner",
                "--subject",
                "owner",
                "--output-file",
                str(OWNER_CONNECTION),
            ],
            env=env,
        )
    if not OWNER_CONNECTION.is_file() or OWNER_CONNECTION.is_symlink():
        raise DeployError("owner connection file is absent or unsafe")
    if stat.S_IMODE(OWNER_CONNECTION.stat().st_mode) != 0o600:
        raise DeployError("owner connection file is not mode 0600")
    value = json.loads(OWNER_CONNECTION.read_text())
    if value.get("mcp_url") != PUBLIC_MCP_URL:
        raise DeployError("owner connection contains the wrong MCP URL")
    if not str(value.get("authorization") or "").startswith("Bearer dlpct_"):
        raise DeployError("owner connection has no valid PCT")
    return value


def api_request(
    port: int,
    connection: dict[str, Any],
    method: str,
    path: str,
    payload: dict[str, Any] | None = None,
    *,
    authenticated: bool = True,
    public: bool = False,
    timeout: int = 90,
) -> Any:
    base = PUBLIC_BASE_URL if public else f"http://127.0.0.1:{port}"
    body = None if payload is None else json.dumps(payload).encode()
    headers = {"Accept": "application/json", "Content-Type": "application/json"}
    if authenticated:
        headers["Authorization"] = str(connection["authorization"])
    request = urllib.request.Request(base + path, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read()
    except urllib.error.HTTPError as exc:
        raw = exc.read(8192).decode(errors="replace")
        raise DeployError(f"API {method} {path} failed: HTTP {exc.code}: {safe_text(raw)}") from None
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise DeployError(f"API {method} {path} returned non-JSON") from exc


def enroll_kaggle(port: int, connection: dict[str, Any], credential: str) -> str:
    profiles = api_request(port, connection, "GET", "/v1/kaggle/profiles")
    if not isinstance(profiles, list):
        raise DeployError("Kaggle profile listing returned invalid data")
    active = [item for item in profiles if not item.get("revoked_at")]
    if active:
        return str(active[0]["id"])
    claim = api_request(
        port,
        connection,
        "POST",
        "/v1/kaggle/enrollment-claims",
        {"ttl_seconds": 600},
    )
    result = api_request(
        port,
        connection,
        "POST",
        "/v1/kaggle/profiles/enroll",
        {
            "claim_document": claim["claim_document"],
            "label": "devcoveer-managed",
            "provider_credential": credential,
        },
        authenticated=False,
    )
    if not isinstance(result, dict) or not result.get("id"):
        raise DeployError("Kaggle profile enrollment did not return an ID")
    return str(result["id"])


def run_schema(port: int, connection: dict[str, Any]) -> dict[str, Any]:
    document = api_request(port, connection, "GET", "/openapi.json", authenticated=False)
    schemas = document.get("components", {}).get("schemas", {})
    value = schemas.get("RunCreate")
    if not isinstance(value, dict):
        raise DeployError("OpenAPI RunCreate schema is absent")
    return value


def smoke_payload(schema: dict[str, Any], profile_id: str, minutes: int = 5) -> dict[str, Any]:
    properties = schema.get("properties", {})
    payload: dict[str, Any] = {
        "workload_type": "infrastructure_smoke",
        "duration_minutes": minutes,
    }
    for name in ("kaggle_profile_id", "profile_id"):
        if name in properties:
            payload[name] = profile_id
    missing = set(schema.get("required") or []) - set(payload)
    if missing:
        raise DeployError(f"infrastructure smoke has unresolved required fields: {sorted(missing)}")
    return payload


def start_run(port: int, connection: dict[str, Any], payload: dict[str, Any], *, public: bool = False) -> dict[str, Any]:
    value = api_request(port, connection, "POST", "/v1/runs", payload, public=public)
    if not isinstance(value, dict) or not value.get("id"):
        raise DeployError("start_run returned invalid data")
    return value


def wait_run(
    port: int,
    connection: dict[str, Any],
    run_id: str,
    *,
    public: bool = False,
    timeout: int = 1800,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    latest: dict[str, Any] = {}
    terminal = {"succeeded", "failed", "cancelled", "stale"}
    while time.monotonic() < deadline:
        value = api_request(port, connection, "GET", f"/v1/runs/{run_id}", public=public)
        if not isinstance(value, dict):
            raise DeployError("get_run returned invalid data")
        latest = value
        if value.get("state") in terminal:
            return value
        time.sleep(15)
    raise DeployError(f"run {run_id} timed out; last state={latest.get('state')!r}")


def safe_run(value: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value.get(key)
        for key in (
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
    }


def local_smoke(port: int, connection: dict[str, Any], profile_id: str) -> dict[str, Any]:
    schema = run_schema(port, connection)
    run_value = start_run(port, connection, smoke_payload(schema, profile_id))
    terminal = wait_run(port, connection, str(run_value["id"]), timeout=1800)
    if terminal.get("state") != "succeeded":
        raise DeployError(f"real Kaggle infrastructure smoke failed: {safe_run(terminal)}")
    artifacts = api_request(port, connection, "GET", f"/v1/runs/{terminal['id']}/artifacts")
    if not isinstance(artifacts, list) or not artifacts:
        raise DeployError("successful smoke produced no artifacts")
    if any(not item.get("verified_readback") for item in artifacts):
        raise DeployError("smoke artifact lacks verified readback")
    return {"run": safe_run(terminal), "artifact_count": len(artifacts)}


def parse_nginx() -> tuple[dict[Path, str], str]:
    raw = run(["sudo", "-n", "nginx", "-T"], timeout=60).stdout
    files: dict[Path, list[str]] = {}
    current: Path | None = None
    marker = re.compile(r"^# configuration file (.+):$")
    for line in raw.splitlines():
        match = marker.match(line)
        if match:
            current = Path(match.group(1)).resolve()
            files.setdefault(current, [])
        elif current is not None:
            files[current].append(line)
    return {path: "\n".join(lines) for path, lines in files.items()}, raw


def source_edge_patch() -> dict[str, Any]:
    candidates: list[tuple[int, Path]] = []
    for path in Path.cwd().rglob("*"):
        try:
            if not path.is_file() or path.is_symlink() or path.stat().st_size > 2_000_000:
                continue
            if ".git" in path.parts or "ops/dataset-loop-mcp" in path.as_posix():
                continue
            if path.suffix.casefold() not in {".py", ".sh", ".conf", ".j2", ".jinja", ".tmpl", ".yaml", ".yml", ".toml"}:
                continue
            text = path.read_text(errors="ignore")
        except OSError:
            continue
        if "mcp-datahub.kenigevents.ru" not in text or "identity.kenigevents.ru" not in text:
            continue
        score = sum(
            token in text
            for token in ("ssl_preread", "127.0.0.1:8444", "stream", "server_name")
        )
        candidates.append((score, path))
    candidates.sort(key=lambda item: (-item[0], len(str(item[1]))))
    if not candidates:
        raise DeployError("Git-tracked DevCoveer edge source was not found")
    _score, path = candidates[0]
    original = path.read_text()
    if "mcp-dataset-loop.kenigevents.ru" in original:
        return {"path": str(path), "changed": False}
    line_pattern = re.compile(
        r"(?m)^(?P<indent>\s*)(?P<quote>[\"']?)mcp-datahub\.kenigevents\.ru(?P=quote)(?P<tail>\s+[^;\n]+;[^\n]*)$"
    )
    match = line_pattern.search(original)
    if not match:
        raise DeployError(f"edge source has no safely duplicable SNI line: {path}")
    quote = match.group("quote")
    line = f"{match.group('indent')}{quote}mcp-dataset-loop.kenigevents.ru{quote}{match.group('tail')}"
    updated = original[: match.end()] + "\n" + line + original[match.end() :]
    path.write_text(updated)
    run(["git", "diff", "--check"])
    if path.suffix == ".py":
        run([sys.executable, "-m", "py_compile", str(path)])
    elif path.suffix == ".sh":
        run(["bash", "-n", str(path)])
    return {"path": str(path), "changed": True}


def existing_ipv4(hostname: str) -> str:
    values = sorted(
        {item[4][0] for item in socket.getaddrinfo(hostname, 443, type=socket.SOCK_STREAM) if ":" not in item[4][0]}
    )
    if len(values) != 1:
        raise DeployError(f"expected one IPv4 address for {hostname}, got {values}")
    return values[0]


def ensure_dns(ip: str) -> None:
    try:
        if existing_ipv4("mcp-dataset-loop.kenigevents.ru") == ip:
            return
    except (DeployError, OSError):
        pass
    if shutil.which("yc") is None:
        raise DeployError("yc CLI is unavailable for the required DNS record")
    zones = json.loads(run(["yc", "dns", "zone", "list", "--format", "json"]).stdout)
    matching = [item for item in zones if item.get("zone") == "kenigevents.ru."]
    if len(matching) != 1:
        raise DeployError("Yandex DNS zone kenigevents.ru. is unavailable or ambiguous")
    zone_id = str(matching[0]["id"])
    records = json.loads(
        run(["yc", "dns", "zone", "list-records", "--id", zone_id, "--format", "json"]).stdout
    )
    for item in records:
        if item.get("name") != "mcp-dataset-loop.kenigevents.ru.":
            continue
        record = f"{item['name']} {item['ttl']} {item['type']} {','.join(item.get('data') or [])}"
        run(["yc", "dns", "zone", "delete-records", "--id", zone_id, "--record", record])
    run(
        [
            "yc",
            "dns",
            "zone",
            "add-records",
            "--id",
            zone_id,
            "--record",
            f"mcp-dataset-loop 300 A {ip}",
        ]
    )


def ensure_certificate(fullchain: Path) -> None:
    san = run(["sudo", "-n", "openssl", "x509", "-in", str(fullchain), "-noout", "-ext", "subjectAltName"]).stdout
    if "DNS:mcp-dataset-loop.kenigevents.ru" in san or "DNS:*.kenigevents.ru" in san:
        return
    match = re.fullmatch(r"/etc/letsencrypt/live/([^/]+)/fullchain\.pem", str(fullchain))
    if not match:
        raise DeployError("active MCP certificate is not Certbot-managed and lacks the new SAN")
    cert_name = match.group(1)
    domains = sorted(set(re.findall(r"DNS:([^,\s]+)", san)) | {"mcp-dataset-loop.kenigevents.ru"})
    if "mcp-datahub.kenigevents.ru" not in domains or "identity.kenigevents.ru" not in domains:
        raise DeployError("active certificate lacks the existing MCP/OAuth identities")
    argv = ["sudo", "-n", "certbot", "certonly", "--cert-name", cert_name, "--expand", "--non-interactive"]
    for domain in domains:
        argv.extend(["-d", domain])
    run(argv, timeout=600)
    renewed = run(["sudo", "-n", "openssl", "x509", "-in", str(fullchain), "-noout", "-ext", "subjectAltName"]).stdout
    if "DNS:mcp-dataset-loop.kenigevents.ru" not in renewed:
        raise DeployError("Certbot completed without the Dataset Loop SAN")


def publish_ingress(port: int) -> dict[str, Any]:
    source = source_edge_patch()
    files, raw = parse_nginx()
    app_file: Path | None = None
    sni_file: Path | None = None
    fullchain: Path | None = None
    privkey: Path | None = None
    sni_match: re.Match[str] | None = None
    sni_pattern = re.compile(
        r"(?m)^(?P<indent>\s*)(?P<quote>[\"']?)mcp-datahub\.kenigevents\.ru(?P=quote)(?P<tail>\s+[^;\n]+;[^\n]*)$"
    )
    for path, text in files.items():
        if "server_name mcp-datahub.kenigevents.ru" in text and "ssl_certificate" in text:
            certificate = re.search(r"(?m)^\s*ssl_certificate\s+([^;]+);", text)
            key = re.search(r"(?m)^\s*ssl_certificate_key\s+([^;]+);", text)
            if certificate and key:
                app_file = path
                fullchain = Path(certificate.group(1).strip())
                privkey = Path(key.group(1).strip())
                break
    for path, text in files.items():
        match = sni_pattern.search(text)
        if match and ("ssl_preread" in raw or "stream {" in raw):
            sni_file = path
            sni_match = match
            break
    if not all((app_file, sni_file, fullchain, privkey, sni_match)):
        raise DeployError("active nginx application/SNI/certificate files could not be resolved")
    assert app_file and sni_file and fullchain and privkey and sni_match
    ensure_certificate(fullchain)
    ip = existing_ipv4("mcp-datahub.kenigevents.ru")
    ensure_dns(ip)

    backup = STATE_ROOT / "ingress-backups" / datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    backup.mkdir(parents=True, exist_ok=True)
    backup.chmod(0o700)
    run(["sudo", "-n", "cp", "--preserve=mode,ownership,timestamps", str(sni_file), str(backup / "sni.conf")])
    new_app = app_file.parent / "dataset-loop-mcp.conf"
    app_preexisted = run(["sudo", "-n", "test", "-e", str(new_app)], check=False).stdout == ""
    if app_preexisted:
        run(["sudo", "-n", "cp", "--preserve=mode,ownership,timestamps", str(new_app), str(backup / "app.conf")])

    def rollback() -> None:
        run(["sudo", "-n", "cp", str(backup / "sni.conf"), str(sni_file)], check=False)
        if (backup / "app.conf").exists():
            run(["sudo", "-n", "cp", str(backup / "app.conf"), str(new_app)], check=False)
        else:
            run(["sudo", "-n", "rm", "-f", str(new_app)], check=False)
        run(["sudo", "-n", "nginx", "-t"], check=False)
        run(["sudo", "-n", "systemctl", "reload", "nginx"], check=False)

    try:
        active_sni = sni_file.read_text()
    except PermissionError:
        active_sni = run(["sudo", "-n", "cat", str(sni_file)]).stdout
    if "mcp-dataset-loop.kenigevents.ru" not in active_sni:
        match = sni_pattern.search(active_sni)
        if not match:
            raise DeployError("active SNI entry disappeared before mutation")
        quote = match.group("quote")
        duplicate = f"{match.group('indent')}{quote}mcp-dataset-loop.kenigevents.ru{quote}{match.group('tail')}"
        active_sni = active_sni[: match.end()] + "\n" + duplicate + active_sni[match.end() :]
    temp_sni = Path(tempfile.mkstemp(prefix="dataset-loop-sni-", suffix=".conf")[1])
    temp_app = Path(tempfile.mkstemp(prefix="dataset-loop-app-", suffix=".conf")[1])
    try:
        temp_sni.write_text(active_sni)
        temp_app.write_text(
            f"""server {{
    listen 127.0.0.1:8444 ssl;
    server_name mcp-dataset-loop.kenigevents.ru;
    ssl_certificate {fullchain};
    ssl_certificate_key {privkey};
    client_max_body_size 1m;
    access_log /var/log/nginx/dataset-loop-mcp-access.log combined;
    error_log /var/log/nginx/dataset-loop-mcp-error.log warn;
    location / {{
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header Forwarded "";
        proxy_set_header X-Forwarded-For "";
        proxy_set_header X-Forwarded-Host "";
        proxy_set_header X-Forwarded-Port "";
        proxy_set_header X-Forwarded-Proto https;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_buffering off;
        proxy_request_buffering off;
        proxy_read_timeout 3600s;
        proxy_send_timeout 3600s;
        proxy_pass http://127.0.0.1:{port};
    }}
}}
"""
        )
        run(["sudo", "-n", "install", "-o", "root", "-g", "root", "-m", "0644", str(temp_sni), str(sni_file)])
        run(["sudo", "-n", "install", "-o", "root", "-g", "root", "-m", "0644", str(temp_app), str(new_app)])
        run(["sudo", "-n", "nginx", "-t"])
        run(["sudo", "-n", "systemctl", "reload", "nginx"])
    except Exception:
        rollback()
        raise
    finally:
        temp_sni.unlink(missing_ok=True)
        temp_app.unlink(missing_ok=True)
    return {
        "source": source,
        "devcoveer_ipv4": ip,
        "nginx_application_file": str(new_app),
        "nginx_sni_file": str(sni_file),
        "certificate_fullchain": str(fullchain),
        "backup_directory": str(backup),
    }


def public_mcp_probe(connection: dict[str, Any]) -> dict[str, Any]:
    body = json.dumps(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": {"name": "dataset-loop-deployment-verifier", "version": "1"},
            },
        }
    )
    config = STATE_ROOT / "owner-curl.conf"
    atomic_private_text(
        config,
        "\n".join(
            (
                "silent",
                "show-error",
                "fail-with-body",
                "location",
                f'header = "Authorization: {connection["authorization"]}"',
                'header = "Content-Type: application/json"',
                'header = "Accept: application/json, text/event-stream"',
                "",
            )
        ),
    )
    try:
        authenticated = run(
            [
                "curl",
                "--config",
                str(config),
                "--data-binary",
                body,
                "--write-out",
                "\n%{http_code}",
                PUBLIC_MCP_URL,
            ],
            timeout=120,
        ).stdout
    finally:
        config.unlink(missing_ok=True)
    if not authenticated.rstrip().endswith("200"):
        raise DeployError(f"public authenticated MCP initialize failed: {safe_text(authenticated)}")
    missing = run(
        [
            "curl",
            "--silent",
            "--show-error",
            "--location",
            "--header",
            "Content-Type: application/json",
            "--header",
            "Accept: application/json, text/event-stream",
            "--data-binary",
            body,
            "--output",
            "/dev/null",
            "--write-out",
            "%{http_code}",
            PUBLIC_MCP_URL,
        ],
        check=False,
    ).stdout.strip()
    if missing != "401":
        raise DeployError(f"public MCP missing bearer returned {missing}, expected 401")
    for url in EXISTING_MCP_URLS:
        run(["curl", "--silent", "--show-error", "--location", "--output", "/dev/null", "--write-out", "%{http_code}", url], check=False)
    run(["curl", "--fail", "--silent", "--show-error", f"{OAUTH_ISSUER}/.well-known/oauth-authorization-server", "--output", "/dev/null"])
    return {"authenticated_initialize": True, "missing_bearer_rejected": True}


def public_acceptance(port: int, connection: dict[str, Any], profile_id: str) -> dict[str, Any]:
    schema = run_schema(port, connection)
    payload = smoke_payload(schema, profile_id, minutes=8)
    starts: list[dict[str, Any]] = []
    errors: list[str] = []
    lock = threading.Lock()
    barrier = threading.Barrier(2)

    def launch() -> None:
        try:
            barrier.wait(timeout=10)
            value = start_run(port, connection, payload, public=True)
            with lock:
                starts.append(value)
        except Exception as exc:
            with lock:
                errors.append(safe_text(str(exc), 2000))

    threads = [threading.Thread(target=launch) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    if errors or len(starts) != 2:
        raise DeployError(f"two concurrent starts failed: {errors}")
    run_ids = [str(item["id"]) for item in starts]
    assigned: dict[str, dict[str, Any]] = {}
    deadline = time.monotonic() + 12 * 60
    while time.monotonic() < deadline:
        assigned = {
            run_id: api_request(port, connection, "GET", f"/v1/runs/{run_id}", public=True)
            for run_id in run_ids
        }
        if all(item.get("slot_id") and item.get("kernel_id") for item in assigned.values()):
            break
        if any(item.get("state") in {"failed", "cancelled", "stale"} for item in assigned.values()):
            raise DeployError(f"concurrent run failed before restart: {assigned}")
        time.sleep(10)
    slots = {str(item.get("slot_id")) for item in assigned.values()}
    if slots != {"s01", "s02"}:
        raise DeployError(f"stable slots were not isolated: {slots}")
    run(["systemctl", "--user", "restart", SERVICE_NAME])
    wait_local_health(port)
    terminal = {
        run_id: wait_run(port, connection, run_id, public=True, timeout=1800) for run_id in run_ids
    }
    if any(item.get("state") != "succeeded" for item in terminal.values()):
        raise DeployError(f"two-slot runs did not both succeed: {terminal}")
    if {str(item.get("slot_id")) for item in terminal.values()} != {"s01", "s02"}:
        raise DeployError("slot identity changed after restart")
    if len({str(item.get("manifest_sha256")) for item in terminal.values()}) != 2:
        raise DeployError("two runs crossed manifest identities")
    artifact_counts: dict[str, int] = {}
    artifact_ids: set[str] = set()
    for run_id in run_ids:
        artifacts = api_request(port, connection, "GET", f"/v1/runs/{run_id}/artifacts", public=True)
        if not isinstance(artifacts, list) or not artifacts:
            raise DeployError(f"run {run_id} has no artifacts")
        for item in artifacts:
            if not item.get("verified_readback"):
                raise DeployError("concurrent artifact lacks verified readback")
            artifact_id = str(item.get("id"))
            if artifact_id in artifact_ids:
                raise DeployError("artifact identity crossed runs")
            artifact_ids.add(artifact_id)
        artifact_counts[run_id] = len(artifacts)

    cancel_run = start_run(port, connection, smoke_payload(schema, profile_id, minutes=10), public=True)
    cancel_id = str(cancel_run["id"])
    deadline = time.monotonic() + 10 * 60
    current: dict[str, Any] = cancel_run
    while time.monotonic() < deadline:
        current = api_request(port, connection, "GET", f"/v1/runs/{cancel_id}", public=True)
        if current.get("provider_session_id"):
            break
        if current.get("state") in {"failed", "succeeded", "cancelled", "stale"}:
            raise DeployError(f"cancellation probe terminated too early: {current}")
        time.sleep(10)
    api_request(port, connection, "POST", f"/v1/runs/{cancel_id}/cancel", public=True)
    cancelled = wait_run(port, connection, cancel_id, public=True, timeout=900)
    if cancelled.get("state") != "cancelled":
        raise DeployError(f"cancel run ended as {cancelled.get('state')}")

    journal = run(
        ["journalctl", "--user", "-u", SERVICE_NAME, "--no-pager", "-n", "2000"],
        check=False,
    ).stdout
    token = str(connection["authorization"])
    if token in journal or token.removeprefix("Bearer ") in journal:
        raise DeployError("owner bearer leaked into the Dataset Loop journal")
    return {
        "concurrent_runs": [safe_run(item) for item in terminal.values()],
        "restart_during_runs_verified": True,
        "artifact_counts": artifact_counts,
        "cancellation_run": safe_run(cancelled),
        "owner_bearer_absent_from_journal": True,
    }


def encrypt_owner_connection(venv: Path) -> dict[str, Any]:
    if not PUBLIC_KEY.is_file() or PUBLIC_KEY.is_symlink():
        raise DeployError("owner handoff public key is absent or unsafe")
    envelope = RUNTIME_DIR / "owner-connection.envelope.json"
    envelope.parent.mkdir(parents=True, exist_ok=True)
    code = r'''
import base64, hashlib, json, os, pathlib, secrets
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
plaintext=pathlib.Path(os.environ["OWNER_CONNECTION"]).read_bytes()
public_path=pathlib.Path(os.environ["PUBLIC_KEY"])
public=serialization.load_pem_public_key(public_path.read_bytes())
key=secrets.token_bytes(32)
nonce=secrets.token_bytes(12)
aad=b"dataset-loop-mcp-owner-connection-v1"
ciphertext=AESGCM(key).encrypt(nonce,plaintext,aad)
wrapped=public.encrypt(key,padding.OAEP(mgf=padding.MGF1(algorithm=hashes.SHA256()),algorithm=hashes.SHA256(),label=None))
value={
 "schema_version":1,
 "algorithm":"RSA-OAEP-SHA256+AES-256-GCM",
 "aad":base64.b64encode(aad).decode(),
 "wrapped_key":base64.b64encode(wrapped).decode(),
 "nonce":base64.b64encode(nonce).decode(),
 "ciphertext":base64.b64encode(ciphertext).decode(),
 "public_key_sha256":hashlib.sha256(public_path.read_bytes()).hexdigest(),
 "contains_plaintext_token":False,
}
pathlib.Path(os.environ["ENVELOPE"]).write_text(json.dumps(value,indent=2,sort_keys=True)+"\n")
'''
    run(
        [str(venv / "bin/python"), "-c", code],
        env={
            "OWNER_CONNECTION": str(OWNER_CONNECTION),
            "PUBLIC_KEY": str(PUBLIC_KEY.resolve()),
            "ENVELOPE": str(envelope.resolve()),
        },
    )
    value = json.loads(envelope.read_text())
    return {
        "path": str(envelope),
        "algorithm": value["algorithm"],
        "public_key_sha256": value["public_key_sha256"],
        "sha256": sha256(envelope),
    }


def git_commit_success() -> str:
    run(["git", "diff", "--check"])
    run(
        [
            "git",
            "add",
            "ops/dataset-loop-mcp",
            "deploy/local-edge",
            "scripts",
        ],
        check=False,
    )
    staged = run(["git", "diff", "--cached", "--quiet"], check=False)
    if staged.command and subprocess.run(["git", "diff", "--cached", "--quiet"]).returncode != 0:
        run(["git", "config", "user.name", "github-actions[bot]"])
        run(["git", "config", "user.email", "41898282+github-actions[bot]@users.noreply.github.com"])
        run(["git", "commit", "-m", "ops: deploy Dataset Loop MCP on DevCoveer"])
        run(["git", "pull", "--rebase", "origin", "main"])
        run(["git", "push", "origin", "HEAD:main"])
    head = run(["git", "rev-parse", "HEAD"]).stdout.strip()
    remote = run(["git", "ls-remote", "origin", "refs/heads/main"]).stdout.split()[0]
    if head != remote:
        raise DeployError("my-data-hub main remote readback mismatch")
    return head


def main() -> int:
    started = utcnow()
    EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    failure_path = EVIDENCE_DIR / "deployment-failure.json"
    failure_path.unlink(missing_ok=True)
    try:
        probe = json.loads(Path("ops/dataset-loop-mcp/evidence/runner-probe.json").read_text())
        if probe.get("status") != "READY":
            raise DeployError(f"runner probe is not READY: {probe.get('missing')}")
        preferred = probe.get("selected_free_loopback_port")
        port_file = STATE_ROOT / "port"
        prior = int(port_file.read_text()) if port_file.is_file() else None
        port = choose_port(prior or (int(preferred) if preferred else None))
        STATE_ROOT.mkdir(parents=True, exist_ok=True)
        STATE_ROOT.chmod(0o700)
        atomic_private_text(port_file, f"{port}\n")

        metadata = release_metadata()
        with tempfile.TemporaryDirectory(prefix="dataset-loop-deploy-") as temporary:
            temp = Path(temporary)
            release = download_release(temp, metadata)
            source, venv = install_release(Path(release["directory"]), metadata)
            kaggle_owner, kaggle_credential = load_kaggle_credential()
            service_env = write_service_env(port, kaggle_owner, venv)
            migrate(source, venv, service_env)
            install_service(source, venv, port)
            connection = bootstrap_owner(venv, service_env)
            profile_id = enroll_kaggle(port, connection, kaggle_credential)
            kaggle_credential = ""
            atomic_private_text(STATE_ROOT / "active-kaggle-profile", profile_id + "\n")
            smoke = local_smoke(port, connection, profile_id)
            ingress = publish_ingress(port)
            public_probe = public_mcp_probe(connection)
            acceptance = public_acceptance(port, connection, profile_id)
            handoff = encrypt_owner_connection(venv)

        result = {
            "schema_version": 1,
            "status": "PUBLIC_INFRASTRUCTURE_READY",
            "started_at": started,
            "completed_at": utcnow(),
            "release": {
                "tag": RELEASE_TAG,
                "tag_object_sha": metadata["tag_object_sha"],
                "peeled_commit_sha": metadata["peeled_commit_sha"],
                "release_id": metadata["release_id"],
                "assets": release["assets"],
            },
            "connection": {
                "mcp_url": PUBLIC_MCP_URL,
                "owner_pct_present_on_host": True,
                "owner_pct_plaintext_in_repository": False,
                "encrypted_handoff": handoff,
            },
            "runtime": {
                "host": "DevCoveer",
                "loopback_port": port,
                "service_unit": SERVICE_NAME,
                "state_root": str(STATE_ROOT),
                "environment": "staging",
                "native_dataset_loop_enabled": False,
            },
            "kaggle": {
                "profile_id": profile_id,
                "owner": kaggle_owner,
                "stable_slots": ["s01", "s02"],
                "local_smoke": smoke,
                "public_acceptance": acceptance,
            },
            "ingress": ingress,
            "public_mcp_probe": public_probe,
            "existing_authorities_preserved": [*EXISTING_MCP_URLS, OAUTH_ISSUER],
        }
        evidence_path = EVIDENCE_DIR / "deployment-result.json"
        evidence_path.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
        completion = {
            "schema_version": 1,
            "status": result["status"],
            "mcp_url": PUBLIC_MCP_URL,
            "encrypted_owner_handoff": str(RUNTIME_DIR / "owner-connection.envelope.json"),
            "evidence": str(evidence_path),
            "release_tag": RELEASE_TAG,
        }
        Path("ops/dataset-loop-mcp/deployment.completed.json").write_text(
            json.dumps(completion, indent=2, sort_keys=True) + "\n"
        )
        main_sha = git_commit_success()
        result["my_data_hub_main_remote_readback_sha"] = main_sha
        evidence_path.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
        return 0
    except Exception as exc:
        failure = {
            "schema_version": 1,
            "status": "FAILED",
            "started_at": started,
            "failed_at": utcnow(),
            "error_type": type(exc).__name__,
            "error": safe_text(str(exc), 8_000),
            "public_mcp_url": PUBLIC_MCP_URL,
            "owner_pct_plaintext_in_repository": False,
            "native_dataset_loop_enabled": False,
        }
        failure_path.write_text(json.dumps(failure, indent=2, sort_keys=True) + "\n")
        print(json.dumps(failure, sort_keys=True))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
