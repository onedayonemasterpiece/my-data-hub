from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def write(path: str, source: str) -> None:
    (ROOT / path).write_text(source.rstrip() + "\n", encoding="utf-8")


def replace_once(source: str, old: str, new: str, label: str) -> str:
    if new in source:
        return source
    if source.count(old) != 1:
        raise RuntimeError(f"expected one {label}, found {source.count(old)}")
    return source.replace(old, new, 1)


def patch_deploy() -> None:
    path = "ops/dataset-loop-mcp/deploy.py"
    source = read(path)
    source = source.replace("import base64\n", "", 1)
    source = replace_once(
        source,
        '                f"ExecStart={venv / \'bin/uvicorn\'} dataset_loop_mcp.main:app --host 127.0.0.1 --port {port} --proxy-headers",\n',
        '                (\n'
        '                    f"ExecStart={venv / \'bin/uvicorn\'} dataset_loop_mcp.main:app "\n'
        '                    f"--host 127.0.0.1 --port {port} --proxy-headers"\n'
        '                ),\n',
        "systemd ExecStart wrapping",
    )
    source = replace_once(
        source,
        'def start_run(port: int, connection: dict[str, Any], payload: dict[str, Any], *, public: bool = False) -> dict[str, Any]:\n',
        'def start_run(\n'
        '    port: int,\n'
        '    connection: dict[str, Any],\n'
        '    payload: dict[str, Any],\n'
        '    *,\n'
        '    public: bool = False,\n'
        ') -> dict[str, Any]:\n',
        "start_run signature wrapping",
    )
    source = replace_once(
        source,
        'def source_edge_patch() -> dict[str, Any]:\n    candidates: list[tuple[int, Path]] = []\n    for path in Path.cwd().rglob("*"):\n',
        'def source_edge_patch() -> dict[str, Any]:\n'
        '    candidates: list[tuple[int, Path]] = []\n'
        '    allowed_suffixes = {\n'
        '        ".py",\n'
        '        ".sh",\n'
        '        ".conf",\n'
        '        ".j2",\n'
        '        ".jinja",\n'
        '        ".tmpl",\n'
        '        ".yaml",\n'
        '        ".yml",\n'
        '        ".toml",\n'
        '    }\n'
        '    for path in Path.cwd().rglob("*"):\n',
        "edge source suffix allowlist",
    )
    source = replace_once(
        source,
        '            if path.suffix.casefold() not in {".py", ".sh", ".conf", ".j2", ".jinja", ".tmpl", ".yaml", ".yml", ".toml"}:\n',
        '            if path.suffix.casefold() not in allowed_suffixes:\n',
        "edge source suffix check",
    )
    source = replace_once(
        source,
        '    for url in EXISTING_MCP_URLS:\n        run(["curl", "--silent", "--show-error", "--location", "--output", "/dev/null", "--write-out", "%{http_code}", url], check=False)\n    run(["curl", "--fail", "--silent", "--show-error", f"{OAUTH_ISSUER}/.well-known/oauth-authorization-server", "--output", "/dev/null"])\n',
        '    for url in EXISTING_MCP_URLS:\n'
        '        run(\n'
        '            [\n'
        '                "curl",\n'
        '                "--silent",\n'
        '                "--show-error",\n'
        '                "--location",\n'
        '                "--output",\n'
        '                "/dev/null",\n'
        '                "--write-out",\n'
        '                "%{http_code}",\n'
        '                url,\n'
        '            ],\n'
        '            check=False,\n'
        '        )\n'
        '    run(\n'
        '        [\n'
        '            "curl",\n'
        '            "--fail",\n'
        '            "--silent",\n'
        '            "--show-error",\n'
        '            f"{OAUTH_ISSUER}/.well-known/oauth-authorization-server",\n'
        '            "--output",\n'
        '            "/dev/null",\n'
        '        ]\n'
        '    )\n',
        "public regression curl wrapping",
    )
    write(path, source)


def patch_native_canary() -> None:
    path = "ops/dataset-loop-mcp/native_canary.py"
    source = read(path)
    source = source.replace("import sys\n", "", 1)
    source = source.replace("import tempfile\n", "", 1)
    if "from contextlib import suppress\n" not in source:
        source = source.replace(
            "import urllib.request\n",
            "import urllib.request\nfrom contextlib import suppress\n",
            1,
        )
    source = replace_once(
        source,
        'def run(argv: list[str], *, check: bool = True, timeout: int = 120, host_gh: bool = False) -> subprocess.CompletedProcess[str]:\n',
        'def run(\n'
        '    argv: list[str],\n'
        '    *,\n'
        '    check: bool = True,\n'
        '    timeout: int = 120,\n'
        '    host_gh: bool = False,\n'
        ') -> subprocess.CompletedProcess[str]:\n',
        "native run signature wrapping",
    )
    source = replace_once(
        source,
        '    headers = {"Accept": "application/json", "Content-Type": "application/json", "Authorization": str(conn["authorization"])}\n',
        '    headers = {\n'
        '        "Accept": "application/json",\n'
        '        "Content-Type": "application/json",\n'
        '        "Authorization": str(conn["authorization"]),\n'
        '    }\n',
        "native API headers wrapping",
    )
    source = replace_once(
        source,
        '            lower = path.name.casefold()\n            if (path.suffix.casefold() == ".pem" or ("github" in lower and ("app" in lower or "key" in lower))) and not mode & 0o077:\n                pems.append(path.resolve())\n            if path.suffix.casefold() in {".env", ".conf", ".ini", ".toml", ".yaml", ".yml"} and not mode & 0o004:\n                envs.append(path.resolve())\n',
        '            lower = path.name.casefold()\n'
        '            private_github_key = path.suffix.casefold() == ".pem" or (\n'
        '                "github" in lower and ("app" in lower or "key" in lower)\n'
        '            )\n'
        '            if private_github_key and not mode & 0o077:\n'
        '                pems.append(path.resolve())\n'
        '            private_config = path.suffix.casefold() in {\n'
        '                ".env",\n'
        '                ".conf",\n'
        '                ".ini",\n'
        '                ".toml",\n'
        '                ".yaml",\n'
        '                ".yml",\n'
        '            }\n'
        '            if private_config and not mode & 0o004:\n'
        '                envs.append(path.resolve())\n',
        "private file conditions wrapping",
    )
    source = replace_once(
        source,
        '    for path in envs:\n        try:\n            lines.extend(path.read_text(errors="ignore").splitlines())\n        except OSError:\n            pass\n',
        '    for path in envs:\n'
        '        with suppress(OSError):\n'
        '            lines.extend(path.read_text(errors="ignore").splitlines())\n',
        "private env read suppression",
    )
    source = replace_once(
        source,
        '            app_jwt = jwt.encode({"iat": now - 60, "exp": now + 480, "iss": str(app_id)}, private_key, algorithm="RS256")\n',
        '            app_jwt = jwt.encode(\n'
        '                {"iat": now - 60, "exp": now + 480, "iss": str(app_id)},\n'
        '                private_key,\n'
        '                algorithm="RS256",\n'
        '            )\n',
        "GitHub App JWT wrapping",
    )
    source = replace_once(
        source,
        '                repositories = {str(value.get("name")) for value in minted.get("repositories") or [] if isinstance(value, dict)}\n',
        '                repositories = {\n'
        '                    str(value.get("name"))\n'
        '                    for value in minted.get("repositories") or []\n'
        '                    if isinstance(value, dict)\n'
        '                }\n',
        "installation repository set wrapping",
    )
    source = replace_once(
        source,
        'def safe_run(value: dict[str, Any]) -> dict[str, Any]:\n    return {key: value.get(key) for key in ("id", "workload_type", "duration_minutes", "state", "provider_session_id", "slot_id", "kernel_id", "notebook_revision", "manifest_sha256", "lease_generation", "branch", "git_head", "checkpoint", "created_at", "updated_at")}\n',
        'def safe_run(value: dict[str, Any]) -> dict[str, Any]:\n'
        '    keys = (\n'
        '        "id",\n'
        '        "workload_type",\n'
        '        "duration_minutes",\n'
        '        "state",\n'
        '        "provider_session_id",\n'
        '        "slot_id",\n'
        '        "kernel_id",\n'
        '        "notebook_revision",\n'
        '        "manifest_sha256",\n'
        '        "lease_generation",\n'
        '        "branch",\n'
        '        "git_head",\n'
        '        "checkpoint",\n'
        '        "created_at",\n'
        '        "updated_at",\n'
        '    )\n'
        '    return {key: value.get(key) for key in keys}\n',
        "safe run projection wrapping",
    )
    source = replace_once(
        source,
        '        base_sha = run(["gh", "api", f"repos/{TARGET_REPOSITORY}/branches/main", "--jq", ".commit.sha"], host_gh=True).stdout.strip()\n',
        '        base_sha = run(\n'
        '            ["gh", "api", f"repos/{TARGET_REPOSITORY}/branches/main", "--jq", ".commit.sha"],\n'
        '            host_gh=True,\n'
        '        ).stdout.strip()\n',
        "base SHA readback wrapping",
    )
    source = replace_once(
        source,
        '        remote = run(["gh", "api", f"repos/{TARGET_REPOSITORY}/git/ref/heads/{branch}", "--jq", ".object.sha"], host_gh=True).stdout.strip()\n',
        '        remote = run(\n'
        '            ["gh", "api", f"repos/{TARGET_REPOSITORY}/git/ref/heads/{branch}", "--jq", ".object.sha"],\n'
        '            host_gh=True,\n'
        '        ).stdout.strip()\n',
        "run branch readback wrapping",
    )
    source = replace_once(
        source,
        '        if not isinstance(artifacts, list) or not artifacts or any(not item.get("verified_readback") for item in artifacts):\n',
        '        artifacts_invalid = (\n'
        '            not isinstance(artifacts, list)\n'
        '            or not artifacts\n'
        '            or any(not item.get("verified_readback") for item in artifacts)\n'
        '        )\n'
        '        if artifacts_invalid:\n',
        "artifact verification condition wrapping",
    )
    source = replace_once(
        source,
        '            text = re.sub(r"(?m)^DATASET_LOOP_NATIVE_DATASET_LOOP_ENABLED=.*$", "DATASET_LOOP_NATIVE_DATASET_LOOP_ENABLED=false", env_path.read_text())\n',
        '            text = re.sub(\n'
        '                r"(?m)^DATASET_LOOP_NATIVE_DATASET_LOOP_ENABLED=.*$",\n'
        '                "DATASET_LOOP_NATIVE_DATASET_LOOP_ENABLED=false",\n'
        '                env_path.read_text(),\n'
        '            )\n',
        "native disable fallback wrapping",
    )
    write(path, source)


def main() -> None:
    patch_deploy()
    patch_native_canary()
    print("Dataset Loop ops lint patch applied")


if __name__ == "__main__":
    main()
