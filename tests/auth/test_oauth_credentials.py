from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from my_data_hub.auth.oauth_credentials import (
    OAuthCredentialError,
    RotatingOAuthBearerSource,
    bearer_source_from_environment,
    validate_oauth_credential_file,
)
from scripts.provider.devstand_acceptance_controller import STATIC_MCP_BEARER_NAMES


def _credential_file(path: Path, *, profiles: tuple[str, ...] = ("reader",)) -> Path:
    payload = {
        "schema_version": "my-data-hub-mcp-oauth-credentials.v1",
        "token_endpoint": "https://identity.kenigevents.ru/token",
        "resource": "https://mcp-datahub.kenigevents.ru/mcp",
        "profiles": {
            profile: {
                "client_id": f"acceptance-{profile}",
                "refresh_token": f"refresh-{profile}-" + "r" * 32,
                "access_token": None,
                "access_expires_at": None,
            }
            for profile in profiles
        },
    }
    path.write_text(json.dumps(payload), encoding="utf-8")
    path.chmod(0o600)
    return path


def test_rotating_source_persists_successor_and_reuses_fresh_access(tmp_path: Path) -> None:
    path = _credential_file(tmp_path / "oauth.json")
    calls: list[dict[str, str]] = []

    def exchange(endpoint: str, parameters: dict[str, str]) -> dict[str, object]:
        assert endpoint == "https://identity.kenigevents.ru/token"
        calls.append(dict(parameters))
        return {
            "access_token": "access-successor-" + "a" * 32,
            "refresh_token": "refresh-successor-" + "b" * 32,
            "token_type": "Bearer",
            "expires_in": 300,
            "scope": "mcp:read",
        }

    source = RotatingOAuthBearerSource(path, now=lambda: 1_000.0, exchange=exchange)
    first = asyncio.run(source.token("reader"))
    second = asyncio.run(source.token("reader"))

    assert first == second == "access-successor-" + "a" * 32
    assert len(calls) == 1
    assert calls[0] == {
        "grant_type": "refresh_token",
        "refresh_token": "refresh-reader-" + "r" * 32,
        "client_id": "acceptance-reader",
        "resource": "https://mcp-datahub.kenigevents.ru/mcp",
    }
    stored = json.loads(path.read_text())
    assert stored["profiles"]["reader"]["refresh_token"] == "refresh-successor-" + "b" * 32
    assert stored["profiles"]["reader"]["access_expires_at"] == 1_300
    assert path.stat().st_mode & 0o777 == 0o600


def test_rotating_source_fails_closed_without_disclosing_credentials(tmp_path: Path) -> None:
    path = _credential_file(tmp_path / "oauth.json")
    original = path.read_text()

    def rejected(_endpoint: str, _parameters: dict[str, str]) -> dict[str, object]:
        raise RuntimeError("provider rejected refresh-reader-" + "r" * 32)

    source = RotatingOAuthBearerSource(path, now=lambda: 1_000.0, exchange=rejected)
    with pytest.raises(OAuthCredentialError) as raised:
        asyncio.run(source.token("reader"))
    assert "refresh-reader" not in str(raised.value)
    assert raised.value.__cause__ is None
    assert path.read_text() == original


def test_oauth_credential_file_requires_owner_mode_and_profiles(tmp_path: Path) -> None:
    path = _credential_file(tmp_path / "oauth.json")
    validate_oauth_credential_file(path, required_profiles=frozenset({"reader"}))
    with pytest.raises(OAuthCredentialError, match="lacks required profiles"):
        validate_oauth_credential_file(path, required_profiles=frozenset({"operator"}))
    path.chmod(0o640)
    with pytest.raises(OAuthCredentialError, match="mode-0600"):
        validate_oauth_credential_file(path, required_profiles=frozenset({"reader"}))


def test_environment_prefers_private_refresh_file_over_static_bearers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = _credential_file(tmp_path / "oauth.json")
    monkeypatch.setenv("MY_DATA_HUB_MCP_OAUTH_CREDENTIAL_FILE", str(path))
    source = bearer_source_from_environment({"reader": "static-" + "s" * 32})
    assert isinstance(source, RotatingOAuthBearerSource)


def test_github_workflow_never_materializes_refresh_credentials() -> None:
    workflow = Path(".github/workflows/provider-real.yml").read_text()
    assert "MY_DATA_HUB_MCP_OAUTH_CREDENTIALS_JSON" not in workflow
    assert "runs-on: [self-hosted, linux, my-data-hub-devstand]" in workflow
    assert "devstand_acceptance_controller.py preflight" in workflow
    assert "MY_DATA_HUB_MCP_OAUTH_CREDENTIAL_FILE:" not in workflow
    for static_name in (
        "MY_DATA_HUB_MCP_CANARY_TOKEN:",
        "MY_DATA_HUB_MCP_ACCEPTANCE_OPERATOR_TOKEN:",
        "MY_DATA_HUB_MCP_MIGRATION_OPERATOR_TOKEN:",
        "MY_DATA_HUB_MCP_PROVIDER_OPERATOR_TOKEN:",
        "MY_DATA_HUB_DATA_MCP_READER_TOKEN:",
        "MY_DATA_HUB_DATA_MCP_OPERATOR_TOKEN:",
    ):
        assert static_name not in workflow


def test_devstand_preflight_accepts_private_file_without_printing_tokens(tmp_path: Path) -> None:
    credential = _credential_file(
        tmp_path / "oauth.json", profiles=("reader", "operator", "provider")
    )
    environment = {
        **{key: value for key, value in os.environ.items() if key not in STATIC_MCP_BEARER_NAMES},
        "MY_DATA_HUB_MCP_OAUTH_CREDENTIAL_FILE": str(credential),
        "RUNNER_ENVIRONMENT": "self-hosted",
    }
    completed = subprocess.run(
        [sys.executable, "scripts/provider/devstand_acceptance_controller.py", "preflight"],
        check=False,
        capture_output=True,
        text=True,
        env=environment,
    )
    combined = completed.stdout + completed.stderr
    assert completed.returncode == 0
    assert combined.strip() == "DEVSTAND_OAUTH_PREFLIGHT_OK"
    assert "refresh-reader" not in combined
    assert "access_token" not in combined


def test_devstand_preflight_rejects_static_bearer_copy(tmp_path: Path) -> None:
    credential = _credential_file(
        tmp_path / "oauth.json", profiles=("reader", "operator", "provider")
    )
    environment = {
        **{key: value for key, value in os.environ.items() if key not in STATIC_MCP_BEARER_NAMES},
        "MY_DATA_HUB_MCP_OAUTH_CREDENTIAL_FILE": str(credential),
        "MY_DATA_HUB_MCP_CANARY_TOKEN": "static-" + "s" * 32,
        "RUNNER_ENVIRONMENT": "self-hosted",
    }
    completed = subprocess.run(
        [sys.executable, "scripts/provider/devstand_acceptance_controller.py", "preflight"],
        check=False,
        capture_output=True,
        text=True,
        env=environment,
    )
    combined = completed.stdout + completed.stderr
    assert completed.returncode == 78
    assert "DEVSTAND_STATIC_MCP_BEARER_FORBIDDEN" in combined
    assert "static-" not in combined


def test_devstand_preflight_rejects_github_hosted_runner(tmp_path: Path) -> None:
    credential = _credential_file(
        tmp_path / "oauth.json", profiles=("reader", "operator", "provider")
    )
    environment = {
        **{key: value for key, value in os.environ.items() if key not in STATIC_MCP_BEARER_NAMES},
        "MY_DATA_HUB_MCP_OAUTH_CREDENTIAL_FILE": str(credential),
        "RUNNER_ENVIRONMENT": "github-hosted",
    }
    completed = subprocess.run(
        [sys.executable, "scripts/provider/devstand_acceptance_controller.py", "preflight"],
        check=False,
        capture_output=True,
        text=True,
        env=environment,
    )
    assert completed.returncode == 78
    assert (completed.stdout + completed.stderr).strip() == "DEVSTAND_SELF_HOSTED_RUNNER_REQUIRED"


def test_devstand_preflight_rejects_credential_below_workspace(tmp_path: Path) -> None:
    (tmp_path / "workspace").mkdir()
    credential = _credential_file(
        tmp_path / "workspace" / "oauth.json", profiles=("reader", "operator", "provider")
    )
    environment = {
        **{key: value for key, value in os.environ.items() if key not in STATIC_MCP_BEARER_NAMES},
        "MY_DATA_HUB_MCP_OAUTH_CREDENTIAL_FILE": str(credential),
        "GITHUB_WORKSPACE": str(tmp_path / "workspace"),
        "RUNNER_ENVIRONMENT": "self-hosted",
    }
    completed = subprocess.run(
        [sys.executable, "scripts/provider/devstand_acceptance_controller.py", "preflight"],
        check=False,
        capture_output=True,
        text=True,
        env=environment,
    )
    assert completed.returncode == 78
    assert (completed.stdout + completed.stderr).strip() == (
        "DEVSTAND_OAUTH_CREDENTIAL_PATH_EPHEMERAL"
    )
