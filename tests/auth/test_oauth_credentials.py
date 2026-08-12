from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

from my_data_hub.auth.oauth_credentials import (
    OAuthCredentialError,
    RotatingOAuthBearerSource,
    bearer_source_from_environment,
    validate_oauth_credential_file,
)


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
    assert "MY_DATA_HUB_MCP_OAUTH_CREDENTIAL_FILE" not in workflow
