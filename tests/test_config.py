from __future__ import annotations

import os

import pytest

from my_data_hub.config import ConfigurationError, Settings

PREFIX = "MY_DATA_HUB_"


def clear_hub_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in list(os.environ):
        if name.startswith(PREFIX):
            monkeypatch.delenv(name, raising=False)


def test_defaults_are_fail_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    clear_hub_environment(monkeypatch)
    settings = Settings.from_env(require_database=False)
    assert settings.database_url == ""
    assert settings.scheduler_enabled is False
    assert settings.production_publish_enabled is False
    assert settings.mcp_remote_enabled is False
    assert settings.mcp_write_enabled is False
    assert settings.api_host == "127.0.0.1"


def test_database_is_required_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    clear_hub_environment(monkeypatch)
    with pytest.raises(ConfigurationError, match="DATABASE_URL"):
        Settings.from_env()


def test_invalid_boolean_is_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    clear_hub_environment(monkeypatch)
    monkeypatch.setenv("MY_DATA_HUB_SCHEDULER_ENABLED", "sometimes")
    with pytest.raises(ConfigurationError, match="boolean"):
        Settings.from_env(require_database=False)


def test_connector_credentials_must_be_distinct(monkeypatch: pytest.MonkeyPatch) -> None:
    clear_hub_environment(monkeypatch)
    monkeypatch.setenv(
        "MY_DATA_HUB_CONNECTOR_CREDENTIALS_JSON",
        '{"connector-a":"same-secret","connector-b":"same-secret"}',
    )
    with pytest.raises(ConfigurationError, match="distinct secret"):
        Settings.from_env(require_database=False)


def test_write_mode_requires_explicit_scope(monkeypatch: pytest.MonkeyPatch) -> None:
    clear_hub_environment(monkeypatch)
    monkeypatch.setenv("MY_DATA_HUB_MCP_WRITE_ENABLED", "true")
    monkeypatch.setenv("MY_DATA_HUB_MCP_SCOPES", "hub:read")
    with pytest.raises(ConfigurationError, match="write scope"):
        Settings.from_env(require_database=False)


def test_remote_stdio_auth_is_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    clear_hub_environment(monkeypatch)
    monkeypatch.setenv("MY_DATA_HUB_MCP_REMOTE_ENABLED", "true")
    with pytest.raises(ConfigurationError, match="remote MCP"):
        Settings.from_env(require_database=False)


def test_production_remote_requires_oauth_and_worker_token(monkeypatch: pytest.MonkeyPatch) -> None:
    clear_hub_environment(monkeypatch)
    monkeypatch.setenv("MY_DATA_HUB_ENVIRONMENT", "production")
    monkeypatch.setenv("MY_DATA_HUB_MCP_REMOTE_ENABLED", "true")
    monkeypatch.setenv("MY_DATA_HUB_MCP_AUTH_MODE", "development-token")
    monkeypatch.setenv("MY_DATA_HUB_MCP_DEVELOPMENT_TOKEN", "secret")
    with pytest.raises(ConfigurationError, match="forbidden in production"):
        Settings.from_env(require_database=False)

    monkeypatch.setenv("MY_DATA_HUB_MCP_AUTH_MODE", "oauth")
    monkeypatch.delenv("MY_DATA_HUB_MCP_DEVELOPMENT_TOKEN", raising=False)
    with pytest.raises(ConfigurationError, match="worker result token"):
        Settings.from_env(require_database=False)


def test_publication_can_only_be_enabled_in_production(monkeypatch: pytest.MonkeyPatch) -> None:
    clear_hub_environment(monkeypatch)
    monkeypatch.setenv("MY_DATA_HUB_PRODUCTION_PUBLISH_ENABLED", "true")
    with pytest.raises(ConfigurationError, match="only in production"):
        Settings.from_env(require_database=False)


def test_development_token_http_must_bind_loopback(monkeypatch: pytest.MonkeyPatch) -> None:
    clear_hub_environment(monkeypatch)
    monkeypatch.setenv("MY_DATA_HUB_MCP_REMOTE_ENABLED", "true")
    monkeypatch.setenv("MY_DATA_HUB_MCP_AUTH_MODE", "development-token")
    monkeypatch.setenv("MY_DATA_HUB_MCP_DEVELOPMENT_TOKEN", "secret")
    monkeypatch.setenv("MY_DATA_HUB_MCP_HOST", "0.0.0.0")
    with pytest.raises(ConfigurationError, match="loopback"):
        Settings.from_env(require_database=False)


def test_development_token_http_can_bind_loopback(monkeypatch: pytest.MonkeyPatch) -> None:
    clear_hub_environment(monkeypatch)
    monkeypatch.setenv("MY_DATA_HUB_MCP_REMOTE_ENABLED", "true")
    monkeypatch.setenv("MY_DATA_HUB_MCP_AUTH_MODE", "development-token")
    monkeypatch.setenv("MY_DATA_HUB_MCP_DEVELOPMENT_TOKEN", "secret")
    monkeypatch.setenv("MY_DATA_HUB_MCP_HOST", "127.0.0.1")
    settings = Settings.from_env(require_database=False)
    assert settings.mcp_remote_enabled is True


def test_production_oauth_requires_separate_revocation_database(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clear_hub_environment(monkeypatch)
    values = {
        "MY_DATA_HUB_DATABASE_URL": "postgresql://reader@db/hub",
        "MY_DATA_HUB_APPLICATION_DATABASE_URL": "postgresql://application@db/hub",
        "MY_DATA_HUB_CONNECTOR_INTAKE_DATABASE_URL": "postgresql://connector@db/hub",
        "MY_DATA_HUB_ORCHESTRATOR_DATABASE_URL": "postgresql://orchestrator@db/hub",
        "MY_DATA_HUB_ENVIRONMENT": "production",
        "MY_DATA_HUB_WORKER_RESULT_TOKEN": "worker-secret",
        "MY_DATA_HUB_MCP_REMOTE_ENABLED": "true",
        "MY_DATA_HUB_MCP_AUTH_MODE": "oauth",
        "MY_DATA_HUB_MCP_SCOPES": "hub:read,connector:read,provider:read",
        "MY_DATA_HUB_MCP_OAUTH_ISSUER": "https://identity.example",
        "MY_DATA_HUB_MCP_OAUTH_AUDIENCE": "https://mcp-datahub.kenigevents.ru/mcp",
        "MY_DATA_HUB_MCP_OAUTH_RESOURCE": "https://mcp-datahub.kenigevents.ru/mcp",
        "MY_DATA_HUB_MCP_OAUTH_JWKS_URL": "https://identity.example/jwks.json",
        "MY_DATA_HUB_MCP_ALLOWED_HOSTS": "mcp-datahub.kenigevents.ru",
    }
    for name, value in values.items():
        monkeypatch.setenv(name, value)
    with pytest.raises(ConfigurationError, match="REVOCATION_DATABASE_URL"):
        Settings.from_env()

    monkeypatch.setenv(
        "MY_DATA_HUB_MCP_REVOCATION_DATABASE_URL",
        "postgresql://authenticator@db/hub",
    )
    with pytest.raises(ConfigurationError, match="READER_DATABASE_URL"):
        Settings.from_env()
    monkeypatch.setenv(
        "MY_DATA_HUB_MCP_READER_DATABASE_URL",
        "postgresql://mcp_reader@db/hub",
    )
    settings = Settings.from_env()
    assert settings.mcp_revocation_database_url.endswith("@db/hub")
    assert settings.mcp_reader_database_url.endswith("@db/hub")
