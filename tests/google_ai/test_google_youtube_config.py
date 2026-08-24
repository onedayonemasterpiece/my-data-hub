from __future__ import annotations

from dataclasses import replace

import pytest

from my_data_hub.config import ConfigurationError, Settings

GOOGLE_ENV_NAMES = {
    "MY_DATA_HUB_GOOGLE_YOUTUBE_ENABLED",
    "MY_DATA_HUB_GOOGLE_YOUTUBE_MODEL",
    "MY_DATA_HUB_GOOGLE_YOUTUBE_ALLOWED_MODELS",
    "MY_DATA_HUB_GOOGLE_YOUTUBE_TIMEOUT_SECONDS",
    "MY_DATA_HUB_GOOGLE_YOUTUBE_MAX_RESPONSE_BYTES",
    "MY_DATA_HUB_GOOGLE_YOUTUBE_MAX_OUTPUT_TOKENS",
    "MY_DATA_HUB_GOOGLE_YOUTUBE_DEFAULT_STORE",
    "GOOGLE_AI_LIMITER_SUPABASE_URL",
    "GOOGLE_AI_LIMITER_SUPABASE_SERVICE_KEY",
    "GOOGLE_AI_NORMAL_KEY_ENVS",
    "SUPABASE_URL",
    "SUPABASE_SERVICE_KEY",
}


def clear(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in GOOGLE_ENV_NAMES:
        monkeypatch.delenv(name, raising=False)


def test_feature_is_disabled_by_default_and_generic_supabase_is_not_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    clear(monkeypatch)
    monkeypatch.setenv("SUPABASE_URL", "https://wrong.example")
    monkeypatch.setenv("SUPABASE_SERVICE_KEY", "wrong-secret")
    settings = Settings.from_env(require_database=False)
    assert settings.google_youtube_enabled is False
    assert settings.google_ai_limiter_supabase_url == ""
    assert settings.google_ai_limiter_supabase_service_key == ""
    assert settings.google_ai_normal_key_envs == ()


@pytest.mark.parametrize(
    "name",
    [
        "GOOGLE_AI_LIMITER_SUPABASE_URL",
        "GOOGLE_AI_LIMITER_SUPABASE_SERVICE_KEY",
        "GOOGLE_AI_NORMAL_KEY_ENVS",
    ],
)
def test_partial_dedicated_limiter_configuration_is_invalid(
    monkeypatch: pytest.MonkeyPatch,
    name: str,
) -> None:
    clear(monkeypatch)
    value = "https://quota.example.supabase.co" if name.endswith("URL") else "present"
    if name == "GOOGLE_AI_NORMAL_KEY_ENVS":
        value = "GOOGLE_KEY_A"
    monkeypatch.setenv(name, value)
    with pytest.raises(ConfigurationError, match="partial"):
        Settings.from_env(require_database=False)


def test_feature_requires_exact_remote_operator_scope_and_store_false(monkeypatch: pytest.MonkeyPatch) -> None:
    clear(monkeypatch)
    base = Settings.from_env(require_database=False)
    configured = replace(
        base,
        google_youtube_enabled=True,
        google_ai_limiter_supabase_url="https://quota.example.supabase.co",
        google_ai_limiter_supabase_service_key="service-secret",
        google_ai_normal_key_envs=("GOOGLE_KEY_A",),
        mcp_remote_enabled=True,
        mcp_write_enabled=True,
        mcp_operator_profile_enabled=True,
        mcp_auth_mode="oauth",
        mcp_scopes=frozenset({"youtube:analyze"}),
        mcp_oauth_issuer="https://identity.example",
        mcp_oauth_audience="https://mcp.example/mcp",
        mcp_oauth_resource="https://mcp.example/mcp",
        mcp_oauth_jwks_url="https://identity.example/.well-known/jwks.json",
        mcp_allowed_hosts=("mcp.example",),
    )
    configured.validate()
    with pytest.raises(ConfigurationError, match="store=false"):
        replace(configured, google_youtube_default_store=True).validate()
    with pytest.raises(ConfigurationError, match="youtube:analyze"):
        replace(configured, mcp_scopes=frozenset({"data:write"})).validate()
