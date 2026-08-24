from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit


class ConfigurationError(RuntimeError):
    """Raised when required runtime configuration is absent or unsafe."""


def _csv(value: str | None) -> tuple[str, ...]:
    return tuple(part.strip() for part in (value or "").split(",") if part.strip())


def _bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ConfigurationError(f"{name} must be a boolean")


def _int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ConfigurationError(f"{name} must be an integer") from exc


def _secret_map(name: str) -> tuple[tuple[str, str], ...]:
    raw = os.getenv(name, "").strip()
    if not raw:
        return ()
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ConfigurationError(f"{name} must be a JSON object") from exc
    if not isinstance(value, dict) or not all(
        isinstance(key, str) and key and isinstance(secret, str) and secret
        for key, secret in value.items()
    ):
        raise ConfigurationError(f"{name} must map connector IDs to non-empty secrets")
    secrets = tuple(value.values())
    if len(secrets) != len(set(secrets)):
        raise ConfigurationError(f"{name} must assign a distinct secret to every connector")
    return tuple(sorted(value.items()))


@dataclass(frozen=True, slots=True)
class Settings:
    database_url: str
    environment: str
    instance_id: str
    log_level: str
    artifact_root: Path
    api_host: str
    api_port: int
    worker_result_token: str | None
    worker_result_max_bytes: int
    scheduler_enabled: bool
    production_publish_enabled: bool
    orchestrator_interval_seconds: int
    orchestrator_batch_size: int
    orchestrator_lease_seconds: int
    mcp_remote_enabled: bool
    mcp_write_enabled: bool
    mcp_host: str
    mcp_port: int
    mcp_allowed_origins: tuple[str, ...]
    mcp_allowed_hosts: tuple[str, ...]
    mcp_auth_mode: str
    mcp_development_token: str | None
    mcp_scopes: frozenset[str]
    connector_credentials: tuple[tuple[str, str], ...] = ()
    connector_intake_max_bytes: int = 2 * 1024 * 1024
    mcp_oauth_issuer: str = ""
    mcp_oauth_audience: str = ""
    mcp_oauth_resource: str = ""
    mcp_oauth_jwks_url: str = ""
    mcp_oauth_algorithms: tuple[str, ...] = ("RS256",)
    mcp_trusted_proxies: tuple[str, ...] = ()
    mcp_token_max_lifetime_seconds: int = 3600
    mcp_operator_profile_enabled: bool = False
    mcp_provider_profile_enabled: bool = False
    mcp_unified_bootstrap_profile_enabled: bool = False
    mcp_acceptance_scenarios_enabled: bool = False
    mcp_control_gateway_url: str = ""
    mcp_control_gateway_token_file: Path | None = None
    application_database_url: str = ""
    connector_intake_database_url: str = ""
    orchestrator_database_url: str = ""
    canonical_committer_database_url: str = ""
    google_youtube_enabled: bool = False
    google_youtube_model: str = "gemini-3.6-flash"
    google_youtube_allowed_models: tuple[str, ...] = (
        "gemini-3.6-flash",
        "gemini-3.7-flash",
    )
    google_youtube_timeout_seconds: int = 300
    google_youtube_max_response_bytes: int = 524_288
    google_youtube_max_output_tokens: int = 8192
    google_youtube_default_store: bool = False
    google_ai_limiter_supabase_url: str = ""
    google_ai_limiter_supabase_service_key: str = ""
    google_ai_normal_key_envs: tuple[str, ...] = ()

    @classmethod
    def from_env(cls, *, require_database: bool = True) -> Settings:
        database_url = os.getenv("MY_DATA_HUB_DATABASE_URL", "").strip()
        if require_database and not database_url:
            raise ConfigurationError("MY_DATA_HUB_DATABASE_URL is required")
        settings = cls(
            database_url=database_url,
            environment=os.getenv("MY_DATA_HUB_ENVIRONMENT", "development").strip().lower(),
            instance_id=os.getenv("MY_DATA_HUB_INSTANCE_ID", "local").strip(),
            log_level=os.getenv("MY_DATA_HUB_LOG_LEVEL", "INFO").strip().upper(),
            artifact_root=Path(
                os.getenv("MY_DATA_HUB_ARTIFACT_ROOT", "./artifacts")
            ).expanduser().resolve(),
            api_host=os.getenv("MY_DATA_HUB_API_HOST", "127.0.0.1").strip(),
            api_port=_int("MY_DATA_HUB_API_PORT", 8080),
            worker_result_token=os.getenv("MY_DATA_HUB_WORKER_RESULT_TOKEN") or None,
            worker_result_max_bytes=_int(
                "MY_DATA_HUB_WORKER_RESULT_MAX_BYTES", 4 * 1024 * 1024
            ),
            scheduler_enabled=_bool("MY_DATA_HUB_SCHEDULER_ENABLED", False),
            production_publish_enabled=_bool(
                "MY_DATA_HUB_PRODUCTION_PUBLISH_ENABLED", False
            ),
            orchestrator_interval_seconds=_int(
                "MY_DATA_HUB_ORCHESTRATOR_INTERVAL_SECONDS", 60
            ),
            orchestrator_batch_size=_int("MY_DATA_HUB_ORCHESTRATOR_BATCH_SIZE", 25),
            orchestrator_lease_seconds=_int(
                "MY_DATA_HUB_ORCHESTRATOR_LEASE_SECONDS", 1800
            ),
            mcp_remote_enabled=_bool("MY_DATA_HUB_MCP_REMOTE_ENABLED", False),
            mcp_write_enabled=_bool("MY_DATA_HUB_MCP_WRITE_ENABLED", False),
            mcp_host=os.getenv("MY_DATA_HUB_MCP_HOST", "127.0.0.1").strip(),
            mcp_port=_int("MY_DATA_HUB_MCP_PORT", 8765),
            mcp_allowed_origins=_csv(
                os.getenv(
                    "MY_DATA_HUB_MCP_ALLOWED_ORIGINS",
                    "http://127.0.0.1,http://localhost",
                )
            ),
            mcp_allowed_hosts=_csv(
                os.getenv("MY_DATA_HUB_MCP_ALLOWED_HOSTS", "127.0.0.1,localhost")
            ),
            mcp_auth_mode=os.getenv(
                "MY_DATA_HUB_MCP_AUTH_MODE", "stdio-environment"
            ).strip(),
            mcp_development_token=os.getenv("MY_DATA_HUB_MCP_DEVELOPMENT_TOKEN") or None,
            mcp_scopes=frozenset(
                _csv(
                    os.getenv(
                        "MY_DATA_HUB_MCP_SCOPES",
                        "platform:read,master:read,operation:read,checkpoint:read,"
                        "embedding:read,provider:read,bloggers:read,region-talk:read,data:read",
                    )
                )
            ),
            connector_credentials=_secret_map(
                "MY_DATA_HUB_CONNECTOR_CREDENTIALS_JSON"
            ),
            connector_intake_max_bytes=_int(
                "MY_DATA_HUB_CONNECTOR_INTAKE_MAX_BYTES", 2 * 1024 * 1024
            ),
            mcp_oauth_issuer=os.getenv("MY_DATA_HUB_MCP_OAUTH_ISSUER", "").strip(),
            mcp_oauth_audience=os.getenv("MY_DATA_HUB_MCP_OAUTH_AUDIENCE", "").strip(),
            mcp_oauth_resource=os.getenv("MY_DATA_HUB_MCP_OAUTH_RESOURCE", "").strip(),
            mcp_oauth_jwks_url=os.getenv("MY_DATA_HUB_MCP_OAUTH_JWKS_URL", "").strip(),
            mcp_oauth_algorithms=_csv(
                os.getenv("MY_DATA_HUB_MCP_OAUTH_ALGORITHMS", "RS256")
            ),
            mcp_trusted_proxies=_csv(
                os.getenv("MY_DATA_HUB_MCP_TRUSTED_PROXIES", "")
            ),
            mcp_token_max_lifetime_seconds=_int(
                "MY_DATA_HUB_MCP_TOKEN_MAX_LIFETIME_SECONDS", 3600
            ),
            mcp_operator_profile_enabled=_bool(
                "MY_DATA_HUB_MCP_OPERATOR_PROFILE_ENABLED", False
            ),
            mcp_provider_profile_enabled=_bool(
                "MY_DATA_HUB_MCP_PROVIDER_PROFILE_ENABLED", False
            ),
            mcp_unified_bootstrap_profile_enabled=_bool(
                "MY_DATA_HUB_MCP_UNIFIED_BOOTSTRAP_PROFILE_ENABLED", False
            ),
            mcp_acceptance_scenarios_enabled=_bool(
                "MY_DATA_HUB_MCP_ACCEPTANCE_SCENARIOS_ENABLED", False
            ),
            mcp_control_gateway_url=os.getenv(
                "MY_DATA_HUB_MCP_CONTROL_GATEWAY_URL", ""
            ).strip(),
            mcp_control_gateway_token_file=(
                Path(os.environ["MY_DATA_HUB_MCP_CONTROL_GATEWAY_TOKEN_FILE"]).expanduser()
                if os.getenv("MY_DATA_HUB_MCP_CONTROL_GATEWAY_TOKEN_FILE")
                else None
            ),
            application_database_url=os.getenv(
                "MY_DATA_HUB_APPLICATION_DATABASE_URL", ""
            ).strip(),
            connector_intake_database_url=os.getenv(
                "MY_DATA_HUB_CONNECTOR_INTAKE_DATABASE_URL", ""
            ).strip(),
            orchestrator_database_url=os.getenv(
                "MY_DATA_HUB_ORCHESTRATOR_DATABASE_URL", ""
            ).strip(),
            canonical_committer_database_url=os.getenv(
                "MY_DATA_HUB_CANONICAL_COMMITTER_DATABASE_URL", ""
            ).strip(),
            google_youtube_enabled=_bool("MY_DATA_HUB_GOOGLE_YOUTUBE_ENABLED", False),
            google_youtube_model=os.getenv(
                "MY_DATA_HUB_GOOGLE_YOUTUBE_MODEL", "gemini-3.6-flash"
            ).strip(),
            google_youtube_allowed_models=_csv(
                os.getenv(
                    "MY_DATA_HUB_GOOGLE_YOUTUBE_ALLOWED_MODELS",
                    "gemini-3.6-flash,gemini-3.7-flash",
                )
            ),
            google_youtube_timeout_seconds=_int(
                "MY_DATA_HUB_GOOGLE_YOUTUBE_TIMEOUT_SECONDS", 300
            ),
            google_youtube_max_response_bytes=_int(
                "MY_DATA_HUB_GOOGLE_YOUTUBE_MAX_RESPONSE_BYTES", 524_288
            ),
            google_youtube_max_output_tokens=_int(
                "MY_DATA_HUB_GOOGLE_YOUTUBE_MAX_OUTPUT_TOKENS", 8192
            ),
            google_youtube_default_store=_bool(
                "MY_DATA_HUB_GOOGLE_YOUTUBE_DEFAULT_STORE", False
            ),
            google_ai_limiter_supabase_url=os.getenv(
                "GOOGLE_AI_LIMITER_SUPABASE_URL", ""
            ).strip(),
            google_ai_limiter_supabase_service_key=os.getenv(
                "GOOGLE_AI_LIMITER_SUPABASE_SERVICE_KEY", ""
            ).strip(),
            google_ai_normal_key_envs=_csv(os.getenv("GOOGLE_AI_NORMAL_KEY_ENVS", "")),
        )
        settings.validate()
        return settings

    def validate(self) -> None:
        if self.environment not in {"development", "test", "staging", "prod", "production"}:
            raise ConfigurationError(f"unsupported environment: {self.environment}")
        if not self.instance_id:
            raise ConfigurationError("MY_DATA_HUB_INSTANCE_ID must not be empty")
        if not 1 <= self.api_port <= 65535:
            raise ConfigurationError("MY_DATA_HUB_API_PORT must be a valid TCP port")
        if not 1 <= self.mcp_port <= 65535:
            raise ConfigurationError("MY_DATA_HUB_MCP_PORT must be a valid TCP port")
        if not 1024 <= self.worker_result_max_bytes <= 64 * 1024 * 1024:
            raise ConfigurationError(
                "MY_DATA_HUB_WORKER_RESULT_MAX_BYTES must be between 1 KiB and 64 MiB"
            )
        if not 1024 <= self.connector_intake_max_bytes <= 16 * 1024 * 1024:
            raise ConfigurationError(
                "MY_DATA_HUB_CONNECTOR_INTAKE_MAX_BYTES must be between 1 KiB and 16 MiB"
            )
        if self.orchestrator_interval_seconds < 1:
            raise ConfigurationError("orchestrator interval must be positive")
        if not 1 <= self.orchestrator_batch_size <= 500:
            raise ConfigurationError("orchestrator batch size must be between 1 and 500")
        if not 30 <= self.orchestrator_lease_seconds <= 86400:
            raise ConfigurationError("orchestrator lease must be between 30 and 86400 seconds")
        if self.mcp_auth_mode not in {"development-token", "oauth", "stdio-environment"}:
            raise ConfigurationError(f"unsupported MCP auth mode: {self.mcp_auth_mode}")
        if self.environment in {"prod", "production"}:
            if self.mcp_auth_mode == "development-token":
                raise ConfigurationError(
                    "development-token MCP authentication is forbidden in production"
                )
            if self.mcp_remote_enabled and self.mcp_auth_mode != "oauth":
                raise ConfigurationError("production remote MCP requires OAuth")
            configured_runtime_urls = tuple(
                value
                for value in (
                    self.application_database_url,
                    self.connector_intake_database_url,
                    self.orchestrator_database_url,
                    self.canonical_committer_database_url,
                )
                if value
            )
            usernames = [urlsplit(value).username for value in configured_runtime_urls]
            if any(not username for username in usernames) or len(set(usernames)) != len(usernames):
                raise ConfigurationError(
                    "database URLs present in one production process require distinct login principals"
                )
        if self.mcp_remote_enabled and self.mcp_auth_mode == "stdio-environment":
            raise ConfigurationError("remote MCP cannot use stdio-environment authentication")
        provider_only_scopes = frozenset(
            {"platform:read", "provider:read", "provider:write"}
        )
        if self.mcp_provider_profile_enabled and (
            self.mcp_operator_profile_enabled
            or not self.mcp_write_enabled
            or self.mcp_scopes != provider_only_scopes
            or self.mcp_acceptance_scenarios_enabled
            or not self.mcp_control_gateway_url
            or self.mcp_control_gateway_token_file is None
        ):
            raise ConfigurationError(
                "provider-only MCP requires its exclusive profile and exactly "
                "platform:read, provider:read and provider:write through the single control gateway"
            )
        unified_bootstrap_scopes = frozenset(
            {
                "platform:read",
                "master:read",
                "operation:read",
                "checkpoint:read",
                "embedding:read",
                "provider:read",
                "provider:write",
                "bloggers:read",
                "region-talk:read",
            }
        )
        if self.mcp_unified_bootstrap_profile_enabled and (
            self.mcp_operator_profile_enabled
            or self.mcp_provider_profile_enabled
            or not self.mcp_write_enabled
            or self.mcp_scopes != unified_bootstrap_scopes
            or self.mcp_acceptance_scenarios_enabled
            or not self.mcp_control_gateway_url
            or self.mcp_control_gateway_token_file is None
        ):
            raise ConfigurationError(
                "unified bootstrap MCP requires its exclusive profile, the exact bounded-read "
                "plus provider scopes, and the single control gateway"
            )
        remote_read_scopes = {
            "platform:read",
            "master:read",
            "operation:read",
            "checkpoint:read",
            "embedding:read",
            "bloggers:read",
            "data:read",
            "hub:read",
            "orchestrator:read",
            "region-talk:read",
            "migration:read",
            "connector:read",
            "provider:read",
        }
        remote_write_scopes = {
            "master:ensure",
            "master:rotate",
            "recovery:request",
            "acceptance:probe",
            "acceptance:operate",
            "data:write",
            "migration:operate",
            "bloggers:write",
            "region-talk:operate",
            "provider:write",
            "youtube:analyze",
        }
        if self.mcp_remote_enabled and (
            (not self.mcp_write_enabled and not self.mcp_scopes <= remote_read_scopes)
            or (self.mcp_write_enabled and not self.mcp_scopes <= remote_read_scopes | remote_write_scopes)
        ):
            raise ConfigurationError(
                "remote MCP scopes exceed the selected reader or guarded owner/operator profile"
            )
        if self.mcp_remote_enabled and self.mcp_auth_mode == "oauth":
            oauth_values = {
                "MY_DATA_HUB_MCP_OAUTH_ISSUER": self.mcp_oauth_issuer,
                "MY_DATA_HUB_MCP_OAUTH_AUDIENCE": self.mcp_oauth_audience,
                "MY_DATA_HUB_MCP_OAUTH_RESOURCE": self.mcp_oauth_resource,
                "MY_DATA_HUB_MCP_OAUTH_JWKS_URL": self.mcp_oauth_jwks_url,
            }
            missing = sorted(name for name, value in oauth_values.items() if not value)
            if missing:
                raise ConfigurationError(
                    "production OAuth configuration is incomplete: " + ", ".join(missing)
                )
            if not self.mcp_oauth_jwks_url.startswith("https://"):
                raise ConfigurationError("OAuth JWKS URL must use HTTPS")
            issuer = urlsplit(self.mcp_oauth_issuer)
            if (
                issuer.scheme != "https"
                or not issuer.netloc
                or issuer.username is not None
                or issuer.password is not None
                or issuer.query
                or issuer.fragment
            ):
                raise ConfigurationError("OAuth issuer must be a canonical HTTPS URL")
            resource = urlsplit(self.mcp_oauth_resource)
            if (
                resource.scheme != "https"
                or not resource.netloc
                or resource.username is not None
                or resource.password is not None
                or resource.query
                or resource.fragment
            ):
                raise ConfigurationError(
                    "OAuth resource must be an HTTPS URL without credentials, query, or fragment"
                )
            if self.mcp_oauth_audience != self.mcp_oauth_resource:
                raise ConfigurationError("OAuth audience must equal the exact protected resource")
            resource_authority = resource.hostname or ""
            if resource.port not in {None, 443}:
                resource_authority = f"{resource_authority}:{resource.port}"
            allowed_authorities = {
                value.casefold().removesuffix(":443") for value in self.mcp_allowed_hosts
            }
            if resource_authority.casefold().removesuffix(":443") not in allowed_authorities:
                raise ConfigurationError(
                    "OAuth resource authority must be present in the MCP Host allowlist"
                )
            allowed_algorithms = {"RS256", "RS384", "RS512", "ES256", "ES384"}
            if not self.mcp_oauth_algorithms or not set(self.mcp_oauth_algorithms) <= allowed_algorithms:
                raise ConfigurationError("OAuth algorithms must be an asymmetric allowlist")
        if not 60 <= self.mcp_token_max_lifetime_seconds <= 86400:
            raise ConfigurationError("OAuth access token maximum lifetime must be 60..86400 seconds")
        if self.mcp_operator_profile_enabled and not self.mcp_write_enabled:
            raise ConfigurationError("database operator profile requires the MCP write gate")
        if self.mcp_acceptance_scenarios_enabled and (
            not self.mcp_write_enabled
            or "acceptance:operate" not in self.mcp_scopes
            or not self.mcp_control_gateway_url
            or self.mcp_control_gateway_token_file is None
        ):
            raise ConfigurationError(
                "acceptance scenarios require write opt-in, acceptance:operate and the single control gateway"
            )
        if self.mcp_control_gateway_url:
            gateway = urlsplit(self.mcp_control_gateway_url)
            if (
                gateway.scheme not in {"http", "https"}
                or not gateway.hostname
                or gateway.username is not None
                or gateway.password is not None
                or gateway.query
                or gateway.fragment
                or gateway.path != "/internal/mcp-provider/invoke"
            ):
                raise ConfigurationError("provider control gateway URL is not the exact internal endpoint")
        if self.mcp_auth_mode == "development-token" and not self.mcp_development_token:
            raise ConfigurationError("development-token auth requires a token")
        if (
            self.mcp_remote_enabled
            and self.mcp_auth_mode == "development-token"
            and self.mcp_host.lower() not in {"127.0.0.1", "localhost", "::1"}
        ):
            raise ConfigurationError(
                "development-token Streamable HTTP must bind to a loopback host; "
                "remote network access requires the OAuth/TLS profile"
            )
        if self.mcp_write_enabled and not {
            "data:write",
            "migration:operate",
            "bloggers:write",
            "region-talk:operate",
            "provider:write",
            "youtube:analyze",
        }.intersection(self.mcp_scopes):
            raise ConfigurationError("MCP write mode requires an explicit write scope")
        if self.production_publish_enabled and self.environment not in {"prod", "production"}:
            raise ConfigurationError("production publication may be enabled only in production")
        self._validate_google_youtube()

    def _validate_google_youtube(self) -> None:
        if not self.google_youtube_allowed_models or len(set(self.google_youtube_allowed_models)) != len(
            self.google_youtube_allowed_models
        ):
            raise ConfigurationError("YouTube model allowlist must be non-empty and unique")
        if self.google_youtube_model not in self.google_youtube_allowed_models:
            raise ConfigurationError("default YouTube model must be in the server allowlist")
        if not 10 <= self.google_youtube_timeout_seconds <= 300:
            raise ConfigurationError("YouTube timeout must be between 10 and 300 seconds")
        if not 65_536 <= self.google_youtube_max_response_bytes <= 2_097_152:
            raise ConfigurationError("YouTube response limit must be between 64 KiB and 2 MiB")
        if not 256 <= self.google_youtube_max_output_tokens <= 65_536:
            raise ConfigurationError("YouTube output-token cap must be between 256 and 65536")
        if self.google_youtube_default_store:
            raise ConfigurationError("the first YouTube release requires store=false")
        env_name = re.compile(r"^[A-Z][A-Z0-9_]{1,127}$")
        if any(not env_name.fullmatch(name) for name in self.google_ai_normal_key_envs):
            raise ConfigurationError("GOOGLE_AI_NORMAL_KEY_ENVS contains an invalid ENV variable name")
        if len(set(self.google_ai_normal_key_envs)) != len(self.google_ai_normal_key_envs):
            raise ConfigurationError("GOOGLE_AI_NORMAL_KEY_ENVS must not contain duplicates")
        dedicated = (
            bool(self.google_ai_limiter_supabase_url),
            bool(self.google_ai_limiter_supabase_service_key),
            bool(self.google_ai_normal_key_envs),
        )
        if any(dedicated) and not all(dedicated):
            raise ConfigurationError("dedicated Google AI limiter configuration is partial")
        if self.google_ai_limiter_supabase_url:
            limiter = urlsplit(self.google_ai_limiter_supabase_url)
            if (
                limiter.scheme != "https"
                or not limiter.hostname
                or limiter.username is not None
                or limiter.password is not None
                or limiter.query
                or limiter.fragment
            ):
                raise ConfigurationError("Google AI limiter URL must be a canonical HTTPS origin")
        if self.google_youtube_enabled and (
            not all(dedicated)
            or not self.mcp_remote_enabled
            or not self.mcp_write_enabled
            or not self.mcp_operator_profile_enabled
            or self.mcp_provider_profile_enabled
            or self.mcp_unified_bootstrap_profile_enabled
            or "youtube:analyze" not in self.mcp_scopes
        ):
            raise ConfigurationError(
                "YouTube analysis requires the exclusive remote operator profile, youtube:analyze, "
                "write opt-in, and the dedicated shared limiter"
            )
