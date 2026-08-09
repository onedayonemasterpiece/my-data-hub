from __future__ import annotations

import json
import os
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
                        "hub:read,orchestrator:read,region-talk:read,migration:read",
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
            if not self.worker_result_token:
                raise ConfigurationError("worker result token is required in production")
            if self.mcp_remote_enabled and self.mcp_auth_mode != "oauth":
                raise ConfigurationError("production remote MCP requires OAuth")
        if self.mcp_remote_enabled and self.mcp_auth_mode == "stdio-environment":
            raise ConfigurationError("remote MCP cannot use stdio-environment authentication")
        remote_read_scopes = {
            "hub:read",
            "orchestrator:read",
            "region-talk:read",
            "migration:read",
            "connector:read",
            "provider:read",
        }
        if self.mcp_remote_enabled and (
            self.mcp_write_enabled or not self.mcp_scopes <= remote_read_scopes
        ):
            raise ConfigurationError(
                "R1 remote MCP is semantic read-only; write/operator/provider mutation scopes are forbidden"
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
            allowed_algorithms = {"RS256", "RS384", "RS512", "ES256", "ES384"}
            if not self.mcp_oauth_algorithms or not set(self.mcp_oauth_algorithms) <= allowed_algorithms:
                raise ConfigurationError("OAuth algorithms must be an asymmetric allowlist")
        if not 60 <= self.mcp_token_max_lifetime_seconds <= 86400:
            raise ConfigurationError("OAuth access token maximum lifetime must be 60..86400 seconds")
        if self.mcp_operator_profile_enabled and not self.mcp_write_enabled:
            raise ConfigurationError("database operator profile requires the MCP write gate")
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
            "hub:write",
            "region-talk:write",
        }.intersection(self.mcp_scopes):
            raise ConfigurationError("MCP write mode requires an explicit write scope")
        if self.production_publish_enabled and self.environment not in {"prod", "production"}:
            raise ConfigurationError("production publication may be enabled only in production")
