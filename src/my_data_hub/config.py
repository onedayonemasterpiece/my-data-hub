from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


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

    @classmethod
    def from_env(cls, *, require_database: bool = True) -> "Settings":
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
        if self.orchestrator_interval_seconds < 1:
            raise ConfigurationError("orchestrator interval must be positive")
        if not 1 <= self.orchestrator_batch_size <= 500:
            raise ConfigurationError("orchestrator batch size must be between 1 and 500")
        if not 30 <= self.orchestrator_lease_seconds <= 86400:
            raise ConfigurationError("orchestrator lease must be between 30 and 86400 seconds")
        if self.mcp_auth_mode not in {"development-token", "oauth", "stdio-environment"}:
            raise ConfigurationError(f"unsupported MCP auth mode: {self.mcp_auth_mode}")
        if self.mcp_remote_enabled and self.mcp_auth_mode == "stdio-environment":
            raise ConfigurationError("remote MCP cannot use stdio-environment authentication")
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
        if self.environment in {"prod", "production"}:
            if self.mcp_auth_mode == "development-token":
                raise ConfigurationError(
                    "development-token MCP authentication is forbidden in production"
                )
            if self.mcp_remote_enabled and self.mcp_auth_mode != "oauth":
                raise ConfigurationError("production remote MCP requires OAuth")
            if not self.worker_result_token:
                raise ConfigurationError("worker result token is required in production")
        if self.production_publish_enabled and self.environment not in {"prod", "production"}:
            raise ConfigurationError("production publication may be enabled only in production")
