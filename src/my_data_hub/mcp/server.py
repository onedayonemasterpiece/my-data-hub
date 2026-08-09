from __future__ import annotations

from typing import Any
from urllib.parse import urlsplit, urlunsplit
from uuid import UUID

from my_data_hub.config import ConfigurationError, Settings
from my_data_hub.mcp.scopes import TOOL_SCOPES, require_scope
from my_data_hub.mcp.service import HubService


def oauth_resource_metadata_url(resource: str) -> str:
    """Return the RFC 9728 path-derived metadata URL for one exact resource."""

    parsed = urlsplit(resource)
    if parsed.scheme != "https" or not parsed.netloc or parsed.query or parsed.fragment:
        raise ConfigurationError("OAuth resource must be an HTTPS URL without query or fragment")
    suffix_path = parsed.path.rstrip("/")
    metadata_path = f"/.well-known/oauth-protected-resource{suffix_path}"
    return urlunsplit((parsed.scheme, parsed.netloc, metadata_path, "", ""))


def create_server(settings: Settings):  # type: ignore[no-untyped-def]
    try:
        from mcp.server import MCPServer
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("install my-data-hub to run the MCP server") from exc

    service = HubService(
        settings.database_url,
        scopes=settings.mcp_scopes,
        write_enabled=settings.mcp_write_enabled,
    )
    mcp = MCPServer(
        "my-data-hub",
        version="0.1.0",
        instructions=(
            "Use bounded domain tools only. Writes are disabled unless an explicit "
            "scope and server-side write gate are both enabled."
        ),
    )

    if TOOL_SCOPES["hub.health"] in settings.mcp_scopes:
        @mcp.tool(name="hub.health")
        def health() -> dict[str, Any]:
            """Return bounded canonical database health and revision."""
            require_scope(settings.mcp_scopes, TOOL_SCOPES["hub.health"])
            return service.health()

    if TOOL_SCOPES["hub.project.list"] in settings.mcp_scopes:
        @mcp.tool(name="hub.project.list")
        def project_list(limit: int = 50) -> list[dict[str, Any]]:
            """List projects, capped at 100 records."""
            require_scope(settings.mcp_scopes, TOOL_SCOPES["hub.project.list"])
            return service.list_projects(limit)

    if TOOL_SCOPES["hub.content.search"] in settings.mcp_scopes:
        @mcp.tool(name="hub.content.search")
        def content_search(query: str, limit: int = 20) -> list[dict[str, Any]]:
            """Search compact content using PostgreSQL Russian full-text search."""
            require_scope(settings.mcp_scopes, TOOL_SCOPES["hub.content.search"])
            return service.search_content(query, limit)

    if TOOL_SCOPES["hub.content.get"] in settings.mcp_scopes:
        @mcp.tool(name="hub.content.get")
        def content_get(content_id: str) -> dict[str, Any] | None:
            """Return one bounded content record by UUID."""
            require_scope(settings.mcp_scopes, TOOL_SCOPES["hub.content.get"])
            return service.get_content(UUID(content_id))

    if TOOL_SCOPES["hub.trace.get"] in settings.mcp_scopes:
        @mcp.tool(name="hub.trace.get")
        def trace_get(
            subject_type: str, subject_id: str, limit: int = 50
        ) -> list[dict[str, Any]]:
            """Return bounded provenance events for one exact subject."""
            require_scope(settings.mcp_scopes, TOOL_SCOPES["hub.trace.get"])
            return service.get_trace(subject_type, UUID(subject_id), limit)

    if TOOL_SCOPES["region_talk.queue.summary"] in settings.mcp_scopes:
        @mcp.tool(name="region_talk.queue.summary")
        def queue_summary() -> list[dict[str, Any]]:
            """Return bounded Region Talk queue counts and oldest timestamps."""
            require_scope(
                settings.mcp_scopes, TOOL_SCOPES["region_talk.queue.summary"]
            )
            return service.region_talk_queue_summary()

    if TOOL_SCOPES["region_talk.plan.preview"] in settings.mcp_scopes:
        @mcp.tool(name="region_talk.plan.preview")
        def plan_preview(max_actions: int = 8) -> dict[str, Any]:
            """Preview the pressure-aware plan without executing side effects."""
            require_scope(
                settings.mcp_scopes, TOOL_SCOPES["region_talk.plan.preview"]
            )
            return service.region_talk_plan(max_actions)

    if TOOL_SCOPES["region_talk.migration.status"] in settings.mcp_scopes:
        @mcp.tool(name="region_talk.migration.status")
        def migration_status(limit: int = 20) -> list[dict[str, Any]]:
            """Return bounded YDB migration batches and unexplained row counts."""
            require_scope(
                settings.mcp_scopes, TOOL_SCOPES["region_talk.migration.status"]
            )
            return service.migration_status(limit)

    if TOOL_SCOPES["region_talk.migration.accounting"] in settings.mcp_scopes:
        @mcp.tool(name="region_talk.migration.accounting")
        def migration_accounting(
            export_batch_id: str | None = None, limit: int = 200
        ) -> list[dict[str, Any]]:
            """Return row-kind accounting for one export batch or recent batches."""
            require_scope(
                settings.mcp_scopes,
                TOOL_SCOPES["region_talk.migration.accounting"],
            )
            parsed = UUID(export_batch_id) if export_batch_id else None
            return service.migration_accounting(parsed, limit)

    if TOOL_SCOPES["connector.status.list"] in settings.mcp_scopes:

        @mcp.tool(name="connector.status.list")
        def connector_status(limit: int = 50) -> list[dict[str, Any]]:
            """Return bounded connector and data-product delivery status."""
            require_scope(settings.mcp_scopes, TOOL_SCOPES["connector.status.list"])
            return service.connector_status(limit)

    if TOOL_SCOPES["provider.resource.status"] in settings.mcp_scopes:

        @mcp.tool(name="provider.resource.status")
        def provider_resource_status(limit: int = 100) -> list[dict[str, Any]]:
            """Return minimal Kaggle resource status without source or output."""
            require_scope(settings.mcp_scopes, TOOL_SCOPES["provider.resource.status"])
            return service.provider_resource_status(limit)

    if (
        settings.mcp_write_enabled
        and TOOL_SCOPES["region_talk.work.enqueue"] in settings.mcp_scopes
    ):
        @mcp.tool(name="region_talk.work.enqueue")
        def work_enqueue(
            stage: str,
            url: str | None = None,
            subject_id: str | None = None,
            subject_type: str = "content_url",
            priority: int = 100,
            dedupe_key: str | None = None,
            dry_run: bool = True,
        ) -> dict[str, Any]:
            """Enqueue one bounded Region Talk task; dry-run defaults to true."""
            require_scope(
                settings.mcp_scopes, TOOL_SCOPES["region_talk.work.enqueue"]
            )
            return service.enqueue_region_talk_work(
                stage=stage,
                url=url,
                subject_id=UUID(subject_id) if subject_id else None,
                subject_type=subject_type,
                priority=priority,
                dedupe_key=dedupe_key,
                dry_run=dry_run,
            )

    if (
        settings.mcp_write_enabled
        and TOOL_SCOPES["hub.command.submit"] in settings.mcp_scopes
    ):
        @mcp.tool(name="hub.command.submit")
        def command_submit(command: dict[str, Any]) -> dict[str, Any]:
            """Submit a typed idempotent command; arbitrary SQL is not accepted."""
            require_scope(settings.mcp_scopes, TOOL_SCOPES["hub.command.submit"])
            return service.submit_command(command)

    return mcp


def serve(*, transport: str) -> None:
    settings = Settings.from_env()
    if transport not in {"stdio", "streamable-http"}:
        raise ConfigurationError(f"unsupported MCP transport: {transport}")
    mcp = create_server(settings)
    if transport == "stdio":
        mcp.run(transport="stdio")
        return

    if not settings.mcp_remote_enabled:
        raise ConfigurationError("remote MCP is disabled by configuration")
    try:
        import uvicorn
        from mcp.server.transport_security import TransportSecuritySettings
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("MCP HTTP dependencies are required for Streamable HTTP") from exc
    from contextlib import asynccontextmanager

    from starlette.applications import Starlette
    from starlette.responses import JSONResponse
    from starlette.routing import Mount, Route

    from my_data_hub.mcp.admission import AdmissionLimits, OAuthAdmissionSecurity
    from my_data_hub.mcp.http_security import DevelopmentBearerSecurity
    from my_data_hub.mcp.oauth import OAuthBearerValidator, OAuthValidationPolicy
    from my_data_hub.mcp.oauth_jwt import JwksJwtDecoder
    from my_data_hub.mcp.oauth_postgres import PostgresRevocationStore

    allowed_hosts = sorted(
        {
            entry
            for host in settings.mcp_allowed_hosts
            for entry in (host, host if host.endswith(":*") else f"{host}:*")
        }
    )
    mcp_app = mcp.streamable_http_app(
        host=settings.mcp_host,
        max_request_body_size=1_048_576,
        transport_security=TransportSecuritySettings(
            allowed_hosts=allowed_hosts,
            allowed_origins=list(settings.mcp_allowed_origins),
        ),
    )

    @asynccontextmanager
    async def lifespan(_app: Starlette):  # type: ignore[no-untyped-def]
        async with mcp.session_manager.run():
            yield

    if settings.mcp_auth_mode == "oauth":
        metadata_url = oauth_resource_metadata_url(settings.mcp_oauth_resource)
        metadata_path = urlsplit(metadata_url).path

        async def protected_resource_metadata(_request: Any) -> JSONResponse:
            return JSONResponse(
                {
                    "resource": settings.mcp_oauth_resource,
                    "authorization_servers": [settings.mcp_oauth_issuer],
                    "bearer_methods_supported": ["header"],
                    "scopes_supported": sorted(settings.mcp_scopes),
                    "resource_name": "my-data-hub read-only MCP",
                }
            )

        mounted = Starlette(
            routes=[
                Route(metadata_path, protected_resource_metadata, methods=["GET"]),
                Mount("/", app=mcp_app),
            ],
            lifespan=lifespan,
        )
        decoder = JwksJwtDecoder(
            jwks_url=settings.mcp_oauth_jwks_url,
            issuer=settings.mcp_oauth_issuer,
            audience=settings.mcp_oauth_audience,
            algorithms=settings.mcp_oauth_algorithms,
        )
        validator = OAuthBearerValidator(
            decoder=decoder,
            policy=OAuthValidationPolicy(
                issuer=settings.mcp_oauth_issuer,
                audience=settings.mcp_oauth_audience,
                resource=settings.mcp_oauth_resource,
                allowed_scopes=settings.mcp_scopes,
                max_token_lifetime_seconds=settings.mcp_token_max_lifetime_seconds,
            ),
            revocations=PostgresRevocationStore(settings.database_url),
        )
        guarded = OAuthAdmissionSecurity(
            mounted,
            validator=validator,
            required_scopes=settings.mcp_scopes,
            allowed_origins=settings.mcp_allowed_origins,
            allowed_hosts=settings.mcp_allowed_hosts,
            trusted_proxy_ips=settings.mcp_trusted_proxies,
            limits=AdmissionLimits(
                max_request_bytes=1_048_576,
                max_response_bytes=2_097_152,
                max_concurrency=16,
                requests_per_window=120,
                rate_window_seconds=60,
                request_timeout_seconds=30,
            ),
            metadata_path=metadata_path,
            resource_metadata_url=metadata_url,
        )
    elif settings.mcp_auth_mode == "development-token" and settings.mcp_development_token:
        mounted = Starlette(routes=[Mount("/", app=mcp_app)], lifespan=lifespan)
        guarded = DevelopmentBearerSecurity(
            mounted,
            token=settings.mcp_development_token,
            allowed_origins=settings.mcp_allowed_origins,
            allowed_hosts=settings.mcp_allowed_hosts,
            max_request_bytes=1_048_576,
        )
    else:
        raise ConfigurationError("Streamable HTTP requires OAuth or a loopback development token")
    uvicorn.run(
        guarded,
        host=settings.mcp_host,
        port=settings.mcp_port,
        proxy_headers=False,
        server_header=False,
    )


def main() -> None:
    serve(transport="stdio")
