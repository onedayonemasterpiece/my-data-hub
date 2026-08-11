from __future__ import annotations

import inspect
from dataclasses import dataclass
from typing import Any

from my_data_hub.auth.context import current_identity
from my_data_hub.auth.control import OAuthAuditEvent
from my_data_hub.auth.metadata import ProtectedResourceMetadata, protected_resource_metadata_url
from my_data_hub.config import ConfigurationError, Settings
from my_data_hub.mcp.catalog import DEFAULT_SECURITY_SCHEMES, TOOL_CONTRACTS, visible_tools
from my_data_hub.mcp.contracts import (
    ControlPlaneReader,
    MasterResolver,
    MasterSessionBroker,
    MCPAuditSink,
    WriteGate,
)
from my_data_hub.mcp.oauth import AccessIdentity, OAuthBearerValidator
from my_data_hub.mcp.service import HubService
from my_data_hub.mcp.sql_policy import BoundedSQLPolicy
from my_data_hub.mcp.transport import ToolSecurityMetadataMiddleware


def oauth_resource_metadata_url(resource: str) -> str:
    """Backward-compatible name for the RFC 9728 path-derived URL."""

    try:
        return protected_resource_metadata_url(resource)
    except ValueError as exc:
        raise ConfigurationError(str(exc)) from exc


@dataclass(frozen=True, slots=True)
class MCPDependencies:
    resolver: MasterResolver | None = None
    broker: MasterSessionBroker | None = None
    control: ControlPlaneReader | None = None
    write_gate: WriteGate | None = None
    audit: MCPAuditSink | None = None
    sql_policy: BoundedSQLPolicy | None = None


def _local_identity(settings: Settings) -> AccessIdentity | None:
    if settings.mcp_remote_enabled:
        return None
    return AccessIdentity(
        subject="local-stdio",
        client_id="my-data-hub-local",
        scopes=settings.mcp_scopes,
        audience="local-stdio",
        token_id="local-process",
        expires_at=2**63 - 1,
        issuer="local-process",
        issued_at=0,
        resource="local-stdio",
    )


def _auth_error(resource_metadata_url: str, *, insufficient_scope: bool = True):  # type: ignore[no-untyped-def]
    from mcp.types import CallToolResult, TextContent

    code = "insufficient_scope" if insufficient_scope else "invalid_token"
    description = "The authenticated identity is not authorized for this tool."
    challenge = (
        f'Bearer resource_metadata="{resource_metadata_url}", '
        f'error="{code}", error_description="{description}"'
    )
    return CallToolResult(
        content=[TextContent(type="text", text="Authentication or additional authorization is required.")],
        isError=True,
        _meta={"mcp/www_authenticate": [challenge]},
    )


def create_server(
    settings: Settings,
    *,
    dependencies: MCPDependencies | None = None,
    default_identity: AccessIdentity | None = None,
):  # type: ignore[no-untyped-def]
    try:
        from mcp.server import MCPServer
        from mcp.types import ToolAnnotations
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("install my-data-hub to run the MCP server") from exc

    deps = dependencies or MCPDependencies()
    fallback = default_identity or _local_identity(settings)
    metadata_url = (
        oauth_resource_metadata_url(settings.mcp_oauth_resource)
        if settings.mcp_oauth_resource
        else "https://invalid.example/.well-known/oauth-protected-resource"
    )

    class IdentityAwareMCPServer(MCPServer):  # type: ignore[misc]
        security_schemes = DEFAULT_SECURITY_SCHEMES

        def _identity(self) -> AccessIdentity | None:
            return current_identity() or fallback

        async def list_tools(self):  # type: ignore[no-untyped-def]
            tools = await super().list_tools()
            allowed = visible_tools(self._identity())
            return [tool for tool in tools if tool.name in allowed]

        async def call_tool(self, name, arguments, context=None):  # type: ignore[no-untyped-def]
            identity = self._identity()
            contract = TOOL_CONTRACTS.get(name)
            if contract is None or identity is None or contract.scope not in identity.scopes:
                if identity is not None and deps.audit is not None:
                    recorded = deps.audit.record_mcp_audit(
                        OAuthAuditEvent(
                            event="mcp_tool",
                            outcome="scope_denied",
                            issuer=identity.issuer,
                            client_id=identity.client_id,
                            subject=identity.subject,
                            token_id=identity.token_id,
                            tool=str(name),
                        )
                    )
                    if inspect.isawaitable(recorded):
                        await recorded
                return _auth_error(metadata_url, insufficient_scope=identity is not None)
            return await super().call_tool(name, arguments, context)

    service = HubService(
        deps.resolver,
        broker=deps.broker,
        control=deps.control,
        write_gate=deps.write_gate,
        audit=deps.audit,
        sql_policy=deps.sql_policy,
        fallback_identity=fallback,
    )
    mcp = IdentityAwareMCPServer(
        "my-data-hub",
        version="0.2.0",
        instructions=(
            "MCP 2026-07-28 bounded domain tools. The reader catalog contains no writes. "
            "Writes require an identity-bound preview and checkpoint lifecycle permit."
        ),
    )

    def register(name: str, function):  # type: ignore[no-untyped-def]
        contract = TOOL_CONTRACTS[name]
        annotations = ToolAnnotations(**contract.annotations())
        meta = {"securitySchemes": contract.security_schemes()}
        return mcp.tool(name=name, annotations=annotations, meta=meta, structured_output=True)(function)

    async def platform_status() -> dict[str, Any]:
        return await service.invoke("platform.status", {})

    async def master_status() -> dict[str, Any]:
        return await service.invoke("master.status", {})

    async def master_ensure() -> dict[str, Any]:
        return await service.invoke("master.ensure", {})

    async def operation_get(operation_id: str) -> dict[str, Any]:
        return await service.invoke("operation.get", {"operation_id": operation_id})

    async def checkpoint_status() -> dict[str, Any]:
        return await service.invoke("checkpoint.status", {})

    async def checkpoint_restore_request(
        target: str,
        checkpoint_id: str,
        exact_version_ref: str,
        timeout_seconds: int,
        idempotency_key: str,
    ) -> dict[str, Any]:
        return await service.invoke("checkpoint.restore.request", locals())

    async def master_rotation_request(
        checkpoint_id: str,
        exact_version_ref: str,
        expected_active_epoch: int,
        expected_canonical_revision: int,
        timeout_seconds: int,
        idempotency_key: str,
    ) -> dict[str, Any]:
        return await service.invoke("master.rotation.request", locals())

    async def connector_coverage() -> dict[str, Any]:
        return await service.invoke("connector.coverage", {})

    async def runtime_stale_epoch_probe(
        expected_active_epoch: int, submitted_epoch: int
    ) -> dict[str, Any]:
        return await service.invoke("runtime.stale_epoch.probe", locals())

    async def provider_protected_resource_probe(resource_ref: str) -> dict[str, Any]:
        return await service.invoke("provider.protected_resource.probe", locals())

    async def embedding_coverage() -> dict[str, Any]:
        return await service.invoke("embedding.coverage", {})

    async def embedding_production_capabilities() -> dict[str, Any]:
        return await service.invoke("embedding.production.capabilities", {})

    async def provider_status(limit: int = 100) -> dict[str, Any]:
        return await service.invoke("provider.resources.status", {"limit": limit})

    async def bloggers_list(cursor: str | None = None, limit: int = 50) -> dict[str, Any]:
        return await service.invoke("bloggers.list", {"cursor": cursor, "limit": limit})

    async def bloggers_get(blogger_id: str) -> dict[str, Any]:
        return await service.invoke("bloggers.get", {"blogger_id": blogger_id})

    async def bloggers_search(
        query: str,
        cursor: str | None = None,
        limit: int = 20,
        e5_query_vector: list[float] | None = None,
        bge_m3_query_vector: list[float] | None = None,
    ) -> dict[str, Any]:
        return await service.invoke(
            "bloggers.search",
            {
                "query": query,
                "cursor": cursor,
                "limit": limit,
                "e5_query_vector": e5_query_vector,
                "bge_m3_query_vector": bge_m3_query_vector,
            },
        )

    async def bloggers_provenance(blogger_id: str, limit: int = 50) -> dict[str, Any]:
        return await service.invoke(
            "bloggers.provenance", {"blogger_id": blogger_id, "limit": limit}
        )

    async def bloggers_statistics() -> dict[str, Any]:
        return await service.invoke("bloggers.statistics", {})

    async def bloggers_migration_accounting(export_batch_id: str) -> dict[str, Any]:
        return await service.invoke(
            "bloggers.migration.accounting", {"export_batch_id": export_batch_id}
        )

    async def data_query(
        sql: str,
        parameters: list[Any] | None = None,
        max_rows: int = 200,
        max_bytes: int = 262_144,
        timeout_ms: int = 5_000,
    ) -> dict[str, Any]:
        return await service.invoke(
            "data.query",
            {
                "sql": sql,
                "parameters": parameters or [],
                "max_rows": max_rows,
                "max_bytes": max_bytes,
                "timeout_ms": timeout_ms,
            },
        )

    async def data_change_preview(
        sql: str,
        parameters: list[Any],
        expected_revision: int,
        max_affected_rows: int,
        idempotency_key: str,
    ) -> dict[str, Any]:
        return await service.invoke("data.change.preview", locals())

    async def data_change_apply(
        sql: str,
        parameters: list[Any],
        expected_revision: int,
        max_affected_rows: int,
        idempotency_key: str,
        preview_receipt: str,
    ) -> dict[str, Any]:
        return await service.invoke("data.change.apply", locals())

    async def data_change_status(operation_id: str) -> dict[str, Any]:
        return await service.invoke("data.change.status", {"operation_id": operation_id})

    async def bloggers_import_preview(
        batch_id: str, expected_revision: int, idempotency_key: str
    ) -> dict[str, Any]:
        return await service.invoke("bloggers.import.preview", locals())

    async def bloggers_import_apply(
        batch_id: str, expected_revision: int, idempotency_key: str, preview_receipt: str
    ) -> dict[str, Any]:
        return await service.invoke("bloggers.import.apply", locals())

    def provider_function(tool: str):  # type: ignore[no-untyped-def]
        async def invoke_provider(
            resource_ref: str,
            control_class: str,
            private: bool,
            payload: dict[str, Any] | None = None,
        ) -> dict[str, Any]:
            return await service.invoke(
                tool,
                {
                    "resource_ref": resource_ref,
                    "control_class": control_class,
                    "private": private,
                    "payload": payload or {},
                },
            )

        return invoke_provider

    functions = {
        "platform.status": platform_status,
        "master.status": master_status,
        "master.ensure": master_ensure,
        "operation.get": operation_get,
        "checkpoint.status": checkpoint_status,
        "checkpoint.restore.request": checkpoint_restore_request,
        "master.rotation.request": master_rotation_request,
        "connector.coverage": connector_coverage,
        "runtime.stale_epoch.probe": runtime_stale_epoch_probe,
        "provider.protected_resource.probe": provider_protected_resource_probe,
        "embedding.coverage": embedding_coverage,
        "embedding.production.capabilities": embedding_production_capabilities,
        "provider.resources.status": provider_status,
        "bloggers.list": bloggers_list,
        "bloggers.get": bloggers_get,
        "bloggers.search": bloggers_search,
        "bloggers.provenance": bloggers_provenance,
        "bloggers.statistics": bloggers_statistics,
        "bloggers.migration.accounting": bloggers_migration_accounting,
        "data.query": data_query,
        "data.change.preview": data_change_preview,
        "data.change.apply": data_change_apply,
        "data.change.status": data_change_status,
        "bloggers.import.preview": bloggers_import_preview,
        "bloggers.import.apply": bloggers_import_apply,
    }
    for tool_name in TOOL_CONTRACTS:
        register(tool_name, functions.get(tool_name) or provider_function(tool_name))
    return mcp


def create_streamable_http_app(
    settings: Settings,
    *,
    dependencies: MCPDependencies,
    validator: OAuthBearerValidator,
):  # type: ignore[no-untyped-def]
    """Build the MCP 2026-07-28 stateless Streamable HTTP resource server."""

    from contextlib import asynccontextmanager
    from urllib.parse import urlsplit

    from mcp.server.transport_security import TransportSecuritySettings
    from starlette.applications import Starlette
    from starlette.responses import JSONResponse
    from starlette.routing import Mount, Route

    from my_data_hub.mcp.admission import AdmissionLimits, OAuthAdmissionSecurity

    server = create_server(settings, dependencies=dependencies)
    mcp_app = server.streamable_http_app(
        host=settings.mcp_host,
        streamable_http_path="/mcp",
        json_response=True,
        stateless_http=True,
        max_request_body_size=1_048_576,
        transport_security=TransportSecuritySettings(
            allowed_hosts=list(settings.mcp_allowed_hosts),
            allowed_origins=list(settings.mcp_allowed_origins),
        ),
    )
    resource_metadata = ProtectedResourceMetadata(
        resource=settings.mcp_oauth_resource,
        authorization_servers=(settings.mcp_oauth_issuer,),
        scopes_supported=frozenset(TOOL_CONTRACTS[name].scope for name in TOOL_CONTRACTS),
    )
    metadata_url = oauth_resource_metadata_url(settings.mcp_oauth_resource)
    metadata_path = urlsplit(metadata_url).path

    async def metadata(_request):  # type: ignore[no-untyped-def]
        return JSONResponse(resource_metadata.document(), headers={"Cache-Control": "no-store"})

    @asynccontextmanager
    async def lifespan(_app):  # type: ignore[no-untyped-def]
        async with server.session_manager.run():
            yield

    mounted = Starlette(
        routes=[
            Route(metadata_path, metadata, methods=["GET"]),
            Mount("/", app=ToolSecurityMetadataMiddleware(mcp_app)),
        ],
        lifespan=lifespan,
    )
    return OAuthAdmissionSecurity(
        mounted,
        validator=validator,
        required_scopes=frozenset(),
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


def serve(*, transport: str) -> None:
    if transport == "streamable-http":
        from my_data_hub.mcp.runtime import serve as serve_remote

        serve_remote()
        return
    settings = Settings.from_env()
    if transport != "stdio":
        raise ConfigurationError(f"unsupported MCP transport: {transport}")
    create_server(settings).run(transport="stdio")


def main() -> None:
    serve(transport="stdio")
