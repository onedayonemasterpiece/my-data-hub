from __future__ import annotations

import inspect
from contextlib import asynccontextmanager
from dataclasses import dataclass
from types import MethodType
from typing import Annotated, Any, Literal
from urllib.parse import urlsplit

from pydantic import Field

from my_data_hub.auth.control import OAuthAuditEvent
from my_data_hub.auth.metadata import ProtectedResourceMetadata
from my_data_hub.config import Settings
from my_data_hub.google_ai.contracts import YouTubeVideoAnalyzer
from my_data_hub.mcp.server import (
    MCPDependencies,
    _auth_error,
    _configured_security_schemes,
    _local_identity,
    _profile_tool_names,
    oauth_resource_metadata_url,
)
from my_data_hub.mcp.server import create_server as create_base_server
from my_data_hub.mcp.youtube_catalog import (
    TOOL_CONTRACTS,
    YOUTUBE_SCOPE,
    YOUTUBE_TOOL_CONTRACT,
    YOUTUBE_TOOL_NAME,
)
from my_data_hub.mcp.youtube_service import YouTubeHubService

YouTubeURL = Annotated[str, Field(min_length=1, max_length=2048)]
Question = Annotated[str | None, Field(min_length=1, max_length=4000)]
CustomPrompt = Annotated[str | None, Field(min_length=1, max_length=8000)]
Language = Annotated[
    str,
    Field(pattern=r"^[A-Za-z]{2,8}(?:-[A-Za-z0-9]{1,8})?$"),
]
ModelName = Annotated[str | None, Field(min_length=1, max_length=128)]
OutputTokens = Annotated[int, Field(ge=256, le=65536)]
IdempotencyKey = Annotated[
    str,
    Field(min_length=8, max_length=200, pattern=r"^[A-Za-z0-9._:-]+$"),
]


@dataclass(frozen=True, slots=True)
class YouTubeMCPDependencies:
    base: MCPDependencies
    analyzer: YouTubeVideoAnalyzer | None = None
    feature_enabled: bool = False


def _youtube_exposed(settings: Settings, dependencies: YouTubeMCPDependencies) -> bool:
    return bool(
        dependencies.feature_enabled
        and dependencies.analyzer is not None
        and settings.mcp_operator_profile_enabled
        and not dependencies.base.reader_profile_enabled
        and not dependencies.base.provider_only_profile_enabled
        and not dependencies.base.unified_bootstrap_profile_enabled
        and YOUTUBE_SCOPE in settings.mcp_scopes
    )


def _metadata_tool_names(
    settings: Settings,
    dependencies: YouTubeMCPDependencies,
) -> set[str]:
    names = _profile_tool_names(dependencies.base)
    if _youtube_exposed(settings, dependencies):
        names.add(YOUTUBE_TOOL_NAME)
    return names


def _security_schemes(
    settings: Settings,
    dependencies: YouTubeMCPDependencies,
) -> list[dict[str, Any]]:
    scopes = {
        scheme_scope
        for scheme in _configured_security_schemes(settings, dependencies.base)
        for scheme_scope in scheme.get("scopes", [])
        if isinstance(scheme_scope, str)
    }
    if _youtube_exposed(settings, dependencies):
        scopes.add(YOUTUBE_SCOPE)
    return [{"type": "oauth2", "scopes": sorted(scopes)}]


def create_server(
    settings: Settings,
    *,
    dependencies: YouTubeMCPDependencies,
    default_identity=None,  # type: ignore[no-untyped-def]
):  # type: ignore[no-untyped-def]
    try:
        from mcp.types import ToolAnnotations
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("install my-data-hub to run the MCP server") from exc

    fallback = default_identity or _local_identity(settings)
    server = create_base_server(
        settings,
        dependencies=dependencies.base,
        default_identity=fallback,
    )
    service = YouTubeHubService(
        analyzer=dependencies.analyzer,
        enabled=dependencies.feature_enabled,
        audit=dependencies.base.audit,
        fallback_identity=fallback,
    )

    async def youtube_video_analyze(
        youtube_url: YouTubeURL,
        idempotency_key: IdempotencyKey,
        mode: Literal["summary", "transcript", "question", "custom"] = "summary",
        question: Question = None,
        prompt: CustomPrompt = None,
        language: Language = "ru",
        include_timestamps: bool = True,
        include_visual_observations: bool = True,
        model: ModelName = None,
        media_resolution: Literal["low", "medium", "high"] | None = None,
        max_output_tokens: OutputTokens = 4096,
        thinking_level: Literal["minimal", "low", "medium", "high"] = "low",
    ) -> dict[str, Any]:
        return await service.invoke(locals())

    annotations = ToolAnnotations(**YOUTUBE_TOOL_CONTRACT.annotations())
    server.tool(
        name=YOUTUBE_TOOL_NAME,
        annotations=annotations,
        meta={"securitySchemes": YOUTUBE_TOOL_CONTRACT.security_schemes()},
        structured_output=True,
    )(youtube_video_analyze)

    base_class = type(server)
    original_list_tools = server.list_tools
    original_call_tool = server.call_tool
    metadata_url = (
        oauth_resource_metadata_url(settings.mcp_oauth_resource)
        if settings.mcp_oauth_resource
        else "https://invalid.example/.well-known/oauth-protected-resource"
    )

    async def list_tools(self):  # type: ignore[no-untyped-def]
        base_tools = list(await original_list_tools())
        identity = self._identity()
        if (
            not _youtube_exposed(settings, dependencies)
            or identity is None
            or YOUTUBE_SCOPE not in identity.scopes
        ):
            return base_tools
        raw_tools = await super(base_class, self).list_tools()
        for tool in raw_tools:
            if tool.name == YOUTUBE_TOOL_NAME:
                tool.input_schema["additionalProperties"] = False
                return [*base_tools, tool]
        raise RuntimeError("YouTube MCP tool registration is missing")

    async def call_tool(self, name, arguments, context=None):  # type: ignore[no-untyped-def]
        if str(name) != YOUTUBE_TOOL_NAME:
            return await original_call_tool(name, arguments, context)
        identity = self._identity()
        if (
            not _youtube_exposed(settings, dependencies)
            or identity is None
            or YOUTUBE_SCOPE not in identity.scopes
        ):
            if identity is not None and dependencies.base.audit is not None:
                recorded = dependencies.base.audit.record_mcp_audit(
                    OAuthAuditEvent(
                        event="mcp_tool",
                        outcome="scope_denied",
                        issuer=identity.issuer,
                        client_id=identity.client_id,
                        subject=identity.subject,
                        token_id=identity.token_id,
                        tool=YOUTUBE_TOOL_NAME,
                    )
                )
                if inspect.isawaitable(recorded):
                    await recorded
            return _auth_error(metadata_url, insufficient_scope=identity is not None)
        return await super(base_class, self).call_tool(name, arguments, context)

    server.list_tools = MethodType(list_tools, server)
    server.call_tool = MethodType(call_tool, server)
    server.security_schemes = _security_schemes(settings, dependencies)
    return server


def create_streamable_http_app(
    settings: Settings,
    *,
    dependencies: YouTubeMCPDependencies,
    validator,  # type: ignore[no-untyped-def]
):  # type: ignore[no-untyped-def]
    from mcp.server.transport_security import TransportSecuritySettings
    from starlette.applications import Starlette
    from starlette.responses import JSONResponse
    from starlette.routing import Mount, Route

    from my_data_hub.mcp.admission import AdmissionLimits, OAuthAdmissionSecurity
    from my_data_hub.mcp.transport import ToolSecurityMetadataMiddleware

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
    metadata_tools = _metadata_tool_names(settings, dependencies)
    configured_security_schemes = _security_schemes(settings, dependencies)
    resource_metadata = ProtectedResourceMetadata(
        resource=settings.mcp_oauth_resource,
        authorization_servers=(settings.mcp_oauth_issuer,),
        scopes_supported=frozenset(
            TOOL_CONTRACTS[name].scope
            for name in metadata_tools
            if name in TOOL_CONTRACTS and TOOL_CONTRACTS[name].scope in settings.mcp_scopes
        ),
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
            Mount(
                "/",
                app=ToolSecurityMetadataMiddleware(
                    mcp_app,
                    security_schemes=configured_security_schemes,
                ),
            ),
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
            max_concurrency=8,
            requests_per_window=60,
            rate_window_seconds=60,
            request_timeout_seconds=max(30, settings.google_youtube_timeout_seconds + 15),
        ),
        metadata_path=metadata_path,
        resource_metadata_url=metadata_url,
    )
