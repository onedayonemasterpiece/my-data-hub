from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from my_data_hub.auth.control import OAuthAuditEvent
from my_data_hub.google_ai.errors import GoogleAIErrorCode
from my_data_hub.mcp.oauth import AccessIdentity
from my_data_hub.mcp.server import MCPDependencies
from my_data_hub.mcp.youtube_catalog import (
    ALL_SCOPES,
    TOOL_CONTRACTS,
    YOUTUBE_SCOPE,
    YOUTUBE_TOOL_CONTRACT,
    YOUTUBE_TOOL_NAME,
)
from my_data_hub.mcp.youtube_server import (
    YouTubeMCPDependencies,
    _metadata_tool_names,
    _security_schemes,
    _youtube_exposed,
    create_server,
)
from my_data_hub.mcp.youtube_service import YouTubeHubService

RESOURCE = "https://mcp-datahub.kenigevents.ru/mcp"


def identity(*scopes: str, subject: str = "owner") -> AccessIdentity:
    return AccessIdentity(
        subject=subject,
        client_id=f"client:{subject}",
        scopes=frozenset(scopes),
        audience=RESOURCE,
        token_id=f"token:{subject}",
        expires_at=2_000_000_300,
        issuer="https://identity.example",
        issued_at=2_000_000_000,
        resource=RESOURCE,
    )


class Analyzer:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def analyze(self, arguments):  # type: ignore[no-untyped-def]
        value = arguments.model_dump(mode="json")
        self.calls.append(value)
        return {
            "status": "completed",
            "request_uid": "request-1",
            "interaction_id": "interaction-1",
            "provider": "google_gemini_interactions",
            "source_type": "public_youtube_url",
            "mode": value["mode"],
            "retryable": False,
            "warnings": [],
        }


class Audit:
    def __init__(self) -> None:
        self.events: list[OAuthAuditEvent] = []

    async def record_mcp_audit(self, event: OAuthAuditEvent) -> None:
        self.events.append(event)


def settings(*, enabled: bool = True, operator: bool = True, scopes: frozenset[str] | None = None):
    return SimpleNamespace(
        mcp_remote_enabled=True,
        mcp_scopes=scopes or frozenset({YOUTUBE_SCOPE}),
        mcp_oauth_resource=RESOURCE,
        mcp_operator_profile_enabled=operator,
    )


def test_contract_is_read_only_quota_consuming_non_idempotent_and_operator_scoped() -> None:
    assert TOOL_CONTRACTS[YOUTUBE_TOOL_NAME] is YOUTUBE_TOOL_CONTRACT
    assert YOUTUBE_SCOPE in ALL_SCOPES
    assert YOUTUBE_TOOL_CONTRACT.read_only is True
    assert YOUTUBE_TOOL_CONTRACT.destructive is False
    assert YOUTUBE_TOOL_CONTRACT.idempotent is False
    assert YOUTUBE_TOOL_CONTRACT.open_world is True
    assert YOUTUBE_TOOL_CONTRACT.role == "operator"


def test_exposure_requires_feature_analyzer_owner_profile_and_scope() -> None:
    analyzer = Analyzer()
    base = MCPDependencies()
    assert _youtube_exposed(
        settings(),
        YouTubeMCPDependencies(base=base, analyzer=analyzer, feature_enabled=True),
    )
    assert not _youtube_exposed(
        settings(enabled=False),
        YouTubeMCPDependencies(base=base, analyzer=analyzer, feature_enabled=False),
    )
    assert not _youtube_exposed(
        settings(operator=False),
        YouTubeMCPDependencies(base=base, analyzer=analyzer, feature_enabled=True),
    )
    assert _youtube_exposed(
        settings(operator=False),
        YouTubeMCPDependencies(
            base=MCPDependencies(unified_bootstrap_profile_enabled=True),
            analyzer=analyzer,
            feature_enabled=True,
        ),
    )
    assert not _youtube_exposed(
        settings(scopes=frozenset({"platform:read"})),
        YouTubeMCPDependencies(base=base, analyzer=analyzer, feature_enabled=True),
    )
    assert not _youtube_exposed(
        settings(),
        YouTubeMCPDependencies(
            base=MCPDependencies(reader_profile_enabled=True),
            analyzer=analyzer,
            feature_enabled=True,
        ),
    )


@pytest.mark.asyncio
async def test_service_does_not_use_canonical_write_path_and_audits_result() -> None:
    analyzer = Analyzer()
    audit = Audit()
    owner = identity(YOUTUBE_SCOPE)
    service = YouTubeHubService(
        analyzer=analyzer,
        enabled=True,
        audit=audit,
        identity_provider=lambda: owner,
    )
    result = await service.invoke(
        {
            "youtube_url": "https://youtu.be/6V2stDksGI8",
            "idempotency_key": "mcp-test-0001",
        }
    )
    assert result["status"] == "completed"
    assert len(analyzer.calls) == 1
    assert [event.outcome for event in audit.events] == ["accepted"]
    assert audit.events[0].tool == YOUTUBE_TOOL_NAME


@pytest.mark.asyncio
async def test_service_returns_typed_feature_disabled_without_provider_call() -> None:
    analyzer = Analyzer()
    owner = identity(YOUTUBE_SCOPE)
    service = YouTubeHubService(
        analyzer=analyzer,
        enabled=False,
        audit=None,
        identity_provider=lambda: owner,
    )
    result = await service.invoke(
        {
            "youtube_url": "https://youtu.be/6V2stDksGI8",
            "idempotency_key": "mcp-test-0002",
        }
    )
    assert result["error"]["code"] == GoogleAIErrorCode.FEATURE_DISABLED.value
    assert analyzer.calls == []


@pytest.mark.asyncio
async def test_server_discovery_is_closed_operator_only_and_truthfully_annotated() -> None:
    analyzer = Analyzer()
    owner = identity(YOUTUBE_SCOPE)
    server = create_server(
        settings(),  # type: ignore[arg-type]
        dependencies=YouTubeMCPDependencies(
            base=MCPDependencies(),
            analyzer=analyzer,
            feature_enabled=True,
        ),
        default_identity=owner,
    )
    tools = {tool.name: tool for tool in await server.list_tools()}
    tool = tools[YOUTUBE_TOOL_NAME]
    assert tool.input_schema["additionalProperties"] is False
    assert set(tool.input_schema["required"]) == {"youtube_url", "idempotency_key"}
    assert tool.annotations.read_only_hint is True
    assert tool.annotations.destructive_hint is False
    assert tool.annotations.idempotent_hint is False
    assert tool.annotations.open_world_hint is True
    assert tool.meta == {"securitySchemes": [{"type": "oauth2", "scopes": [YOUTUBE_SCOPE]}]}


@pytest.mark.asyncio
async def test_server_call_forwards_only_declared_youtube_arguments() -> None:
    analyzer = Analyzer()
    owner = identity(YOUTUBE_SCOPE)
    server = create_server(
        settings(),  # type: ignore[arg-type]
        dependencies=YouTubeMCPDependencies(
            base=MCPDependencies(),
            analyzer=analyzer,
            feature_enabled=True,
        ),
        default_identity=owner,
    )

    result = await server.call_tool(
        YOUTUBE_TOOL_NAME,
        {
            "youtube_url": "https://youtu.be/6V2stDksGI8",
            "idempotency_key": "mcp-server-call-0001",
            "mode": "question",
            "question": "Какие пять основных тезисов формулирует автор?",
            "model": "gemini-3.7-flash",
            "media_resolution": "low",
            "max_output_tokens": 8192,
            "thinking_level": "low",
        },
    )

    assert result.structured_content is not None
    assert result.structured_content["status"] == "completed"
    assert len(analyzer.calls) == 1
    assert set(analyzer.calls[0]) == {
        "youtube_url",
        "mode",
        "question",
        "prompt",
        "language",
        "include_timestamps",
        "include_visual_observations",
        "model",
        "media_resolution",
        "max_output_tokens",
        "thinking_level",
        "idempotency_key",
    }


@pytest.mark.asyncio
async def test_reader_profile_never_lists_youtube_tool() -> None:
    analyzer = Analyzer()
    reader = identity("platform:read", subject="reader")
    server = create_server(
        settings(scopes=frozenset({"platform:read", YOUTUBE_SCOPE})),  # type: ignore[arg-type]
        dependencies=YouTubeMCPDependencies(
            base=MCPDependencies(reader_profile_enabled=True),
            analyzer=analyzer,
            feature_enabled=True,
        ),
        default_identity=reader,
    )
    assert YOUTUBE_TOOL_NAME not in {tool.name for tool in await server.list_tools()}


@pytest.mark.asyncio
async def test_provider_only_profile_never_lists_youtube_tool() -> None:
    analyzer = Analyzer()
    owner = identity(YOUTUBE_SCOPE)
    server = create_server(
        settings(),  # type: ignore[arg-type]
        dependencies=YouTubeMCPDependencies(
            base=MCPDependencies(provider_only_profile_enabled=True),
            analyzer=analyzer,
            feature_enabled=True,
        ),
        default_identity=owner,
    )
    assert YOUTUBE_TOOL_NAME not in {tool.name for tool in await server.list_tools()}


@pytest.mark.asyncio
async def test_bounded_unified_owner_profile_lists_youtube_tool_without_operator_writes() -> None:
    analyzer = Analyzer()
    owner = identity(YOUTUBE_SCOPE)
    server = create_server(
        settings(operator=False),  # type: ignore[arg-type]
        dependencies=YouTubeMCPDependencies(
            base=MCPDependencies(unified_bootstrap_profile_enabled=True),
            analyzer=analyzer,
            feature_enabled=True,
        ),
        default_identity=owner,
    )
    assert YOUTUBE_TOOL_NAME in {tool.name for tool in await server.list_tools()}


def test_oauth_metadata_adds_scope_only_for_enabled_operator_profile() -> None:
    analyzer = Analyzer()
    base = MCPDependencies()
    enabled = YouTubeMCPDependencies(base=base, analyzer=analyzer, feature_enabled=True)
    disabled = YouTubeMCPDependencies(base=base, analyzer=analyzer, feature_enabled=False)
    assert YOUTUBE_TOOL_NAME in _metadata_tool_names(settings(), enabled)
    assert YOUTUBE_SCOPE in _security_schemes(settings(), enabled)[0]["scopes"]
    assert YOUTUBE_TOOL_NAME not in _metadata_tool_names(settings(), disabled)
    assert YOUTUBE_SCOPE not in _security_schemes(settings(), disabled)[0]["scopes"]
