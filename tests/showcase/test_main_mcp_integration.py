from __future__ import annotations

from types import SimpleNamespace

import pytest

from my_data_hub.mcp.oauth import AccessIdentity
from my_data_hub.mcp.server import MCPDependencies, create_server

RESOURCE = "https://mcp.example.test/mcp"
SHOWCASE_TOOLS = {
    "showcase.list",
    "showcase.get_link",
    "showcase.get_source",
    "showcase.apply",
    "showcase.rebuild",
    "showcase.create_view",
    "showcase.rotate_link",
    "showcase.revoke_link",
}


class FakeShowcaseManager:
    pass


def identity() -> AccessIdentity:
    return AccessIdentity(
        subject="owner",
        client_id="owner-client",
        scopes=frozenset({"showcase:read", "showcase:write"}),
        audience=RESOURCE,
        token_id="showcase-test",
        expires_at=2**63 - 1,
        issuer="https://issuer.example.test",
        issued_at=0,
        resource=RESOURCE,
    )


def settings() -> SimpleNamespace:
    return SimpleNamespace(
        mcp_remote_enabled=True,
        mcp_scopes=frozenset({"showcase:read", "showcase:write"}),
        mcp_oauth_resource=RESOURCE,
    )


@pytest.mark.asyncio
async def test_standard_mcp_lists_showcase_tools_when_manager_is_enabled(monkeypatch) -> None:
    monkeypatch.delenv("MY_DATA_HUB_SHOWCASE_ENABLED", raising=False)
    server = create_server(
        settings(),  # type: ignore[arg-type]
        dependencies=MCPDependencies(showcase_manager=FakeShowcaseManager()),  # type: ignore[arg-type]
        default_identity=identity(),
    )
    tools = {tool.name: tool for tool in await server.list_tools()}
    assert set(tools) >= SHOWCASE_TOOLS
    assert tools["showcase.get_link"].annotations.read_only_hint is True
    assert tools["showcase.rebuild"].annotations.idempotent_hint is True
    assert tools["showcase.rotate_link"].annotations.destructive_hint is True
    assert tools["showcase.rotate_link"].annotations.idempotent_hint is False
    assert "items" not in tools["showcase.apply"].input_schema.get("required", [])


@pytest.mark.asyncio
async def test_unified_profile_lists_all_showcase_tools_when_manager_is_enabled(monkeypatch) -> None:
    monkeypatch.delenv("MY_DATA_HUB_SHOWCASE_ENABLED", raising=False)
    server = create_server(
        settings(),  # type: ignore[arg-type]
        dependencies=MCPDependencies(
            showcase_manager=FakeShowcaseManager(),  # type: ignore[arg-type]
            unified_bootstrap_profile_enabled=True,
        ),
        default_identity=identity(),
    )
    names = {tool.name for tool in await server.list_tools()}
    assert names >= SHOWCASE_TOOLS


@pytest.mark.asyncio
async def test_standard_mcp_hides_showcase_tools_when_not_configured(monkeypatch) -> None:
    monkeypatch.delenv("MY_DATA_HUB_SHOWCASE_ENABLED", raising=False)
    server = create_server(
        settings(),  # type: ignore[arg-type]
        dependencies=MCPDependencies(),
        default_identity=identity(),
    )
    names = {tool.name for tool in await server.list_tools()}
    assert names.isdisjoint(SHOWCASE_TOOLS)
