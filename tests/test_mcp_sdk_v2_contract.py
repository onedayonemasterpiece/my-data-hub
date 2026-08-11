from __future__ import annotations

import pytest

from my_data_hub.config import Settings
from my_data_hub.mcp.server import create_server, oauth_resource_metadata_url

mcp_server_module = pytest.importorskip("mcp.server")

READ_ONLY_TOOLS = {
    "platform.status",
    "master.status",
    "operation.get",
    "checkpoint.status",
    "embedding.coverage",
    "provider.resources.status",
    "bloggers.list",
    "bloggers.get",
    "bloggers.search",
    "bloggers.provenance",
    "bloggers.statistics",
    "bloggers.migration.accounting",
    "data.query",
    "data.change.status",
}
WRITE_TOOLS = {"master.ensure", "data.change.preview", "data.change.apply"}


def read_only_settings(monkeypatch: pytest.MonkeyPatch) -> Settings:
    monkeypatch.setenv(
        "MY_DATA_HUB_DATABASE_URL",
        "postgresql://contract:contract@127.0.0.1:5432/contract",
    )
    monkeypatch.setenv("MY_DATA_HUB_ENVIRONMENT", "test")
    monkeypatch.setenv("MY_DATA_HUB_MCP_REMOTE_ENABLED", "false")
    monkeypatch.setenv("MY_DATA_HUB_MCP_WRITE_ENABLED", "false")
    monkeypatch.setenv(
        "MY_DATA_HUB_MCP_SCOPES",
        "platform:read,master:read,operation:read,checkpoint:read,embedding:read,provider:read,bloggers:read,data:read",
    )
    return Settings.from_env()


@pytest.mark.asyncio
async def test_mcp_v2_read_only_tool_catalog(monkeypatch: pytest.MonkeyPatch) -> None:
    server = create_server(read_only_settings(monkeypatch))
    tools = await server.list_tools()
    names = {tool.name for tool in tools}
    assert names == READ_ONLY_TOOLS
    assert names.isdisjoint(WRITE_TOOLS)


def test_mcp_v2_streamable_http_builder_accepts_security_limits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from mcp.server.transport_security import TransportSecuritySettings

    server = create_server(read_only_settings(monkeypatch))
    app = server.streamable_http_app(
        host="127.0.0.1",
        max_request_body_size=1_048_576,
        transport_security=TransportSecuritySettings(
            allowed_hosts=["127.0.0.1", "127.0.0.1:*"],
            allowed_origins=["http://127.0.0.1"],
        ),
    )
    assert app is not None
    assert server.session_manager is not None


def test_rfc9728_metadata_url_is_path_derived() -> None:
    assert oauth_resource_metadata_url("https://mcp-datahub.kenigevents.ru/mcp") == (
        "https://mcp-datahub.kenigevents.ru/.well-known/oauth-protected-resource/mcp"
    )
