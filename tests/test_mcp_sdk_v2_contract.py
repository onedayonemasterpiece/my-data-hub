from __future__ import annotations

import pytest

mcp_server_module = pytest.importorskip("mcp.server")

from my_data_hub.config import Settings
from my_data_hub.mcp.server import create_server


READ_ONLY_TOOLS = {
    "hub.health",
    "hub.project.list",
    "hub.content.search",
    "hub.content.get",
    "hub.trace.get",
    "region_talk.queue.summary",
    "region_talk.plan.preview",
    "region_talk.migration.status",
    "region_talk.migration.accounting",
}
WRITE_TOOLS = {"region_talk.work.enqueue", "hub.command.submit"}


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
        "hub:read,orchestrator:read,region-talk:read,migration:read",
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
