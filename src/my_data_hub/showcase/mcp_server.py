from __future__ import annotations

import asyncio
from typing import Any

from .manager import ShowcaseManager


def create_server(manager: ShowcaseManager | None = None):  # type: ignore[no-untyped-def]
    """Create the standalone showcase MCP server.

    Production should mount these tools behind the existing my-data-hub OAuth
    boundary. The standalone stdio server keeps local development and contract
    testing independent from deployment wiring.
    """

    try:
        from mcp.server import MCPServer
        from mcp.types import ToolAnnotations
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("install my-data-hub to run the showcase MCP server") from exc

    control = manager or ShowcaseManager.from_env()
    mcp = MCPServer(
        "my-data-hub-showcase",
        version="0.1.0",
        instructions=(
            "Build and manage curated IdeaHub static showcase surfaces. "
            "Ordinary rebuilds preserve the secret URL; rotation is explicit."
        ),
    )

    def tool(name: str, *, read_only: bool, destructive: bool = False, idempotent: bool = True):
        annotations = ToolAnnotations(
            readOnlyHint=read_only,
            destructiveHint=destructive,
            idempotentHint=idempotent,
            openWorldHint=not read_only,
        )
        return mcp.tool(name=name, annotations=annotations, structured_output=True)

    @tool("showcase.list", read_only=True)
    async def showcase_list() -> dict[str, Any]:
        return await asyncio.to_thread(control.list_surfaces)

    @tool("showcase.get_link", read_only=True)
    async def showcase_get_link(view_id: str) -> dict[str, Any]:
        return await asyncio.to_thread(control.get_link, view_id)

    @tool("showcase.rebuild", read_only=False, idempotent=True)
    async def showcase_rebuild(view_id: str, idempotency_key: str) -> dict[str, Any]:
        return await asyncio.to_thread(control.rebuild, view_id, idempotency_key=idempotency_key)

    @tool("showcase.create_view", read_only=False, idempotent=True)
    async def showcase_create_view(view_id: str, idempotency_key: str, publish: bool = True) -> dict[str, Any]:
        return await asyncio.to_thread(
            control.create_view,
            view_id,
            publish=publish,
            idempotency_key=idempotency_key,
        )

    @tool("showcase.rotate_link", read_only=False, destructive=True, idempotent=False)
    async def showcase_rotate_link(view_id: str, idempotency_key: str) -> dict[str, Any]:
        return await asyncio.to_thread(control.rotate_link, view_id, idempotency_key=idempotency_key)

    @tool("showcase.revoke_link", read_only=False, destructive=True, idempotent=True)
    async def showcase_revoke_link(view_id: str, idempotency_key: str) -> dict[str, Any]:
        return await asyncio.to_thread(control.revoke_link, view_id, idempotency_key=idempotency_key)

    return mcp


def main() -> None:
    create_server().run(transport="stdio")


if __name__ == "__main__":
    main()
