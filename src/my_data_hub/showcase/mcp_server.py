from __future__ import annotations

import asyncio
from typing import Any

from .manager import ShowcaseManager
from .models import ShowcaseViewInput, ShowcaseWriteItem
from .requests import ShowcaseMode


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

    @tool("showcase.get_source", read_only=True)
    async def showcase_get_source(view_id: str) -> dict[str, Any]:
        return await asyncio.to_thread(control.get_source, view_id)

    @tool("showcase.apply", read_only=False, idempotent=True)
    async def showcase_apply(
        view_id: str,
        expected_source_revision: str,
        view: ShowcaseViewInput | None = None,
        idempotency_key: str | None = None,
        items: list[ShowcaseWriteItem] | None = None,
        mode: ShowcaseMode | None = None,
        dry_run: bool | None = None,
        publish: bool | None = None,
    ) -> dict[str, Any] | list[Any]:
        """Update a showcase; keep its URL. Read get_source first and copy source_revision.

        Prefer mode=preview (no writes), save (draft) or publish (save/build/publish).
        Omit unchanged cards. Pass full definitions only for new/changed cards.
        Changing a card shared with another view is rejected: give the adaptation a new ID.
        Preview needs no key; save/publish require a unique idempotency_key, reused on identical retry.
        Example: view_id='pharma', expected_source_revision='<from get_source>',
        view={title:'Updated title', subtitle:'Updated description', item_ids:['existing-card']}, mode='preview'.
        Legacy dry_run/publish and expected_source_revision='absent' remain compatible; do not mix flags with mode.
        """
        return await asyncio.to_thread(
            control.apply,
            view_id,
            expected_source_revision=expected_source_revision,
            view=view,
            items=items or [],
            mode=mode,
            dry_run=dry_run,
            publish=publish,
            idempotency_key=idempotency_key,
        )

    @tool("showcase.rebuild", read_only=False, idempotent=True)
    async def showcase_rebuild(view_id: str, idempotency_key: str) -> dict[str, Any]:
        return await asyncio.to_thread(control.rebuild, view_id, idempotency_key=idempotency_key)

    @tool("showcase.create_view", read_only=False, idempotent=True)
    async def showcase_create_view(
        view_id: str,
        view: ShowcaseViewInput | None = None,
        items: list[ShowcaseWriteItem] | None = None,
        mode: ShowcaseMode | None = None,
        idempotency_key: str | None = None,
        dry_run: bool | None = None,
        publish: bool | None = None,
    ) -> dict[str, Any] | list[Any]:
        """Create a NEW showcase from a manifest and return its stable link on publication.

        No source revision or 'absent' value is needed. view.id can be omitted.
        Existing cards: view={title:'For pharma', subtitle:'Four working tasks', item_ids:['existing-card']}.
        New card: add its ID to item_ids and pass its complete definition in items,
        including capability_type (technical/product/business) and publish_state='ready' after review.
        mode=preview validates without writes/key; mode=save saves a draft; mode=publish saves/builds/publishes.
        save/publish require idempotency_key; identical retries reuse it. Default with a manifest is preview.
        Optional contacts override the default Telegram contact; filters=[] hides extra filters.
        Legacy calls without view only register an already existing source; never use that form to create content.
        """
        return await asyncio.to_thread(
            control.create_view,
            view_id,
            view=view,
            items=items or [],
            mode=mode,
            dry_run=dry_run,
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
