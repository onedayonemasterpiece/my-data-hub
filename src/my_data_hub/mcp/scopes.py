from __future__ import annotations


class ScopeDenied(PermissionError):
    pass


def require_scope(granted: frozenset[str], required: str) -> None:
    if required not in granted:
        raise ScopeDenied(f"required MCP scope is not granted: {required}")


TOOL_SCOPES: dict[str, str] = {
    "hub.health": "hub:read",
    "hub.project.list": "hub:read",
    "hub.content.search": "hub:read",
    "hub.content.get": "hub:read",
    "hub.trace.get": "hub:read",
    "region_talk.queue.summary": "region-talk:read",
    "region_talk.plan.preview": "region-talk:read",
    "region_talk.migration.status": "migration:read",
    "region_talk.migration.accounting": "migration:read",
    "region_talk.work.enqueue": "region-talk:write",
    "hub.command.submit": "hub:write",
}
