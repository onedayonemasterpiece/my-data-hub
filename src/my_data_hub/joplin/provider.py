from __future__ import annotations

import ipaddress
from dataclasses import dataclass
from typing import Any, Protocol
from urllib.parse import urlparse

from my_data_hub.joplin.bridge import JoplinNoteSnapshot


class JoplinProviderError(RuntimeError):
    pass


class JoplinProvider(Protocol):
    async def list_notes(
        self, *, page: int = 1, limit: int = 100
    ) -> tuple[list[JoplinNoteSnapshot], bool]: ...

    async def get_note(self, note_id: str) -> JoplinNoteSnapshot | None: ...


@dataclass(slots=True)
class HttpJoplinDataApi:
    """Read-only adapter for a desktop-local Joplin Data API endpoint.

    The Joplin token stays on the Windows bridge host. Agents call my-data-hub MCP;
    they do not receive this token or direct access to the desktop API.
    """

    base_url: str
    token: str
    timeout_seconds: float = 10.0
    allow_non_loopback: bool = False

    def __post_init__(self) -> None:
        parsed = urlparse(self.base_url)
        if parsed.scheme != "http" or not parsed.hostname:
            raise JoplinProviderError("Joplin Data API base URL must be local HTTP")
        try:
            address = ipaddress.ip_address(parsed.hostname)
            loopback = address.is_loopback
        except ValueError:
            loopback = parsed.hostname in {"localhost"}
        if not loopback and not self.allow_non_loopback:
            raise JoplinProviderError("non-loopback Joplin endpoint is disabled")
        if not self.token:
            raise JoplinProviderError("Joplin API token is required")

    async def _get(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        try:
            import aiohttp
        except ImportError as exc:  # pragma: no cover
            raise JoplinProviderError("aiohttp is required") from exc
        timeout = aiohttp.ClientTimeout(total=self.timeout_seconds)
        query = {**params, "token": self.token}
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(f"{self.base_url.rstrip('/')}{path}", params=query) as response:
                if response.status == 404:
                    return {"_not_found": True}
                if response.status != 200:
                    body = (await response.text())[:500]
                    raise JoplinProviderError(
                        f"Joplin API returned HTTP {response.status}: {body}"
                    )
                payload = await response.json()
        if not isinstance(payload, dict):
            raise JoplinProviderError("Joplin API returned a non-object payload")
        return payload

    @staticmethod
    def _snapshot(raw: dict[str, Any]) -> JoplinNoteSnapshot:
        return JoplinNoteSnapshot(
            note_id=str(raw["id"]),
            title=str(raw.get("title") or ""),
            body=str(raw.get("body") or ""),
            updated_time=int(raw.get("updated_time") or 0),
            deleted_time=int(raw.get("deleted_time") or 0),
        )

    async def list_notes(
        self, *, page: int = 1, limit: int = 100
    ) -> tuple[list[JoplinNoteSnapshot], bool]:
        if page < 1:
            raise ValueError("page must be positive")
        limit = max(1, min(limit, 100))
        payload = await self._get(
            "/notes",
            {
                "page": page,
                "limit": limit,
                "order_by": "updated_time",
                "order_dir": "ASC",
                "fields": "id,title,body,updated_time,deleted_time,parent_id",
            },
        )
        items = payload.get("items", [])
        if not isinstance(items, list):
            raise JoplinProviderError("Joplin notes response has invalid items")
        return [self._snapshot(item) for item in items], bool(payload.get("has_more"))

    async def get_note(self, note_id: str) -> JoplinNoteSnapshot | None:
        if not note_id or len(note_id) > 200:
            raise ValueError("invalid Joplin note ID")
        payload = await self._get(
            f"/notes/{note_id}",
            {"fields": "id,title,body,updated_time,deleted_time,parent_id"},
        )
        if payload.get("_not_found"):
            return None
        return self._snapshot(payload)
