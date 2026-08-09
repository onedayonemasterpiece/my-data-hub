from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol

from pydantic import BaseModel, ConfigDict, Field

from .models import ObservedProviderResource, ProviderKind, ProviderResource
from .policy import ProviderRegistry


class InventoryPage(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    resources: tuple[ObservedProviderResource, ...]
    next_cursor: str | None = Field(default=None, min_length=1, max_length=1000)


class InventoryAdapter(Protocol):
    def list_resources(self, *, kind: ProviderKind, cursor: str | None, limit: int) -> InventoryPage: ...


class InventoryLimits(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    page_size: int = Field(default=50, ge=1, le=100)
    max_pages: int = Field(default=20, ge=1, le=100)
    max_resources: int = Field(default=1000, ge=1, le=5000)


class InventoryProtocolError(RuntimeError):
    pass


class InventoryBoundExceeded(RuntimeError):
    pass


class BoundedInventory:
    def __init__(
        self,
        adapter: InventoryAdapter,
        registry: ProviderRegistry,
        limits: InventoryLimits | None = None,
    ) -> None:
        self.adapter = adapter
        self.registry = registry
        self.limits = limits or InventoryLimits()

    def collect(self, kind: ProviderKind) -> Sequence[ProviderResource]:
        cursor: str | None = None
        seen_cursors: set[str] = set()
        seen_resources: set[tuple[str, str]] = set()
        result: list[ProviderResource] = []

        for _ in range(self.limits.max_pages):
            page = self.adapter.list_resources(kind=kind, cursor=cursor, limit=self.limits.page_size)
            if len(page.resources) > self.limits.page_size:
                raise InventoryProtocolError("provider returned more resources than requested")
            for observed in page.resources:
                if observed.kind != kind:
                    raise InventoryProtocolError("provider page returned the wrong resource kind")
                identity = (observed.provider, observed.provider_ref)
                if identity in seen_resources:
                    raise InventoryProtocolError("provider inventory contains a duplicate resource")
                seen_resources.add(identity)
                if len(result) >= self.limits.max_resources:
                    raise InventoryBoundExceeded("inventory resource bound exceeded")
                result.append(self.registry.resolve_discovery(observed))
            if page.next_cursor is None:
                return tuple(result)
            if page.next_cursor == cursor or page.next_cursor in seen_cursors:
                raise InventoryProtocolError("provider repeated an inventory cursor")
            seen_cursors.add(page.next_cursor)
            cursor = page.next_cursor

        raise InventoryBoundExceeded("inventory page bound exceeded")
