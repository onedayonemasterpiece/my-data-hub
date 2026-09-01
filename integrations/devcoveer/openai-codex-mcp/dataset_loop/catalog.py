from __future__ import annotations

import re
from collections.abc import Iterable
from typing import Any

from .models import CatalogModel

PROFILES = frozenset({"zen", "zen_nvidia_audit", "nvidia_assisted"})


def classify_catalog(items: Iterable[dict[str, Any]]) -> list[CatalogModel]:
    result: list[CatalogModel] = []
    for item in items:
        selection = item.get("selection") or item.get("id")
        provider = item.get("provider")
        if not isinstance(selection, str) or not isinstance(provider, str):
            continue
        metadata = item.get("metadata") if isinstance(item.get("metadata"), dict) else {}
        tags = metadata.get("tags", item.get("tags", []))
        zen = metadata.get("profile") == "zen" or (
            isinstance(tags, list) and any(str(tag).casefold() == "zen" for tag in tags)
        )
        result.append(
            CatalogModel(
                selection=selection,
                provider=provider.casefold(),
                active=item.get("active") is True,
                free=item.get("free") is True,
                zen=zen,
                metadata=metadata,
            )
        )
    return result


def rank_nvidia_candidates(catalog: Iterable[CatalogModel]) -> list[CatalogModel]:
    def priority(model: CatalogModel) -> tuple[int, str]:
        name = model.selection.casefold()
        if "deepseek" in name and re.search(r"20\d{2}[-_.]?\d{2}[-_.]?\d{2}", name):
            return (0, name)
        if "kimi" in name and "k3" in name:
            return (1, name)
        if "nemotron" in name:
            return (2, name)
        return (3, name)

    return sorted(
        (model for model in catalog if model.provider == "nvidia" and model.active),
        key=priority,
    )


def profile_models(profile: str, catalog: Iterable[CatalogModel]) -> dict[str, list[CatalogModel]]:
    if profile not in PROFILES:
        raise ValueError("unknown profile")
    materialized = list(catalog)
    zen = [model for model in materialized if model.active and model.free and model.zen]
    return {"zen": zen, "nvidia": [] if profile == "zen" else rank_nvidia_candidates(materialized)}


def has_real_tool_receipt(receipt: object) -> bool:
    """Require terminal executed tool parts rather than claimed capability fields."""
    if not isinstance(receipt, dict) or receipt.get("terminal") is not True:
        return False
    messages = receipt.get("messages")
    if not isinstance(messages, list):
        return False
    seen: set[str] = set()
    for message in messages:
        parts = message.get("parts") if isinstance(message, dict) else None
        if not isinstance(parts, list):
            continue
        for part in parts:
            if not isinstance(part, dict):
                continue
            if part.get("type") not in {"tool", "tool_call", "tool-invocation"}:
                continue
            tool = part.get("tool") or part.get("name") or part.get("toolName")
            if isinstance(tool, str) and part.get("state", "completed") in {"completed", "success"}:
                seen.add(tool.casefold())
    return {"websearch", "webfetch"}.issubset(seen)
