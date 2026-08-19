from __future__ import annotations

from collections.abc import Awaitable, Mapping
from typing import Annotated, Any, Literal, Protocol, runtime_checkable
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from my_data_hub.mcp.oauth import AccessIdentity

RegionTalkCursor = Annotated[
    str,
    Field(
        min_length=4,
        max_length=8,
        pattern=r"^v1:(?:0|[1-9][0-9]{0,3}|10000)$",
        description="Opaque bounded Region Talk pagination cursor.",
    ),
]
RegionTalkLimit = Annotated[int, Field(ge=1, le=100)]
RegionTalkMaxBytes = Annotated[int, Field(ge=1_024, le=262_144)]
RegionTalkFilter = Annotated[
    str,
    Field(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9][A-Za-z0-9_.:-]*$"),
]
RegionTalkQuery = Annotated[str, Field(min_length=1, max_length=256)]
RegionTalkSourceRevision = Annotated[
    str,
    Field(pattern=r"^(?:[a-f0-9]{40}|[a-f0-9]{64})$"),
]
RegionTalkIdempotencyKey = Annotated[
    str,
    Field(
        min_length=8,
        max_length=128,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9_.:-]{7,127}$",
    ),
]


class _ClosedModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)


class RegionTalkEmptyRequest(_ClosedModel):
    pass


class RegionTalkListRequest(_ClosedModel):
    cursor: RegionTalkCursor | None = None
    limit: RegionTalkLimit = 50
    status: RegionTalkFilter | None = None
    category: RegionTalkFilter | None = None
    max_bytes: RegionTalkMaxBytes = 262_144


class RegionTalkSearchRequest(RegionTalkListRequest):
    query: RegionTalkQuery


class RegionTalkPostListRequest(_ClosedModel):
    cursor: RegionTalkCursor | None = None
    limit: RegionTalkLimit = 50
    status: RegionTalkFilter | None = None
    platform: RegionTalkFilter | None = None
    max_bytes: RegionTalkMaxBytes = 262_144


class RegionTalkPostSearchRequest(RegionTalkPostListRequest):
    query: RegionTalkQuery


class RegionTalkQueueListRequest(_ClosedModel):
    cursor: RegionTalkCursor | None = None
    limit: RegionTalkLimit = 50
    status: RegionTalkFilter | None = None
    category: RegionTalkFilter | None = None
    max_bytes: RegionTalkMaxBytes = 262_144


class RegionTalkGetRequest(_ClosedModel):
    item_id: UUID
    max_bytes: RegionTalkMaxBytes = 262_144


class RegionTalkPipelineRunRequest(_ClosedModel):
    project_slug: Literal["region-talk"] = "region-talk"
    mode: Literal["supervised"] = "supervised"
    source_revision: RegionTalkSourceRevision
    idempotency_key: RegionTalkIdempotencyKey
    publication_dispatch: Literal[False] = False


_REQUEST_MODELS: dict[str, type[_ClosedModel]] = {
    "region_talk.inventory": RegionTalkEmptyRequest,
    "region_talk.articles.list": RegionTalkListRequest,
    "region_talk.articles.get": RegionTalkGetRequest,
    "region_talk.articles.search": RegionTalkSearchRequest,
    "region_talk.posts.list": RegionTalkPostListRequest,
    "region_talk.posts.get": RegionTalkGetRequest,
    "region_talk.posts.search": RegionTalkPostSearchRequest,
    "region_talk.queue.list": RegionTalkQueueListRequest,
    "region_talk.queue.summary": RegionTalkEmptyRequest,
    "region_talk.pipeline.status": RegionTalkEmptyRequest,
    "region_talk.pipeline.run": RegionTalkPipelineRunRequest,
}


def validate_region_talk_arguments(tool: str, arguments: Mapping[str, Any]) -> dict[str, Any]:
    """Validate one exact semantic request before master/control-plane activity."""

    model = _REQUEST_MODELS.get(tool)
    if model is None:
        raise ValueError("unknown Region Talk MCP tool")
    return model.model_validate(dict(arguments)).model_dump(mode="json", exclude_none=True)


def region_talk_reader_request(arguments: Mapping[str, Any]) -> dict[str, Any]:
    """Translate the opaque public cursor into a bounded internal reader offset."""

    cursor = arguments.get("cursor")
    offset = int(str(cursor).split(":", 1)[1]) if cursor is not None else 0
    request = {
        key: value
        for key, value in arguments.items()
        if key in {"limit", "status", "category", "platform", "query"}
    }
    request["offset"] = offset
    return request


def region_talk_page(
    result: Mapping[str, Any] | list[Mapping[str, Any]],
    *,
    arguments: Mapping[str, Any],
) -> dict[str, Any]:
    """Normalize reader output without exposing an implementation-defined cursor."""

    raw_items: Any = result.get("items") if isinstance(result, Mapping) else result
    if not isinstance(raw_items, list) or not all(isinstance(item, Mapping) for item in raw_items):
        raise ValueError("Region Talk reader returned an invalid page")
    limit = int(arguments.get("limit", 50))
    if len(raw_items) > limit:
        raise ValueError("Region Talk reader exceeded the requested row limit")
    cursor = arguments.get("cursor")
    offset = int(str(cursor).split(":", 1)[1]) if cursor is not None else 0
    next_offset = offset + len(raw_items)
    complete = len(raw_items) < limit or next_offset >= 10_000
    return {
        "items": [dict(item) for item in raw_items],
        "next_cursor": None if complete else f"v1:{next_offset}",
        "complete": complete,
    }


@runtime_checkable
class RegionTalkPipelineController(Protocol):
    """Metadata-only seam implemented by the root control-plane runtime."""

    def request_supervised_run(
        self,
        *,
        request: RegionTalkPipelineRunRequest,
        principal: AccessIdentity,
    ) -> Mapping[str, Any] | Awaitable[Mapping[str, Any]]: ...
