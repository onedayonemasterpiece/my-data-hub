"""Fixed, sanitized Region Talk read facade for MCP adapters."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any
from uuid import UUID


def _bounded_page(request: Mapping[str, Any]) -> tuple[int, int]:
    limit = int(request.get("limit", 50))
    offset = int(request.get("offset", 0))
    if not 1 <= limit <= 100 or not 0 <= offset <= 10_000:
        raise ValueError("Region Talk page is outside the bounded contract")
    return limit, offset


def _filter(request: Mapping[str, Any], name: str) -> str | None:
    value = request.get(name)
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip() or len(value) > 100:
        raise ValueError(f"invalid Region Talk {name} filter")
    return value.strip()


def _query(request: Mapping[str, Any]) -> str:
    value = request.get("query")
    if not isinstance(value, str) or not value.strip() or len(value) > 500:
        raise ValueError("Region Talk search query is invalid")
    return value.strip()


def _rows(cursor: Any, result: Any) -> list[dict[str, Any]]:
    values: Sequence[Any] = result.fetchall()
    if not values:
        return []
    if isinstance(values[0], Mapping):
        return [dict(row) for row in values]
    names = [item.name if hasattr(item, "name") else item[0] for item in cursor.description]
    return [dict(zip(names, row, strict=True)) for row in values]


def _one(cursor: Any, result: Any) -> dict[str, Any] | None:
    value = result.fetchone()
    if value is None:
        return None
    if isinstance(value, Mapping):
        return dict(value)
    names = [item.name if hasattr(item, "name") else item[0] for item in cursor.description]
    return dict(zip(names, value, strict=True))


class RegionTalkReader:
    """No dynamic table names, selected columns, or caller-supplied SQL."""

    @staticmethod
    def inventory(cursor: Any) -> dict[str, Any]:
        result = cursor.execute(
            """
            SELECT export_batch_id,task_run_id,master_epoch,state,expected_row_count,
                   landed_row_count,dispositioned_row_count,quarantined_row_count,
                   logical_sha256,created_at,completed_at
              FROM region_talk.snapshot_inventory_v2
             ORDER BY created_at DESC,export_batch_id DESC LIMIT 1
            """
        )
        return _one(cursor, result) or {"state": "absent"}

    @staticmethod
    def list_articles(cursor: Any, request: Mapping[str, Any]) -> dict[str, Any]:
        return RegionTalkReader._list_content(cursor, "articles_v2", request)

    @staticmethod
    def get_article(cursor: Any, item_id: UUID) -> dict[str, Any] | None:
        return RegionTalkReader._get_content(cursor, "articles_v2", item_id)

    @staticmethod
    def search_articles(cursor: Any, request: Mapping[str, Any]) -> dict[str, Any]:
        return RegionTalkReader._search_content(cursor, "articles_v2", request)

    @staticmethod
    def list_posts(cursor: Any, request: Mapping[str, Any]) -> dict[str, Any]:
        return RegionTalkReader._list_content(cursor, "posts_v2", request, posts=True)

    @staticmethod
    def get_post(cursor: Any, item_id: UUID) -> dict[str, Any] | None:
        return RegionTalkReader._get_content(cursor, "posts_v2", item_id, posts=True)

    @staticmethod
    def search_posts(cursor: Any, request: Mapping[str, Any]) -> dict[str, Any]:
        return RegionTalkReader._search_content(cursor, "posts_v2", request, posts=True)

    @staticmethod
    def list_queue(cursor: Any, request: Mapping[str, Any]) -> dict[str, Any]:
        limit, offset = _bounded_page(request)
        # The public closed MCP schema names this filter ``category``.  Keep
        # the database vocabulary private instead of silently dropping it.
        family = _filter(request, "category")
        status = _filter(request, "status")
        result = cursor.execute(
            """
            SELECT item_id,queue_family,source_ref,lane,status,priority_text,available_at,
                   source_updated_at,imported_at
              FROM region_talk.queue_v2
             WHERE (%s IS NULL OR queue_family=%s) AND (%s IS NULL OR status=%s)
             ORDER BY imported_at DESC,item_id LIMIT %s OFFSET %s
            """,
            (family, family, status, status, limit, offset),
        )
        return {"items": _rows(cursor, result)}

    @staticmethod
    def queue_summary(cursor: Any) -> dict[str, Any]:
        result = cursor.execute(
            """
            SELECT queue_family,status,item_count,oldest_imported_at,latest_source_update
              FROM region_talk.queue_summary_v2
             ORDER BY queue_family,status NULLS FIRST
            """
        )
        items = _rows(cursor, result)
        return {"items": items, "total_items": sum(int(item["item_count"]) for item in items)}

    @staticmethod
    def list_publication_queue(cursor: Any, request: Mapping[str, Any]) -> dict[str, Any]:
        """Read the executable canonical queue, never the raw landing tables."""

        limit, offset = _bounded_page(request)
        status = _filter(request, "status")
        channel = _filter(request, "channel")
        result = cursor.execute(
            """
            SELECT candidate_id,candidate_status,current_revision,content_id,content_type,
                   title,summary,canonical_url,publication_plan_id,channel,plan_status,
                   scheduled_for,legacy_status,canonical_revision,updated_at,
                   review_decision,review_actor_ref,review_reason,review_occurred_at
              FROM region_talk.publication_queue_v3
             WHERE (%s IS NULL OR candidate_status=%s OR plan_status=%s)
               AND (%s IS NULL OR channel=%s)
             ORDER BY coalesce(scheduled_for,updated_at) DESC,candidate_id
             LIMIT %s OFFSET %s
            """,
            (status, status, status, channel, channel, limit, offset),
        )
        return {"items": _rows(cursor, result)}

    @staticmethod
    def publication_queue_summary(cursor: Any) -> dict[str, Any]:
        result = cursor.execute(
            """
            SELECT candidate_status,plan_status,channel,item_count,
                   earliest_scheduled_for,latest_update
              FROM region_talk.publication_queue_summary_v3
             ORDER BY candidate_status,plan_status NULLS FIRST,channel NULLS FIRST
            """
        )
        items = _rows(cursor, result)
        return {"items": items, "total_items": sum(int(item["item_count"]) for item in items)}

    @staticmethod
    def _list_content(
        cursor: Any,
        view: str,
        request: Mapping[str, Any],
        *,
        posts: bool = False,
    ) -> dict[str, Any]:
        if view not in {"articles_v2", "posts_v2"}:  # internal fail-closed guard
            raise ValueError("unsupported Region Talk content view")
        limit, offset = _bounded_page(request)
        status = _filter(request, "status")
        category = _filter(request, "category")
        platform = _filter(request, "platform") if posts else None
        platform_column = ",platform,external_id" if posts else ""
        result = cursor.execute(
            f"""
            SELECT item_id,title,summary,exact_url,category,status{platform_column},
                   source_updated_at,imported_at
              FROM region_talk.{view}
             WHERE (%s IS NULL OR status=%s) AND (%s IS NULL OR category=%s)
                   AND (%s IS NULL OR {'platform=%s' if posts else '%s IS NULL'})
             ORDER BY coalesce(source_updated_at,imported_at) DESC,item_id LIMIT %s OFFSET %s
            """,
            (status, status, category, category, platform, platform, limit, offset),
        )
        return {"items": _rows(cursor, result)}

    @staticmethod
    def _get_content(
        cursor: Any,
        view: str,
        item_id: UUID,
        *,
        posts: bool = False,
    ) -> dict[str, Any] | None:
        if view not in {"articles_v2", "posts_v2"}:
            raise ValueError("unsupported Region Talk content view")
        platform_column = ",platform,external_id" if posts else ""
        result = cursor.execute(
            f"""
            SELECT item_id,title,summary,body_text,exact_url,category,status{platform_column},
                   source_updated_at,imported_at
              FROM region_talk.{view} WHERE item_id=%s
            """,
            (item_id,),
        )
        return _one(cursor, result)

    @staticmethod
    def _search_content(
        cursor: Any,
        view: str,
        request: Mapping[str, Any],
        *,
        posts: bool = False,
    ) -> dict[str, Any]:
        if view not in {"articles_v2", "posts_v2"}:
            raise ValueError("unsupported Region Talk content view")
        limit, offset = _bounded_page(request)
        query = _query(request)
        status = _filter(request, "status")
        category = _filter(request, "category")
        platform = _filter(request, "platform") if posts else None
        platform_column = ",platform,external_id" if posts else ""
        result = cursor.execute(
            f"""
            SELECT item_id,title,summary,exact_url,category,status{platform_column},
                   source_updated_at,imported_at
              FROM region_talk.{view}
             WHERE to_tsvector('pg_catalog.russian',coalesce(title,'') || ' ' || coalesce(summary,''))
                       @@ websearch_to_tsquery('pg_catalog.russian',%s)
               AND (%s IS NULL OR status=%s) AND (%s IS NULL OR category=%s)
               AND (%s IS NULL OR {'platform=%s' if posts else '%s IS NULL'})
             ORDER BY coalesce(source_updated_at,imported_at) DESC,item_id LIMIT %s OFFSET %s
            """,
            (query, status, status, category, category, platform, platform, limit, offset),
        )
        return {"items": _rows(cursor, result)}
