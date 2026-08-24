from __future__ import annotations

from typing import Any
from uuid import UUID

import pytest

from my_data_hub.workloads.region_talk.reader import RegionTalkReader


class Column:
    def __init__(self, name: str) -> None:
        self.name = name


class Cursor:
    def __init__(self, rows: list[tuple[Any, ...]], names: list[str]) -> None:
        self.rows = rows
        self.description = [Column(name) for name in names]
        self.statement = ""
        self.params: tuple[Any, ...] = ()

    def execute(self, statement: str, params: tuple[Any, ...] = ()) -> Cursor:
        self.statement = statement
        self.params = params
        return self

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self.rows

    def fetchone(self) -> tuple[Any, ...] | None:
        return self.rows[0] if self.rows else None


def test_article_list_is_bounded_and_omits_body_and_raw_payload() -> None:
    cursor = Cursor(
        [(UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"), "Title")],
        ["item_id", "title"],
    )
    result = RegionTalkReader.list_articles(cursor, {"limit": 20, "offset": 40})
    assert result["items"][0]["title"] == "Title"
    assert "body_text" not in cursor.statement
    assert "payload" not in cursor.statement
    assert cursor.params[-2:] == (20, 40)


def test_article_get_returns_typed_body_without_raw_json() -> None:
    item_id = UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
    cursor = Cursor([(item_id, "Title", "Body")], ["item_id", "title", "body_text"])
    result = RegionTalkReader.get_article(cursor, item_id)
    assert result == {"item_id": item_id, "title": "Title", "body_text": "Body"}
    assert "payload" not in cursor.statement


def test_search_requires_nonempty_query_and_never_interpolates_it() -> None:
    cursor = Cursor([], ["item_id"])
    with pytest.raises(ValueError, match="search query"):
        RegionTalkReader.search_posts(cursor, {"query": " "})
    RegionTalkReader.search_posts(cursor, {"query": "Калининград"})
    assert "Калининград" not in cursor.statement
    assert cursor.params[0] == "Калининград"


def test_queue_summary_is_typed_and_aggregated() -> None:
    cursor = Cursor(
        [("source_frontier", "pending", 7)],
        ["queue_family", "status", "item_count"],
    )
    result = RegionTalkReader.queue_summary(cursor)
    assert result["total_items"] == 7
    assert result["items"][0]["queue_family"] == "source_frontier"


def test_public_category_filter_reaches_private_queue_family_column() -> None:
    cursor = Cursor([], ["item_id"])
    RegionTalkReader.list_queue(
        cursor, {"category": "publication_schedule", "status": "planned"}
    )
    assert cursor.params[:4] == (
        "publication_schedule",
        "publication_schedule",
        "planned",
        "planned",
    )


def test_publication_queue_reader_uses_only_fixed_canonical_view() -> None:
    cursor = Cursor(
        [(UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"), "ready", 1)],
        ["candidate_id", "candidate_status", "current_revision"],
    )
    result = RegionTalkReader.list_publication_queue(
        cursor, {"status": "ready", "channel": "region-talk-new-channel"}
    )
    assert result["items"][0]["candidate_status"] == "ready"
    assert "region_talk.publication_queue_v3" in cursor.statement
    assert "migration.raw_record" not in cursor.statement


@pytest.mark.parametrize("page", [{"limit": 0}, {"limit": 101}, {"offset": -1}, {"offset": 10001}])
def test_reader_rejects_unbounded_pages(page: dict[str, int]) -> None:
    with pytest.raises(ValueError, match="bounded contract"):
        RegionTalkReader.list_articles(Cursor([], ["item_id"]), page)
