from __future__ import annotations

from uuid import UUID

import pytest
from pydantic import ValidationError

from my_data_hub.workloads.bloggers.discovery_reader import (
    BloggerDiscoveryReader,
    BloggerSearchRequest,
)


class _Cursor:
    def __init__(self) -> None:
        self.statement = ""
        self.parameters: tuple[object, ...] = ()

    def execute(self, statement: str, parameters: tuple[object, ...]):
        self.statement = statement
        self.parameters = parameters
        return self

    def fetchall(self):  # type: ignore[no-untyped-def]
        return [
            {
                "blogger_id": "11111111-1111-4111-8111-111111111111",
                "display_name": "Автор",
                "actor_kind": "person",
                "public_description": None,
                "public_accounts": [
                    {"platform": "telegram", "handle": "author", "url": "https://t.me/author"}
                ],
                "requires_review": False,
            }
        ]


def test_reader_is_fixed_bounded_and_returns_only_projection_fields() -> None:
    cursor = _Cursor()
    result = BloggerDiscoveryReader.search(
        cursor,
        BloggerSearchRequest(project_slug="region-talk", query="Автор", limit=10),
    )
    assert result[0].blogger_id == UUID("11111111-1111-4111-8111-111111111111")
    assert "FROM hub.bloggers_v1" in cursor.statement
    assert cursor.parameters[-1] == 10
    assert "raw_payload" not in cursor.statement
    assert "evidence" not in cursor.statement


def test_reader_rejects_unbounded_or_partial_cursor_input() -> None:
    with pytest.raises(ValidationError):
        BloggerSearchRequest(project_slug="region-talk", limit=101)
    with pytest.raises(ValidationError, match="extra_forbidden"):
        BloggerSearchRequest.model_validate(
            {"project_slug": "region-talk", "sql": "select * from hub.actor"}
        )
    with pytest.raises(ValueError, match="both reader cursor fields"):
        BloggerDiscoveryReader.search(
            _Cursor(),
            BloggerSearchRequest(project_slug="region-talk", after_name="Автор"),
        )
