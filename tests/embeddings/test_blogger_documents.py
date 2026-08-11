from uuid import UUID

import pytest

from my_data_hub.embeddings.blogger_documents import (
    BloggerDocumentError,
    build_compact_blogger_documents,
)


def _row(number: int):
    return {
        "blogger_id": f"00000000-0000-4000-8000-{number:012d}",
        "display_name": f" Блогер   {number} ",
        "actor_kind": "person",
        "public_description": "Калининград и путешествия",
        "geography_signal": "Калининград",
        "project_id": "10000000-0000-4000-8000-000000000001",
        "public_accounts": [
            {"platform": "telegram", "handle": f"blogger{number}", "url": None},
            {"url": f"https://example.test/{number}", "platform": "web", "handle": None},
        ],
    }


def test_compact_documents_are_order_and_whitespace_stable() -> None:
    first = build_compact_blogger_documents([_row(2), _row(1)], expected_count=2)
    second = build_compact_blogger_documents([_row(1), _row(2)], expected_count=2)
    assert first == second
    assert [item.actor_id for item in first] == [UUID(str(_row(1)["blogger_id"])), UUID(str(_row(2)["blogger_id"]))]
    assert first[0].document.representation_kind == "blogger_compact_v1"
    assert first[0].document.compact_text().startswith("name: Блогер 1\nactor_kind: person")


def test_compact_documents_fail_closed_on_count_duplicate_or_private_field() -> None:
    with pytest.raises(BloggerDocumentError, match="count"):
        build_compact_blogger_documents([_row(1)], expected_count=2)
    with pytest.raises(BloggerDocumentError, match="duplicate"):
        build_compact_blogger_documents([_row(1), _row(1)], expected_count=2)
    bad = _row(1)
    bad["public_accounts"] = [{"platform": "telegram", "handle": "a", "token": "secret"}]
    with pytest.raises(BloggerDocumentError, match="unapproved"):
        build_compact_blogger_documents([bad], expected_count=1)
