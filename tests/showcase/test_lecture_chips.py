# ruff: noqa: RUF001
"""Lecture chips are explicit metadata, never inferred from readiness or prose."""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from my_data_hub.showcase.models import Contact, LectureMetadata, ShowcaseItem, ShowcaseWriteItem
from my_data_hub.showcase.source import FilesystemShowcaseSource

FIXTURES = Path(__file__).parent / "fixtures"


def lecture_item(**changes: object) -> ShowcaseItem:
    raw = FilesystemShowcaseSource(FIXTURES).load_bundle("main").items[0].model_dump()
    return ShowcaseItem.model_validate({**raw, **changes})


@pytest.mark.parametrize(
    "kind,verified,labels",
    [
        ("master", False, ["Мастер-лекция"]),
        ("master", True, ["Мастер-лекция"]),
        ("author", False, ["Авторская"]),
        ("author", True, ["Авторская", "Верификация РО «Знание»"]),
    ],
)
def test_lecture_chip_matrix(kind, verified, labels):
    item = lecture_item(lecture={"kind": kind, "znanie_verified": verified})
    published = item.published(Contact())
    assert [chip["label"] for chip in published["lecture_chips"]] == labels
    # Stored verification is retained, but never leaks into a master lecture chip/search.
    assert item.lecture.znanie_verified is verified
    assert ("верификация ро «знание»" in published["search_text"]) is (kind == "author" and verified)


def test_no_lecture_inference_from_title_readiness_or_publish_state():
    item = lecture_item(title="Мастер-лекция: это только название", publish_state="ready")
    assert item.published(Contact())["lecture_chips"] == []
    assert LectureMetadata(kind="author").chips() == [{"label": "Авторская", "tone": "neutral"}]


@pytest.mark.parametrize("bad", ["false", "true", 1, 0, None])
def test_verification_must_be_a_real_boolean(bad):
    with pytest.raises(ValidationError):
        LectureMetadata(kind="author", znanie_verified=bad)


def test_lecture_kind_is_explicit_and_closed():
    for raw in [{}, {"kind": "lecture"}, {"kind": "author", "verified": True}]:
        with pytest.raises(ValidationError):
            LectureMetadata.model_validate(raw)


def test_multi_audience_chips_are_searchable_and_replace_legacy_group():
    audiences = [{"id": "school", "label": "Старшеклассники"}, {"id": "students", "label": "Студенты"}]
    item = lecture_item(audience={"id": "legacy", "label": "Старая группа"}, audiences=audiences)
    published = item.published(Contact())
    assert published["audiences"] == audiences
    assert "старшеклассники" in published["search_text"] and "студенты" in published["search_text"]
    assert "старая группа" not in published["search_text"]
    # Existing adaptation qualifications are preserved, not replaced by short chips.
    assert published["for_whom"] == item.for_whom
    assert published["requirements"] == item.requirements


@pytest.mark.parametrize("audiences", [None, []])
def test_legacy_single_audience_fallback(audiences):
    item = lecture_item(**({} if audiences is None else {"audiences": audiences}))
    assert item.published(Contact())["audiences"] == [item.audience.model_dump()]


@pytest.mark.parametrize(
    "audiences",
    [
        [{"id": "school", "label": "Школьники"}, {"id": "school", "label": "Студенты"}],
        [{"id": "school", "label": "Школьники"}, {"id": "pupils", "label": "школьники"}],
        [{"id": f"group-{i}", "label": f"Группа {i}"} for i in range(9)],
    ],
)
def test_ambiguous_or_excessive_audiences_are_rejected(audiences):
    with pytest.raises(ValidationError):
        lecture_item(audiences=audiences)


def test_mcp_write_schema_and_source_roundtrip():
    raw = lecture_item(
        lecture={"kind": "author", "znanie_verified": True},
        audiences=[{"id": "students", "label": "Студенты"}],
    ).model_dump()
    raw["capability_type"] = "product"
    item = ShowcaseWriteItem.model_validate(raw)
    assert ShowcaseItem.model_validate_json(item.model_dump_json()).lecture == item.lecture
    schema = ShowcaseWriteItem.model_json_schema()
    assert schema["$defs"]["LectureMetadata"]["properties"]["kind"]["enum"] == ["master", "author"]
    assert "audiences" in schema["properties"]
    with pytest.raises(ValidationError):
        ShowcaseWriteItem.model_validate({**raw, "lecture_chips": [{"label": "Unverified claim"}]})


@pytest.mark.asyncio
async def test_actual_mcp_transports_lecture_and_audiences(tmp_path):
    import shutil

    from my_data_hub.showcase.manager import ShowcaseManager
    from my_data_hub.showcase.mcp_server import create_server
    from my_data_hub.showcase.state import ShowcaseStateStore
    from tests.showcase.test_manager import FakeBuilder, FakePublisher
    from tests.showcase.test_product_constructor import LocalTestWriter

    root = tmp_path / "source"
    shutil.copytree(FIXTURES, root)
    writer = LocalTestWriter(root)
    manager = ShowcaseManager(
        source=writer.source,
        writer=writer,
        builder=FakeBuilder(),
        publisher=FakePublisher(),
        state=ShowcaseStateStore(tmp_path / "state.json"),
        origin="https://ideas.example",
    )
    server = create_server(manager)
    tools = {tool.name: tool for tool in await server.list_tools()}
    for name in ["showcase.create_view", "showcase.apply"]:
        definitions = tools[name].input_schema["$defs"]
        assert "lecture" in definitions["ShowcaseWriteItem"]["properties"]
        assert "audiences" in definitions["ShowcaseWriteItem"]["properties"]
    card = lecture_item(
        id="lecture-roundtrip",
        capability_type="product",
        lecture={"kind": "author", "znanie_verified": True},
        audiences=[{"id": "students", "label": "Студенты"}],
    ).model_dump()
    created = await server.call_tool(
        "showcase.create_view",
        {
            "view_id": "lecture-catalog",
            "mode": "save",
            "idempotency_key": "lecture-chips-create",
            "view": {"title": "Лекции", "subtitle": "Тест метаданных", "item_ids": [card["id"]]},
            "items": [card],
        },
    )
    assert not created.is_error
    read = await server.call_tool("showcase.get_source", {"view_id": "lecture-catalog"})
    assert not read.is_error
    saved = manager.get_source("lecture-catalog")
    assert saved["items"][0]["lecture"] == card["lecture"]
    assert saved["items"][0]["audiences"] == card["audiences"]
    # A metadata-only update uses the same public method, with no new tools.
    card["lecture"]["znanie_verified"] = False
    updated = await server.call_tool(
        "showcase.apply",
        {
            "view_id": "lecture-catalog",
            "expected_source_revision": saved["source_revision"],
            "mode": "save",
            "items": [card],
            "idempotency_key": "lecture-chips-update",
        },
    )
    assert not updated.is_error
    assert manager.get_source("lecture-catalog")["items"][0]["lecture"]["znanie_verified"] is False
