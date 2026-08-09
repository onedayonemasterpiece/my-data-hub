from __future__ import annotations

import pytest

from my_data_hub.joplin.bridge import JoplinNoteSnapshot, decide_sync
from my_data_hub.joplin.provider import HttpJoplinDataApi, JoplinProviderError


def snapshot(*, deleted_time: int = 0) -> JoplinNoteSnapshot:
    return JoplinNoteSnapshot(
        note_id="note-1",
        title="Title",
        body="Body",
        updated_time=123,
        deleted_time=deleted_time,
    )


def test_joplin_sync_decisions_are_explicit() -> None:
    current = snapshot()
    assert decide_sync(
        current,
        last_joplin_hash=current.content_hash,
        last_hub_revision=1,
        hub_changed_since_revision=False,
    ) == "no_change"
    assert decide_sync(
        current,
        last_joplin_hash=None,
        last_hub_revision=None,
        hub_changed_since_revision=False,
    ) == "import"
    assert decide_sync(
        current,
        last_joplin_hash=current.content_hash,
        last_hub_revision=1,
        hub_changed_since_revision=True,
    ) == "push"
    assert decide_sync(
        current,
        last_joplin_hash="0" * 64,
        last_hub_revision=1,
        hub_changed_since_revision=True,
    ) == "conflict"
    assert decide_sync(
        snapshot(deleted_time=1),
        last_joplin_hash=None,
        last_hub_revision=None,
        hub_changed_since_revision=True,
    ) == "tombstone"


def test_joplin_http_provider_is_loopback_only_by_default() -> None:
    provider = HttpJoplinDataApi("http://127.0.0.1:41184", "secret")
    assert provider.base_url.startswith("http://127.0.0.1")
    with pytest.raises(JoplinProviderError, match="non-loopback"):
        HttpJoplinDataApi("http://192.0.2.10:41184", "secret")
    with pytest.raises(JoplinProviderError, match="local HTTP"):
        HttpJoplinDataApi("https://127.0.0.1:41184", "secret")
    with pytest.raises(JoplinProviderError, match="token"):
        HttpJoplinDataApi("http://localhost:41184", "")


def test_joplin_snapshot_normalisation() -> None:
    snap = HttpJoplinDataApi._snapshot(
        {"id": "abc", "title": None, "body": "body", "updated_time": "42"}
    )
    assert snap.note_id == "abc"
    assert snap.title == ""
    assert snap.updated_time == 42
