from __future__ import annotations

from pathlib import Path

import pytest

from my_data_hub.showcase.manager import ShowcaseManager
from my_data_hub.showcase.models import ShowcaseBundle
from my_data_hub.showcase.source import FilesystemShowcaseSource, ShowcaseSourceError, ShowcaseSourceNotFoundError
from my_data_hub.showcase.state import ShowcaseStateStore
from tests.showcase.test_manager import FakeBuilder, FakePublisher

FIXTURES = Path(__file__).parent / "fixtures"
REVISION = "c" * 40


class MutableSource:
    def __init__(self, initial: ShowcaseBundle) -> None:
        self.bundles = {initial.view.id: initial}

    def get_source(self, view_id: str) -> ShowcaseBundle:
        try:
            return self.bundles[view_id]
        except KeyError as exc:
            raise ShowcaseSourceNotFoundError("showcase source is absent") from exc

    load_bundle = get_source


class Writer:
    def __init__(self, source: MutableSource, bundle: ShowcaseBundle) -> None:
        self.source = source
        self.bundle = bundle
        self.create_calls = 0
        self.apply_calls = 0

    def create(self, *, view_id: str, files: dict[str, str], message: str) -> str:
        self.create_calls += 1
        assert f"views/{view_id}.yaml" in files
        self.source.bundles[view_id] = self.bundle.model_copy(update={"source_revision": REVISION})
        return REVISION

    def apply(self, *, expected_revision: str, files: dict[str, str], message: str) -> str:
        self.apply_calls += 1
        current = self.source.get_source(self.bundle.view.id)
        self.source.bundles[self.bundle.view.id] = current.model_copy(update={"source_revision": REVISION})
        return REVISION


def proposed_bundle() -> ShowcaseBundle:
    original = FilesystemShowcaseSource(FIXTURES).load_bundle("main")
    item = original.items[0].model_copy(update={"capability_type": "technical"})
    view = original.view.model_copy(update={"id": "new-partner", "item_ids": [item.id]})
    return ShowcaseBundle(source_revision="a" * 40, view=view, items=[item])


def control(tmp_path: Path, source: MutableSource, writer: Writer, publisher: FakePublisher | None = None) -> ShowcaseManager:
    return ShowcaseManager(
        source=source, state=ShowcaseStateStore(tmp_path / "state.json"),
        builder=FakeBuilder(), publisher=publisher or FakePublisher(), origin="https://ideas.example", writer=writer  # type: ignore[arg-type]
    )


def test_absent_dry_run_writes_no_source_or_registry(tmp_path: Path) -> None:
    existing = FilesystemShowcaseSource(FIXTURES).load_bundle("main")
    bundle = proposed_bundle()
    source = MutableSource(existing)
    writer = Writer(source, bundle)
    result = control(tmp_path, source, writer).apply("new-partner", expected_source_revision="absent", view=bundle.view, items=bundle.items)
    assert result["status"] == "dry_run"
    assert "new-partner" not in source.bundles
    assert not (tmp_path / "state.json").exists()
    assert writer.create_calls == 0


def test_absent_publish_creates_stable_link_and_update_preserves_it(tmp_path: Path) -> None:
    existing = FilesystemShowcaseSource(FIXTURES).load_bundle("main")
    bundle = proposed_bundle()
    source = MutableSource(existing)
    writer = Writer(source, bundle)
    manager = control(tmp_path, source, writer)
    created = manager.apply("new-partner", expected_source_revision="absent", view=bundle.view, items=bundle.items, dry_run=False, publish=True)
    assert created["status"] == "published"
    link = manager.get_link("new-partner")["url"]
    updated = manager.apply("new-partner", expected_source_revision=REVISION, view=None, items=[], dry_run=False, publish=True)
    assert updated["status"] == "published"
    assert manager.get_link("new-partner")["url"] == link


def test_absent_collision_and_incomplete_bundle_fail_closed(tmp_path: Path) -> None:
    bundle = proposed_bundle()
    source = MutableSource(bundle)
    writer = Writer(source, bundle)
    manager = control(tmp_path, source, writer)
    with pytest.raises(ShowcaseSourceError, match="already exists"):
        manager.apply("new-partner", expected_source_revision="absent", view=bundle.view, items=bundle.items)
    missing = bundle.view.model_copy(update={"id": "other-partner", "item_ids": ["missing-item"]})
    with pytest.raises(ShowcaseSourceError, match="not included"):
        manager.apply("other-partner", expected_source_revision="absent", view=missing, items=[])


def test_publish_failure_is_explicit_and_rebuild_recovers(tmp_path: Path) -> None:
    existing = FilesystemShowcaseSource(FIXTURES).load_bundle("main")
    bundle = proposed_bundle()
    source = MutableSource(existing)
    writer = Writer(source, bundle)
    publisher = FakePublisher()
    manager = control(tmp_path, source, writer, publisher)
    def fail_rebuild(*args: object, **kwargs: object) -> dict[str, object]:
        raise RuntimeError("publisher unavailable")
    manager.rebuild = fail_rebuild  # type: ignore[method-assign]
    result = manager.apply("new-partner", expected_source_revision="absent", view=bundle.view, items=bundle.items, dry_run=False, publish=True)
    assert result["status"] == "applied_not_published"
    assert result["new_source_revision"] == REVISION
    manager.rebuild = ShowcaseManager.rebuild.__get__(manager)  # type: ignore[method-assign]
    recovered = manager.rebuild("new-partner")
    assert recovered["status"] == "published"
