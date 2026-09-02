from __future__ import annotations

import json
from pathlib import Path

from my_data_hub.showcase.manager import ShowcaseManager
from my_data_hub.showcase.models import BuildReceipt, ShowcaseBundle
from my_data_hub.showcase.source import FilesystemShowcaseSource
from my_data_hub.showcase.state import ShowcaseStateStore

FIXTURES = Path(__file__).parent / "fixtures"


class FakeBuilder:
    def build(self, bundle: ShowcaseBundle, *, slug: str, output_dir: Path) -> BuildReceipt:
        output_dir.mkdir(parents=True)
        (output_dir / "index.html").write_text("checked", encoding="utf-8")
        return BuildReceipt(
            view_id=bundle.view.id,
            source_revision=bundle.source_revision,
            slug=slug,
            url=f"https://ideas.example/v/{slug}/",
            tree_sha256="a" * 64,
            file_count=1,
            html_count=1,
        )


class FakePublisher:
    def __init__(self) -> None:
        self.published: list[str] = []
        self.revoked: list[str] = []

    def publish(self, source: Path, receipt: BuildReceipt) -> str:
        assert (source / "index.html").exists()
        self.published.append(receipt.slug)
        return f"https://ideas.example/v/{receipt.slug}/"

    def revoke(self, *, view_id: str, slug: str) -> None:
        assert view_id
        self.revoked.append(slug)


def manager(tmp_path: Path) -> tuple[ShowcaseManager, FakePublisher]:
    publisher = FakePublisher()
    return (
        ShowcaseManager(
            source=FilesystemShowcaseSource(FIXTURES),
            state=ShowcaseStateStore(tmp_path / "state.json"),
            builder=FakeBuilder(),  # type: ignore[arg-type]
            publisher=publisher,
            origin="https://ideas.example",
        ),
        publisher,
    )


def test_rebuild_preserves_secret_link(tmp_path: Path) -> None:
    control, publisher = manager(tmp_path)
    first = control.rebuild("main")
    second = control.rebuild("main")
    assert first["url"] == second["url"]
    assert publisher.published[0] == publisher.published[1]
    state = json.loads((tmp_path / "state.json").read_text(encoding="utf-8"))
    assert state["surfaces"]["main"]["last_build"]["url"] == first["url"]


def test_rotation_publishes_new_link_and_revokes_old(tmp_path: Path) -> None:
    control, publisher = manager(tmp_path)
    first = control.rebuild("main")
    rotated = control.rotate_link("main")
    assert first["url"] != rotated["url"]
    assert rotated["old_url_revoked"] == first["url"]
    assert publisher.revoked == [first["url"].split("/")[-2]]
    assert control.get_link("main")["url"] == rotated["url"]


def test_create_separate_surface_and_revoke(tmp_path: Path) -> None:
    control, publisher = manager(tmp_path)
    main = control.rebuild("main")
    audience = control.create_view("lecturers-guides")
    assert audience["created"] is True
    assert main["url"] != audience["url"]
    revoked = control.revoke_link("lecturers-guides")
    assert revoked["status"] == "revoked"
    assert control.get_link("lecturers-guides")["url"] is None
    assert publisher.revoked
