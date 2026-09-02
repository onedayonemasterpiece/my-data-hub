from pathlib import Path

import pytest

from my_data_hub.showcase.source import FilesystemShowcaseSource, ShowcaseSourceError

FIXTURES = Path(__file__).parent / "fixtures"


def test_filesystem_source_builds_exact_public_bundle() -> None:
    bundle = FilesystemShowcaseSource(FIXTURES).load_bundle("main")
    published = bundle.published()
    assert bundle.view.id == "main"
    assert len(bundle.items) == 5
    assert len(bundle.source_revision) == 64
    assert published["view"]["title"] == "Что уже умеем и что можем сделать"
    assert published["items"][0]["id"] == "voice-cloning-audioguides"
    assert "visibility" not in published["items"][0]
    assert "publish_state" not in published["items"][0]
    assert "source_ref" not in str(published)


def test_source_fails_closed_for_missing_view() -> None:
    with pytest.raises(ShowcaseSourceError):
        FilesystemShowcaseSource(FIXTURES).load_bundle("missing-view")
