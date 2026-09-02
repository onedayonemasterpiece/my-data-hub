from pathlib import Path

from my_data_hub.showcase.models import BuildReceipt
from my_data_hub.showcase.publisher import LocalDirectoryPublisher


def test_local_publisher_replaces_and_revokes_prefix(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "index.html").write_text("v1", encoding="utf-8")
    receipt = BuildReceipt(
        view_id="main",
        source_revision="revision-1",
        slug="abcdefghijklmnopqrstuvwxyz_123456",
        url="https://ideas.example/v/abcdefghijklmnopqrstuvwxyz_123456/",
        tree_sha256="b" * 64,
        file_count=1,
        html_count=1,
    )
    publisher = LocalDirectoryPublisher(root=tmp_path / "public", origin="https://ideas.example")
    url = publisher.publish(source, receipt)
    target = tmp_path / "public" / "v" / receipt.slug / "index.html"
    assert url == receipt.url
    assert target.read_text(encoding="utf-8") == "v1"
    (source / "index.html").write_text("v2", encoding="utf-8")
    publisher.publish(source, receipt)
    assert target.read_text(encoding="utf-8") == "v2"
    publisher.revoke(view_id="main", slug=receipt.slug)
    assert not target.exists()
