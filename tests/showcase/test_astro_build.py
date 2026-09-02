from pathlib import Path

import pytest

from my_data_hub.showcase.builder import AstroShowcaseBuilder
from my_data_hub.showcase.source import FilesystemShowcaseSource

FIXTURES = Path(__file__).parent / "fixtures"
REPOSITORY = Path(__file__).resolve().parents[2]
SITE = REPOSITORY / "showcase-site"


@pytest.mark.skipif(not (SITE / "node_modules").is_dir(), reason="run npm ci in showcase-site")
def test_real_astro_build_is_checked_and_prefixed(tmp_path: Path) -> None:
    bundle = FilesystemShowcaseSource(FIXTURES).load_bundle("main")
    slug = "abcdefghijklmnopqrstuvwx_12345678"
    receipt = AstroShowcaseBuilder(site_root=SITE, origin="https://ideas.example").build(
        bundle,
        slug=slug,
        output_dir=tmp_path / "dist",
    )
    assert receipt.html_count == 6
    index = (tmp_path / "dist" / "index.html").read_text(encoding="utf-8")
    assert "noindex" in index
    assert f"/v/{slug}/ideas/voice-cloning-audioguides/" in index
    assert (tmp_path / "dist" / "ideas" / "voice-cloning-audioguides" / "index.html").exists()
    assert (tmp_path / "dist" / "showcase-headers.json").exists()
