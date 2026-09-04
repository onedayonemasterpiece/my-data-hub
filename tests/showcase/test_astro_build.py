import json
import re
import subprocess
import textwrap
from pathlib import Path

import pytest

from my_data_hub.showcase.builder import AstroShowcaseBuilder
from my_data_hub.showcase.source import FilesystemShowcaseSource

FIXTURES = Path(__file__).parent / "fixtures"
REPOSITORY = Path(__file__).resolve().parents[2]
SITE = REPOSITORY / "showcase-site"
NGINX = REPOSITORY / "deploy" / "showcase-runtime" / "nginx.conf"


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
    script_tags = re.findall(r"<script\b([^>]*)>([\s\S]*?)</script>", index)
    assert script_tags
    assert all(" src=" in attributes and not body.strip() for attributes, body in script_tags)
    expected_asset = re.compile(rf'^/v/{re.escape(slug)}/_assets/[^"/]+\.js$')
    script_srcs = re.findall(r'<script\b[^>]*\bsrc="([^"]+)"', index)
    same_origin_srcs = [source for source in script_srcs if expected_asset.fullmatch(source)]
    assert len(same_origin_srcs) == 2
    assets = []
    for script_src in same_origin_srcs:
        relative_asset = script_src.removeprefix(f"/v/{slug}/")
        asset = tmp_path / "dist" / relative_asset
        assert asset.is_file()
        assets.append(asset.read_text(encoding="utf-8"))
    asset_source = "\n".join(assets)
    for marker in ("data-showcase-filter", "showcase-result-count", "addEventListener", "navigator.share"):
        assert marker in asset_source
    detail_path = tmp_path / "dist" / "ideas" / "voice-cloning-audioguides" / "index.html"
    assert detail_path.exists()
    detail = detail_path.read_text(encoding="utf-8")
    assert f'href="/v/{slug}/"' in detail
    assert same_origin_srcs[0] not in detail
    assert (tmp_path / "dist" / "showcase-headers.json").exists()


def test_showcase_csp_allows_only_same_origin_scripts() -> None:
    nginx = NGINX.read_text(encoding="utf-8")
    csp = next(line for line in nginx.splitlines() if "Content-Security-Policy" in line)
    assert "default-src 'self'" in csp
    assert "script-src 'self'" in csp
    assert not re.search(r"script-src[^;\"]*'unsafe-inline'", csp)


def test_showcase_asset_filters_category_readiness_and_search() -> None:
    asset = SITE / "src" / "scripts" / "showcase-filters.js"
    scenario = textwrap.dedent(
        f"""
        const fs = require('node:fs');
        const vm = require('node:vm');
        const listeners = new Map();
        const search = {{ value: '', addEventListener: (name, fn) => listeners.set(`search:${{name}}`, fn) }};
        const filters = ['audience', 'category', 'capabilityType', 'maturity', 'effort'].map((name) => ({{
          value: '',
          dataset: {{ showcaseFilter: name }},
          addEventListener: (event, fn) => listeners.set(`${{name}}:${{event}}`, fn),
        }}));
        const cards = [
          {{ dataset: {{
            audience: 'guides', category: 'voice', capabilityType: 'technical', maturity: 'prototype', effort: 'medium',
            search: 'клонирование голоса аудиогиды',
          }}, hidden: false }},
          {{ dataset: {{
            audience: 'business', category: 'business', capabilityType: 'business',
            maturity: 'designed', effort: 'high',
            search: 'персональный помощник аналитика',
          }}, hidden: false }},
          {{ dataset: {{
            audience: 'media', category: 'content', capabilityType: 'product',
            maturity: 'idea', effort: 'medium', search: 'короткие видео',
          }}, hidden: false }},
        ];
        const count = {{ textContent: '3 возможностей' }};
        const empty = {{ hidden: true }};
        global.document = {{
          querySelector: (selector) =>
            selector === '#showcase-search' ? search :
            selector === '#showcase-result-count' ? count : empty,
          querySelectorAll: (selector) => selector === '[data-showcase-filter]' ? filters : cards,
        }};
        vm.runInThisContext(fs.readFileSync({json.dumps(str(asset))}, 'utf8'));
        const snapshot = () => ({{ count: count.textContent, visible: cards.filter((card) => !card.hidden).length }});
        filters[1].value = 'voice'; listeners.get('category:change')();
        const category = snapshot();
        filters[1].value = ''; filters[3].value = 'designed'; listeners.get('maturity:change')();
        const readiness = snapshot();
        filters[3].value = ''; filters[2].value = 'product'; listeners.get('capabilityType:change')();
        const capability = snapshot();
        filters[2].value = ''; search.value = 'видео'; listeners.get('search:input')();
        const searchResult = snapshot();
        process.stdout.write(JSON.stringify({{ category, readiness, capability, searchResult }}));
        """
    )
    completed = subprocess.run(
        ["node", "-e", scenario],
        check=True,
        capture_output=True,
        text=True,
    )
    assert json.loads(completed.stdout) == {
        "category": {"count": "1 возможность", "visible": 1},
        "readiness": {"count": "1 возможность", "visible": 1},
        "capability": {"count": "1 возможность", "visible": 1},
        "searchResult": {"count": "1 возможность", "visible": 1},
    }


def test_share_controls_and_static_svg_routes_exist() -> None:
    index = (SITE / "src/pages/index.astro").read_text(encoding="utf-8")
    detail = (SITE / "src/pages/ideas/[id].astro").read_text(encoding="utf-8")
    share = (SITE / "src/scripts/showcase-share.js").read_text(encoding="utf-8")
    assert "ShareButton" in index and "ShareButton" in detail
    assert 'data-showcase-filter="capabilityType"' in index
    for value in ("technical", "product", "business"):
        assert f'<option value="{value}"' in index
    assert "navigator.share" in share and "navigator.canShare" in share and "clipboard" in share
    assert (SITE / "src/pages/share/idea-hub.svg.ts").is_file()
    assert (SITE / "src/pages/share/[id].svg.ts").is_file()


def test_production_telegram_cta_contract_is_preserved() -> None:
    contract = (REPOSITORY / "docs" / "ideahub-showcase.md").read_text(encoding="utf-8")
    assert "@confidentmax" in contract
    assert "https://t.me/confidentmax" in contract


def test_share_asset_has_image_payload_and_truthful_copy_fallback() -> None:
    share = (SITE / "src/scripts/showcase-share.js").read_text(encoding="utf-8")
    assert "files: [file]" in share
    assert "const copiedText" in share
    assert "await navigator.clipboard.writeText" in share
    assert "Текст и ссылка скопированы" in share
    assert "скопировать текст и ссылку" in share
    assert "AbortError" in share and "Поделиться отменено" in share
