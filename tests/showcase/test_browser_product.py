"""Real Chromium against Astro published by the real constructor, without live services.

Run: SHOWCASE_BROWSER=1 SHOWCASE_CHROMIUM=/usr/bin/chromium pytest -q tests/showcase/test_browser_product.py
On CI omit SHOWCASE_CHROMIUM after `playwright install --with-deps chromium`.
"""

from __future__ import annotations

import functools
import os
import shutil
import threading
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

from my_data_hub.showcase.builder import AstroShowcaseBuilder
from my_data_hub.showcase.manager import ShowcaseManager
from my_data_hub.showcase.publisher import LocalDirectoryPublisher
from my_data_hub.showcase.runtime import ShowcaseOperationController, ShowcaseOperationJournal
from my_data_hub.showcase.state import ShowcaseStateStore
from tests.showcase.test_product_constructor import FIXTURES, LocalTestWriter, invoke

pytestmark = pytest.mark.skipif(os.getenv("SHOWCASE_BROWSER") != "1", reason="opt-in real browser suite")
SITE = Path(__file__).resolve().parents[2] / "showcase-site"


@pytest.fixture(scope="module")
def live(tmp_path_factory):
    from playwright.sync_api import sync_playwright

    root = tmp_path_factory.mktemp("showcase-browser")
    source = root / "source"
    shutil.copytree(FIXTURES, source)
    writer = LocalTestWriter(source)
    public = root / "public"
    public.mkdir()

    class Handler(SimpleHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def end_headers(self):
            self.send_header(
                "Content-Security-Policy",
                "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self'; object-src 'none'",
            )
            self.send_header("Referrer-Policy", "no-referrer")
            super().end_headers()

    server = ThreadingHTTPServer(("127.0.0.1", 0), functools.partial(Handler, directory=str(public)))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    origin = f"http://127.0.0.1:{server.server_port}"
    manager = ShowcaseManager(
        source=writer.source,
        writer=writer,
        builder=AstroShowcaseBuilder(site_root=SITE, origin=origin),
        publisher=LocalDirectoryPublisher(root=public, origin=origin),
        state=ShowcaseStateStore(root / "state.json"),
        origin=origin,
    )
    controller = ShowcaseOperationController(manager, ShowcaseOperationJournal(root / "operations.json"))
    original = manager.get_source("main")
    result = invoke(
        controller,
        "create_view",
        view_id="browser-showcase",
        mode="publish",
        idempotency_key="browser-create-key",
        view={
            "title": "ИИ для медицинских и операционных команд",
            "subtitle": (
                "Исследования, материалы и документы: выберите задачу и посмотрите, какой результат можно получить."
            ),
            "item_ids": original["view"]["item_ids"],
            "contact": {"value": "@confidentmax", "href": "https://t.me/confidentmax"},
        },
    )
    assert result["status"] == "published"
    with sync_playwright() as pw:
        args = {"headless": True}
        if os.getenv("SHOWCASE_CHROMIUM"):
            args["executable_path"] = os.environ["SHOWCASE_CHROMIUM"]
        browser = pw.chromium.launch(**args)
        yield browser, result["url"], manager, controller, root
        browser.close()
    server.shutdown()
    server.server_close()


@pytest.mark.parametrize("width,height", [(360, 800), (390, 844), (1440, 900)])
def test_compact_page_search_details_interest_and_png(live, width, height):
    from playwright.sync_api import expect

    browser, url, _manager, _controller, root = live
    context = browser.new_context(viewport={"width": width, "height": height})
    page = context.new_page()
    errors = []
    page.on("pageerror", lambda error: errors.append(str(error)))
    page.goto(url)
    cards = page.locator("[data-showcase-card]")
    expect(cards).to_have_count(5)
    expect(page.locator("#showcase-filter-panel")).to_be_hidden()
    first = cards.first.bounding_box()
    assert first["y"] < height * 0.65
    assert page.evaluate("document.documentElement.scrollWidth <= innerWidth")
    expect(cards.first.locator(".card-number")).to_have_text("#1")
    expect(cards.first.locator(".card-result")).not_to_be_empty()
    search = page.get_by_role("searchbox")
    search.fill("голос")
    expect(page.locator("#showcase-filter-panel")).to_be_visible()
    assert page.locator("[data-showcase-card]:visible").count() >= 1
    search.fill("does-not-exist-xyz")
    expect(page.locator("#showcase-empty")).to_be_visible()
    page.locator("#showcase-empty button").click()
    expect(page.locator("[data-showcase-card]:visible")).to_have_count(5)
    cards.first.locator("[data-interest-id]").click()
    expect(cards.first.locator("[data-interest-id]")).to_have_attribute("aria-pressed", "true")
    first_url = cards.first.locator(".idea-card__link").get_attribute("href")
    cards.first.locator(".idea-card__link").click()
    expect(page.locator("[data-interest-id]")).to_have_attribute("aria-pressed", "true")
    assert page.locator('meta[property="og:url"]').get_attribute("content").endswith(first_url)
    image = page.locator('meta[property="og:image"]').get_attribute("content")
    response = page.request.get(image)
    assert response.status == 200 and response.body().startswith(b"\x89PNG\r\n\x1a\n")
    import struct

    assert struct.unpack(">II", response.body()[16:24]) == (1200, 630)
    page.locator("[data-interest-message] summary").click()
    expect(page.locator("[data-interest-text]")).to_have_value(__import__("re").compile(r"#1.*", __import__("re").S))
    assert first_url in page.locator("[data-interest-text]").input_value()
    assert page.locator(".contact-links a").first.get_attribute("href") == "https://t.me/confidentmax"
    assert not errors
    page.get_by_role("link", name="Назад к витрине").click()
    page.reload()
    expect(page.locator("[data-interest-id]").first).to_have_attribute("aria-pressed", "true")
    screenshots = Path(os.getenv("SHOWCASE_SCREENSHOTS", str(root / "screenshots")))
    screenshots.mkdir(parents=True, exist_ok=True)
    page.screenshot(path=str(screenshots / f"showcase-{width}.png"), full_page=True)
    context.close()


def test_card_share_uses_card_url_abort_no_copy_and_clipboard_fallback(live):
    from playwright.sync_api import expect

    browser, url, *_ = live
    context = browser.new_context(viewport={"width": 390, "height": 844})
    context.add_init_script("""window.shared = []; window.copied = [];
      Object.defineProperty(navigator, 'share', { configurable: true, value: async (data) => {
        window.shared.push(data); } });
      Object.defineProperty(navigator, 'canShare', { configurable: true, value: () => true });
      Object.defineProperty(navigator, 'clipboard', { configurable: true, value: { writeText: async (text) => {
        window.copied.push(text); } } });""")
    page = context.new_page()
    page.goto(url)
    card = page.locator("[data-showcase-card]").first
    detail = card.locator(".idea-card__link").get_attribute("href")
    # Wait for same-origin PNG prefetch; native-share API is stubbed, rendering/fetch are real.
    page.wait_for_function("performance.getEntriesByType('resource').some(e => e.name.includes('/share/cards/'))")
    card.locator("[data-share-button]").click()
    page.wait_for_function("window.shared.length === 1")
    assert page.evaluate("window.shared[0].url").endswith(detail)
    assert page.evaluate("window.copied.length") == 0
    page.evaluate(
        "Object.defineProperty(navigator, 'share', {value: async () => { "
        "throw new DOMException('cancel', 'AbortError'); }, configurable:true})"
    )
    card.locator("[data-share-button]").click()
    expect(card.locator("[data-share-status]")).to_have_text("Поделиться отменено")
    assert page.evaluate("window.copied.length") == 0
    page.evaluate("Object.defineProperty(navigator, 'share', {value: undefined, configurable:true})")
    card.locator("[data-share-button]").click()
    assert page.evaluate("window.copied[0]").endswith(detail)
    page.evaluate("Object.defineProperty(navigator, 'clipboard', {value: undefined, configurable:true})")
    card.locator("[data-share-button]").click()
    expect(card.locator("[data-share-fallback]")).to_be_visible()
    assert card.locator("textarea").input_value().endswith(detail)
    context.close()


def test_mcp_update_rebuild_refresh_same_link_and_no_irrelevant_filters(live):
    from playwright.sync_api import expect

    browser, url, manager, control, _ = live
    current = manager.get_source("browser-showcase")
    view = {**current["view"], "title": "Новая подборка без лишних фильтров", "filters": []}
    result = invoke(
        control,
        "apply",
        view_id="browser-showcase",
        expected_source_revision=current["source_revision"],
        view=view,
        mode="publish",
        idempotency_key="browser-update-key",
    )
    assert result["url"] == url
    page = browser.new_page()
    page.goto(url)
    expect(page.locator("h1")).to_have_text(view["title"])
    assert page.locator("[data-filter-toggle]").count() == 0
    assert page.locator("[data-showcase-filter]").count() == 0
    page.close()
