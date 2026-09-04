"""Expanded interaction states, not just the closed list screenshot."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from tests.showcase.test_browser_product import live

__all__ = ["live"]

pytestmark = pytest.mark.skipif(os.getenv("SHOWCASE_BROWSER") != "1", reason="opt-in real browser suite")


@pytest.mark.parametrize("width", [360, 390])
def test_detail_share_menu_stays_inside_screen_and_copies_card(live, width):
    from playwright.sync_api import expect

    browser, url, *_ = live
    context = browser.new_context(viewport={"width": width, "height": 844})
    context.add_init_script("""window.copied = [];
      Object.defineProperty(navigator, 'clipboard', {value: {writeText: async (text) => {
        window.copied.push(text); }}, configurable: true});""")
    page = context.new_page()
    page.goto(url)
    page.locator(".idea-card__link").first.click()
    detail_url = page.url
    page.locator(".share-menu summary").click()
    menu = page.locator(".share-menu__items")
    expect(menu).to_be_visible()
    bounds = menu.bounding_box()
    assert bounds["x"] >= 0 and bounds["x"] + bounds["width"] <= width
    page.locator("[data-share-copy]").click()
    page.wait_for_function("window.copied.length === 1")
    assert page.evaluate("window.copied[0]").endswith(detail_url)
    page.locator(".share-menu summary").click()
    destination = Path(os.getenv("SHOWCASE_SCREENSHOTS", "/tmp/showcase-screenshots"))
    destination.mkdir(parents=True, exist_ok=True)
    page.screenshot(path=str(destination / f"showcase-detail-{width}.png"), full_page=True)
    context.close()


def test_interest_remains_usable_when_storage_is_disabled(live):
    from playwright.sync_api import expect

    browser, url, *_ = live
    context = browser.new_context(viewport={"width": 390, "height": 844})
    context.add_init_script("Storage.prototype.setItem = () => {throw new Error('storage disabled')}")
    page = context.new_page()
    page.goto(url)
    toggle = page.locator("[data-interest-id]").first
    toggle.click()
    expect(toggle).to_have_attribute("aria-pressed", "true")
    page.locator("[data-interest-message] summary").click()
    expect(page.locator("[data-interest-status]")).to_contain_text("только до закрытия страницы")
    toggle.click()
    expect(toggle).to_have_attribute("aria-pressed", "false")
    expect(page.locator("[data-interest-message]")).to_be_hidden()
    context.close()
