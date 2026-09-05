"""Real Astro/Chromium list/detail chip parity, no live catalogue changes."""

# ruff: noqa: RUF001
from __future__ import annotations

import os
from pathlib import Path

import pytest

from tests.showcase.test_browser_product import live  # noqa: F401 -- shared browser fixture
from tests.showcase.test_product_constructor import invoke

pytestmark = pytest.mark.skipif(os.getenv("SHOWCASE_BROWSER") != "1", reason="opt-in real browser suite")


@pytest.fixture(scope="module")
def lecture_live(request):
    browser, _url, manager, control, root = request.getfixturevalue("live")
    template = manager.get_source("main")["items"][0]
    cards = []
    variants = [
        ("master", True, ["school", "students"]),
        ("author", True, ["students", "adults"]),
        ("author", False, ["adults"]),
        (None, False, []),
    ]
    labels = {"school": "Старшеклассники", "students": "Студенты", "adults": "Взрослые"}
    for index, (kind, verified, audience_ids) in enumerate(variants):
        cards.append(
            {
                **template,
                "id": f"lecture-chip-{index}",
                "title": f"Пример карточки {index + 1}",
                "capability_type": "product",
                "audience": {"id": "legacy", "label": "Прежняя аудитория"},
                "audiences": [{"id": value, "label": labels[value]} for value in audience_ids],
                "lecture": {"kind": kind, "znanie_verified": verified} if kind else None,
            }
        )
    result = invoke(
        control,
        "create_view",
        view_id="lecture-chips",
        mode="publish",
        idempotency_key="lecture-chips-browser-create",
        items=cards,
        view={
            "title": "Каталог лекций",
            "subtitle": "Тип лекции и целевая аудитория",
            "item_ids": [card["id"] for card in cards],
            "filters": ["audience"],
        },
    )
    assert result["status"] == "published"
    return browser, result["url"], root


@pytest.mark.parametrize("width,height", [(360, 800), (390, 844), (1440, 900)])
def test_lecture_and_audience_chips_list_detail_and_filter(lecture_live, width, height):
    from playwright.sync_api import expect

    browser, url, root = lecture_live
    context = browser.new_context(viewport={"width": width, "height": height})
    page = context.new_page()
    errors = []
    page.on("pageerror", lambda error: errors.append(str(error)))
    page.goto(url)
    cards = page.locator("[data-showcase-card]")
    expected_lecture = [["Мастер-лекция"], ["Авторская", "Верификация РО «Знание»"], ["Авторская"], []]
    expected_audiences = [
        ["Старшеклассники", "Студенты"],
        ["Студенты", "Взрослые"],
        ["Взрослые"],
        ["Прежняя аудитория"],
    ]
    for index in range(4):
        expect(cards.nth(index).locator(".lecture-chips .badge")).to_have_text(expected_lecture[index])
        expect(cards.nth(index).locator(".audience-chips .badge")).to_have_text(expected_audiences[index])
    assert page.evaluate("document.documentElement.scrollWidth <= innerWidth")
    assert cards.first.bounding_box()["y"] < height * 0.65
    screenshots = Path(os.getenv("SHOWCASE_SCREENSHOTS", str(root / "screenshots")))
    screenshots.mkdir(exist_ok=True, parents=True)
    page.screenshot(path=str(screenshots / f"lecture-chips-{width}.png"))
    # Chips are labels, not unexplained controls; filtering matches any audience.
    assert page.locator(".card-chips button, .card-chips a").count() == 0
    page.locator("[data-filter-toggle]").click()
    page.locator('[data-showcase-filter="audience"]').select_option("students")
    expect(page.locator("[data-showcase-card]:visible")).to_have_count(2)
    page.locator('[data-showcase-filter="audience"]').select_option("adults")
    expect(page.locator("[data-showcase-card]:visible")).to_have_count(2)
    expect(cards.nth(0)).to_be_hidden()
    page.locator('[data-showcase-filter="audience"]').select_option("school")
    expect(page.locator("[data-showcase-card]:visible")).to_have_count(1)
    # Detailed page uses the same chip component and keeps audience qualifications.
    for index in range(4):
        page.goto(url)
        cards.nth(index).locator(".idea-card__link").click()
        expect(page.locator(".lecture-chips .badge")).to_have_text(expected_lecture[index])
        expect(page.locator(".audience-chips .badge")).to_have_text(expected_audiences[index])
        expect(page.get_by_role("heading", name="Для кого", exact=True)).to_be_visible()
        assert page.evaluate("document.documentElement.scrollWidth <= innerWidth")
        if index == 1:
            page.screenshot(path=str(screenshots / f"lecture-chips-detail-{width}.png"))
    assert not errors
    context.close()
