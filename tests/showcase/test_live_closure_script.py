import sys
from types import ModuleType

playwright = ModuleType("playwright")
playwright_async = ModuleType("playwright.async_api")
playwright_async.Page = object
playwright_async.async_playwright = object()
sys.modules.setdefault("playwright", playwright)
sys.modules.setdefault("playwright.async_api", playwright_async)

from scripts.showcase_live_closure import resolve_page_url, select_disposable_items  # noqa: E402


def test_disposable_selection_does_not_require_optional_taxonomy() -> None:
    items = [
        {"id": "first", "capability_type": None},
        {"id": "second"},
        {"id": "third", "capability_type": "technical"},
    ]

    selected = select_disposable_items(items, "acceptance-run")

    assert [item["id"] for item in selected] == [
        "acceptance-run-item-1",
        "acceptance-run-item-2",
    ]
    assert [item["capability_type"] for item in selected] == ["technical", "technical"]
    assert selected is not items
    assert [item["id"] for item in items] == ["first", "second", "third"]


def test_root_relative_detail_link_is_resolved_for_playwright() -> None:
    assert (
        resolve_page_url("https://ideas.example/v/secret/", "/v/secret/ideas/one/")
        == "https://ideas.example/v/secret/ideas/one/"
    )
