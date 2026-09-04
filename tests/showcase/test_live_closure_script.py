import sys
from types import ModuleType

playwright = ModuleType("playwright")
playwright_async = ModuleType("playwright.async_api")
playwright_async.Page = object
playwright_async.async_playwright = object()
sys.modules.setdefault("playwright", playwright)
sys.modules.setdefault("playwright.async_api", playwright_async)

from scripts.showcase_live_closure import select_disposable_items  # noqa: E402


def test_disposable_selection_does_not_require_optional_taxonomy() -> None:
    items = [
        {"id": "first", "capability_type": None},
        {"id": "second"},
        {"id": "third", "capability_type": "technical"},
    ]

    selected = select_disposable_items(items)

    assert [item["id"] for item in selected] == ["first", "second"]
    assert selected is not items
