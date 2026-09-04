import importlib.util
from types import ModuleType
from unittest.mock import patch

if importlib.util.find_spec("playwright") is None:
    playwright = ModuleType("playwright")
    playwright_async = ModuleType("playwright.async_api")
    playwright_async.Page = object
    playwright_async.async_playwright = object()
    with patch.dict("sys.modules", {"playwright": playwright, "playwright.async_api": playwright_async}):
        from scripts import showcase_live_closure as live
else:
    from scripts import showcase_live_closure as live


def test_disposable_selection_does_not_require_optional_taxonomy() -> None:
    items = [
        {"id": "first", "capability_type": None},
        {"id": "second"},
        {"id": "third", "capability_type": "technical"},
    ]

    selected = live.select_disposable_items(items, "acceptance-run")

    assert [item["id"] for item in selected] == [
        "acceptance-run-item-1",
        "acceptance-run-item-2",
    ]
    assert [item["capability_type"] for item in selected] == ["technical", "technical"]
    assert selected is not items
    assert [item["id"] for item in items] == ["first", "second", "third"]


def test_root_relative_detail_link_is_resolved_for_playwright() -> None:
    assert (
        live.resolve_page_url("https://ideas.example/v/secret/", "/v/secret/ideas/one/")
        == "https://ideas.example/v/secret/ideas/one/"
    )


def test_live_failure_never_serializes_secret_urls_or_tokens() -> None:
    secret = "https://ideas.example/v/secret-link/ token=private"
    assert live.safe_failure(RuntimeError(secret)) == "RuntimeError"
    assert live.safe_failure(live.LiveClosureError(secret)) == "LiveClosureError"
    assert live.safe_failure(live.LiveClosureError("MAIN_NOT_200")) == "LiveClosureError:MAIN_NOT_200"
    assert "secret-link" not in live.safe_failure(ExceptionGroup("private", [RuntimeError(secret)]))


def test_absent_irrelevant_filter_is_not_a_live_failure() -> None:
    import asyncio

    class Locator:
        async def count(self):
            return 0

    class Page:
        def locator(self, _selector):
            return Locator()

    assert asyncio.run(live.exercise_filter(Page(), "capabilityType", "select", 5)) is None


def test_live_writes_only_disposable_view_and_preserves_shared_cards(tmp_path, monkeypatch) -> None:
    import asyncio

    from tests.showcase.test_product_constructor import invoke, setup

    manager, _writer, controller, _journal, _view = setup.__wrapped__(tmp_path)
    original = manager.get_source("main")
    expected_hash = live.sha256_json({"view": original["view"], "items": original["items"]})
    writes = []

    async def local_invoke(_session, tool, args):
        if tool not in {"showcase.get_link", "showcase.get_source", "showcase.list"}:
            writes.append((tool, args))
        return invoke(controller, tool.split(".")[1], **args)

    async def local_response(url):
        slug = live.slug_from_url(url)
        headers = {
            "x-robots-tag": ",".join(live.REQUIRED_ROBOTS),
            "referrer-policy": "no-referrer",
            "content-security-policy": "script-src 'self'",
        }
        return (
            (404 if slug in manager.publisher.revoked else 200),
            headers,
            '<meta name="robots" content="noindex">test-run',
        )

    monkeypatch.setattr(live, "invoke", local_invoke)
    monkeypatch.setattr(live, "public_response", local_response)
    checks = {}
    asyncio.run(live.exercise_disposable(None, original, "test-run", checks))
    assert checks["disposable_lifecycle"]["status"] == "PASS"
    assert checks["disposable_lifecycle"]["revoked"] is True
    assert all(args["view_id"] == "acceptance-test-run" for _, args in writes)
    create_calls = [args for tool, args in writes if tool == "showcase.create_view"]
    assert len(create_calls[0]["items"]) == 1
    assert create_calls[0]["view"]["item_ids"][0] == original["items"][0]["id"]
    assert "expected_source_revision" not in create_calls[0]
    after = manager.get_source("main")
    assert live.sha256_json({"view": after["view"], "items": after["items"]}) == expected_hash
