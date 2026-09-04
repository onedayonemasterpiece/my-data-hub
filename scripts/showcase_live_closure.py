#!/usr/bin/env python3
"""Bounded live acceptance for IdeaHub Showcase.

Only disposable source is changed. Main is read/previewed, never rewritten or
rotated. Full secret URLs stay out of receipts and console output; the existing
main link is written only to the explicitly configured private 0600 file.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlsplit, urlunsplit

import httpx2
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client
from playwright.async_api import Page, async_playwright

from my_data_hub.auth.oauth_credentials import RotatingOAuthBearerSource

EXPECTED_SHOWCASE_TOOLS = {
    "showcase.list",
    "showcase.get_source",
    "showcase.apply",
    "showcase.rebuild",
    "showcase.create_view",
    "showcase.get_link",
    "showcase.rotate_link",
    "showcase.revoke_link",
}
REQUIRED_ROBOTS = {
    "noindex",
    "nofollow",
    "noarchive",
    "nosnippet",
    "noimageindex",
}


class LiveClosureError(RuntimeError):
    """Fail-closed acceptance error without embedding secret values."""


def safe_failure(exc: BaseException) -> str:
    """Flatten task-group failures without serializing requests or bearer headers."""

    if isinstance(exc, BaseExceptionGroup):
        leaves = [safe_failure(child) for child in exc.exceptions]
        return f"{type(exc).__name__}[{';'.join(leaves)}]"[:400]
    if isinstance(exc, LiveClosureError) and re.fullmatch(r"[A-Za-z0-9_.:-]{1,160}", str(exc)):
        return f"LiveClosureError:{exc}"
    return type(exc).__name__


def require(condition: bool, code: str) -> None:
    if not condition:
        raise LiveClosureError(code)


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def sha256_json(value: Any) -> str:
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def select_disposable_items(items: list[dict[str, Any]], view_id: str) -> list[dict[str, Any]]:
    """Create two isolated test items without changing shared source item files."""

    eligible = [item for item in items if isinstance(item.get("id"), str) and item["id"]]
    require(len(eligible) >= 2, "DISPOSABLE_VIEW_ITEMS_UNAVAILABLE")
    selected = deepcopy(eligible[:2])
    for position, item in enumerate(selected, start=1):
        item["id"] = f"{view_id}-item-{position}"
        item["capability_type"] = item.get("capability_type") or "technical"
    return selected


def mask_url(url: str) -> str:
    parts = urlsplit(url)
    segments = [segment for segment in parts.path.split("/") if segment]
    if not segments:
        return urlunsplit((parts.scheme, parts.netloc, "/", "", ""))
    slug = segments[-1]
    segments[-1] = f"{slug[:4]}…{slug[-4:]}" if len(slug) > 10 else "…"
    return urlunsplit((parts.scheme, parts.netloc, "/" + "/".join(segments) + "/", "", ""))


def slug_from_url(url: str) -> str:
    segments = [segment for segment in urlsplit(url).path.split("/") if segment]
    require(bool(segments), "URL_WITHOUT_SLUG")
    return segments[-1]


def resolve_page_url(base_url: str, href: str) -> str:
    """Resolve same-origin root-relative links for Playwright navigation."""

    return urljoin(base_url, href)


def result_document(result: Any, tool: str) -> dict[str, Any]:
    if bool(getattr(result, "is_error", getattr(result, "isError", False))):
        raise LiveClosureError(f"{tool}:MCP_ERROR")
    structured = getattr(
        result,
        "structured_content",
        getattr(result, "structuredContent", None),
    )
    if isinstance(structured, dict):
        if set(structured) == {"result"} and isinstance(structured["result"], dict):
            return structured["result"]
        return structured
    for content in getattr(result, "content", ()):
        text = getattr(content, "text", None)
        if not isinstance(text, str):
            continue
        try:
            value = json.loads(text)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            if set(value) == {"result"} and isinstance(value["result"], dict):
                return value["result"]
            return value
    raise LiveClosureError(f"{tool}:NO_STRUCTURED_RESULT")


async def invoke(
    session: ClientSession,
    tool: str,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    return result_document(await session.call_tool(tool, arguments), tool)


async def public_response(url: str) -> tuple[int, dict[str, str], str]:
    async with httpx2.AsyncClient(
        follow_redirects=False,
        timeout=httpx2.Timeout(30.0, connect=10.0),
        headers={"User-Agent": "IdeaHub-Showcase-live-closure/1"},
    ) as client:
        response = await client.get(url)
    return response.status_code, dict(response.headers), response.text


def verify_security_headers(headers: dict[str, str], html: str) -> dict[str, Any]:
    robots = {token.strip().lower() for token in headers.get("x-robots-tag", "").split(",") if token.strip()}
    require(REQUIRED_ROBOTS.issubset(robots), "PUBLIC_X_ROBOTS_TAG_INCOMPLETE")
    require(headers.get("referrer-policy", "").lower() == "no-referrer", "PUBLIC_REFERRER_POLICY_INVALID")
    csp = headers.get("content-security-policy", "")
    require(bool(csp), "PUBLIC_CSP_MISSING")
    script_src = next(
        (directive for directive in csp.split(";") if directive.strip().lower().startswith("script-src")),
        "",
    )
    require(bool(script_src), "PUBLIC_CSP_SCRIPT_SRC_MISSING")
    require("'unsafe-inline'" not in script_src.lower(), "PUBLIC_CSP_SCRIPT_UNSAFE_INLINE")
    lowered = html.lower()
    require('name="robots"' in lowered and "noindex" in lowered, "PUBLIC_META_ROBOTS_MISSING")
    return {
        "x_robots_tag": sorted(robots),
        "referrer_policy": headers.get("referrer-policy"),
        "csp_sha256": hashlib.sha256(csp.encode("utf-8")).hexdigest(),
    }


async def visible_card_count(page: Page) -> int:
    return await page.locator("[data-showcase-card]").evaluate_all(
        "nodes => nodes.filter(node => !node.hidden).length"
    )


async def exercise_filter(page: Page, key: str, selector: str, total: int) -> int | None:
    # The renderer omits irrelevant/internal filters; absence is not a defect.
    if await page.locator(selector).count() == 0:
        return None
    await page.locator("#showcase-search").focus()
    values = await page.locator("[data-showcase-card]").evaluate_all(
        "(nodes, key) => nodes.map(node => node.dataset[key] || '')",
        key,
    )
    frequencies: dict[str, int] = {}
    for value in values:
        if value:
            frequencies[value] = frequencies.get(value, 0) + 1
    candidates = [(count, value) for value, count in frequencies.items() if count < total]
    require(bool(candidates), f"FILTER_{key.upper()}_HAS_NO_REDUCING_OPTION")
    expected, value = min(candidates)
    await page.locator(selector).select_option(value)
    await page.wait_for_timeout(150)
    observed = await visible_card_count(page)
    require(observed == expected, f"FILTER_{key.upper()}_COUNT_MISMATCH")
    await page.locator(selector).select_option("")
    await page.wait_for_timeout(100)
    require(await visible_card_count(page) == total, f"FILTER_{key.upper()}_RESET_FAILED")
    return observed


async def browser_acceptance(url: str) -> dict[str, Any]:
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True)
        context = await browser.new_context(viewport={"width": 390, "height": 844})
        page = await context.new_page()
        console_errors: list[str] = []
        failed_requests: list[str] = []
        bad_responses: list[int] = []
        page.on(
            "console",
            lambda message: console_errors.append(message.type)
            if message.type == "error"
            else None,
        )
        page.on(
            "requestfailed",
            lambda request: failed_requests.append(request.failure or "request-failed")
            if "ERR_ABORTED" not in str(request.failure)
            else None,
        )
        page.on(
            "response",
            lambda response: bad_responses.append(response.status)
            if response.status >= 400 and urlsplit(response.url).hostname == urlsplit(url).hostname
            else None,
        )
        await page.add_init_script(
            """
            Object.defineProperty(navigator, 'share', {
              configurable: true,
              value: async payload => { window.__showcaseSharePayload = payload; }
            });
            Object.defineProperty(navigator, 'canShare', {
              configurable: true,
              value: () => true
            });
            """
        )
        response = await page.goto(url, wait_until="networkidle")
        require(response is not None and response.status == 200, "BROWSER_INDEX_NOT_200")
        total = await page.locator("[data-showcase-card]").count()
        require(total >= 2, "BROWSER_INDEX_TOO_FEW_CARDS")
        overflow = await page.evaluate("document.documentElement.scrollWidth > window.innerWidth")
        require(not overflow, "BROWSER_HORIZONTAL_OVERFLOW")
        lefts = await page.locator("[data-showcase-card]").evaluate_all(
            "nodes => [...new Set(nodes.filter(n => !n.hidden).map(n => Math.round(n.getBoundingClientRect().left))) ]"
        )
        require(len(lefts) == 1, "BROWSER_NOT_ONE_COLUMN")

        category_count = await exercise_filter(
            page,
            "category",
            'select[data-showcase-filter="category"]',
            total,
        )
        capability_count = await exercise_filter(
            page,
            "capabilityType",
            'select[data-showcase-filter="capabilityType"]',
            total,
        )
        maturity_count = await exercise_filter(
            page,
            "maturity",
            'select[data-showcase-filter="maturity"]',
            total,
        )
        title = (await page.locator("[data-showcase-card] h2").first.text_content() or "").strip()
        require(bool(title), "BROWSER_FIRST_TITLE_MISSING")
        await page.locator("#showcase-search").fill(title)
        await page.wait_for_timeout(150)
        search_count = await visible_card_count(page)
        require(0 < search_count < total, "BROWSER_SEARCH_DID_NOT_CHANGE_COUNT")
        await page.locator("#showcase-search").fill("")
        await page.wait_for_timeout(100)

        detail_href = await page.locator(".idea-card__link").first.get_attribute("href")
        require(bool(detail_href), "BROWSER_DETAIL_LINK_MISSING")
        share_controls = await page.locator("[data-share]").count()
        require(share_controls >= total + 1, "BROWSER_INDEX_SHARING_INCOMPLETE")
        await page.locator("[data-share-button]").first.click()
        await page.wait_for_timeout(100)
        payload = await page.evaluate("window.__showcaseSharePayload || null")
        require(
            isinstance(payload, dict)
            and bool(payload.get("title"))
            and bool(payload.get("text"))
            and payload.get("url") == resolve_page_url(url, detail_href),
            "BROWSER_WEB_SHARE_PAYLOAD_INVALID",
        )
        require(
            await page.locator('a[href="https://t.me/confidentmax"]').count() >= 1,
            "BROWSER_OWNER_CTA_MISSING",
        )
        inline_executable = await page.locator("script:not([src])").evaluate_all(
            """nodes => nodes.filter(node => {
              const type = (node.type || '').toLowerCase();
              return !['application/json', 'application/ld+json'].includes(type)
                && (node.textContent || '').trim().length > 0;
            }).length"""
        )
        require(inline_executable == 0, "BROWSER_EXECUTABLE_INLINE_SCRIPT_FOUND")

        detail_href = await page.locator(".idea-card__link").first.get_attribute("href")
        require(bool(detail_href), "BROWSER_DETAIL_LINK_MISSING")
        detail_response = await page.goto(resolve_page_url(url, detail_href), wait_until="networkidle")
        require(detail_response is not None and detail_response.status == 200, "BROWSER_DETAIL_NOT_200")
        require(await page.locator("[data-share]").count() >= 1, "BROWSER_DETAIL_SHARING_MISSING")
        require(
            await page.locator('a[href="https://t.me/confidentmax"]').count() >= 1,
            "BROWSER_DETAIL_CTA_MISSING",
        )
        detail_overflow = await page.evaluate("document.documentElement.scrollWidth > window.innerWidth")
        require(not detail_overflow, "BROWSER_DETAIL_HORIZONTAL_OVERFLOW")
        require(not console_errors, "BROWSER_CONSOLE_ERRORS")
        require(not failed_requests, "BROWSER_NETWORK_FAILURES")
        require(not bad_responses, "BROWSER_BAD_RESPONSES")
        await browser.close()
        return {
            "viewport": "390x844",
            "card_count": total,
            "one_column": True,
            "horizontal_overflow": False,
            "filter_counts": {
                "category": category_count,
                "capability_type": capability_count,
                "maturity": maturity_count,
                "search": search_count,
            },
            "index_share_controls": share_controls,
            "detail_page": "PASS",
            "web_share_payload": "PASS",
            "cta": "@confidentmax",
            "inline_executable_scripts": 0,
            "console_errors": 0,
            "network_errors": 0,
        }


def build_evidence(link: dict[str, Any], url: str) -> dict[str, Any]:
    last_build = link.get("last_build")
    require(isinstance(last_build, dict), "LINK_BUILD_RECEIPT_MISSING")
    for key in ("source_revision", "tree_sha256", "file_count", "html_count", "slug"):
        require(last_build.get(key) not in (None, ""), f"LINK_BUILD_RECEIPT_{key.upper()}_MISSING")
    return {
        "url_masked": mask_url(url),
        "slug_sha256": hashlib.sha256(slug_from_url(url).encode("utf-8")).hexdigest(),
        "source_revision": last_build["source_revision"],
        "tree_sha256": last_build["tree_sha256"],
        "file_count": last_build["file_count"],
        "html_count": last_build["html_count"],
    }


async def exercise_disposable(
    session: ClientSession, source: dict[str, Any], run_id: str, checks: dict[str, Any]
) -> None:
    """Live writes are restricted to this disposable view; existing cards are reused read-only."""
    view_id = f"acceptance-{run_id}".lower()
    definitions = select_disposable_items(source["items"], view_id)[1:]
    view = {
        "title": "Временная проверка IdeaHub Showcase",
        "subtitle": "Проверка создания по ссылкам на карточки, правок и жизненного цикла ссылки.",
        "item_ids": [source["items"][0]["id"], definitions[0]["id"]],
        "contact": deepcopy(source["view"]["contact"]),
    }
    evidence: dict[str, Any] = {
        "status": "FAIL",
        "view_id": view_id,
        "revoked": False,
        "write_attempted": False,
        "source_cleanup_paths": [
            f"showcase/views/{view_id}.yaml",
            *[f"showcase/items/{item['id']}.yaml" for item in definitions],
        ],
    }
    checks["disposable_lifecycle"] = evidence
    arguments = {"view_id": view_id, "view": view, "items": definitions}
    preview = await invoke(session, "showcase.create_view", {**arguments, "mode": "preview"})
    require(preview.get("status") == "dry_run", "DISPOSABLE_PREVIEW_FAILED")
    require(preview.get("validation", {}).get("build_checked") is False, "PREVIEW_BUILD_CLAIM_INVALID")
    require(preview.get("validation", {}).get("buildable") is None, "PREVIEW_BUILDABILITY_CLAIM_INVALID")
    write = {**arguments, "mode": "publish", "idempotency_key": f"closure:{run_id}:create"}
    # A lost response may still have applied a write. Do not report clean before checking.
    evidence["write_attempted"] = True
    created = await invoke(session, "showcase.create_view", write)
    require(created.get("status") in {"published", "applied_not_published"}, "DISPOSABLE_CREATE_FAILED")
    if created.get("status") == "applied_not_published":
        await invoke(session, "showcase.rebuild", {"view_id": view_id, "idempotency_key": f"closure:{run_id}:rebuild"})
    link = await invoke(session, "showcase.get_link", {"view_id": view_id})
    url = link.get("url")
    require(isinstance(url, str), "DISPOSABLE_LINK_MISSING")
    duplicate = await invoke(session, "showcase.create_view", write)
    require(duplicate.get("duplicate") is True, "CREATE_RETRY_NOT_IDEMPOTENT")
    same_link = await invoke(session, "showcase.get_link", {"view_id": view_id})
    require(same_link.get("url") == url, "CREATE_RETRY_CHANGED_URL")
    current = await invoke(session, "showcase.get_source", {"view_id": view_id})
    updated_view = {**current["view"], "subtitle": f"Проверенная правка · {run_id}"}
    changed = await invoke(
        session,
        "showcase.apply",
        {
            "view_id": view_id,
            "expected_source_revision": current["source_revision"],
            "view": updated_view,
            "mode": "publish",
            "idempotency_key": f"closure:{run_id}:update",
        },
    )
    require(changed.get("status") == "published", "DISPOSABLE_UPDATE_NOT_PUBLISHED")
    updated_link = await invoke(session, "showcase.get_link", {"view_id": view_id})
    require(updated_link.get("url") == url, "UPDATE_CHANGED_URL")
    readback = await invoke(session, "showcase.get_source", {"view_id": view_id})
    require(readback["view"]["subtitle"] == updated_view["subtitle"], "DISPOSABLE_UPDATE_READBACK_FAILED")
    status, headers, html = await public_response(url)
    require(status == 200 and run_id in html, "DISPOSABLE_UPDATE_NOT_VISIBLE")
    evidence["security"] = verify_security_headers(headers, html)
    evidence.update(
        stable_update_url=True,
        duplicate_create=True,
        reuse_and_new_card=True,
        source_revision=readback["source_revision"],
        build=build_evidence(updated_link, url),
    )
    rotation = {"view_id": view_id, "idempotency_key": f"closure:{run_id}:rotate"}
    rotated = await invoke(session, "showcase.rotate_link", rotation)
    rotated_url = rotated.get("url")
    require(isinstance(rotated_url, str) and rotated_url != url, "DISPOSABLE_ROTATION_FAILED")
    new_status, _, _ = await public_response(rotated_url)
    old_status, _, _ = await public_response(url)
    require(new_status == 200 and old_status >= 400, "ROTATED_LINK_STATE_INVALID")
    await invoke(session, "showcase.rotate_link", rotation)
    repeated = await invoke(session, "showcase.get_link", {"view_id": view_id})
    require(repeated.get("url") == rotated_url, "ROTATION_RETRY_CHANGED_URL")
    await invoke(session, "showcase.revoke_link", {"view_id": view_id, "idempotency_key": f"closure:{run_id}:revoke"})
    revoked_status, _, _ = await public_response(rotated_url)
    require(revoked_status >= 400, "REVOKED_URL_STILL_AVAILABLE")
    evidence.update(
        status="PASS",
        revoked=True,
        duplicate_rotation=True,
        old_status_after_rotation=old_status,
        revoked_status=revoked_status,
    )


async def run() -> int:
    artifact = Path(os.environ.get("SHOWCASE_LIVE_RECEIPT", "artifacts/showcase-live-closure.json"))
    artifact.parent.mkdir(parents=True, exist_ok=True)
    run_id = os.environ.get("SHOWCASE_LIVE_RUN_ID", datetime.now(UTC).strftime("%Y%m%d%H%M%S")).lower()
    endpoint = os.environ.get("MY_DATA_HUB_MCP_CANARY_ENDPOINT", "").strip()
    credential_path = Path(os.environ.get("MY_DATA_HUB_MCP_OAUTH_CREDENTIAL_FILE", "").strip())
    private_link_path = Path(os.environ.get("SHOWCASE_MAIN_LINK_FILE", "").strip())
    receipt: dict[str, Any] = {
        "schema_version": 1,
        "status": "FAIL",
        "run_id": run_id,
        "deployed_commit": os.environ.get("MY_DATA_HUB_DEPLOY_COMMIT", "unknown"),
        "control_image_id": os.environ.get("SHOWCASE_CONTROL_IMAGE_ID", "unknown"),
        "showcase_image_id": os.environ.get("SHOWCASE_RUNTIME_IMAGE_ID", "unknown"),
        "checks": {},
    }
    main_url: str | None = None
    try:
        require(bool(re.fullmatch(r"[a-z0-9][a-z0-9-]{0,34}", run_id)), "RUN_ID_INVALID")
        require(endpoint == "https://mcp-datahub.kenigevents.ru/mcp", "MCP_ENDPOINT_INVALID")
        require(credential_path.is_absolute() and credential_path.is_file(), "MCP_CREDENTIAL_FILE_MISSING")
        require(private_link_path.is_absolute() and not private_link_path.is_symlink(), "PRIVATE_LINK_PATH_INVALID")
        token = await RotatingOAuthBearerSource(credential_path).token("operator")
        async with (
            httpx2.AsyncClient(
                headers={"Authorization": f"Bearer {token}"},
                follow_redirects=False,
                timeout=httpx2.Timeout(300.0, connect=10.0),
            ) as client,
            streamable_http_client(endpoint, http_client=client) as streams,
        ):
            read_stream, write_stream = streams
            async with ClientSession(read_stream, write_stream, read_timeout_seconds=300) as session:
                await session.initialize()
                listed = await session.list_tools()
                tools = {tool.name: tool for tool in listed.tools if tool.name.startswith("showcase.")}
                require(set(tools) == EXPECTED_SHOWCASE_TOOLS, "MCP_EIGHT_TOOLS_NOT_DISCOVERED")
                schema = tools["showcase.create_view"].input_schema
                require(
                    "mode" in schema.get("properties", {}) and "view" in schema.get("properties", {}),
                    "MCP_CREATE_SCHEMA_STALE",
                )
                receipt["checks"]["tool_discovery"] = {"status": "PASS", "showcase_tools": sorted(tools)}
                surfaces = await invoke(session, "showcase.list", {})
                source = await invoke(session, "showcase.get_source", {"view_id": "main"})
                link = await invoke(session, "showcase.get_link", {"view_id": "main"})
                main_url = link.get("url")
                require(isinstance(main_url, str) and main_url.startswith("https://"), "MAIN_LINK_MISSING")
                require(
                    slug_from_url(main_url) not in canonical_bytes(surfaces).decode(), "LIST_LEAKS_FULL_SECRET_SLUG"
                )
                original_hash = sha256_json({"view": source["view"], "items": source["items"]})
                preview = await invoke(
                    session,
                    "showcase.apply",
                    {
                        "view_id": "main",
                        "expected_source_revision": source["source_revision"],
                        "view": source["view"],
                        "mode": "preview",
                    },
                )
                require(preview.get("status") == "dry_run", "MAIN_PREVIEW_FAILED")
                after_preview = await invoke(session, "showcase.get_source", {"view_id": "main"})
                require(after_preview["source_revision"] == source["source_revision"], "MAIN_PREVIEW_CHANGED_REVISION")
                await exercise_disposable(session, source, run_id, receipt["checks"])
                after = await invoke(session, "showcase.get_source", {"view_id": "main"})
                final_link = await invoke(session, "showcase.get_link", {"view_id": "main"})
                require(
                    sha256_json({"view": after["view"], "items": after["items"]}) == original_hash,
                    "MAIN_CONTENT_CHANGED",
                )
                require(final_link.get("url") == main_url, "MAIN_LINK_CHANGED")
                status, headers, html = await public_response(main_url)
                require(status == 200, "MAIN_NOT_200")
                receipt["checks"]["main_read_only"] = {
                    "status": "PASS",
                    "bundle_sha256": original_hash,
                    "stable_url": True,
                    "preview_no_mutation": True,
                    "security": verify_security_headers(headers, html),
                    "build": build_evidence(final_link, main_url),
                }
        require(main_url is not None, "MAIN_URL_UNAVAILABLE_FOR_BROWSER")
        receipt["checks"]["mobile_browser"] = {"status": "PASS", **await browser_acceptance(main_url)}
        descriptor = os.open(private_link_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            os.fchmod(handle.fileno(), 0o600)
            handle.write(main_url + "\n")
        receipt["status"] = "PASS"
    except Exception as exc:
        receipt["failure"] = safe_failure(exc)
    finally:
        lifecycle = receipt["checks"].get("disposable_lifecycle", {})
        receipt["cleanup"] = {
            "main_source_writes": False,
            "disposable_revoked": lifecycle.get("revoked") is True,
            "manual_recovery_required": lifecycle.get("write_attempted") is True
            and lifecycle.get("revoked") is not True,
            "source_cleanup_required": lifecycle.get("source_cleanup_paths")
            if lifecycle.get("write_attempted")
            else [],
        }
        artifact.write_text(json.dumps(receipt, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return 0 if receipt["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(run()))
