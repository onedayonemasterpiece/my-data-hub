#!/usr/bin/env python3
"""Bounded live acceptance for IdeaHub Showcase.

The script never prints or persists complete secret Showcase URLs. It temporarily
changes one presentation string on the existing main surface, restores the exact
source bundle, and uses a disposable view for rotation/revocation checks.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
from copy import deepcopy
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

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
    return f"{type(exc).__name__}:{exc}"[:400]


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


async def exercise_filter(page: Page, key: str, selector: str, total: int) -> int:
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
            "nodes => [...new Set(nodes.filter(n => !n.hidden).map(n => Math.round(n.getBoundingClientRect().left)))]"
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

        share_controls = await page.locator("[data-share]").count()
        require(share_controls >= total + 1, "BROWSER_INDEX_SHARING_INCOMPLETE")
        await page.locator("[data-share-button]").first.click()
        await page.wait_for_timeout(100)
        payload = await page.evaluate("window.__showcaseSharePayload || null")
        require(
            isinstance(payload, dict)
            and bool(payload.get("title"))
            and bool(payload.get("text"))
            and bool(payload.get("url")),
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
        detail_response = await page.goto(detail_href, wait_until="networkidle")
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


async def run() -> int:
    artifact = Path(os.environ.get("SHOWCASE_LIVE_RECEIPT", "artifacts/showcase-live-closure.json"))
    artifact.parent.mkdir(parents=True, exist_ok=True)
    run_id = os.environ.get("SHOWCASE_LIVE_RUN_ID", "local")
    endpoint = os.environ.get("MY_DATA_HUB_MCP_CANARY_ENDPOINT", "").strip()
    credential_path = Path(os.environ.get("MY_DATA_HUB_MCP_OAUTH_CREDENTIAL_FILE", "").strip())
    private_link_path = Path(os.environ.get("SHOWCASE_MAIN_LINK_FILE", "").strip())
    receipt: dict[str, Any] = {
        "schema_version": 1,
        "status": "FAIL",
        "run_id": run_id,
        "endpoint": endpoint,
        "deployed_commit": os.environ.get("MY_DATA_HUB_DEPLOY_COMMIT", "unknown"),
        "control_image_id": os.environ.get("SHOWCASE_CONTROL_IMAGE_ID", "unknown"),
        "showcase_image_id": os.environ.get("SHOWCASE_RUNTIME_IMAGE_ID", "unknown"),
        "checks": {},
    }
    original_source: dict[str, Any] | None = None
    main_url: str | None = None
    disposable_view_id = f"acceptance-{run_id}".lower()
    disposable_active = False
    failure: str | None = None

    try:
        require(endpoint == "https://mcp-datahub.kenigevents.ru/mcp", "MCP_ENDPOINT_INVALID")
        require(credential_path.is_absolute() and credential_path.is_file(), "MCP_CREDENTIAL_FILE_MISSING")
        require(private_link_path.is_absolute(), "PRIVATE_LINK_PATH_INVALID")
        token = await RotatingOAuthBearerSource(credential_path).token("operator")
        async with (
            httpx2.AsyncClient(
                headers={"Authorization": f"Bearer {token}"},
                follow_redirects=False,
                timeout=httpx2.Timeout(45.0, connect=10.0),
            ) as client,
            streamable_http_client(endpoint, http_client=client) as streams,
        ):
            read_stream, write_stream = streams
            async with ClientSession(read_stream, write_stream, read_timeout_seconds=90) as session:
                await session.initialize()
                listed = await session.list_tools()
                names = sorted(tool.name for tool in listed.tools)
                require(EXPECTED_SHOWCASE_TOOLS.issubset(set(names)), "MCP_EIGHT_TOOLS_NOT_DISCOVERED")
                receipt["checks"]["tool_discovery"] = {
                    "status": "PASS",
                    "showcase_tools": sorted(EXPECTED_SHOWCASE_TOOLS),
                    "total_tools": len(names),
                }

                surfaces = await invoke(session, "showcase.list", {})
                original_source = await invoke(session, "showcase.get_source", {"view_id": "main"})
                link_before = await invoke(session, "showcase.get_link", {"view_id": "main"})
                main_url = link_before.get("url")
                require(isinstance(main_url, str) and main_url.startswith("https://"), "MAIN_LINK_MISSING")
                slug = slug_from_url(main_url)
                require(slug not in canonical_bytes(surfaces).decode("utf-8"), "LIST_LEAKS_FULL_SECRET_SLUG")
                require(isinstance(original_source.get("view"), dict), "MAIN_SOURCE_VIEW_MISSING")
                require(isinstance(original_source.get("items"), list), "MAIN_SOURCE_ITEMS_MISSING")
                original_hash = sha256_json(
                    {"view": original_source["view"], "items": original_source["items"]}
                )
                source_revision = original_source.get("source_revision")
                require(isinstance(source_revision, str) and len(source_revision) >= 8, "MAIN_SOURCE_REVISION_MISSING")

                marker = f"проверка-{run_id[-8:]}"
                changed_view = deepcopy(original_source["view"])
                base_access = str(changed_view.get("access_label", "Доступ по секретной ссылке"))
                changed_view["access_label"] = f"{base_access[:70]} · {marker}"
                dry_key = f"closure:{run_id}:main:dry"
                dry = await invoke(
                    session,
                    "showcase.apply",
                    {
                        "view_id": "main",
                        "expected_source_revision": source_revision,
                        "view": changed_view,
                        "items": [],
                        "dry_run": True,
                        "publish": False,
                        "idempotency_key": dry_key,
                    },
                )
                require(dry.get("status") == "dry_run", "MAIN_DRY_RUN_FAILED")
                after_dry = await invoke(session, "showcase.get_source", {"view_id": "main"})
                require(after_dry.get("source_revision") == source_revision, "MAIN_DRY_RUN_MUTATED_SOURCE")
                require(
                    sha256_json({"view": after_dry["view"], "items": after_dry["items"]}) == original_hash,
                    "MAIN_DRY_RUN_CHANGED_BUNDLE",
                )

                apply_key = f"closure:{run_id}:main:apply"
                applied = await invoke(
                    session,
                    "showcase.apply",
                    {
                        "view_id": "main",
                        "expected_source_revision": source_revision,
                        "view": changed_view,
                        "items": [],
                        "dry_run": False,
                        "publish": True,
                        "idempotency_key": apply_key,
                    },
                )
                require(applied.get("status") in {"published", "applied_not_published"}, "MAIN_APPLY_FAILED")
                if applied.get("status") == "applied_not_published":
                    await invoke(
                        session,
                        "showcase.rebuild",
                        {"view_id": "main", "idempotency_key": f"closure:{run_id}:main:rebuild"},
                    )
                changed_source = await invoke(session, "showcase.get_source", {"view_id": "main"})
                require(
                    changed_source["view"].get("access_label", "").endswith(marker),
                    "MAIN_SOURCE_CHANGE_NOT_VISIBLE",
                )
                link_changed = await invoke(session, "showcase.get_link", {"view_id": "main"})
                require(link_changed.get("url") == main_url, "MAIN_LINK_CHANGED_ON_UPDATE")
                status, headers, html = await public_response(main_url)
                require(status == 200 and marker in html, "MAIN_PUBLIC_CHANGE_NOT_VISIBLE")
                security = verify_security_headers(headers, html)

                rollback_key = f"closure:{run_id}:main:rollback"
                rolled_back = await invoke(
                    session,
                    "showcase.apply",
                    {
                        "view_id": "main",
                        "expected_source_revision": changed_source["source_revision"],
                        "view": original_source["view"],
                        "items": [],
                        "dry_run": False,
                        "publish": True,
                        "idempotency_key": rollback_key,
                    },
                )
                require(
                    rolled_back.get("status") in {"published", "applied_not_published"},
                    "MAIN_ROLLBACK_APPLY_FAILED",
                )
                if rolled_back.get("status") == "applied_not_published":
                    await invoke(
                        session,
                        "showcase.rebuild",
                        {"view_id": "main", "idempotency_key": f"closure:{run_id}:main:rollback-rebuild"},
                    )
                restored = await invoke(session, "showcase.get_source", {"view_id": "main"})
                restored_hash = sha256_json({"view": restored["view"], "items": restored["items"]})
                require(restored_hash == original_hash, "MAIN_ROLLBACK_BUNDLE_MISMATCH")
                link_restored = await invoke(session, "showcase.get_link", {"view_id": "main"})
                require(link_restored.get("url") == main_url, "MAIN_LINK_CHANGED_AFTER_ROLLBACK")
                restored_status, restored_headers, restored_html = await public_response(main_url)
                require(restored_status == 200 and marker not in restored_html, "MAIN_PUBLIC_ROLLBACK_FAILED")
                verify_security_headers(restored_headers, restored_html)
                receipt["checks"]["main_content_cycle"] = {
                    "status": "PASS",
                    "dry_run_no_mutation": True,
                    "source_change_and_readback": True,
                    "stable_url": True,
                    "rollback_exact_bundle": True,
                    "initial_bundle_sha256": original_hash,
                    "restored_bundle_sha256": restored_hash,
                    "restored_source_revision": restored["source_revision"],
                    "security": security,
                    "build": build_evidence(link_restored, main_url),
                }

                eligible_items = [
                    item
                    for item in original_source["items"]
                    if item.get("capability_type") in {"technical", "product", "business"}
                ]
                require(len(eligible_items) >= 2, "DISPOSABLE_VIEW_ITEMS_UNAVAILABLE")
                disposable_items = deepcopy(eligible_items[:2])
                disposable_view = {
                    "schema_version": 1,
                    "id": disposable_view_id,
                    "title": "Временная проверка IdeaHub Showcase",
                    "subtitle": "Одноразовая поверхность для проверки создания, ротации и отзыва ссылки.",
                    "access_label": "Временная секретная ссылка",
                    "visibility_ceiling": "partner",
                    "item_ids": [item["id"] for item in disposable_items],
                    "contact": deepcopy(original_source["view"]["contact"]),
                }
                disposable_dry = await invoke(
                    session,
                    "showcase.apply",
                    {
                        "view_id": disposable_view_id,
                        "expected_source_revision": "absent",
                        "view": disposable_view,
                        "items": disposable_items,
                        "dry_run": True,
                        "publish": False,
                        "idempotency_key": f"closure:{run_id}:disposable:dry",
                    },
                )
                require(disposable_dry.get("status") == "dry_run", "DISPOSABLE_DRY_RUN_FAILED")
                disposable_apply = await invoke(
                    session,
                    "showcase.apply",
                    {
                        "view_id": disposable_view_id,
                        "expected_source_revision": "absent",
                        "view": disposable_view,
                        "items": disposable_items,
                        "dry_run": False,
                        "publish": True,
                        "idempotency_key": f"closure:{run_id}:disposable:create",
                    },
                )
                require(
                    disposable_apply.get("status") in {"published", "applied_not_published"},
                    "DISPOSABLE_CREATE_FAILED",
                )
                if disposable_apply.get("status") == "applied_not_published":
                    await invoke(
                        session,
                        "showcase.rebuild",
                        {
                            "view_id": disposable_view_id,
                            "idempotency_key": f"closure:{run_id}:disposable:rebuild",
                        },
                    )
                disposable_active = True
                disposable_source = await invoke(
                    session,
                    "showcase.get_source",
                    {"view_id": disposable_view_id},
                )
                disposable_link = await invoke(
                    session,
                    "showcase.get_link",
                    {"view_id": disposable_view_id},
                )
                disposable_url = disposable_link.get("url")
                require(isinstance(disposable_url, str), "DISPOSABLE_LINK_MISSING")
                disposable_status, _, _ = await public_response(disposable_url)
                require(disposable_status == 200, "DISPOSABLE_PUBLIC_NOT_200")

                rotated = await invoke(
                    session,
                    "showcase.rotate_link",
                    {
                        "view_id": disposable_view_id,
                        "idempotency_key": f"closure:{run_id}:disposable:rotate",
                    },
                )
                rotated_url = rotated.get("url")
                require(isinstance(rotated_url, str) and rotated_url != disposable_url, "DISPOSABLE_ROTATION_FAILED")
                rotated_status, _, _ = await public_response(rotated_url)
                old_status, _, _ = await public_response(disposable_url)
                require(rotated_status == 200, "DISPOSABLE_ROTATED_URL_NOT_200")
                require(old_status >= 400, "DISPOSABLE_OLD_URL_STILL_AVAILABLE")
                await invoke(
                    session,
                    "showcase.rotate_link",
                    {
                        "view_id": disposable_view_id,
                        "idempotency_key": f"closure:{run_id}:disposable:rotate",
                    },
                )
                duplicate_link = await invoke(
                    session,
                    "showcase.get_link",
                    {"view_id": disposable_view_id},
                )
                require(duplicate_link.get("url") == rotated_url, "DISPOSABLE_ROTATION_NOT_IDEMPOTENT")
                await invoke(
                    session,
                    "showcase.revoke_link",
                    {
                        "view_id": disposable_view_id,
                        "idempotency_key": f"closure:{run_id}:disposable:revoke",
                    },
                )
                disposable_active = False
                revoked_status, _, _ = await public_response(rotated_url)
                require(revoked_status >= 400, "DISPOSABLE_REVOKED_URL_STILL_AVAILABLE")
                receipt["checks"]["disposable_lifecycle"] = {
                    "status": "PASS",
                    "view_id": disposable_view_id,
                    "source_revision": disposable_source["source_revision"],
                    "created_url_masked": mask_url(disposable_url),
                    "created_slug_sha256": hashlib.sha256(slug_from_url(disposable_url).encode()).hexdigest(),
                    "rotated_url_masked": mask_url(rotated_url),
                    "rotated_slug_sha256": hashlib.sha256(slug_from_url(rotated_url).encode()).hexdigest(),
                    "old_status_after_rotation": old_status,
                    "revoked_status": revoked_status,
                    "duplicate_rotation_created_no_third_url": True,
                    "source_cleanup_path": f"showcase/views/{disposable_view_id}.yaml",
                }

        require(main_url is not None, "MAIN_URL_UNAVAILABLE_FOR_BROWSER")
        receipt["checks"]["mobile_browser"] = {
            "status": "PASS",
            **await browser_acceptance(main_url),
        }
        descriptor = os.open(private_link_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(main_url + "\n")
        os.chmod(private_link_path, 0o600)
        receipt["status"] = "PASS"
    except Exception as exc:  # fail closed; full URLs and raw tool payloads stay out of the receipt
        failure = safe_failure(exc)
        receipt["failure"] = failure
    finally:
        # The normal path performs exact rollback and revocation inside the live session.
        # If transport failed mid-operation, record that manual recovery is required rather
        # than emitting secret state or pretending closure succeeded.
        receipt["cleanup"] = {
            "main_rollback_completed": receipt.get("checks", {})
            .get("main_content_cycle", {})
            .get("rollback_exact_bundle")
            is True,
            "disposable_revoked": not disposable_active,
            "source_cleanup_required": f"showcase/views/{disposable_view_id}.yaml"
            if "disposable_lifecycle" in receipt.get("checks", {})
            else None,
        }
        artifact.write_text(
            json.dumps(receipt, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    return 0 if receipt["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(run()))
