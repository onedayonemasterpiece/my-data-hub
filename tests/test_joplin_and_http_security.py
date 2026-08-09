from __future__ import annotations

import httpx
import pytest

from my_data_hub.joplin.bridge import JoplinNoteSnapshot, decide_sync
from my_data_hub.joplin.provider import HttpJoplinDataApi, JoplinProviderError
from my_data_hub.mcp.http_security import DevelopmentBearerSecurity
from my_data_hub.mcp.policy import MCPAuthorizationError, ScopeAuthorizer


def test_joplin_sync_decisions() -> None:
    note = JoplinNoteSnapshot("n1", "Title", "Body", 1)
    assert (
        decide_sync(
            note,
            last_joplin_hash=None,
            last_hub_revision=None,
            hub_changed_since_revision=False,
        )
        == "import"
    )
    assert (
        decide_sync(
            note,
            last_joplin_hash=note.content_hash,
            last_hub_revision=1,
            hub_changed_since_revision=True,
        )
        == "push"
    )
    assert (
        decide_sync(
            note,
            last_joplin_hash="0" * 64,
            last_hub_revision=1,
            hub_changed_since_revision=True,
        )
        == "conflict"
    )
    deleted = JoplinNoteSnapshot("n1", "Title", "Body", 2, deleted_time=2)
    assert (
        decide_sync(
            deleted,
            last_joplin_hash=note.content_hash,
            last_hub_revision=1,
            hub_changed_since_revision=False,
        )
        == "tombstone"
    )


def test_joplin_provider_is_loopback_only_by_default() -> None:
    HttpJoplinDataApi("http://127.0.0.1:41184", "token")
    HttpJoplinDataApi("http://localhost:41184", "token")
    with pytest.raises(JoplinProviderError, match="non-loopback"):
        HttpJoplinDataApi("http://192.168.1.3:41184", "token")
    with pytest.raises(JoplinProviderError, match="local HTTP"):
        HttpJoplinDataApi("https://localhost:41184", "token")


def test_scope_authorizer_is_fail_closed() -> None:
    authorizer = ScopeAuthorizer(frozenset({"hub:read"}))
    authorizer.require("hub:read")
    with pytest.raises(MCPAuthorizationError, match="region-talk:write"):
        authorizer.require("region-talk:write")


async def _echo_app(scope, receive, send) -> None:  # type: ignore[no-untyped-def]
    while True:
        message = await receive()
        if message["type"] != "http.request" or not message.get("more_body", False):
            break
    await send({"type": "http.response.start", "status": 200, "headers": []})
    await send({"type": "http.response.body", "body": b"ok"})


@pytest.mark.asyncio
async def test_http_security_requires_host_origin_and_bearer() -> None:
    app = DevelopmentBearerSecurity(
        _echo_app,
        token="secret",
        allowed_origins=("http://localhost",),
        allowed_hosts=("localhost",),
        max_request_bytes=10,
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://localhost") as client:
        denied = await client.post("/mcp", content=b"x")
        assert denied.status_code == 401
        wrong_origin = await client.post(
            "/mcp",
            content=b"x",
            headers={"Authorization": "Bearer secret", "Origin": "https://evil.invalid"},
        )
        assert wrong_origin.status_code == 403
        accepted = await client.post(
            "/mcp",
            content=b"x",
            headers={"Authorization": "Bearer secret", "Origin": "http://localhost"},
        )
        assert accepted.status_code == 200
        assert accepted.headers["cache-control"] == "no-store"


@pytest.mark.asyncio
async def test_http_security_rejects_large_request() -> None:
    app = DevelopmentBearerSecurity(
        _echo_app,
        token="secret",
        allowed_origins=(),
        allowed_hosts=("localhost",),
        max_request_bytes=3,
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://localhost") as client:
        response = await client.post(
            "/mcp", content=b"four", headers={"Authorization": "Bearer secret"}
        )
    assert response.status_code == 413
