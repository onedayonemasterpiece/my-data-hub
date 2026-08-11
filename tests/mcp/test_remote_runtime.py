from __future__ import annotations

import asyncio
import os
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import httpx
import httpx2
import pytest
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client

from my_data_hub.control_plane.app import DATABASE_ENVIRONMENT_NAMES
from my_data_hub.mcp.oauth import TokenValidationError
from my_data_hub.mcp.runtime import build_remote_runtime
from my_data_hub.mcp.sql_policy import BoundedSQLPolicy

RESOURCE = "https://mcp-datahub.kenigevents.ru/mcp"
ISSUER = "https://identity.kenigevents.ru"
READER_SCOPES = frozenset(
    {
        "platform:read",
        "master:read",
        "operation:read",
        "checkpoint:read",
        "embedding:read",
        "provider:read",
        "bloggers:read",
        "data:read",
    }
)


def _configure(monkeypatch: pytest.MonkeyPatch, root: Path) -> None:
    for name in DATABASE_ENVIRONMENT_NAMES:
        monkeypatch.delenv(name, raising=False)
    for name in tuple(os.environ):
        if name.startswith("KAGGLE_"):
            monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("MY_DATA_HUB_ENVIRONMENT", "production")
    monkeypatch.setenv("MY_DATA_HUB_CONTROL_LEDGER_PATH", str(root / "control.sqlite3"))
    monkeypatch.setenv("MY_DATA_HUB_MCP_REMOTE_ENABLED", "true")
    monkeypatch.setenv("MY_DATA_HUB_MCP_WRITE_ENABLED", "false")
    monkeypatch.setenv("MY_DATA_HUB_MCP_AUTH_MODE", "oauth")
    monkeypatch.setenv("MY_DATA_HUB_MCP_HOST", "127.0.0.1")
    monkeypatch.setenv("MY_DATA_HUB_MCP_ALLOWED_HOSTS", "mcp-datahub.kenigevents.ru")
    monkeypatch.setenv("MY_DATA_HUB_MCP_ALLOWED_ORIGINS", "https://chatgpt.com")
    monkeypatch.setenv("MY_DATA_HUB_MCP_OAUTH_ISSUER", ISSUER)
    monkeypatch.setenv("MY_DATA_HUB_MCP_OAUTH_AUDIENCE", RESOURCE)
    monkeypatch.setenv("MY_DATA_HUB_MCP_OAUTH_RESOURCE", RESOURCE)
    monkeypatch.setenv("MY_DATA_HUB_MCP_OAUTH_JWKS_URL", f"{ISSUER}/.well-known/jwks.json")
    monkeypatch.setenv("MY_DATA_HUB_MCP_SCOPES", ",".join(sorted(READER_SCOPES)))


def _claims(*, revoked: bool = False) -> dict[str, object]:
    now = int(time.time())
    return {
        "iss": ISSUER,
        "aud": RESOURCE,
        "resource": RESOURCE,
        "sub": "datahub-reader",
        "client_id": "chatgpt-reader",
        "jti": "revoked-token" if revoked else "reader-token",
        "iat": now - 5,
        "nbf": now - 5,
        "exp": now + 120,
        "scope": " ".join(sorted(READER_SCOPES)),
    }


@asynccontextmanager
async def _lifespan(app: Any) -> AsyncIterator[None]:
    """Drive ASGI lifespan without adding a test-only runtime dependency."""

    receive_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
    send_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()

    async def receive() -> dict[str, Any]:
        return await receive_queue.get()

    async def send(message: dict[str, Any]) -> None:
        await send_queue.put(message)

    task = asyncio.create_task(
        app({"type": "lifespan", "asgi": {"version": "3.0"}, "state": {}}, receive, send)
    )
    await receive_queue.put({"type": "lifespan.startup"})
    startup = await send_queue.get()
    assert startup["type"] == "lifespan.startup.complete"
    try:
        yield
    finally:
        await receive_queue.put({"type": "lifespan.shutdown"})
        shutdown = await send_queue.get()
        assert shutdown["type"] == "lifespan.shutdown.complete"
        await task


@pytest.mark.asyncio
async def test_remote_runtime_uses_only_control_ledger_and_denies_revoked_token(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _configure(monkeypatch, tmp_path)
    decoded = _claims()
    runtime = build_remote_runtime(decoder=lambda _token: decoded)
    runtime.ledger.register_oauth_client(
        issuer=ISSUER,
        client_id="chatgpt-reader",
        principal_id="datahub-reader",
        allowed_scopes=READER_SCOPES,
        profile_kind="reader",
    )

    identity = await runtime.validator.validate_token("signed-reader-token")
    assert identity.subject == "datahub-reader"
    assert runtime.ledger.resolve_service("postgres-master") is None

    query = {
        "issuer": ISSUER,
        "token_id": "reader-token",
        "client_id": "chatgpt-reader",
        "subject": "datahub-reader",
        "issued_at": int(decoded["iat"]),
    }
    import json

    runtime.ledger.revoke_oauth_reference(
        token_reference=json.dumps(query, sort_keys=True, separators=(",", ":")),
        client_id="chatgpt-reader",
        principal_id="datahub-reader",
        reason_code="test_rotation",
        audit_ref="test-revocation",
    )
    with pytest.raises(TokenValidationError):
        await runtime.validator.validate_token("signed-reader-token")


@pytest.mark.asyncio
async def test_remote_runtime_serves_rfc9728_metadata_without_master(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _configure(monkeypatch, tmp_path)
    runtime = build_remote_runtime(decoder=lambda _token: _claims())
    transport = httpx.ASGITransport(app=runtime.app)  # type: ignore[arg-type]
    async with httpx.AsyncClient(
        transport=transport, base_url="https://mcp-datahub.kenigevents.ru"
    ) as client:
        response = await client.get(
            "/.well-known/oauth-protected-resource/mcp",
            headers={"Host": "mcp-datahub.kenigevents.ru"},
        )
    assert response.status_code == 200
    assert response.json()["resource"] == RESOURCE
    assert response.headers["cache-control"] == "no-store"


@pytest.mark.asyncio
async def test_standard_mcp_client_lists_reader_catalog_and_reads_absent_status(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _configure(monkeypatch, tmp_path)
    runtime = build_remote_runtime(decoder=lambda _token: _claims())
    runtime.ledger.register_oauth_client(
        issuer=ISSUER,
        client_id="chatgpt-reader",
        principal_id="datahub-reader",
        allowed_scopes=READER_SCOPES,
        profile_kind="reader",
    )
    client = httpx2.AsyncClient(
        transport=httpx2.ASGITransport(app=runtime.app),
        headers={
            "Authorization": "Bearer signed-reader-token",
            "Host": "mcp-datahub.kenigevents.ru",
            "Origin": "https://chatgpt.com",
        },
    )
    async with _lifespan(runtime.app), client, streamable_http_client(
        "https://mcp-datahub.kenigevents.ru/mcp", http_client=client
    ) as (read_stream, write_stream), ClientSession(read_stream, write_stream) as session:
        await session.initialize()
        tools = await session.list_tools()
        result = await session.call_tool("platform.status", {})
    names = {tool.name for tool in tools.tools}
    assert "bloggers.search" in names
    assert "data.change.apply" not in names
    assert result.is_error is False
    assert result.structured_content["master_state"] == "ABSENT"


def test_remote_runtime_rejects_any_database_environment(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _configure(monkeypatch, tmp_path)
    monkeypatch.setenv("PGSSLKEY", "/secret/client.key")
    with pytest.raises(Exception, match="must not receive master database credentials"):
        build_remote_runtime(decoder=lambda _token: _claims())


def test_remote_operator_runtime_requires_and_accepts_only_injected_dependencies(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _configure(monkeypatch, tmp_path)
    monkeypatch.setenv("MY_DATA_HUB_MCP_WRITE_ENABLED", "true")
    monkeypatch.setenv("MY_DATA_HUB_MCP_OPERATOR_PROFILE_ENABLED", "true")
    monkeypatch.setenv(
        "MY_DATA_HUB_MCP_SCOPES",
        ",".join(sorted(READER_SCOPES | {"data:write", "provider:write"})),
    )
    with pytest.raises(Exception, match="require injected gate"):
        build_remote_runtime(decoder=lambda _token: _claims())

    runtime = build_remote_runtime(
        decoder=lambda _token: _claims(),
        write_gate=object(),  # type: ignore[arg-type]
        provider_control=object(),
        sql_policy=BoundedSQLPolicy(change_targets=frozenset({"hub.project"})),
    )

    assert runtime.settings.mcp_write_enabled is True
    assert runtime.settings.mcp_operator_profile_enabled is True


def test_remote_runtime_never_accepts_or_constructs_kaggle_provider_authority(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _configure(monkeypatch, tmp_path)
    monkeypatch.setenv("KAGGLE_API_TOKEN", "must-remain-control-only")
    with pytest.raises(Exception, match="must not receive Kaggle"):
        build_remote_runtime(decoder=lambda _token: _claims())

    source = Path("src/my_data_hub/mcp/runtime.py").read_text(encoding="utf-8")
    assert "KaggleProviderAdapter" not in source
    assert "ControlLedgerKaggleJournal" not in source
    assert "from_environment" not in source
