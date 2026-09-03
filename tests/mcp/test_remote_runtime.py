from __future__ import annotations

import asyncio
import json
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
from my_data_hub.mcp.catalog import ALL_SCOPES, TOOL_CONTRACTS
from my_data_hub.mcp.contracts import MasterSnapshot, MasterState
from my_data_hub.mcp.oauth import AccessIdentity, TokenValidationError
from my_data_hub.mcp.runtime import ProviderOnlyWriteGate, UnifiedBootstrapWriteGate, build_remote_runtime
from my_data_hub.mcp.server import (
    PROVIDER_ONLY_TOOLS,
    UNIFIED_BOOTSTRAP_TOOLS,
    MCPDependencies,
    create_server,
)
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
        "region-talk:read",
    }
)
OAUTH_PROTOCOL_SCOPES = frozenset({"openid", "offline_access"})
UNIFIED_SCOPES = READER_SCOPES | {"provider:write"}


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


def _claims(*, revoked: bool = False, scopes: list[str] | None = None) -> dict[str, object]:
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
        "scope": " ".join(sorted(scopes or READER_SCOPES)),
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

    task = asyncio.create_task(app({"type": "lifespan", "asgi": {"version": "3.0"}, "state": {}}, receive, send))
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


def _capturing_app(app: Any, payloads: list[dict[str, object]]):  # type: ignore[no-untyped-def]
    async def capture_http(scope, receive, send):  # type: ignore[no-untyped-def]
        bodies: list[bytes] = []

        async def capture_send(message):  # type: ignore[no-untyped-def]
            if message.get("type") == "http.response.body":
                bodies.append(message.get("body", b""))
            await send(message)

        await app(scope, receive, capture_send)
        if bodies:
            try:
                value = json.loads(b"".join(bodies))
            except (UnicodeDecodeError, json.JSONDecodeError):
                return
            if isinstance(value, dict):
                payloads.append(value)

    return capture_http


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
async def test_remote_runtime_accepts_oauth_protocol_scopes_without_advertising_them(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _configure(monkeypatch, tmp_path)
    granted = READER_SCOPES | OAUTH_PROTOCOL_SCOPES
    runtime = build_remote_runtime(decoder=lambda _token: _claims(scopes=sorted(granted)))
    runtime.ledger.register_oauth_client(
        issuer=ISSUER,
        client_id="chatgpt-reader",
        principal_id="datahub-reader",
        allowed_scopes=granted,
        profile_kind="reader",
    )

    identity = await runtime.validator.validate_token("signed-reader-token")
    assert identity.scopes == granted

    transport = httpx.ASGITransport(app=runtime.app)  # type: ignore[arg-type]
    async with httpx.AsyncClient(
        transport=transport, base_url="https://mcp-datahub.kenigevents.ru"
    ) as client:
        metadata = await client.get(
            "/.well-known/oauth-protected-resource/mcp",
            headers={"Host": "mcp-datahub.kenigevents.ru"},
        )
    assert metadata.status_code == 200
    assert OAUTH_PROTOCOL_SCOPES.isdisjoint(metadata.json()["scopes_supported"])


@pytest.mark.asyncio
async def test_remote_runtime_serves_rfc9728_metadata_without_master(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _configure(monkeypatch, tmp_path)
    runtime = build_remote_runtime(decoder=lambda _token: _claims())
    transport = httpx.ASGITransport(app=runtime.app)  # type: ignore[arg-type]
    async with httpx.AsyncClient(transport=transport, base_url="https://mcp-datahub.kenigevents.ru") as client:
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
    http_payloads: list[dict[str, object]] = []

    client = httpx2.AsyncClient(
        transport=httpx2.ASGITransport(app=_capturing_app(runtime.app, http_payloads)),
        headers={
            "Authorization": "Bearer signed-reader-token",
            "Host": "mcp-datahub.kenigevents.ru",
            "Origin": "https://chatgpt.com",
        },
    )
    async with (
        _lifespan(runtime.app),
        client,
        streamable_http_client("https://mcp-datahub.kenigevents.ru/mcp", http_client=client) as (
            read_stream,
            write_stream,
        ),
        ClientSession(read_stream, write_stream) as session,
    ):
        await session.initialize()
        tools = await session.list_tools()
        result = await session.call_tool("platform.status", {})
    names = {tool.name for tool in tools.tools}
    assert "bloggers.search" in names
    assert "data.query" not in names
    assert "submit_discovery_batch" not in names
    assert "data.change.apply" not in names
    assert result.is_error is False
    assert result.structured_content["master_state"] == "ABSENT"
    tools_payload = next(
        payload
        for payload in http_payloads
        if isinstance(payload.get("result"), dict)
        and isinstance(payload["result"].get("tools"), list)  # type: ignore[union-attr]
    )
    assert tools_payload["result"]["securitySchemes"] == [  # type: ignore[index]
        {"type": "oauth2", "scopes": sorted(READER_SCOPES)}
    ]


def test_remote_runtime_rejects_any_database_environment(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
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


def test_remote_provider_only_profile_has_exact_catalog_and_no_master_checkpoint_or_data_tools(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _configure(monkeypatch, tmp_path)
    monkeypatch.setenv("MY_DATA_HUB_MCP_WRITE_ENABLED", "true")
    monkeypatch.setenv("MY_DATA_HUB_MCP_PROVIDER_PROFILE_ENABLED", "true")
    monkeypatch.setenv("MY_DATA_HUB_MCP_SCOPES", "platform:read,provider:read,provider:write")
    gate = tmp_path / "provider-write-gate.key"
    gate.write_text("w" * 32, encoding="ascii")
    gate.chmod(0o600)
    monkeypatch.setenv("MY_DATA_HUB_MCP_WRITE_GATE_SECRET_FILE", str(gate))
    gateway_token = tmp_path / "provider-gateway.token"
    gateway_token.write_text("g" * 32, encoding="ascii")
    gateway_token.chmod(0o600)
    monkeypatch.setenv(
        "MY_DATA_HUB_MCP_CONTROL_GATEWAY_URL",
        "http://control-plane:8080/internal/mcp-provider/invoke",
    )
    monkeypatch.setenv("MY_DATA_HUB_MCP_CONTROL_GATEWAY_TOKEN_FILE", str(gateway_token))
    runtime = build_remote_runtime(
        decoder=lambda _token: _claims(scopes=["platform:read", "provider:read", "provider:write"]),
        provider_control=object(),
    )
    assert runtime.settings.mcp_provider_profile_enabled is True
    assert runtime.settings.mcp_operator_profile_enabled is False
    assert runtime.settings.mcp_scopes == {
        "platform:read",
        "provider:read",
        "provider:write",
    }

    async def resource_metadata() -> dict[str, object]:
        transport = httpx.ASGITransport(app=runtime.app)  # type: ignore[arg-type]
        async with httpx.AsyncClient(
            transport=transport, base_url="https://mcp-datahub.kenigevents.ru"
        ) as client:
            response = await client.get(
                "/.well-known/oauth-protected-resource/mcp",
                headers={"Host": "mcp-datahub.kenigevents.ru"},
            )
        assert response.status_code == 200
        return response.json()  # type: ignore[no-any-return]

    assert set(asyncio.run(resource_metadata())["scopes_supported"]) == {
        "platform:read",
        "provider:read",
        "provider:write",
    }

    identity = AccessIdentity(
        subject="provider-owner",
        client_id="provider-operator",
        scopes=frozenset({"platform:read", "provider:read", "provider:write"}),
        audience="https://mcp-datahub.kenigevents.ru/mcp",
        token_id="provider-jti",
        expires_at=2**31,
        issuer="https://identity.kenigevents.ru",
        issued_at=1,
        resource="https://mcp-datahub.kenigevents.ru/mcp",
    )
    allowed = PROVIDER_ONLY_TOOLS & TOOL_CONTRACTS.keys()
    server = create_server(
        runtime.settings,
        dependencies=MCPDependencies(provider_only_profile_enabled=True),
        default_identity=identity,
    )
    assert {tool.name for tool in asyncio.run(server.list_tools())} == allowed
    for forbidden in TOOL_CONTRACTS.keys() - allowed:
        result = asyncio.run(server.call_tool(forbidden, {}))
        assert result.is_error is True


@pytest.mark.parametrize(
    "scopes",
    [
        "provider:read,provider:write",
        "platform:read,provider:write",
        *[
            "platform:read,provider:read,provider:write," + scope
            for scope in sorted(ALL_SCOPES - {"platform:read", "provider:read", "provider:write"})
        ],
    ],
)
def test_remote_provider_only_profile_rejects_missing_or_extra_scope(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, scopes: str
) -> None:
    _configure(monkeypatch, tmp_path)
    monkeypatch.setenv("MY_DATA_HUB_MCP_WRITE_ENABLED", "true")
    monkeypatch.setenv("MY_DATA_HUB_MCP_PROVIDER_PROFILE_ENABLED", "true")
    monkeypatch.setenv("MY_DATA_HUB_MCP_SCOPES", scopes)
    with pytest.raises(Exception, match="provider-only"):
        build_remote_runtime(decoder=lambda _token: _claims())


def test_provider_only_write_gate_is_canonical_independent_and_private() -> None:
    identity = AccessIdentity(
        subject="provider-owner",
        client_id="provider-client",
        scopes=frozenset({"provider:write"}),
        audience=RESOURCE,
        token_id="write-jti",
        expires_at=2**31,
        issuer=ISSUER,
        issued_at=1,
        resource=RESOURCE,
    )
    gate = ProviderOnlyWriteGate(b"w" * 32, clock=lambda: 1000)
    permit = gate.authorize_write(
        principal=identity,
        tool="provider.resources.run",
        arguments={"control_class": "mcp_managed", "private": True},
        master=MasterSnapshot(MasterState.ABSENT),
    )
    assert permit.canonical_data_independent is True
    assert permit.master_epoch == 0
    assert permit.canonical_revision == 0
    assert permit.checkpoint_lifecycle_bound is False
    assert permit.pre_change_checkpoint_verified is False


def test_unified_bootstrap_profile_has_exact_bounded_catalog_and_provider_only_writes(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _configure(monkeypatch, tmp_path)
    monkeypatch.setenv("MY_DATA_HUB_MCP_WRITE_ENABLED", "true")
    monkeypatch.setenv("MY_DATA_HUB_MCP_UNIFIED_BOOTSTRAP_PROFILE_ENABLED", "true")
    monkeypatch.setenv("MY_DATA_HUB_MCP_SCOPES", ",".join(sorted(UNIFIED_SCOPES)))
    gate = tmp_path / "unified-write-gate.key"
    gate.write_text("u" * 32, encoding="ascii")
    gate.chmod(0o600)
    gateway = tmp_path / "gateway.token"
    gateway.write_text("g" * 32, encoding="ascii")
    gateway.chmod(0o600)
    monkeypatch.setenv("MY_DATA_HUB_MCP_WRITE_GATE_SECRET_FILE", str(gate))
    monkeypatch.setenv(
        "MY_DATA_HUB_MCP_CONTROL_GATEWAY_URL",
        "http://control-plane:8080/internal/mcp-provider/invoke",
    )
    monkeypatch.setenv("MY_DATA_HUB_MCP_CONTROL_GATEWAY_TOKEN_FILE", str(gateway))
    runtime = build_remote_runtime(
        decoder=lambda _token: _claims(scopes=sorted(UNIFIED_SCOPES)),
        provider_control=object(),
    )

    assert runtime.settings.mcp_unified_bootstrap_profile_enabled is True
    assert runtime.settings.mcp_operator_profile_enabled is False
    assert runtime.settings.mcp_provider_profile_enabled is False
    assert isinstance(runtime.app, object)

    identity = AccessIdentity(
        subject="unified-owner",
        client_id="opencode-my-data-hub",
        scopes=frozenset(UNIFIED_SCOPES),
        audience=RESOURCE,
        token_id="unified-jti",
        expires_at=2**31,
        issuer=ISSUER,
        issued_at=1,
        resource=RESOURCE,
    )
    server = create_server(
        runtime.settings,
        dependencies=MCPDependencies(unified_bootstrap_profile_enabled=True),
        default_identity=identity,
    )
    catalog = {tool.name for tool in asyncio.run(server.list_tools())}
    # This direct fixture intentionally injects no Showcase backend; profile
    # allowlists never make unavailable tools discoverable.
    assert catalog == (UNIFIED_BOOTSTRAP_TOOLS & TOOL_CONTRACTS.keys()) - {"showcase.list"}
    assert "bloggers.search" in catalog
    assert "provider.resources.create" in catalog
    assert "data.change.apply" not in catalog
    assert "master.ensure" not in catalog
    assert "acceptance.scenario.request" not in catalog
    assert "provider.acceptance.dataset.lifecycle" not in catalog
    assert "provider.acceptance.notebook.lifecycle" not in catalog

    async def metadata() -> dict[str, object]:
        transport = httpx.ASGITransport(app=runtime.app)  # type: ignore[arg-type]
        async with httpx.AsyncClient(
            transport=transport, base_url="https://mcp-datahub.kenigevents.ru"
        ) as client:
            response = await client.get(
                "/.well-known/oauth-protected-resource/mcp",
                headers={"Host": "mcp-datahub.kenigevents.ru"},
            )
        return response.json()  # type: ignore[no-any-return]

    assert set(asyncio.run(metadata())["scopes_supported"]) == UNIFIED_SCOPES

    runtime.ledger.register_oauth_client(
        issuer=ISSUER,
        client_id="chatgpt-reader",
        principal_id="datahub-reader",
        allowed_scopes=UNIFIED_SCOPES,
        profile_kind="reader",
    )

    async def http_catalog() -> dict[str, object]:
        payloads: list[dict[str, object]] = []
        client = httpx2.AsyncClient(
            transport=httpx2.ASGITransport(app=_capturing_app(runtime.app, payloads)),
            headers={
                "Authorization": "Bearer unified-token",
                "Host": "mcp-datahub.kenigevents.ru",
                "Origin": "https://chatgpt.com",
            },
        )
        async with (
            _lifespan(runtime.app),
            client,
            streamable_http_client(
                "https://mcp-datahub.kenigevents.ru/mcp", http_client=client
            ) as (read_stream, write_stream),
            ClientSession(read_stream, write_stream) as session,
        ):
            await session.initialize()
            await session.list_tools()
        return next(
            payload
            for payload in payloads
            if isinstance(payload.get("result"), dict)
            and isinstance(payload["result"].get("tools"), list)  # type: ignore[union-attr]
        )

    actual_catalog = asyncio.run(http_catalog())
    assert actual_catalog["result"]["securitySchemes"] == [  # type: ignore[index]
        {"type": "oauth2", "scopes": sorted(UNIFIED_SCOPES)}
    ]
    assert {
        tool["name"] for tool in actual_catalog["result"]["tools"]  # type: ignore[index]
    } == (UNIFIED_BOOTSTRAP_TOOLS & TOOL_CONTRACTS.keys()) - {"showcase.list"}


def test_unified_bootstrap_gate_allows_provider_effect_during_active_master_only() -> None:
    identity = AccessIdentity(
        subject="provider-owner",
        client_id="unified-client",
        scopes=frozenset({"provider:write"}),
        audience=RESOURCE,
        token_id="write-jti",
        expires_at=2**31,
        issuer=ISSUER,
        issued_at=1,
        resource=RESOURCE,
    )
    gate = UnifiedBootstrapWriteGate(b"u" * 32, clock=lambda: 1000)
    permit = gate.authorize_write(
        principal=identity,
        tool="provider.resources.create",
        arguments={"control_class": "mcp_managed", "private": True},
        master=MasterSnapshot(MasterState.ACTIVE, instance_id="master", epoch=9),
    )
    assert permit.canonical_data_independent is True
    with pytest.raises(PermissionError, match="unified bootstrap"):
        gate.authorize_write(
            principal=identity,
            tool="data.change.apply",
            arguments={},
            master=MasterSnapshot(MasterState.ACTIVE, instance_id="master", epoch=9),
        )


@pytest.mark.parametrize(
    "scopes",
    [UNIFIED_SCOPES - {"bloggers:read"}, UNIFIED_SCOPES | {"data:read"}],
)
def test_unified_bootstrap_profile_rejects_nonexact_scope_catalog(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, scopes: frozenset[str]
) -> None:
    _configure(monkeypatch, tmp_path)
    monkeypatch.setenv("MY_DATA_HUB_MCP_WRITE_ENABLED", "true")
    monkeypatch.setenv("MY_DATA_HUB_MCP_UNIFIED_BOOTSTRAP_PROFILE_ENABLED", "true")
    monkeypatch.setenv("MY_DATA_HUB_MCP_SCOPES", ",".join(sorted(scopes)))
    gateway = tmp_path / "gateway.token"
    gateway.write_text("g" * 32, encoding="ascii")
    gateway.chmod(0o600)
    monkeypatch.setenv(
        "MY_DATA_HUB_MCP_CONTROL_GATEWAY_URL",
        "http://control-plane:8080/internal/mcp-provider/invoke",
    )
    monkeypatch.setenv("MY_DATA_HUB_MCP_CONTROL_GATEWAY_TOKEN_FILE", str(gateway))

    with pytest.raises(Exception, match="unified bootstrap"):
        build_remote_runtime(decoder=lambda _token: _claims(scopes=sorted(scopes)))


@pytest.mark.parametrize(
    ("tool", "arguments", "master"),
    [
        ("data.change.apply", {"control_class": "mcp_managed", "private": True}, MasterSnapshot(MasterState.ABSENT)),
        ("master.ensure", {"control_class": "mcp_managed", "private": True}, MasterSnapshot(MasterState.ABSENT)),
        ("checkpoint.restore", {"control_class": "mcp_managed", "private": True}, MasterSnapshot(MasterState.ABSENT)),
        (
            "provider.resources.run",
            {"control_class": "orchestrator_protected", "private": True},
            MasterSnapshot(MasterState.ABSENT),
        ),
        (
            "provider.resources.run",
            {"control_class": "mcp_managed", "private": False},
            MasterSnapshot(MasterState.ABSENT),
        ),
        (
            "provider.resources.run",
            {"control_class": "mcp_managed", "private": True},
            MasterSnapshot(MasterState.ACTIVE, instance_id="master", epoch=1),
        ),
    ],
)
def test_provider_only_write_gate_rejects_non_provider_or_canonical_effects(
    tool: str, arguments: dict[str, object], master: MasterSnapshot
) -> None:
    identity = AccessIdentity(
        subject="provider-owner",
        client_id="provider-client",
        scopes=frozenset({"provider:write"}),
        audience=RESOURCE,
        token_id="write-jti",
        expires_at=2**31,
        issuer=ISSUER,
        issued_at=1,
        resource=RESOURCE,
    )
    with pytest.raises(PermissionError, match="provider-only"):
        ProviderOnlyWriteGate(b"w" * 32).authorize_write(
            principal=identity, tool=tool, arguments=arguments, master=master
        )


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
