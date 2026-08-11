from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from jsonschema import Draft202012Validator

from my_data_hub.auth.context import current_identity, identity_context
from my_data_hub.auth.control import OAuthAuditEvent
from my_data_hub.mcp.admission import HTTPAdmissionSecurity
from my_data_hub.mcp.catalog import TOOL_CONTRACTS, security_catalog
from my_data_hub.mcp.contracts import (
    EnsureMasterReceipt,
    MasterSnapshot,
    MasterState,
    SessionRequest,
    WritePermit,
)
from my_data_hub.mcp.oauth import AccessIdentity
from my_data_hub.mcp.postgres_broker import SessionBrokerError
from my_data_hub.mcp.server import create_server
from my_data_hub.mcp.service import HubPermissionError, HubService

NOW = 2_000_000_000
RESOURCE = "https://mcp-datahub.kenigevents.ru/mcp"


def identity(*scopes: str, subject: str = "datahub-owner") -> AccessIdentity:
    return AccessIdentity(
        subject=subject,
        client_id="chatgpt-reader" if subject == "reader" else "chatgpt-owner-operator",
        scopes=frozenset(scopes),
        audience=RESOURCE,
        token_id=f"token:{subject}",
        expires_at=NOW + 300,
        issuer="https://identity.example",
        issued_at=NOW - 10,
        resource=RESOURCE,
    )


READER = identity(
    "platform:read",
    "master:read",
    "operation:read",
    "checkpoint:read",
    "embedding:read",
    "provider:read",
    "bloggers:read",
    "data:read",
    subject="reader",
)
OWNER = identity(*(contract.scope for contract in TOOL_CONTRACTS.values()))


class Resolver:
    def __init__(self, snapshot: MasterSnapshot) -> None:
        self.snapshot = snapshot
        self.ensures: list[tuple[str, str]] = []

    async def resolve_master(self, principal: AccessIdentity) -> MasterSnapshot:
        assert principal.subject
        return self.snapshot

    async def ensure_master(self, principal: AccessIdentity, *, intent: str) -> EnsureMasterReceipt:
        self.ensures.append((principal.subject, intent))
        return EnsureMasterReceipt("op-cold-start-01", MasterState.REQUESTED, False, intent)


class Session:
    def __init__(self, result: dict[str, Any]) -> None:
        self.result = result
        self.closed = False
        self.arguments: dict[str, Any] | None = None

    async def execute(self, arguments):  # type: ignore[no-untyped-def]
        self.arguments = dict(arguments)
        return self.result

    async def close(self) -> None:
        self.closed = True


class Broker:
    def __init__(self, result: dict[str, Any]) -> None:
        self.session = Session(result)
        self.requests: list[SessionRequest] = []

    async def issue_session(self, request: SessionRequest) -> Session:
        self.requests.append(request)
        return self.session


class RejectingStaleBroker(Broker):
    async def issue_session(self, request: SessionRequest) -> Session:
        self.requests.append(request)
        if request.epoch < 7:
            raise SessionBrokerError("credential is bound to a different master epoch")
        return self.session


class Control:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any], str]] = []

    async def invoke_control(self, tool, arguments, principal):  # type: ignore[no-untyped-def]
        self.calls.append((tool, dict(arguments), principal.subject))
        return {"tool": tool, "master_state": "spoofed", "operation_id": arguments.get("operation_id")}


class Audit:
    def __init__(self) -> None:
        self.events: list[OAuthAuditEvent] = []

    async def record_mcp_audit(self, event: OAuthAuditEvent) -> None:
        self.events.append(event)


class Gate:
    def __init__(self, permit: WritePermit) -> None:
        self.permit = permit

    async def authorize_write(self, **_kwargs):  # type: ignore[no-untyped-def]
        return self.permit

    async def record_write_result(self, *, permit, result):  # type: ignore[no-untyped-def]
        assert permit == self.permit
        return result


def active() -> MasterSnapshot:
    return MasterSnapshot(
        MasterState.ACTIVE,
        instance_id="master-01",
        epoch=7,
        canonical_revision=41,
        capabilities=frozenset({"bloggers_ru_v1"}),
    )


@pytest.mark.asyncio
async def test_stale_epoch_probe_uses_real_broker_admission_without_session_mutation() -> None:
    broker = RejectingStaleBroker({})
    service = HubService(Resolver(active()), broker=broker, fallback_identity=OWNER)

    result = await service.invoke(
        "runtime.stale_epoch.probe",
        {"expected_active_epoch": 7, "submitted_epoch": 6},
    )

    assert result == {
        "evaluated": True,
        "denied": True,
        "mutation_attempted": False,
        "reason_code": "STALE_EPOCH",
    }
    assert [request.epoch for request in broker.requests] == [7, 6]
    assert all(request.tool == "runtime.stale_epoch.probe" for request in broker.requests)


def permit(tool: str, **changes: Any) -> WritePermit:
    result = WritePermit(
        permit_id="permit-01",
        tool=tool,
        principal=OWNER.subject,
        client_id=OWNER.client_id,
        master_epoch=7,
        canonical_revision=41,
        expires_at=NOW + 60,
        preview_bound=True,
        checkpoint_lifecycle_bound=True,
        pre_change_checkpoint_verified=True,
    )
    return replace(result, **changes)


@pytest.mark.asyncio
async def test_status_is_healthy_when_master_is_absent() -> None:
    service = HubService(Resolver(MasterSnapshot(MasterState.ABSENT)), fallback_identity=READER)
    assert await service.invoke("platform.status", {}) == {
        "control_plane_ready": True,
        "master_state": "ABSENT",
        "operation_id": None,
        "instance_id": None,
        "master_epoch": None,
        "canonical_revision": None,
        "lease_expires_at": None,
        "capabilities": [],
        "canonical_database_location": "kaggle-master-only",
    }


@pytest.mark.asyncio
async def test_data_cold_start_returns_durable_operation_id_without_opening_session() -> None:
    resolver = Resolver(MasterSnapshot(MasterState.ABSENT))
    service = HubService(resolver, fallback_identity=READER)
    result = await service.invoke("bloggers.search", {"query": "kaliningrad", "limit": 10})
    assert result["operation_id"] == "op-cold-start-01"
    assert result["master_state"] == "REQUESTED"
    assert resolver.ensures == [("reader", "mcp-read:bloggers.search")]


@pytest.mark.asyncio
async def test_provider_resource_read_uses_control_gateway_without_master_resolution() -> None:
    resolver = Resolver(MasterSnapshot(MasterState.ABSENT))
    control = Control()
    service = HubService(resolver, control=control, fallback_identity=OWNER)
    arguments = {
        "resource_ref": "owner/notebook",
        "control_class": "mcp_managed",
        "private": True,
        "payload": {"kind": "notebook", "claim_sha256": "a" * 64},
    }

    result = await service.invoke("provider.resources.read", arguments)

    assert result["tool"] == "provider.resources.read"
    assert resolver.ensures == []
    assert control.calls == [("provider.resources.read", arguments, OWNER.subject)]


@pytest.mark.asyncio
async def test_active_read_uses_actual_identity_and_epoch_bound_reader_session() -> None:
    broker = Broker({"results": [], "master_epoch": 7, "canonical_revision": 41})
    audit = Audit()
    service = HubService(Resolver(active()), broker=broker, audit=audit, fallback_identity=OWNER)
    with identity_context(READER):
        result = await service.invoke("bloggers.list", {"limit": 20})
    assert result["master_epoch"] == 7
    assert broker.requests[0].principal.subject == "reader"
    assert broker.requests[0].role == "reader"
    assert broker.requests[0].epoch == 7
    assert broker.session.closed
    assert audit.events[0].subject == "reader"


@pytest.mark.asyncio
async def test_reader_catalog_never_lists_or_executes_write_tools() -> None:
    settings = SimpleNamespace(mcp_remote_enabled=True, mcp_scopes=frozenset(), mcp_oauth_resource=RESOURCE)
    server = create_server(settings, default_identity=OWNER)  # type: ignore[arg-type]
    with identity_context(READER):
        tools = await server.list_tools()
        denied = await server.call_tool("data.change.apply", {})
    names = {tool.name for tool in tools}
    assert "bloggers.search" in names
    assert "data.change.apply" not in names
    assert "provider.resources.delete" not in names
    assert denied.is_error
    assert denied.meta and "mcp/www_authenticate" in denied.meta


@pytest.mark.asyncio
async def test_owner_catalog_has_per_tool_security_and_truthful_annotations() -> None:
    settings = SimpleNamespace(mcp_remote_enabled=True, mcp_scopes=frozenset(), mcp_oauth_resource=RESOURCE)
    server = create_server(settings, default_identity=OWNER)  # type: ignore[arg-type]
    tools = {tool.name: tool for tool in await server.list_tools()}
    assert tools["bloggers.search"].annotations.read_only_hint is True
    assert tools["bloggers.search"].annotations.destructive_hint is False
    assert tools["data.change.apply"].annotations.read_only_hint is False
    assert tools["data.change.apply"].annotations.destructive_hint is True
    assert tools["data.change.apply"].meta == {
        "securitySchemes": [{"type": "oauth2", "scopes": ["data:write"]}]
    }
    assert server.security_schemes[0]["type"] == "oauth2"


@pytest.mark.asyncio
async def test_writes_fail_closed_without_injected_preview_checkpoint_gate() -> None:
    service = HubService(Resolver(active()), broker=Broker({}), fallback_identity=OWNER, clock=lambda: NOW)
    arguments = {
        "sql": "UPDATE hub.project SET name=$1 WHERE project_id=$2",
        "parameters": ["name", "p1"],
        "expected_revision": 41,
        "max_affected_rows": 1,
        "idempotency_key": "request-0001",
    }
    with pytest.raises(HubPermissionError, match="fail-closed"):
        await service.invoke("data.change.preview", arguments)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    [
        {"principal": "other"},
        {"client_id": "other"},
        {"master_epoch": 6},
        {"expires_at": NOW},
        {"checkpoint_lifecycle_bound": False},
        {"preview_bound": False},
        {"pre_change_checkpoint_verified": False},
    ],
)
async def test_apply_rejects_stale_or_mismatched_write_permits(change: dict[str, Any]) -> None:
    tool = "data.change.apply"
    service = HubService(
        Resolver(active()),
        broker=Broker({}),
        write_gate=Gate(permit(tool, **change)),
        fallback_identity=OWNER,
        clock=lambda: NOW,
    )
    arguments = {
        "sql": "DELETE FROM hub.project WHERE project_id=$1",
        "parameters": ["p1"],
        "expected_revision": 41,
        "max_affected_rows": 1,
        "idempotency_key": "request-0001",
        "preview_receipt": "signed-preview",
    }
    with pytest.raises(HubPermissionError):
        await service.invoke(tool, arguments)


@pytest.mark.asyncio
async def test_apply_only_returns_checkpoint_lifecycle_operation() -> None:
    arguments = {
        "sql": "UPDATE hub.project SET name=$1 WHERE project_id=$2",
        "parameters": ["name", "p1"],
        "expected_revision": 41,
        "max_affected_rows": 1,
        "idempotency_key": "request-0001",
        "preview_receipt": "signed-preview",
    }
    broker = Broker(
        {
            "operation_id": "op-write-01",
            "status": "COMMITTED_PENDING_CHECKPOINT",
            "master_epoch": 7,
            "canonical_revision": 42,
        }
    )
    service = HubService(
        Resolver(active()), broker=broker, write_gate=Gate(permit("data.change.apply")),
        fallback_identity=OWNER, clock=lambda: NOW
    )
    assert (await service.invoke("data.change.apply", arguments))["operation_id"] == "op-write-01"

    broker.session.result = {"status": "applied", "master_epoch": 7, "canonical_revision": 42}
    with pytest.raises(RuntimeError, match="durability"):
        await service.invoke("data.change.apply", arguments)


@pytest.mark.asyncio
async def test_provider_mutations_deny_protected_or_public_resources_before_gateway() -> None:
    control = Control()
    provider_permit = permit(
        "provider.resources.delete", allowed_resource_class="mcp_managed", private_resource_only=True
    )
    service = HubService(
        Resolver(active()), control=control, write_gate=Gate(provider_permit),
        fallback_identity=OWNER, clock=lambda: NOW
    )
    with pytest.raises(HubPermissionError, match="control classes"):
        await service.invoke(
            "provider.resources.delete",
            {"resource_ref": "x", "control_class": "orchestrator_protected", "private": True},
        )
    with pytest.raises(HubPermissionError, match="public"):
        await service.invoke(
            "provider.resources.delete",
            {"resource_ref": "x", "control_class": "mcp_managed", "private": False},
        )
    assert not control.calls


def test_reader_security_catalog_schema_and_no_writes() -> None:
    catalog = security_catalog(READER)
    schema = json.loads(Path("schemas/mcp/security-catalog.v1.schema.json").read_text())
    Draft202012Validator(schema).validate(catalog)
    assert all(item["annotations"]["readOnlyHint"] for item in catalog["tools"])
    assert {item["name"] for item in catalog["tools"]}.isdisjoint(
        {"data.change.apply", "provider.resources.delete", "master.ensure"}
    )


@pytest.mark.asyncio
async def test_http_admission_binds_verified_token_identity_only_for_request() -> None:
    seen: list[AccessIdentity | None] = []

    async def app(_scope, _receive, send):  # type: ignore[no-untyped-def]
        seen.append(current_identity())
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"{}"})

    security = HTTPAdmissionSecurity(
        app,
        allowed_hosts=("mcp-datahub.kenigevents.ru",),
        allowed_origins=("https://chatgpt.com",),
        authenticator=lambda _header: READER,
    )
    request_messages = iter([{"type": "http.request", "body": b"{}", "more_body": False}])
    response: list[dict[str, Any]] = []

    async def receive() -> dict[str, Any]:
        return next(request_messages)

    async def send(message: dict[str, Any]) -> None:
        response.append(message)

    await security(
        {
            "type": "http",
            "method": "POST",
            "path": "/mcp",
            "query_string": b"",
            "client": ("203.0.113.10", 50000),
            "headers": [
                (b"host", b"mcp-datahub.kenigevents.ru"),
                (b"origin", b"https://chatgpt.com"),
                (b"authorization", b"Bearer signed-token"),
                (b"content-length", b"2"),
            ],
        },
        receive,
        send,
    )
    assert seen == [READER]
    assert current_identity() is None
    headers = dict(response[0]["headers"])
    assert headers[b"cache-control"] == b"no-store"


def test_committed_examples_validate() -> None:
    pairs = [
        ("schemas/mcp/security-catalog.v1.schema.json", "examples/mcp/reader-security-catalog.v1.json"),
        ("schemas/oauth/provider-metadata.v1.schema.json", "examples/oauth/provider-metadata.v1.json"),
    ]
    for schema_path, example_path in pairs:
        schema = json.loads(Path(schema_path).read_text())
        example = json.loads(Path(example_path).read_text())
        Draft202012Validator(schema, format_checker=Draft202012Validator.FORMAT_CHECKER).validate(example)
