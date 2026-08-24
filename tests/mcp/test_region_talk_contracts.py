from __future__ import annotations

import json
import sys
from datetime import UTC, datetime, timedelta
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest
from pydantic import ValidationError

from my_data_hub.control_plane.adapters import LedgerControlReader
from my_data_hub.control_plane.ledger import ControlLedger
from my_data_hub.mcp.catalog import TOOL_CONTRACTS
from my_data_hub.mcp.contracts import (
    EnsureMasterReceipt,
    ExecutionLimits,
    MasterSnapshot,
    MasterState,
    SessionRequest,
)
from my_data_hub.mcp.oauth import AccessIdentity
from my_data_hub.mcp.postgres_broker import EpochDatabaseCredential, PostgresMasterSession
from my_data_hub.mcp.region_talk_schemas import RegionTalkPipelineRunRequest
from my_data_hub.mcp.server import (
    PROVIDER_ONLY_TOOLS,
    READER_PROFILE_TOOLS,
    UNIFIED_BOOTSTRAP_TOOLS,
    MCPDependencies,
    create_server,
)
from my_data_hub.mcp.service import HubPermissionError, HubService

RESOURCE = "https://mcp-datahub.kenigevents.ru/mcp"
SOURCE_REVISION = "1" * 40


def identity(*scopes: str, client_id: str = "opencode-my-data-hub") -> AccessIdentity:
    return AccessIdentity(
        subject="owner:one",
        client_id=client_id,
        scopes=frozenset(scopes),
        audience=RESOURCE,
        token_id=f"token:{client_id}",
        expires_at=2_000_000_300,
        issuer="https://identity.example",
        issued_at=1_999_999_990,
        resource=RESOURCE,
    )


class Resolver:
    def __init__(self, snapshot: MasterSnapshot) -> None:
        self.snapshot = snapshot
        self.resolves = 0
        self.ensures: list[str] = []

    async def resolve_master(self, _principal: AccessIdentity) -> MasterSnapshot:
        self.resolves += 1
        return self.snapshot

    async def ensure_master(
        self, _principal: AccessIdentity, *, intent: str
    ) -> EnsureMasterReceipt:
        self.ensures.append(intent)
        return EnsureMasterReceipt("region-talk-cold-master", MasterState.REQUESTED, False, intent)


class Control:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any], str]] = []

    async def invoke_control(self, tool, arguments, principal):  # type: ignore[no-untyped-def]
        self.calls.append((tool, dict(arguments), principal.client_id))
        return {"pipeline_state": "idle", "last_operation_id": None}


class Controller:
    def __init__(self) -> None:
        self.operations: dict[str, str] = {}
        self.requests: list[tuple[RegionTalkPipelineRunRequest, str]] = []

    async def request_supervised_run(
        self,
        *,
        request: RegionTalkPipelineRunRequest,
        principal: AccessIdentity,
    ) -> dict[str, Any]:
        duplicate = request.idempotency_key in self.operations
        operation_id = self.operations.setdefault(request.idempotency_key, "region-talk-run-01")
        self.requests.append((request, principal.client_id))
        return {"operation_id": operation_id, "duplicate": duplicate, "state": "REQUESTED"}


def settings() -> SimpleNamespace:
    return SimpleNamespace(
        mcp_remote_enabled=True,
        mcp_scopes=frozenset({"region-talk:read", "region-talk:operate"}),
        mcp_oauth_resource=RESOURCE,
    )


def test_region_talk_catalog_is_typed_and_provider_profile_is_unchanged() -> None:
    read_tools = {
        "region_talk.inventory",
        "region_talk.articles.list",
        "region_talk.articles.get",
        "region_talk.articles.search",
        "region_talk.posts.list",
        "region_talk.posts.get",
        "region_talk.posts.search",
        "region_talk.queue.list",
        "region_talk.queue.summary",
        "region_talk.pipeline.status",
    }
    assert read_tools <= READER_PROFILE_TOOLS
    assert read_tools <= UNIFIED_BOOTSTRAP_TOOLS
    assert not read_tools & PROVIDER_ONLY_TOOLS
    assert "region_talk.pipeline.run" not in UNIFIED_BOOTSTRAP_TOOLS
    assert "region_talk.pipeline.run" not in PROVIDER_ONLY_TOOLS
    assert all(TOOL_CONTRACTS[name].scope == "region-talk:read" for name in read_tools)
    assert all(TOOL_CONTRACTS[name].role == "reader" for name in read_tools)
    assert TOOL_CONTRACTS["region_talk.pipeline.run"].scope == "region-talk:operate"
    assert TOOL_CONTRACTS["region_talk.pipeline.run"].idempotent is True


@pytest.mark.asyncio
async def test_region_talk_tool_schemas_are_closed_bounded_and_accept_no_sql() -> None:
    controller = Controller()
    owner = identity("region-talk:read", "region-talk:operate")
    server = create_server(
        settings(),
        dependencies=MCPDependencies(
            region_talk_controller=controller,
            region_talk_pipeline_run_enabled=True,
        ),
        default_identity=owner,
    )

    tools = {tool.name: tool for tool in await server.list_tools()}
    for name, tool in tools.items():
        if not name.startswith("region_talk."):
            continue
        assert tool.input_schema["additionalProperties"] is False
        assert "sql" not in json.dumps(tool.input_schema).casefold()
    list_schema = tools["region_talk.articles.list"].input_schema["properties"]
    assert list_schema["limit"]["maximum"] == 100
    assert list_schema["max_bytes"]["maximum"] == 262_144
    assert list_schema["cursor"]["anyOf"][0]["pattern"].startswith("^v1:")
    queue_schema = tools["region_talk.queue.list"].input_schema["properties"]
    assert "channel" in queue_schema
    assert "category" not in queue_schema
    run_schema = tools["region_talk.pipeline.run"].input_schema
    assert set(run_schema["properties"]) == {"source_revision", "idempotency_key"}
    assert "publication_dispatch" not in run_schema["properties"]


@pytest.mark.asyncio
async def test_cold_region_talk_read_returns_waiting_continuation_after_validation() -> None:
    resolver = Resolver(MasterSnapshot(MasterState.ABSENT))
    service = HubService(
        resolver,
        fallback_identity=identity("region-talk:read"),
    )

    result = await service.invoke(
        "region_talk.articles.search",
        {"query": "Калининград", "limit": 25, "max_bytes": 65_536},
    )

    assert result["outcome"] == "WAITING_FOR_MASTER"
    assert result["operation_id"] == "region-talk-cold-master"
    assert result["continuation"] == {
        "operation_id": "region-talk-cold-master",
        "status_tool": "operation.get",
        "retry_original_request_when": "state=ACTIVE",
    }
    assert resolver.ensures == ["mcp-read:region_talk.articles.search"]

    with pytest.raises(ValidationError):
        await service.invoke("region_talk.articles.list", {"cursor": "raw-sql-offset"})
    assert resolver.ensures == ["mcp-read:region_talk.articles.search"]


@pytest.mark.asyncio
async def test_pipeline_status_is_control_only_and_preserves_distinct_oauth_clients() -> None:
    resolver = Resolver(MasterSnapshot(MasterState.ABSENT))
    control = Control()
    opencode = HubService(
        resolver,
        control=control,
        fallback_identity=identity("region-talk:read", client_id="opencode-my-data-hub"),
    )
    chatgpt = HubService(
        resolver,
        control=control,
        fallback_identity=identity(
            "region-talk:read",
            client_id="https://chatgpt.com/oauth/my-data-hub/client.json",
        ),
    )

    assert (await opencode.invoke("region_talk.pipeline.status", {}))["pipeline_state"] == "idle"
    assert (await chatgpt.invoke("region_talk.pipeline.status", {}))["pipeline_state"] == "idle"
    assert resolver.resolves == 0
    assert resolver.ensures == []
    assert [call[2] for call in control.calls] == [
        "opencode-my-data-hub",
        "https://chatgpt.com/oauth/my-data-hub/client.json",
    ]


@pytest.mark.asyncio
async def test_reader_pipeline_status_uses_local_ledger_without_write_gateway_or_master(
    tmp_path,
) -> None:  # type: ignore[no-untyped-def]
    ledger = ControlLedger(tmp_path / "control.sqlite3")
    resolver = Resolver(MasterSnapshot(MasterState.ABSENT))
    service = HubService(
        resolver,
        control=LedgerControlReader(ledger),
        fallback_identity=identity("region-talk:read"),
    )

    result = await service.invoke("region_talk.pipeline.status", {})

    assert result == {
        "ready": True,
        "state": "IDLE",
        "publication_dispatch": False,
        "latest": None,
    }
    assert resolver.resolves == 0
    assert resolver.ensures == []


@pytest.mark.asyncio
async def test_pipeline_run_is_gated_idempotent_and_hard_disables_publication() -> None:
    controller = Controller()
    resolver = Resolver(MasterSnapshot(MasterState.ABSENT))
    owner = identity("region-talk:operate")
    service = HubService(
        resolver,
        region_talk_controller=controller,
        fallback_identity=owner,
    )
    arguments = {
        "source_revision": SOURCE_REVISION,
        "idempotency_key": "region-talk-supervised-001",
    }

    first = await service.invoke("region_talk.pipeline.run", arguments)
    replay = await service.invoke("region_talk.pipeline.run", arguments)

    assert first["operation_id"] == replay["operation_id"] == "region-talk-run-01"
    assert first["duplicate"] is False
    assert replay["duplicate"] is True
    assert first["publication_dispatch"] is False
    assert controller.requests[0][0].mode == "supervised"
    assert controller.requests[0][0].project_slug == "region-talk"
    assert resolver.resolves == 0
    assert resolver.ensures == []

    with pytest.raises(ValidationError):
        await service.invoke("region_talk.pipeline.run", {**arguments, "publication_dispatch": True})
    with pytest.raises(HubPermissionError, match="not enabled"):
        await HubService(
            resolver,
            fallback_identity=owner,
        ).invoke("region_talk.pipeline.run", arguments)


@pytest.mark.asyncio
async def test_pipeline_run_is_advertised_only_with_enabled_controller() -> None:
    owner = identity("region-talk:operate")
    hidden = create_server(
        settings(),
        dependencies=MCPDependencies(region_talk_pipeline_run_enabled=True),
        default_identity=owner,
    )
    disabled = create_server(
        settings(),
        dependencies=MCPDependencies(region_talk_controller=Controller()),
        default_identity=owner,
    )
    enabled = create_server(
        settings(),
        dependencies=MCPDependencies(
            region_talk_controller=Controller(),
            region_talk_pipeline_run_enabled=True,
        ),
        default_identity=owner,
    )

    assert "region_talk.pipeline.run" not in {tool.name for tool in await hidden.list_tools()}
    assert "region_talk.pipeline.run" not in {tool.name for tool in await disabled.list_tools()}
    assert "region_talk.pipeline.run" in {tool.name for tool in await enabled.list_tools()}


def test_postgres_broker_uses_fixed_reader_facade_and_opaque_cursor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, Any] = {}

    class RegionTalkReader:
        @staticmethod
        def list_articles(cursor, request):  # type: ignore[no-untyped-def]
            observed.update(cursor=cursor, request=request)
            return [{"item_id": "11111111-1111-4111-8111-111111111111", "title": "One"}]

    fake_module = ModuleType("my_data_hub.workloads.region_talk.reader")
    fake_module.RegionTalkReader = RegionTalkReader  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, fake_module.__name__, fake_module)
    principal = identity("region-talk:read")
    request = SessionRequest(
        principal=principal,
        master_instance_id="22222222-2222-4222-8222-222222222222",
        epoch=7,
        role="reader",
        tool="region_talk.articles.list",
        limits=ExecutionLimits(max_rows=25, max_bytes=65_536),
    )
    credential = EpochDatabaseCredential(
        master_instance_id=request.master_instance_id,
        epoch=7,
        role="reader",
        database_url=(
            "postgresql://reader:password@postgres-master.internal:15432/postgres"
            "?sslmode=verify-full&sslrootcert=/state/master-tls/ca.pem&connect_timeout=5"
        ),
        expires_at=datetime.now(UTC) + timedelta(minutes=3),
    )
    session = PostgresMasterSession(request, credential)
    cursor = object()

    result = session._dispatch(
        cursor,
        {"cursor": "v1:25", "limit": 25, "status": "accepted", "max_bytes": 65_536},
    )

    assert observed == {
        "cursor": cursor,
        "request": {"limit": 25, "status": "accepted", "offset": 25},
    }
    assert result == {
        "items": [
            {"item_id": "11111111-1111-4111-8111-111111111111", "title": "One"}
        ],
        "next_cursor": None,
        "complete": True,
    }


def test_postgres_broker_queue_tools_use_canonical_publication_reader(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, Any]] = []

    class RegionTalkReader:
        @staticmethod
        def list_publication_queue(cursor, request):  # type: ignore[no-untyped-def]
            calls.append(("list", request))
            return {"items": [{"candidate_id": "11111111-1111-4111-8111-111111111111"}]}

        @staticmethod
        def publication_queue_summary(cursor):  # type: ignore[no-untyped-def]
            calls.append(("summary", cursor))
            return {"items": [{"candidate_status": "approved", "item_count": 1}], "total_items": 1}

    fake_module = ModuleType("my_data_hub.workloads.region_talk.reader")
    fake_module.RegionTalkReader = RegionTalkReader  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, fake_module.__name__, fake_module)
    principal = identity("region-talk:read")
    credential = EpochDatabaseCredential(
        master_instance_id="22222222-2222-4222-8222-222222222222",
        epoch=7,
        role="reader",
        database_url=(
            "postgresql://reader:password@postgres-master.internal:15432/postgres"
            "?sslmode=verify-full&sslrootcert=/state/master-tls/ca.pem&connect_timeout=5"
        ),
        expires_at=datetime.now(UTC) + timedelta(minutes=3),
    )
    list_request = SessionRequest(
        principal=principal, master_instance_id=credential.master_instance_id,
        epoch=7, role="reader", tool="region_talk.queue.list",
        limits=ExecutionLimits(max_rows=25, max_bytes=65_536),
    )
    summary_request = SessionRequest(
        principal=principal, master_instance_id=credential.master_instance_id,
        epoch=7, role="reader", tool="region_talk.queue.summary",
        limits=ExecutionLimits(max_rows=25, max_bytes=65_536),
    )
    cursor = object()
    page = PostgresMasterSession(list_request, credential)._dispatch(
        cursor, {"limit": 25, "status": "approved", "channel": "region-talk", "max_bytes": 65_536}
    )
    summary = PostgresMasterSession(summary_request, credential)._dispatch(cursor, {})
    assert calls == [
        ("list", {"limit": 25, "status": "approved", "channel": "region-talk", "offset": 0}),
        ("summary", cursor),
    ]
    assert page["items"][0]["candidate_id"].startswith("11111111")
    assert summary["total_items"] == 1
