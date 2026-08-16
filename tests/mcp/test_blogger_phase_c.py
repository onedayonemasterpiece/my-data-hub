from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from my_data_hub.control_plane.adapters import LedgerWriteGate
from my_data_hub.control_plane.ledger import ControlLedger
from my_data_hub.mcp.catalog import TOOL_CONTRACTS
from my_data_hub.mcp.contracts import (
    EnsureMasterReceipt,
    MasterSnapshot,
    MasterState,
    SessionRequest,
)
from my_data_hub.mcp.oauth import AccessIdentity
from my_data_hub.mcp.server import (
    PROVIDER_ONLY_TOOLS,
    READER_PROFILE_TOOLS,
    UNIFIED_BOOTSTRAP_TOOLS,
)
from my_data_hub.mcp.service import HubService


def owner() -> AccessIdentity:
    return AccessIdentity(
        subject="owner:one",
        client_id="opencode-blogger-operator",
        scopes=frozenset({"bloggers:read", "bloggers:write"}),
        audience="https://mcp.example/mcp",
        token_id="token-1",
        expires_at=2_000_000_300,
        issuer="https://identity.example",
        issued_at=1_999_999_990,
        resource="https://mcp.example/mcp",
    )


class Resolver:
    def __init__(self, snapshot: MasterSnapshot) -> None:
        self.snapshot = snapshot
        self.ensure_intents: list[str] = []

    async def resolve_master(self, _principal: AccessIdentity) -> MasterSnapshot:
        return self.snapshot

    async def ensure_master(
        self, _principal: AccessIdentity, *, intent: str
    ) -> EnsureMasterReceipt:
        self.ensure_intents.append(intent)
        return EnsureMasterReceipt("master-cold-operation", MasterState.REQUESTED, False, intent)


class Session:
    def __init__(self) -> None:
        self.arguments: dict[str, Any] | None = None

    async def execute(self, arguments):  # type: ignore[no-untyped-def]
        self.arguments = dict(arguments)
        payload = arguments["payload"]
        return {
            "batch_id": payload["batch_id"],
            "request_sha256": "a" * 64,
            "disposition": "accepted",
            "master_epoch": 7,
            "canonical_revision": 12,
        }

    async def close(self) -> None:
        return None


class Broker:
    def __init__(self, result: dict[str, Any] | None = None) -> None:
        self.requests: list[SessionRequest] = []
        self.session = Session()
        if result is not None:
            async def execute(arguments):  # type: ignore[no-untyped-def]
                self.session.arguments = dict(arguments)
                return result

            self.session.execute = execute  # type: ignore[method-assign]

    async def issue_session(self, request: SessionRequest) -> Session:
        self.requests.append(request)
        return self.session


def discovery_payload() -> dict[str, Any]:
    return {
        "contract_version": "submit-discovery-batch.v1",
        "batch_id": "11111111-1111-4111-8111-111111111111",
        "idempotency_key": "blogger-batch-001",
        "project_slug": "region-talk",
        "produced_at": "2026-08-16T12:00:00Z",
        "observed_period": {
            "start": "2026-08-16T11:00:00Z",
            "end": "2026-08-16T12:00:00Z",
            "timezone": "UTC",
        },
        "rows": [
            {
                "source_record_id": "source-1",
                "actor_kind": "person",
                "display_name": "Example Blogger",
                "accounts": [{"platform": "telegram", "handle": "example"}],
                "source_uri": "https://example.com/bloggers/1",
                "observed_at": "2026-08-16T11:30:00Z",
                "evidence": {"source": "owner research"},
            }
        ],
    }


def test_reader_and_unified_catalogs_are_typed_and_provider_catalog_is_unchanged() -> None:
    assert "data.query" not in READER_PROFILE_TOOLS
    assert "data.query" not in UNIFIED_BOOTSTRAP_TOOLS
    assert "submit_discovery_batch" not in UNIFIED_BOOTSTRAP_TOOLS
    assert PROVIDER_ONLY_TOOLS <= UNIFIED_BOOTSTRAP_TOOLS
    assert TOOL_CONTRACTS["submit_discovery_batch"].role == "connector"
    assert TOOL_CONTRACTS["bloggers.import.apply"].role == "canonical_committer"
    assert TOOL_CONTRACTS["bloggers.import.status"].scope == "bloggers:write"


@pytest.mark.asyncio
async def test_submit_discovery_batch_uses_active_connector_session_without_sql() -> None:
    resolver = Resolver(
        MasterSnapshot(
            MasterState.ACTIVE,
            instance_id="11111111-1111-4111-8111-111111111111",
            epoch=7,
            canonical_revision=12,
        )
    )
    broker = Broker()
    service = HubService(resolver, broker=broker, fallback_identity=owner())

    result = await service.invoke("submit_discovery_batch", {"payload": discovery_payload()})

    assert result["disposition"] == "accepted"
    assert broker.requests[0].role == "connector"
    assert broker.requests[0].tool == "submit_discovery_batch"
    assert "sql" not in broker.session.arguments
    assert resolver.ensure_intents == []


@pytest.mark.asyncio
async def test_submit_discovery_batch_rejects_semantic_collision_before_broker() -> None:
    payload = discovery_payload()
    duplicate = dict(payload["rows"][0])
    duplicate["source_record_id"] = "source-2"
    payload["rows"].append(duplicate)
    broker = Broker()
    service = HubService(
        Resolver(
            MasterSnapshot(
                MasterState.ACTIVE,
                instance_id="11111111-1111-4111-8111-111111111111",
                epoch=7,
                canonical_revision=12,
            )
        ),
        broker=broker,
        fallback_identity=owner(),
    )

    with pytest.raises(Exception, match="account identity"):
        await service.invoke("submit_discovery_batch", {"payload": payload})
    assert broker.requests == []


@pytest.mark.asyncio
async def test_blogger_preview_persists_metadata_before_cold_master_ensure(
    tmp_path: Path,
) -> None:
    ledger = ControlLedger(tmp_path / "control.sqlite3")
    gate = LedgerWriteGate(ledger, signing_secret=b"s" * 32, clock=lambda: 2_000_000_000)
    resolver = Resolver(MasterSnapshot(MasterState.ABSENT))
    service = HubService(
        resolver,
        write_gate=gate,
        fallback_identity=owner(),
        clock=lambda: 2_000_000_000,
    )

    result = await service.invoke(
        "bloggers.import.preview",
        {
            "batch_id": "11111111-1111-4111-8111-111111111111",
            "expected_revision": 12,
            "idempotency_key": "blogger-import-001",
        },
    )

    assert result["status"] == "WAITING_MASTER"
    assert result["outcome"] == "WAITING_FOR_MASTER"
    assert result["master_operation_id"] == "master-cold-operation"
    assert result["continuation"]["status_tool"] == "bloggers.import.status"
    stored = ledger.blogger_import_operation(result["operation_id"])
    assert stored is not None
    assert stored["state"] == "WAITING_MASTER"
    assert stored["master_instance_id"] is None
    assert stored["preview_summary_json"] is None
    assert resolver.ensure_intents == ["mcp-write:bloggers.import.preview"]


def test_discovery_contract_uses_timezone_aware_wire_values() -> None:
    assert datetime.fromisoformat(discovery_payload()["produced_at"].replace("Z", "+00:00")).tzinfo is UTC


def _verified_checkpoint(ledger: ControlLedger) -> str:
    operation_id = str(uuid4())
    master_id = "11111111-1111-4111-8111-111111111111"
    ledger.ensure_operation(
        operation_id=operation_id,
        idempotency_key="phase-c-checkpoint",
        operation_kind="ensure_master",
        intent={"test": True},
        initial_state="READY",
        identity={"master_instance_id": master_id, "epoch": 7},
    )
    checkpoint_id = str(uuid4())
    ledger.add_checkpoint_candidate(
        checkpoint_id=checkpoint_id,
        operation_id=operation_id,
        dataset_ref="owner/checkpoints",
        version_ref=None,
        manifest_sha256="c" * 64,
        source_checkpoint_id=None,
        master_instance_id=master_id,
        epoch=7,
        manifest_payload={"canonical_revision": 12},
    )
    ledger.mark_checkpoint_uploaded(checkpoint_id, "owner/checkpoints:1")
    ledger.mark_checkpoint_readback_verified(checkpoint_id)
    ledger.mark_checkpoint_restore_verified(checkpoint_id)
    ledger.promote_checkpoint(
        "postgres-master",
        checkpoint_id,
        expected_generation=0,
        expected_parent_checkpoint_id=None,
    )
    return checkpoint_id


@pytest.mark.asyncio
async def test_active_blogger_preview_is_fixed_canonical_committer_call(
    tmp_path: Path,
) -> None:
    ledger = ControlLedger(tmp_path / "control.sqlite3")
    checkpoint_id = _verified_checkpoint(ledger)
    gate = LedgerWriteGate(ledger, signing_secret=b"s" * 32, clock=lambda: 2_000_000_000)
    broker = Broker(
        {
            "operation_id": "unused-by-gate",
            "batch_id": "11111111-1111-4111-8111-111111111111",
            "request_sha256": "b" * 64,
            "plan_sha256": "d" * 64,
            "summary": {
                "create_actor_count": 1,
                "link_existing_count": 0,
                "quarantine_count": 0,
                "account_count": 1,
            },
            "master_epoch": 7,
            "canonical_revision": 12,
        }
    )
    service = HubService(
        Resolver(
            MasterSnapshot(
                MasterState.ACTIVE,
                instance_id="11111111-1111-4111-8111-111111111111",
                epoch=7,
                canonical_revision=12,
            )
        ),
        broker=broker,
        write_gate=gate,
        fallback_identity=owner(),
        clock=lambda: 2_000_000_000,
    )

    result = await service.invoke(
        "bloggers.import.preview",
        {
            "batch_id": "11111111-1111-4111-8111-111111111111",
            "expected_revision": 12,
            "idempotency_key": "blogger-import-active-001",
        },
    )

    assert result["status"] == "PREVIEWED"
    assert result["preview_receipt"]
    assert result["pre_change_checkpoint_id"] == checkpoint_id
    assert broker.requests[0].role == "canonical_committer"
    assert broker.requests[0].tool == "bloggers.import.preview"
    assert set(broker.session.arguments or {}) == {
        "operation_id",
        "batch_id",
        "request_sha256",
        "expected_revision",
        "principal_id",
        "client_id",
        "_write_permit",
    }


@pytest.mark.asyncio
async def test_blogger_apply_uses_exact_preview_plan_and_enters_checkpoint_lifecycle(
    tmp_path: Path,
) -> None:
    ledger = ControlLedger(tmp_path / "control.sqlite3")
    _verified_checkpoint(ledger)
    gate = LedgerWriteGate(ledger, signing_secret=b"s" * 32, clock=lambda: 2_000_000_000)
    broker = Broker(
        {
            "operation_id": "ignored",
            "batch_id": "11111111-1111-4111-8111-111111111111",
            "request_sha256": "b" * 64,
            "plan_sha256": "d" * 64,
            "summary": {
                "create_actor_count": 1,
                "link_existing_count": 0,
                "quarantine_count": 0,
                "account_count": 1,
            },
            "master_epoch": 7,
            "canonical_revision": 12,
        }
    )
    service = HubService(
        Resolver(
            MasterSnapshot(
                MasterState.ACTIVE,
                instance_id="11111111-1111-4111-8111-111111111111",
                epoch=7,
                canonical_revision=12,
            )
        ),
        broker=broker,
        write_gate=gate,
        fallback_identity=owner(),
        clock=lambda: 2_000_000_000,
    )
    arguments = {
        "batch_id": "11111111-1111-4111-8111-111111111111",
        "expected_revision": 12,
        "idempotency_key": "blogger-import-apply-001",
    }
    preview = await service.invoke("bloggers.import.preview", arguments)

    async def apply_execute(arguments):  # type: ignore[no-untyped-def]
        broker.session.arguments = dict(arguments)
        return {
            "found": True,
            "operation_id": arguments["operation_id"],
            "batch_id": arguments["batch_id"],
            "plan_sha256": arguments["plan_sha256"],
            "affected_rows": 3,
            "committed_revision": 13,
            "duplicate": False,
            "master_epoch": 7,
            "canonical_revision": 13,
        }

    broker.session.execute = apply_execute  # type: ignore[method-assign]
    applied = await service.invoke(
        "bloggers.import.apply",
        {**arguments, "preview_receipt": preview["preview_receipt"]},
    )

    assert applied["status"] == "COMMITTED_PENDING_CHECKPOINT"
    assert applied["canonical_revision"] == 13
    assert broker.requests[-1].role == "canonical_committer"
    assert broker.requests[-1].tool == "bloggers.import.apply"
    assert broker.session.arguments is not None
    assert broker.session.arguments["plan_sha256"] == "d" * 64
    stored = ledger.blogger_import_operation(applied["operation_id"])
    assert stored is not None and stored["state"] == "COMMITTED_PENDING_CHECKPOINT"
