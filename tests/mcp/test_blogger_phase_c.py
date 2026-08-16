from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from jsonschema import Draft202012Validator, ValidationError

from my_data_hub.control_plane.adapters import LedgerControlReader, LedgerWriteGate
from my_data_hub.control_plane.app import ControlPlaneSettings, create_app
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
from my_data_hub.workloads.bloggers.discovery import (
    SubmitDiscoveryBatch,
    validate_submit_discovery_batch,
)


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


def test_ingress_uses_authoritative_schema_before_semantic_model() -> None:
    payload = discovery_payload()
    payload["rows"][0]["source_uri"] = "https://exa mple.com/bloggers/1"
    # This proves why the generated Pydantic schema is not the authoritative
    # structural contract: it has no URI format/pattern for source_uri.
    assert list(Draft202012Validator(SubmitDiscoveryBatch.model_json_schema()).iter_errors(payload)) == []
    with pytest.raises(ValidationError):
        validate_submit_discovery_batch(payload)


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


def _verified_checkpoint(
    ledger: ControlLedger,
    *,
    master_id: str = "11111111-1111-4111-8111-111111111111",
    epoch: int = 7,
    revision: int = 12,
) -> str:
    operation_id = str(uuid4())
    ledger.ensure_operation(
        operation_id=operation_id,
        idempotency_key="phase-c-checkpoint",
        operation_kind="ensure_master",
        intent={"test": True},
        initial_state="READY",
        identity={"master_instance_id": master_id, "epoch": epoch},
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
        epoch=epoch,
        manifest_payload={"canonical_revision": revision},
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


def _verified_post_change_checkpoint(
    ledger: ControlLedger,
    *,
    master_id: str = "11111111-1111-4111-8111-111111111111",
    epoch: int = 7,
    revision: int = 13,
) -> str:
    head = ledger.checkpoint_head("postgres-master")
    assert head is not None and head.current_checkpoint_id is not None
    operation_id = str(uuid4())
    ledger.ensure_operation(
        operation_id=operation_id,
        idempotency_key="phase-c-post-change-checkpoint",
        operation_kind="ensure_master",
        intent={"source": "canonical_outbox:verified_checkpoint_required"},
        initial_state="READY",
        identity={
            "master_instance_id": master_id,
            "epoch": epoch,
        },
    )
    checkpoint_id = str(uuid4())
    ledger.add_checkpoint_candidate(
        checkpoint_id=checkpoint_id,
        operation_id=operation_id,
        dataset_ref="owner/checkpoints",
        version_ref=None,
        manifest_sha256="e" * 64,
        source_checkpoint_id=head.current_checkpoint_id,
        source_head_generation=head.generation,
        master_instance_id=master_id,
        epoch=epoch,
        manifest_payload={"canonical_revision": revision},
    )
    ledger.mark_checkpoint_uploaded(
        checkpoint_id, f"owner/checkpoints:{head.generation + 1}"
    )
    ledger.mark_checkpoint_readback_verified(checkpoint_id)
    ledger.mark_checkpoint_restore_verified(checkpoint_id)
    ledger.promote_checkpoint(
        "postgres-master",
        checkpoint_id,
        expected_generation=head.generation,
        expected_parent_checkpoint_id=head.current_checkpoint_id,
    )
    return checkpoint_id


def _activate_master(
    ledger: ControlLedger, *, master_id: str, revision: int
) -> dict[str, str | int]:
    operation_id = str(uuid4())
    run_id = str(uuid4())
    attempt_id = str(uuid4())
    service_instance_id = str(uuid4())
    identity = {
        "run_id": run_id,
        "attempt_id": attempt_id,
        "service_instance_id": service_instance_id,
        "master_instance_id": master_id,
        "epoch": 1,
    }
    ledger.ensure_operation(
        operation_id=operation_id,
        idempotency_key=f"active-master-{operation_id}",
        operation_kind="ensure_master",
        intent={"test": True},
        initial_state="READY",
        identity=identity,
        allocate_epoch_for="postgres-master",
    )
    stored = ledger.get_operation(operation_id)
    assert stored is not None
    identity["epoch"] = int(stored.identity["epoch"])
    ledger.record_attempt(
        attempt_id=attempt_id,
        run_id=run_id,
        operation_id=operation_id,
        source_identity="owner/master",
        source_version="git:" + "a" * 40,
        service_instance_id=service_instance_id,
        master_instance_id=master_id,
        epoch=int(identity["epoch"]),
        state="RUNNING",
    )
    ledger.activate_service_operation(
        operation_id=operation_id,
        expected_state="READY",
        service_instance_id=service_instance_id,
        service_kind="postgres-master",
        run_id=run_id,
        attempt_id=attempt_id,
        master_instance_id=master_id,
        epoch=int(identity["epoch"]),
        endpoint="tunnel://127.0.0.1:25432",
        protocol="postgresql+tls",
        tls_fingerprint="sha256:" + "b" * 64,
        capabilities=("sql",),
        canonical_revision=revision,
        schema_version="20",
        lease_until=datetime.now(UTC) + timedelta(minutes=10),
        latest_event_id="event-ready",
    )
    return {**identity, "operation_id": operation_id}


def _promote_checkpoint_for_active_operation(
    ledger: ControlLedger,
    *,
    active: dict[str, str | int],
    revision: int,
) -> str:
    head = ledger.checkpoint_head("postgres-master")
    assert head is not None and head.current_checkpoint_id is not None
    checkpoint_id = str(uuid4())
    ledger.add_checkpoint_candidate(
        checkpoint_id=checkpoint_id,
        operation_id=str(active["operation_id"]),
        dataset_ref="owner/checkpoints",
        version_ref=None,
        manifest_sha256="f" * 64,
        source_checkpoint_id=head.current_checkpoint_id,
        source_head_generation=head.generation,
        master_instance_id=str(active["master_instance_id"]),
        epoch=int(active["epoch"]),
        manifest_payload={"canonical_revision": revision},
    )
    ledger.mark_checkpoint_uploaded(
        checkpoint_id, f"owner/checkpoints:{head.generation + 1}"
    )
    ledger.mark_checkpoint_readback_verified(checkpoint_id)
    ledger.mark_checkpoint_restore_verified(checkpoint_id)
    ledger.promote_checkpoint(
        "postgres-master",
        checkpoint_id,
        expected_generation=head.generation,
        expected_parent_checkpoint_id=head.current_checkpoint_id,
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
    now = [2_000_000_000]
    gate = LedgerWriteGate(ledger, signing_secret=b"s" * 32, clock=lambda: now[0])
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
        clock=lambda: now[0],
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

    request_count = len(broker.requests)
    replay = await service.invoke(
        "bloggers.import.apply",
        {**arguments, "preview_receipt": preview["preview_receipt"]},
    )
    assert replay["status"] == "COMMITTED_PENDING_CHECKPOINT"
    assert replay["duplicate"] is True
    assert replay["canonical_revision"] == 13
    assert len(broker.requests) == request_count

    now[0] += 301
    expired_replay = await service.invoke(
        "bloggers.import.apply",
        {**arguments, "preview_receipt": preview["preview_receipt"]},
    )
    assert expired_replay["status"] == "COMMITTED_PENDING_CHECKPOINT"
    assert expired_replay["duplicate"] is True
    assert len(broker.requests) == request_count

    with pytest.raises(PermissionError):
        await service.invoke(
            "bloggers.import.apply",
            {
                **arguments,
                "idempotency_key": "conflicting-import-replay",
                "preview_receipt": preview["preview_receipt"],
            },
        )
    assert len(broker.requests) == request_count
    forged_receipt = preview["preview_receipt"][:-1] + (
        "A" if preview["preview_receipt"][-1] != "A" else "B"
    )
    with pytest.raises(PermissionError):
        await service.invoke(
            "bloggers.import.apply",
            {**arguments, "preview_receipt": forged_receipt},
        )
    assert len(broker.requests) == request_count

    ledger.advance_blogger_import_checkpoint(
        applied["operation_id"], state="CHECKPOINTING"
    )
    checkpointing = await service.invoke(
        "bloggers.import.apply",
        {**arguments, "preview_receipt": preview["preview_receipt"]},
    )
    assert checkpointing["status"] == "CHECKPOINTING"
    assert len(broker.requests) == request_count


@pytest.mark.asyncio
async def test_status_does_not_fake_checkpointing_without_verified_head(
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
        control=LedgerControlReader(ledger, write_gate=gate),
        write_gate=gate,
        fallback_identity=owner(),
        clock=lambda: 2_000_000_000,
    )
    arguments = {
        "batch_id": "11111111-1111-4111-8111-111111111111",
        "expected_revision": 12,
        "idempotency_key": "blogger-import-status-001",
    }
    preview = await service.invoke("bloggers.import.preview", arguments)

    async def apply_execute(arguments):  # type: ignore[no-untyped-def]
        return {
            "found": True,
            "operation_id": arguments["operation_id"],
            "batch_id": arguments["batch_id"],
            "plan_sha256": arguments["plan_sha256"],
            "affected_rows": 2,
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
    head_before = ledger.checkpoint_head("postgres-master")
    status = await service.invoke(
        "bloggers.import.status", {"operation_id": applied["operation_id"]}
    )
    head_after = ledger.checkpoint_head("postgres-master")
    assert status["state"] == "COMMITTED_PENDING_CHECKPOINT"
    assert status["checkpoint_request_state"] == "NOT_REQUESTED"
    assert head_before == head_after

    unrelated_checkpoint_id = _verified_post_change_checkpoint(ledger)
    still_pending = await service.invoke(
        "bloggers.import.status", {"operation_id": applied["operation_id"]}
    )
    assert still_pending["state"] == "COMMITTED_PENDING_CHECKPOINT"
    assert still_pending["post_change_checkpoint_id"] is None
    assert ledger.checkpoint_head("postgres-master").current_checkpoint_id == unrelated_checkpoint_id  # type: ignore[union-attr]


@pytest.mark.asyncio
async def test_operator_runtime_requests_and_observes_bound_checkpoint_without_connector_intake(
    tmp_path: Path,
) -> None:
    ledger = ControlLedger(tmp_path / "control.sqlite3")
    master_id = "11111111-1111-4111-8111-111111111111"
    pre_checkpoint_id = _verified_checkpoint(
        ledger, master_id=master_id, epoch=1, revision=12
    )
    active = _activate_master(ledger, master_id=master_id, revision=12)
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
            "master_epoch": 1,
            "canonical_revision": 12,
        }
    )
    service = HubService(
        Resolver(
            MasterSnapshot(
                MasterState.ACTIVE,
                instance_id=master_id,
                epoch=1,
                canonical_revision=12,
            )
        ),
        broker=broker,
        control=LedgerControlReader(ledger, write_gate=gate),
        write_gate=gate,
        fallback_identity=owner(),
        clock=lambda: 2_000_000_000,
    )
    arguments = {
        "batch_id": "11111111-1111-4111-8111-111111111111",
        "expected_revision": 12,
        "idempotency_key": "blogger-import-production-checkpoint",
    }
    preview = await service.invoke("bloggers.import.preview", arguments)

    async def apply_execute(arguments):  # type: ignore[no-untyped-def]
        return {
            "found": True,
            "operation_id": arguments["operation_id"],
            "batch_id": arguments["batch_id"],
            "plan_sha256": arguments["plan_sha256"],
            "affected_rows": 2,
            "committed_revision": 13,
            "duplicate": False,
            "master_epoch": 1,
            "canonical_revision": 13,
        }

    broker.session.execute = apply_execute  # type: ignore[method-assign]
    applied = await service.invoke(
        "bloggers.import.apply",
        {**arguments, "preview_receipt": preview["preview_receipt"]},
    )
    assert applied["status"] == "COMMITTED_PENDING_CHECKPOINT"
    assert applied["checkpoint_request_state"] == "REQUESTED"
    request_id = applied["checkpoint_request_id"]
    request = ledger.connector_checkpoint_request(f"connector-checkpoint:{request_id}")
    assert request is not None
    assert request["master_operation_id"] == active["operation_id"]
    assert request["master_instance_id"] == master_id
    assert request["epoch"] == 1
    assert request["canonical_revision"] == 13

    token = "r" * 64
    ledger.store_runtime_token_hash(str(active["run_id"]), str(active["attempt_id"]), token)
    app = create_app(
        ControlPlaneSettings(
            ledger_path=ledger.path,
            operator_credentials_enabled=True,
            connector_runtime_enabled=False,
        ),
        ledger=ledger,
    )
    claim = TestClient(app).get(
        f"/internal/runtime/connector-checkpoint/{active['run_id']}/{active['attempt_id']}",
        headers={
            "Authorization": f"Bearer {token}",
            "X-MDH-Master-Instance-ID": master_id,
            "X-MDH-Epoch": "1",
        },
    )
    assert claim.status_code == 200
    assert claim.json() == {
        "available": True,
        "operation_id": f"connector-checkpoint:{request_id}",
        "canonical_revision": 13,
    }
    checkpointing = await service.invoke(
        "bloggers.import.status", {"operation_id": applied["operation_id"]}
    )
    assert checkpointing["state"] == "CHECKPOINTING"
    assert checkpointing["checkpoint_request_state"] == "CHECKPOINTING"
    assert ledger.checkpoint_head("postgres-master").current_checkpoint_id == pre_checkpoint_id  # type: ignore[union-attr]

    unrelated_checkpoint_id = _verified_post_change_checkpoint(
        ledger, master_id=master_id, epoch=1, revision=13
    )
    unrelated_status = await service.invoke(
        "bloggers.import.status", {"operation_id": applied["operation_id"]}
    )
    assert unrelated_status["state"] == "CHECKPOINTING"
    assert unrelated_status["post_change_checkpoint_id"] is None
    assert ledger.checkpoint_head("postgres-master").current_checkpoint_id == unrelated_checkpoint_id  # type: ignore[union-attr]

    post_checkpoint_id = _promote_checkpoint_for_active_operation(
        ledger, active=active, revision=13
    )
    request_operation_id = f"connector-checkpoint:{request_id}"
    assert gate.blogger_checkpoint_coordinator.checkpoint_status(request_operation_id)[
        "state"
    ] == "DURABLE_COMPLETE"
    ledger.advance_blogger_import_checkpoint(
        applied["operation_id"],
        state="CHECKPOINT_VERIFIED",
        post_change_checkpoint_id=post_checkpoint_id,
    )
    broker_count = len(broker.requests)
    verified_replay = await service.invoke(
        "bloggers.import.apply",
        {**arguments, "preview_receipt": preview["preview_receipt"]},
    )
    assert verified_replay["status"] == "CHECKPOINT_VERIFIED"
    assert len(broker.requests) == broker_count
    ledger.advance_blogger_import_checkpoint(
        applied["operation_id"],
        state="DURABLE_COMPLETE",
        post_change_checkpoint_id=post_checkpoint_id,
    )
    durable_replay = await service.invoke(
        "bloggers.import.apply",
        {**arguments, "preview_receipt": preview["preview_receipt"]},
    )
    assert durable_replay["status"] == "DURABLE_COMPLETE"
    assert durable_replay["post_change_checkpoint_id"] == post_checkpoint_id
    assert durable_replay["checkpoint_request_state"] == "DURABLE_COMPLETE"
    assert len(broker.requests) == broker_count


@pytest.mark.asyncio
async def test_active_new_epoch_reconciles_exact_old_epoch_receipt(tmp_path: Path) -> None:
    ledger = ControlLedger(tmp_path / "control.sqlite3")
    old_master_id = "11111111-1111-4111-8111-111111111111"
    new_master_id = "22222222-2222-4222-8222-222222222222"
    _verified_checkpoint(ledger, master_id=old_master_id, epoch=1, revision=12)
    active_a = _activate_master(ledger, master_id=old_master_id, revision=12)
    assert active_a["epoch"] == 1
    gate = LedgerWriteGate(ledger, signing_secret=b"s" * 32, clock=lambda: 2_000_000_000)
    old_master = MasterSnapshot(
        MasterState.ACTIVE,
        instance_id=old_master_id,
        epoch=1,
        canonical_revision=12,
    )
    preview_broker = Broker(
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
            "master_epoch": 1,
            "canonical_revision": 12,
        }
    )
    arguments = {
        "batch_id": "11111111-1111-4111-8111-111111111111",
        "expected_revision": 12,
        "idempotency_key": "blogger-import-epoch-reconcile",
    }
    preview_service = HubService(
        Resolver(old_master),
        broker=preview_broker,
        write_gate=gate,
        fallback_identity=owner(),
        clock=lambda: 2_000_000_000,
    )
    preview = await preview_service.invoke("bloggers.import.preview", arguments)
    gate.authorize_write(
        principal=owner(),
        tool="bloggers.import.apply",
        arguments={**arguments, "preview_receipt": preview["preview_receipt"]},
        master=old_master,
    )
    operation_id = preview["operation_id"]
    assert ledger.blogger_import_operation(operation_id)["state"] == "APPLYING"  # type: ignore[index]

    ledger.transition_operation(
        str(active_a["operation_id"]),
        expected_state="ACTIVE",
        new_state="FENCED",
        metadata={"reason": "test-successor"},
    )
    active_b = _activate_master(ledger, master_id=new_master_id, revision=13)
    assert active_b["epoch"] == 2
    new_master = MasterSnapshot(
        MasterState.ACTIVE,
        instance_id=new_master_id,
        epoch=2,
        canonical_revision=13,
    )
    reconcile_broker = Broker(
        {
            "found": True,
            "operation_id": operation_id,
            "batch_id": arguments["batch_id"],
            "request_sha256": ledger.blogger_import_operation(operation_id)["request_sha256"],  # type: ignore[index]
            "plan_sha256": "d" * 64,
            "affected_rows": 2,
            "committed_revision": 13,
            "committed_at": "2026-08-16T12:00:00Z",
            "duplicate": True,
            "receipt_master_instance_id": old_master.instance_id,
            "receipt_master_epoch": old_master.epoch,
            "expected_revision": 12,
            "principal_id": owner().subject,
            "client_id": owner().client_id,
            "master_epoch": 2,
            "canonical_revision": 13,
        }
    )
    service = HubService(
        Resolver(new_master),
        broker=reconcile_broker,
        control=LedgerControlReader(ledger, write_gate=gate),
        write_gate=gate,
        fallback_identity=owner(),
        clock=lambda: 2_000_000_000,
    )
    status = await service.invoke(
        "bloggers.import.status", {"operation_id": operation_id}
    )
    assert status["state"] == "COMMITTED_PENDING_CHECKPOINT"
    assert status["checkpoint_request_state"] == "REQUESTED"
    request_id = status["checkpoint_request_id"]
    request = ledger.connector_checkpoint_request(f"connector-checkpoint:{request_id}")
    assert request is not None
    assert request["master_operation_id"] == active_b["operation_id"]
    assert request["master_instance_id"] == new_master_id
    assert request["epoch"] == 2
    assert ledger.blogger_import_operation(operation_id)["epoch"] == 1  # type: ignore[index]
    assert reconcile_broker.requests[0].epoch == 2
    assert reconcile_broker.requests[0].role == "canonical_committer"

    token = "s" * 64
    ledger.store_runtime_token_hash(str(active_b["run_id"]), str(active_b["attempt_id"]), token)
    app = create_app(
        ControlPlaneSettings(
            ledger_path=ledger.path,
            operator_credentials_enabled=True,
            connector_runtime_enabled=False,
        ),
        ledger=ledger,
    )
    claim = TestClient(app).get(
        f"/internal/runtime/connector-checkpoint/{active_b['run_id']}/{active_b['attempt_id']}",
        headers={
            "Authorization": f"Bearer {token}",
            "X-MDH-Master-Instance-ID": new_master_id,
            "X-MDH-Epoch": "2",
        },
    )
    assert claim.status_code == 200
    assert claim.json()["operation_id"] == f"connector-checkpoint:{request_id}"
    checkpointing = await service.invoke(
        "bloggers.import.status", {"operation_id": operation_id}
    )
    assert checkpointing["state"] == "CHECKPOINTING"

    post_checkpoint_id = _promote_checkpoint_for_active_operation(
        ledger, active=active_b, revision=13
    )
    durable = await service.invoke(
        "bloggers.import.status", {"operation_id": operation_id}
    )
    assert durable["state"] == "DURABLE_COMPLETE"
    assert durable["post_change_checkpoint_id"] == post_checkpoint_id
    assert ledger.blogger_import_operation(operation_id)["epoch"] == 1  # type: ignore[index]
