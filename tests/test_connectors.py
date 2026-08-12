from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from uuid import uuid5

import pytest

from my_data_hub.connectors.contracts import (
    ConnectorCheckpointRequest,
    ConnectorCheckpointState,
    ConnectorCheckpointStatusReceipt,
    ConnectorContractError,
    ConnectorDurabilityReceipt,
    ConnectorDurabilityState,
    ConnectorReceipt,
    DeliveryMode,
    ReceiptStatus,
    canonical_json_bytes,
    payload_sha256,
    validate_envelope_bytes,
)
from my_data_hub.connectors.durability import (
    ConnectorDurabilityConflict,
    ConnectorDurabilityService,
    ConnectorDurabilitySupervisor,
    build_connector_checkpoint_gateway,
    checkpoint_request_for,
)
from my_data_hub.connectors.interfaces import ConnectorRegistration, OrchestratorPullInterface
from my_data_hub.connectors.postgres import normalize_daily_counters
from my_data_hub.connectors.repository import (
    AcceptanceDisposition,
    AcceptanceSubmission,
    ExistingAcceptance,
    QuarantineEvidence,
    ReplayDisposition,
    RepositoryDecision,
    classify_replay,
)
from my_data_hub.connectors.runtime import (
    ActiveMasterConnectorDurabilityRuntime,
    ActiveMasterConnectorRuntime,
    ConnectorCapabilityBlocked,
    ConnectorDurabilitySessionRequest,
    ConnectorSessionRequest,
    connector_principal,
)
from my_data_hub.connectors.service import ConnectorAuthorizationError, ConnectorIntakeService
from my_data_hub.connectors.spool import (
    ConnectorDeliveryService,
    DeliveryDisposition,
    DeliveryResult,
    DurabilityDeliveryResult,
    DurabilityDisposition,
    DurableConnectorSpool,
    RetryPolicy,
    SpoolConflict,
)
from my_data_hub.connectors.synthetic import SYNTHETIC_NAMESPACE, SyntheticConnectorProducer
from my_data_hub.connectors.transport import HttpConnectorTransport
from my_data_hub.mcp.contracts import EnsureMasterReceipt, MasterSnapshot, MasterState

ROOT = Path(__file__).resolve().parents[1]
ACCEPTED_AT = datetime(2026, 8, 9, 12, tzinfo=UTC)


class MemoryAcceptanceRepository:
    """Transaction-shaped pure fake for core replay semantics."""

    def __init__(self) -> None:
        self.accepted: dict[tuple[str, str], ExistingAcceptance] = {}
        self.quarantines: list[QuarantineEvidence] = []

    def accept(self, submission: AcceptanceSubmission) -> RepositoryDecision:
        key = (submission.identity.connector_id, submission.identity.idempotency_key)
        existing = self.accepted.get(key)
        if existing is not None:
            disposition = classify_replay(existing, submission)
            if disposition is ReplayDisposition.EXACT_REPLAY:
                return RepositoryDecision(AcceptanceDisposition.REPLAYED, receipt=existing.receipt)
            quarantine = QuarantineEvidence(
                quarantine_id=uuid5(SYNTHETIC_NAMESPACE, f"quarantine:{submission.envelope_sha256}"),
                reason="conflicting_replay",
                identity=submission.identity,
                incoming_batch_id=submission.batch_id,
                existing_batch_id=existing.batch_id,
                incoming_payload_sha256=submission.payload_sha256,
                existing_payload_sha256=existing.payload_sha256,
                incoming_envelope_sha256=submission.envelope_sha256,
                existing_envelope_sha256=existing.envelope_sha256,
            )
            self.quarantines.append(quarantine)
            return RepositoryDecision(AcceptanceDisposition.QUARANTINED, quarantine=quarantine)

        receipt = ConnectorReceipt(
            receipt_id=uuid5(SYNTHETIC_NAMESPACE, f"receipt:{submission.envelope_sha256}"),
            status=ReceiptStatus.ACCEPTED,
            connector_id=submission.identity.connector_id,
            batch_id=submission.batch_id,
            idempotency_key=submission.identity.idempotency_key,
            payload_sha256=submission.payload_sha256,
            envelope_sha256=submission.envelope_sha256,
            accepted_at=ACCEPTED_AT,
        )
        self.accepted[key] = ExistingAcceptance(
            identity=submission.identity,
            batch_id=submission.batch_id,
            payload_sha256=submission.payload_sha256,
            envelope_sha256=submission.envelope_sha256,
            receipt=receipt,
        )
        return RepositoryDecision(AcceptanceDisposition.ACCEPTED, receipt=receipt)


def synthetic_bytes(*, sequence: int = 1, values: dict[str, int] | None = None) -> bytes:
    return SyntheticConnectorProducer().exact_bytes(
        date(2026, 8, 9),
        sequence=sequence,
        values=values,
    )


def test_documented_example_validates_payload_hash_and_count() -> None:
    raw = (ROOT / "examples/contracts/data-connector-envelope.v1.example.json").read_bytes()
    validated = validate_envelope_bytes(raw)

    assert validated.envelope.record_count == len(validated.envelope.inline_records or [])
    assert validated.envelope.payload_sha256 == payload_sha256(
        validated.envelope.inline_records or []
    )
    assert validated.exact_bytes == raw


def test_canonical_json_is_stable_and_uses_ecmascript_number_thresholds() -> None:
    first = {"z": 1e-7, "a": 1e20, "middle": -0.0}
    second = {"middle": -0.0, "a": 1e20, "z": 1e-7}

    assert canonical_json_bytes(first) == canonical_json_bytes(second)
    assert canonical_json_bytes(first) == b'{"a":100000000000000000000,"middle":0,"z":1e-7}'
    with pytest.raises(ConnectorContractError, match="IEEE-754"):
        canonical_json_bytes({"unsafe_integer": 2**53})


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda value: value.update({"unknown": True}), "extra"),
        (lambda value: value.update({"record_count": 2}), "record_count"),
        (lambda value: value.update({"payload_sha256": "0" * 64}), "payload_sha256"),
        (lambda value: value.update({"contract_version": "future.v2"}), "contract_version"),
    ],
)
def test_runtime_validation_rejects_unknown_fields_count_hash_and_version(
    mutation: object,
    message: str,
) -> None:
    value = json.loads(synthetic_bytes())
    mutation(value)  # type: ignore[operator]
    with pytest.raises(ConnectorContractError, match=message):
        validate_envelope_bytes(canonical_json_bytes(value))


def test_runtime_validation_rejects_duplicate_keys_and_body_limit() -> None:
    with pytest.raises(ConnectorContractError, match="duplicate JSON object key"):
        validate_envelope_bytes(b'{"contract_version":"x","contract_version":"y"}')
    with pytest.raises(ConnectorContractError, match="exceeds"):
        validate_envelope_bytes(synthetic_bytes(), max_envelope_bytes=10)


def test_exact_replay_returns_same_receipt_but_changed_content_is_quarantined() -> None:
    repository = MemoryAcceptanceRepository()
    service = ConnectorIntakeService(repository)
    exact = synthetic_bytes()

    accepted = service.submit(
        exact,
        authenticated_connector_id="synthetic.daily-statistics",
    )
    reordered = json.dumps(json.loads(exact), ensure_ascii=False, indent=2).encode()
    replayed = service.submit(
        reordered,
        authenticated_connector_id="synthetic.daily-statistics",
    )

    changed = json.loads(exact)
    changed["inline_records"][0]["counts"]["accepted"] = 99
    changed["payload_sha256"] = payload_sha256(changed["inline_records"])
    conflict = service.submit(
        canonical_json_bytes(changed),
        authenticated_connector_id="synthetic.daily-statistics",
    )

    assert accepted.disposition is AcceptanceDisposition.ACCEPTED
    assert replayed.disposition is AcceptanceDisposition.REPLAYED
    assert replayed.receipt is accepted.receipt
    assert conflict.disposition is AcceptanceDisposition.QUARANTINED
    assert conflict.quarantine is repository.quarantines[0]
    assert len(repository.accepted) == 1


def test_authenticated_principal_cannot_submit_for_another_connector() -> None:
    repository = MemoryAcceptanceRepository()
    service = ConnectorIntakeService(repository)

    with pytest.raises(ConnectorAuthorizationError, match="not bound"):
        service.submit(synthetic_bytes(), authenticated_connector_id="other.connector")
    assert repository.accepted == {}


class RecordingTransport:
    def __init__(self, *, fail: bool, durable: bool = False) -> None:
        self.fail = fail
        self.durable = durable
        self.submissions: list[bytes] = []

    def submit(self, exact_envelope_bytes: bytes) -> DeliveryResult:
        self.submissions.append(exact_envelope_bytes)
        if self.fail:
            raise TimeoutError("synthetic outage")
        validated = validate_envelope_bytes(exact_envelope_bytes)
        envelope = validated.envelope
        receipt = ConnectorReceipt(
            receipt_id=uuid5(SYNTHETIC_NAMESPACE, f"delivery:{validated.envelope_sha256}"),
            status=ReceiptStatus.ACCEPTED,
            connector_id=envelope.connector_id,
            batch_id=envelope.batch_id,
            idempotency_key=envelope.idempotency_key,
            payload_sha256=envelope.payload_sha256,
            envelope_sha256=validated.envelope_sha256,
            accepted_at=ACCEPTED_AT,
        )
        return DeliveryResult(DeliveryDisposition.ACCEPTED, receipt=receipt)

    def durability(self, acceptance: ConnectorReceipt) -> DurabilityDeliveryResult:
        state = (
            ConnectorDurabilityState.DURABLE_COMPLETE
            if self.durable
            else ConnectorDurabilityState.CANONICAL_COMMITTED
        )
        receipt = ConnectorDurabilityReceipt(
            state=state,
            acceptance=acceptance,
            canonical_revision=18,
            checkpoint_request_id="b" * 64 if self.durable else None,
            checkpoint_operation_id="checkpoint-operation" if self.durable else None,
            checkpoint_receipt_sha256="c" * 64 if self.durable else None,
            checkpoint_id="checkpoint-18" if self.durable else None,
            updated_at=ACCEPTED_AT,
        )
        return DurabilityDeliveryResult(
            DurabilityDisposition.COMPLETE if self.durable else DurabilityDisposition.PENDING,
            receipt=receipt,
        )


def test_outage_spool_survives_restart_and_eventually_delivers_exact_bytes(tmp_path: Path) -> None:
    exact = synthetic_bytes()
    queued_at = datetime(2026, 8, 9, 10, tzinfo=UTC)
    first_spool = DurableConnectorSpool(tmp_path / "spool")
    original = first_spool.enqueue(exact, queued_at=queued_at)
    unavailable = RecordingTransport(fail=True)

    first = ConnectorDeliveryService(first_spool, unavailable).deliver_ready(now=queued_at)
    assert first.deferred == 1
    assert original.envelope_path.exists()

    restarted_spool = DurableConnectorSpool(tmp_path / "spool")
    available = RecordingTransport(fail=False)
    second = ConnectorDeliveryService(restarted_spool, available).deliver_ready(
        now=queued_at + timedelta(seconds=2)
    )

    assert second.deferred == 1
    assert original.envelope_path.exists()
    assert unavailable.submissions == [exact]
    assert available.submissions == [exact]
    final = RecordingTransport(fail=False, durable=True)
    third = ConnectorDeliveryService(restarted_spool, final).deliver_ready(
        now=queued_at + timedelta(seconds=4)
    )
    assert third.delivered == 1
    assert final.submissions == []
    assert restarted_spool.pending(ready_at=queued_at + timedelta(seconds=4)) == []
    assert len(list(restarted_spool.receipts_dir.glob("*.json"))) == 1
    with pytest.raises(SpoolConflict, match="durable receipt"):
        restarted_spool.enqueue(exact)


def test_terminal_conflict_is_retained_in_local_quarantine(tmp_path: Path) -> None:
    class ConflictTransport:
        def submit(self, exact_envelope_bytes: bytes) -> DeliveryResult:
            return DeliveryResult(DeliveryDisposition.CONFLICT, message="server hash conflict")

    spool = DurableConnectorSpool(tmp_path / "spool")
    item = spool.enqueue(synthetic_bytes(), queued_at=ACCEPTED_AT)
    summary = ConnectorDeliveryService(spool, ConflictTransport()).deliver_ready(now=ACCEPTED_AT)

    assert summary.quarantined == 1
    assert not item.envelope_path.exists()
    assert (spool.quarantine_dir / f"{item.spool_id}.json").read_bytes() == item.exact_bytes
    evidence = json.loads((spool.quarantine_dir / f"{item.spool_id}.state.json").read_bytes())
    assert evidence["reason"] == "conflict"


def test_synthetic_producer_is_deterministic_and_changes_identity_by_sequence() -> None:
    producer = SyntheticConnectorProducer()
    first = producer.exact_bytes(date(2026, 8, 9), sequence=7)
    replay = producer.exact_bytes(date(2026, 8, 9), sequence=7)
    correction = producer.exact_bytes(date(2026, 8, 9), sequence=8)

    assert first == replay
    assert validate_envelope_bytes(first).envelope.batch_id != validate_envelope_bytes(
        correction
    ).envelope.batch_id


def test_events_bot_product_normalizes_the_deployed_producer_shape() -> None:
    record = {
        "events_added_total": 27,
        "counts_by_city": {"Калининград": 27},
        "counts_by_type": {"концерт": 9, "другое": 18},
    }
    assert normalize_daily_counters("events-bot.daily-statistics.v1", record) == {
        **record,
        "deferred_total": 0,
        "error_total": 0,
    }


def test_connector_transport_requires_https_and_retry_has_bounded_jitter() -> None:
    with pytest.raises(ValueError, match="HTTPS"):
        HttpConnectorTransport("http://intake.example/v1/batches", "secret")
    local = HttpConnectorTransport(
        "http://127.0.0.1:8080/intake/v1/batches",
        "secret",
        allow_insecure_loopback=True,
    )
    assert local.intake_url.startswith("http://127.0.0.1")
    policy = RetryPolicy(initial_seconds=10, maximum_seconds=30, jitter_fraction=0.2)
    first = policy.delay(0, jitter_key="batch-a").total_seconds()
    second = policy.delay(0, jitter_key="batch-b").total_seconds()
    assert 8 <= first <= 12
    assert 8 <= second <= 12
    assert first != second
    assert policy.delay(99, jitter_key="batch-a").total_seconds() <= 30
    assert policy.delay(1_000_000, jitter_key="batch-a").total_seconds() <= 30


def test_delivery_mode_is_one_registry_authorization_vocabulary() -> None:
    validated = validate_envelope_bytes(synthetic_bytes())
    registration = ConnectorRegistration(
        connector_id=validated.envelope.connector_id,
        delivery_mode=DeliveryMode.PUSH,
        status="active",
        policy_revision=1,
        enabled_data_products=frozenset({validated.envelope.data_product}),
    )
    registration.authorize(validated.envelope)
    mismatched = ConnectorRegistration(
        connector_id=validated.envelope.connector_id,
        delivery_mode=DeliveryMode.PULL,
        status="active",
        policy_revision=1,
        enabled_data_products=frozenset({validated.envelope.data_product}),
    )
    with pytest.raises(PermissionError, match="delivery_mode"):
        mismatched.authorize(validated.envelope)


def test_region_talk_pull_stops_before_adapter_or_spool_mutation(tmp_path: Path) -> None:
    class ForbiddenAdapter:
        called = False

        def pull(self, registration: ConnectorRegistration) -> bytes:
            self.called = True
            raise AssertionError("paused connector must not pull")

    adapter = ForbiddenAdapter()
    spool = DurableConnectorSpool(tmp_path / "spool")
    interface = OrchestratorPullInterface(spool, adapter)
    registration = ConnectorRegistration(
        connector_id="region-talk-ydb-bloggers-v1",
        delivery_mode=DeliveryMode.PULL,
        status="paused",
        policy_revision=1,
        enabled_data_products=frozenset(),
    )
    with pytest.raises(RuntimeError, match="CONNECTOR_REGISTRY_PAUSED"):
        interface.run_once(registration)
    assert adapter.called is False
    assert spool.pending() == []


class MemoryDurabilityRepository:
    def __init__(self, receipt: ConnectorDurabilityReceipt) -> None:
        self.receipt = receipt
        self.request_sha256: str | None = None
        self.operation_id: str | None = None
        self.terminal_sha256: str | None = None

    def get_durability_receipt(self, batch_id) -> ConnectorDurabilityReceipt:  # type: ignore[no-untyped-def]
        assert batch_id == self.receipt.acceptance.batch_id
        return self.receipt

    def pending_durability_batch_ids(self, *, limit: int = 25):  # type: ignore[no-untyped-def]
        if self.receipt.state in {
            ConnectorDurabilityState.CANONICAL_COMMITTED,
            ConnectorDurabilityState.CHECKPOINT_REQUESTED,
            ConnectorDurabilityState.CHECKPOINTING,
        }:
            return [self.receipt.acceptance.batch_id][:limit]
        return []

    def record_checkpoint_request(
        self,
        batch_id,
        *,
        request: ConnectorCheckpointRequest,
        operation: ConnectorCheckpointStatusReceipt,
    ) -> ConnectorDurabilityReceipt:  # type: ignore[no-untyped-def]
        exact = request.exact_sha256()
        if self.request_sha256 is not None and (
            self.request_sha256 != exact or self.operation_id != operation.operation_id
        ):
            raise ConnectorDurabilityConflict("checkpoint request changed")
        self.request_sha256 = exact
        self.operation_id = operation.operation_id
        self.receipt = self.receipt.model_copy(
            update={
                "state": ConnectorDurabilityState.CHECKPOINT_REQUESTED,
                "checkpoint_request_id": request.request_id,
                "checkpoint_operation_id": operation.operation_id,
            }
        )
        return self.receipt

    def record_checkpoint_status(
        self,
        batch_id,
        *,
        status: ConnectorCheckpointStatusReceipt,
    ) -> ConnectorDurabilityReceipt:  # type: ignore[no-untyped-def]
        exact = status.exact_sha256()
        if self.terminal_sha256 is not None and self.terminal_sha256 != exact:
            raise ConnectorDurabilityConflict("terminal checkpoint receipt changed")
        if status.state is ConnectorCheckpointState.DURABLE_COMPLETE:
            self.terminal_sha256 = exact
            state = ConnectorDurabilityState.DURABLE_COMPLETE
        elif status.state is ConnectorCheckpointState.RUNNING:
            state = ConnectorDurabilityState.CHECKPOINTING
        else:
            state = ConnectorDurabilityState.CHECKPOINT_REQUESTED
        self.receipt = self.receipt.model_copy(
            update={
                "state": state,
                "checkpoint_receipt_sha256": (
                    exact if state is ConnectorDurabilityState.DURABLE_COMPLETE else None
                ),
                "checkpoint_id": status.checkpoint_id,
            }
        )
        return self.receipt


@pytest.mark.asyncio
async def test_checkpoint_request_status_replay_is_exact_and_durable() -> None:
    acceptance = RecordingTransport(fail=False).submit(synthetic_bytes()).receipt
    assert acceptance is not None
    initial = ConnectorDurabilityReceipt(
        state=ConnectorDurabilityState.CANONICAL_COMMITTED,
        acceptance=acceptance,
        canonical_revision=18,
        updated_at=ACCEPTED_AT,
    )
    repository = MemoryDurabilityRepository(initial)

    class Gateway:
        def __init__(self) -> None:
            self.requests: list[ConnectorCheckpointRequest] = []

        def request_checkpoint(
            self, request: ConnectorCheckpointRequest
        ) -> ConnectorCheckpointStatusReceipt:
            self.requests.append(request)
            return ConnectorCheckpointStatusReceipt(
                request_id=request.request_id,
                operation_id="checkpoint-operation",
                state=ConnectorCheckpointState.REQUESTED,
                canonical_revision=request.canonical_revision,
            )

        def checkpoint_status(self, operation_id: str) -> ConnectorCheckpointStatusReceipt:
            assert operation_id == "checkpoint-operation"
            request = self.requests[0]
            return ConnectorCheckpointStatusReceipt(
                request_id=request.request_id,
                operation_id=operation_id,
                state=ConnectorCheckpointState.DURABLE_COMPLETE,
                canonical_revision=request.canonical_revision,
                checkpoint_id="checkpoint-18",
                manifest_sha256="d" * 64,
                verified_at=ACCEPTED_AT,
            )

    gateway = Gateway()
    completed = await ConnectorDurabilityService(repository, gateway).advance(
        acceptance.batch_id
    )
    assert completed.state is ConnectorDurabilityState.DURABLE_COMPLETE
    assert checkpoint_request_for(initial) == gateway.requests[0]
    replay = await ConnectorDurabilityService(repository, gateway).advance(acceptance.batch_id)
    assert replay == completed
    assert len(gateway.requests) == 1


@pytest.mark.asyncio
async def test_missing_checkpoint_gateway_blocks_before_repository_mutation() -> None:
    acceptance = RecordingTransport(fail=False).submit(synthetic_bytes()).receipt
    assert acceptance is not None
    initial = ConnectorDurabilityReceipt(
        state=ConnectorDurabilityState.CANONICAL_COMMITTED,
        acceptance=acceptance,
        canonical_revision=18,
        updated_at=ACCEPTED_AT,
    )
    repository = MemoryDurabilityRepository(initial)
    with pytest.raises(ConnectorCapabilityBlocked) as raised:
        await ConnectorDurabilityService(repository, None).advance(acceptance.batch_id)
    assert raised.value.public()["code"] == "CONNECTOR_CHECKPOINT_GATEWAY_UNAVAILABLE"
    assert raised.value.public()["mutation_started"] is False
    assert repository.receipt == initial
    assert repository.request_sha256 is None


@pytest.mark.asyncio
async def test_injected_checkpoint_coordinator_recovers_after_restart_to_verified_durable() -> None:
    acceptance = RecordingTransport(fail=False).submit(synthetic_bytes()).receipt
    assert acceptance is not None
    repository = MemoryDurabilityRepository(
        ConnectorDurabilityReceipt(
            state=ConnectorDurabilityState.CANONICAL_COMMITTED,
            acceptance=acceptance,
            canonical_revision=18,
            updated_at=ACCEPTED_AT,
        )
    )

    class Coordinator:
        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []
            self.complete = False

        def request_verified_checkpoint(self, **kwargs):  # type: ignore[no-untyped-def]
            self.calls.append(kwargs)
            return {**kwargs, "state": "REQUESTED"}

        def checkpoint_status(self, operation_id: str):  # type: ignore[no-untyped-def]
            request = self.calls[0]
            common = {
                **request,
                "operation_id": operation_id,
                "state": "DURABLE_COMPLETE" if self.complete else "RUNNING",
            }
            if self.complete:
                common.update(
                    {
                        "checkpoint_status": "VERIFIED",
                        "checkpoint_id": "checkpoint-18",
                        "current_checkpoint_id": "checkpoint-18",
                        "manifest_sha256": "d" * 64,
                        "verified_at": ACCEPTED_AT.isoformat(),
                    }
                )
            return common

    coordinator = Coordinator()
    gateway = build_connector_checkpoint_gateway(coordinator)
    assert gateway is not None
    first_process = ConnectorDurabilitySupervisor(repository, gateway)
    assert await first_process.reconcile_once() == 0
    assert repository.receipt.state is ConnectorDurabilityState.CHECKPOINTING
    assert len(coordinator.calls) == 1

    coordinator.complete = True
    restarted_process = ConnectorDurabilitySupervisor(repository, gateway)
    assert await restarted_process.reconcile_once() == 1
    assert repository.receipt.state is ConnectorDurabilityState.DURABLE_COMPLETE
    assert repository.receipt.checkpoint_id == "checkpoint-18"
    assert len(coordinator.calls) == 1
    assert coordinator.calls[0] == {
        "operation_id": f"connector-checkpoint:{repository.receipt.checkpoint_request_id}",
        "canonical_revision": 18,
        "idempotency_key": repository.receipt.checkpoint_request_id,
    }


def test_checkpoint_coordinator_adapter_is_fail_closed_and_requires_verified_head() -> None:
    assert build_connector_checkpoint_gateway(None) is None
    with pytest.raises(ConnectorCapabilityBlocked) as invalid:
        build_connector_checkpoint_gateway(object())  # type: ignore[arg-type]
    assert invalid.value.code == "CONNECTOR_VERIFIED_CHECKPOINT_COORDINATOR_INVALID"

    class IncompleteCoordinator:
        def request_verified_checkpoint(self, **kwargs):  # type: ignore[no-untyped-def]
            return {
                **kwargs,
                "state": "DURABLE_COMPLETE",
                "checkpoint_status": "VERIFIED",
                "checkpoint_id": "checkpoint-18",
                "current_checkpoint_id": "different-head",
                "manifest_sha256": "d" * 64,
                "verified_at": ACCEPTED_AT.isoformat(),
            }

        def checkpoint_status(self, operation_id: str):  # type: ignore[no-untyped-def]
            raise AssertionError(operation_id)

    acceptance = RecordingTransport(fail=False).submit(synthetic_bytes()).receipt
    assert acceptance is not None
    request = checkpoint_request_for(
        ConnectorDurabilityReceipt(
            state=ConnectorDurabilityState.CANONICAL_COMMITTED,
            acceptance=acceptance,
            canonical_revision=18,
            updated_at=ACCEPTED_AT,
        )
    )
    gateway = build_connector_checkpoint_gateway(IncompleteCoordinator())
    assert gateway is not None
    with pytest.raises(ConnectorCapabilityBlocked) as incomplete:
        gateway.request_checkpoint(request)
    assert incomplete.value.code == "CONNECTOR_CHECKPOINT_VERIFIED_RECEIPT_INCOMPLETE"


@pytest.mark.asyncio
async def test_durability_runtime_binds_committer_session_to_exact_active_epoch() -> None:
    class Resolver:
        def resolve_master(self, principal) -> MasterSnapshot:  # type: ignore[no-untyped-def]
            return MasterSnapshot(
                MasterState.ACTIVE,
                instance_id="master-instance",
                epoch=9,
                capabilities=frozenset({"sql", "fts", "pgvector"}),
            )

        def ensure_master(self, principal, *, intent: str):  # type: ignore[no-untyped-def]
            raise AssertionError((principal, intent))

    class Gateway:
        def request_checkpoint(self, request):  # type: ignore[no-untyped-def]
            raise AssertionError(request)

        def checkpoint_status(self, operation_id: str):  # type: ignore[no-untyped-def]
            raise AssertionError(operation_id)

    class Session:
        probed = False
        closed = False

        async def probe(self) -> None:
            self.probed = True

        async def reconcile_once(self, *, limit: int) -> int:
            assert limit == 7
            return 2

        async def close(self) -> None:
            self.closed = True

    class Broker:
        def __init__(self) -> None:
            self.request: ConnectorDurabilitySessionRequest | None = None
            self.sessions: list[Session] = []

        def issue_durability_session(
            self, request: ConnectorDurabilitySessionRequest
        ) -> Session:
            self.request = request
            session = Session()
            self.sessions.append(session)
            return session

    broker = Broker()
    gateway = Gateway()
    runtime = ActiveMasterConnectorDurabilityRuntime(Resolver(), broker, gateway)  # type: ignore[arg-type]
    await runtime.preflight()
    assert await runtime.reconcile_once(limit=7) == 2
    assert broker.request == ConnectorDurabilitySessionRequest("master-instance", 9)
    assert broker.request.role == "canonical_committer"
    assert broker.sessions[0].probed is True
    assert all(session.closed for session in broker.sessions)


@pytest.mark.asyncio
async def test_active_master_runtime_ensures_absent_master_before_any_mutation() -> None:
    class Resolver:
        ensured = 0

        def resolve_master(self, principal) -> MasterSnapshot:  # type: ignore[no-untyped-def]
            return MasterSnapshot(MasterState.ABSENT)

        def ensure_master(self, principal, *, intent: str) -> EnsureMasterReceipt:  # type: ignore[no-untyped-def]
            self.ensured += 1
            return EnsureMasterReceipt("ensure-operation", MasterState.REQUESTED, False, intent)

    class ForbiddenBroker:
        def issue_connector_session(self, request: ConnectorSessionRequest):  # type: ignore[no-untyped-def]
            raise AssertionError("broker must not run before ACTIVE")

    resolver = Resolver()
    runtime = ActiveMasterConnectorRuntime(resolver, ForbiddenBroker())  # type: ignore[arg-type]
    with pytest.raises(ConnectorCapabilityBlocked) as raised:
        await runtime.submit(
            synthetic_bytes(),
            principal=connector_principal("synthetic.daily-statistics"),
            correlation_id="correlation",
        )
    assert raised.value.public() == {
        "code": "MASTER_ENSURE_REQUESTED",
        "master_state": "REQUESTED",
        "operation_id": "ensure-operation",
        "retryable": True,
        "mutation_started": False,
    }
    assert resolver.ensured == 1


@pytest.mark.asyncio
async def test_active_master_runtime_binds_connector_session_to_exact_epoch() -> None:
    repository = MemoryAcceptanceRepository()

    class Resolver:
        def resolve_master(self, principal) -> MasterSnapshot:  # type: ignore[no-untyped-def]
            return MasterSnapshot(
                MasterState.ACTIVE,
                operation_id="master-operation",
                instance_id="master-instance",
                epoch=7,
                capabilities=frozenset({"sql"}),
            )

        def ensure_master(self, principal, *, intent: str) -> EnsureMasterReceipt:  # type: ignore[no-untyped-def]
            raise AssertionError("ACTIVE master must not be re-ensured")

    class Session:
        closed = False

        async def submit(self, exact_bytes: bytes, **kwargs) -> RepositoryDecision:  # type: ignore[no-untyped-def]
            return ConnectorIntakeService(repository).submit(
                exact_bytes,
                authenticated_connector_id=kwargs["authenticated_connector_id"],
                authenticated_principal=kwargs["authenticated_principal"],
                correlation_id=kwargs["correlation_id"],
            )

        async def acceptance_receipt(self, batch_id):  # type: ignore[no-untyped-def]
            return None

        async def durability_receipt(self, batch_id):  # type: ignore[no-untyped-def]
            return None

        async def health(self, connector_id: str) -> dict[str, object]:
            return {"connector_id": connector_id}

        async def close(self) -> None:
            self.closed = True

    class Broker:
        request: ConnectorSessionRequest | None = None
        session = Session()

        def issue_connector_session(self, request: ConnectorSessionRequest) -> Session:
            self.request = request
            return self.session

    broker = Broker()
    runtime = ActiveMasterConnectorRuntime(Resolver(), broker)  # type: ignore[arg-type]
    decision = await runtime.submit(
        synthetic_bytes(),
        principal=connector_principal("synthetic.daily-statistics"),
        correlation_id="correlation",
    )
    assert decision.disposition is AcceptanceDisposition.ACCEPTED
    assert broker.request is not None
    assert (
        broker.request.master_instance_id,
        broker.request.epoch,
        broker.request.role,
    ) == ("master-instance", 7, "connector")
    assert broker.session.closed is True
