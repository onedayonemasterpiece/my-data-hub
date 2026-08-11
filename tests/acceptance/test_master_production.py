from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from uuid import UUID, uuid4

import pytest
from jsonschema import Draft202012Validator, FormatChecker
from pydantic import ValidationError

from my_data_hub.acceptance.master_lifecycle import (
    ACCEPTANCE_OPERATE_SCOPE,
    MasterAcceptanceBinding,
    MasterAcceptanceRequest,
    command_for,
)
from my_data_hub.acceptance.master_production import (
    CallbackLossEvidence,
    ControlMasterAcceptanceExecutor,
    ControlRestartReceipt,
    MasterAcceptanceOperatorAdapter,
    OldEpochDenials,
    PostgresH1ExpiredLeaseDenialProbe,
    ProductionAcceptanceBlocked,
    ProductionControlAcceptanceContext,
    ProductionControlHostEffects,
    ProductionMasterAcceptanceEffectsFactory,
    StoredCallbackRef,
)
from my_data_hub.orchestrator.master import MasterState


@dataclass(frozen=True)
class Principal:
    subject: str = "owner"
    client_id: str = "acceptance-client"
    scopes: frozenset[str] = frozenset({ACCEPTANCE_OPERATE_SCOPE})


def _command(scenario: str):
    operation_id = uuid4()
    request = MasterAcceptanceRequest(
        task_id=uuid4(),
        scenario=scenario,
        idempotency_key=f"master-production-{scenario.lower()}-{uuid4()}",
        source_revision="a" * 40,
        target_operation_id=operation_id,
    )
    return command_for(
        request,
        MasterAcceptanceBinding(
            operation_id=operation_id,
            run_id=uuid4(),
            attempt_id=uuid4(),
            service_instance_id=str(uuid4()),
            master_instance_id=uuid4(),
            epoch=3,
        ),
    )


class Cursor:
    def __init__(self, rows: list[tuple[int]]) -> None:
        self.rows = rows

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return self.rows


class RecordingConnection:
    def __init__(self, *, revision: int = 0, counts: list[tuple[int]] | None = None) -> None:
        self.revision = revision
        self.counts = counts if counts is not None else [(0,)] * 12
        self.queries: list[str] = []

    def execute(self, query: str) -> Cursor:
        self.queries.append(query)
        if "canonical_revision" in query:
            return Cursor([(self.revision,)])
        return Cursor(self.counts)


def test_fm04_factory_runs_only_source_pinned_empty_probe() -> None:
    connection = RecordingConnection()
    effects = ProductionMasterAcceptanceEffectsFactory().build(
        connection=connection, boot_source="empty_baseline"
    )
    request = MasterAcceptanceRequest(
        task_id=uuid4(),
        scenario="FM04",
        idempotency_key=f"fm04-empty-{uuid4()}",
        source_revision="a" * 40,
    )
    binding = MasterAcceptanceBinding(
        operation_id=uuid4(),
        run_id=uuid4(),
        attempt_id=uuid4(),
        service_instance_id=str(uuid4()),
        master_instance_id=uuid4(),
        epoch=1,
    )
    evidence = effects.empty_master_bootstrap(command_for(request, binding))
    assert evidence.canonical_row_count == 0
    assert len(connection.queries) == 2
    probe = connection.queries[1]
    assert "region_talk.blogger_profile" in probe
    assert ";" not in probe and "DROP" not in probe and "INSERT" not in probe


def test_fm04_example_and_operator_status_schema_validate() -> None:
    root = Path(__file__).resolve().parents[2]
    example = json.loads(
        (root / "examples/acceptance/master-lifecycle-request-fm04.v1.example.json").read_text()
    )
    MasterAcceptanceRequest.model_validate(example)
    schema = json.loads(
        (root / "schemas/acceptance/master-lifecycle-status-request.v1.schema.json").read_text()
    )
    Draft202012Validator.check_schema(schema)
    Draft202012Validator(schema, format_checker=FormatChecker()).validate(
        {"task_id": example["task_id"]}
    )


def test_fm04_rejects_checkpoint_boot_and_nonempty_relation() -> None:
    command = command_for(
        MasterAcceptanceRequest(
            task_id=uuid4(),
            scenario="FM04",
            idempotency_key=f"fm04-reject-{uuid4()}",
            source_revision="a" * 40,
        ),
        _command("FM10").binding,
    )
    checkpoint = ProductionMasterAcceptanceEffectsFactory().build(
        connection=RecordingConnection(), boot_source="verified_checkpoint"
    )
    with pytest.raises(ProductionAcceptanceBlocked, match="FM04_NOT_EMPTY_BASELINE"):
        checkpoint.empty_master_bootstrap(command)
    nonempty = ProductionMasterAcceptanceEffectsFactory().build(
        connection=RecordingConnection(counts=[(0,)] * 11 + [(1,)]), boot_source="empty_baseline"
    )
    with pytest.raises(ProductionAcceptanceBlocked, match="FM04_CANONICAL_ROWS_NOT_EMPTY"):
        nonempty.empty_master_bootstrap(command)


def test_fm10_missing_h1_receipt_blocks_before_any_database_action() -> None:
    connection = RecordingConnection()
    effects = ProductionMasterAcceptanceEffectsFactory().build(
        connection=connection, boot_source="verified_checkpoint"
    )
    with pytest.raises(ProductionAcceptanceBlocked, match="FM10_H1_DENIAL_RECEIPT_UNAVAILABLE"):
        effects.lease_expiry_denial(_command("FM10"))
    assert connection.queries == []


class ProbeConnection:
    def __init__(self, command) -> None:
        import psycopg

        self.command = command
        self.info = SimpleNamespace(transaction_status=psycopg.pq.TransactionStatus.IDLE)
        self.queries: list[str] = []
        self.revision_reads = 0

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, query: str):
        from psycopg.errors import ObjectNotInPrerequisiteState
        from psycopg.pq import TransactionStatus

        self.queries.append(query)
        if "FROM pg_roles" in query:
            return Cursor([(False, False, False, False, False, True, False)])
        if "lease_until<=clock_timestamp" in query:
            return Cursor([(True, self.command.binding.epoch, str(self.command.binding.master_instance_id))])
        if "greatest(0,ceil(extract(EPOCH" in query:
            return Cursor([(10, self.command.binding.epoch, str(self.command.binding.master_instance_id))])
        if "FROM master_control.epoch_state" in query:
            return Cursor(
                [(self.command.binding.epoch, str(self.command.binding.master_instance_id), object(), "open", 10)]
            )
        if "canonical_revision" in query:
            self.revision_reads += 1
            return Cursor([(7,)])
        if "assert_session_write_epoch" in query:
            self.info.transaction_status = TransactionStatus.INERROR
            raise ObjectNotInPrerequisiteState("write rejected by epoch lease gate")
        return Cursor([])

    def commit(self) -> None:
        return None

    def rollback(self) -> None:
        import psycopg

        self.info.transaction_status = psycopg.pq.TransactionStatus.IDLE


class ProbeConnections:
    def __init__(self, connection: ProbeConnection) -> None:
        self.connection = connection

    def open(self, _binding):
        return self.connection


class Renewal:
    def __init__(self) -> None:
        self.commands = []

    def suspend_exact_renewal(self, command) -> None:
        self.commands.append(command)


def test_fm10_real_probe_observes_55000_rollback_only_and_revision_unchanged(monkeypatch) -> None:
    command = _command("FM10")
    connection = ProbeConnection(command)
    renewal = Renewal()
    monotonic_ns = iter((0, 0, 60_000_000_000))
    monotonic = iter((0.0, 60.0))
    monkeypatch.setattr("my_data_hub.acceptance.master_production.time.monotonic_ns", lambda: next(monotonic_ns))
    monkeypatch.setattr("my_data_hub.acceptance.master_production.time.monotonic", lambda: next(monotonic))
    monkeypatch.setattr(
        "my_data_hub.acceptance.master_production.time.sleep",
        lambda _seconds: pytest.fail("fixed FM10 unit clock should not sleep"),
    )
    evidence = PostgresH1ExpiredLeaseDenialProbe(
        connections=ProbeConnections(connection), renewal=renewal
    ).prove_expired_lease_denial(command)
    assert evidence.observed_wait_seconds == 60
    assert evidence.transaction_state == "rollback_only"
    assert evidence.denial_code == "MDH_EPOCH_LEASE_EXPIRED"
    assert evidence.canonical_revision_before == evidence.canonical_revision_after == 7
    assert renewal.commands == [command]
    assert any(query == "UPDATE hub.project SET status=status WHERE false" for query in connection.queries) is False


def test_fm07_blocks_before_provider_ensure_without_owner_host_cas() -> None:
    calls: list[str] = []
    runtime = SimpleNamespace(
        ensure=lambda _key: calls.append("ensure"),
        coordinator=SimpleNamespace(provider=object()),
    )
    executor = ControlMasterAcceptanceExecutor(runtime=runtime)  # type: ignore[arg-type]
    request = MasterAcceptanceRequest(
        task_id=uuid4(),
        scenario="FM07",
        idempotency_key=f"fm07-owner-claim-{uuid4()}",
        source_revision="a" * 40,
    )
    with pytest.raises(ProductionAcceptanceBlocked, match="FM07_OWNER_HOST_CLAIM_UNAVAILABLE"):
        executor._ensure_twenty(request, {"state": "PENDING"}, Principal())
    assert calls == []


class CallbackSupervisor:
    def __init__(self) -> None:
        self.before = UUID(int=1)
        self.after = UUID(int=2)
        self.event = StoredCallbackRef(UUID(int=3), "b" * 64)
        self.calls: list[str] = []

    def suppress_next_task_callback(self, _command) -> StoredCallbackRef:
        self.calls.append("suppress")
        return self.event

    def restart_control_process(self, _command) -> ControlRestartReceipt:
        self.calls.append("restart")
        return ControlRestartReceipt(self.before, self.after)

    def replay_stored_callback(self, _command, event_id: UUID) -> str:
        assert event_id == self.event.event_id
        self.calls.append("replay")
        return "duplicate"

    def exact_service_active(self, _binding) -> bool:
        self.calls.append("active")
        return True


def test_fm08_control_effect_requires_real_restart_and_replays_stored_id() -> None:
    supervisor = CallbackSupervisor()
    effects = ProductionControlHostEffects(runtime=Any, callback_supervisor=supervisor)  # type: ignore[arg-type]
    evidence = effects.execute(_command("FM08"))
    assert isinstance(evidence, CallbackLossEvidence)
    assert evidence.control_boot_id_before == UUID(int=1)
    assert evidence.control_boot_id_after == UUID(int=2)
    assert evidence.exact_event_id == UUID(int=3)
    assert supervisor.calls == ["suppress", "restart", "replay", "active"]


class OwnerClaims:
    def __init__(self, command) -> None:
        self.command = command
        self.claimed: tuple[UUID, str, str] | None = None
        self.completed = False

    def claim(self, *, task_id, expected_scenario, principal):
        self.claimed = (task_id, expected_scenario.value, principal.subject)
        return self.command

    def complete(self, *, receipt, principal):
        assert receipt.task_id == self.command.task_id
        assert principal.subject == "owner"
        self.completed = True
        return {"state": "PASSED"}


def test_host_executor_uses_separate_owner_claim_and_exact_receipt_cas() -> None:
    command = _command("FM08")
    claims = OwnerClaims(command)
    effects = ProductionControlHostEffects(
        runtime=Any, callback_supervisor=CallbackSupervisor()  # type: ignore[arg-type]
    )
    executor = ControlMasterAcceptanceExecutor(
        runtime=Any, host_claims=claims, host_effects=effects  # type: ignore[arg-type]
    )
    task = executor._reconcile_host(
        command.task_id,
        command.scenario,
        {"state": "BOUND"},
        Principal(),
    )
    assert task == {"state": "PASSED"}
    assert claims.claimed == (command.task_id, "FM08", "owner")
    assert claims.completed


class StoredReplay:
    def __init__(self, *, change_state: bool = False) -> None:
        self.change_state = change_state
        self.state_calls = 0

    def exact_acked_callback(self, _binding) -> StoredCallbackRef:
        return StoredCallbackRef(UUID(int=4), "c" * 64)

    def control_state_sha256(self, _binding) -> str:
        self.state_calls += 1
        return ("e" if self.change_state and self.state_calls > 1 else "d") * 64

    def replay_stored_callback(self, _event_id: UUID) -> str:
        return "duplicate"

    def replay_with_retired_runtime_auth(self, _event_id: UUID) -> bool:
        return True

    def replay_with_stale_epoch(self, _event_id: UUID) -> bool:
        return True


def test_fm09_exact_replay_and_stale_rejections_leave_state_unchanged() -> None:
    effects = ProductionControlHostEffects(runtime=Any, stored_replay=StoredReplay())  # type: ignore[arg-type]
    evidence = effects.execute(_command("FM09"))
    assert evidence.duplicate_disposition == "duplicate"
    assert evidence.state_sha256_before == evidence.state_sha256_after
    changed = ProductionControlHostEffects(runtime=Any, stored_replay=StoredReplay(change_state=True))  # type: ignore[arg-type]
    with pytest.raises(ValidationError, match="stale replay changed current control state"):
        changed.execute(_command("FM09"))


class CleanLedger:
    def __init__(self, operation_id: UUID) -> None:
        self.operation_id = operation_id

    def get_operation(self, operation_id: str):
        assert operation_id == str(self.operation_id)
        return SimpleNamespace(state="STOPPED")

    def verified_checkpoint_for_operation(self, operation_id: str) -> dict[str, str]:
        assert operation_id == str(self.operation_id)
        return {
            "checkpoint_id": str(UUID(int=8)),
            "version_ref": "owner/checkpoints/8",
            "manifest_sha256": "f" * 64,
        }


def test_fm12_finalizer_uses_real_stopped_operation_and_verified_head() -> None:
    command = _command("FM12")
    runtime = SimpleNamespace(ledger=CleanLedger(command.binding.operation_id))
    evidence = ProductionControlHostEffects(runtime=runtime).execute(command)
    assert evidence.terminal_state == "STOPPED"
    assert evidence.checkpoint_id == UUID(int=8)
    assert evidence.exact_readback_verified and evidence.restore_smoke_verified and evidence.head_promoted


class RotationLedger(CleanLedger):
    def runtime_event_history(self, **_identity):
        return [{"event_type": "runtime.draining"}, {"event_type": "runtime.terminal"}]


class OldDenialProbe:
    replacement = None

    def bind_replacement(self, replacement) -> None:
        self.replacement = replacement

    def prove_old_epoch_denials(self, _binding) -> OldEpochDenials:
        assert self.replacement is not None
        return OldEpochDenials(True, True, True, True, "1" * 64, "2" * 64)


def test_fm11_waits_for_old_stopped_checkpoint_then_activates_new_epoch() -> None:
    command = _command("FM11")
    ledger = RotationLedger(command.binding.operation_id)
    replacement = SimpleNamespace(
        state=MasterState.ACTIVE,
        epoch=command.binding.epoch + 1,
        operation_id=str(UUID(int=12)),
        master_instance_id=str(UUID(int=13)),
    )
    runtime = SimpleNamespace(ledger=ledger, ensure=lambda _key: (replacement, False))
    denial_probe = OldDenialProbe()
    evidence = ProductionControlHostEffects(
        runtime=runtime, old_epoch_denials=denial_probe  # type: ignore[arg-type]
    ).execute(command)
    assert evidence.old_runtime_draining_before_rotation
    assert evidence.new_epoch == command.binding.epoch + 1
    assert evidence.new_operation_id == UUID(int=12)
    assert evidence.renew_denied and evidence.register_denied
    assert evidence.bounded_write_denied and evidence.tunnel_denied
    assert denial_probe.replacement.master_instance_id == UUID(int=13)


class SoakSessions:
    def __init__(self) -> None:
        self.renewals = self.rotations = self.reads = self.denials = 0

    def renew_lease_and_tunnel(self, _binding) -> None:
        self.renewals += 1

    def rotate_credentials(self, _binding) -> None:
        self.rotations += 1

    def bounded_read(self, _binding) -> None:
        self.reads += 1

    def stale_session_reconnect_denied(self, _binding) -> bool:
        self.denials += 1
        return True

    def exact_service_active(self, _binding) -> bool:
        return True


def test_fm24_controller_has_fixed_3600_second_twelve_rotation_schedule(monkeypatch) -> None:
    times = iter((0, 3_600_000_000_000))
    monkeypatch.setattr("my_data_hub.acceptance.master_production.time.monotonic_ns", lambda: next(times))
    sleeps: list[int] = []
    monkeypatch.setattr("my_data_hub.acceptance.master_production.time.sleep", sleeps.append)
    sessions = SoakSessions()
    effects = ProductionMasterAcceptanceEffectsFactory(soak_sessions=sessions).build(
        connection=RecordingConnection(), boot_source="verified_checkpoint"
    )
    evidence = effects.session_rotation_soak(_command("FM24"))
    assert sleeps == [300] * 12
    assert (sessions.renewals, sessions.rotations, sessions.reads, sessions.denials) == (12, 12, 12, 12)
    assert evidence.observed_duration_seconds == 3600


def test_operator_adapter_schemas_are_closed_and_adapter_requires_scope() -> None:
    schemas = MasterAcceptanceOperatorAdapter.tool_schemas()
    assert set(schemas) == {"master.acceptance.request", "master.acceptance.status"}
    assert schemas["master.acceptance.status"]["additionalProperties"] is False
    adapter = MasterAcceptanceOperatorAdapter(executor=SimpleNamespace())  # type: ignore[arg-type]
    with pytest.raises(PermissionError, match="acceptance:operate"):
        adapter.call(
            "master.acceptance.status",
            {"task_id": str(uuid4())},
            Principal(scopes=frozenset({"data:read"})),
        )
    with pytest.raises(ValueError, match="unknown"):
        adapter.call("master.acceptance.list", {}, Principal())


def test_control_context_factory_installs_owner_claim_cas() -> None:
    runtime = SimpleNamespace(ledger=object())
    executor = ProductionControlAcceptanceContext().build(runtime)  # type: ignore[arg-type]
    assert executor.runtime is runtime
    assert executor.host_claims is not None and executor.host_effects is not None
