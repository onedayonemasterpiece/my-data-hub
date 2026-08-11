from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

import pytest
from jsonschema import Draft202012Validator

from my_data_hub.acceptance.master_lifecycle import MasterAcceptanceBinding
from my_data_hub.acceptance.soak_session import (
    SOAK_MAX_SECONDS,
    SOAK_STEP_COUNT,
    SOAK_STEP_SECONDS,
    ActiveServiceReceipt,
    BoundedReadReceipt,
    CredentialExpiryReceipt,
    CredentialRotationReceipt,
    ProductionSoakSessionPort,
    SoakSessionCancelled,
    SoakSessionDeadlineExceeded,
    SoakSessionError,
    SoakSessionNotDue,
    SoakSessionState,
    SoakStateJournal,
    StaleReconnectReceipt,
)
from my_data_hub.runtime_sdk import RetryPolicy, RuntimeClient, TransportResponse

H = "a" * 64
H2 = "b" * 64
H3 = "c" * 64


class FakeClock:
    def __init__(self) -> None:
        self.ns = 0
        self.wall = datetime(2026, 8, 11, tzinfo=UTC)

    def monotonic_ns(self) -> int:
        return self.ns

    def utc_now(self) -> datetime:
        return self.wall

    def advance(self, seconds: int) -> None:
        self.ns += seconds * 1_000_000_000
        self.wall += timedelta(seconds=seconds)


class DeliveredTransport:
    def __init__(self, events: list[str]) -> None:
        self.events = events

    def post(self, _url, _body, _headers, _timeout_seconds):
        self.events.append("heartbeat")
        return TransportResponse(status=204, body=b"")


class Gate:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.renewals = 0

    def renew(self, identity, lease_until) -> None:
        assert identity.epoch == 7
        assert lease_until.tzinfo is not None
        self.events.append("database")
        self.renewals += 1


@dataclass(frozen=True)
class Lease:
    master_instance_id: str
    run_id: str
    attempt_id: str
    epoch: int
    lease_until: datetime

    def to_json(self):
        return {
            "master_instance_id": self.master_instance_id,
            "run_id": self.run_id,
            "attempt_id": self.attempt_id,
            "epoch": self.epoch,
            "lease_until": self.lease_until.isoformat(),
        }


class Tunnel:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.renewals = 0

    def renew(self, *, master_instance_id, run_id, attempt_id, epoch, lease_until, now):
        assert now.tzinfo is not None
        self.events.append("tunnel")
        self.renewals += 1
        return Lease(master_instance_id, run_id, attempt_id, epoch, lease_until)


class Registrar:
    def __init__(self, events: list[str], *, lose_first_rotation_response: bool = False) -> None:
        self.events = events
        self.lose_first_rotation_response = lose_first_rotation_response
        self.rotations: dict[str, CredentialRotationReceipt] = {}
        self.rotation_effects = 0
        self.expiry_effects = 0

    def ensure_rotation(self, _binding, *, step, intent_sha256, expires_at):
        receipt = self.rotations.get(intent_sha256)
        if receipt is None:
            self.rotation_effects += 1
            receipt = CredentialRotationReceipt(
                evidence_class="injected",
                step=step,
                current_credential_sha256=_hash(f"current:{step}"),
                prior_credential_sha256=_hash(f"prior:{step}"),
                registration_receipt_sha256=_hash(f"register:{step}"),
                expires_at=expires_at,
            )
            self.rotations[intent_sha256] = receipt
            self.events.append("rotate")
            if self.lose_first_rotation_response:
                self.lose_first_rotation_response = False
                raise ConnectionError("response lost after broker committed rotation")
        else:
            self.events.append("rotate-reconcile")
        return receipt

    def ensure_prior_expired(self, _binding, *, step, intent_sha256):
        del intent_sha256
        self.events.append("expire")
        self.expiry_effects += 1
        return CredentialExpiryReceipt(
            evidence_class="injected",
            step=step,
            prior_credential_sha256=_hash(f"prior:{step}"),
            expiry_receipt_sha256=_hash(f"expire:{step}"),
            expired=True,
        )


class Probe:
    def __init__(self, events: list[str]) -> None:
        self.events = events

    def bounded_read(self, binding, *, step, intent_sha256):
        del intent_sha256
        self.events.append("read")
        return BoundedReadReceipt(
            evidence_class="injected",
            step=step,
            query_contract="fm24_active_epoch_read.v1",
            observed_rows=1,
            active_epoch=binding.epoch,
            read_receipt_sha256=_hash(f"read:{step}"),
        )

    def stale_reconnect_denied(self, _binding, *, step, intent_sha256):
        del intent_sha256
        self.events.append("deny")
        return StaleReconnectReceipt(
            evidence_class="injected",
            step=step,
            denied=True,
            denial_code="MDH_CREDENTIAL_EXPIRED_OR_REVOKED",
            broker_binding_verified=True,
            denial_receipt_sha256=_hash(f"deny:{step}"),
        )

    def exact_service_active(self, binding):
        self.events.append("active")
        return ActiveServiceReceipt(
            evidence_class="injected",
            active=True,
            epoch=binding.epoch,
            service_receipt_sha256=H,
        )


@dataclass
class Rig:
    port: ProductionSoakSessionPort
    binding: MasterAcceptanceBinding
    clock: FakeClock
    events: list[str]
    gate: Gate
    tunnel: Tunnel
    registrar: Registrar
    state_path: Path


def _hash(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _binding() -> MasterAcceptanceBinding:
    return MasterAcceptanceBinding(
        operation_id=UUID(int=1),
        run_id=UUID(int=2),
        attempt_id=UUID(int=3),
        service_instance_id="master-7",
        master_instance_id=UUID(int=4),
        epoch=7,
    )


def _rig(tmp_path: Path, *, registrar: Registrar | None = None, cancelled=lambda: False) -> Rig:
    binding = _binding()
    clock = FakeClock()
    events: list[str] = []
    gate = Gate(events)
    tunnel = Tunnel(events)
    registrar = registrar or Registrar(events)
    state_path = tmp_path / "task" / "fm24.json"
    runtime = RuntimeClient(
        callback_url="https://control.example/internal/runtime/events",
        run_secret="x" * 32,
        run_id=str(binding.run_id),
        attempt_id=str(binding.attempt_id),
        service_instance_id=binding.service_instance_id,
        source_identity="owner/master",
        source_version="a" * 40,
        epoch=binding.epoch,
        spool_path=tmp_path / "runtime" / "events.jsonl",
        transport=DeliveredTransport(events),
        retry_policy=RetryPolicy(max_attempts=1),
        now=clock.utc_now,
    )
    port = ProductionSoakSessionPort(
        task_id=UUID(int=5),
        binding=binding,
        journal=SoakStateJournal(state_path),
        runtime_client=runtime,
        database_gate=gate,  # type: ignore[arg-type]
        tunnel_authority=tunnel,  # type: ignore[arg-type]
        credential_registrar=registrar,
        read_probe=Probe(events),
        evidence_class="injected",
        clock=clock,
        cancelled=cancelled,
    )
    return Rig(port, binding, clock, events, gate, tunnel, registrar, state_path)


def _run_step(rig: Rig) -> None:
    rig.clock.advance(SOAK_STEP_SECONDS)
    rig.port.renew_lease_and_tunnel(rig.binding)
    rig.port.rotate_credentials(rig.binding)
    rig.port.bounded_read(rig.binding)
    assert rig.port.stale_session_reconnect_denied(rig.binding)


def test_fixed_twelve_step_hour_persists_intents_acks_and_metadata_only_state(tmp_path: Path) -> None:
    rig = _rig(tmp_path)
    with pytest.raises(SoakSessionNotDue, match="FM24_NEXT_STEP_NOT_DUE"):
        rig.port.renew_lease_and_tunnel(rig.binding)

    for _ in range(SOAK_STEP_COUNT):
        _run_step(rig)

    assert rig.clock.ns == 3600 * 1_000_000_000
    assert rig.port.exact_service_active(rig.binding)
    state = rig.port.durable_state(rig.binding)
    assert state.status == "COMPLETE"
    assert state.completed_steps == SOAK_STEP_COUNT
    assert state.lease_renewals == state.tunnel_renewals == SOAK_STEP_COUNT
    assert state.session_rotations == state.bounded_reads == SOAK_STEP_COUNT
    assert state.rejected_stale_sessions == SOAK_STEP_COUNT
    assert rig.gate.renewals == rig.tunnel.renewals == SOAK_STEP_COUNT
    assert rig.registrar.rotation_effects == rig.registrar.expiry_effects == SOAK_STEP_COUNT
    assert rig.events[:7] == ["heartbeat", "database", "tunnel", "rotate", "read", "expire", "deny"]
    assert rig.events[-1] == "active"
    assert rig.state_path.stat().st_mode & 0o777 == 0o600
    persisted = rig.state_path.read_text()
    assert all(secret not in persisted for secret in ("postgresql://", "password", "secret", "row-value"))
    assert str(rig.binding.run_id) not in persisted
    assert str(rig.binding.master_instance_id) not in persisted


def test_rotation_response_loss_resumes_same_intent_without_double_counting(tmp_path: Path) -> None:
    events: list[str] = []
    registrar = Registrar(events, lose_first_rotation_response=True)
    rig = _rig(tmp_path, registrar=registrar)
    # Use the rig's event list so ordering assertions still cover the other ports.
    registrar.events = rig.events
    rig.clock.advance(SOAK_STEP_SECONDS)
    rig.port.renew_lease_and_tunnel(rig.binding)
    with pytest.raises(ConnectionError, match="response lost"):
        rig.port.rotate_credentials(rig.binding)

    state = rig.port.durable_state(rig.binding)
    assert state.completed_steps == 0
    assert state.steps[-1].actions[-1].state == "INTENT_COMMITTED"
    intent = state.steps[-1].actions[-1].intent_sha256

    # Reconstruct the adapter over the exact task-owned state, as a process restart would.
    resumed = ProductionSoakSessionPort(
        task_id=rig.port.task_id,
        binding=rig.binding,
        journal=SoakStateJournal(rig.state_path),
        runtime_client=rig.port.runtime_client,
        database_gate=rig.gate,  # type: ignore[arg-type]
        tunnel_authority=rig.tunnel,  # type: ignore[arg-type]
        credential_registrar=registrar,
        read_probe=rig.port.read_probe,
        evidence_class="injected",
        clock=rig.clock,
    )
    resumed.renew_lease_and_tunnel(rig.binding)
    resumed.rotate_credentials(rig.binding)
    resumed.bounded_read(rig.binding)
    assert resumed.stale_session_reconnect_denied(rig.binding)
    final = resumed.durable_state(rig.binding)
    assert final.completed_steps == 1
    assert registrar.rotation_effects == 1
    assert final.steps[0].actions[3].intent_sha256 == intent
    assert rig.gate.renewals == rig.tunnel.renewals == 1


def test_cancel_is_durable_and_prevents_any_side_effect(tmp_path: Path) -> None:
    cancelled = True
    rig = _rig(tmp_path, cancelled=lambda: cancelled)
    rig.clock.advance(SOAK_STEP_SECONDS)
    with pytest.raises(SoakSessionCancelled, match="FM24_CANCELLED"):
        rig.port.renew_lease_and_tunnel(rig.binding)
    assert rig.events == []
    assert rig.port.durable_state(rig.binding).status == "CANCELLED"


def test_absolute_deadline_is_durable_and_prevents_late_side_effect(tmp_path: Path) -> None:
    rig = _rig(tmp_path)
    rig.clock.advance(SOAK_MAX_SECONDS + 1)
    with pytest.raises(SoakSessionDeadlineExceeded, match="FM24_DEADLINE_EXCEEDED"):
        rig.port.renew_lease_and_tunnel(rig.binding)
    assert rig.events == []
    state = rig.port.durable_state(rig.binding)
    assert state.status == "FAILED"
    assert state.terminal_code == "FM24_DEADLINE_EXCEEDED"


def test_live_evidence_rejects_fake_clock_before_any_state_write(tmp_path: Path) -> None:
    rig = _rig(tmp_path)
    with pytest.raises(ValueError, match="non-accelerated system"):
        ProductionSoakSessionPort(
            task_id=UUID(int=9),
            binding=rig.binding,
            journal=SoakStateJournal(tmp_path / "other.json"),
            runtime_client=rig.port.runtime_client,
            database_gate=rig.gate,  # type: ignore[arg-type]
            tunnel_authority=rig.tunnel,  # type: ignore[arg-type]
            credential_registrar=rig.registrar,
            read_probe=rig.port.read_probe,
            evidence_class="live",
            clock=rig.clock,
        )
    assert not (tmp_path / "other.json").exists()


def test_state_schema_and_example_are_valid() -> None:
    root = Path(__file__).resolve().parents[2]
    schema = json.loads((root / "schemas" / "fm24-soak-state.v1.schema.json").read_text())
    example = json.loads((root / "examples" / "contracts" / "fm24-soak-state.v1.example.json").read_text())
    Draft202012Validator.check_schema(schema)
    Draft202012Validator(schema).validate(example)
    SoakSessionState.model_validate(example)
    assert example["deadline_monotonic_ns"] == SOAK_MAX_SECONDS * 1_000_000_000


def test_journal_rejects_world_readable_state(tmp_path: Path) -> None:
    rig = _rig(tmp_path)
    rig.state_path.chmod(0o644)
    with pytest.raises(SoakSessionError, match="permissions"):
        rig.port.durable_state(rig.binding)
