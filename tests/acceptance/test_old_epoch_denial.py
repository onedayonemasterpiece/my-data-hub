from __future__ import annotations

import inspect
import json
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from uuid import UUID

import jsonschema
import pytest

from my_data_hub.acceptance.master_lifecycle import (
    MasterAcceptanceBinding,
    MasterAcceptanceRequest,
    command_for,
)
from my_data_hub.acceptance.master_production import ProductionAcceptanceBlocked
from my_data_hub.acceptance.old_epoch_denial import (
    BoundedWriteDenial,
    CredentialRegistrationDenial,
    OldEpochDenialReceipt,
    OldRuntimeProbeContext,
    ProductionOldEpochDenialProbe,
    PsycopgRetiredBoundedWriteClient,
    ReplacementEpochContext,
    RetiredTunnelCertificateIdentity,
    RuntimeRenewalDenial,
    TunnelRenewalDenial,
)
from my_data_hub.acceptance.old_epoch_production import TaskBoundOldEpochDenialFactory
from my_data_hub.mcp.postgres_broker import DirectoryEpochCredentialSource

ZERO = "0" * 64
ONE = "1" * 64
TWO = "2" * 64


def context() -> OldRuntimeProbeContext:
    return OldRuntimeProbeContext(
        task_id=UUID("00000000-0000-4000-8000-000000000011"),
        old_operation_id=UUID("00000000-0000-4000-8000-000000000101"),
        run_id=UUID("00000000-0000-4000-8000-000000000102"),
        attempt_id=UUID("00000000-0000-4000-8000-000000000103"),
        service_instance_id="postgres-master-old",
        master_instance_id=UUID("00000000-0000-4000-8000-000000000104"),
        epoch=4,
        runtime_token_sha256=ZERO,
        credential_handle=UUID("00000000-0000-4000-8000-000000000105"),
        tunnel_certificate=RetiredTunnelCertificateIdentity(
            serial=17, principal_sha256=ONE, public_key_sha256=TWO
        ),
    )


def replacement() -> ReplacementEpochContext:
    return ReplacementEpochContext(
        task_id=UUID("00000000-0000-4000-8000-000000000011"),
        operation_id=UUID("00000000-0000-4000-8000-000000000201"),
        master_instance_id=UUID("00000000-0000-4000-8000-000000000202"),
        epoch=5,
        active=True,
        checkpoint_id=UUID("00000000-0000-4000-8000-000000000203"),
        exact_version_ref="owner/checkpoint/9",
        manifest_sha256="3" * 64,
        checkpoint_status="VERIFIED",
        checkpoint_is_current=True,
    )


@dataclass
class Clients:
    calls: list[str] = field(default_factory=list)
    released: list[tuple[UUID, int]] = field(default_factory=list)

    def deny_retired_runtime(
        self, old: OldRuntimeProbeContext, new: ReplacementEpochContext
    ) -> RuntimeRenewalDenial:
        assert old.epoch < new.epoch
        self.calls.append("runtime")
        return RuntimeRenewalDenial(True, True, True, old.runtime_token_sha256, "MDH_RETIRED_RUNTIME_TOKEN")

    def deny_retired_credential(
        self, old: OldRuntimeProbeContext, new: ReplacementEpochContext
    ) -> CredentialRegistrationDenial:
        assert old.task_id == new.task_id
        self.calls.append("credential")
        return CredentialRegistrationDenial(
            True, True, "MDH_RETIRED_RUNTIME_REGISTER", "MDH_RETIRED_CREDENTIAL_BIND"
        )

    def deny_retired_write(
        self, old: OldRuntimeProbeContext, new: ReplacementEpochContext
    ) -> BoundedWriteDenial:
        del old, new
        self.calls.append("write")
        return BoundedWriteDenial(True, "55000", "rollback_only", 12, 12, "MDH_OLD_EPOCH_WRITE")

    def deny_retired_tunnel(
        self, old: OldRuntimeProbeContext, new: ReplacementEpochContext
    ) -> TunnelRenewalDenial:
        del new
        self.calls.append("tunnel")
        return TunnelRenewalDenial(
            True,
            True,
            "MDH_RETIRED_TUNNEL_LEASE",
            "MDH_RETIRED_TUNNEL_CERTIFICATE",
            old.tunnel_certificate.serial,
            old.tunnel_certificate.principal_sha256,
        )

    def release(self, *, credential_handle: UUID, certificate_serial: int) -> bool:
        self.released.append((credential_handle, certificate_serial))
        return True


def probe(old: OldRuntimeProbeContext, clients: Clients) -> ProductionOldEpochDenialProbe:
    return ProductionOldEpochDenialProbe(
        context=old,
        replacement=replacement(),
        runtime=clients,
        credentials=clients,
        writes=clients,
        tunnels=clients,
        release_port=clients,
    )


def test_fixed_probe_clears_context_and_recovers_identical_response() -> None:
    old = context()
    clients = Clients()
    adapter = probe(old, clients)

    first = adapter.prove_old_epoch_denials(old.binding)
    second = adapter.prove_old_epoch_denials(old.binding)

    assert first == second
    assert first.renew_denied is first.register_denied is first.bounded_write_denied is first.tunnel_denied is True
    assert clients.calls == ["runtime", "credential", "write", "tunnel"]
    assert clients.released == [(old.credential_handle, old.tunnel_certificate.serial)]
    assert adapter.context is None
    assert adapter.result_sha256 is not None and len(adapter.result_sha256) == 64


def test_context_can_be_issued_before_rotation_then_bound_exactly_once() -> None:
    old = context()
    clients = Clients()
    adapter = ProductionOldEpochDenialProbe(
        old, None, clients, clients, clients, clients, clients
    )
    with pytest.raises(ProductionAcceptanceBlocked, match="FM11_REPLACEMENT_NOT_BOUND"):
        adapter.prove_old_epoch_denials(old.binding)
    assert adapter.context == old
    assert clients.calls == []

    adapter.bind_replacement(replacement())
    adapter.bind_replacement(replacement())
    switched = ReplacementEpochContext(
        task_id=old.task_id,
        operation_id=UUID("00000000-0000-4000-8000-000000000211"),
        master_instance_id=UUID("00000000-0000-4000-8000-000000000212"),
        epoch=6,
        active=True,
        checkpoint_id=replacement().checkpoint_id,
        exact_version_ref="owner/checkpoint/9",
        manifest_sha256="3" * 64,
        checkpoint_status="VERIFIED",
        checkpoint_is_current=True,
    )
    with pytest.raises(ProductionAcceptanceBlocked, match="FM11_REPLACEMENT_REBIND_DENIED"):
        adapter.bind_replacement(switched)
    assert adapter.prove_old_epoch_denials(old.binding).bounded_write_denied is True


def test_task_factory_persists_context_and_holds_operator_before_stopped(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    old = context()
    request = MasterAcceptanceRequest(
        task_id=old.task_id,
        scenario="FM11",
        idempotency_key="fm11-pre-stopped-context",
        source_revision="a" * 40,
        target_operation_id=old.old_operation_id,
    )
    command = command_for(request, old.binding)
    connection = SimpleNamespace(close=lambda: None)

    class Clock:
        def now(self) -> datetime:
            return datetime(2026, 8, 11, tzinfo=UTC)

    class Ledger:
        clock = Clock()
        captured: dict[str, Any] | None = None

        def fm11_old_epoch_context(self, _task_id: str):  # type: ignore[no-untyped-def]
            return None

        def begin_fm11_old_epoch_context(self, **values: Any):  # type: ignore[no-untyped-def]
            assert values["task_id"] == str(old.task_id)
            return {
                **values,
                "runtime_token_sha256": ZERO,
                "old_binding": old.binding.model_dump(mode="json"),
            }, True

        def complete_fm11_old_epoch_context_capture(self, **values: Any):  # type: ignore[no-untyped-def]
            self.captured = values
            return values

        def release_fm11_old_epoch_context(self, **_values: Any) -> None:
            pass

        def fm11_retired_admission_observation(self, **_values: Any):  # type: ignore[no-untyped-def]
            raise AssertionError("denial is post-replacement only")

    class Tunnel:
        def acceptance_identity_snapshot(self, **values: Any) -> dict[str, object]:
            assert values["epoch"] == old.epoch
            return {"serial": 17, "principal_sha256": ONE, "public_key_sha256": TWO}

        def acceptance_retired_denial(self, **_values: Any) -> dict[str, object]:
            raise AssertionError("denial is post-replacement only")

    monkeypatch.setattr(
        "my_data_hub.acceptance.old_epoch_production.DirectoryOperatorConnectionFactory.open",
        lambda _self, binding: connection if binding == old.binding else pytest.fail("wrong binding"),
    )
    ledger = Ledger()
    factory = TaskBoundOldEpochDenialFactory(
        ledger=ledger,  # type: ignore[arg-type]
        source=DirectoryEpochCredentialSource(tmp_path),
        tunnel=Tunnel(),
    )
    first = factory.for_command(command)
    second = factory.for_command(command)
    assert first is second
    assert first.context is not None and first.context.binding == old.binding
    assert ledger.captured is not None
    assert ledger.captured["context_sha256"] == first.context.context_sha256
    assert first.context.credential_handle in factory.registry._values


def test_response_replay_cannot_switch_old_binding() -> None:
    old = context()
    adapter = probe(old, Clients())
    adapter.prove_old_epoch_denials(old.binding)
    another = old.binding.model_copy(update={"attempt_id": UUID("00000000-0000-4000-8000-000000000999")})

    with pytest.raises(ProductionAcceptanceBlocked, match="FM11_RESPONSE_REPLAY_BINDING_MISMATCH"):
        adapter.prove_old_epoch_denials(another)


def test_mismatched_observation_fails_closed_and_releases_handle() -> None:
    old = context()

    @dataclass
    class WrongToken(Clients):
        def deny_retired_runtime(
            self, old: OldRuntimeProbeContext, new: ReplacementEpochContext
        ) -> RuntimeRenewalDenial:
            del old, new
            self.calls.append("runtime")
            return RuntimeRenewalDenial(True, True, True, ONE, "MDH_RETIRED_RUNTIME_TOKEN")

    clients = WrongToken()
    adapter = probe(old, clients)
    with pytest.raises(ProductionAcceptanceBlocked, match="FM11_RUNTIME_TOKEN_HASH_MISMATCH"):
        adapter.prove_old_epoch_denials(old.binding)
    assert adapter.context is None
    assert clients.released == [(old.credential_handle, 17)]
    assert adapter.result_sha256 is None


def test_expired_context_is_released_before_any_admission_probe() -> None:
    old = context()
    clients = Clients()
    adapter = probe(old, clients)
    object.__setattr__(adapter, "_issued_monotonic_ns", time.monotonic_ns() - 901 * 1_000_000_000)
    with pytest.raises(ProductionAcceptanceBlocked, match="FM11_CONTEXT_EXPIRED"):
        adapter.prove_old_epoch_denials(old.binding)
    assert clients.calls == []
    assert clients.released == [(old.credential_handle, 17)]
    assert adapter.context is None


def test_release_failure_cannot_leave_a_successful_cached_result() -> None:
    old = context()

    @dataclass
    class ReleaseFailure(Clients):
        def release(self, *, credential_handle: UUID, certificate_serial: int) -> bool:
            self.released.append((credential_handle, certificate_serial))
            return False

    clients = ReleaseFailure()
    adapter = probe(old, clients)
    with pytest.raises(ProductionAcceptanceBlocked, match="FM11_CONTEXT_RELEASE_FAILED"):
        adapter.prove_old_epoch_denials(old.binding)
    assert adapter.context is None
    assert adapter.result_sha256 is None


def test_probe_rejects_unrelated_task_or_nonadvancing_replacement() -> None:
    old = context()
    clients = Clients()
    wrong_task = ReplacementEpochContext(
        task_id=UUID("00000000-0000-4000-8000-000000000012"),
        operation_id=replacement().operation_id,
        master_instance_id=replacement().master_instance_id,
        epoch=5,
        active=True,
        checkpoint_id=replacement().checkpoint_id,
        exact_version_ref="owner/checkpoint/9",
        manifest_sha256="3" * 64,
        checkpoint_status="VERIFIED",
        checkpoint_is_current=True,
    )
    with pytest.raises(ValueError, match="another task"):
        ProductionOldEpochDenialProbe(old, wrong_task, clients, clients, clients, clients, clients)

    nonadvancing = ReplacementEpochContext(
        task_id=old.task_id,
        operation_id=replacement().operation_id,
        master_instance_id=replacement().master_instance_id,
        epoch=4,
        active=True,
        checkpoint_id=replacement().checkpoint_id,
        exact_version_ref="owner/checkpoint/9",
        manifest_sha256="3" * 64,
        checkpoint_status="VERIFIED",
        checkpoint_is_current=True,
    )
    with pytest.raises(ValueError, match="did not advance"):
        ProductionOldEpochDenialProbe(old, nonadvancing, clients, clients, clients, clients, clients)


@pytest.mark.parametrize(
    "factory",
    [
        lambda: RuntimeRenewalDenial(False, True, True, ZERO, "MDH_RETIRED_RUNTIME_TOKEN"),
        lambda: CredentialRegistrationDenial(
            True, False, "MDH_RETIRED_RUNTIME_REGISTER", "MDH_RETIRED_CREDENTIAL_BIND"
        ),
        lambda: BoundedWriteDenial(True, "55000", "rollback_only", 1, 2, "MDH_OLD_EPOCH_WRITE"),
        lambda: TunnelRenewalDenial(
            True, False, "MDH_RETIRED_TUNNEL_LEASE", "MDH_RETIRED_TUNNEL_CERTIFICATE", 1, ZERO
        ),
    ],
)
def test_observation_contracts_reject_partial_or_mutating_results(factory: Any) -> None:
    with pytest.raises(ValueError):
        factory()


def test_sanitized_receipt_validates_schema_and_contains_no_secret_shape() -> None:
    old = context()
    runtime = RuntimeRenewalDenial(True, True, True, ZERO, "MDH_RETIRED_RUNTIME_TOKEN")
    credential = CredentialRegistrationDenial(
        True, True, "MDH_RETIRED_RUNTIME_REGISTER", "MDH_RETIRED_CREDENTIAL_BIND"
    )
    write = BoundedWriteDenial(True, "55000", "rollback_only", 4, 4, "MDH_OLD_EPOCH_WRITE")
    tunnel = TunnelRenewalDenial(
        True, True, "MDH_RETIRED_TUNNEL_LEASE", "MDH_RETIRED_TUNNEL_CERTIFICATE", 17, ONE
    )
    new = replacement()
    receipt = OldEpochDenialReceipt(
        old.context_sha256,
        old.task_id,
        old.old_operation_id,
        old.epoch,
        new.operation_id,
        new.master_instance_id,
        new.epoch,
        new.checkpoint_id,
        new.exact_version_ref,
        new.manifest_sha256,
        runtime,
        credential,
        write,
        tunnel,
    )
    schema = json.loads(Path("schemas/acceptance/old-epoch-denial-receipt.v1.schema.json").read_text())
    jsonschema.validate(receipt.public, schema)
    encoded = json.dumps(receipt.public).lower()
    for forbidden in ("database_url", "postgresql://", "password", "private_key", "bearer "):
        assert forbidden not in encoded
    assert len(receipt.receipt_sha256) == 64


def test_binding_model_is_the_only_runtime_argument() -> None:
    old = context()
    clients = Clients()
    adapter = probe(old, clients)
    public_method = adapter.prove_old_epoch_denials
    assert tuple(inspect.signature(public_method).parameters) == ("binding",)
    assert old.binding.__class__ is MasterAcceptanceBinding


def test_psycopg_helper_uses_only_fixed_h1_assertion_and_rollback_readback() -> None:
    import psycopg
    from psycopg.pq import TransactionStatus

    class EpochDenied(psycopg.Error):
        @property
        def sqlstate(self) -> str:
            return "55000"

    class Cursor:
        def __init__(self, row: tuple[object, ...] | None = None) -> None:
            self.row = row

        def fetchone(self) -> tuple[object, ...] | None:
            return self.row

    class Connection:
        def __init__(self) -> None:
            self.queries: list[str] = []
            self.rollbacks = 0
            self.commits = 0
            self.info = type("Info", (), {"transaction_status": TransactionStatus.INERROR})()

        def execute(self, query: str) -> Cursor:
            self.queries.append(query)
            if query.startswith("SELECT c.canonical_revision"):
                return Cursor((12, 5, str(replacement().master_instance_id)))
            if query == "SELECT master_control.assert_session_write_epoch()":
                raise EpochDenied("write rejected by epoch lease gate")
            if query.startswith("SELECT canonical_revision"):
                return Cursor((12,))
            return Cursor()

        def rollback(self) -> None:
            self.rollbacks += 1

        def commit(self) -> None:
            self.commits += 1

    connection = Connection()

    class Registry:
        def resolve(self, credential_handle: UUID) -> Connection:
            assert credential_handle == context().credential_handle
            return connection

    observation = PsycopgRetiredBoundedWriteClient(Registry()).deny_retired_write(
        context(), replacement()
    )
    assert observation.canonical_revision_before == observation.canonical_revision_after == 12
    assert observation.sqlstate == "55000"
    assert connection.commits == 1 and connection.rollbacks == 2
    assert connection.queries == [
        "SELECT c.canonical_revision,e.current_epoch,e.master_instance_id::text "
        "FROM hub.canonical_state c CROSS JOIN master_control.epoch_state e "
        "WHERE c.singleton=true AND e.singleton=true",
        "SET TRANSACTION READ WRITE",
        "SELECT master_control.assert_session_write_epoch()",
        "SELECT canonical_revision FROM hub.canonical_state WHERE singleton=true",
    ]
