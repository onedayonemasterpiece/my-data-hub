from __future__ import annotations

import json
import threading
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import UUID

import pytest

from my_data_hub.master_runtime.contracts import MasterIdentity
from my_data_hub.master_runtime.task_credentials import (
    IssuedTaskCredential,
    TaskCredentialBatch,
    TaskCredentialCommand,
    TaskCredentialContractError,
    TaskCredentialPoller,
    TaskCredentialReconciler,
    TaskCredentialRevocation,
    task_command_sha256,
)

NOW = datetime(2026, 8, 19, 12, tzinfo=UTC)
IDENTITY = MasterIdentity(
    UUID("44444444-4444-4444-8444-444444444444"),
    "master-run",
    7,
)
TASK = UUID("22222222-2222-4222-8222-222222222222")


def _command(*, kind: str = "region_talk", generation: int = 1, epoch: int = 7) -> dict[str, object]:
    value: dict[str, object] = {
        "schema_version": "my-data-hub-task-credential-command.v1",
        "worker_kind": kind,
        "task_run_id": str(TASK),
        "epoch": epoch,
        "generation": generation,
        "task_token_sha256": "a" * 64,
    }
    value["command_sha256"] = task_command_sha256(value)
    return value


def _binding_receipt(**value: object) -> dict[str, object]:
    identity = value["identity"]
    assert isinstance(identity, MasterIdentity)
    return {
        "registered": True,
        "credential_id": str(value["credential_id"]),
        "principal": value["principal"],
        "worker_kind": value["worker_kind"],
        "task_run_id": str(value["task_run_id"]),
        "generation": value["generation"],
        "master_instance_id": str(identity.master_instance_id),
        "epoch": identity.epoch,
        "command_sha256": value["command_sha256"],
        "task_token_sha256": value["task_token_sha256"],
    }


def _gate(
    *,
    revoked: list[tuple[object, ...]] | None = None,
    bindings: list[dict[str, object]] | None = None,
) -> SimpleNamespace:
    revoked = revoked if revoked is not None else []
    bindings = bindings if bindings is not None else []

    def bind(**value: object) -> dict[str, object]:
        bindings.append(dict(value))
        return _binding_receipt(**value)

    return SimpleNamespace(
        revoke_credential=lambda *args: revoked.append(args),
        register_task_credential_binding=bind,
    )


def test_task_credential_command_rejects_wrong_kind_epoch_generation_and_hash() -> None:
    TaskCredentialCommand.model_validate(_command())
    for field, value in (
        ("worker_kind", "unknown"),
        ("generation", 0),
        ("task_run_id", "not-a-uuid"),
        ("command_sha256", "b" * 64),
    ):
        body = _command()
        body[field] = value
        with pytest.raises(ValueError):
            TaskCredentialCommand.model_validate(body)
    with pytest.raises(ValueError):
        TaskCredentialCommand.model_validate(
            {**_command(), "database_url": "postgresql://secret@forbidden"}
        )


def test_reconciler_keeps_old_region_credential_until_exact_worker_activation() -> None:
    created: list[dict[str, object]] = []
    dropped: list[str] = []
    revoked: list[tuple[object, ...]] = []
    bindings: list[dict[str, object]] = []
    registrations: list[dict[str, object]] = []
    provisioner = SimpleNamespace(
        create=lambda **kwargs: created.append(kwargs) or kwargs["principal"],
        drop=dropped.append,
    )
    gate = _gate(revoked=revoked, bindings=bindings)
    reconciler = TaskCredentialReconciler(identity=IDENTITY, local_postgres_port=25432)

    reconciler.reconcile(
        batch=TaskCredentialBatch(commands=(TaskCredentialCommand.model_validate(_command()),)),
        provisioner=provisioner,
        gate=gate,
        lease_until=NOW + timedelta(minutes=5),
        register=lambda body: registrations.append(body) or {
            "registered": True,
            "worker_kind": body["worker_kind"],
            "task_run_id": body["task_run_id"],
            "epoch": body["epoch"],
            "generation": body["generation"],
            "credential_id": body["credential_id"],
            "command_sha256": body["command_sha256"],
        },
        now=NOW,
    )
    assert created[0]["group"] == "mdh_region_talk_pipeline"
    assert len(bindings) == 1
    assert bindings[0]["task_run_id"] == TASK
    assert created[0]["expires_at"] == NOW + timedelta(minutes=4)
    assert len(registrations) == 1
    assert "opaque-secret" not in json.dumps(registrations)
    first = next(iter(reconciler.issued.values()))
    first_acks = reconciler.registration_acknowledgements()
    assert len(first_acks) == 1 and first_acks[0]["generation"] == 1
    reconciler.mark_registration_acknowledged(first_acks)

    refreshed = TaskCredentialCommand.model_validate(_command(generation=2))
    reconciler.reconcile(
        batch=TaskCredentialBatch(commands=(refreshed,)),
        provisioner=provisioner,
        gate=gate,
        lease_until=NOW + timedelta(minutes=8),
        register=lambda body: {
            "registered": True,
            "worker_kind": body["worker_kind"],
            "task_run_id": body["task_run_id"],
            "epoch": body["epoch"],
            "generation": body["generation"],
            "credential_id": body["credential_id"],
            "command_sha256": body["command_sha256"],
        },
        now=NOW + timedelta(minutes=3),
    )
    # Registration only stages the replacement.  The old LOGIN remains usable
    # until the worker proves the new SSH tunnel and epoch-bound DB session.
    assert next(iter(reconciler.issued.values())) == first
    second = next(iter(reconciler.pending.values()))
    assert second.generation == 2 and second.credential_id != first.credential_id
    assert first.principal not in dropped and revoked == []

    activation = TaskCredentialRevocation.model_validate(
        {
            "schema_version": "my-data-hub-task-credential-revocation.v1",
            "worker_kind": "region_talk",
            "task_run_id": str(TASK),
            "epoch": first.epoch,
            "generation": first.generation,
            "task_token_sha256": first.task_token_sha256,
            "command_sha256": first.command_sha256,
            "credential_id": str(first.credential_id),
            "reason": "task_credential_rotated",
        }
    )
    reconciler.reconcile(
        batch=TaskCredentialBatch(revocations=(activation,)),
        provisioner=provisioner,
        gate=gate,
        lease_until=NOW + timedelta(minutes=8),
        register=lambda _body: pytest.fail("activation must not re-register"),
        now=NOW + timedelta(minutes=3, seconds=10),
    )
    assert next(iter(reconciler.issued.values())) == second
    assert reconciler.pending == {}
    assert first.principal in dropped
    assert revoked and revoked[0][0] == first.credential_id

    # A lost GET response may deliver the exact activation again.  It is an
    # idempotent no-op, not an attempt to revoke the replacement generation.
    reconciler.reconcile(
        batch=TaskCredentialBatch(revocations=(activation,)),
        provisioner=provisioner,
        gate=gate,
        lease_until=NOW + timedelta(minutes=8),
        register=lambda _body: pytest.fail("activation replay must be a no-op"),
        now=NOW + timedelta(minutes=3, seconds=20),
    )
    assert next(iter(reconciler.issued.values())) == second


def test_reconciler_rejects_stale_or_conflicting_commands_and_exact_revocation() -> None:
    reconciler = TaskCredentialReconciler(identity=IDENTITY, local_postgres_port=25432)
    key = ("embedding", TASK)
    issued = IssuedTaskCredential(
        worker_kind="embedding",
        task_run_id=TASK,
        epoch=7,
        generation=3,
        task_token_sha256="a" * 64,
        command_sha256="b" * 64,
        credential_id=UUID("33333333-3333-4333-8333-333333333333"),
        principal="mdh_e7_embed_deadbeef",
        expires_at=NOW + timedelta(minutes=2),
    )
    reconciler.issued[key] = issued
    provisioner = SimpleNamespace(create=lambda **_kwargs: None, drop=lambda _principal: None)
    gate = SimpleNamespace(revoke_credential=lambda *_args: None)
    with pytest.raises(TaskCredentialContractError, match="stale"):
        reconciler.reconcile(
            batch=TaskCredentialBatch(
                commands=(
                    TaskCredentialCommand.model_validate(
                        _command(kind="embedding", generation=2)
                    ),
                )
            ),
            provisioner=provisioner,
            gate=gate,
            lease_until=NOW + timedelta(minutes=5),
            register=lambda _body: {},
            now=NOW,
        )

    revocation_body = {
        "schema_version": "my-data-hub-task-credential-revocation.v1",
        "worker_kind": "embedding",
        "task_run_id": str(TASK),
        "epoch": 7,
        "generation": 3,
        "task_token_sha256": issued.task_token_sha256,
        "command_sha256": issued.command_sha256,
        "credential_id": str(issued.credential_id),
        "reason": "task_terminal",
    }
    revocation = TaskCredentialRevocation.model_validate(revocation_body)
    wrong = revocation.model_copy(update={"generation": 4})
    with pytest.raises(TaskCredentialContractError, match="not bound"):
        reconciler.reconcile(
            batch=TaskCredentialBatch(revocations=(wrong,)),
            provisioner=provisioner,
            gate=gate,
            lease_until=NOW + timedelta(minutes=5),
            register=lambda _body: {},
            now=NOW,
        )


def test_reconciler_replay_after_restart_is_idempotent_when_exact_credential_is_restored() -> None:
    command = TaskCredentialCommand.model_validate(_command())
    restored = IssuedTaskCredential(
        worker_kind="region_talk",
        task_run_id=TASK,
        epoch=7,
        generation=1,
        task_token_sha256=command.task_token_sha256,
        command_sha256=command.command_sha256,
        credential_id=UUID("33333333-3333-4333-8333-333333333333"),
        principal="mdh_e7_region_deadbeef",
        expires_at=NOW + timedelta(minutes=3),
    )
    reconciler = TaskCredentialReconciler(
        identity=IDENTITY,
        local_postgres_port=25432,
        issued={("region_talk", TASK): restored},
    )
    created: list[object] = []
    reconciler.reconcile(
        batch=TaskCredentialBatch(commands=(command,)),
        provisioner=SimpleNamespace(create=lambda **kwargs: created.append(kwargs), drop=lambda _principal: None),
        gate=_gate(),
        lease_until=NOW + timedelta(minutes=5),
        register=lambda _body: pytest.fail("exact replay must not re-register"),
        now=NOW,
    )
    assert created == []


def test_registration_response_loss_replays_same_login_and_secret_without_duplicate_create() -> None:
    command = TaskCredentialCommand.model_validate(_command())
    created: list[dict[str, object]] = []
    dropped: list[str] = []
    registrations: list[dict[str, object]] = []
    provisioner = SimpleNamespace(
        create=lambda **kwargs: created.append(kwargs), drop=dropped.append
    )
    reconciler = TaskCredentialReconciler(
        identity=IDENTITY, local_postgres_port=25432
    )

    def lost(body: dict[str, object]):
        registrations.append(dict(body))
        raise TimeoutError("registration response was lost")

    reconciler.reconcile(
        batch=TaskCredentialBatch(commands=(command,)),
        provisioner=provisioner,
        gate=_gate(),
        lease_until=NOW + timedelta(minutes=5),
        register=lost,
        now=NOW,
    )
    assert len(created) == 1 and dropped == [] and reconciler.issued == {}

    def replay(body: dict[str, object]):
        registrations.append(dict(body))
        return {
            "registered": True,
            "worker_kind": body["worker_kind"],
            "task_run_id": body["task_run_id"],
            "epoch": body["epoch"],
            "generation": body["generation"],
            "credential_id": body["credential_id"],
            "command_sha256": body["command_sha256"],
        }

    reconciler.reconcile(
        batch=TaskCredentialBatch(commands=(command,)),
        provisioner=provisioner,
        gate=SimpleNamespace(revoke_credential=lambda *_args: None),
        lease_until=NOW + timedelta(minutes=5),
        register=replay,
        now=NOW + timedelta(seconds=10),
    )
    assert len(created) == 1 and len(reconciler.issued) == 1
    assert registrations[0] == registrations[1]
    assert registrations[0]["database_url"] == registrations[1]["database_url"]
    acks = reconciler.registration_acknowledgements()
    assert len(acks) == 1 and acks[0]["credential_id"] == registrations[0]["credential_id"]


def test_database_binding_response_loss_replays_before_private_registration() -> None:
    command = TaskCredentialCommand.model_validate(_command())
    order: list[str] = []
    binding_attempts: list[dict[str, object]] = []
    created: list[dict[str, object]] = []

    def bind(**value: object) -> dict[str, object]:
        order.append("bind")
        binding_attempts.append(dict(value))
        if len(binding_attempts) == 1:
            raise TimeoutError("binding commit response was lost")
        return _binding_receipt(**value)

    gate = SimpleNamespace(
        revoke_credential=lambda *_args: None,
        register_task_credential_binding=bind,
    )
    reconciler = TaskCredentialReconciler(
        identity=IDENTITY, local_postgres_port=25432
    )

    def register(body: dict[str, object]) -> dict[str, object]:
        order.append("register")
        return {
            "registered": True,
            "worker_kind": body["worker_kind"],
            "task_run_id": body["task_run_id"],
            "epoch": body["epoch"],
            "generation": body["generation"],
            "credential_id": body["credential_id"],
            "command_sha256": body["command_sha256"],
        }

    for now in (NOW, NOW + timedelta(seconds=10)):
        reconciler.reconcile(
            batch=TaskCredentialBatch(commands=(command,)),
            provisioner=SimpleNamespace(
                create=lambda **value: created.append(value),
                drop=lambda _principal: None,
            ),
            gate=gate,
            lease_until=NOW + timedelta(minutes=5),
            register=register,
            now=now,
        )

    assert len(created) == 1
    assert binding_attempts[0] == binding_attempts[1]
    assert order == ["bind", "bind", "register"]
    assert len(reconciler.issued) == 1


def test_embedding_rotation_preserves_immediate_legacy_handoff() -> None:
    created: list[dict[str, object]] = []
    dropped: list[str] = []
    provisioner = SimpleNamespace(
        create=lambda **kwargs: created.append(kwargs), drop=dropped.append
    )
    reconciler = TaskCredentialReconciler(
        identity=IDENTITY, local_postgres_port=25432
    )

    def register(body: dict[str, object]):
        return {
            "registered": True,
            "worker_kind": body["worker_kind"],
            "task_run_id": body["task_run_id"],
            "epoch": body["epoch"],
            "generation": body["generation"],
            "credential_id": body["credential_id"],
            "command_sha256": body["command_sha256"],
        }

    for generation, now in ((1, NOW), (2, NOW + timedelta(minutes=2))):
        reconciler.reconcile(
            batch=TaskCredentialBatch(
                commands=(
                    TaskCredentialCommand.model_validate(
                        _command(kind="embedding", generation=generation)
                    ),
                )
            ),
            provisioner=provisioner,
            gate=SimpleNamespace(revoke_credential=lambda *_args: None),
            lease_until=NOW + timedelta(minutes=8),
            register=register,
            now=now,
        )
    assert reconciler.pending == {}
    assert reconciler.issued[("embedding", TASK)].generation == 2
    assert len(dropped) == 1


def test_task_credential_poller_runs_independently_and_surfaces_fatal_contract_errors() -> None:
    called = threading.Event()
    invocations: list[int] = []

    def poll() -> None:
        invocations.append(1)
        called.set()

    poller = TaskCredentialPoller(poll=poll, interval_seconds=0.01)
    poller.start()
    assert called.wait(1)
    poller.stop()
    poller.check()
    assert invocations

    failed = threading.Event()

    def invalid() -> None:
        failed.set()
        raise TaskCredentialContractError("wrong generation")

    poller = TaskCredentialPoller(poll=invalid, interval_seconds=0.01)
    poller.start()
    assert failed.wait(1)
    poller.stop()
    with pytest.raises(TaskCredentialContractError, match="wrong generation"):
        poller.check()
