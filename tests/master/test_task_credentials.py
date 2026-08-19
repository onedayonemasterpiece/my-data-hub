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


def test_reconciler_issues_region_credential_and_refreshes_before_expiry() -> None:
    created: list[dict[str, object]] = []
    dropped: list[str] = []
    revoked: list[tuple[object, ...]] = []
    registrations: list[dict[str, object]] = []
    provisioner = SimpleNamespace(
        create=lambda **kwargs: created.append(kwargs) or kwargs["principal"],
        drop=dropped.append,
    )
    gate = SimpleNamespace(revoke_credential=lambda *args: revoked.append(args))
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
    assert created[0]["expires_at"] == NOW + timedelta(minutes=4)
    assert len(registrations) == 1
    assert "opaque-secret" not in json.dumps(registrations)
    first = next(iter(reconciler.issued.values()))

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
    second = next(iter(reconciler.issued.values()))
    assert second.generation == 2 and second.credential_id != first.credential_id
    assert first.principal in dropped
    assert revoked and revoked[0][0] == first.credential_id


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
        gate=SimpleNamespace(revoke_credential=lambda *_args: None),
        lease_until=NOW + timedelta(minutes=5),
        register=lambda _body: pytest.fail("exact replay must not re-register"),
        now=NOW,
    )
    assert created == []


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
