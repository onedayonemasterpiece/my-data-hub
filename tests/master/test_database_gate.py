from __future__ import annotations

from types import SimpleNamespace
from uuid import UUID

from my_data_hub.master_runtime.contracts import MasterIdentity
from my_data_hub.master_runtime.database_gate import DatabaseGate


def test_register_task_credential_binding_uses_exact_authoritative_function() -> None:
    identity = MasterIdentity(
        UUID("44444444-4444-4444-8444-444444444444"), "master-run", 7
    )
    receipt = {
        "registered": True,
        "credential_id": "33333333-3333-4333-8333-333333333333",
        "principal": "mdh_e7_regiong1_deadbeef",
        "worker_kind": "region_talk",
        "task_run_id": "22222222-2222-4222-8222-222222222222",
        "generation": 1,
        "master_instance_id": str(identity.master_instance_id),
        "epoch": 7,
        "command_sha256": "b" * 64,
        "task_token_sha256": "a" * 64,
    }
    calls: list[tuple[str, tuple[object, ...]]] = []

    class Cursor:
        def __enter__(self) -> Cursor:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def execute(self, sql: str, parameters: tuple[object, ...]) -> None:
            calls.append((sql, parameters))

        def fetchone(self) -> tuple[dict[str, object]]:
            return (receipt,)

    connection = SimpleNamespace(
        cursor=lambda: Cursor(),
        commit=lambda: calls.append(("commit", ())),
        rollback=lambda: calls.append(("rollback", ())),
    )
    result = DatabaseGate(connection).register_task_credential_binding(
        credential_id=UUID(receipt["credential_id"]),
        principal=str(receipt["principal"]),
        worker_kind="region_talk",
        task_run_id=UUID(receipt["task_run_id"]),
        generation=1,
        identity=identity,
        command_sha256="b" * 64,
        task_token_sha256="a" * 64,
    )

    assert result == receipt
    assert calls[0] == (
        "SELECT master_control.register_task_credential_binding("
        "%s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (
            UUID(receipt["credential_id"]),
            receipt["principal"],
            "region_talk",
            UUID(receipt["task_run_id"]),
            1,
            identity.master_instance_id,
            7,
            "b" * 64,
            "a" * 64,
        ),
    )
    assert calls[-1] == ("commit", ())
