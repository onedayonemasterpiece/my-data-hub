from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid5

import pytest
from jsonschema import Draft202012Validator, FormatChecker
from psycopg.pq import TransactionStatus

from my_data_hub.acceptance.lease_expiry_denial import (
    AtomicLeaseExpiryCompletionJournal,
    BrokeredH1ExpiredLeaseDenial,
    DirectoryAcceptanceObserverConnectionFactory,
    LeaseExpiryCompletionJournal,
    LeaseExpiryDenialBlocked,
    LeaseExpiryDenialCompletion,
)
from my_data_hub.acceptance.master_lifecycle import (
    MasterAcceptanceBinding,
    MasterAcceptanceRequest,
    command_for,
)


def command():  # type: ignore[no-untyped-def]
    operation_id = UUID("11111111-1111-4111-8111-111111111111")
    request = MasterAcceptanceRequest(
        task_id=UUID("22222222-2222-4222-8222-222222222222"),
        scenario="FM10",
        idempotency_key="fm10-fixed-lease-expiry-denial",
        source_revision="a" * 40,
        target_operation_id=operation_id,
    )
    return command_for(
        request,
        MasterAcceptanceBinding(
            operation_id=operation_id,
            run_id=UUID("33333333-3333-4333-8333-333333333333"),
            attempt_id=UUID("44444444-4444-4444-8444-444444444444"),
            service_instance_id="postgres-master:fm10",
            master_instance_id=UUID("55555555-5555-4555-8555-555555555555"),
            epoch=7,
        ),
    )


class Denied(Exception):
    def __init__(self, sqlstate: str = "55000") -> None:
        self.sqlstate = sqlstate
        super().__init__("write rejected by epoch lease gate")


class Result:
    def __init__(self, row: Any = None, *, rowcount: int = -1) -> None:
        self.row = row
        self.rowcount = rowcount

    def fetchone(self):  # type: ignore[no-untyped-def]
        return self.row


@dataclass
class OperatorConnection:
    exact_command: Any
    denial_sqlstate: str = "55000"
    denial_inerror: bool = True
    queries: list[str] = field(default_factory=list)
    rollbacks: int = 0
    commits: int = 0

    def __post_init__(self) -> None:
        self.info = SimpleNamespace(transaction_status=TransactionStatus.IDLE)

    def __enter__(self):  # type: ignore[no-untyped-def]
        return self

    def __exit__(self, *_args):  # type: ignore[no-untyped-def]
        return False

    def execute(self, query: str):  # type: ignore[no-untyped-def]
        self.queries.append(query)
        if "FROM pg_roles" in query:
            assert "mdh_role_admin" not in query
            return Result((False, False, False, False, False, True, False))
        if "lease_until>clock_timestamp" in query:
            binding = self.exact_command.binding
            return Result((binding.epoch, str(binding.master_instance_id), "open", True, 59))
        if "lease_until<=clock_timestamp" in query:
            binding = self.exact_command.binding
            return Result((True, binding.epoch, str(binding.master_instance_id)))
        if query.startswith("INSERT INTO hub.project"):
            return Result(rowcount=1)
        if query in {
            "SET CONSTRAINTS mdh_epoch_write_guard IMMEDIATE",
            "SELECT master_control.assert_session_write_epoch()",
        }:
            self.info.transaction_status = (
                TransactionStatus.INERROR if self.denial_inerror else TransactionStatus.INTRANS
            )
            raise Denied(self.denial_sqlstate)
        return Result()

    def commit(self) -> None:
        self.commits += 1
        self.info.transaction_status = TransactionStatus.IDLE

    def rollback(self) -> None:
        self.rollbacks += 1
        self.info.transaction_status = TransactionStatus.IDLE


@dataclass
class ObserverConnection:
    states: list[tuple[int, int, int, int, int]]
    queries: list[str] = field(default_factory=list)
    rollbacks: int = 0

    def __enter__(self):  # type: ignore[no-untyped-def]
        return self

    def __exit__(self, *_args):  # type: ignore[no-untyped-def]
        return False

    def execute(self, query: str):  # type: ignore[no-untyped-def]
        self.queries.append(query)
        if "FROM pg_roles" in query:
            assert "mdh_role_admin" not in query
            return Result((False, False, False, False, False, True, False))
        if "FROM hub.canonical_state" in query:
            return Result(self.states.pop(0))
        raise AssertionError(f"unexpected observer query: {query}")

    def rollback(self) -> None:
        self.rollbacks += 1


@dataclass
class Factory:
    connection: Any
    opens: list[MasterAcceptanceBinding] = field(default_factory=list)

    def open(self, binding: MasterAcceptanceBinding):  # type: ignore[no-untyped-def]
        self.opens.append(binding)
        return self.connection


@dataclass
class Renewal:
    commands: list[Any] = field(default_factory=list)

    def suspend_exact_renewal(self, exact_command) -> None:  # type: ignore[no-untyped-def]
        self.commands.append(exact_command)


@dataclass
class Journal(LeaseExpiryCompletionJournal):
    receipt: LeaseExpiryDenialCompletion | None = None
    puts: int = 0

    def load(self, command_id: UUID) -> LeaseExpiryDenialCompletion | None:
        if self.receipt is not None:
            assert self.receipt.command_id == command_id
        return self.receipt

    def put_if_absent(
        self, completion: LeaseExpiryDenialCompletion
    ) -> LeaseExpiryDenialCompletion:
        self.puts += 1
        if self.receipt is None:
            self.receipt = completion
        return self.receipt


@dataclass
class FakeWait:
    now_ns: int = 0
    sleeps: list[float] = field(default_factory=list)

    def monotonic_ns(self) -> int:
        return self.now_ns

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now_ns += int(seconds * 1_000_000_000)


def harness(
    *,
    states: list[tuple[int, int, int, int, int]] | None = None,
    denial_sqlstate: str = "55000",
    denial_inerror: bool = True,
    journal: Journal | None = None,
):  # type: ignore[no-untyped-def]
    exact_command = command()
    operator = OperatorConnection(
        exact_command,
        denial_sqlstate=denial_sqlstate,
        denial_inerror=denial_inerror,
    )
    observer = ObserverConnection(states or [(12, 3, 4, 5, 6), (12, 3, 4, 5, 6)])
    operator_factory = Factory(operator)
    observer_factory = Factory(observer)
    renewal = Renewal()
    durable = journal or Journal()
    wait = FakeWait()
    adapter = BrokeredH1ExpiredLeaseDenial(
        operator_connections=operator_factory,
        observer_connections=observer_factory,
        renewal=renewal,
        journal=durable,
        wait=wait,
    )
    return exact_command, adapter, operator, observer, renewal, durable, wait


def test_fm10_fixed_probe_forces_deferred_and_immediate_55000_then_rolls_back() -> None:
    exact_command, adapter, operator, observer, renewal, journal, wait = harness()
    evidence = adapter.prove_expired_lease_denial(exact_command)

    assert evidence.denial_code == "MDH_EPOCH_LEASE_EXPIRED"
    assert evidence.transaction_state == "rollback_only"
    assert evidence.observed_wait_seconds == 60
    assert evidence.canonical_revision_before == evidence.canonical_revision_after == 12
    assert renewal.commands == [exact_command]
    assert wait.sleeps == [60.0]
    assert journal.receipt is not None and journal.puts == 1
    assert journal.receipt.state_before == journal.receipt.state_after
    assert journal.receipt.receipt_sha256 == journal.receipt.receipt_sha256
    assert operator.queries.index("SET CONSTRAINTS ALL DEFERRED") < operator.queries.index(
        "SET CONSTRAINTS mdh_epoch_write_guard IMMEDIATE"
    )
    assert "SET LOCAL idle_in_transaction_session_timeout = '905s'" in operator.queries
    assert operator.queries.count("SELECT master_control.assert_session_write_epoch()") == 1
    staged = next(query for query in operator.queries if query.startswith("INSERT INTO hub.project"))
    assert "__mdh_fm10_lease_denial_probe__" in staged
    assert "%s" not in staged and ";" not in staged
    assert operator.rollbacks >= 2 and observer.rollbacks == 2


def test_completed_receipt_replays_without_opening_sessions_or_reissuing_dml() -> None:
    exact_command, first, *_rest = harness()
    first_evidence = first.prove_expired_lease_denial(exact_command)
    journal = _rest[-2]

    exact_command, replay, operator, observer, renewal, same_journal, wait = harness(
        journal=journal
    )
    replay_evidence = replay.prove_expired_lease_denial(exact_command)

    assert replay_evidence == first_evidence
    assert operator.queries == [] and observer.queries == []
    assert renewal.commands == [] and wait.sleeps == []
    assert same_journal.puts == 1


@pytest.mark.parametrize(
    ("states", "sqlstate", "inerror", "expected"),
    [
        ([(12, 3, 4, 5, 6), (12, 4, 4, 5, 6)], "55000", True, "FM10_ROLLBACK_STATE_CHANGED"),
        (None, "42501", True, "FM10_H1_ROLLBACK_DENIAL_NOT_OBSERVED"),
        (None, "55000", False, "FM10_H1_ROLLBACK_DENIAL_NOT_OBSERVED"),
    ],
)
def test_fm10_fails_closed_on_state_change_wrong_sqlstate_or_non_inerror(
    states, sqlstate: str, inerror: bool, expected: str  # type: ignore[no-untyped-def]
) -> None:
    exact_command, adapter, _operator, _observer, _renewal, journal, _wait = harness(
        states=states,
        denial_sqlstate=sqlstate,
        denial_inerror=inerror,
    )
    with pytest.raises(LeaseExpiryDenialBlocked, match=expected):
        adapter.prove_expired_lease_denial(exact_command)
    assert journal.receipt is None


def test_completion_journal_conflict_is_rejected_before_any_session() -> None:
    exact_command, first, *_rest = harness()
    first.prove_expired_lease_denial(exact_command)
    journal = _rest[-2]
    assert journal.receipt is not None
    journal.receipt = journal.receipt.model_copy(update={"command_sha256": "f" * 64})

    exact_command, replay, operator, observer, renewal, _journal, _wait = harness(journal=journal)
    with pytest.raises(LeaseExpiryDenialBlocked, match="FM10_COMPLETION_JOURNAL_CONFLICT"):
        replay.prove_expired_lease_denial(exact_command)
    assert operator.queries == [] and observer.queries == [] and renewal.commands == []


def test_fm10_completion_schema_validates_exact_metadata_only_receipt() -> None:
    exact_command, adapter, *_rest = harness()
    adapter.prove_expired_lease_denial(exact_command)
    journal = _rest[-2]
    assert journal.receipt is not None

    root = Path(__file__).resolve().parents[2]
    schema = json.loads(
        (root / "schemas/acceptance/lease-expiry-denial-completion.v1.schema.json").read_text()
    )
    Draft202012Validator.check_schema(schema)
    Draft202012Validator(schema, format_checker=FormatChecker()).validate(
        journal.receipt.model_dump(mode="json")
    )
    serialized = journal.receipt.model_dump_json()
    assert "postgresql://" not in serialized
    assert "password" not in serialized
    assert "rows\"" in serialized  # aggregate counts only
    assert "row_payload" not in serialized and "sql\"" not in serialized


def test_fm10_operation_and_receipt_hashes_are_stable() -> None:
    exact_command, first, *_ = harness()
    first_evidence = first.prove_expired_lease_denial(exact_command)
    exact_command, second, *_ = harness()
    second_evidence = second.prove_expired_lease_denial(exact_command)
    assert second_evidence.operator_operation_id == first_evidence.operator_operation_id
    assert second_evidence.operator_receipt_sha256 == first_evidence.operator_receipt_sha256
    assert first_evidence.operator_operation_id == uuid5(
        NAMESPACE_URL, f"fm10-h1-denial:{exact_command.task_id}"
    )


def test_atomic_completion_journal_is_private_create_once_and_parse_checked(tmp_path) -> None:  # type: ignore[no-untyped-def]
    exact_command, adapter, *_rest = harness()
    adapter.prove_expired_lease_denial(exact_command)
    memory_journal = _rest[-2]
    assert memory_journal.receipt is not None

    root = tmp_path / "fm10"
    journal = AtomicLeaseExpiryCompletionJournal(root)
    stored = journal.put_if_absent(memory_journal.receipt)
    path = root / f"{exact_command.command_id}.json"
    assert root.stat().st_mode & 0o077 == 0
    assert path.stat().st_mode & 0o077 == 0
    assert journal.load(exact_command.command_id) == stored
    assert journal.put_if_absent(memory_journal.receipt).receipt_sha256 == stored.receipt_sha256

    path.write_text("{}")
    path.chmod(0o600)
    with pytest.raises(LeaseExpiryDenialBlocked, match="FM10_COMPLETION_FILE_INVALID"):
        journal.load(exact_command.command_id)


def test_directory_observer_factory_loads_exact_reader_envelope_at_open(monkeypatch) -> None:
    exact_command = command()

    class Source:
        request = None

        def load(self, request):  # type: ignore[no-untyped-def]
            self.request = request
            return SimpleNamespace(
                database_url=(
                    "postgresql://reader:secret@postgres-master.internal:15432/postgres"
                    "?sslmode=verify-full&sslrootcert=/state/master-tls/ca.pem&connect_timeout=5"
                )
            )

    source = Source()
    sentinel = object()
    monkeypatch.setattr("psycopg.connect", lambda *_args, **_kwargs: sentinel)
    factory = DirectoryAcceptanceObserverConnectionFactory(source)
    assert factory.open(exact_command.binding) is sentinel
    assert source.request is not None
    assert source.request.role == "reader"
    assert source.request.master_instance_id == str(exact_command.binding.master_instance_id)
    assert source.request.epoch == exact_command.binding.epoch
    assert source.request.limits.max_rows == 1
    assert source.request.limits.timeout_ms == 5_000
    assert source.request.limits.max_bytes == 16 * 1024
    assert not hasattr(factory, "database_url")
