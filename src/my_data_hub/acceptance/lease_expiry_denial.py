"""Concrete, fixed FM10 PostgreSQL lease-expiry denial proof.

The adapter in this module is deliberately narrower than a generic SQL
executor.  It opens two already brokered, epoch-bound sessions: the restricted
H1 operator login performs one source-defined probe and the read-only observer
compares aggregate state.  Request data can never select SQL or parameters.

The operator transaction is staged while the lease is valid, renewal is then
suspended for the exact acceptance command, and PostgreSQL's deferred epoch
guard is forced after real lease expiry.  A second transaction proves that the
immediate guard also rejects the expired session.  Both failures must be
SQLSTATE 55000 and leave the connection INERROR before an explicit rollback.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import time
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Protocol
from uuid import NAMESPACE_URL, UUID, uuid5

from pydantic import BaseModel, ConfigDict, Field, model_validator

from my_data_hub.hashing import canonical_json_bytes

from .master_lifecycle import (
    LeaseExpiryEvidence,
    MasterAcceptanceBinding,
    MasterAcceptanceCommand,
    MasterAcceptanceCommandKind,
    MasterLifecycleAcceptanceError,
)

LEASE_EXPIRY_COMPLETION_SCHEMA = "my-data-hub-fm10-lease-expiry-completion.v1"
LEASE_EXPIRY_DENIAL_CODE = "MDH_EPOCH_LEASE_EXPIRED"
MIN_LEASE_EXPIRY_WAIT_SECONDS = 60
MAX_LEASE_EXPIRY_WAIT_SECONDS = 900
MAX_LEASE_EXPIRY_COMPLETION_BYTES = 64 * 1024

# This row exists only inside the probe transaction and is always rolled back.
# A source-owned literal prevents the acceptance request from becoming a SQL or
# parameter channel.  Concurrent FM10 tasks are already excluded by the
# owner-bound scenario claim.
_STAGE_DEFERRED_GUARD_SQL = (
    "INSERT INTO hub.project(project_id,slug,name,status,metadata) VALUES ("
    "'00000000-0000-4000-8000-000000000f10'::uuid,"
    "'__mdh_fm10_lease_denial_probe__',"
    "'FM10 lease expiry denial probe','paused',"
    "'{\"acceptance\":\"FM10\"}'::jsonb)"
)
_SET_PROBE_IDLE_TIMEOUT_SQL = "SET LOCAL idle_in_transaction_session_timeout = '905s'"
_FORCE_DEFERRED_GUARD_SQL = "SET CONSTRAINTS mdh_epoch_write_guard IMMEDIATE"
_FORCE_IMMEDIATE_GUARD_SQL = "SELECT master_control.assert_session_write_epoch()"

_OPERATOR_ROLE_SQL = (
    "SELECT r.rolsuper,r.rolcreatedb,r.rolcreaterole,r.rolreplication,r.rolbypassrls,"
    "pg_has_role(session_user,'mdh_mcp_editor','member'),"
    "pg_has_role(session_user,'mdh_owner','member') "
    "FROM pg_roles r WHERE r.rolname=session_user"
)
_OBSERVER_ROLE_SQL = (
    "SELECT r.rolsuper,r.rolcreatedb,r.rolcreaterole,r.rolreplication,r.rolbypassrls,"
    "pg_has_role(session_user,'mdh_mcp_reader','member'),"
    "pg_has_role(session_user,'mdh_owner','member') "
    "FROM pg_roles r WHERE r.rolname=session_user"
)
_ACTIVE_BINDING_SQL = (
    "SELECT current_epoch,master_instance_id::text,gate_state,"
    "lease_until>clock_timestamp(),"
    "greatest(0,ceil(extract(epoch FROM (lease_until-clock_timestamp()))))::bigint "
    "FROM master_control.epoch_state WHERE singleton=true"
)
_EXPIRED_BINDING_SQL = (
    "SELECT lease_until<=clock_timestamp(),current_epoch,master_instance_id::text "
    "FROM master_control.epoch_state WHERE singleton=true"
)
_AGGREGATE_STATE_SQL = (
    "SELECT c.canonical_revision,"
    "(SELECT count(*)::bigint FROM hub.project),"
    "(SELECT count(*)::bigint FROM hub.content_item),"
    "(SELECT count(*)::bigint FROM sync.external_outbox),"
    "(SELECT count(*)::bigint FROM sync.audit_event) "
    "FROM hub.canonical_state c WHERE c.singleton=true"
)


class LeaseExpiryDenialBlocked(MasterLifecycleAcceptanceError):
    """A fixed FM10 prerequisite or database assertion was not proven."""

    def __init__(self, code: str) -> None:
        if not code or code.upper() != code or not code.replace("_", "").isalnum():
            raise ValueError("FM10 blocker code is invalid")
        self.code = code
        super().__init__(code)


class LeaseExpiryRenewalPort(Protocol):
    """Durably suspend renewals for the exact task/run/attempt/master/epoch."""

    def suspend_exact_renewal(self, command: MasterAcceptanceCommand) -> None: ...


class FixedEpochConnectionFactory(Protocol):
    """Open a broker-validated session for exactly one command binding."""

    def open(self, binding: MasterAcceptanceBinding) -> Any: ...


class MonotonicWait(Protocol):
    """Internal evidence clock; it is never supplied by an acceptance request."""

    def monotonic_ns(self) -> int: ...

    def sleep(self, seconds: float) -> None: ...


@dataclass(frozen=True, slots=True)
class SystemMonotonicWait:
    def monotonic_ns(self) -> int:
        return time.monotonic_ns()

    def sleep(self, seconds: float) -> None:
        time.sleep(seconds)


class LeaseExpiryStateFingerprint(BaseModel):
    """Aggregate-only proof; no canonical row or payload leaves PostgreSQL."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    canonical_revision: int = Field(ge=0)
    project_rows: int = Field(ge=0)
    content_item_rows: int = Field(ge=0)
    outbox_rows: int = Field(ge=0)
    audit_rows: int = Field(ge=0)


class LeaseExpiryDenialCompletion(BaseModel):
    """Durable, metadata-only completion used for response-loss reconciliation."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["my-data-hub-fm10-lease-expiry-completion.v1"] = (
        LEASE_EXPIRY_COMPLETION_SCHEMA
    )
    command_id: UUID
    command_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    task_id: UUID
    binding: MasterAcceptanceBinding
    operator_operation_id: UUID
    operation_receipt_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    deferred_guard_sqlstate: Literal["55000"]
    deferred_transaction_state: Literal["rollback_only"]
    immediate_guard_sqlstate: Literal["55000"]
    immediate_transaction_state: Literal["rollback_only"]
    state_before: LeaseExpiryStateFingerprint
    state_after: LeaseExpiryStateFingerprint
    evidence: LeaseExpiryEvidence

    @model_validator(mode="after")
    def exact_denial(self) -> LeaseExpiryDenialCompletion:
        if self.state_before != self.state_after:
            raise ValueError("FM10 completion changed canonical, row, outbox, or audit state")
        if (
            self.evidence.operator_operation_id != self.operator_operation_id
            or self.evidence.operator_receipt_sha256 != self.operation_receipt_sha256
            or self.evidence.denial_code != LEASE_EXPIRY_DENIAL_CODE
            or self.evidence.canonical_revision_before != self.state_before.canonical_revision
            or self.evidence.canonical_revision_after != self.state_after.canonical_revision
        ):
            raise ValueError("FM10 evidence differs from the exact operator denial receipt")
        if len(canonical_json_bytes(self.model_dump(mode="json"))) > MAX_LEASE_EXPIRY_COMPLETION_BYTES:
            raise ValueError("FM10 completion exceeds 64 KiB")
        return self

    @property
    def receipt_sha256(self) -> str:
        return hashlib.sha256(canonical_json_bytes(self.model_dump(mode="json"))).hexdigest()


class LeaseExpiryCompletionJournal(Protocol):
    """CAS journal. Implementations persist receipt metadata, never DSNs or rows."""

    def load(self, command_id: UUID) -> LeaseExpiryDenialCompletion | None: ...

    def put_if_absent(
        self, completion: LeaseExpiryDenialCompletion
    ) -> LeaseExpiryDenialCompletion: ...


@dataclass(frozen=True, slots=True)
class AtomicLeaseExpiryCompletionJournal:
    """Mode-0600, create-once receipt journal for a control-host state volume."""

    root: Path

    def load(self, command_id: UUID) -> LeaseExpiryDenialCompletion | None:
        self._require_root()
        path = self._path(command_id)
        if not path.exists():
            return None
        if path.is_symlink() or not path.is_file() or path.stat().st_mode & 0o077:
            raise LeaseExpiryDenialBlocked("FM10_COMPLETION_FILE_NOT_PRIVATE")
        size = path.stat().st_size
        if size < 2 or size > MAX_LEASE_EXPIRY_COMPLETION_BYTES:
            raise LeaseExpiryDenialBlocked("FM10_COMPLETION_FILE_INVALID")
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
            completion = LeaseExpiryDenialCompletion.model_validate(value)
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            raise LeaseExpiryDenialBlocked("FM10_COMPLETION_FILE_INVALID") from exc
        if completion.command_id != command_id:
            raise LeaseExpiryDenialBlocked("FM10_COMPLETION_JOURNAL_CONFLICT")
        return completion

    def put_if_absent(
        self, completion: LeaseExpiryDenialCompletion
    ) -> LeaseExpiryDenialCompletion:
        self._require_root()
        prior = self.load(completion.command_id)
        if prior is not None:
            return prior
        payload = canonical_json_bytes(completion.model_dump(mode="json")) + b"\n"
        if len(payload) > MAX_LEASE_EXPIRY_COMPLETION_BYTES:
            raise LeaseExpiryDenialBlocked("FM10_COMPLETION_FILE_INVALID")
        path = self._path(completion.command_id)
        temporary = self.root / f".{path.name}.tmp-{os.getpid()}-{time.monotonic_ns()}"
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        descriptor = os.open(temporary, flags, 0o600)
        try:
            with os.fdopen(descriptor, "wb", closefd=True) as stream:
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
            temporary.chmod(0o600)
            with suppress(FileExistsError):
                os.link(temporary, path)
            directory = os.open(self.root, os.O_RDONLY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        finally:
            temporary.unlink(missing_ok=True)
        stored = self.load(completion.command_id)
        if stored is None:
            raise LeaseExpiryDenialBlocked("FM10_COMPLETION_FILE_INVALID")
        return stored

    def _require_root(self) -> None:
        if not self.root.exists():
            self.root.mkdir(parents=True, mode=0o700)
        if self.root.is_symlink() or not self.root.is_dir() or self.root.stat().st_mode & 0o077:
            raise LeaseExpiryDenialBlocked("FM10_COMPLETION_DIRECTORY_NOT_PRIVATE")

    def _path(self, command_id: UUID) -> Path:
        return self.root / f"{command_id}.json"


@dataclass(frozen=True, slots=True)
class DirectoryAcceptanceObserverConnectionFactory:
    """Load the exact reader envelope without retaining its DSN."""

    source: Any

    def open(self, binding: MasterAcceptanceBinding) -> Any:
        from my_data_hub.mcp.contracts import ExecutionLimits, SessionRequest
        from my_data_hub.mcp.oauth import AccessIdentity

        principal = AccessIdentity(
            subject="master-acceptance-fm10-observer",
            client_id="control-master-acceptance",
            scopes=frozenset({"acceptance:operate"}),
            audience="local-control",
            token_id="owner-host-claim",
            expires_at=2**63 - 1,
            issuer="local-control",
            issued_at=0,
            resource="local-control",
        )
        credential = self.source.load(
            SessionRequest(
                principal=principal,
                master_instance_id=str(binding.master_instance_id),
                epoch=binding.epoch,
                role="reader",
                tool="acceptance.scenario.status",
                limits=ExecutionLimits(
                    max_rows=1,
                    timeout_ms=5_000,
                    max_bytes=16 * 1024,
                ),
            )
        )
        import psycopg

        return psycopg.connect(credential.database_url, connect_timeout=5)


@dataclass(frozen=True, slots=True)
class BrokeredH1ExpiredLeaseDenial:
    """Fixed H1 adapter implementing ``prove_expired_lease_denial``.

    ``operator_connections`` and ``observer_connections`` are expected to load
    short-lived role-bound envelopes from the production session broker.  This
    object never receives, stores, logs, or serializes their DSNs or tokens.
    """

    operator_connections: FixedEpochConnectionFactory
    observer_connections: FixedEpochConnectionFactory
    renewal: LeaseExpiryRenewalPort
    journal: LeaseExpiryCompletionJournal
    wait: MonotonicWait = SystemMonotonicWait()

    def prove_expired_lease_denial(
        self, command: MasterAcceptanceCommand
    ) -> LeaseExpiryEvidence:
        self._require_command(command)
        prior = self.journal.load(command.command_id)
        if prior is not None:
            self._require_exact_completion(command, prior)
            return prior.evidence

        operation_id = uuid5(NAMESPACE_URL, f"fm10-h1-denial:{command.task_id}")
        started_ns = self.wait.monotonic_ns()
        with (
            self.operator_connections.open(command.binding) as operator,
            self.observer_connections.open(command.binding) as observer,
        ):
            self._require_role(operator, _OPERATOR_ROLE_SQL, "FM10_OPERATOR_ROLE_NOT_RESTRICTED")
            self._require_role(observer, _OBSERVER_ROLE_SQL, "FM10_OBSERVER_ROLE_NOT_RESTRICTED")
            self._require_active_binding(operator, command.binding)
            state_before = self._read_state(observer)
            observer.rollback()
            operator.commit()

            # Stage a fixed bounded change while the lease is still valid.  It
            # remains invisible outside this transaction and must later roll
            # back when the deferred epoch constraint is forced.
            operator.execute("SET TRANSACTION READ WRITE")
            # The restricted role normally has a 30-second idle transaction
            # cap. FM10 alone needs to retain this rollback-only probe across
            # the source-defined <=900-second lease window.
            operator.execute(_SET_PROBE_IDLE_TIMEOUT_SQL)
            operator.execute("SET CONSTRAINTS ALL DEFERRED")
            staged = operator.execute(_STAGE_DEFERRED_GUARD_SQL)
            if int(getattr(staged, "rowcount", -1)) != 1:
                operator.rollback()
                raise LeaseExpiryDenialBlocked("FM10_FIXED_PROBE_ROW_NOT_STAGED")

            # The concrete control-ledger port records the directive before it
            # returns and waits for the exact runtime ACK (heartbeat, local
            # DatabaseGate, and tunnel/session renewal all suspended).
            self.renewal.suspend_exact_renewal(command)
            active = operator.execute(_ACTIVE_BINDING_SQL).fetchone()
            if not self._binding_matches(active, command.binding, expect_active=True):
                operator.rollback()
                raise LeaseExpiryDenialBlocked("FM10_ACTIVE_BINDING_MISMATCH_AFTER_ACK")
            remaining_seconds = int(active[4])
            wait_seconds = max(MIN_LEASE_EXPIRY_WAIT_SECONDS, remaining_seconds + 1)
            if wait_seconds > MAX_LEASE_EXPIRY_WAIT_SECONDS:
                operator.rollback()
                raise LeaseExpiryDenialBlocked("FM10_LEASE_EXPIRY_EXCEEDS_BOUND")
            self.wait.sleep(float(wait_seconds))

            expired = operator.execute(_EXPIRED_BINDING_SQL).fetchone()
            if not self._binding_matches(expired, command.binding, expect_active=False):
                operator.rollback()
                raise LeaseExpiryDenialBlocked("FM10_REAL_LEASE_EXPIRY_NOT_OBSERVED")

            deferred_state = self._force_denial(operator, _FORCE_DEFERRED_GUARD_SQL)
            operator.rollback()
            immediate_state = self._force_denial(operator, _FORCE_IMMEDIATE_GUARD_SQL)
            operator.rollback()

            state_after = self._read_state(observer)
            observer.rollback()

        finished_ns = self.wait.monotonic_ns()
        observed_wait_seconds = math.floor((finished_ns - started_ns) / 1_000_000_000)
        if not MIN_LEASE_EXPIRY_WAIT_SECONDS <= observed_wait_seconds <= MAX_LEASE_EXPIRY_WAIT_SECONDS:
            raise LeaseExpiryDenialBlocked("FM10_MONOTONIC_WAIT_OUTSIDE_BOUND")
        if state_before != state_after:
            raise LeaseExpiryDenialBlocked("FM10_ROLLBACK_STATE_CHANGED")

        operation_receipt = {
            "schema_version": "my-data-hub-fm10-h1-denial.v1",
            "command_id": str(command.command_id),
            "command_sha256": command.command_sha256,
            "task_id": str(command.task_id),
            "binding": command.binding.model_dump(mode="json"),
            "operator_operation_id": str(operation_id),
            "deferred_guard_sqlstate": "55000",
            "deferred_transaction_state": deferred_state,
            "immediate_guard_sqlstate": "55000",
            "immediate_transaction_state": immediate_state,
            "state_before": state_before.model_dump(mode="json"),
            "state_after": state_after.model_dump(mode="json"),
        }
        operation_receipt_sha256 = hashlib.sha256(
            canonical_json_bytes(operation_receipt)
        ).hexdigest()
        evidence = LeaseExpiryEvidence(
            kind="LEASE_EXPIRY_DENIAL",
            observed_wait_seconds=observed_wait_seconds,
            lease_expired=True,
            credentials_invalidated=True,
            bounded_operator_dml_denied=True,
            transaction_state="rollback_only",
            operator_operation_id=operation_id,
            operator_receipt_sha256=operation_receipt_sha256,
            denial_code=LEASE_EXPIRY_DENIAL_CODE,
            canonical_revision_before=state_before.canonical_revision,
            canonical_revision_after=state_after.canonical_revision,
        )
        candidate = LeaseExpiryDenialCompletion(
            command_id=command.command_id,
            command_sha256=command.command_sha256,
            task_id=command.task_id,
            binding=command.binding,
            operator_operation_id=operation_id,
            operation_receipt_sha256=operation_receipt_sha256,
            deferred_guard_sqlstate="55000",
            deferred_transaction_state="rollback_only",
            immediate_guard_sqlstate="55000",
            immediate_transaction_state="rollback_only",
            state_before=state_before,
            state_after=state_after,
            evidence=evidence,
        )
        completed = self.journal.put_if_absent(candidate)
        self._require_exact_completion(command, completed)
        if completed.receipt_sha256 != candidate.receipt_sha256:
            raise LeaseExpiryDenialBlocked("FM10_COMPLETION_JOURNAL_CONFLICT")
        return completed.evidence

    @staticmethod
    def _require_command(command: MasterAcceptanceCommand) -> None:
        if command.command_kind is not MasterAcceptanceCommandKind.LEASE_EXPIRY_DENIAL:
            raise MasterLifecycleAcceptanceError("FM10 adapter received another fixed command")

    @staticmethod
    def _require_exact_completion(
        command: MasterAcceptanceCommand, completion: LeaseExpiryDenialCompletion
    ) -> None:
        if (
            completion.command_id != command.command_id
            or completion.command_sha256 != command.command_sha256
            or completion.task_id != command.task_id
            or completion.binding != command.binding
        ):
            raise LeaseExpiryDenialBlocked("FM10_COMPLETION_JOURNAL_CONFLICT")

    @staticmethod
    def _require_role(connection: Any, query: str, code: str) -> None:
        row = connection.execute(query).fetchone()
        if (
            row is None
            or any(bool(row[index]) for index in range(5))
            or not bool(row[5])
            or bool(row[6])
        ):
            connection.rollback()
            raise LeaseExpiryDenialBlocked(code)

    @staticmethod
    def _binding_matches(
        row: Any, binding: MasterAcceptanceBinding, *, expect_active: bool
    ) -> bool:
        if row is None:
            return False
        if expect_active:
            return (
                int(row[0]) == binding.epoch
                and str(row[1]) == str(binding.master_instance_id)
                and str(row[2]) == "open"
                and row[3] is True
                and int(row[4]) >= 0
            )
        return (
            row[0] is True
            and int(row[1]) == binding.epoch
            and str(row[2]) == str(binding.master_instance_id)
        )

    @classmethod
    def _require_active_binding(
        cls, connection: Any, binding: MasterAcceptanceBinding
    ) -> None:
        row = connection.execute(_ACTIVE_BINDING_SQL).fetchone()
        if not cls._binding_matches(row, binding, expect_active=True):
            connection.rollback()
            raise LeaseExpiryDenialBlocked("FM10_ACTIVE_BINDING_MISMATCH")

    @staticmethod
    def _read_state(connection: Any) -> LeaseExpiryStateFingerprint:
        row = connection.execute(_AGGREGATE_STATE_SQL).fetchone()
        if row is None or len(row) != 5:
            connection.rollback()
            raise LeaseExpiryDenialBlocked("FM10_AGGREGATE_READBACK_MISSING")
        return LeaseExpiryStateFingerprint(
            canonical_revision=int(row[0]),
            project_rows=int(row[1]),
            content_item_rows=int(row[2]),
            outbox_rows=int(row[3]),
            audit_rows=int(row[4]),
        )

    @staticmethod
    def _force_denial(connection: Any, query: str) -> Literal["rollback_only"]:
        from psycopg.pq import TransactionStatus

        sqlstate: str | None = None
        try:
            connection.execute(query)
        except Exception as exc:  # psycopg subclasses vary by server release.
            sqlstate = getattr(exc, "sqlstate", None)
        if (
            sqlstate != "55000"
            or connection.info.transaction_status != TransactionStatus.INERROR
        ):
            connection.rollback()
            raise LeaseExpiryDenialBlocked("FM10_H1_ROLLBACK_DENIAL_NOT_OBSERVED")
        return "rollback_only"
