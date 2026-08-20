"""Workload-neutral, epoch-bound credentials for direct master task workers.

Only command metadata crosses the control plane before issuance.  The generated
password exists in memory long enough to create the PostgreSQL LOGIN and hand
the exact task-bound registration to the private worker-status publisher.  It
is never logged or returned by this module.
"""

from __future__ import annotations

import hashlib
import json
import re
import secrets
import threading
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Literal, Protocol
from urllib.parse import quote, urlencode
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

from my_data_hub.hashing import canonical_json_bytes

from .contracts import MasterIdentity, require_utc
from .credentials import CredentialProvisioner, LoginPolicy

TaskWorkerKind = Literal["embedding", "region_talk"]
TaskCredentialKey = tuple[TaskWorkerKind, UUID]
_SHA256 = re.compile(r"^[a-f0-9]{64}$")
_REASON = re.compile(r"^[a-z][a-z0-9_.-]{0,63}$")
_ROLE_BINDINGS: Mapping[TaskWorkerKind, tuple[str, str, str]] = {
    "embedding": ("mdh_embedding_worker", "embedding_worker", "embed"),
    "region_talk": ("mdh_region_talk_pipeline", "region_talk_pipeline", "region"),
}


class TaskCredentialContractError(RuntimeError):
    """A task command or registration did not preserve its exact binding."""


class TaskCredentialCommandClient(Protocol):
    def fetch(self) -> TaskCredentialBatch: ...

    def register(self, body: dict[str, object]) -> Mapping[str, object]: ...

    def acknowledge(self, batch: TaskCredentialBatch) -> None: ...

    def acknowledge_registrations(
        self, receipts: tuple[Mapping[str, object], ...]
    ) -> None: ...


def task_command_sha256(value: Mapping[str, object]) -> str:
    """Hash the complete issue command except its self-authenticating field."""

    body = dict(value)
    body.pop("command_sha256", None)
    return hashlib.sha256(canonical_json_bytes(body)).hexdigest()


class TaskCredentialCommand(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["my-data-hub-task-credential-command.v1"]
    worker_kind: TaskWorkerKind
    task_run_id: UUID
    epoch: int = Field(ge=1)
    generation: int = Field(ge=1)
    task_token_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    command_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")

    @model_validator(mode="after")
    def exact_hash(self) -> TaskCredentialCommand:
        if self.command_sha256 != task_command_sha256(
            self.model_dump(mode="json", exclude={"command_sha256"})
        ):
            raise ValueError("task credential command hash does not match its exact body")
        return self


class TaskCredentialRevocation(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["my-data-hub-task-credential-revocation.v1"]
    worker_kind: TaskWorkerKind
    task_run_id: UUID
    epoch: int = Field(ge=1)
    generation: int = Field(ge=1)
    task_token_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    command_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    credential_id: UUID
    reason: str = Field(pattern=r"^[a-z][a-z0-9_.-]{0,63}$")


class TaskCredentialBatch(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["my-data-hub-task-credential-batch.v1"] = (
        "my-data-hub-task-credential-batch.v1"
    )
    commands: tuple[TaskCredentialCommand, ...] = ()
    revocations: tuple[TaskCredentialRevocation, ...] = ()

    @model_validator(mode="after")
    def bounded_unique_batch(self) -> TaskCredentialBatch:
        if len(self.commands) + len(self.revocations) > 256:
            raise ValueError("task credential command batch exceeds 256 items")
        keys = [
            (item.worker_kind, item.task_run_id, item.generation)
            for item in self.commands
        ]
        if len(keys) != len(set(keys)):
            raise ValueError("task credential command batch contains duplicate generations")
        return self


class HttpTaskCredentialClient:
    """Bounded HTTP client for the runtime-owned task credential mailbox."""

    def __init__(self, *, base_url: str, run_secret: str) -> None:
        if not base_url.startswith("https://") or len(base_url) > 2048:
            raise ValueError("task credential endpoint must be bounded HTTPS")
        if not run_secret or len(run_secret) > 4096:
            raise ValueError("task credential runtime secret is invalid")
        self.base_url = base_url.rstrip("/")
        self._authorization = f"Bearer {run_secret}"

    def fetch(self) -> TaskCredentialBatch:
        request = urllib.request.Request(
            f"{self.base_url}/commands",
            headers={"Authorization": self._authorization},
        )
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                raw = response.read(64 * 1024 + 1)
        except urllib.error.HTTPError as exc:
            # During a rolling upgrade an older control plane may not expose
            # the generic mailbox yet.  Absence mints nothing and is safe;
            # every other HTTP failure remains visible to the ACTIVE master.
            if exc.code == 404:
                return TaskCredentialBatch()
            raise
        if len(raw) > 64 * 1024:
            raise TaskCredentialContractError("task credential command response is oversized")
        try:
            body = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise TaskCredentialContractError(
                "task credential command response is malformed"
            ) from exc
        return TaskCredentialBatch.model_validate(body)

    def register(self, body: dict[str, object]) -> Mapping[str, object]:
        encoded = canonical_json_bytes(body)
        if len(encoded) > 16 * 1024:
            raise TaskCredentialContractError("task credential registration is oversized")
        request = urllib.request.Request(
            self.base_url,
            data=encoded,
            headers={
                "Authorization": self._authorization,
                "Content-Type": "application/json",
            },
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=10) as response:
            raw = response.read(16 * 1024 + 1)
        if len(raw) > 16 * 1024:
            raise TaskCredentialContractError("task credential receipt is oversized")
        try:
            receipt = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise TaskCredentialContractError("task credential receipt is malformed") from exc
        if not isinstance(receipt, dict):
            raise TaskCredentialContractError("task credential receipt is not an object")
        return receipt

    def acknowledge(self, batch: TaskCredentialBatch) -> None:
        if not batch.revocations:
            return
        encoded = canonical_json_bytes(
            {
                "schema_version": "my-data-hub-task-credential-ack.v1",
                "revocations": [
                    item.model_dump(mode="json") for item in batch.revocations
                ],
            }
        )
        request = urllib.request.Request(
            f"{self.base_url}/acknowledgements",
            data=encoded,
            headers={
                "Authorization": self._authorization,
                "Content-Type": "application/json",
            },
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=10) as response:
            raw = response.read(4097)
        if len(raw) > 4096:
            raise TaskCredentialContractError(
                "task credential acknowledgement is oversized"
            )
        try:
            receipt = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise TaskCredentialContractError(
                "task credential acknowledgement is malformed"
            ) from exc
        if receipt != {"acknowledged": True, "count": len(batch.revocations)}:
            raise TaskCredentialContractError(
                "task credential acknowledgement was not exact"
            )

    def acknowledge_registrations(
        self, receipts: tuple[Mapping[str, object], ...]
    ) -> None:
        if not receipts:
            return
        encoded = canonical_json_bytes(
            {
                "schema_version": "my-data-hub-task-registration-ack.v1",
                "registrations": [dict(item) for item in receipts],
            }
        )
        request = urllib.request.Request(
            f"{self.base_url}/registration-acknowledgements",
            data=encoded,
            headers={
                "Authorization": self._authorization,
                "Content-Type": "application/json",
            },
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=10) as response:
            raw = response.read(4097)
        if len(raw) > 4096:
            raise TaskCredentialContractError(
                "task registration acknowledgement is oversized"
            )
        try:
            receipt = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise TaskCredentialContractError(
                "task registration acknowledgement is malformed"
            ) from exc
        if receipt != {"acknowledged": True, "count": len(receipts)}:
            raise TaskCredentialContractError(
                "task registration acknowledgement was not exact"
            )


@dataclass(frozen=True, slots=True)
class IssuedTaskCredential:
    worker_kind: TaskWorkerKind
    task_run_id: UUID
    epoch: int
    generation: int
    task_token_sha256: str
    command_sha256: str
    credential_id: UUID
    principal: str
    expires_at: datetime

    def __post_init__(self) -> None:
        require_utc(self.expires_at, "expires_at")
        if self.worker_kind not in _ROLE_BINDINGS:
            raise ValueError("issued task credential kind is not allowlisted")
        if self.epoch < 1 or self.generation < 1:
            raise ValueError("issued task credential epoch/generation must be positive")
        if not _SHA256.fullmatch(self.task_token_sha256) or not _SHA256.fullmatch(
            self.command_sha256
        ):
            raise ValueError("issued task credential hashes are invalid")


class TaskCredentialReconciler:
    """Issue, rotate, and revoke exact worker credentials for one ACTIVE epoch."""

    def __init__(
        self,
        *,
        identity: MasterIdentity,
        local_postgres_port: int,
        issued: dict[TaskCredentialKey, IssuedTaskCredential] | None = None,
        pending: dict[TaskCredentialKey, IssuedTaskCredential] | None = None,
    ) -> None:
        if not 1024 <= local_postgres_port <= 65535:
            raise ValueError("task credential PostgreSQL port is invalid")
        self.identity = identity
        self.local_postgres_port = local_postgres_port
        self.issued = issued if issued is not None else {}
        self.pending = pending if pending is not None else {}
        self._all_principals = {
            item.principal for item in (*self.issued.values(), *self.pending.values())
        }
        self._retired: set[
            tuple[TaskWorkerKind, UUID, int, str, str, UUID]
        ] = set()
        # Master-memory only.  It retains the exact generated password across
        # an HTTP response-loss retry; it is never serialized or logged.
        self._registration_replays: dict[
            TaskCredentialKey,
            tuple[IssuedTaskCredential, dict[str, object], bool, bool],
        ] = {}
        self._registration_acknowledgements: dict[
            TaskCredentialKey, dict[str, object]
        ] = {}

    def _drop(
        self,
        issued: IssuedTaskCredential,
        *,
        provisioner: CredentialProvisioner,
        gate: Any,
        reason: str,
    ) -> None:
        gate.revoke_credential(issued.credential_id, reason)
        provisioner.drop(issued.principal)

    def reconcile(
        self,
        *,
        batch: TaskCredentialBatch,
        provisioner: CredentialProvisioner,
        gate: Any,
        lease_until: datetime,
        register: Callable[[dict[str, object]], Mapping[str, object]],
        now: datetime,
    ) -> None:
        observed = require_utc(now, "now")
        active_until = require_utc(lease_until, "lease_until")

        # A replacement is allowed to overlap the current LOGIN only for its
        # own short credential lifetime.  If the worker never proves the new
        # tunnel/session, remove the unactivated replacement and retain the
        # current exact binding for a later, higher generation decision.
        for key, pending in tuple(self.pending.items()):
            if pending.expires_at <= observed:
                self._drop(
                    pending,
                    provisioner=provisioner,
                    gate=gate,
                    reason="task_credential_activation_expired",
                )
                self._retired.add(self._exact(pending))
                self.pending.pop(key, None)
        for key, (candidate, _body, _had_current, _database_bound) in tuple(
            self._registration_replays.items()
        ):
            if candidate.expires_at <= observed:
                self._drop(
                    candidate,
                    provisioner=provisioner,
                    gate=gate,
                    reason="task_credential_registration_expired",
                )
                self._retired.add(self._exact(candidate))
                self._registration_replays.pop(key, None)

        for revoke in batch.revocations:
            key: TaskCredentialKey = (revoke.worker_kind, revoke.task_run_id)
            issued = self.issued.get(key)
            pending = self.pending.get(key)
            requested = self._revocation_exact(revoke)
            if requested in self._retired:
                # The control mailbox intentionally replays an activation or
                # terminal revocation until it can safely retire private
                # metadata.  Exact replay must not target the promoted LOGIN.
                continue
            if issued is not None and self._exact(issued) == requested:
                if revoke.reason == "task_credential_rotated":
                    if (
                        pending is None
                        or pending.generation <= issued.generation
                        or pending.epoch != issued.epoch
                        or pending.task_token_sha256 != issued.task_token_sha256
                    ):
                        raise TaskCredentialContractError(
                            "task credential activation has no exact pending replacement"
                        )
                    self._drop(
                        issued,
                        provisioner=provisioner,
                        gate=gate,
                        reason=revoke.reason,
                    )
                    self._retired.add(self._exact(issued))
                    self.issued[key] = pending
                    self.pending.pop(key, None)
                    continue
                self._drop(
                    issued,
                    provisioner=provisioner,
                    gate=gate,
                    reason=revoke.reason,
                )
                self._retired.add(self._exact(issued))
                self.issued.pop(key, None)
                if pending is not None:
                    self._drop(
                        pending,
                        provisioner=provisioner,
                        gate=gate,
                        reason=revoke.reason,
                    )
                    self._retired.add(self._exact(pending))
                    self.pending.pop(key, None)
                continue
            if pending is not None and self._exact(pending) == requested:
                self._drop(
                    pending,
                    provisioner=provisioner,
                    gate=gate,
                    reason=revoke.reason,
                )
                self._retired.add(self._exact(pending))
                self.pending.pop(key, None)
                continue
            else:
                raise TaskCredentialContractError(
                    "task credential revocation is not bound to an issued credential"
                )

        for command in batch.commands:
            if command.epoch != self.identity.epoch:
                raise TaskCredentialContractError(
                    "task credential command targets a different master epoch"
                )
            key = (command.worker_kind, command.task_run_id)
            replay = self._registration_replays.get(key)
            if replay is not None:
                candidate, registration, had_current, database_bound = replay
                if (
                    command.generation != candidate.generation
                    or command.command_sha256 != candidate.command_sha256
                    or command.task_token_sha256 != candidate.task_token_sha256
                ):
                    raise TaskCredentialContractError(
                        "task credential registration replay conflicts with its command"
                    )
                if not database_bound:
                    try:
                        self._bind_region_talk_credential(
                            command=command,
                            candidate=candidate,
                            gate=gate,
                        )
                    except TaskCredentialContractError:
                        self._registration_replays.pop(key, None)
                        self._drop(
                            candidate,
                            provisioner=provisioner,
                            gate=gate,
                            reason="task_credential_binding_rejected",
                        )
                        raise
                    except Exception:
                        # The SECURITY DEFINER binding is exact-idempotent.
                        # An uncertain database response must be replayed
                        # before the credential can cross to the worker.
                        continue
                    database_bound = True
                    self._registration_replays[key] = (
                        candidate,
                        registration,
                        had_current,
                        True,
                    )
                try:
                    receipt = register(dict(registration))
                except Exception:
                    # Exact replay remains queued for the next bounded poll.
                    continue
                try:
                    self._assert_registration_receipt(
                        receipt=receipt,
                        registration=registration,
                    )
                except TaskCredentialContractError:
                    self._registration_replays.pop(key, None)
                    self._drop(
                        candidate,
                        provisioner=provisioner,
                        gate=gate,
                        reason="task_credential_registration_rejected",
                    )
                    raise
                self._registration_replays.pop(key, None)
                if had_current and candidate.worker_kind == "region_talk":
                    self.pending[key] = candidate
                elif had_current:
                    current = self.issued[key]
                    self.issued[key] = candidate
                    self._drop(
                        current,
                        provisioner=provisioner,
                        gate=gate,
                        reason="task_credential_rotated",
                    )
                else:
                    self.issued[key] = candidate
                self._registration_acknowledgements[key] = dict(receipt)
                continue
            current = self.issued.get(key)
            pending = self.pending.get(key)
            if pending is not None:
                if command.generation < pending.generation:
                    raise TaskCredentialContractError(
                        "stale task credential generation cannot replace the pending credential"
                    )
                if command.generation == pending.generation:
                    if (
                        command.command_sha256 != pending.command_sha256
                        or command.task_token_sha256 != pending.task_token_sha256
                    ):
                        raise TaskCredentialContractError(
                            "pending task credential generation conflicts with its exact binding"
                        )
                    continue
                raise TaskCredentialContractError(
                    "a newer task credential generation cannot bypass pending activation"
                )
            if current is not None:
                if command.generation < current.generation:
                    raise TaskCredentialContractError(
                        "stale task credential generation cannot replace the active credential"
                    )
                if command.generation == current.generation:
                    if (
                        command.command_sha256 != current.command_sha256
                        or command.task_token_sha256 != current.task_token_sha256
                    ):
                        raise TaskCredentialContractError(
                            "task credential generation conflicts with its exact binding"
                        )
                    # Exact command replays are intentionally no-ops.  The
                    # control authority must increment generation before the
                    # refresh window and receives a new credential only then.
                    continue

            expiry = min(observed + timedelta(minutes=4), active_until)
            if expiry <= observed + timedelta(seconds=45):
                # No credential is safer than one without enough time for a
                # private handoff and worker connection.
                continue
            group, role, principal_label = _ROLE_BINDINGS[command.worker_kind]
            credential_id = UUID(bytes=secrets.token_bytes(16), version=4)
            principal = (
                f"mdh_e{self.identity.epoch}_{principal_label}g{command.generation}_"
                f"{credential_id.hex[:8]}"
            )
            password = secrets.token_urlsafe(36)
            provisioner.create(
                principal=principal,
                password=password,
                group=group,
                identity=self.identity,
                credential_id=credential_id,
                expires_at=expiry,
                now=observed,
                policy=LoginPolicy(statement_timeout_ms=300_000, connection_limit=2),
            )
            # Retain every created name until terminal cleanup, even after a
            # successful rotation/drop.  DROP ROLE IF EXISTS makes the final
            # sweep idempotent and prevents a partial handoff failure from
            # leaving an untracked LOGIN behind.
            self._all_principals.add(principal)
            query = urlencode(
                {
                    "sslmode": "verify-ca",
                    "sslrootcert": "/state/master-tls/ca.pem",
                    "connect_timeout": "5",
                }
            )
            registration: dict[str, object] = {
                "schema_version": "my-data-hub-task-credential-registration.v1",
                "master_instance_id": str(self.identity.master_instance_id),
                "epoch": self.identity.epoch,
                "worker_kind": command.worker_kind,
                "task_run_id": str(command.task_run_id),
                "generation": command.generation,
                "credential_id": str(credential_id),
                "role": role,
                "database_url": (
                    f"postgresql://{quote(principal, safe='')}:{quote(password, safe='')}@"
                    f"127.0.0.1:{self.local_postgres_port}/postgres?{query}"
                ),
                "expires_at": expiry.isoformat().replace("+00:00", "Z"),
                "task_token_sha256": command.task_token_sha256,
                "command_sha256": command.command_sha256,
            }
            replacement = IssuedTaskCredential(
                worker_kind=command.worker_kind,
                task_run_id=command.task_run_id,
                epoch=self.identity.epoch,
                generation=command.generation,
                task_token_sha256=command.task_token_sha256,
                command_sha256=command.command_sha256,
                credential_id=credential_id,
                principal=principal,
                expires_at=expiry,
            )
            database_bound = command.worker_kind != "region_talk"
            if not database_bound:
                try:
                    self._bind_region_talk_credential(
                        command=command,
                        candidate=replacement,
                        gate=gate,
                    )
                except TaskCredentialContractError:
                    self._drop(
                        replacement,
                        provisioner=provisioner,
                        gate=gate,
                        reason="task_credential_binding_rejected",
                    )
                    raise
                except Exception:
                    self._registration_replays[key] = (
                        replacement,
                        registration,
                        current is not None,
                        False,
                    )
                    continue
                database_bound = True
            try:
                receipt = register(registration)
            except Exception:
                self._registration_replays[key] = (
                    replacement,
                    registration,
                    current is not None,
                    database_bound,
                )
                continue
            try:
                self._assert_registration_receipt(
                    receipt=receipt,
                    registration=registration,
                )
            except TaskCredentialContractError:
                self._drop(
                    replacement,
                    provisioner=provisioner,
                    gate=gate,
                    reason="task_credential_registration_rejected",
                )
                raise
            self._registration_acknowledgements[key] = dict(receipt)
            if current is None:
                self.issued[key] = replacement
            elif command.worker_kind == "region_talk":
                # Two-phase rotation: registration makes the new generation
                # available to the private worker, but does not revoke the old
                # LOGIN.  The worker tests its new SSH tunnel and executes the
                # ACTIVE-epoch DB assertion before the control authority emits
                # the old generation's exact `task_credential_rotated`
                # revocation in a later poll.
                self.pending[key] = replacement
            else:
                # Existing embedding workers retain the original immediate
                # rotation contract; they do not implement Region Talk's
                # private activation callback.
                self.issued[key] = replacement
                self._drop(
                    current,
                    provisioner=provisioner,
                    gate=gate,
                    reason="task_credential_rotated",
                )

    def _bind_region_talk_credential(
        self,
        *,
        command: TaskCredentialCommand,
        candidate: IssuedTaskCredential,
        gate: Any,
    ) -> None:
        """Create the authoritative task binding before private publication."""

        receipt = gate.register_task_credential_binding(
            credential_id=candidate.credential_id,
            principal=candidate.principal,
            worker_kind=command.worker_kind,
            task_run_id=command.task_run_id,
            generation=command.generation,
            identity=self.identity,
            command_sha256=command.command_sha256,
            task_token_sha256=command.task_token_sha256,
        )
        expected: dict[str, object] = {
            "registered": True,
            "credential_id": str(candidate.credential_id),
            "principal": candidate.principal,
            "worker_kind": command.worker_kind,
            "task_run_id": str(command.task_run_id),
            "generation": command.generation,
            "master_instance_id": str(self.identity.master_instance_id),
            "epoch": self.identity.epoch,
            "command_sha256": command.command_sha256,
            "task_token_sha256": command.task_token_sha256,
        }
        if not isinstance(receipt, Mapping) or dict(receipt) != expected:
            raise TaskCredentialContractError(
                "database task credential binding receipt is not exact"
            )

    def registration_acknowledgements(self) -> tuple[Mapping[str, object], ...]:
        return tuple(
            dict(value)
            for _key, value in sorted(
                self._registration_acknowledgements.items(),
                key=lambda item: (item[0][0], str(item[0][1])),
            )
        )

    def mark_registration_acknowledged(
        self, receipts: tuple[Mapping[str, object], ...]
    ) -> None:
        for receipt in receipts:
            try:
                key: TaskCredentialKey = (
                    str(receipt["worker_kind"]),  # type: ignore[assignment]
                    UUID(str(receipt["task_run_id"])),
                )
            except (KeyError, ValueError) as exc:
                raise TaskCredentialContractError(
                    "task registration acknowledgement binding is invalid"
                ) from exc
            current = self._registration_acknowledgements.get(key)
            if current is None or current != dict(receipt):
                raise TaskCredentialContractError(
                    "task registration acknowledgement binding is unknown"
                )
            self._registration_acknowledgements.pop(key, None)

    @staticmethod
    def _assert_registration_receipt(
        *, receipt: Mapping[str, object], registration: Mapping[str, object]
    ) -> None:
        exact = {
            "registered": True,
            "worker_kind": registration["worker_kind"],
            "task_run_id": registration["task_run_id"],
            "epoch": registration["epoch"],
            "generation": registration["generation"],
            "credential_id": registration["credential_id"],
            "command_sha256": registration["command_sha256"],
        }
        if dict(receipt) != exact:
            raise TaskCredentialContractError(
                "task credential registrar did not acknowledge the exact binding"
            )

    @staticmethod
    def _exact(
        value: IssuedTaskCredential,
    ) -> tuple[TaskWorkerKind, UUID, int, str, str, UUID]:
        return (
            value.worker_kind,
            value.task_run_id,
            value.generation,
            value.task_token_sha256,
            value.command_sha256,
            value.credential_id,
        )

    @staticmethod
    def _revocation_exact(
        value: TaskCredentialRevocation,
    ) -> tuple[TaskWorkerKind, UUID, int, str, str, UUID]:
        return (
            value.worker_kind,
            value.task_run_id,
            value.generation,
            value.task_token_sha256,
            value.command_sha256,
            value.credential_id,
        )

    def principal_names(self) -> tuple[str, ...]:
        return tuple(sorted(self._all_principals))


class TaskCredentialPoller:
    """Continuously reconcile task access independently of workload stages."""

    def __init__(
        self,
        *,
        poll: Callable[[], None],
        interval_seconds: float = 10.0,
        initial_delay_seconds: float = 0.0,
    ) -> None:
        if not 0.01 <= interval_seconds <= 60:
            raise ValueError("task credential poll interval is outside 0.01..60 seconds")
        if not 0 <= initial_delay_seconds <= 60:
            raise ValueError("task credential initial delay is outside 0..60 seconds")
        self.poll = poll
        self.interval_seconds = interval_seconds
        self.initial_delay_seconds = initial_delay_seconds
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._error: BaseException | None = None

    def start(self) -> None:
        if self._thread is not None:
            raise RuntimeError("task credential poller is already started")
        self._thread = threading.Thread(
            target=self._run,
            name="my-data-hub-task-credential-poller",
            daemon=True,
        )
        self._thread.start()

    def _run(self) -> None:
        if self.initial_delay_seconds and self._stop.wait(self.initial_delay_seconds):
            return
        while not self._stop.is_set():
            try:
                self.poll()
            except BaseException as exc:
                self._error = exc
                self._stop.set()
                return
            if self._stop.wait(self.interval_seconds):
                return

    def check(self) -> None:
        if self._error is not None:
            raise self._error

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=max(1.0, self.interval_seconds + 1.0))
            if self._thread.is_alive():
                raise TimeoutError("task credential poller did not stop")
            self._thread = None
