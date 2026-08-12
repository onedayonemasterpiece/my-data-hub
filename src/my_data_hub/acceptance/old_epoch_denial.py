"""Fixed FM11 probes for a retired PostgreSQL master epoch.

This module deliberately has no public request model.  Trusted composition
captures one metadata-only context while the old runtime is still ACTIVE and
hands it, together with the exact replacement/checkpoint identity, to
``ProductionOldEpochDenialProbe``.  The probe then calls four narrow clients;
none of those clients accepts a URL, SQL, credential, clock, duration, or
arbitrary payload from the acceptance caller.

The clients are production boundaries rather than generic transports.  Their
implementations must normalize observations from the existing runtime-event,
session-credential, H1 PostgreSQL, and tunnel admission paths into the fixed
records below.  Raw bearer tokens, database URLs, passwords, certificates and
private keys cannot be represented by this module.
"""

from __future__ import annotations

import hashlib
import re
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Literal, Protocol
from uuid import UUID

from my_data_hub.hashing import canonical_json_bytes

from .master_lifecycle import MasterAcceptanceBinding
from .master_production import OldEpochDenials, ProductionAcceptanceBlocked

SHA256_PATTERN = frozenset("0123456789abcdef")
CONTEXT_TTL_SECONDS = 900
EXACT_VERSION_REF = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/[1-9][0-9]*$")


def _sha256(value: object) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def _require_sha256(value: str, label: str) -> None:
    if len(value) != 64 or any(character not in SHA256_PATTERN for character in value):
        raise ValueError(f"{label} is not SHA-256")


@dataclass(frozen=True, slots=True)
class RetiredTunnelCertificateIdentity:
    """Non-secret identity of the certificate issued to the old runtime."""

    serial: int
    principal_sha256: str
    public_key_sha256: str

    def __post_init__(self) -> None:
        if self.serial < 1:
            raise ValueError("retired tunnel certificate serial must be positive")
        _require_sha256(self.principal_sha256, "tunnel principal digest")
        _require_sha256(self.public_key_sha256, "tunnel public-key digest")


@dataclass(frozen=True, slots=True)
class OldRuntimeProbeContext:
    """Task-bound metadata captured before the old operation becomes STOPPED.

    ``credential_handle`` addresses a protected in-memory connection/session
    registry.  It is not a login, DSN, file path, or credential envelope.
    ``runtime_token_sha256`` is computed before construction; the bearer is
    owned only by the fixed runtime-admission client.
    """

    task_id: UUID
    old_operation_id: UUID
    run_id: UUID
    attempt_id: UUID
    service_instance_id: str
    master_instance_id: UUID
    epoch: int
    runtime_token_sha256: str
    credential_handle: UUID
    tunnel_certificate: RetiredTunnelCertificateIdentity

    def __post_init__(self) -> None:
        if not 1 <= len(self.service_instance_id) <= 200:
            raise ValueError("old service instance identity is invalid")
        if self.epoch < 1:
            raise ValueError("old runtime epoch must be positive")
        _require_sha256(self.runtime_token_sha256, "runtime token digest")

    @property
    def binding(self) -> MasterAcceptanceBinding:
        return MasterAcceptanceBinding(
            operation_id=self.old_operation_id,
            run_id=self.run_id,
            attempt_id=self.attempt_id,
            service_instance_id=self.service_instance_id,
            master_instance_id=self.master_instance_id,
            epoch=self.epoch,
        )

    @property
    def context_sha256(self) -> str:
        return _sha256(
            {
                "task_id": str(self.task_id),
                "old_binding": self.binding.model_dump(mode="json"),
                "runtime_token_sha256": self.runtime_token_sha256,
                "credential_handle_sha256": _sha256(str(self.credential_handle)),
                "tunnel_certificate": {
                    "serial": self.tunnel_certificate.serial,
                    "principal_sha256": self.tunnel_certificate.principal_sha256,
                    "public_key_sha256": self.tunnel_certificate.public_key_sha256,
                },
            }
        )


@dataclass(frozen=True, slots=True)
class ReplacementEpochContext:
    """Exact replacement and the current verified handoff checkpoint."""

    task_id: UUID
    operation_id: UUID
    master_instance_id: UUID
    epoch: int
    active: Literal[True]
    checkpoint_id: UUID
    exact_version_ref: str
    manifest_sha256: str
    checkpoint_status: Literal["VERIFIED"]
    checkpoint_is_current: Literal[True]

    def __post_init__(self) -> None:
        if self.epoch < 2 or self.active is not True:
            raise ValueError("replacement epoch must be at least two")
        if self.checkpoint_status != "VERIFIED" or self.checkpoint_is_current is not True:
            raise ValueError("replacement checkpoint must be current and VERIFIED")
        parts = self.exact_version_ref.split("/")
        if not EXACT_VERSION_REF.fullmatch(self.exact_version_ref):
            raise ValueError("replacement checkpoint must use an exact numeric version reference")
        if any(not part or len(part) > 100 for part in parts):
            raise ValueError("replacement checkpoint version reference is invalid")
        _require_sha256(self.manifest_sha256, "replacement checkpoint manifest digest")


@dataclass(frozen=True, slots=True)
class RuntimeRenewalDenial:
    heartbeat_denied: Literal[True]
    lease_renewal_denied: Literal[True]
    runtime_token_revoked: Literal[True]
    runtime_token_sha256: str
    denial_code: Literal["MDH_RETIRED_RUNTIME_TOKEN"]

    def __post_init__(self) -> None:
        if (
            self.heartbeat_denied is not True
            or self.lease_renewal_denied is not True
            or self.runtime_token_revoked is not True
            or self.denial_code != "MDH_RETIRED_RUNTIME_TOKEN"
        ):
            raise ValueError("retired runtime renewal was not denied by the fixed path")
        _require_sha256(self.runtime_token_sha256, "observed runtime token digest")


@dataclass(frozen=True, slots=True)
class CredentialRegistrationDenial:
    registration_denied: Literal[True]
    bind_denied: Literal[True]
    registration_code: Literal["MDH_RETIRED_RUNTIME_REGISTER"]
    bind_code: Literal["MDH_RETIRED_CREDENTIAL_BIND"]

    def __post_init__(self) -> None:
        if (
            self.registration_denied is not True
            or self.bind_denied is not True
            or self.registration_code != "MDH_RETIRED_RUNTIME_REGISTER"
            or self.bind_code != "MDH_RETIRED_CREDENTIAL_BIND"
        ):
            raise ValueError("retired credential registration/binding was not exactly denied")


@dataclass(frozen=True, slots=True)
class BoundedWriteDenial:
    denied: Literal[True]
    sqlstate: Literal["55000"]
    transaction_state: Literal["rollback_only"]
    canonical_revision_before: int
    canonical_revision_after: int
    denial_code: Literal["MDH_OLD_EPOCH_WRITE"]

    def __post_init__(self) -> None:
        if (
            self.denied is not True
            or self.sqlstate != "55000"
            or self.transaction_state != "rollback_only"
            or self.denial_code != "MDH_OLD_EPOCH_WRITE"
        ):
            raise ValueError("old-epoch H1 write was not denied in rollback-only state")
        if self.canonical_revision_before < 0 or self.canonical_revision_after < 0:
            raise ValueError("canonical revision cannot be negative")
        if self.canonical_revision_before != self.canonical_revision_after:
            raise ValueError("old-epoch write denial changed canonical revision")


@dataclass(frozen=True, slots=True)
class TunnelRenewalDenial:
    lease_renewal_denied: Literal[True]
    certificate_renewal_denied: Literal[True]
    lease_denial_code: Literal["MDH_RETIRED_TUNNEL_LEASE"]
    certificate_denial_code: Literal["MDH_RETIRED_TUNNEL_CERTIFICATE"]
    certificate_serial: int
    principal_sha256: str

    def __post_init__(self) -> None:
        if (
            self.lease_renewal_denied is not True
            or self.certificate_renewal_denied is not True
            or self.lease_denial_code != "MDH_RETIRED_TUNNEL_LEASE"
            or self.certificate_denial_code != "MDH_RETIRED_TUNNEL_CERTIFICATE"
        ):
            raise ValueError("retired tunnel lease/certificate was not exactly denied")
        if self.certificate_serial < 1:
            raise ValueError("observed tunnel certificate serial must be positive")
        _require_sha256(self.principal_sha256, "observed tunnel principal digest")


class RetiredRuntimeRenewalClient(Protocol):
    """Probe the fixed heartbeat and runtime-lease renewal admission paths."""

    def deny_retired_runtime(
        self, context: OldRuntimeProbeContext, replacement: ReplacementEpochContext
    ) -> RuntimeRenewalDenial: ...


class RetiredCredentialClient(Protocol):
    """Probe fixed control registration and master-local credential binding."""

    def deny_retired_credential(
        self, context: OldRuntimeProbeContext, replacement: ReplacementEpochContext
    ) -> CredentialRegistrationDenial: ...


class RetiredBoundedWriteClient(Protocol):
    """Run H1's fixed rollback-only no-row UPDATE through the old handle."""

    def deny_retired_write(
        self, context: OldRuntimeProbeContext, replacement: ReplacementEpochContext
    ) -> BoundedWriteDenial: ...


class HeldOperatorConnectionRegistry(Protocol):
    """Resolve only a pre-opened, task-owned restricted operator session."""

    def resolve(self, credential_handle: UUID) -> Any: ...


@dataclass(frozen=True, slots=True)
class PsycopgRetiredBoundedWriteClient:
    """Concrete H1 fixed-SQL probe over a pre-opened operator connection.

    The registry is populated while the old epoch is ACTIVE.  This helper does
    not accept a DSN and never opens an owner connection.  The only attempted
    DML is the allowlisted no-row UPDATE after H1's mandatory epoch assertion.
    """

    connections: HeldOperatorConnectionRegistry = field(repr=False)

    def deny_retired_write(
        self, context: OldRuntimeProbeContext, replacement: ReplacementEpochContext
    ) -> BoundedWriteDenial:
        import psycopg
        from psycopg.pq import TransactionStatus

        connection = self.connections.resolve(context.credential_handle)
        state = connection.execute(
            "SELECT c.canonical_revision,e.current_epoch,e.master_instance_id::text "
            "FROM hub.canonical_state c CROSS JOIN master_control.epoch_state e "
            "WHERE c.singleton=true AND e.singleton=true"
        ).fetchone()
        if (
            state is None
            or int(state[1]) != replacement.epoch
            or str(state[2]) != str(replacement.master_instance_id)
        ):
            connection.rollback()
            raise ProductionAcceptanceBlocked("FM11_H1_REPLACEMENT_BINDING_MISMATCH")
        revision_before = int(state[0])
        connection.commit()
        denied = rollback_only = False
        try:
            connection.execute("SET TRANSACTION READ WRITE")
            connection.execute("SELECT master_control.assert_session_write_epoch()")
            connection.execute("UPDATE hub.project SET status=status WHERE false")
        except psycopg.Error as exc:
            denied = exc.sqlstate == "55000"
            rollback_only = connection.info.transaction_status == TransactionStatus.INERROR
        if not denied or not rollback_only:
            connection.rollback()
            raise ProductionAcceptanceBlocked("FM11_H1_ROLLBACK_DENIAL_NOT_OBSERVED")
        connection.rollback()
        revision = connection.execute(
            "SELECT canonical_revision FROM hub.canonical_state WHERE singleton=true"
        ).fetchone()
        connection.rollback()
        if revision is None:
            raise ProductionAcceptanceBlocked("FM11_H1_REVISION_READBACK_MISSING")
        return BoundedWriteDenial(
            denied=True,
            sqlstate="55000",
            transaction_state="rollback_only",
            canonical_revision_before=revision_before,
            canonical_revision_after=int(revision[0]),
            denial_code="MDH_OLD_EPOCH_WRITE",
        )


class RetiredTunnelClient(Protocol):
    """Probe both old tunnel lease and certificate renewal paths."""

    def deny_retired_tunnel(
        self, context: OldRuntimeProbeContext, replacement: ReplacementEpochContext
    ) -> TunnelRenewalDenial: ...


class RetiredContextReleasePort(Protocol):
    """Destroy the protected handle and any task-local tunnel material."""

    def release(self, *, credential_handle: UUID, certificate_serial: int) -> bool: ...


class OldEpochReplacementBinder(Protocol):
    """One-shot handoff invoked after the replacement/checkpoint are exact."""

    def bind_replacement(self, replacement: ReplacementEpochContext) -> None: ...


@dataclass(frozen=True, slots=True)
class OldEpochDenialReceipt:
    """Sanitized internal receipt used only to derive the two public hashes."""

    context_sha256: str
    task_id: UUID
    old_operation_id: UUID
    old_epoch: int
    new_operation_id: UUID
    new_master_instance_id: UUID
    new_epoch: int
    checkpoint_id: UUID
    checkpoint_version_ref: str
    checkpoint_manifest_sha256: str
    runtime: RuntimeRenewalDenial
    credential: CredentialRegistrationDenial
    write: BoundedWriteDenial
    tunnel: TunnelRenewalDenial

    @property
    def public(self) -> dict[str, object]:
        return {
            "schema_version": "my-data-hub-old-epoch-denial-receipt.v1",
            "context_sha256": self.context_sha256,
            "task_id": str(self.task_id),
            "old_operation_id": str(self.old_operation_id),
            "old_epoch": self.old_epoch,
            "new_operation_id": str(self.new_operation_id),
            "new_master_instance_id": str(self.new_master_instance_id),
            "new_epoch": self.new_epoch,
            "new_epoch_active": True,
            "checkpoint": {
                "checkpoint_id": str(self.checkpoint_id),
                "exact_version_ref": self.checkpoint_version_ref,
                "manifest_sha256": self.checkpoint_manifest_sha256,
                "status": "VERIFIED",
                "current": True,
            },
            "runtime": {
                "heartbeat_denied": self.runtime.heartbeat_denied,
                "lease_renewal_denied": self.runtime.lease_renewal_denied,
                "runtime_token_revoked": self.runtime.runtime_token_revoked,
                "runtime_token_sha256": self.runtime.runtime_token_sha256,
                "denial_code": self.runtime.denial_code,
            },
            "credential": {
                "registration_denied": self.credential.registration_denied,
                "bind_denied": self.credential.bind_denied,
                "registration_code": self.credential.registration_code,
                "bind_code": self.credential.bind_code,
            },
            "write": {
                "denied": self.write.denied,
                "sqlstate": self.write.sqlstate,
                "transaction_state": self.write.transaction_state,
                "canonical_revision_before": self.write.canonical_revision_before,
                "canonical_revision_after": self.write.canonical_revision_after,
                "denial_code": self.write.denial_code,
            },
            "tunnel": {
                "lease_renewal_denied": self.tunnel.lease_renewal_denied,
                "certificate_renewal_denied": self.tunnel.certificate_renewal_denied,
                "lease_denial_code": self.tunnel.lease_denial_code,
                "certificate_denial_code": self.tunnel.certificate_denial_code,
                "certificate_serial": self.tunnel.certificate_serial,
                "principal_sha256": self.tunnel.principal_sha256,
            },
        }

    @property
    def receipt_sha256(self) -> str:
        return _sha256(self.public)

    @property
    def write_receipt_sha256(self) -> str:
        return _sha256({key: self.public[key] for key in ("context_sha256", "runtime", "credential", "write")})

    @property
    def tunnel_receipt_sha256(self) -> str:
        return _sha256({key: self.public[key] for key in ("context_sha256", "tunnel")})


@dataclass(frozen=True, slots=True)
class ProductionOldEpochDenialProbe:
    """Concrete, single-task implementation of ``OldEpochDenialPort``.

    Composition constructs this object before STOPPED.  The replacement value
    is attached only after the ordinary verified-checkpoint rotation selects
    the exact new operation.  A fixed 15-minute monotonic TTL starts at object
    construction and cannot be widened by request data.

    Successful results are cached as hashes/booleans, making an exact retry
    after a lost response idempotent.  The context reference and its protected
    credential/tunnel resources are released after the first physical probe.
    """

    context: OldRuntimeProbeContext | None = field(repr=False)
    replacement: ReplacementEpochContext | None
    runtime: RetiredRuntimeRenewalClient = field(repr=False)
    credentials: RetiredCredentialClient = field(repr=False)
    writes: RetiredBoundedWriteClient = field(repr=False)
    tunnels: RetiredTunnelClient = field(repr=False)
    release_port: RetiredContextReleasePort = field(repr=False)
    _issued_monotonic_ns: int = field(default_factory=time.monotonic_ns, init=False, repr=False)
    _binding_sha256: str | None = field(default=None, init=False, repr=False)
    _completed: OldEpochDenials | None = field(default=None, init=False, repr=False)
    _result_sha256: str | None = field(default=None, init=False, repr=False)
    _lock: threading.RLock = field(default_factory=threading.RLock, init=False, repr=False)

    def __post_init__(self) -> None:
        context = self.context
        if context is None:
            raise ValueError("FM11 old runtime context is required")
        if self.replacement is not None:
            self._validate_replacement(context, self.replacement)

    @staticmethod
    def _validate_replacement(
        context: OldRuntimeProbeContext, replacement: ReplacementEpochContext
    ) -> None:
        if context.task_id != replacement.task_id:
            raise ValueError("FM11 replacement is bound to another task")
        if replacement.epoch <= context.epoch:
            raise ValueError("FM11 replacement epoch did not advance")
        if replacement.operation_id == context.old_operation_id:
            raise ValueError("FM11 replacement operation did not change")

    def bind_replacement(self, replacement: ReplacementEpochContext) -> None:
        """Attach the exact ACTIVE replacement after ordinary rotation.

        The old context and its fixed TTL already exist at this point.  Exact
        replay is allowed; switching the replacement is not.
        """

        with self._lock:
            context = self.context
            if context is None:
                raise ProductionAcceptanceBlocked("FM11_CONTEXT_ALREADY_CLEARED")
            self._validate_replacement(context, replacement)
            if self.replacement is not None and self.replacement != replacement:
                raise ProductionAcceptanceBlocked("FM11_REPLACEMENT_REBIND_DENIED")
            object.__setattr__(self, "replacement", replacement)

    @property
    def result_sha256(self) -> str | None:
        """Return only the sanitized completed receipt hash."""

        return self._result_sha256

    def prove_old_epoch_denials(self, binding: MasterAcceptanceBinding) -> OldEpochDenials:
        binding_sha256 = _sha256(binding.model_dump(mode="json"))
        with self._lock:
            if self._completed is not None:
                if binding_sha256 != self._binding_sha256:
                    raise ProductionAcceptanceBlocked("FM11_RESPONSE_REPLAY_BINDING_MISMATCH")
                return self._completed
            context = self.context
            if context is None:
                raise ProductionAcceptanceBlocked("FM11_CONTEXT_ALREADY_CLEARED")
            replacement = self.replacement
            if replacement is None:
                raise ProductionAcceptanceBlocked("FM11_REPLACEMENT_NOT_BOUND")
            if binding != context.binding:
                raise ProductionAcceptanceBlocked("FM11_OLD_BINDING_MISMATCH")
            elapsed = time.monotonic_ns() - self._issued_monotonic_ns
            if elapsed < 0 or elapsed > CONTEXT_TTL_SECONDS * 1_000_000_000:
                self._release_context(context)
                raise ProductionAcceptanceBlocked("FM11_CONTEXT_EXPIRED")

            try:
                runtime = self.runtime.deny_retired_runtime(context, replacement)
                credential = self.credentials.deny_retired_credential(context, replacement)
                write = self.writes.deny_retired_write(context, replacement)
                tunnel = self.tunnels.deny_retired_tunnel(context, replacement)
                self._validate_observations(context, runtime, credential, write, tunnel)
                receipt = OldEpochDenialReceipt(
                    context_sha256=context.context_sha256,
                    task_id=context.task_id,
                    old_operation_id=context.old_operation_id,
                    old_epoch=context.epoch,
                    new_operation_id=replacement.operation_id,
                    new_master_instance_id=replacement.master_instance_id,
                    new_epoch=replacement.epoch,
                    checkpoint_id=replacement.checkpoint_id,
                    checkpoint_version_ref=replacement.exact_version_ref,
                    checkpoint_manifest_sha256=replacement.manifest_sha256,
                    runtime=runtime,
                    credential=credential,
                    write=write,
                    tunnel=tunnel,
                )
                completed = OldEpochDenials(
                    renew_denied=True,
                    register_denied=True,
                    bounded_write_denied=True,
                    tunnel_denied=True,
                    write_receipt_sha256=receipt.write_receipt_sha256,
                    tunnel_receipt_sha256=receipt.tunnel_receipt_sha256,
                )
                object.__setattr__(self, "_binding_sha256", binding_sha256)
                object.__setattr__(self, "_result_sha256", receipt.receipt_sha256)
                object.__setattr__(self, "_completed", completed)
            finally:
                self._release_context(context)
            return completed

    def _release_context(self, context: OldRuntimeProbeContext) -> None:
        if self.context is None:
            return
        released = self.release_port.release(
            credential_handle=context.credential_handle,
            certificate_serial=context.tunnel_certificate.serial,
        )
        object.__setattr__(self, "context", None)
        if released is not True:
            object.__setattr__(self, "_completed", None)
            object.__setattr__(self, "_result_sha256", None)
            raise ProductionAcceptanceBlocked("FM11_CONTEXT_RELEASE_FAILED")

    @staticmethod
    def _validate_observations(
        context: OldRuntimeProbeContext,
        runtime: RuntimeRenewalDenial,
        credential: CredentialRegistrationDenial,
        write: BoundedWriteDenial,
        tunnel: TunnelRenewalDenial,
    ) -> None:
        del credential, write  # Literal/dataclass validation already proves their fixed shape.
        if runtime.runtime_token_sha256 != context.runtime_token_sha256:
            raise ProductionAcceptanceBlocked("FM11_RUNTIME_TOKEN_HASH_MISMATCH")
        if (
            tunnel.certificate_serial != context.tunnel_certificate.serial
            or tunnel.principal_sha256 != context.tunnel_certificate.principal_sha256
        ):
            raise ProductionAcceptanceBlocked("FM11_TUNNEL_CERTIFICATE_IDENTITY_MISMATCH")
