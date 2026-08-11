"""Production, task-keyed FM11 pre-STOPPED capture and denial clients."""

from __future__ import annotations

import hashlib
import threading
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, Protocol
from uuid import UUID, uuid4

from my_data_hub.hashing import canonical_json_bytes
from my_data_hub.mcp.contracts import ExecutionLimits, SessionRequest
from my_data_hub.mcp.oauth import AccessIdentity
from my_data_hub.mcp.postgres_broker import (
    DirectoryEpochCredentialSource,
    SessionBrokerError,
)

from .master_lifecycle import MasterAcceptanceCommand, MasterAcceptanceCommandKind
from .master_production import DirectoryOperatorConnectionFactory, ProductionAcceptanceBlocked
from .old_epoch_denial import (
    CredentialRegistrationDenial,
    OldRuntimeProbeContext,
    ProductionOldEpochDenialProbe,
    PsycopgRetiredBoundedWriteClient,
    ReplacementEpochContext,
    RetiredTunnelCertificateIdentity,
    RuntimeRenewalDenial,
    TunnelRenewalDenial,
)


class OldEpochLedgerPort(Protocol):
    clock: Any

    def fm11_old_epoch_context(self, task_id: str) -> dict[str, Any] | None: ...

    def begin_fm11_old_epoch_context(self, **values: Any) -> tuple[dict[str, Any], bool]: ...

    def complete_fm11_old_epoch_context_capture(self, **values: Any) -> dict[str, Any]: ...

    def release_fm11_old_epoch_context(self, **values: Any) -> None: ...

    def fm11_retired_admission_observation(self, **values: Any) -> dict[str, Any]: ...


class OldEpochTunnelAuthority(Protocol):
    def acceptance_identity_snapshot(self, **values: Any) -> dict[str, object]: ...

    def acceptance_retired_denial(self, **values: Any) -> dict[str, object]: ...


@dataclass(slots=True)
class HeldOperatorConnectionRegistry:
    """Process-private registry; only opaque UUID handles leave this object."""

    _values: dict[UUID, tuple[UUID, Any, str, int]] = field(default_factory=dict, repr=False)
    _lock: threading.RLock = field(default_factory=threading.RLock, repr=False)

    def register(
        self,
        *,
        handle: UUID,
        task_id: UUID,
        connection: Any,
        context_sha256: str,
        certificate_serial: int,
    ) -> None:
        with self._lock:
            if handle in self._values:
                raise ProductionAcceptanceBlocked("FM11_CREDENTIAL_HANDLE_REUSED")
            self._values[handle] = (task_id, connection, context_sha256, certificate_serial)

    def resolve(self, credential_handle: UUID) -> Any:
        with self._lock:
            value = self._values.get(credential_handle)
        if value is None:
            raise ProductionAcceptanceBlocked("FM11_HELD_OPERATOR_SESSION_UNAVAILABLE")
        return value[1]

    def release(self, credential_handle: UUID, certificate_serial: int) -> tuple[UUID, str] | None:
        with self._lock:
            value = self._values.pop(credential_handle, None)
        if value is None or value[3] != certificate_serial:
            return None
        value[1].close()
        return value[0], value[2]


@dataclass(frozen=True, slots=True)
class LedgerRetiredRuntimeClient:
    ledger: OldEpochLedgerPort

    def deny_retired_runtime(
        self, context: OldRuntimeProbeContext, replacement: ReplacementEpochContext
    ) -> RuntimeRenewalDenial:
        observed = self.ledger.fm11_retired_admission_observation(
            task_id=str(context.task_id), replacement_operation_id=str(replacement.operation_id)
        )
        return RuntimeRenewalDenial(
            heartbeat_denied=bool(observed["heartbeat_denied"]),
            lease_renewal_denied=bool(observed["lease_renewal_denied"]),
            runtime_token_revoked=bool(observed["runtime_token_revoked"]),
            runtime_token_sha256=str(observed["runtime_token_sha256"]),
            denial_code="MDH_RETIRED_RUNTIME_TOKEN",
        )


@dataclass(frozen=True, slots=True)
class DirectoryRetiredCredentialClient:
    ledger: OldEpochLedgerPort
    source: DirectoryEpochCredentialSource = field(repr=False)

    def deny_retired_credential(
        self, context: OldRuntimeProbeContext, replacement: ReplacementEpochContext
    ) -> CredentialRegistrationDenial:
        observed = self.ledger.fm11_retired_admission_observation(
            task_id=str(context.task_id), replacement_operation_id=str(replacement.operation_id)
        )
        principal = AccessIdentity(
            subject="master-acceptance-fm11",
            client_id="control-master-acceptance",
            scopes=frozenset({"acceptance:operate"}),
            audience="local-control",
            token_id="owner-host-claim",
            expires_at=2**63 - 1,
            issuer="local-control",
            issued_at=0,
            resource="local-control",
        )
        try:
            self.source.load(
                SessionRequest(
                    principal=principal,
                    master_instance_id=str(context.master_instance_id),
                    epoch=context.epoch,
                    role="operator",
                    tool="acceptance.scenario.request",
                    limits=ExecutionLimits(timeout_ms=5_000, max_rows=1, max_bytes=16 * 1024),
                )
            )
        except SessionBrokerError:
            bind_denied = True
        else:
            bind_denied = False
        if not observed["registration_denied"] or not observed["old_credential_bind_denied"] or not bind_denied:
            raise ProductionAcceptanceBlocked("FM11_RETIRED_CREDENTIAL_STILL_AVAILABLE")
        return CredentialRegistrationDenial(
            registration_denied=True,
            bind_denied=True,
            registration_code="MDH_RETIRED_RUNTIME_REGISTER",
            bind_code="MDH_RETIRED_CREDENTIAL_BIND",
        )


@dataclass(frozen=True, slots=True)
class BrokerRetiredTunnelClient:
    authority: OldEpochTunnelAuthority = field(repr=False)

    def deny_retired_tunnel(
        self, context: OldRuntimeProbeContext, replacement: ReplacementEpochContext
    ) -> TunnelRenewalDenial:
        certificate = context.tunnel_certificate
        result = self.authority.acceptance_retired_denial(
            master_instance_id=str(context.master_instance_id),
            run_id=str(context.run_id),
            attempt_id=str(context.attempt_id),
            epoch=context.epoch,
            certificate_serial=certificate.serial,
            principal_sha256=certificate.principal_sha256,
            public_key_sha256=certificate.public_key_sha256,
            replacement_master_instance_id=str(replacement.master_instance_id),
            replacement_epoch=replacement.epoch,
        )
        return TunnelRenewalDenial(
            lease_renewal_denied=bool(result["lease_renewal_denied"]),
            certificate_renewal_denied=bool(result["certificate_renewal_denied"]),
            lease_denial_code=str(result["lease_denial_code"]),  # type: ignore[arg-type]
            certificate_denial_code=str(result["certificate_denial_code"]),  # type: ignore[arg-type]
            certificate_serial=int(result["certificate_serial"]),
            principal_sha256=str(result["principal_sha256"]),
        )


@dataclass(frozen=True, slots=True)
class TaskContextReleasePort:
    ledger: OldEpochLedgerPort
    registry: HeldOperatorConnectionRegistry = field(repr=False)

    def release(self, *, credential_handle: UUID, certificate_serial: int) -> bool:
        value = self.registry.release(credential_handle, certificate_serial)
        if value is None:
            return False
        task_id, context_sha256 = value
        receipt_sha256 = hashlib.sha256(
            canonical_json_bytes(
                {
                    "schema_version": "my-data-hub-fm11-context-release.v1",
                    "task_id": str(task_id),
                    "context_sha256": context_sha256,
                    "certificate_serial": certificate_serial,
                    "released": True,
                }
            )
        ).hexdigest()
        self.ledger.release_fm11_old_epoch_context(
            task_id=str(task_id),
            context_sha256=context_sha256,
            result_receipt_sha256=receipt_sha256,
        )
        return True


@dataclass(slots=True)
class TaskBoundOldEpochDenialFactory:
    """Capture one FM11 context before drain and resolve it by task thereafter."""

    ledger: OldEpochLedgerPort
    source: DirectoryEpochCredentialSource = field(repr=False)
    tunnel: OldEpochTunnelAuthority = field(repr=False)
    registry: HeldOperatorConnectionRegistry = field(default_factory=HeldOperatorConnectionRegistry, repr=False)
    _probes: dict[UUID, ProductionOldEpochDenialProbe] = field(default_factory=dict, repr=False)
    _lock: threading.RLock = field(default_factory=threading.RLock, repr=False)

    def for_command(self, command: MasterAcceptanceCommand) -> ProductionOldEpochDenialProbe:
        if command.command_kind is not MasterAcceptanceCommandKind.OLD_EPOCH_RETURN_DENIAL:
            raise ValueError("FM11 context factory received another command")
        with self._lock:
            existing_probe = self._probes.get(command.task_id)
            if existing_probe is not None:
                return existing_probe
            existing = self.ledger.fm11_old_epoch_context(str(command.task_id))
            if existing is not None:
                raise ProductionAcceptanceBlocked("FM11_CONTEXT_PROCESS_LOST")
            handle = uuid4()
            row, created = self.ledger.begin_fm11_old_epoch_context(
                task_id=str(command.task_id),
                command_id=str(command.command_id),
                command_sha256=command.command_sha256,
                credential_handle=str(handle),
                expires_at=self.ledger.clock.now() + timedelta(seconds=900),
            )
            if not created:
                raise ProductionAcceptanceBlocked("FM11_CONTEXT_CAPTURE_AMBIGUOUS")
            connection = DirectoryOperatorConnectionFactory(self.source).open(command.binding)
            try:
                observed = self.tunnel.acceptance_identity_snapshot(
                    master_instance_id=str(command.binding.master_instance_id),
                    run_id=str(command.binding.run_id),
                    attempt_id=str(command.binding.attempt_id),
                    epoch=command.binding.epoch,
                )
                certificate = RetiredTunnelCertificateIdentity(
                    serial=int(observed["serial"]),
                    principal_sha256=str(observed["principal_sha256"]),
                    public_key_sha256=str(observed["public_key_sha256"]),
                )
                context = OldRuntimeProbeContext(
                    task_id=command.task_id,
                    old_operation_id=command.binding.operation_id,
                    run_id=command.binding.run_id,
                    attempt_id=command.binding.attempt_id,
                    service_instance_id=command.binding.service_instance_id,
                    master_instance_id=command.binding.master_instance_id,
                    epoch=command.binding.epoch,
                    runtime_token_sha256=str(row["runtime_token_sha256"]),
                    credential_handle=handle,
                    tunnel_certificate=certificate,
                )
                self.ledger.complete_fm11_old_epoch_context_capture(
                    task_id=str(command.task_id),
                    tunnel_certificate={
                        "serial": certificate.serial,
                        "principal_sha256": certificate.principal_sha256,
                        "public_key_sha256": certificate.public_key_sha256,
                    },
                    context_sha256=context.context_sha256,
                )
                self.registry.register(
                    handle=handle,
                    task_id=command.task_id,
                    connection=connection,
                    context_sha256=context.context_sha256,
                    certificate_serial=certificate.serial,
                )
            except Exception:
                connection.close()
                raise
            probe = ProductionOldEpochDenialProbe(
                context=context,
                replacement=None,
                runtime=LedgerRetiredRuntimeClient(self.ledger),
                credentials=DirectoryRetiredCredentialClient(self.ledger, self.source),
                writes=PsycopgRetiredBoundedWriteClient(self.registry),
                tunnels=BrokerRetiredTunnelClient(self.tunnel),
                release_port=TaskContextReleasePort(self.ledger, self.registry),
            )
            self._probes[command.task_id] = probe
            return probe
