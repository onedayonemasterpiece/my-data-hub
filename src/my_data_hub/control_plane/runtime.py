"""Production assembly for the lightweight master lifecycle control plane."""

from __future__ import annotations

import hashlib
import ipaddress
import json
import os
import secrets
import tempfile
from collections.abc import Callable, Mapping
from contextlib import suppress
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import urlsplit
from uuid import NAMESPACE_URL, UUID, uuid5

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID, ObjectIdentifier

from my_data_hub.acceptance.master_lifecycle import (
    AcceptancePrincipal,
    MasterAcceptanceReceipt,
    MasterAcceptanceRequest,
    require_acceptance_operator,
)
from my_data_hub.checkpoints import CheckpointManifest
from my_data_hub.checkpoints.brokered_upload import (
    BrokeredCheckpointUploadService,
    CheckpointUploadSecretBox,
)
from my_data_hub.checkpoints.kaggle_runtime import (
    KaggleCheckpointRestoreVerifier,
    KaggleCheckpointVerifierAssets,
)
from my_data_hub.control_plane.ledger import ControlLedger
from my_data_hub.hashing import canonical_json_bytes, sha256_value
from my_data_hub.orchestrator.master import MasterCoordinator, MasterHandle, MasterIntent, MasterState
from my_data_hub.providers.kaggle import (
    ControlLedgerKaggleJournal,
    KaggleMasterLaunchAssets,
    KaggleMasterRuntimeProvider,
    KaggleProviderAdapter,
    PollPolicy,
)
from my_data_hub.providers.kaggle.adapter import mapping_sha256
from my_data_hub.providers.kaggle.contracts import (
    KaggleDatasetIdentity,
    ProviderEffectReceipt,
    TaskResourceClaim,
)
from my_data_hub.providers.kaggle.credentials import kaggle_credentials_configured
from my_data_hub.providers.kaggle.master_runtime import (
    POSTGRES_RUNTIME_ARCHIVE_NAME,
    POSTGRES_RUNTIME_MANIFEST_NAME,
    MasterLaunchContractError,
)
from my_data_hub.providers.models import ControlClass, ProviderFingerprint, ProviderKind
from my_data_hub.runtime_sdk import CANONICAL_RUNTIME_CALLBACK_URL
from my_data_hub.tunnel_broker_ipc import TunnelBrokerClient


class MasterProviderUnavailable(RuntimeError):
    """The control plane is healthy but no authenticated provider is available."""


@dataclass(slots=True)
class KaggleAcceptanceOperationExecutor:
    """Run isolated restore verification while retaining receipt metadata only."""

    adapter: KaggleProviderAdapter
    verifier_assets_factory: Callable[[int], KaggleCheckpointVerifierAssets]
    output_root: Path

    def restore(
        self,
        operation_id: str,
        candidate: dict[str, Any],
        *,
        timeout_seconds: int,
    ) -> dict[str, Any]:
        if not 60 <= timeout_seconds <= 1800:
            raise MasterProviderUnavailable("checkpoint restore timeout is outside the verifier allocation")
        manifest_payload = candidate.get("manifest")
        package_sha = candidate.get("package_sha256")
        exact_ref = str(candidate.get("version_ref") or "")
        provider_ref, separator, version_text = exact_ref.rpartition("/")
        if (
            not isinstance(manifest_payload, dict)
            or not isinstance(package_sha, str)
            or len(package_sha) != 64
            or not separator
            or not version_text.isdigit()
        ):
            raise MasterProviderUnavailable("checkpoint restore metadata is incomplete")
        manifest = CheckpointManifest.from_payload(manifest_payload)
        version = int(version_text)
        fingerprint = ProviderFingerprint(
            value=sha256_value(
                {
                    "provider_ref": provider_ref,
                    "version": version,
                    "privacy": "private",
                    "package_sha256": package_sha,
                }
            )
        )
        identity = KaggleDatasetIdentity(
            provider_ref=provider_ref,
            version=version,
            privacy="private",
            package_sha256=package_sha,
            fingerprint=fingerprint,
            observed_at=manifest.created_at,
        )
        self.output_root.mkdir(parents=True, exist_ok=True, mode=0o700)
        verifier = KaggleCheckpointRestoreVerifier(
            self.adapter,
            self.verifier_assets_factory(timeout_seconds),
            output_directory=self.output_root,
            operation_id=uuid5(NAMESPACE_URL, f"acceptance:{operation_id}"),
            authorization_task_id=UUID(manifest.source_run_id),
            metadata_only_output=True,
            poll_policy=PollPolicy(timeout_seconds=timeout_seconds, max_polls=120),
        )
        receipt = verifier.verify_restore(
            exact_version_ref=exact_ref,
            dataset_identity=identity,
            manifest=manifest,
        )
        return {
            "checkpoint_id": str(manifest.checkpoint_id),
            "exact_version_ref": exact_ref,
            "manifest_sha256": manifest.manifest_sha256,
            "ok": receipt.get("ok") is True,
        }


class SessionCredentialRegistrar(Protocol):
    def store(self, credential: SessionCredential) -> Path: ...


@dataclass(frozen=True, slots=True)
class TunnelCertificate:
    certificate: str
    serial: int
    principal: str
    valid_before: datetime
    listen_host: str
    listen_port: int

    def __post_init__(self) -> None:
        if (
            not self.certificate.startswith("ssh-ed25519-cert-v01@openssh.com ")
            or "\n" in self.certificate
            or self.serial < 1
            or not self.principal
            or self.valid_before.tzinfo is None
            or self.listen_host != "127.0.0.1"
            or not 1 <= self.listen_port <= 65535
        ):
            raise ValueError("tunnel certificate result violates the public contract")


class TunnelCertificateBroker(Protocol):
    def renew(
        self,
        *,
        master_instance_id: str,
        run_id: str,
        attempt_id: str,
        epoch: int,
        lease_until: datetime,
        now: datetime,
    ) -> object: ...

    def issue_public_key(
        self,
        *,
        master_instance_id: str,
        run_id: str,
        attempt_id: str,
        epoch: int,
        public_key: str,
        valid_before: datetime,
        now: datetime,
    ) -> TunnelCertificate: ...


@dataclass(frozen=True, slots=True)
class SessionCredential:
    master_instance_id: str
    epoch: int
    role: str
    database_url: str = field(repr=False)
    expires_at: Any

    def validate_binding(
        self, *, master_instance_id: str, epoch: int, role: str, now: datetime
    ) -> None:
        """Use the registrar's canonical epoch/TLS validation at the runtime boundary."""

        from my_data_hub.mcp.postgres_broker import EpochDatabaseCredential

        EpochDatabaseCredential(
            master_instance_id=self.master_instance_id,
            epoch=self.epoch,
            role=self.role,
            database_url=self.database_url,
            expires_at=self.expires_at,
        ).validate_binding(
            master_instance_id=master_instance_id,
            epoch=epoch,
            role=role,
            now=now,
        )


@dataclass(frozen=True, slots=True)
class MasterRuntimeSettings:
    assets: KaggleMasterLaunchAssets

    @classmethod
    def from_env(cls) -> MasterRuntimeSettings | None:
        names = {
            "source_identity": "MY_DATA_HUB_KAGGLE_MASTER_SOURCE_IDENTITY",
            "source_version": "MY_DATA_HUB_KAGGLE_MASTER_SOURCE_VERSION",
            "checkpoint_ref": "MY_DATA_HUB_KAGGLE_MASTER_CHECKPOINT_REF",
            "dataset_ref": "MY_DATA_HUB_KAGGLE_MASTER_DATASET_REF",
            "notebook_ref": "MY_DATA_HUB_KAGGLE_MASTER_NOTEBOOK_REF",
            "dataset_dir": "MY_DATA_HUB_KAGGLE_MASTER_DATASET_DIR",
            "notebook_source": "MY_DATA_HUB_KAGGLE_MASTER_NOTEBOOK_SOURCE",
            "callback_url": "MY_DATA_HUB_CALLBACK_URL",
            "checkpoint_verifier_ref": "MY_DATA_HUB_KAGGLE_CHECKPOINT_VERIFIER_REF",
            "checkpoint_verifier_source_file": "MY_DATA_HUB_KAGGLE_CHECKPOINT_VERIFIER_SOURCE_FILE",
            "tunnel_gateway_host": "MY_DATA_HUB_MASTER_TUNNEL_GATEWAY_HOST",
            "tunnel_gateway_port": "MY_DATA_HUB_MASTER_TUNNEL_GATEWAY_PORT",
            "tunnel_gateway_user": "MY_DATA_HUB_MASTER_TUNNEL_GATEWAY_USER",
            "tunnel_remote_port": "MY_DATA_HUB_MASTER_TUNNEL_REMOTE_PORT",
        }
        raw = {key: os.getenv(name, "").strip() for key, name in names.items()}
        if not any(raw.values()):
            return None
        if not all(raw.values()):
            # Partial launch configuration never causes a best-effort provider call.
            return None
        raw.update({
            "runtime_image_identity": os.getenv(
                "MY_DATA_HUB_EMBEDDING_RUNTIME_IMAGE_IDENTITY",
                "gcr.io/kaggle-images/python@sha256:c1fa4de30bc268e601e6dcddb6ceb2519b9adde3527dbbfb05e6bdfbbbdcd1a2",
            ).strip(),
            "runtime_image_source_commit": os.getenv(
                "MY_DATA_HUB_EMBEDDING_RUNTIME_SOURCE_COMMIT", "fc61d5cda7da39530055bae9bd0e92865f995cd9"
            ).strip(),
            "runtime_python_series": os.getenv("MY_DATA_HUB_EMBEDDING_RUNTIME_PYTHON_SERIES", "3.12").strip(),
        })
        ydb_names = {
            "ydb_endpoint": "MY_DATA_HUB_YDB_ENDPOINT",
            "ydb_database": "MY_DATA_HUB_YDB_DATABASE",
            "ydb_reader_service_account_id": "MY_DATA_HUB_YDB_READER_SERVICE_ACCOUNT_ID",
        }
        ydb_values = {key: os.getenv(name, "").strip() for key, name in ydb_names.items()}
        if any(ydb_values.values()) and not all(ydb_values.values()):
            raise ValueError("YDB master runtime configuration must be complete")
        if all(ydb_values.values()):
            raw.update(ydb_values)
        callback = urlsplit(raw["callback_url"])
        if (
            raw["callback_url"] != CANONICAL_RUNTIME_CALLBACK_URL
            or callback.scheme != "https"
            or callback.hostname != "mcp-datahub.kenigevents.ru"
            or callback.port is not None
            or callback.username
            or callback.password
            or callback.query
            or callback.fragment
        ):
            raise ValueError("runtime callback must use the owner-approved canonical HTTPS audience")
        dataset_dir = Path(raw.pop("dataset_dir")).expanduser().resolve()
        notebook_source = Path(raw.pop("notebook_source")).expanduser().resolve()
        try:
            tunnel_gateway_port = int(raw.pop("tunnel_gateway_port"))
            tunnel_remote_port = int(raw.pop("tunnel_remote_port"))
        except ValueError as exc:
            raise ValueError("master tunnel ports must be integers") from exc
        files = _bounded_files(dataset_dir)
        source = _bounded_file(notebook_source, max_bytes=8 * 1024 * 1024)
        probe_relations_raw = os.getenv("MY_DATA_HUB_KAGGLE_CHECKPOINT_PROBE_RELATIONS_JSON", "").strip()
        try:
            probe_relations_value = json.loads(probe_relations_raw)
        except json.JSONDecodeError as exc:
            raise ValueError("checkpoint probe relations must be a JSON array") from exc
        if not isinstance(probe_relations_value, list) or not all(
            isinstance(value, str) for value in probe_relations_value
        ):
            raise ValueError("checkpoint probe relations must be a JSON string array")
        bindings_raw = os.getenv("MY_DATA_HUB_KAGGLE_MASTER_SECRET_BINDINGS_JSON", "{}").strip()
        try:
            bindings = json.loads(bindings_raw)
        except json.JSONDecodeError as exc:
            raise ValueError("master secret bindings must be a JSON object") from exc
        if not isinstance(bindings, dict) or not all(
            isinstance(key, str) and isinstance(value, str) for key, value in bindings.items()
        ):
            raise ValueError("master secret bindings must map environment names to Kaggle secret names")
        assets = KaggleMasterLaunchAssets(
            **raw,
            tunnel_gateway_port=tunnel_gateway_port,
            tunnel_remote_port=tunnel_remote_port,
            dataset_files=files,
            notebook_source=source,
            runtime_secret_bindings=bindings,
            checkpoint_probe_relations=tuple(probe_relations_value),
        )
        return cls(assets=assets)


@dataclass(slots=True)
class ControlPlaneMasterRuntime:
    ledger: ControlLedger
    coordinator: MasterCoordinator
    settings: MasterRuntimeSettings
    acceptance_executor: KaggleAcceptanceOperationExecutor | None = None
    master_tls_ca_path: Path | None = None

    def request_master_acceptance(
        self,
        request: MasterAcceptanceRequest,
        principal: AcceptancePrincipal,
    ) -> tuple[dict[str, Any], bool]:
        """Persist one fixed scenario under the dedicated operator scope."""

        require_acceptance_operator(principal)
        return self.ledger.ensure_master_acceptance_task(
            task_id=str(request.task_id),
            scenario_id=request.scenario.value,
            idempotency_key=request.idempotency_key,
            request_sha256=request.request_sha256,
            principal_id=principal.subject,
            client_id=principal.client_id,
            source_revision=request.source_revision,
            target_operation_id=(str(request.target_operation_id) if request.target_operation_id is not None else None),
        )

    def bind_master_acceptance(self, task_id: str, operation_id: str) -> dict[str, Any]:
        """Bind an admitted FM04/FM07 after its fixed real provider ensure(s)."""

        return self.ledger.bind_master_acceptance_task(task_id=task_id, operation_id=operation_id)

    def complete_master_acceptance(
        self,
        receipt: MasterAcceptanceReceipt,
        principal: AcceptancePrincipal,
    ) -> dict[str, Any]:
        """Complete only an owner-host claim under its principal/client CAS."""

        require_acceptance_operator(principal)
        return self.ledger.complete_master_acceptance_host_command(
            command_id=str(receipt.command_id),
            command_sha256=receipt.command_sha256,
            principal_id=principal.subject,
            client_id=principal.client_id,
            receipt=receipt.model_dump(mode="json"),
        )

    def record_connector_heartbeat(
        self,
        *,
        run_id: str,
        attempt_id: str,
        runtime_token: str,
        connector_kind: str,
        contract_version: str,
        state: str,
        observed_at: Any,
    ) -> None:
        if not self.ledger.runtime_token_valid(run_id, attempt_id, runtime_token):
            raise PermissionError("runtime token is invalid")
        operation = self.ledger.operation_for_attempt(run_id, attempt_id)
        service = self.ledger.resolve_service("postgres-master")
        if (
            operation is None
            or service is None
            or operation.state != "ACTIVE"
            or service.run_id != run_id
            or service.attempt_id != attempt_id
            or int(operation.identity.get("epoch", 0)) != service.epoch
        ):
            raise PermissionError("runtime epoch is not ACTIVE")
        self.ledger.record_connector_coverage(
            connector_kind=connector_kind,
            contract_version=contract_version,
            state=state,
            observed_at=observed_at,
        )

    def intent(self, idempotency_key: str) -> MasterIntent:
        assets = self.settings.assets
        return MasterIntent(
            idempotency_key=idempotency_key,
            source_identity=assets.source_identity,
            source_version=assets.source_version,
            checkpoint_ref=assets.checkpoint_ref,
            dataset_ref=assets.dataset_ref,
            notebook_ref=assets.notebook_ref,
        )

    def ensure(self, idempotency_key: str) -> tuple[MasterHandle, bool]:
        identity = MasterCoordinator.identity_for(idempotency_key)
        intent = self.intent(idempotency_key)
        operation, created = self.ledger.ensure_master_operation(
            operation_id=identity["operation_id"],
            idempotency_key=idempotency_key,
            intent=intent.as_dict(),
            identity=identity,
        )
        durable = operation.identity
        self.ledger.record_attempt(
            attempt_id=str(durable["attempt_id"]),
            run_id=str(durable["run_id"]),
            operation_id=operation.operation_id,
            source_identity=intent.source_identity,
            source_version=intent.source_version,
            service_instance_id=str(durable["service_instance_id"]),
            master_instance_id=str(durable["master_instance_id"]),
            epoch=int(durable["epoch"]),
            state=MasterState.REQUESTED.value,
        )
        if not self._status_dataset_ready(operation.operation_id, durable):
            current = self.ledger.get_operation(operation.operation_id)
            assert current is not None
            return self._handle(current), not created
        handle = self.coordinator.ensure_master(intent)
        self._cleanup_terminal_status_dataset(handle)
        return handle, not created

    def reconcile_startup(self) -> list[MasterHandle]:
        handles: list[MasterHandle] = []
        for operation in self.ledger.incomplete_operations("ensure_master"):
            recovery = self.ledger.fm08_recovery_for_operation(operation.operation_id)
            owner = self._fm08_recovery_runtime(recovery) if recovery is not None else self
            if not owner._status_dataset_ready(operation.operation_id, operation.identity):
                current = self.ledger.get_operation(operation.operation_id)
                assert current is not None
                handles.append(owner._handle(current))
                continue
            handle = owner.coordinator.ensure_master(owner.intent(operation.idempotency_key))
            owner._cleanup_terminal_status_dataset(handle)
            handles.append(handle)
        return handles

    def recover_abrupt_master(self, command: Any, capture: Any) -> Any:
        """Terminate the exact FM08 old run and start one distinct next epoch.

        The command/capture types are imported lazily to keep the control
        runtime independent of the acceptance assembly cycle. All identities
        are derived from the already owner-claimed task; no provider ref,
        payload, clock, token, or command is caller supplied.
        """

        from my_data_hub.acceptance.master_lifecycle import (
            MasterAcceptanceBinding,
            MasterAcceptanceCommandKind,
        )
        from my_data_hub.acceptance.master_production import (
            AbruptMasterRecoveryReceipt,
            ProductionAcceptanceBlocked,
        )
        from my_data_hub.providers.kaggle.contracts import KaggleKernelRunIdentity

        if command.command_kind is not MasterAcceptanceCommandKind.CALLBACK_LOSS_RECOVERY:
            raise ValueError("abrupt master recovery received another command")
        provider = self.coordinator.provider
        if not isinstance(provider, KaggleMasterRuntimeProvider):
            raise ProductionAcceptanceBlocked("FM08_OFFICIAL_KAGGLE_ADAPTER_REQUIRED")
        trigger = self.ledger.get_effect_by_idempotency_key(f"{command.binding.operation_id}:trigger_run")
        try:
            old_run = KaggleKernelRunIdentity.model_validate(
                trigger.receipt["exact_identity"] if trigger is not None and trigger.receipt else None
            )
        except (TypeError, ValueError) as exc:
            raise ProductionAcceptanceBlocked("FM08_OLD_PROVIDER_RUN_RECEIPT_INVALID") from exc
        if old_run.task_run_id != command.binding.run_id or old_run.provider_ref != self.settings.assets.notebook_ref:
            raise ProductionAcceptanceBlocked("FM08_OLD_PROVIDER_RUN_BINDING_MISMATCH")
        owner = self.settings.assets.notebook_ref.split("/", 1)[0]
        replacement_key = f"fm08-recovery:{command.task_id}"
        replacement_ref = f"{owner}/mdh-master-fm08-{command.task_id.hex}"
        row, _created = self.ledger.ensure_fm08_abrupt_recovery(
            task_id=str(command.task_id),
            command_id=str(command.command_id),
            command_sha256=command.command_sha256,
            old_operation_id=str(command.binding.operation_id),
            old_run=old_run.model_dump(mode="json"),
            old_epoch=command.binding.epoch,
            replacement_idempotency_key=replacement_key,
            replacement_notebook_ref=replacement_ref,
        )
        if row["state"] == "INTENT":
            old_operation = self.ledger.get_operation(str(command.binding.operation_id))
            if old_operation is None:
                raise ProductionAcceptanceBlocked("FM08_OLD_OPERATION_MISSING")
            termination = provider.terminate_run_for_fm08(
                task_id=command.task_id,
                operation_id=command.binding.operation_id,
                run=old_run,
                requested_at=old_operation.created_at,
            )
            termination_payload = termination.model_dump(mode="json")
            termination_sha256 = hashlib.sha256(canonical_json_bytes(termination_payload)).hexdigest()
            row = self.ledger.fence_fm08_abrupt_master(
                task_id=str(command.task_id),
                termination_receipt=termination_payload,
                termination_receipt_sha256=termination_sha256,
            )
            self.coordinator.deactivate_terminal_operation(
                str(command.binding.operation_id), "fm08_abrupt_master_terminated"
            )
            current_old = self.ledger.get_operation(str(command.binding.operation_id))
            assert current_old is not None
            self._cleanup_terminal_status_dataset(self._handle(current_old))
        recovery_runtime = self._fm08_recovery_runtime(row)
        replacement, _duplicate = recovery_runtime.ensure(replacement_key)
        self.ledger.bind_fm08_replacement(
            task_id=str(command.task_id),
            replacement_operation_id=replacement.operation_id,
            replacement_run=None,
            recovery_receipt_sha256=None,
            active=False,
        )
        replacement_trigger = self.ledger.get_effect_by_idempotency_key(f"{replacement.operation_id}:trigger_run")
        try:
            replacement_run = KaggleKernelRunIdentity.model_validate(
                replacement_trigger.receipt["exact_identity"]
                if replacement_trigger is not None and replacement_trigger.receipt
                else None
            )
        except (TypeError, ValueError) as exc:
            raise ProductionAcceptanceBlocked("FM08_RECOVERY_PROVIDER_RUN_PENDING") from exc
        recovery_payload = {
            "schema_version": "my-data-hub-fm08-abrupt-recovery.v1",
            "task_id": str(command.task_id),
            "old_operation_id": str(command.binding.operation_id),
            "old_provider_run_ref": old_run.provider_run_ref,
            "old_provider_kernel_id": old_run.provider_kernel_id,
            "old_epoch": command.binding.epoch,
            "replacement_operation_id": replacement.operation_id,
            "replacement_provider_run_ref": replacement_run.provider_run_ref,
            "replacement_provider_kernel_id": replacement_run.provider_kernel_id,
            "replacement_epoch": replacement.epoch,
            "captured_event_id": str(capture.event_id),
            "captured_body_sha256": capture.body_sha256,
        }
        recovery_sha256 = hashlib.sha256(canonical_json_bytes(recovery_payload)).hexdigest()
        row = self.ledger.bind_fm08_replacement(
            task_id=str(command.task_id),
            replacement_operation_id=replacement.operation_id,
            replacement_run=replacement_run.model_dump(mode="json"),
            recovery_receipt_sha256=recovery_sha256,
            active=replacement.state is MasterState.ACTIVE,
        )
        if replacement.state is not MasterState.ACTIVE:
            raise ProductionAcceptanceBlocked("FM08_RECOVERY_NOT_ACTIVE")
        binding = MasterAcceptanceBinding(
            operation_id=UUID(replacement.operation_id),
            run_id=UUID(replacement.run_id),
            attempt_id=UUID(replacement.attempt_id),
            service_instance_id=replacement.service_instance_id,
            master_instance_id=UUID(replacement.master_instance_id),
            epoch=replacement.epoch,
        )
        return AbruptMasterRecoveryReceipt(
            old_binding=command.binding,
            recovery_binding=binding,
            old_provider_run_ref=old_run.provider_run_ref,
            old_provider_kernel_id=old_run.provider_kernel_id,
            recovery_provider_run_ref=replacement_run.provider_run_ref,
            recovery_provider_kernel_id=replacement_run.provider_kernel_id,
            termination_receipt_sha256=str(row["termination_receipt_sha256"]),
            recovery_receipt_sha256=str(row["recovery_receipt_sha256"]),
        )

    def _fm08_recovery_runtime(self, row: Mapping[str, Any]) -> ControlPlaneMasterRuntime:
        provider = self.coordinator.provider
        if not isinstance(provider, KaggleMasterRuntimeProvider):
            raise MasterProviderUnavailable("FM08 recovery requires the official Kaggle adapter")
        assets = replace(
            self.settings.assets,
            notebook_ref=str(row["replacement_notebook_ref"]),
        )
        recovery_provider = KaggleMasterRuntimeProvider(
            provider.adapter,
            assets,
            status_authority=self.ledger,
            ydb_access_token=provider._ydb_access_token,
        )
        recovery_coordinator = MasterCoordinator(
            self.ledger,
            recovery_provider,
            lease_ttl=self.coordinator.lease_ttl,
            tunnel_authority=self.coordinator.tunnel_authority,
            tunnel_listen_port=self.coordinator.tunnel_listen_port,
        )
        return ControlPlaneMasterRuntime(
            ledger=self.ledger,
            coordinator=recovery_coordinator,
            settings=MasterRuntimeSettings(assets),
            acceptance_executor=self.acceptance_executor,
            master_tls_ca_path=self.master_tls_ca_path,
        )

    def _status_dataset_ready(self, operation_id: str, identity: dict[str, Any]) -> bool:
        provider = self.coordinator.provider
        if not isinstance(provider, KaggleMasterRuntimeProvider):
            return True
        existing = self.ledger.master_status_dataset_authority(operation_id)
        if existing is not None:
            if existing["status_dataset"] is not None:
                return existing["state"] in {"READY", "CLEANED"}
            claim_until = datetime.fromisoformat(str(existing["creator_claim_until"]).replace("Z", "+00:00"))
            if claim_until > self.ledger.clock.now():
                return False
            self.ledger.fail_ambiguous_master_status_dataset(operation_id)
            self._cleanup_ambiguous_status_dataset(provider, operation_id, identity)
            self._release_status_resource_lease(identity)
            return False

        token = secrets.token_hex(32)
        deadline = self.ledger.clock.now() + timedelta(seconds=900)
        lease = self.ledger.acquire_resource_lease(
            lease_id=self._status_lease_id(operation_id),
            resource_kind="kaggle_notebook",
            resource_ref=self.settings.assets.notebook_ref,
            holder_id=str(identity["run_id"]),
            lease_until=self.ledger.clock.now() + timedelta(seconds=self.settings.assets.notebook_timeout_seconds),
        )
        resource_lease = {
            "lease_id": lease.lease_id,
            "resource_kind": lease.resource_kind,
            "resource_ref": lease.resource_ref,
            "holder_id": lease.holder_id,
            "epoch": lease.epoch,
            "lease_until": lease.lease_until.isoformat(),
        }
        candidate_identity = {
            **identity,
            "operation_id": operation_id,
            "status_resource_lease": resource_lease,
            "boot_checkpoint": self._boot_checkpoint_snapshot(),
        }
        tls_certificate, tls_private_key = self._generate_master_tls(candidate_identity)
        files = provider.status_files(
            candidate_identity,
            token,
            tls_certificate=tls_certificate,
            tls_private_key=tls_private_key,
        )
        authority, created = self.ledger.ensure_master_status_dataset_authority(
            operation_id=operation_id,
            run_id=str(identity["run_id"]),
            attempt_id=str(identity["attempt_id"]),
            token=token,
            creator_claim_until=deadline,
            expected_content_tree_sha256=mapping_sha256(files),
            resource_lease=resource_lease,
        )
        if not created:
            return authority["status_dataset"] is not None
        exact_identity = {
            **candidate_identity,
            "status_requested_at": authority["created_at"],
        }
        result = provider.create_status_dataset(exact_identity, files)
        claim = result.claim
        self._publish_master_tls_ca(tls_certificate)
        self.ledger.record_master_status_dataset(
            operation_id=operation_id,
            status_dataset={
                "provider_ref": claim.provider_ref,
                "exact_version_ref": f"{claim.provider_ref}/{claim.provider_version}",
                "claim": claim.model_dump(mode="json"),
                "content_tree_sha256": mapping_sha256(files),
                "status_config_sha256": hashlib.sha256(files["kaggle_run.json"]).hexdigest(),
                "status_helper_sha256": hashlib.sha256(files["kaggle_status_client.py"]).hexdigest(),
                "master_config_sha256": hashlib.sha256(files["master-config.json"]).hexdigest(),
                "tls_certificate_sha256": hashlib.sha256(tls_certificate).hexdigest(),
                "tls_key_material_sha256": hashlib.sha256(tls_private_key).hexdigest(),
                "tls_certificate_pem": tls_certificate.decode("ascii"),
                "boot_checkpoint": candidate_identity["boot_checkpoint"],
                "resource_lease": resource_lease,
            },
        )
        return True

    def _generate_master_tls(self, identity: Mapping[str, Any]) -> tuple[bytes, bytes]:
        operation_id = str(identity["operation_id"])
        master_instance_id = UUID(str(identity["master_instance_id"]))
        epoch = int(identity["epoch"])
        if epoch < 1:
            raise MasterProviderUnavailable("master TLS epoch is invalid")
        key = rsa.generate_private_key(public_exponent=65537, key_size=3072)
        now = self.ledger.clock.now()
        subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, f"mdh-postgres-{master_instance_id}")])
        binding = canonical_json_bytes(
            {
                "schema_version": "my-data-hub-master-tls-binding.v1",
                "operation_id": operation_id,
                "run_id": str(identity["run_id"]),
                "attempt_id": str(identity["attempt_id"]),
                "master_instance_id": str(master_instance_id),
                "epoch": epoch,
            }
        )
        certificate = (
            x509.CertificateBuilder()
            .subject_name(subject)
            .issuer_name(subject)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now - timedelta(minutes=5))
            .not_valid_after(now + timedelta(hours=12))
            .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
            .add_extension(
                x509.SubjectAlternativeName([x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]),
                critical=False,
            )
            .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]), critical=False)
            .add_extension(
                x509.UnrecognizedExtension(ObjectIdentifier("1.3.6.1.4.1.57264.1.1"), binding),
                critical=False,
            )
            .sign(key, hashes.SHA256())
        )
        return (
            certificate.public_bytes(serialization.Encoding.PEM),
            key.private_bytes(
                serialization.Encoding.PEM,
                serialization.PrivateFormat.PKCS8,
                serialization.NoEncryption(),
            ),
        )

    def _publish_master_tls_ca(self, certificate: bytes) -> None:
        path = self.master_tls_ca_path
        if path is None:
            raise MasterProviderUnavailable("master TLS CA publication path is unavailable")
        path = path.resolve(strict=False)
        parent = path.parent
        if parent.is_symlink() or not parent.is_dir():
            raise MasterProviderUnavailable("master TLS CA directory is unavailable")
        if path.exists() and (path.is_symlink() or not path.is_file()):
            raise MasterProviderUnavailable("master TLS CA target is unsafe")
        descriptor, temporary_name = tempfile.mkstemp(prefix=".ca.pem.", dir=parent)
        temporary = Path(temporary_name)
        try:
            os.fchmod(descriptor, 0o600)
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(certificate)
                stream.flush()
                os.fsync(stream.fileno())
            temporary.replace(path)
            os.chmod(path, 0o600)
            directory_descriptor = os.open(parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory_descriptor)
            finally:
                os.close(directory_descriptor)
        finally:
            temporary.unlink(missing_ok=True)

    def _boot_checkpoint_snapshot(self) -> dict[str, Any]:
        head = self.ledger.checkpoint_head("postgres-master")
        if head is None or head.current_checkpoint_id is None:
            return {"kind": "EMPTY", "generation": 0}
        candidate = self.ledger.checkpoint_candidate(head.current_checkpoint_id)
        if (
            candidate is None
            or candidate.get("status") != "VERIFIED"
            or candidate.get("dataset_ref") != self.settings.assets.checkpoint_ref
            or not isinstance(candidate.get("version_ref"), str)
            or not str(candidate["version_ref"]).startswith(self.settings.assets.checkpoint_ref + "/")
            or not str(candidate["version_ref"]).rsplit("/", 1)[-1].isdigit()
            or int(str(candidate["version_ref"]).rsplit("/", 1)[-1]) < 1
        ):
            raise MasterProviderUnavailable("current checkpoint HEAD is not exact VERIFIED metadata")
        return {
            "kind": "VERIFIED",
            "generation": head.generation,
            "checkpoint_id": head.current_checkpoint_id,
            "exact_version_ref": candidate["version_ref"],
            "manifest_sha256": candidate["manifest_sha256"],
        }

    def _cleanup_terminal_status_dataset(self, handle: MasterHandle) -> None:
        if handle.state not in {
            MasterState.STOPPED,
            MasterState.FAILED,
            MasterState.FENCED,
            MasterState.ORPHANED,
        }:
            return
        stored = self.ledger.master_status_dataset_authority(handle.operation_id)
        if stored is None or stored["cleanup_receipt"] is not None:
            return
        if not self.ledger.claim_master_status_dataset_cleanup(
            handle.operation_id,
            claim_until=self.ledger.clock.now() + timedelta(seconds=900),
        ):
            return
        stored = self.ledger.master_status_dataset_authority(handle.operation_id)
        assert stored is not None
        status = stored["status_dataset"]
        if not isinstance(status, dict):
            return
        provider = self.coordinator.provider
        if not isinstance(provider, KaggleMasterRuntimeProvider):
            raise MasterProviderUnavailable("master status Dataset provider is unavailable")
        operation = self.ledger.get_operation(handle.operation_id)
        assert operation is not None
        receipt = provider.delete_status_dataset(
            {
                **operation.identity,
                "operation_id": handle.operation_id,
                "status_requested_at": stored["created_at"],
            },
            TaskResourceClaim.model_validate(status["claim"]),
        )
        lease = status["resource_lease"]
        self.ledger.release_resource_lease_exact(str(lease["lease_id"]), str(lease["holder_id"]), int(lease["epoch"]))
        self.ledger.complete_master_status_dataset_cleanup(
            operation_id=handle.operation_id,
            cleanup_receipt=receipt.model_dump(mode="json"),
        )

    def reconcile_status_cleanup_once(self) -> str | None:
        candidates = self.ledger.terminal_master_status_dataset_authorities(limit=1)
        if not candidates:
            return None
        operation = self.ledger.get_operation(str(candidates[0]["operation_id"]))
        if operation is None:
            raise RuntimeError("terminal master status authority lost its operation")
        self._cleanup_terminal_status_dataset(self._handle(operation))
        return operation.operation_id

    def _release_status_resource_lease(self, identity: dict[str, Any]) -> None:
        stored = self.ledger.master_status_dataset_authority(str(identity["operation_id"]))
        if stored is None:
            raise RuntimeError("master status authority disappeared before lease release")
        lease = stored["resource_lease"]
        self.ledger.release_resource_lease_exact(str(lease["lease_id"]), str(lease["holder_id"]), int(lease["epoch"]))

    def _cleanup_ambiguous_status_dataset(
        self,
        provider: KaggleMasterRuntimeProvider,
        operation_id: str,
        identity: dict[str, Any],
    ) -> None:
        effect_id = str(uuid5(NAMESPACE_URL, f"master-status-create:{operation_id}"))
        payload = self.ledger.provider_resource_claim_for_effect(effect_id)
        claim: TaskResourceClaim | None = None
        if payload is not None:
            claim = TaskResourceClaim.model_validate(payload)
        else:
            receipt_payload = self.ledger.latest_provider_effect_receipt(effect_id)
            if receipt_payload is not None:
                receipt = ProviderEffectReceipt.model_validate(receipt_payload)
                if (
                    receipt.observed_fingerprint is not None
                    and receipt.provider_version is not None
                    and receipt.outcome.value in {"applied", "already_applied"}
                ):
                    stored = self.ledger.master_status_dataset_authority(operation_id)
                    assert stored is not None
                    claim = TaskResourceClaim.create(
                        task_id=UUID(str(identity["run_id"])),
                        effect_id=UUID(effect_id),
                        provider_ref=provider.status_dataset_ref(identity),
                        kind=ProviderKind.DATASET,
                        control_class=ControlClass.ORCHESTRATOR_PROTECTED,
                        disposable=True,
                        fingerprint=receipt.observed_fingerprint,
                        provider_version=receipt.provider_version,
                        registered_at=datetime.fromisoformat(str(stored["created_at"]).replace("Z", "+00:00")),
                    )
        if claim is None:
            return
        expected_ref = provider.status_dataset_ref(identity)
        if (
            claim.task_id != UUID(str(identity["run_id"]))
            or claim.provider_ref != expected_ref
            or claim.control_class is not ControlClass.ORCHESTRATOR_PROTECTED
            or not claim.disposable
            or claim.provider_version != 1
        ):
            raise RuntimeError("ambiguous master status claim differs from deterministic owner task")
        stored = self.ledger.master_status_dataset_authority(operation_id)
        assert stored is not None
        if stored["cleanup_receipt"] is not None:
            return
        receipt = provider.delete_status_dataset(
            {
                **identity,
                "operation_id": operation_id,
                "status_requested_at": stored["created_at"],
            },
            claim,
        )
        self.ledger.complete_ambiguous_master_status_dataset_cleanup(
            operation_id=operation_id,
            cleanup_receipt=receipt.model_dump(mode="json"),
        )

    @staticmethod
    def _status_lease_id(operation_id: str) -> str:
        return str(uuid5(NAMESPACE_URL, f"master-status-lease:{operation_id}"))

    @staticmethod
    def _handle(operation: Any) -> MasterHandle:
        return MasterHandle(
            operation_id=operation.operation_id,
            run_id=str(operation.identity["run_id"]),
            attempt_id=str(operation.identity["attempt_id"]),
            service_instance_id=str(operation.identity["service_instance_id"]),
            master_instance_id=str(operation.identity["master_instance_id"]),
            epoch=int(operation.identity["epoch"]),
            state=MasterState(operation.state),
        )

    def reconcile_requested_once(self) -> MasterHandle | None:
        """Claim one MCP cold-start request and drive the real provider lifecycle."""

        request = self.ledger.claim_master_request()
        if request is None:
            return None
        existing = self.ledger.get_operation(str(request["operation_id"]))
        if existing is not None and existing.state in {
            MasterState.STOPPED.value,
            MasterState.FAILED.value,
            MasterState.FENCED.value,
            MasterState.ORPHANED.value,
        }:
            # The provider lifecycle may become terminal immediately before a
            # control-process restart, leaving its bridge request claimed but
            # unacknowledged.  Replaying ``ensure`` cannot make that immutable
            # operation non-terminal and used to release the request forever.
            # Consume the recovered bridge so the resolver exposes ABSENT and
            # the next cold call can allocate exactly one new epoch.
            self.ledger.complete_master_request(
                str(request["request_id"]), existing.operation_id
            )
            return self._handle(existing)
        try:
            handle, _duplicate = self.ensure(str(request["idempotency_key"]))
            if handle.operation_id != str(request["operation_id"]):
                raise RuntimeError("master request operation identity differs from coordinator")
            # A real provider effect can be durable while its local
            # post-effect projection is not yet recoverable (for example,
            # after response loss).  REQUESTED is therefore not a consumed
            # bridge request: release it so the bounded reconcile loop keeps
            # driving the same operation instead of stranding it forever.
            if handle.state is MasterState.REQUESTED:
                self.ledger.release_master_request(str(request["request_id"]))
                return handle
            self.ledger.complete_master_request(str(request["request_id"]), handle.operation_id)
            return handle
        except Exception:
            self.ledger.release_master_request(str(request["request_id"]))
            raise

    def reconcile_incomplete_once(self) -> list[MasterHandle]:
        """Poll every admitted master lifecycle after its bridge request is consumed.

        The request bridge is deliberately completed once all three launch
        effects are durable.  The provider Notebook is still REGISTERING at
        that point, so a separate bounded reconciliation pass must continue
        observing its exact run for an authenticated callback or a terminal
        provider result.
        """

        operations = self.ledger.incomplete_operations("ensure_master")
        intents = {
            operation.idempotency_key: self.intent(operation.idempotency_key)
            for operation in operations
        }
        return self.coordinator.reconcile_all(intents)

    def reconcile_acceptance_once(self) -> dict[str, Any] | None:
        self.ledger.record_acceptance_consumer_heartbeat(self.acceptance_executor is not None)
        operations = [
            *self.ledger.incomplete_operations("checkpoint_restore_smoke"),
            *self.ledger.incomplete_operations("forced_master_rotation"),
        ]
        if not operations:
            return None
        operation = sorted(operations, key=lambda item: item.created_at)[0]
        timeout_seconds = int(operation.identity.get("timeout_seconds", 0))
        elapsed_seconds = (self.ledger.clock.now() - operation.created_at).total_seconds()
        remaining_seconds = int(timeout_seconds - elapsed_seconds)
        if timeout_seconds < 60 or remaining_seconds < 60:
            self.ledger.transition_operation(
                operation.operation_id,
                expected_state=operation.state,
                new_state="FAILED",
                metadata={"code": "ACCEPTANCE_OPERATION_TIMEOUT"},
            )
            return {"operation_id": operation.operation_id, "state": "FAILED"}
        checkpoint_id = str(operation.identity.get("checkpoint_id", ""))
        candidate = self.ledger.checkpoint_candidate(checkpoint_id)
        if candidate is None or candidate.get("status") != "VERIFIED":
            self.ledger.transition_operation(
                operation.operation_id,
                expected_state=operation.state,
                new_state="FAILED",
                metadata={"code": "CHECKPOINT_BINDING_INVALID"},
            )
            return {"operation_id": operation.operation_id, "state": "FAILED"}
        if operation.operation_kind == "forced_master_rotation":
            head = self.ledger.checkpoint_head("postgres-master")
            manifest = candidate.get("manifest")
            source_operation = self.ledger.get_operation(str(candidate.get("operation_id", "")))
            source_identity = source_operation.identity if source_operation is not None else {}
            replacement_identity = MasterCoordinator.identity_for(f"forced-rotation:{operation.operation_id}")
            active_service = self.ledger.resolve_service("postgres-master")
            replacement_is_active = active_service is not None and (
                active_service.run_id == replacement_identity["run_id"]
                and active_service.attempt_id == replacement_identity["attempt_id"]
                and active_service.master_instance_id == replacement_identity["master_instance_id"]
                and active_service.epoch > int(operation.identity.get("expected_active_epoch", 0))
            )
            if (
                head is None
                or head.current_checkpoint_id != checkpoint_id
                or head.generation != int(operation.identity.get("head_generation", -1))
                or not isinstance(manifest, dict)
                or manifest.get("canonical_revision") != operation.identity.get("expected_canonical_revision")
                or source_operation is None
                or source_operation.state != "STOPPED"
                or candidate.get("epoch") != operation.identity.get("expected_active_epoch")
                or source_identity.get("epoch") != operation.identity.get("expected_active_epoch")
                or candidate.get("master_instance_id") != source_identity.get("master_instance_id")
                or (active_service is not None and not replacement_is_active)
            ):
                self.ledger.transition_operation(
                    operation.operation_id,
                    expected_state=operation.state,
                    new_state="FAILED",
                    metadata={"code": "ROTATION_CHECKPOINT_BINDING_STALE"},
                )
                return {"operation_id": operation.operation_id, "state": "FAILED"}
        if operation.operation_kind == "checkpoint_restore_smoke":
            if self.acceptance_executor is None:
                raise MasterProviderUnavailable("restore provider is unavailable")
            receipt = self.acceptance_executor.restore(
                operation.operation_id,
                candidate,
                timeout_seconds=min(remaining_seconds, 1800),
            )
            if (self.ledger.clock.now() - operation.created_at).total_seconds() > timeout_seconds:
                self.ledger.transition_operation(
                    operation.operation_id,
                    expected_state=operation.state,
                    new_state="FAILED",
                    metadata={"code": "ACCEPTANCE_OPERATION_TIMEOUT"},
                )
                return {"operation_id": operation.operation_id, "state": "FAILED"}
            self.ledger.transition_operation(
                operation.operation_id,
                expected_state=operation.state,
                new_state="DURABLE_COMPLETE",
                metadata=receipt,
            )
            return {"operation_id": operation.operation_id, "state": "DURABLE_COMPLETE"}
        rotated, _duplicate = self.ensure(f"forced-rotation:{operation.operation_id}")
        if rotated.state.value == "ACTIVE":
            self.ledger.transition_operation(
                operation.operation_id,
                expected_state=operation.state,
                new_state="DURABLE_COMPLETE",
                metadata={"replacement_operation_id": rotated.operation_id, "epoch": rotated.epoch},
            )
            return {"operation_id": operation.operation_id, "state": "DURABLE_COMPLETE"}
        return {"operation_id": operation.operation_id, "state": operation.state}


@dataclass(frozen=True, slots=True)
class ProductionRuntimeBuild:
    master: ControlPlaneMasterRuntime | None
    provider_status: str
    session_registrar: SessionCredentialRegistrar | None = None
    provider_adapter: KaggleProviderAdapter | None = None
    checkpoint_broker: BrokeredCheckpointUploadService | None = None


def _checkpoint_verifier_assets_from_verified_master_claim(
    ledger: ControlLedger,
    assets: KaggleMasterLaunchAssets,
    *,
    timeout_seconds: int,
) -> KaggleCheckpointVerifierAssets:
    """Project a verifier runtime only from the exact durable master asset effect."""

    claim = ledger.latest_provider_resource_claim(
        provider_ref=assets.dataset_ref,
        resource_kind=ProviderKind.DATASET.value,
        control_class=ControlClass.ORCHESTRATOR_PROTECTED.value,
    )
    if claim is None:
        raise MasterProviderUnavailable("exact master asset Dataset claim is unavailable")
    effect_id = str(claim.get("effect_id", ""))
    authority = ledger.provider_effect_authority(effect_id)
    receipt = ledger.latest_successful_provider_effect_receipt(effect_id)
    expected_key = f"{authority['operation_id']}:ensure_dataset" if authority is not None else ""
    expected_arguments_sha256 = sha256_value(
        {
            "content_tree_sha256": KaggleMasterRuntimeProvider._mapping_sha(assets.dataset_files),
            "control_class": ControlClass.ORCHESTRATOR_PROTECTED.value,
            "disposable": False,
        }
    )
    if (
        authority is None
        or authority.get("action") != "create_dataset"
        or authority.get("provider_ref") != assets.dataset_ref
        or ledger.provider_effect_idempotency_key(effect_id) != expected_key
        or str(uuid5(NAMESPACE_URL, expected_key)) != effect_id
        or ledger.provider_effect_arguments_sha256(effect_id) != expected_arguments_sha256
        or claim.get("kind") != ProviderKind.DATASET.value
        or claim.get("control_class") != ControlClass.ORCHESTRATOR_PROTECTED.value
        or claim.get("disposable") is not False
        or receipt is None
        or receipt.get("provider_version") != claim.get("provider_version")
        or receipt.get("observed_fingerprint") != claim.get("fingerprint")
        or receipt.get("outcome") not in {"applied", "already_applied"}
    ):
        raise MasterProviderUnavailable("master asset Dataset claim does not bind the exact build effect")
    try:
        version = int(claim["provider_version"])
    except (KeyError, TypeError, ValueError) as exc:
        raise MasterProviderUnavailable("master asset Dataset claim has no exact numeric version") from exc
    try:
        wheel, wheel_bytes = assets.project_wheel()
    except MasterLaunchContractError as exc:
        raise MasterProviderUnavailable("verified master assets lack one exact verifier wheel") from exc
    if version < 1:
        raise MasterProviderUnavailable("master asset Dataset claim has no exact numeric version")
    try:
        verifier_source = assets.dataset_files[assets.checkpoint_verifier_source_file]
        archive = assets.dataset_files[POSTGRES_RUNTIME_ARCHIVE_NAME]
        runtime_manifest = assets.dataset_files[POSTGRES_RUNTIME_MANIFEST_NAME]
    except KeyError as exc:
        raise MasterProviderUnavailable("verified master assets lack checkpoint verifier runtime files") from exc
    return KaggleCheckpointVerifierAssets(
        notebook_ref=assets.checkpoint_verifier_ref,
        notebook_source=verifier_source,
        timeout_seconds=timeout_seconds,
        runtime_dataset_exact_ref=f"{assets.dataset_ref}/{version}",
        runtime_image_identity=assets.runtime_image_identity,
        runtime_image_source_commit=assets.runtime_image_source_commit,
        runtime_python_series=assets.runtime_python_series,
        wheel_relative_path=wheel,
        wheel_sha256=hashlib.sha256(wheel_bytes).hexdigest(),
        postgres_runtime_archive_relative_path=POSTGRES_RUNTIME_ARCHIVE_NAME,
        postgres_runtime_archive_sha256=hashlib.sha256(archive).hexdigest(),
        postgres_runtime_manifest_relative_path=POSTGRES_RUNTIME_MANIFEST_NAME,
        postgres_runtime_manifest_sha256=hashlib.sha256(runtime_manifest).hexdigest(),
    )


def build_production_runtime(
    ledger: ControlLedger,
    settings: MasterRuntimeSettings | None,
    *,
    adapter_factory: Callable[[ControlLedgerKaggleJournal], KaggleProviderAdapter] | None = None,
    session_credentials_path: Path | None = None,
    master_tls_ca_path: Path | None = None,
    tunnel_authority: TunnelBrokerClient | None = None,
    tunnel_listen_port: int = 25432,
    provider_only: bool = False,
) -> ProductionRuntimeBuild:
    if provider_only and settings is not None:
        raise ValueError("provider-only runtime cannot receive master settings")
    registrar = None if provider_only else _build_session_registrar(session_credentials_path)
    if settings is None:
        if not provider_only or not kaggle_credentials_configured():
            return ProductionRuntimeBuild(None, "provider_unavailable", registrar, None, None)
        journal = ControlLedgerKaggleJournal(ledger)
        factory = adapter_factory or (
            lambda value: KaggleProviderAdapter.from_environment(journal=value)
        )
        try:
            adapter = factory(journal)
        except Exception:
            return ProductionRuntimeBuild(None, "provider_unavailable", registrar, None, None)
        return ProductionRuntimeBuild(None, "available", None, adapter, None)
    if not kaggle_credentials_configured():
        return ProductionRuntimeBuild(None, "provider_unavailable", registrar, None, None)
    journal = ControlLedgerKaggleJournal(ledger)
    factory = adapter_factory or (lambda value: KaggleProviderAdapter.from_environment(journal=value))
    try:
        adapter = factory(journal)
    except Exception:
        # Authentication/dependency failures do not make the stable control plane
        # unhealthy and must not leak provider exception/credential detail.
        return ProductionRuntimeBuild(None, "provider_unavailable", registrar, None, None)
    key_value = os.getenv("MY_DATA_HUB_CHECKPOINT_UPLOAD_BROKER_KEY_FILE", "").strip()
    if not key_value:
        return ProductionRuntimeBuild(
            None,
            "checkpoint_upload_broker_unavailable",
            registrar,
            adapter,
            None,
        )
    try:
        secret_box = CheckpointUploadSecretBox.from_file(Path(key_value))
        receipt_root = (session_credentials_path or Path(tempfile.gettempdir())) / "checkpoint-verifier-receipts"
        receipt_root.mkdir(parents=True, exist_ok=True, mode=0o700)
        receipt_root.chmod(0o700)

        def verifier_factory(operation_id: UUID, task_id: UUID) -> KaggleCheckpointRestoreVerifier:
            return KaggleCheckpointRestoreVerifier(
                adapter,
                _checkpoint_verifier_assets_from_verified_master_claim(
                    ledger, settings.assets, timeout_seconds=1800
                ),
                output_directory=receipt_root,
                operation_id=operation_id,
                authorization_task_id=task_id,
                metadata_only_output=True,
            )

        from my_data_hub.checkpoints.acceptance_broker import CentralBrokeredFM15Verifier

        fm15_root = receipt_root / "fm15"
        fm15_root.mkdir(parents=True, exist_ok=True, mode=0o700)
        fm15_root.chmod(0o700)

        def forced_failure_factory(_authority: object) -> CentralBrokeredFM15Verifier:
            return CentralBrokeredFM15Verifier(adapter, fm15_root)

        checkpoint_broker = BrokeredCheckpointUploadService(
            ledger,
            adapter,
            secret_box,
            restore_verifier_factory=verifier_factory,
            forced_failure_verifier_factory=forced_failure_factory,
        )
    except Exception:
        return ProductionRuntimeBuild(
            None,
            "checkpoint_upload_broker_unavailable",
            registrar,
            adapter,
            None,
        )
    ydb_access_token = os.getenv("MY_DATA_HUB_YDB_ACCESS_TOKEN_CREDENTIALS", "").strip() or None
    provider = KaggleMasterRuntimeProvider(
        adapter,
        settings.assets,
        status_authority=ledger,
        ydb_access_token=ydb_access_token,
    )
    coordinator = MasterCoordinator(
        ledger,
        provider,
        tunnel_authority=tunnel_authority,
        tunnel_listen_port=tunnel_listen_port,
    )
    acceptance_root = (session_credentials_path or Path(tempfile.gettempdir())) / "acceptance-receipts"
    runtime = ControlPlaneMasterRuntime(
        ledger,
        coordinator,
        settings,
        KaggleAcceptanceOperationExecutor(
            adapter,
            lambda timeout: _checkpoint_verifier_assets_from_verified_master_claim(
                ledger, settings.assets, timeout_seconds=timeout
            ),
            acceptance_root,
        ),
        master_tls_ca_path,
    )
    with suppress(Exception):
        runtime.reconcile_startup()
    return ProductionRuntimeBuild(runtime, "available", registrar, adapter, checkpoint_broker)


def _bounded_file(path: Path, *, max_bytes: int) -> bytes:
    if path.is_symlink() or not path.is_file():
        raise ValueError(f"launch asset must be a regular file: {path}")
    size = path.stat().st_size
    if not 1 <= size <= max_bytes:
        raise ValueError(f"launch asset size is outside its bound: {path}")
    return path.read_bytes()


def _bounded_files(root: Path) -> dict[str, bytes]:
    if root.is_symlink() or not root.is_dir():
        raise ValueError("master dataset asset directory is invalid")
    result: dict[str, bytes] = {}
    total = 0
    for path in sorted(root.rglob("*")):
        if path.is_symlink():
            raise ValueError("master dataset assets may not contain symlinks")
        if not path.is_file():
            continue
        relative = path.relative_to(root).as_posix()
        content = _bounded_file(path, max_bytes=128 * 1024 * 1024)
        total += len(content)
        if total > 256 * 1024 * 1024 or len(result) >= 1_000:
            raise ValueError("master dataset assets exceed their bounded contract")
        result[relative] = content
    if not result:
        raise ValueError("master dataset asset directory is empty")
    return result


def _build_session_registrar(path: Path | None) -> SessionCredentialRegistrar | None:
    if path is None:
        return None
    try:
        from my_data_hub.mcp.postgres_broker import DirectoryEpochCredentialSource
    except ImportError:
        # Integration may supply this optional bounded broker module.
        return None
    return DirectoryEpochCredentialSource(path)
