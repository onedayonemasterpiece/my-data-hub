"""Production assembly for the lightweight master lifecycle control plane."""

from __future__ import annotations

import hashlib
import json
import os
import secrets
import tempfile
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import urlsplit
from uuid import NAMESPACE_URL, UUID, uuid5

from my_data_hub.acceptance.master_lifecycle import (
    AcceptancePrincipal,
    MasterAcceptanceReceipt,
    MasterAcceptanceRequest,
    require_acceptance_operator,
)
from my_data_hub.checkpoints import CheckpointManifest
from my_data_hub.checkpoints.kaggle_runtime import (
    KaggleCheckpointRestoreVerifier,
    KaggleCheckpointVerifierAssets,
)
from my_data_hub.control_plane.ledger import ControlLedger
from my_data_hub.hashing import sha256_value
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
from my_data_hub.providers.models import ControlClass, ProviderFingerprint, ProviderKind
from my_data_hub.runtime_sdk import CANONICAL_RUNTIME_CALLBACK_URL
from my_data_hub.tunnel_broker_ipc import TunnelBrokerClient


class MasterProviderUnavailable(RuntimeError):
    """The control plane is healthy but no authenticated provider is available."""


@dataclass(slots=True)
class KaggleAcceptanceOperationExecutor:
    """Run isolated restore verification while retaining receipt metadata only."""

    adapter: KaggleProviderAdapter
    assets: KaggleMasterLaunchAssets
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
        source = self.assets.dataset_files[self.assets.checkpoint_verifier_source_file]
        self.output_root.mkdir(parents=True, exist_ok=True, mode=0o700)
        verifier = KaggleCheckpointRestoreVerifier(
            self.adapter,
            KaggleCheckpointVerifierAssets(
                notebook_ref=self.assets.checkpoint_verifier_ref,
                notebook_source=source,
                timeout_seconds=timeout_seconds,
            ),
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
        }
        raw = {key: os.getenv(name, "").strip() for key, name in names.items()}
        if not any(raw.values()):
            return None
        if not all(raw.values()):
            # Partial launch configuration never causes a best-effort provider call.
            return None
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
            if not self._status_dataset_ready(operation.operation_id, operation.identity):
                current = self.ledger.get_operation(operation.operation_id)
                assert current is not None
                handles.append(self._handle(current))
                continue
            handle = self.coordinator.ensure_master(self.intent(operation.idempotency_key))
            self._cleanup_terminal_status_dataset(handle)
            handles.append(handle)
        return handles

    def _status_dataset_ready(self, operation_id: str, identity: dict[str, Any]) -> bool:
        provider = self.coordinator.provider
        if not isinstance(provider, KaggleMasterRuntimeProvider):
            return True
        existing = self.ledger.master_status_dataset_authority(operation_id)
        if existing is not None:
            if existing["status_dataset"] is not None:
                return existing["state"] in {"READY", "CLEANED"}
            claim_until = datetime.fromisoformat(
                str(existing["creator_claim_until"]).replace("Z", "+00:00")
            )
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
            lease_until=self.ledger.clock.now()
            + timedelta(seconds=self.settings.assets.notebook_timeout_seconds),
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
        }
        files = provider.status_files(candidate_identity, token)
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
        result = provider.create_status_dataset(exact_identity, token)
        claim = result.claim
        self.ledger.record_master_status_dataset(
            operation_id=operation_id,
            status_dataset={
                "provider_ref": claim.provider_ref,
                "exact_version_ref": f"{claim.provider_ref}/{claim.provider_version}",
                "claim": claim.model_dump(mode="json"),
                "content_tree_sha256": mapping_sha256(files),
                "status_config_sha256": hashlib.sha256(files["kaggle_run.json"]).hexdigest(),
                "status_helper_sha256": hashlib.sha256(files["kaggle_status_client.py"]).hexdigest(),
                "resource_lease": resource_lease,
            },
        )
        return True

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
        self.ledger.release_resource_lease_exact(
            str(lease["lease_id"]), str(lease["holder_id"]), int(lease["epoch"])
        )
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
        self.ledger.release_resource_lease_exact(
            str(lease["lease_id"]), str(lease["holder_id"]), int(lease["epoch"])
        )

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
                        registered_at=datetime.fromisoformat(
                            str(stored["created_at"]).replace("Z", "+00:00")
                        ),
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
        try:
            handle, _duplicate = self.ensure(str(request["idempotency_key"]))
            if handle.operation_id != str(request["operation_id"]):
                raise RuntimeError("master request operation identity differs from coordinator")
            self.ledger.complete_master_request(str(request["request_id"]), handle.operation_id)
            return handle
        except Exception:
            self.ledger.release_master_request(str(request["request_id"]))
            raise

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


def build_production_runtime(
    ledger: ControlLedger,
    settings: MasterRuntimeSettings | None,
    *,
    adapter_factory: Callable[[ControlLedgerKaggleJournal], KaggleProviderAdapter] | None = None,
    session_credentials_path: Path | None = None,
    tunnel_authority: TunnelBrokerClient | None = None,
    tunnel_listen_port: int = 25432,
) -> ProductionRuntimeBuild:
    registrar = _build_session_registrar(session_credentials_path)
    if settings is None:
        return ProductionRuntimeBuild(None, "provider_unavailable", registrar, None)
    if not kaggle_credentials_configured():
        return ProductionRuntimeBuild(None, "provider_unavailable", registrar, None)
    journal = ControlLedgerKaggleJournal(ledger)
    factory = adapter_factory or (lambda value: KaggleProviderAdapter.from_environment(journal=value))
    try:
        adapter = factory(journal)
    except Exception:
        # Authentication/dependency failures do not make the stable control plane
        # unhealthy and must not leak provider exception/credential detail.
        return ProductionRuntimeBuild(None, "provider_unavailable", registrar, None)
    provider = KaggleMasterRuntimeProvider(adapter, settings.assets, status_authority=ledger)
    coordinator = MasterCoordinator(
        ledger,
        provider,
        tunnel_authority=tunnel_authority,
        tunnel_listen_port=tunnel_listen_port,
    )
    receipt_root = (session_credentials_path or Path(tempfile.gettempdir())) / "acceptance-receipts"
    runtime = ControlPlaneMasterRuntime(
        ledger,
        coordinator,
        settings,
        KaggleAcceptanceOperationExecutor(adapter, settings.assets, receipt_root),
    )
    with suppress(Exception):
        runtime.reconcile_startup()
    # Durable operations remain resumable; readiness is still control-plane
    # readiness, not an assertion that Kaggle accepted an effect.
    return ProductionRuntimeBuild(runtime, "available", registrar, adapter)



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
