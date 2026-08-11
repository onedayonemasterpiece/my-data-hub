"""Production assembly for the lightweight master lifecycle control plane."""

from __future__ import annotations

import json
import os
import tempfile
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import urlsplit
from uuid import NAMESPACE_URL, UUID, uuid5

from my_data_hub.checkpoints import CheckpointManifest
from my_data_hub.checkpoints.kaggle_runtime import (
    KaggleCheckpointRestoreVerifier,
    KaggleCheckpointVerifierAssets,
)
from my_data_hub.control_plane.ledger import ControlLedger
from my_data_hub.hashing import sha256_value
from my_data_hub.orchestrator.master import MasterCoordinator, MasterHandle, MasterIntent
from my_data_hub.providers.kaggle import (
    ControlLedgerKaggleJournal,
    KaggleMasterLaunchAssets,
    KaggleMasterRuntimeProvider,
    KaggleProviderAdapter,
    derive_runtime_secret,
)
from my_data_hub.providers.kaggle.contracts import KaggleDatasetIdentity
from my_data_hub.providers.models import ProviderFingerprint
from my_data_hub.runtime_sdk import CANONICAL_RUNTIME_CALLBACK_URL


class MasterProviderUnavailable(RuntimeError):
    """The control plane is healthy but no authenticated provider is available."""


@dataclass(slots=True)
class KaggleAcceptanceOperationExecutor:
    """Run isolated restore verification while retaining receipt metadata only."""

    adapter: KaggleProviderAdapter
    assets: KaggleMasterLaunchAssets
    output_root: Path

    def restore(self, operation_id: str, candidate: dict[str, Any]) -> dict[str, Any]:
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
            ),
            output_directory=self.output_root,
            operation_id=uuid5(NAMESPACE_URL, f"acceptance:{operation_id}"),
            authorization_task_id=UUID(manifest.source_run_id),
            metadata_only_output=True,
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
class SessionCredential:
    master_instance_id: str
    epoch: int
    role: str
    database_url: str = field(repr=False)
    expires_at: Any


@dataclass(frozen=True, slots=True)
class MasterRuntimeSettings:
    assets: KaggleMasterLaunchAssets
    runtime_token_root: str = field(repr=False)

    def __post_init__(self) -> None:
        # Validate at assembly time, without ever storing the token in ledger state.
        derive_runtime_secret(self.runtime_token_root, "validation-run", "validation-attempt")

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
            "runtime_token_secret_name": "MY_DATA_HUB_KAGGLE_RUNTIME_TOKEN_SECRET_NAME",
            "checkpoint_verifier_ref": "MY_DATA_HUB_KAGGLE_CHECKPOINT_VERIFIER_REF",
            "checkpoint_verifier_source_file": "MY_DATA_HUB_KAGGLE_CHECKPOINT_VERIFIER_SOURCE_FILE",
            "runtime_token_root": "MY_DATA_HUB_MASTER_RUNTIME_TOKEN_ROOT",
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
        root = raw.pop("runtime_token_root")
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
        return cls(assets=assets, runtime_token_root=root)


@dataclass(slots=True)
class ControlPlaneMasterRuntime:
    ledger: ControlLedger
    coordinator: MasterCoordinator
    settings: MasterRuntimeSettings
    acceptance_executor: KaggleAcceptanceOperationExecutor | None = None

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
        existed = self.ledger.get_operation(identity["operation_id"]) is not None
        secret = derive_runtime_secret(self.settings.runtime_token_root, identity["run_id"], identity["attempt_id"])
        return self.coordinator.ensure_master(self.intent(idempotency_key), runtime_secret=secret), existed

    def reconcile_startup(self) -> list[MasterHandle]:
        handles: list[MasterHandle] = []
        for operation in self.ledger.incomplete_operations("ensure_master"):
            identity = operation.identity
            secret = derive_runtime_secret(
                self.settings.runtime_token_root,
                str(identity["run_id"]),
                str(identity["attempt_id"]),
            )
            handles.append(
                self.coordinator.ensure_master(self.intent(operation.idempotency_key), runtime_secret=secret)
            )
        return handles

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
        operations = [
            *self.ledger.incomplete_operations("checkpoint_restore_smoke"),
            *self.ledger.incomplete_operations("forced_master_rotation"),
        ]
        if not operations:
            return None
        operation = sorted(operations, key=lambda item: item.created_at)[0]
        timeout_seconds = int(operation.identity.get("timeout_seconds", 0))
        if timeout_seconds < 60 or (self.ledger.clock.now() - operation.created_at).total_seconds() > timeout_seconds:
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
            receipt = self.acceptance_executor.restore(operation.operation_id, candidate)
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


def build_production_runtime(
    ledger: ControlLedger,
    settings: MasterRuntimeSettings | None,
    *,
    adapter_factory: Callable[[ControlLedgerKaggleJournal], KaggleProviderAdapter] | None = None,
    session_credentials_path: Path | None = None,
) -> ProductionRuntimeBuild:
    registrar = _build_session_registrar(session_credentials_path)
    if settings is None:
        return ProductionRuntimeBuild(None, "provider_unavailable", registrar)
    if not _modern_kaggle_token_available():
        return ProductionRuntimeBuild(None, "provider_unavailable", registrar)
    journal = ControlLedgerKaggleJournal(ledger)
    factory = adapter_factory or (lambda value: KaggleProviderAdapter.from_environment(journal=value))
    try:
        adapter = factory(journal)
    except Exception:
        # Authentication/dependency failures do not make the stable control plane
        # unhealthy and must not leak provider exception/credential detail.
        return ProductionRuntimeBuild(None, "provider_unavailable", registrar)
    provider = KaggleMasterRuntimeProvider(adapter, settings.assets)
    coordinator = MasterCoordinator(ledger, provider)
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
    return ProductionRuntimeBuild(runtime, "available", registrar)


def _modern_kaggle_token_available() -> bool:
    if os.getenv("KAGGLE_API_TOKEN", "").strip():
        return True
    token_path = Path(os.getenv("KAGGLE_CONFIG_DIR", "~/.kaggle")).expanduser() / "access_token"
    try:
        return token_path.is_file() and bool(token_path.read_text(encoding="utf-8").strip())
    except OSError:
        return False


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
