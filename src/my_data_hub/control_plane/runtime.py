"""Production assembly for the lightweight master lifecycle control plane."""

from __future__ import annotations

import json
import os
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from my_data_hub.control_plane.ledger import ControlLedger
from my_data_hub.orchestrator.master import MasterCoordinator, MasterHandle, MasterIntent
from my_data_hub.providers.kaggle import (
    ControlLedgerKaggleJournal,
    KaggleMasterLaunchAssets,
    KaggleMasterRuntimeProvider,
    KaggleProviderAdapter,
    derive_runtime_secret,
)


class MasterProviderUnavailable(RuntimeError):
    """The control plane is healthy but no authenticated provider is available."""


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
            "runtime_token_root": "MY_DATA_HUB_MASTER_RUNTIME_TOKEN_ROOT",
        }
        raw = {key: os.getenv(name, "").strip() for key, name in names.items()}
        if not any(raw.values()):
            return None
        if not all(raw.values()):
            # Partial launch configuration never causes a best-effort provider call.
            return None
        dataset_dir = Path(raw.pop("dataset_dir")).expanduser().resolve()
        notebook_source = Path(raw.pop("notebook_source")).expanduser().resolve()
        files = _bounded_files(dataset_dir)
        source = _bounded_file(notebook_source, max_bytes=8 * 1024 * 1024)
        root = raw.pop("runtime_token_root")
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
        )
        return cls(assets=assets, runtime_token_root=root)


@dataclass(slots=True)
class ControlPlaneMasterRuntime:
    ledger: ControlLedger
    coordinator: MasterCoordinator
    settings: MasterRuntimeSettings

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
        secret = derive_runtime_secret(
            self.settings.runtime_token_root, identity["run_id"], identity["attempt_id"]
        )
        return self.coordinator.ensure_master(
            self.intent(idempotency_key), runtime_secret=secret
        ), existed

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
                self.coordinator.ensure_master(
                    self.intent(operation.idempotency_key), runtime_secret=secret
                )
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
    runtime = ControlPlaneMasterRuntime(ledger, coordinator, settings)
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
