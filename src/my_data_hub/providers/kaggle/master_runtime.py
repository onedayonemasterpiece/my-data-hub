"""Concrete master-lifecycle bridge over the repository's single Kaggle adapter.

The bridge translates the provider-neutral, persist-before-effect coordinator port
to :class:`KaggleProviderAdapter`.  It does not create an API client or transport.
"""

from __future__ import annotations

import hashlib
import hmac
import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from types import MappingProxyType
from typing import Any, Protocol
from uuid import UUID

from my_data_hub.orchestrator.master.provider import (
    EffectReconciliation,
    MasterRuntimeProvider,
    PlannedProviderEffect,
    ProviderEffectReceipt,
    ReconciliationStatus,
)
from my_data_hub.providers.models import ControlClass

from .adapter import KaggleProviderAdapter
from .contracts import (
    KaggleKernelRunIdentity,
    MutationAction,
    NotebookMutationResult,
    ProviderEffectIntent,
)


class MasterLaunchContractError(ValueError):
    """Raised before a provider call when launch identity/assets are incomplete."""


class NotebookRunReconciler(Protocol):
    def reconcile_private_notebook_run(
        self, *, task_run_id: UUID, provider_ref: str, expected_source_sha256: str
    ) -> KaggleKernelRunIdentity | None: ...


@dataclass(frozen=True, slots=True)
class KaggleMasterLaunchAssets:
    """Exact, secret-free assets and provider identities for one master deployment."""

    source_identity: str
    source_version: str
    checkpoint_ref: str
    dataset_ref: str
    notebook_ref: str
    dataset_files: Mapping[str, bytes]
    notebook_source: bytes
    callback_url: str
    runtime_token_secret_name: str
    checkpoint_verifier_ref: str
    checkpoint_verifier_source_file: str
    checkpoint_probe_relations: tuple[str, ...]
    runtime_secret_bindings: Mapping[str, str] = field(default_factory=dict)
    notebook_code_file: str = "worker.ipynb"
    notebook_kernel_type: str = "notebook"
    notebook_language: str = "python"
    notebook_timeout_seconds: int = 43_200
    enable_internet: bool = True

    def __post_init__(self) -> None:
        object.__setattr__(self, "dataset_files", MappingProxyType(dict(self.dataset_files)))
        object.__setattr__(self, "runtime_secret_bindings", MappingProxyType(dict(self.runtime_secret_bindings)))
        for field_name in ("dataset_ref", "notebook_ref", "checkpoint_ref", "checkpoint_verifier_ref"):
            value = getattr(self, field_name)
            parts = value.split("/")
            if len(parts) != 2 or not all(parts):
                raise MasterLaunchContractError(f"{field_name} must be exact owner/slug")
        if not self.source_identity or not self.source_version or not self.checkpoint_ref:
            raise MasterLaunchContractError("source/checkpoint identity must be exact")
        if not self.dataset_files or not self.notebook_source:
            raise MasterLaunchContractError("master launch assets must be non-empty")
        if not self.callback_url.startswith("https://") or not self.callback_url.endswith("/internal/runtime/events"):
            raise MasterLaunchContractError("callback URL must be the exact HTTPS runtime endpoint")
        if not self.runtime_token_secret_name or len(self.runtime_token_secret_name) > 200:
            raise MasterLaunchContractError("runtime token secret name is invalid")
        if (
            self.checkpoint_verifier_source_file not in self.dataset_files
            or not self.checkpoint_verifier_source_file.endswith(".ipynb")
            or not self.checkpoint_probe_relations
            or len(set(self.checkpoint_probe_relations)) != len(self.checkpoint_probe_relations)
            or any(not value or len(value) > 200 for value in self.checkpoint_probe_relations)
        ):
            raise MasterLaunchContractError("checkpoint verifier/probe assets are incomplete")
        if self.notebook_kernel_type not in {"notebook", "script"}:
            raise MasterLaunchContractError("master notebook kernel type is invalid")
        for environment_name, secret_name in self.runtime_secret_bindings.items():
            if (
                (
                    environment_name not in {"KAGGLE_API_TOKEN"}
                    and not environment_name.startswith("MY_DATA_HUB_")
                )
                or environment_name == "MY_DATA_HUB_RUN_SECRET"
                or not secret_name
            ):
                raise MasterLaunchContractError("runtime secret binding is invalid")

    def render_values(self, identity: Mapping[str, Any]) -> dict[str, str]:
        values = {
            "MY_DATA_HUB_CALLBACK_URL": self.callback_url,
            "MY_DATA_HUB_OPERATION_ID": str(identity["operation_id"]),
            "MY_DATA_HUB_RUN_ID": str(identity["run_id"]),
            "MY_DATA_HUB_ATTEMPT_ID": str(identity["attempt_id"]),
            "MY_DATA_HUB_SERVICE_INSTANCE_ID": str(identity["service_instance_id"]),
            "MY_DATA_HUB_MASTER_INSTANCE_ID": str(identity["master_instance_id"]),
            "MY_DATA_HUB_EPOCH": str(identity["epoch"]),
            "MY_DATA_HUB_SOURCE_IDENTITY": self.source_identity,
            "MY_DATA_HUB_SOURCE_VERSION": self.source_version,
            "MY_DATA_HUB_CHECKPOINT_REF": self.checkpoint_ref,
        }
        input_root = f"/kaggle/input/{self.dataset_ref.split('/', 1)[1]}"
        if "master-config.json" in self.dataset_files:
            values["MY_DATA_HUB_MASTER_CONFIG"] = f"{input_root}/master-config.json"
        verifier_source = self.dataset_files[self.checkpoint_verifier_source_file]
        values.update(
            {
                "MY_DATA_HUB_CONTROL_PLANE_URL": self.callback_url.removesuffix("/internal/runtime/events"),
                "MY_DATA_HUB_CHECKPOINT_DATASET_REF": self.checkpoint_ref,
                "MY_DATA_HUB_CHECKPOINT_VERIFIER_REF": self.checkpoint_verifier_ref,
                "MY_DATA_HUB_CHECKPOINT_VERIFIER_SOURCE_PATH": (f"{input_root}/{self.checkpoint_verifier_source_file}"),
                "MY_DATA_HUB_CHECKPOINT_VERIFIER_SOURCE_SHA256": hashlib.sha256(verifier_source).hexdigest(),
                "MY_DATA_HUB_CHECKPOINT_PROBE_RELATIONS_JSON": json.dumps(
                    self.checkpoint_probe_relations, separators=(",", ":")
                ),
            }
        )
        wheels = sorted(path for path in self.dataset_files if path.endswith(".whl"))
        if len(wheels) == 1:
            values["MY_DATA_HUB_WHEEL_PATH"] = f"{input_root}/{wheels[0]}"
            values["MY_DATA_HUB_WHEEL_SHA256"] = hashlib.sha256(self.dataset_files[wheels[0]]).hexdigest()
        return values


def derive_runtime_secret(root_secret: str, run_id: str, attempt_id: str) -> str:
    """Derive a role-bound per-attempt callback token without persisting plaintext."""

    if len(root_secret) < 24:
        raise MasterLaunchContractError("runtime token root must be at least 24 characters")
    message = f"my-data-hub-runtime-v1:{run_id}:{attempt_id}".encode()
    return hmac.new(root_secret.encode(), message, hashlib.sha256).hexdigest()


def _replace_nonsecret_markers(content: bytes, values: Mapping[str, str]) -> bytes:
    rendered = content
    for key, value in values.items():
        rendered = rendered.replace(f"{{{{{key}}}}}".encode(), value.encode())
    if b"{{MY_DATA_HUB_RUN_SECRET}}" in rendered:
        raise MasterLaunchContractError("launch assets may not contain a runtime secret placeholder")
    return rendered


def _runtime_bootstrap(values: Mapping[str, str], secret_name: str, secret_bindings: Mapping[str, str]) -> str:
    # Only identities and a Kaggle User Secrets label are embedded.  The secret
    # value is fetched inside Kaggle and the per-attempt token is derived there.
    encoded = json.dumps(dict(values), sort_keys=True)
    bindings = json.dumps(dict(secret_bindings), sort_keys=True)
    return (
        "import hashlib as _mdh_hashlib, hmac as _mdh_hmac, os as _mdh_os\n"
        "from kaggle_secrets import UserSecretsClient as _MdhSecrets\n"
        f"_mdh_values = {encoded}\n"
        "_mdh_os.environ.update(_mdh_values)\n"
        "_mdh_secrets = _MdhSecrets()\n"
        f"_mdh_root = _mdh_secrets.get_secret({secret_name!r})\n"
        f"for _mdh_env, _mdh_name in {bindings}.items():\n"
        "    _mdh_os.environ[_mdh_env] = _mdh_secrets.get_secret(_mdh_name)\n"
        "_mdh_message = ('my-data-hub-runtime-v1:' + _mdh_values['MY_DATA_HUB_RUN_ID'] + ':' + "
        "_mdh_values['MY_DATA_HUB_ATTEMPT_ID']).encode()\n"
        "_mdh_os.environ['MY_DATA_HUB_RUN_SECRET'] = _mdh_hmac.new("
        "_mdh_root.encode(), _mdh_message, _mdh_hashlib.sha256).hexdigest()\n"
        "del _mdh_root\n"
    )


def render_notebook_source(
    source: bytes,
    *,
    kernel_type: str,
    values: Mapping[str, str],
    secret_name: str,
    secret_bindings: Mapping[str, str] | None = None,
) -> bytes:
    source = _replace_nonsecret_markers(source, values)
    bootstrap = _runtime_bootstrap(values, secret_name, secret_bindings or {})
    if kernel_type == "script":
        return (bootstrap + "\n").encode() + source
    try:
        body = json.loads(source)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MasterLaunchContractError("master notebook asset is not valid UTF-8 JSON") from exc
    if not isinstance(body, dict) or not isinstance(body.get("cells"), list):
        raise MasterLaunchContractError("master notebook asset lacks a cells array")
    body["cells"].insert(
        0,
        {
            "cell_type": "code",
            "execution_count": None,
            "id": "my-data-hub-runtime-identity",
            "metadata": {},
            "outputs": [],
            "source": bootstrap,
        },
    )
    return json.dumps(body).encode()


class KaggleMasterRuntimeProvider(MasterRuntimeProvider):
    """MasterRuntimeProvider implemented by the one official Kaggle adapter."""

    def __init__(self, adapter: KaggleProviderAdapter, assets: KaggleMasterLaunchAssets) -> None:
        self.adapter = adapter
        self.assets = assets

    def execute(self, effect: PlannedProviderEffect) -> ProviderEffectReceipt:
        self._validate_effect(effect)
        if effect.effect_kind == "ensure_dataset":
            files = {
                path: _replace_nonsecret_markers(content, self.assets.render_values(effect.exact_identity))
                for path, content in self.assets.dataset_files.items()
            }
            intent = self._intent(
                effect,
                MutationAction.CREATE_DATASET,
                self.assets.dataset_ref,
                {
                    "content_tree_sha256": self._mapping_sha(files),
                    "control_class": ControlClass.ORCHESTRATOR_PROTECTED.value,
                    "disposable": False,
                },
            )
            result = self.adapter.create_private_dataset(
                intent=intent,
                files=files,
                title=self.assets.dataset_ref.split("/", 1)[1],
                control_class=ControlClass.ORCHESTRATOR_PROTECTED,
                disposable=False,
            )
            return self._receipt(
                effect,
                self.assets.dataset_ref,
                {
                    "provider_ref": result.identity.provider_ref,
                    "provider_version": result.identity.version,
                    "package_sha256": result.identity.package_sha256,
                },
            )
        if effect.effect_kind == "push_notebook":
            source = self._source(effect.exact_identity)
            intent = self._intent(
                effect,
                MutationAction.PUSH_NOTEBOOK,
                self.assets.notebook_ref,
                {
                    "task_run_id": str(effect.exact_identity["run_id"]),
                    "source_sha256": hashlib.sha256(self._canonical_source(source)).hexdigest(),
                    "dataset_sources": (self.assets.dataset_ref,),
                    "control_class": ControlClass.ORCHESTRATOR_PROTECTED.value,
                    "disposable": False,
                },
            )
            result = self.adapter.push_private_notebook(
                intent=intent,
                task_run_id=UUID(str(effect.exact_identity["run_id"])),
                source=source,
                title=self.assets.notebook_ref.split("/", 1)[1],
                code_file=self.assets.notebook_code_file,
                kernel_type=self.assets.notebook_kernel_type,
                language=self.assets.notebook_language,
                control_class=ControlClass.ORCHESTRATOR_PROTECTED,
                disposable=False,
                dataset_sources=(self.assets.dataset_ref,),
                enable_internet=self.assets.enable_internet,
                timeout_seconds=self.assets.notebook_timeout_seconds,
            )
            return self._notebook_receipt(effect, result)
        if effect.effect_kind == "trigger_run":
            launch = effect.exact_identity.get("notebook_launch")
            run = self._run_from_identity(launch)
            self.adapter.read_run_status(run)
            return self._receipt(effect, run.provider_run_ref, run.model_dump(mode="json"))
        raise MasterLaunchContractError(f"unsupported master provider effect: {effect.effect_kind}")

    def reconcile(self, effect: PlannedProviderEffect) -> EffectReconciliation:
        self._validate_effect(effect)
        if effect.effect_kind == "ensure_dataset":
            # Replaying the exact deterministic create intent is safe: the adapter
            # performs exact package readback on provider conflict and returns
            # ALREADY_APPLIED rather than creating a second dataset.
            try:
                return EffectReconciliation(ReconciliationStatus.FOUND, self.execute(effect))
            except Exception:
                return EffectReconciliation(ReconciliationStatus.AMBIGUOUS)
        if effect.effect_kind == "push_notebook":
            source = self._source(effect.exact_identity)
            reconciler = getattr(self.adapter, "reconcile_private_notebook_run", None)
            if not callable(reconciler):
                return EffectReconciliation(ReconciliationStatus.AMBIGUOUS)
            run = reconciler(
                task_run_id=UUID(str(effect.exact_identity["run_id"])),
                provider_ref=self.assets.notebook_ref,
                expected_source_sha256=hashlib.sha256(self._canonical_source(source)).hexdigest(),
            )
            if run is None:
                return EffectReconciliation(ReconciliationStatus.ABSENT)
            result = self._receipt(effect, run.provider_run_ref, run.model_dump(mode="json"))
            return EffectReconciliation(ReconciliationStatus.FOUND, result)
        if effect.effect_kind == "trigger_run":
            try:
                return EffectReconciliation(ReconciliationStatus.FOUND, self.execute(effect))
            except Exception:
                return EffectReconciliation(ReconciliationStatus.AMBIGUOUS)
        return EffectReconciliation(ReconciliationStatus.AMBIGUOUS)

    def _validate_effect(self, effect: PlannedProviderEffect) -> None:
        expected = {
            "ensure_dataset": self.assets.dataset_ref,
            "push_notebook": self.assets.notebook_ref,
        }.get(effect.effect_kind)
        if expected is not None and effect.exact_identity.get("exact_ref") != expected:
            raise MasterLaunchContractError("provider effect ref differs from configured exact launch identity")
        if effect.exact_identity.get("source_identity") != self.assets.source_identity:
            raise MasterLaunchContractError("provider effect source identity differs from launch assets")
        if effect.exact_identity.get("source_version") != self.assets.source_version:
            raise MasterLaunchContractError("provider effect source version differs from launch assets")

    def _source(self, identity: Mapping[str, Any]) -> bytes:
        return render_notebook_source(
            self.assets.notebook_source,
            kernel_type=self.assets.notebook_kernel_type,
            values=self.assets.render_values(identity),
            secret_name=self.assets.runtime_token_secret_name,
            secret_bindings=self.assets.runtime_secret_bindings,
        )

    def _canonical_source(self, source: bytes) -> bytes:
        if self.assets.notebook_kernel_type != "notebook":
            return source
        body = json.loads(source)
        for cell in body.get("cells", []):
            if isinstance(cell, dict):
                if cell.get("cell_type") == "code" and "outputs" in cell:
                    cell["outputs"] = []
                if isinstance(cell.get("source"), list):
                    cell["source"] = "".join(str(item) for item in cell["source"])
        return json.dumps(body).encode()

    @staticmethod
    def _mapping_sha(files: Mapping[str, bytes]) -> str:
        from .adapter import mapping_sha256

        return mapping_sha256(files)

    @staticmethod
    def _intent(
        effect: PlannedProviderEffect,
        action: MutationAction,
        provider_ref: str,
        arguments: Mapping[str, Any],
    ) -> ProviderEffectIntent:
        return ProviderEffectIntent.create(
            operation_id=UUID(str(effect.exact_identity.get("operation_id", effect.idempotency_key.split(":", 1)[0]))),
            effect_id=UUID(effect.effect_id),
            idempotency_key=effect.idempotency_key,
            task_id=UUID(str(effect.exact_identity["run_id"])),
            action=action,
            provider_ref=provider_ref,
            arguments=arguments,
            requested_at=datetime(2000, 1, 1, tzinfo=UTC),
        )

    def _receipt(
        self, effect: PlannedProviderEffect, exact_ref: str, exact_identity: dict[str, Any]
    ) -> ProviderEffectReceipt:
        return ProviderEffectReceipt(
            provider="kaggle",
            effect_kind=effect.effect_kind,
            exact_ref=exact_ref,
            source_identity=self.assets.source_identity,
            source_version=self.assets.source_version,
            exact_identity=exact_identity,
        )

    def _notebook_receipt(self, effect: PlannedProviderEffect, result: NotebookMutationResult) -> ProviderEffectReceipt:
        return self._receipt(effect, result.run.provider_run_ref, result.run.model_dump(mode="json"))

    @staticmethod
    def _run_from_identity(value: object) -> KaggleKernelRunIdentity:
        if not isinstance(value, dict):
            raise MasterLaunchContractError("trigger_run lacks exact notebook launch identity")
        return KaggleKernelRunIdentity.model_validate(value)
