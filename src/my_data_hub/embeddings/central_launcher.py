"""Single-adapter Kaggle launcher for direct ACTIVE-master embedding workers.

Only launch metadata and short-lived connection capability material are placed in
the disposable private status Dataset.  Job documents and vectors are never
accepted by this component.
"""

from __future__ import annotations

import hashlib
import secrets
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Protocol
from uuid import NAMESPACE_URL, UUID, uuid5

from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_validator

from my_data_hub.embeddings.direct_plane import EmbeddingLaunchMetadata
from my_data_hub.hashing import canonical_json_bytes
from my_data_hub.providers.kaggle.adapter import mapping_sha256
from my_data_hub.providers.kaggle.contracts import MutationAction, ProviderEffectIntent
from my_data_hub.providers.models import ControlClass


class EmbeddingWorkerDirectAccess(BaseModel):
    """Epoch-bound material minted by the ACTIVE master, never by control."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    database_url: SecretStr
    tls_ca_pem: SecretStr
    expires_at: datetime
    epoch: int = Field(ge=1)
    tunnel_endpoint: str = Field(min_length=3, max_length=300)
    credential_id: UUID

    @field_validator("expires_at")
    @classmethod
    def _utc_expiry(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("worker credential expiry must be timezone-aware")
        return value.astimezone(UTC)


class WorkerAccessFactory(Protocol):
    def __call__(self, metadata: EmbeddingLaunchMetadata, task_token: str) -> EmbeddingWorkerDirectAccess: ...


class CentralAdapter(Protocol):
    def create_private_dataset(self, **kwargs: Any) -> Any: ...
    def push_private_notebook_pending_runtime_attestation(self, **kwargs: Any) -> Any: ...


@dataclass(frozen=True, slots=True)
class EmbeddingWorkerLaunchConfig:
    owner: str
    runtime_dataset_exact_ref: str
    runtime_image_identity: str
    wheel_relative_path: str
    wheel_sha256: str
    callback_url: str


@dataclass(frozen=True, slots=True)
class EmbeddingWorkerLaunchReceipt:
    task_run_id: UUID
    status_dataset_exact_ref: str
    provider_run_ref: str
    source_sha256: str
    credential_id: UUID
    credential_expires_at: datetime


@dataclass(slots=True)
class CentralEmbeddingWorkerLauncher:
    """Concrete production launcher using the already-constructed central adapter."""

    adapter: CentralAdapter
    access_factory: WorkerAccessFactory
    config: EmbeddingWorkerLaunchConfig
    clock: Callable[[], datetime] = lambda: datetime.now(UTC)
    _receipts: dict[UUID, EmbeddingWorkerLaunchReceipt] = field(default_factory=dict, init=False)

    def launch(self, metadata: EmbeddingLaunchMetadata) -> EmbeddingWorkerLaunchReceipt:
        existing = self._receipts.get(metadata.task_run_id)
        if existing is not None:
            return existing
        now = self.clock().astimezone(UTC)
        task_token = secrets.token_urlsafe(32)
        access = self.access_factory(metadata, task_token)
        if access.epoch != metadata.epoch or access.expires_at <= now:
            raise ValueError("worker direct access is stale or belongs to another epoch")
        status_ref = f"{self.config.owner}/mdh-embedding-{metadata.task_run_id.hex[:20]}"
        notebook_ref = f"{self.config.owner}/mdh-embed-worker-{metadata.task_run_id.hex[:16]}"
        operation_id = uuid5(NAMESPACE_URL, f"embedding-worker:{metadata.request_id}:{metadata.epoch}")
        status = {
            "schema_version": "embedding-worker-status.v1",
            "launch": metadata.model_dump(mode="json"),
            "direct_access": {
                "database_url": access.database_url.get_secret_value(),
                "tls_ca_pem": access.tls_ca_pem.get_secret_value(),
                "expires_at": access.expires_at.isoformat(),
                "epoch": access.epoch,
                "tunnel_endpoint": access.tunnel_endpoint,
                "credential_id": str(access.credential_id),
            },
            "callback": {"url": self.config.callback_url, "task_token": task_token},
            "runtime": {
                "dataset_exact_ref": self.config.runtime_dataset_exact_ref,
                "image_identity": self.config.runtime_image_identity,
                "wheel_relative_path": self.config.wheel_relative_path,
                "wheel_sha256": self.config.wheel_sha256,
            },
        }
        files = {"embedding-worker.json": canonical_json_bytes(status)}
        create_intent = ProviderEffectIntent.create(
            operation_id=operation_id,
            effect_id=uuid5(NAMESPACE_URL, f"embedding-worker-status:{metadata.task_run_id}"),
            idempotency_key=f"embedding-worker-status:{metadata.task_run_id}",
            task_id=metadata.task_run_id,
            action=MutationAction.CREATE_DATASET,
            provider_ref=status_ref,
            arguments={
                "content_tree_sha256": mapping_sha256(files),
                "control_class": ControlClass.ORCHESTRATOR_PROTECTED.value,
                "disposable": True,
            }, requested_at=now,
        )
        dataset = self.adapter.create_private_dataset(
            intent=create_intent, files=files, title=status_ref.split("/", 1)[1],
            control_class=ControlClass.ORCHESTRATOR_PROTECTED, disposable=True,
        )
        exact_status = f"{status_ref}/{dataset.claim.provider_version}"
        source = self._render_source(status_ref)
        source_sha = hashlib.sha256(source).hexdigest()
        sources = (self.config.runtime_dataset_exact_ref, exact_status)
        push_intent = ProviderEffectIntent.create(
            operation_id=operation_id,
            effect_id=uuid5(NAMESPACE_URL, f"embedding-worker-run:{metadata.task_run_id}"),
            idempotency_key=f"embedding-worker-run:{metadata.task_run_id}",
            task_id=metadata.task_run_id, action=MutationAction.PUSH_NOTEBOOK,
            provider_ref=notebook_ref,
            arguments={"task_run_id": str(metadata.task_run_id), "source_sha256": source_sha,
                       "dataset_sources": sources,
                       "control_class": ControlClass.ORCHESTRATOR_PROTECTED.value,
                       "disposable": False}, requested_at=now,
        )
        run = self.adapter.push_private_notebook_pending_runtime_attestation(
            intent=push_intent, task_run_id=metadata.task_run_id, source=source,
            title=notebook_ref.split("/", 1)[1], code_file="worker.py", kernel_type="script",
            language="python", control_class=ControlClass.ORCHESTRATOR_PROTECTED,
            disposable=False, dataset_sources=sources, enable_internet=True,
            timeout_seconds=10_800,
        )
        receipt = EmbeddingWorkerLaunchReceipt(
            task_run_id=metadata.task_run_id, status_dataset_exact_ref=exact_status,
            provider_run_ref=run.run.provider_run_ref, source_sha256=source_sha,
            credential_id=access.credential_id, credential_expires_at=access.expires_at,
        )
        self._receipts[metadata.task_run_id] = receipt
        return receipt

    def _render_source(self, status_ref: str) -> bytes:
        status_mount = f"/kaggle/input/{status_ref.split('/', 1)[1]}"
        runtime_mount = f"/kaggle/input/{self.config.runtime_dataset_exact_ref.split('/', 1)[1]}"
        lines = [
            "import json, os, pathlib",
            f's=json.loads(pathlib.Path({status_mount!r}, "embedding-worker.json").read_text())',
            'assert s["schema_version"] == "embedding-worker-status.v1"',
            'a=s["direct_access"]; m=s["launch"]; r=s["runtime"]',
            'if int(a["epoch"]) != int(m["epoch"]): raise RuntimeError("epoch mismatch")',
            'ca=pathlib.Path("/kaggle/working/mdh-worker-ca.pem")',
            'ca.write_text(a["tls_ca_pem"]); ca.chmod(0o600)',
            'os.environ["MY_DATA_HUB_EMBEDDING_DIRECT_DATABASE_URL"]=a["database_url"]',
            'os.environ["PGSSLROOTCERT"]=str(ca)',
            'os.environ["MY_DATA_HUB_EMBEDDING_REQUEST_ID"]=m["request_id"]',
            'os.environ["MY_DATA_HUB_RUN_ID"]=m["task_run_id"]',
            'os.environ["MY_DATA_HUB_EMBEDDING_INPUT_JOBS_SHA256"]=m["input_jobs_sha256"]',
            f'os.environ["MY_DATA_HUB_WHEEL_PATH"]=str(pathlib.Path({runtime_mount!r},r["wheel_relative_path"]))',
            'os.environ["MY_DATA_HUB_WHEEL_SHA256"]=r["wheel_sha256"]',
            'os.environ["MY_DATA_HUB_KAGGLE_RUNTIME_IMAGE_IDENTITY"]=r["image_identity"]',
            'os.environ["MY_DATA_HUB_NOTEBOOK_IS_PRIVATE"]="true"',
            'asset="e5-worker.json" if "multilingual-e5" in m["model_exact_id"] else "bge-worker.json"',
            f'n=json.loads(pathlib.Path({runtime_mount!r},"embeddings",asset).read_text())',
            'for c in n["cells"]:',
            '    if c["cell_type"]=="code": exec(compile(c["source"],asset,"exec"),globals())',
        ]
        code = "\n".join(lines) + "\n"
        return code.encode()

    @property
    def ready(self) -> bool:
        return callable(self.access_factory) and self.adapter is not None
