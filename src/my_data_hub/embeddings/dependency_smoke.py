"""Durable central verification of the exact embedding dependency runtime."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Literal
from uuid import NAMESPACE_URL, UUID, uuid5

from pydantic import BaseModel, ConfigDict, Field

from my_data_hub.hashing import canonical_json_bytes
from my_data_hub.providers.kaggle.contracts import MutationAction, ProviderEffectIntent
from my_data_hub.providers.models import ControlClass

OBSERVATION = "embedding-dependency-smoke-observation.json"


class EmbeddingDependencySmokeReceipt(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    schema_version: Literal["my-data-hub-embedding-dependency-smoke-receipt.v1"] = (
        "my-data-hub-embedding-dependency-smoke-receipt.v1"
    )
    status: Literal["pass"] = "pass"
    observed_at: datetime
    provider_run_ref: str = Field(pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/[1-9][0-9]*$")
    observation_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    image_identity: str
    image_source_commit: str = Field(pattern=r"^[a-f0-9]{40}$")
    python_version: str
    dependency_manifest_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    project_wheel_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    wheel_sha256s: dict[str, str]
    imports: list[str]
    psycopg_implementation: Literal["binary"]
    distributions: dict[str, str]
    notebook_private: Literal[True] = True
    internet_enabled: Literal[False] = False
    verified_by_central_adapter: Literal[True] = True

    @property
    def receipt_sha256(self) -> str:
        return hashlib.sha256(canonical_json_bytes(self.model_dump(mode="json"))).hexdigest()


@dataclass(slots=True)
class CentralDependencySmoke:
    adapter: Any
    owner: str
    runtime_dataset_exact_ref: str
    image_identity: str
    image_source_commit: str
    project_wheel_sha256: str
    project_wheel_relative_path: str
    dependency_manifest_sha256: str
    state_path: Path
    receipt_path: Path
    clock: Any = lambda: datetime.now(UTC)

    def _source(self, task_id: UUID) -> bytes:
        values = {
            "MY_DATA_HUB_WHEEL_SHA256": self.project_wheel_sha256,
            "MY_DATA_HUB_KAGGLE_RUNTIME_IMAGE_IDENTITY": self.image_identity,
            "MY_DATA_HUB_KAGGLE_RUNTIME_SOURCE_COMMIT": self.image_source_commit,
            "MY_DATA_HUB_DEPENDENCY_SMOKE_OBSERVATION_PATH": f"/kaggle/working/{OBSERVATION}",
        }
        code = (
            f"TASK_RUN_ID={str(task_id)!r}\n"
            "import os,runpy\nfrom pathlib import Path\n"
            "if any(os.environ.get(k) for k in ('KAGGLE_USERNAME','KAGGLE_KEY','KAGGLE_API_TOKEN')): "
            "raise RuntimeError('Kaggle credential env is forbidden')\n"
            "if any(p.exists() for p in (Path.home()/'.kaggle'/'kaggle.json',"
            "Path.home()/'.kaggle'/'access_token')): raise RuntimeError('Kaggle credential file is forbidden')\n"
            "def one(name):\n"
            "    matches=[p for p in Path('/kaggle/input').rglob(name) if p.is_file() and not p.is_symlink()]\n"
            "    if len(matches)!=1: raise RuntimeError('exact dependency smoke input is absent or ambiguous')\n"
            "    return matches[0]\n"
            "manifest=one('embedding-worker-dependencies.json')\n"
            "runner=one('embedding-dependency-smoke.py')\n"
            f"project=one({Path(self.project_wheel_relative_path).name!r})\n"
            f"os.environ.update({values!r})\n"
            "os.environ['MY_DATA_HUB_EMBEDDING_DEPENDENCY_MANIFEST_PATH']=str(manifest)\n"
            "os.environ['MY_DATA_HUB_EMBEDDING_WHEELHOUSE_PATH']=str(manifest.parent/'embedding-worker-wheelhouse')\n"
            "os.environ['MY_DATA_HUB_WHEEL_PATH']=str(project)\n"
            "runpy.run_path(str(runner),run_name='__main__')\n"
        )
        return code.encode()

    def _persist(self, value: dict[str, Any]) -> None:
        if not self.state_path.is_absolute() or self.state_path.is_symlink():
            raise ValueError("dependency smoke state path must be absolute and non-symlink")
        self.state_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.state_path.parent.chmod(0o700)
        body = canonical_json_bytes(value) + b"\n"
        fd, raw = tempfile.mkstemp(prefix=".dependency-smoke.", dir=self.state_path.parent)
        try:
            os.fchmod(fd, 0o600)
            with os.fdopen(fd, "wb") as stream:
                stream.write(body)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(raw, self.state_path)
            directory = os.open(self.state_path.parent, os.O_RDONLY)
            os.fsync(directory)
            os.close(directory)
        finally:
            Path(raw).unlink(missing_ok=True)

    def _load(self) -> dict[str, Any] | None:
        if not self.state_path.exists():
            return None
        if self.state_path.is_symlink() or self.state_path.stat().st_mode & 0o077:
            raise ValueError("dependency smoke state is unsafe")
        return json.loads(self.state_path.read_text())

    def _load_receipt(self) -> EmbeddingDependencySmokeReceipt | None:
        if not self.receipt_path.exists():
            return None
        if self.receipt_path.is_symlink() or self.receipt_path.stat().st_mode & 0o077:
            raise ValueError("dependency smoke receipt is unsafe")
        body = self.receipt_path.read_bytes()
        receipt = EmbeddingDependencySmokeReceipt.model_validate_json(body)
        if body != canonical_json_bytes(receipt.model_dump(mode="json")):
            raise ValueError("dependency smoke receipt is not canonical")
        if (
            receipt.image_identity != self.image_identity
            or receipt.image_source_commit != self.image_source_commit
            or receipt.project_wheel_sha256 != self.project_wheel_sha256
            or receipt.dependency_manifest_sha256 != self.dependency_manifest_sha256
        ):
            raise ValueError("dependency smoke receipt differs from configured runtime")
        return receipt

    def _write_receipt(self, body: bytes) -> None:
        if not self.receipt_path.is_absolute() or self.receipt_path.is_symlink():
            raise ValueError("dependency smoke receipt path must be absolute and non-symlink")
        self.receipt_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.receipt_path.parent.chmod(0o700)
        fd, raw = tempfile.mkstemp(prefix=".dependency-smoke-receipt.", dir=self.receipt_path.parent)
        try:
            os.fchmod(fd, 0o600)
            with os.fdopen(fd, "wb") as stream:
                stream.write(body)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(raw, self.receipt_path)
            directory = os.open(self.receipt_path.parent, os.O_RDONLY)
            os.fsync(directory)
            os.close(directory)
        finally:
            Path(raw).unlink(missing_ok=True)

    def _cleanup(self, state: dict[str, Any], task_id: UUID, operation_id: UUID) -> None:
        from my_data_hub.providers.kaggle.contracts import TaskResourceClaim

        claim = TaskResourceClaim.model_validate(state["claim"])
        state["state"] = "CLEANUP_REQUESTED"
        self._persist(state)
        delete = ProviderEffectIntent.create(
            operation_id=operation_id,
            effect_id=uuid5(NAMESPACE_URL, f"dependency-smoke-delete:{task_id}"),
            idempotency_key=f"dependency-smoke-delete:{task_id}",
            task_id=task_id,
            action=MutationAction.DELETE_NOTEBOOK,
            provider_ref=claim.provider_ref,
            expected_fingerprint=claim.fingerprint,
            arguments={"claim_sha256": claim.claim_sha256, "provider_version": claim.provider_version},
            requested_at=self.clock(),
        )
        self.adapter.delete_task_created_resource(intent=delete, claim=claim)
        state["state"] = "COMPLETE"
        self._persist(state)

    def run_once(self) -> EmbeddingDependencySmokeReceipt | None:
        task_id = uuid5(
            NAMESPACE_URL, f"embedding-dependency-smoke:{self.runtime_dataset_exact_ref}:{self.image_identity}"
        )
        state = self._load()
        receipt = self._load_receipt()
        source = self._source(task_id)
        source_sha = hashlib.sha256(source).hexdigest()
        notebook_ref = f"{self.owner}/mdh-embedding-dependency-smoke"
        operation_id = uuid5(NAMESPACE_URL, f"dependency-smoke-operation:{task_id}")
        if state is not None and (
            state.get("schema_version") != "embedding-dependency-smoke-state.v1"
            or state.get("task_id") != str(task_id)
            or state.get("source_sha256") != source_sha
            or state.get("state")
            not in {"REQUESTED", "LAUNCHED", "RECEIPT", "CLEANUP_REQUESTED", "COMPLETE"}
            or not isinstance(state.get("created_at"), str)
        ):
            raise ValueError("dependency smoke state differs from the exact runtime Dataset/source")
        if state is not None and self.clock() > datetime.fromisoformat(state["created_at"]) + timedelta(minutes=30):
            raise TimeoutError("dependency smoke exceeded its fixed 30-minute deadline")
        if receipt is not None and state is not None:
            if state["state"] != "COMPLETE":
                self._cleanup(state, task_id, operation_id)
            return receipt
        if state is None:
            state = {
                "schema_version": "embedding-dependency-smoke-state.v1",
                "state": "REQUESTED",
                "task_id": str(task_id),
                "source_sha256": source_sha,
                "created_at": self.clock().isoformat(),
            }
            self._persist(state)
        if state["state"] == "REQUESTED":
            intent = ProviderEffectIntent.create(
                operation_id=operation_id,
                effect_id=uuid5(NAMESPACE_URL, f"dependency-smoke-push:{task_id}"),
                idempotency_key=f"dependency-smoke-push:{task_id}",
                task_id=task_id,
                action=MutationAction.PUSH_NOTEBOOK,
                provider_ref=notebook_ref,
                arguments={
                    "task_run_id": str(task_id),
                    "source_sha256": source_sha,
                    "dataset_sources": (self.runtime_dataset_exact_ref,),
                    "control_class": "orchestrator_protected",
                    "disposable": True,
                    "docker_image": self.image_identity,
                    "docker_image_pinning_type": "original",
                },
                requested_at=self.clock(),
            )
            launched = self.adapter.push_private_dependency_smoke_notebook(
                intent=intent,
                task_run_id=task_id,
                source=source,
                title=notebook_ref.split("/")[1],
                code_file="smoke.py",
                kernel_type="script",
                language="python",
                control_class=ControlClass.ORCHESTRATOR_PROTECTED,
                disposable=True,
                dataset_sources=(self.runtime_dataset_exact_ref,),
                enable_internet=False,
                docker_image=self.image_identity,
                docker_image_pinning_type="original",
                timeout_seconds=1800,
            )
            state.update(
                {
                    "state": "LAUNCHED",
                    "run": launched.run.model_dump(mode="json"),
                    "claim": launched.claim.model_dump(mode="json"),
                }
            )
            self._persist(state)
        from my_data_hub.providers.kaggle.contracts import KaggleKernelRunIdentity

        run = KaggleKernelRunIdentity.model_validate(state["run"])
        status = self.adapter.read_run_status(run)
        if str(status.state) == "failed":
            self._cleanup(state, task_id, operation_id)
            raise RuntimeError("dependency smoke provider run failed")
        if str(status.state) != "complete":
            return None
        with tempfile.TemporaryDirectory(prefix="mdh-dependency-smoke-") as folder:
            target = Path(folder)
            self.adapter.download_exact_run_output_file(
                run, destination=target, file_name=OBSERVATION, max_bytes=256 * 1024
            )
            raw = (target / OBSERVATION).read_bytes()
        observation = json.loads(raw)
        if raw != canonical_json_bytes(observation):
            raise ValueError("dependency smoke observation is not canonical")
        required = {
            "schema_version",
            "status",
            "expected_image_identity",
            "image_source_commit",
            "python_version",
            "dependency_manifest_sha256",
            "project_wheel_sha256",
            "wheel_sha256s",
            "imports",
            "psycopg_implementation",
            "distributions",
        }
        if (
            set(observation) != required
            or observation["schema_version"] != "my-data-hub-embedding-dependency-smoke-observation.v1"
            or observation["status"] != "imports_passed"
            or observation["expected_image_identity"] != self.image_identity
            or observation["image_source_commit"] != self.image_source_commit
            or observation["dependency_manifest_sha256"] != self.dependency_manifest_sha256
            or observation["project_wheel_sha256"] != self.project_wheel_sha256
        ):
            raise ValueError("dependency smoke observation differs from exact launch")
        receipt = EmbeddingDependencySmokeReceipt(
            observed_at=self.clock(),
            provider_run_ref=run.provider_run_ref,
            observation_sha256=hashlib.sha256(raw).hexdigest(),
            image_identity=self.image_identity,
            image_source_commit=self.image_source_commit,
            python_version=observation["python_version"],
            dependency_manifest_sha256=self.dependency_manifest_sha256,
            project_wheel_sha256=self.project_wheel_sha256,
            wheel_sha256s=observation["wheel_sha256s"],
            imports=observation["imports"],
            psycopg_implementation=observation["psycopg_implementation"],
            distributions=observation["distributions"],
        )
        body = canonical_json_bytes(receipt.model_dump(mode="json"))
        self._write_receipt(body)
        state["state"] = "RECEIPT"
        self._persist(state)
        self._cleanup(state, task_id, operation_id)
        return receipt
