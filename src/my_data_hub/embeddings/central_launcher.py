"""Single-adapter Kaggle launcher for direct ACTIVE-master embedding workers.

Only launch metadata and short-lived connection capability material are placed in
the disposable private status Dataset.  Job documents and vectors are never
accepted by this component.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import secrets
import tempfile
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
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
    ssh_private_key: SecretStr | None = None
    ssh_certificate: SecretStr | None = None
    ssh_known_hosts: SecretStr | None = None
    ssh_gateway_host: str | None = None
    ssh_gateway_port: int | None = None
    ssh_account: str | None = None
    ssh_certificate_serial: int | None = None

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
    def push_private_worker_notebook_pending_attestation(self, **kwargs: Any) -> Any: ...


@dataclass(frozen=True, slots=True)
class EmbeddingWorkerLaunchConfig:
    owner: str
    runtime_dataset_exact_ref: str
    runtime_image_identity: str
    wheel_relative_path: str
    wheel_sha256: str
    callback_url: str
    runtime_python_series: str = "3.12"
    runtime_image_source_commit: str = ""
    dependency_manifest_sha256: str = ""
    dependency_smoke_receipt: bytes = b""


@dataclass(frozen=True, slots=True)
class EmbeddingWorkerLaunchReceipt:
    task_run_id: UUID
    status_dataset_exact_ref: str
    provider_run_ref: str
    source_sha256: str
    credential_id: UUID
    credential_expires_at: datetime
    status_claim: Any = None
    notebook_claim: Any = None
    ssh_certificate_serial: int | None = None
    epoch: int = 1


@dataclass(slots=True)
class CentralEmbeddingWorkerLauncher:
    """Concrete production launcher using the already-constructed central adapter."""

    adapter: CentralAdapter
    access_factory: WorkerAccessFactory
    config: EmbeddingWorkerLaunchConfig
    clock: Callable[[], datetime] = lambda: datetime.now(UTC)
    journal_path: Path | None = None
    _receipts: dict[UUID, EmbeddingWorkerLaunchReceipt] = field(default_factory=dict, init=False)
    _states: dict[UUID, dict[str, Any]] = field(default_factory=dict, init=False)

    def launch(self, metadata: EmbeddingLaunchMetadata) -> EmbeddingWorkerLaunchReceipt:
        self._load_journal()
        existing = self._receipts.get(metadata.task_run_id)
        if existing is not None:
            return existing
        now = self.clock().astimezone(UTC)
        prior = self._states.get(metadata.task_run_id)
        if prior is None:
            self._states[metadata.task_run_id] = {
                "state": "REQUESTED", "metadata": metadata.model_dump(mode="json"),
                "updated_at": now.isoformat(),
            }
            self._save_journal()
        elif prior["state"] in {"STATUS_CREATED", "CLEANUP_REQUESTED", "COMPLETE"}:
            raise ValueError("partial embedding launch requires idempotent cleanup before relaunch")
        secret_path = self._secret_path(metadata.task_run_id)
        saved = self._read_secret(secret_path, metadata) if secret_path is not None and secret_path.exists() else None
        if saved is not None and saved.get("direct_access") is not None:
            status = saved
            direct = status["direct_access"]
            access = EmbeddingWorkerDirectAccess(
                database_url=direct["database_url"], tls_ca_pem=direct["tls_ca_pem"],
                expires_at=datetime.fromisoformat(direct["expires_at"]), epoch=direct["epoch"],
                tunnel_endpoint=direct["tunnel_endpoint"], credential_id=UUID(direct["credential_id"]),
                ssh_private_key=direct.get("ssh_private_key"), ssh_certificate=direct.get("ssh_certificate"),
                ssh_known_hosts=direct.get("ssh_known_hosts"), ssh_gateway_host=direct.get("ssh_gateway_host"),
                ssh_gateway_port=direct.get("ssh_gateway_port"), ssh_account=direct.get("ssh_account"),
                ssh_certificate_serial=direct.get("ssh_certificate_serial"),
            )
            task_token = status["callback"]["task_token"]
        else:
            task_token = str(saved["callback"]["task_token"]) if saved is not None else secrets.token_urlsafe(32)
            if saved is None and secret_path is not None:
                self._write_secret(secret_path, canonical_json_bytes({
                    "schema_version": "embedding-worker-capability.v1",
                    "launch": metadata.model_dump(mode="json"),
                    "callback": {"task_token": task_token}, "direct_access": None,
                }))
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
                "ssh_private_key": access.ssh_private_key.get_secret_value() if access.ssh_private_key else None,
                "ssh_certificate": access.ssh_certificate.get_secret_value() if access.ssh_certificate else None,
                "ssh_known_hosts": access.ssh_known_hosts.get_secret_value() if access.ssh_known_hosts else None,
                "ssh_gateway_host": access.ssh_gateway_host,
                "ssh_gateway_port": access.ssh_gateway_port,
                "ssh_account": access.ssh_account,
                "ssh_certificate_serial": access.ssh_certificate_serial,
            },
            "callback": {"url": self.config.callback_url, "task_token": task_token},
            "runtime": {
                "dataset_exact_ref": self.config.runtime_dataset_exact_ref,
                "image_identity": self.config.runtime_image_identity,
                "wheel_relative_path": self.config.wheel_relative_path,
                "wheel_sha256": self.config.wheel_sha256,
                "input_dataset_versions": [self.config.runtime_dataset_exact_ref, f"{status_ref}/1"],
            },
        }
        files = {"embedding-worker.json": canonical_json_bytes(status)}
        worker_assets = __import__(
            "my_data_hub.embeddings.production", fromlist=["WORKER_ASSETS"]
        ).WORKER_ASSETS
        asset = next(item for item in worker_assets if item.model.exact_id == metadata.model_exact_id)
        pins = {
            "schema": "my-data-hub-notebook-execution-pins/v1", "notebook": asset.notebook_slug,
            "python_series": self.config.runtime_python_series,
            "image_source_commit": self.config.runtime_image_source_commit,
            "kaggle_runtime_image_identity": self.config.runtime_image_identity,
            "input_dataset_versions": [self.config.runtime_dataset_exact_ref, f"{status_ref}/1"],
            "immutable_asset_sha256s": {
                "my_data_hub_wheel_sha256": self.config.wheel_sha256,
                "primary_source_sha256": metadata.worker_primary_source_sha256,
            },
            "output_contract": "my-data-hub-blogger-embedding-artifact.v1",
            "model": {"id": asset.model.model_key, "revision": asset.model.revision},
            "privacy": "private", "resource_class": "orchestrator_protected",
            "cleanup_retention_policy": {
                "cleanup_receipt_required": True,
                "notebook_resource": "orchestrator_protected_until_owner_supersedes",
                "run_outputs": "retain_until_terminal_receipt_then_control_policy",
                "task_owned_inputs": "claim_bound_delete_after_terminal_or_expiry",
            },
        }
        files["execution-pins.json"] = canonical_json_bytes(pins)
        if self.config.dependency_manifest_sha256:
            if not self.config.dependency_smoke_receipt:
                raise ValueError("verified embedding dependency smoke receipt is required")
            smoke_sha = hashlib.sha256(self.config.dependency_smoke_receipt).hexdigest()
            pins["immutable_asset_sha256s"].update({
                "embedding_dependency_manifest_sha256": self.config.dependency_manifest_sha256,
                "embedding_dependency_smoke_receipt_sha256": smoke_sha,
            })
            files["execution-pins.json"] = canonical_json_bytes(pins)
            files["embedding-dependency-smoke-receipt.json"] = self.config.dependency_smoke_receipt
        if secret_path is not None and (saved is None or saved.get("direct_access") is None):
            self._write_secret(secret_path, files["embedding-worker.json"])
            self._states[metadata.task_run_id].update({
                "state": "ACCESS_READY", "credential_id": str(access.credential_id),
                "credential_expires_at": access.expires_at.isoformat(),
            })
            self._save_journal()
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
        self._states[metadata.task_run_id]["state"] = "STATUS_CREATED"
        self._states[metadata.task_run_id]["status_claim"] = dataset.claim.model_dump(mode="json")
        self._save_journal()
        exact_status = f"{status_ref}/{dataset.claim.provider_version}"
        source = self._render_source(status_ref, metadata.task_run_id)
        source_sha = hashlib.sha256(source).hexdigest()
        self._states[metadata.task_run_id]["expected_source_sha256"] = source_sha
        self._save_journal()
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
                       "disposable": True, "docker_image": self.config.runtime_image_identity,
                       "docker_image_pinning_type": "original"}, requested_at=now,
        )
        run = self.adapter.push_private_worker_notebook_pending_attestation(
            intent=push_intent, task_run_id=metadata.task_run_id, source=source,
            title=notebook_ref.split("/", 1)[1], code_file="worker.py", kernel_type="script",
            language="python", control_class=ControlClass.ORCHESTRATOR_PROTECTED,
            disposable=True, dataset_sources=sources, enable_internet=True,
            timeout_seconds=10_800, docker_image=self.config.runtime_image_identity,
            docker_image_pinning_type="original",
        )
        receipt = EmbeddingWorkerLaunchReceipt(
            task_run_id=metadata.task_run_id, status_dataset_exact_ref=exact_status,
            provider_run_ref=run.run.provider_run_ref, source_sha256=source_sha,
            credential_id=access.credential_id, credential_expires_at=access.expires_at,
            status_claim=dataset.claim, notebook_claim=run.claim,
            ssh_certificate_serial=access.ssh_certificate_serial,
            epoch=access.epoch,
        )
        self._receipts[metadata.task_run_id] = receipt
        self._states[metadata.task_run_id]["state"] = "LAUNCHED"
        self._save_journal()
        return receipt

    def attest_runtime_source(
        self, *, task_run_id: UUID, task_token: str, source_sha256: str,
        image_identity: str, epoch: int,
        image_source_commit: str,
    ) -> None:
        self._load_journal()
        state = self._states.get(task_run_id)
        secret_path = self._secret_path(task_run_id)
        if state is None or secret_path is None:
            raise ValueError("embedding launch attestation is unknown")
        metadata = EmbeddingLaunchMetadata.model_validate(state["metadata"])
        secret = self._read_secret(secret_path, metadata)
        expected_token = str(secret["callback"]["task_token"])
        if (
            not hmac.compare_digest(task_token, expected_token)
            or source_sha256 != state.get("expected_source_sha256")
            or image_identity != self.config.runtime_image_identity
            or image_source_commit != self.config.runtime_image_source_commit
            or epoch != metadata.epoch
        ):
            raise ValueError("embedding runtime attestation binding differs")
        state["runtime_attested"] = True
        self._save_journal()

    def cleanup(self, task_run_id: UUID) -> tuple[object | None, object | None]:
        """Idempotently revoke access then delete exact disposable resources."""

        self._load_journal()
        receipt = self._receipts.get(task_run_id)
        state = self._states.get(task_run_id)
        if state is None:
            raise ValueError("embedding worker cleanup lacks durable launch state")
        if state["state"] == "COMPLETE":
            return None, None
        revoke = getattr(self.access_factory, "revoke", None)
        if not callable(revoke):
            raise ValueError("embedding worker access revocation is unavailable")
        state["state"] = "CLEANUP_REQUESTED"
        state["updated_at"] = self.clock().astimezone(UTC).isoformat()
        self._save_journal()
        secret_path = self._secret_path(task_run_id)
        metadata = EmbeddingLaunchMetadata.model_validate(state["metadata"])
        secret = self._read_secret(secret_path, metadata) if secret_path is not None else None
        direct = secret.get("direct_access") if isinstance(secret, dict) else None
        credential_id = receipt.credential_id if receipt is not None else UUID(str(state["credential_id"]))
        certificate_serial = (
            receipt.ssh_certificate_serial if receipt is not None
            else direct.get("ssh_certificate_serial") if isinstance(direct, dict) else None
        )
        epoch = receipt.epoch if receipt is not None else metadata.epoch
        revoke(credential_id, task_run_id=task_run_id, serial=certificate_serial, epoch=epoch)
        operation_id = uuid5(NAMESPACE_URL, f"embedding-worker-cleanup:{task_run_id}")
        claims: list[tuple[str, Any, MutationAction]] = []
        if receipt is not None:
            claims.extend((
                ("notebook", receipt.notebook_claim, MutationAction.DELETE_NOTEBOOK),
                ("status", receipt.status_claim, MutationAction.DELETE_DATASET),
            ))
        elif state.get("status_claim") is not None:
            from my_data_hub.providers.kaggle.contracts import TaskResourceClaim
            claims.append((
                "status", TaskResourceClaim.model_validate(state["status_claim"]), MutationAction.DELETE_DATASET,
            ))
        results: list[object] = []
        for label, claim, action in claims:
            intent = ProviderEffectIntent.create(
                operation_id=operation_id,
                effect_id=uuid5(NAMESPACE_URL, f"embedding-worker-cleanup:{label}:{task_run_id}"),
                idempotency_key=f"embedding-worker-cleanup:{label}:{task_run_id}",
                task_id=task_run_id, action=action, provider_ref=claim.provider_ref,
                expected_fingerprint=claim.fingerprint,
                arguments={"claim_sha256": claim.claim_sha256,
                           "provider_version": claim.provider_version},
                requested_at=self.clock().astimezone(UTC),
            )
            results.append(self.adapter.delete_task_created_resource(intent=intent, claim=claim))
        self._receipts.pop(task_run_id, None)
        self._states[task_run_id]["state"] = "COMPLETE"
        if secret_path is not None:
            secret_path.unlink(missing_ok=True)
        self._save_journal()
        if receipt is not None:
            return results[0], results[1]
        return (results[0] if results else None), None

    def reconcile_timeouts(self, *, now: datetime | None = None) -> tuple[UUID, ...]:
        self._load_journal()
        observed = (now or self.clock()).astimezone(UTC)
        cleaned: list[UUID] = []
        for task, receipt in tuple(self._receipts.items()):
            if receipt.credential_expires_at.astimezone(UTC) <= observed:
                self.cleanup(task)
                cleaned.append(task)
        return tuple(cleaned)

    def _save_journal(self) -> None:
        if self.journal_path is None:
            return
        if not self.journal_path.is_absolute() or self.journal_path.is_symlink():
            raise ValueError("embedding launcher journal path is unsafe")
        receipts = {str(task): {
            "task_run_id": str(r.task_run_id), "status_dataset_exact_ref": r.status_dataset_exact_ref,
            "provider_run_ref": r.provider_run_ref, "source_sha256": r.source_sha256,
            "credential_id": str(r.credential_id), "credential_expires_at": r.credential_expires_at.isoformat(),
            "status_claim": r.status_claim.model_dump(mode="json"),
            "notebook_claim": r.notebook_claim.model_dump(mode="json"),
            "ssh_certificate_serial": r.ssh_certificate_serial,
            "epoch": r.epoch,
        } for task, r in self._receipts.items()}
        states = {str(task): value for task, value in self._states.items()}
        encoded = canonical_json_bytes({
            "schema_version": "embedding-launch-journal.v2", "states": states, "receipts": receipts,
        }) + b"\n"
        if len(encoded) > 1024 * 1024:
            raise ValueError("embedding launcher journal is oversized")
        parent = self.journal_path.parent
        parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        parent.chmod(0o700)
        descriptor, raw = tempfile.mkstemp(prefix=".embedding-launches.", dir=parent)
        try:
            os.fchmod(descriptor, 0o600)
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(encoded)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(raw, self.journal_path)
            directory = os.open(parent, os.O_RDONLY)
            os.fsync(directory)
            os.close(directory)
        finally:
            Path(raw).unlink(missing_ok=True)

    def _secret_path(self, task_run_id: UUID) -> Path | None:
        if self.journal_path is None:
            return None
        return self.journal_path.parent / f"{task_run_id}.capability"

    @staticmethod
    def _write_secret(path: Path, content: bytes) -> None:
        if not path.is_absolute() or path.is_symlink() or len(content) > 1024 * 1024:
            raise ValueError("embedding capability path or content is unsafe")
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        path.parent.chmod(0o700)
        descriptor, raw = tempfile.mkstemp(prefix=".capability.", dir=path.parent)
        try:
            os.fchmod(descriptor, 0o600)
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(content)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(raw, path)
            directory = os.open(path.parent, os.O_RDONLY)
            os.fsync(directory)
            os.close(directory)
        finally:
            Path(raw).unlink(missing_ok=True)

    @staticmethod
    def _read_secret(path: Path, metadata: EmbeddingLaunchMetadata) -> dict[str, Any]:
        if (path.is_symlink() or not path.is_file() or path.stat().st_mode & 0o077
                or path.stat().st_size > 1024 * 1024 or path.parent.stat().st_mode & 0o077):
            raise ValueError("embedding capability file is unsafe")
        value = json.loads(path.read_text())
        if (not isinstance(value, dict) or value.get("schema_version") not in {
                "embedding-worker-capability.v1", "embedding-worker-status.v1"}
                or value.get("launch") != metadata.model_dump(mode="json")
                or not isinstance(value.get("callback", {}).get("task_token"), str)):
            raise ValueError("embedding capability binding differs")
        return value

    def _load_journal(self) -> None:
        if self._receipts or self.journal_path is None or not self.journal_path.exists():
            return
        from my_data_hub.providers.kaggle.contracts import TaskResourceClaim
        if (self.journal_path.is_symlink() or not self.journal_path.is_file()
                or self.journal_path.stat().st_mode & 0o077 or self.journal_path.stat().st_size > 1024 * 1024):
            raise ValueError("embedding launcher journal is unsafe")
        envelope = json.loads(self.journal_path.read_text())
        if (set(envelope) != {"schema_version", "states", "receipts"}
                or envelope["schema_version"] != "embedding-launch-journal.v2"):
            raise ValueError("embedding launcher journal schema differs")
        allowed_states = {"REQUESTED", "ACCESS_READY", "STATUS_CREATED", "LAUNCHED", "CLEANUP_REQUESTED", "COMPLETE"}
        for task, value in envelope["states"].items():
            task_id = UUID(task)
            allowed_keys = {
                "state", "metadata", "updated_at", "credential_id", "credential_expires_at", "status_claim",
                "expected_source_sha256", "runtime_attested",
            }
            if (not isinstance(value, dict) or not {"state", "metadata", "updated_at"} <= set(value)
                    or set(value) - allowed_keys or value.get("state") not in allowed_states
                    or str(value.get("metadata", {}).get("task_run_id")) != task):
                raise ValueError("embedding launcher journal task binding differs")
            datetime.fromisoformat(str(value["updated_at"]).replace("Z", "+00:00")).astimezone(UTC)
            self._states[task_id] = value
        for task, value in envelope["receipts"].items():
            self._receipts[UUID(task)] = EmbeddingWorkerLaunchReceipt(
                task_run_id=UUID(value["task_run_id"]), status_dataset_exact_ref=value["status_dataset_exact_ref"],
                provider_run_ref=value["provider_run_ref"], source_sha256=value["source_sha256"],
                credential_id=UUID(value["credential_id"]),
                credential_expires_at=datetime.fromisoformat(value["credential_expires_at"]),
                status_claim=TaskResourceClaim.model_validate(value["status_claim"]),
                notebook_claim=TaskResourceClaim.model_validate(value["notebook_claim"]),
                ssh_certificate_serial=value.get("ssh_certificate_serial"),
                epoch=int(value["epoch"]),
            )

    def _render_source(self, status_ref: str, task_run_id: UUID) -> bytes:
        status_mount = f"/kaggle/input/{status_ref.split('/', 1)[1]}"
        runtime_mount = f"/kaggle/input/{self.config.runtime_dataset_exact_ref.split('/', 1)[1]}"
        lines = [
            "import json, os, pathlib, subprocess, time",
            f'EXPECTED_TASK_RUN_ID={str(task_run_id)!r}',
            f's=json.loads(pathlib.Path({status_mount!r}, "embedding-worker.json").read_text())',
            'assert s["schema_version"] == "embedding-worker-status.v1"',
            f'pins=pathlib.Path({status_mount!r},"execution-pins.json")',
            'os.environ["MY_DATA_HUB_EXECUTION_PINS_PATH"]=str(pins)',
            'os.environ["MY_DATA_HUB_EXECUTION_PINS_SHA256"]=__import__("hashlib").sha256(pins.read_bytes()).hexdigest()',
            'a=s["direct_access"]; m=s["launch"]; r=s["runtime"]',
            'if m["task_run_id"] != EXPECTED_TASK_RUN_ID: raise RuntimeError("task run mismatch")',
            'if int(a["epoch"]) != int(m["epoch"]): raise RuntimeError("epoch mismatch")',
            'observed_commit=pathlib.Path("/etc/git_commit").read_text().strip()',
            'body=json.dumps({"task_run_id":m["task_run_id"],"source_sha256":__import__("hashlib").sha256(pathlib.Path(__file__).read_bytes()).hexdigest(),"image_identity":r["image_identity"],"image_source_commit":observed_commit,"epoch":m["epoch"]}).encode()',
            'attest_url=s["callback"]["url"].rstrip("/")+"/embedding-worker-attestation"',
            'headers={"Authorization":"Bearer "+s["callback"]["task_token"],"Content-Type":"application/json"}',
            'req=__import__("urllib.request",fromlist=["Request"]).Request(attest_url,data=body,headers=headers,method="POST")',
            '__import__("urllib.request",fromlist=["urlopen"]).urlopen(req,timeout=30).read()',
            'ca=pathlib.Path("/kaggle/working/mdh-worker-ca.pem")',
            'ca.write_text(a["tls_ca_pem"]); ca.chmod(0o600)',
            'if a.get("ssh_private_key"):',
            '    key=pathlib.Path("/kaggle/working/mdh-worker-ssh")',
            '    key.write_text(a["ssh_private_key"]); key.chmod(0o600)',
            '    cert=key.with_name(key.name+"-cert.pub")',
            '    cert.write_text(a["ssh_certificate"]+"\\n"); cert.chmod(0o600)',
            '    known=pathlib.Path("/kaggle/working/mdh-known-hosts")',
            '    known.write_text(a["ssh_known_hosts"]); known.chmod(0o600)',
            '    local_port=25433',
            '    destination=f"{a[\"ssh_account\"]}@{a[\"ssh_gateway_host\"]}"',
            '    ssh=subprocess.Popen(["ssh","-N","-L",',
            '      f"127.0.0.1:{local_port}:127.0.0.1:25432","-i",str(key),',
            '      "-o",f"CertificateFile={cert}","-o",f"UserKnownHostsFile={known}",',
            '      "-o","StrictHostKeyChecking=yes","-p",str(a["ssh_gateway_port"]),destination])',
            '    time.sleep(2); assert ssh.poll() is None',
            '    a["database_url"]=a["database_url"].replace(a["tunnel_endpoint"],f"127.0.0.1:{local_port}")',
            'os.environ["MY_DATA_HUB_EMBEDDING_DIRECT_DATABASE_URL"]=a["database_url"]',
            'os.environ["PGSSLROOTCERT"]=str(ca)',
            'os.environ["MY_DATA_HUB_EMBEDDING_REQUEST_ID"]=m["request_id"]',
            'os.environ["MY_DATA_HUB_RUN_ID"]=m["task_run_id"]',
            'os.environ["MY_DATA_HUB_EMBEDDING_INPUT_JOBS_SHA256"]=m["input_jobs_sha256"]',
            f'os.environ["MY_DATA_HUB_WHEEL_PATH"]=str(pathlib.Path({runtime_mount!r},r["wheel_relative_path"]))',
            'os.environ["MY_DATA_HUB_WHEEL_SHA256"]=r["wheel_sha256"]',
            'os.environ["MY_DATA_HUB_KAGGLE_RUNTIME_IMAGE_IDENTITY"]=r["image_identity"]',
            f'os.environ["MY_DATA_HUB_KAGGLE_RUNTIME_SOURCE_COMMIT"]={self.config.runtime_image_source_commit!r}',
            'os.environ["MY_DATA_HUB_NOTEBOOK_IS_PRIVATE"]="true"',
            'os.environ["MY_DATA_HUB_INPUT_DATASET_VERSIONS_JSON"]=json.dumps(r["input_dataset_versions"])',
            *( [
                f'os.environ["MY_DATA_HUB_EMBEDDING_DEPENDENCY_MANIFEST_SHA256"]={self.config.dependency_manifest_sha256!r}',
                f'os.environ["MY_DATA_HUB_EMBEDDING_DEPENDENCY_SMOKE_RECEIPT_PATH"]=str(pathlib.Path({status_mount!r},"embedding-dependency-smoke-receipt.json"))',
                f'os.environ["MY_DATA_HUB_EMBEDDING_DEPENDENCY_SMOKE_RECEIPT_SHA256"]={hashlib.sha256(self.config.dependency_smoke_receipt).hexdigest()!r}',
            ] if self.config.dependency_manifest_sha256 else []),
            'asset="e5-worker.json" if "multilingual-e5" in m["model_exact_id"] else "bge-worker.json"',
            f'n=json.loads(pathlib.Path({runtime_mount!r},asset).read_text())',
            'for c in n["cells"]:',
            '    if c["cell_type"]=="code": exec(compile(c["source"],asset,"exec"),globals())',
        ]
        code = "\n".join(lines) + "\n"
        return code.encode()

    @property
    def ready(self) -> bool:
        return callable(self.access_factory) and self.adapter is not None
