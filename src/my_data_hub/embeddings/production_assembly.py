"""Environment assembly for the one central embedding provider adapter."""

from __future__ import annotations

import os
import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from my_data_hub.embeddings.central_launcher import (
    CentralEmbeddingWorkerLauncher,
    EmbeddingWorkerLaunchConfig,
)
from my_data_hub.embeddings.credential_authority import DirectoryEmbeddingCredentialAuthority
from my_data_hub.embeddings.direct_access_factory import (
    ExistingEpochEmbeddingAccessFactory,
    WorkerReachableTunnel,
)
from my_data_hub.embeddings.direct_plane import EmbeddingLaunchMetadata


@dataclass(slots=True)
class SSHEmbeddingAccessFactory:
    direct: ExistingEpochEmbeddingAccessFactory
    broker: Any
    master_instance: Callable[[], str]
    gateway_host: str
    gateway_port: int
    known_hosts_path: Path

    @property
    def ready(self) -> bool:
        return self.direct.ready and self.known_hosts_path.is_file() and not self.known_hosts_path.is_symlink()

    def __call__(self, metadata: EmbeddingLaunchMetadata, task_token: str):  # type: ignore[no-untyped-def]
        access = self.direct(metadata, task_token)
        key = Ed25519PrivateKey.generate()
        private = key.private_bytes(
            serialization.Encoding.PEM, serialization.PrivateFormat.OpenSSH,
            serialization.NoEncryption(),
        ).decode()
        public = key.public_key().public_bytes(
            serialization.Encoding.OpenSSH, serialization.PublicFormat.OpenSSH,
        ).decode()
        certificate = self.broker.issue_worker_public_key(
            master_instance_id=self.master_instance(), epoch=metadata.epoch,
            task_run_id=str(metadata.task_run_id), credential_id=str(access.credential_id),
            public_key=public, valid_before=access.expires_at, now=datetime.now(UTC),
        )
        return access.model_copy(update={
            "ssh_private_key": private, "ssh_certificate": certificate.certificate,
            "ssh_known_hosts": self.known_hosts_path.read_text(),
            "ssh_gateway_host": self.gateway_host, "ssh_gateway_port": self.gateway_port,
            "ssh_account": certificate.account,
            "ssh_certificate_serial": certificate.serial,
        })

    def revoke(self, credential_id, *, task_run_id, serial=None, epoch=None):  # type: ignore[no-untyped-def]
        if serial is None:
            raise ValueError("worker SSH certificate serial is absent")
        self.broker.revoke_worker_certificate(
            master_instance_id=self.master_instance(), epoch=int(epoch or 0), task_run_id=str(task_run_id),
            credential_id=str(credential_id), serial=serial, reason="embedding_worker_terminal",
        )
        self.direct.revoke(credential_id, task_run_id=task_run_id)


def build_embedding_production_assembly(
    adapter: object, *, broker: Any = None, master_instance: Callable[[], str] | None = None,
    runtime_dataset_exact_ref: str | None = None,
) -> tuple[
    CentralEmbeddingWorkerLauncher, DirectoryEmbeddingCredentialAuthority
] | None:
    enabled = os.getenv("MY_DATA_HUB_EMBEDDING_WORKERS_ENABLED", "false").strip().lower()
    if enabled in {"0", "false", "no", "off"}:
        return None
    if enabled not in {"1", "true", "yes", "on"}:
        raise ValueError("embedding worker enablement must be boolean")
    names = (
        "MY_DATA_HUB_EMBEDDING_CREDENTIAL_DIR", "MY_DATA_HUB_MASTER_TUNNEL_GATEWAY_HOST",
        "MY_DATA_HUB_MASTER_TUNNEL_GATEWAY_PORT", "MY_DATA_HUB_MASTER_TLS_CA_PATH",
        "MY_DATA_HUB_EMBEDDING_RUNTIME_IMAGE_IDENTITY",
        "MY_DATA_HUB_EMBEDDING_WHEEL_RELATIVE_PATH", "MY_DATA_HUB_EMBEDDING_WHEEL_SHA256",
        "MY_DATA_HUB_CALLBACK_URL", "MY_DATA_HUB_KAGGLE_OWNER",
        "MY_DATA_HUB_MASTER_TUNNEL_KNOWN_HOSTS_PATH",
        "MY_DATA_HUB_EMBEDDING_RUNTIME_PYTHON_SERIES",
        "MY_DATA_HUB_EMBEDDING_RUNTIME_SOURCE_COMMIT",
    )
    values = {name: os.getenv(name, "").strip() for name in names}
    if not all(values.values()) or runtime_dataset_exact_ref is None:
        raise ValueError("embedding production assembly environment is incomplete")
    if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/[1-9][0-9]*", runtime_dataset_exact_ref):
        raise ValueError("embedding runtime Dataset claim is not exact numeric")
    authority = DirectoryEmbeddingCredentialAuthority(Path(values[names[0]]))
    tunnel = WorkerReachableTunnel(
        values[names[1]], int(values[names[2]]), Path(values[names[3]])
    )
    direct = ExistingEpochEmbeddingAccessFactory(authority, tunnel)
    if not direct.ready or broker is None or master_instance is None:
        raise ValueError(direct.missing_component() or "embedding SSH tunnel authority unavailable")
    access = SSHEmbeddingAccessFactory(
        direct, broker, master_instance, values[names[1]], int(values[names[2]]), Path(values[names[9]])
    )
    launcher = CentralEmbeddingWorkerLauncher(
        adapter=adapter, access_factory=access,
        config=EmbeddingWorkerLaunchConfig(
            owner=values[names[8]], runtime_dataset_exact_ref=runtime_dataset_exact_ref,
            runtime_image_identity=values[names[4]], wheel_relative_path=values[names[5]],
            wheel_sha256=values[names[6]], callback_url=values[names[7]],
            runtime_python_series=values[names[10]], runtime_image_source_commit=values[names[11]],
        ),
        journal_path=Path(values[names[0]]) / "launcher-journal.json",
    )
    return launcher, authority
