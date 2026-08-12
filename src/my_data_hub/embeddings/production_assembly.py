"""Environment assembly for the one central embedding provider adapter."""

from __future__ import annotations

import os
from pathlib import Path

from my_data_hub.embeddings.central_launcher import (
    CentralEmbeddingWorkerLauncher,
    EmbeddingWorkerLaunchConfig,
)
from my_data_hub.embeddings.credential_authority import DirectoryEmbeddingCredentialAuthority
from my_data_hub.embeddings.direct_access_factory import (
    ExistingEpochEmbeddingAccessFactory,
    WorkerReachableTunnel,
)


def build_embedding_production_assembly(adapter: object) -> tuple[
    CentralEmbeddingWorkerLauncher, DirectoryEmbeddingCredentialAuthority
] | None:
    names = (
        "MY_DATA_HUB_EMBEDDING_CREDENTIAL_DIR", "MY_DATA_HUB_EMBEDDING_WORKER_TUNNEL_HOST",
        "MY_DATA_HUB_EMBEDDING_WORKER_TUNNEL_PORT", "MY_DATA_HUB_MASTER_TLS_CA_PATH",
        "MY_DATA_HUB_EMBEDDING_RUNTIME_DATASET_EXACT_REF", "MY_DATA_HUB_EMBEDDING_RUNTIME_IMAGE_IDENTITY",
        "MY_DATA_HUB_EMBEDDING_WHEEL_RELATIVE_PATH", "MY_DATA_HUB_EMBEDDING_WHEEL_SHA256",
        "MY_DATA_HUB_CALLBACK_URL", "MY_DATA_HUB_KAGGLE_OWNER",
    )
    values = {name: os.getenv(name, "").strip() for name in names}
    if not any(values.values()):
        return None
    if not all(values.values()):
        raise ValueError("embedding production assembly environment is incomplete")
    authority = DirectoryEmbeddingCredentialAuthority(Path(values[names[0]]))
    tunnel = WorkerReachableTunnel(
        values[names[1]], int(values[names[2]]), Path(values[names[3]])
    )
    access = ExistingEpochEmbeddingAccessFactory(authority, tunnel)
    if not access.ready:
        raise ValueError(access.missing_component() or "embedding direct access unavailable")
    launcher = CentralEmbeddingWorkerLauncher(
        adapter=adapter, access_factory=access,
        config=EmbeddingWorkerLaunchConfig(
            owner=values[names[9]], runtime_dataset_exact_ref=values[names[4]],
            runtime_image_identity=values[names[5]], wheel_relative_path=values[names[6]],
            wheel_sha256=values[names[7]], callback_url=values[names[8]],
        ),
    )
    return launcher, authority
