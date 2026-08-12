from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import UUID

from my_data_hub.embeddings.central_launcher import (
    CentralEmbeddingWorkerLauncher,
    EmbeddingWorkerDirectAccess,
    EmbeddingWorkerLaunchConfig,
)
from my_data_hub.embeddings.direct_plane import EmbeddingLaunchMetadata


class Adapter:
    def __init__(self) -> None:
        self.datasets: list[dict[str, object]] = []
        self.runs: list[dict[str, object]] = []

    def create_private_dataset(self, **kwargs):  # type: ignore[no-untyped-def]
        self.datasets.append(kwargs)
        return SimpleNamespace(claim=SimpleNamespace(provider_version=1))

    def push_private_notebook_pending_runtime_attestation(self, **kwargs):  # type: ignore[no-untyped-def]
        self.runs.append(kwargs)
        return SimpleNamespace(run=SimpleNamespace(provider_run_ref="owner/kernel/runs/1"))


def _metadata() -> EmbeddingLaunchMetadata:
    return EmbeddingLaunchMetadata(
        schema_version="embedding-central-launch-metadata.v1",
        request_id=UUID("11111111-1111-4111-8111-111111111111"), request_sha256="a" * 64,
        task_run_id=UUID("22222222-2222-4222-8222-222222222222"),
        model_exact_id="intfloat/multilingual-e5-base@revision", input_jobs_sha256="b" * 64,
        job_count=267, worker_source_sha256="c" * 64,
        worker_primary_source_sha256="c" * 64, epoch=7,
    )


def test_single_adapter_launch_is_idempotent_and_status_contains_no_business_bytes() -> None:
    adapter = Adapter()
    now = datetime(2026, 8, 12, tzinfo=UTC)
    calls = 0

    def access(metadata, token):  # type: ignore[no-untyped-def]
        nonlocal calls
        calls += 1
        assert metadata.epoch == 7 and len(token) >= 32
        return EmbeddingWorkerDirectAccess(
            database_url="postgresql://worker:secret@direct.example:443/hub?sslmode=verify-ca",
            tls_ca_pem="-----BEGIN CERTIFICATE-----\ncertificate\n-----END CERTIFICATE-----",
            expires_at=now + timedelta(minutes=5), epoch=7,
            tunnel_endpoint="direct.example:443",
            credential_id=UUID("33333333-3333-4333-8333-333333333333"),
        )

    launcher = CentralEmbeddingWorkerLauncher(
        adapter=adapter, access_factory=access,
        config=EmbeddingWorkerLaunchConfig(
            owner="owner", runtime_dataset_exact_ref="owner/runtime/12",
            runtime_image_identity="runtime@sha256:" + "d" * 64,
            wheel_relative_path="dist/my_data_hub.whl", wheel_sha256="e" * 64,
            callback_url="https://control.example/internal/embedding-workers/events",
        ), clock=lambda: now,
    )
    first = launcher.launch(_metadata())
    second = launcher.launch(_metadata())
    assert first == second and calls == 1
    assert len(adapter.datasets) == len(adapter.runs) == 1
    payload = json.loads(adapter.datasets[0]["files"]["embedding-worker.json"])  # type: ignore[index]
    assert set(payload) == {"schema_version", "launch", "direct_access", "callback", "runtime"}
    assert "jobs" not in payload["launch"] and "vectors" not in payload
    assert payload["launch"]["input_jobs_sha256"] == "b" * 64
    assert adapter.runs[0]["dataset_sources"] == ("owner/runtime/12", first.status_dataset_exact_ref)


def test_launcher_rejects_access_for_wrong_epoch_before_provider_mutation() -> None:
    adapter = Adapter()
    now = datetime(2026, 8, 12, tzinfo=UTC)

    def access(_metadata, _token):  # type: ignore[no-untyped-def]
        return EmbeddingWorkerDirectAccess(
            database_url="postgresql://worker:secret@direct.example/hub", tls_ca_pem="ca",
            expires_at=now + timedelta(minutes=5), epoch=6, tunnel_endpoint="direct.example:443",
            credential_id=UUID("33333333-3333-4333-8333-333333333333"),
        )

    launcher = CentralEmbeddingWorkerLauncher(
        adapter=adapter, access_factory=access,
        config=EmbeddingWorkerLaunchConfig("owner", "owner/runtime/12", "image", "wheel.whl", "e"*64, "https://c/e"),
        clock=lambda: now,
    )
    import pytest
    with pytest.raises(ValueError, match="another epoch"):
        launcher.launch(_metadata())
    assert adapter.datasets == adapter.runs == []
