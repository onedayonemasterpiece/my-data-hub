from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import UUID

import pytest

from my_data_hub.embeddings.central_launcher import (
    CentralEmbeddingWorkerLauncher,
    EmbeddingWorkerDirectAccess,
    EmbeddingWorkerLaunchConfig,
)
from my_data_hub.embeddings.direct_plane import EmbeddingLaunchMetadata
from my_data_hub.providers.kaggle.contracts import TaskResourceClaim
from my_data_hub.providers.models import ControlClass, ProviderFingerprint, ProviderKind


class Adapter:
    def __init__(self) -> None:
        self.datasets: list[dict[str, object]] = []
        self.runs: list[dict[str, object]] = []

    def create_private_dataset(self, **kwargs):  # type: ignore[no-untyped-def]
        self.datasets.append(kwargs)
        return SimpleNamespace(claim=self._claim("owner/status", ProviderKind.DATASET))

    def push_private_notebook_pending_runtime_attestation(self, **kwargs):  # type: ignore[no-untyped-def]
        self.runs.append(kwargs)
        return SimpleNamespace(
            run=SimpleNamespace(provider_run_ref="owner/kernel/runs/1"),
            claim=self._claim("owner/kernel", ProviderKind.NOTEBOOK),
        )

    def delete_task_created_resource(self, **kwargs):  # type: ignore[no-untyped-def]
        return kwargs["claim"].provider_ref

    @staticmethod
    def _claim(ref, kind):  # type: ignore[no-untyped-def]
        return TaskResourceClaim.create(
            task_id=UUID("22222222-2222-4222-8222-222222222222"),
            effect_id=UUID("33333333-3333-4333-8333-333333333333"), provider_ref=ref,
            kind=kind, control_class=ControlClass.ORCHESTRATOR_PROTECTED, disposable=True,
            fingerprint=ProviderFingerprint(value="a" * 64), provider_version=1,
            registered_at=datetime(2026, 8, 12, tzinfo=UTC),
        )


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


def test_cleanup_requires_revocation_and_deletes_exact_claims() -> None:
    adapter = Adapter()
    now = datetime(2026, 8, 12, tzinfo=UTC)
    revoked = []

    class Access:
        def __call__(self, _metadata, _token):  # type: ignore[no-untyped-def]
            return EmbeddingWorkerDirectAccess(
                database_url="postgresql://w:s@direct.example/hub", tls_ca_pem="ca",
                expires_at=now + timedelta(minutes=5), epoch=7,
                tunnel_endpoint="direct.example:443",
                credential_id=UUID("33333333-3333-4333-8333-333333333333"),
            )

        def revoke(self, credential_id, *, task_run_id, **_kwargs):  # type: ignore[no-untyped-def]
            revoked.append((credential_id, task_run_id))

    launcher = CentralEmbeddingWorkerLauncher(
        adapter=adapter, access_factory=Access(),
        config=EmbeddingWorkerLaunchConfig(
            "owner", "owner/runtime/12", "image", "wheel.whl", "e" * 64, "https://c/e"
        ), clock=lambda: now,
    )
    receipt = launcher.launch(_metadata())
    deleted = launcher.cleanup(receipt.task_run_id)
    assert revoked == [(receipt.credential_id, receipt.task_run_id)]
    assert deleted == ("owner/kernel", "owner/status")
    with pytest.raises(ValueError, match="exact durable claims"):
        launcher.cleanup(receipt.task_run_id)


def test_restart_loads_secret_free_journal_and_cleans_without_relaunch(tmp_path) -> None:  # type: ignore[no-untyped-def]
    adapter = Adapter()
    now = datetime(2026, 8, 12, tzinfo=UTC)
    revoked = []
    class Access:
        def __call__(self, *_args):  # type: ignore[no-untyped-def]
            return EmbeddingWorkerDirectAccess(
                database_url="postgresql://w:s@d/h", tls_ca_pem="ca",
                expires_at=now + timedelta(minutes=5), epoch=7, tunnel_endpoint="d:1",
                credential_id=UUID("33333333-3333-4333-8333-333333333333"))
        def revoke(self, credential_id, *, task_run_id, **_kwargs): revoked.append((credential_id, task_run_id))  # type: ignore[no-untyped-def]
    config = EmbeddingWorkerLaunchConfig("owner", "owner/runtime/12", "image", "wheel", "e"*64, "https://c/e")
    journal = tmp_path / "launches.json"
    first = CentralEmbeddingWorkerLauncher(adapter, Access(), config, clock=lambda: now, journal_path=journal)
    receipt = first.launch(_metadata())
    assert "opaque-secret" not in journal.read_text() and journal.stat().st_mode & 0o777 == 0o600
    restarted = CentralEmbeddingWorkerLauncher(adapter, Access(), config, clock=lambda: now, journal_path=journal)
    restarted.cleanup(receipt.task_run_id)
    assert len(adapter.runs) == 1 and len(revoked) == 1
