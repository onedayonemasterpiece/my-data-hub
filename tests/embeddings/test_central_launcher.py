from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import NAMESPACE_URL, UUID, uuid5

import pytest

from my_data_hub.embeddings.central_launcher import (
    CentralEmbeddingWorkerLauncher,
    EmbeddingWorkerDirectAccess,
    EmbeddingWorkerLaunchConfig,
)
from my_data_hub.embeddings.direct_plane import EmbeddingLaunchMetadata
from my_data_hub.providers.kaggle import (
    KaggleContractError,
    KaggleProviderAdapter,
    KaggleProviderIdentity,
    MutationAction,
    ProviderEffectIntent,
)
from my_data_hub.providers.kaggle.contracts import TaskResourceClaim
from my_data_hub.providers.models import ControlClass, ProviderFingerprint, ProviderKind


class Adapter:
    def __init__(self) -> None:
        self.datasets: list[dict[str, object]] = []
        self.runs: list[dict[str, object]] = []

    def create_private_dataset(self, **kwargs):  # type: ignore[no-untyped-def]
        self.datasets.append(kwargs)
        return SimpleNamespace(claim=self._claim("owner/status", ProviderKind.DATASET))

    def push_private_worker_notebook_pending_attestation(self, **kwargs):  # type: ignore[no-untyped-def]
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
        model_exact_id=(
            "intfloat/multilingual-e5-base@d128750597153bb5987e10b1c3493a34e5a4502a"
        ), input_jobs_sha256="b" * 64,
        job_count=267, worker_source_sha256="c" * 64,
        worker_primary_source_sha256="c" * 64, epoch=7,
    )


class _Journal:
    def persist_intent(self, _value): pass  # type: ignore[no-untyped-def]
    def persist_receipt(self, _value): pass  # type: ignore[no-untyped-def]
    def persist_resource_claim(self, _value): pass  # type: ignore[no-untyped-def]
    def assert_resource_claim(self, _value): pass  # type: ignore[no-untyped-def]


class _PushApi:
    calls = 0

    def kernels_push(self, folder, timeout=None, acc=None):  # type: ignore[no-untyped-def]
        self.calls += 1
        metadata = json.loads((__import__("pathlib").Path(folder) / "kernel-metadata.json").read_text())
        return SimpleNamespace(ref=metadata["id"], versionNumber=1, kernelId=7, error="")


def test_rendered_source_embeds_full_task_uuid_and_passes_real_adapter_pre_provider_contract() -> None:
    metadata = _metadata()
    launcher = CentralEmbeddingWorkerLauncher(
        adapter=Adapter(), access_factory=lambda *_args: None,  # type: ignore[arg-type]
        config=EmbeddingWorkerLaunchConfig(
            "owner", "owner/runtime/12", "image@sha256:" + "d" * 64,
            "wheel", "e" * 64, "https://control.example/events",
        ),
    )
    source = launcher._render_source("owner/status", metadata.task_run_id)
    assert source.count(str(metadata.task_run_id).encode()) == 1
    assert b'm["task_run_id"] != EXPECTED_TASK_RUN_ID' in source
    api = _PushApi()
    adapter = KaggleProviderAdapter(
        api, identity=KaggleProviderIdentity(username="owner"), journal=_Journal(),
        sleep=lambda _seconds: None,
    )
    arguments = {
        "task_run_id": str(metadata.task_run_id),
        "source_sha256": hashlib.sha256(source).hexdigest(),
        "dataset_sources": ("owner/runtime/12", "owner/status/1"),
        "control_class": "orchestrator_protected", "disposable": True,
        "docker_image": "image@sha256:" + "d" * 64,
        "docker_image_pinning_type": "original",
    }
    intent = ProviderEffectIntent.create(
        operation_id=uuid5(NAMESPACE_URL, "operation"), effect_id=uuid5(NAMESPACE_URL, "effect"),
        idempotency_key="embedding-worker-contract", task_id=metadata.task_run_id,
        action=MutationAction.PUSH_NOTEBOOK, provider_ref="owner/worker-notebook",
        arguments=arguments, requested_at=datetime(2026, 8, 12, tzinfo=UTC),
    )
    with pytest.raises(KaggleContractError, match="embed the exact task_run_id"):
        adapter.push_private_worker_notebook_pending_attestation(
            intent=intent, task_run_id=metadata.task_run_id, source=b"print('missing identity')\n",
            title="worker-notebook", code_file="worker.py", kernel_type="script", language="python",
            control_class=ControlClass.ORCHESTRATOR_PROTECTED, disposable=True,
            dataset_sources=("owner/runtime/12", "owner/status/1"),
            docker_image="image@sha256:" + "d" * 64, docker_image_pinning_type="original",
        )
    assert api.calls == 0
    result = adapter.push_private_worker_notebook_pending_attestation(
        intent=intent, task_run_id=metadata.task_run_id, source=source,
        title="worker-notebook", code_file="worker.py", kernel_type="script", language="python",
        control_class=ControlClass.ORCHESTRATOR_PROTECTED, disposable=True,
        dataset_sources=("owner/runtime/12", "owner/status/1"),
        docker_image="image@sha256:" + "d" * 64, docker_image_pinning_type="original",
    )
    assert api.calls == 1 and result.run.task_run_id == metadata.task_run_id


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
    assert launcher.cleanup(receipt.task_run_id) == (None, None)
    assert revoked == [(receipt.credential_id, receipt.task_run_id)]


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


def test_journal_records_every_effect_boundary(tmp_path) -> None:  # type: ignore[no-untyped-def]
    adapter = Adapter()
    now = datetime(2026, 8, 12, tzinfo=UTC)
    class Access:
        def __call__(self, *_args):  # type: ignore[no-untyped-def]
            return EmbeddingWorkerDirectAccess(
                database_url="postgresql://w:s@d/h", tls_ca_pem="ca",
                expires_at=now + timedelta(minutes=5), epoch=7, tunnel_endpoint="d:1",
                credential_id=UUID("33333333-3333-4333-8333-333333333333"))
        def revoke(self, *_args, **_kwargs): return None  # type: ignore[no-untyped-def]
    launcher = CentralEmbeddingWorkerLauncher(
        adapter, Access(), EmbeddingWorkerLaunchConfig(
            "owner", "owner/runtime/12", "image", "wheel", "e"*64, "https://c/e"),
        clock=lambda: now, journal_path=tmp_path / "journal.json")
    receipt = launcher.launch(_metadata())
    assert launcher._states[receipt.task_run_id]["state"] == "LAUNCHED"
    launcher.cleanup(receipt.task_run_id)
    assert launcher._states[receipt.task_run_id]["state"] == "COMPLETE"


def test_restart_reuses_exact_capability_after_status_response_loss(tmp_path) -> None:  # type: ignore[no-untyped-def]
    adapter = Adapter()
    original = adapter.create_private_dataset
    calls = 0
    def ambiguous(**kwargs):  # type: ignore[no-untyped-def]
        nonlocal calls
        calls += 1
        result = original(**kwargs)
        if calls == 1:
            raise RuntimeError("lost response")
        return result
    adapter.create_private_dataset = ambiguous  # type: ignore[method-assign]
    now = datetime(2026, 8, 12, tzinfo=UTC)
    class Access:
        calls = 0
        def __call__(self, *_args):  # type: ignore[no-untyped-def]
            self.calls += 1
            return EmbeddingWorkerDirectAccess(
                database_url="postgresql://w:s@d/h", tls_ca_pem="ca", expires_at=now+timedelta(minutes=5),
                epoch=7, tunnel_endpoint="d:1", credential_id=UUID("33333333-3333-4333-8333-333333333333"))
    access = Access()
    journal = tmp_path / "journal.json"
    config = EmbeddingWorkerLaunchConfig("owner", "owner/runtime/12", "image", "wheel", "e"*64, "https://c/e")
    with pytest.raises(RuntimeError, match="lost response"):
        CentralEmbeddingWorkerLauncher(
            adapter, access, config, clock=lambda: now, journal_path=journal
        ).launch(_metadata())
    restarted = CentralEmbeddingWorkerLauncher(adapter, access, config, clock=lambda: now, journal_path=journal)
    restarted.launch(_metadata())
    assert access.calls == 1
    assert adapter.datasets[0]["files"] == adapter.datasets[1]["files"]


def test_restart_after_push_failure_revokes_and_deletes_status_without_second_push(tmp_path) -> None:  # type: ignore[no-untyped-def]
    adapter = Adapter()
    original = adapter.push_private_worker_notebook_pending_attestation
    calls = 0
    def ambiguous(**kwargs):  # type: ignore[no-untyped-def]
        nonlocal calls
        calls += 1
        result = original(**kwargs)
        if calls == 1:
            raise RuntimeError("lost push response")
        return result
    adapter.push_private_worker_notebook_pending_attestation = ambiguous  # type: ignore[method-assign]
    now = datetime(2026, 8, 12, tzinfo=UTC)
    class Access:
        calls = 0
        revoked = 0
        def __call__(self, *_args):  # type: ignore[no-untyped-def]
            self.calls += 1
            return EmbeddingWorkerDirectAccess(
                database_url="postgresql://w:s@d/h", tls_ca_pem="ca", expires_at=now+timedelta(minutes=5),
                epoch=7, tunnel_endpoint="d:1", credential_id=UUID("33333333-3333-4333-8333-333333333333"))
        def revoke(self, *_args, **_kwargs):  # type: ignore[no-untyped-def]
            self.revoked += 1
    access = Access()
    journal = tmp_path / "journal.json"
    config = EmbeddingWorkerLaunchConfig("owner", "owner/runtime/12", "image", "wheel", "e"*64, "https://c/e")
    with pytest.raises(RuntimeError, match="lost push"):
        CentralEmbeddingWorkerLauncher(
            adapter, access, config, clock=lambda: now, journal_path=journal
        ).launch(_metadata())
    restarted = CentralEmbeddingWorkerLauncher(
        adapter, access, config, clock=lambda: now, journal_path=journal
    )
    with pytest.raises(ValueError, match="requires idempotent cleanup"):
        restarted.launch(_metadata())
    assert access.calls == 1 and calls == 1
    assert restarted.cleanup(_metadata().task_run_id) == ("owner/status", None)
    assert access.revoked == 1
    assert restarted.cleanup(_metadata().task_run_id) == (None, None)
    assert access.revoked == 1
    journal_text = journal.read_text()
    assert "postgresql://" not in journal_text and "task_token" not in journal_text


def test_partial_cleanup_replays_lost_status_delete_after_restart(tmp_path) -> None:  # type: ignore[no-untyped-def]
    adapter = Adapter()
    adapter.push_private_worker_notebook_pending_attestation = (  # type: ignore[method-assign]
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("push failed"))
    )
    deleted = 0
    delete = adapter.delete_task_created_resource

    def lost_delete(**kwargs):  # type: ignore[no-untyped-def]
        nonlocal deleted
        deleted += 1
        result = delete(**kwargs)
        if deleted == 1:
            raise RuntimeError("delete response lost")
        return result

    adapter.delete_task_created_resource = lost_delete  # type: ignore[method-assign]
    now = datetime(2026, 8, 12, tzinfo=UTC)

    class Access:
        revoked = 0
        def __call__(self, *_args):  # type: ignore[no-untyped-def]
            return EmbeddingWorkerDirectAccess(
                database_url="postgresql://w:s@d/h", tls_ca_pem="ca",
                expires_at=now + timedelta(minutes=5), epoch=7, tunnel_endpoint="d:1",
                credential_id=UUID("33333333-3333-4333-8333-333333333333"),
            )
        def revoke(self, *_args, **_kwargs):  # type: ignore[no-untyped-def]
            self.revoked += 1

    access = Access()
    journal = tmp_path / "journal.json"
    config = EmbeddingWorkerLaunchConfig(
        "owner", "owner/runtime/12", "image", "wheel", "e" * 64, "https://c/e"
    )
    first = CentralEmbeddingWorkerLauncher(adapter, access, config, clock=lambda: now, journal_path=journal)
    with pytest.raises(RuntimeError, match="push failed"):
        first.launch(_metadata())
    with pytest.raises(RuntimeError, match="delete response lost"):
        first.cleanup(_metadata().task_run_id)
    restarted = CentralEmbeddingWorkerLauncher(adapter, access, config, clock=lambda: now, journal_path=journal)
    assert restarted.cleanup(_metadata().task_run_id) == ("owner/status", None)
    assert deleted == 2 and access.revoked == 2
    assert restarted.cleanup(_metadata().task_run_id) == (None, None)


def test_runtime_attestation_is_task_token_source_image_commit_and_epoch_bound(tmp_path) -> None:  # type: ignore[no-untyped-def]
    adapter = Adapter()
    now = datetime(2026, 8, 12, tzinfo=UTC)

    def access(_metadata, _token):  # type: ignore[no-untyped-def]
        return EmbeddingWorkerDirectAccess(
            database_url="postgresql://w:s@d/h", tls_ca_pem="ca",
            expires_at=now + timedelta(minutes=5), epoch=7, tunnel_endpoint="d:1",
            credential_id=UUID("33333333-3333-4333-8333-333333333333"),
        )

    commit = "f" * 40
    launcher = CentralEmbeddingWorkerLauncher(
        adapter, access, EmbeddingWorkerLaunchConfig(
            "owner", "owner/runtime/12", "runtime@sha256:" + "d" * 64,
            "wheel", "e" * 64, "https://control.example/internal/runtime/events",
            runtime_image_source_commit=commit,
        ), clock=lambda: now, journal_path=tmp_path / "journal.json",
    )
    receipt = launcher.launch(_metadata())
    capability = json.loads((tmp_path / f"{receipt.task_run_id}.capability").read_text())
    launcher.attest_runtime_source(
        task_run_id=receipt.task_run_id,
        task_token=capability["callback"]["task_token"],
        source_sha256=receipt.source_sha256,
        image_identity="runtime@sha256:" + "d" * 64,
        image_source_commit=commit,
        epoch=7,
    )
    assert launcher._states[receipt.task_run_id]["runtime_attested"] is True
    with pytest.raises(ValueError, match="binding differs"):
        launcher.attest_runtime_source(
            task_run_id=receipt.task_run_id, task_token="wrong", source_sha256=receipt.source_sha256,
            image_identity="runtime@sha256:" + "d" * 64, image_source_commit=commit, epoch=7,
        )
