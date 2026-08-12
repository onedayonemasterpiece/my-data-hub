from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from my_data_hub.embeddings.dependency_smoke import CentralDependencySmoke
from my_data_hub.hashing import canonical_json_bytes
from my_data_hub.providers.kaggle.contracts import (
    KaggleKernelRunIdentity,
    KaggleKernelStatus,
    KernelState,
    TaskResourceClaim,
)
from my_data_hub.providers.models import ControlClass, ProviderFingerprint, ProviderKind

IMAGE = "gcr.io/kaggle-images/python@sha256:" + "a" * 64
COMMIT = "f" * 40


class Adapter:
    def __init__(self, observation: bytes) -> None:
        self.observation = observation
        self.pushes = 0
        self.deletes = 0
        self.kwargs = None

    def push_private_dependency_smoke_notebook(self, **kwargs):  # type: ignore[no-untyped-def]
        self.pushes += 1
        self.kwargs = kwargs
        now = datetime(2026, 8, 12, tzinfo=UTC)
        run = KaggleKernelRunIdentity(
            task_run_id=kwargs["task_run_id"],
            provider_ref="owner/mdh-embedding-dependency-smoke",
            source_version=7,
            source_sha256=__import__("hashlib").sha256(kwargs["source"]).hexdigest(),
            provider_kernel_id=9,
            provider_run_ref="owner/mdh-embedding-dependency-smoke/7",
            started_at=now,
        )
        claim = TaskResourceClaim.create(
            task_id=kwargs["task_run_id"],
            effect_id=kwargs["intent"].effect_id,
            provider_ref="owner/mdh-embedding-dependency-smoke",
            kind=ProviderKind.NOTEBOOK,
            control_class=ControlClass.ORCHESTRATOR_PROTECTED,
            disposable=True,
            fingerprint=ProviderFingerprint(value="b" * 64),
            provider_version=7,
            registered_at=now,
        )
        return SimpleNamespace(run=run, claim=claim)

    def read_run_status(self, run):  # type: ignore[no-untyped-def]
        return KaggleKernelStatus(
            run=run,
            state=KernelState.COMPLETE,
            provider_status="complete",
            observed_at=datetime(2026, 8, 12, tzinfo=UTC),
        )

    def download_attested_master_output_file(self, run, *, destination, file_name, **kwargs):  # type: ignore[no-untyped-def]
        (destination / file_name).write_bytes(self.observation)

    def delete_task_created_resource(self, **kwargs):  # type: ignore[no-untyped-def]
        self.deletes += 1


def observation(**updates: object) -> bytes:
    value = {
        "schema_version": "my-data-hub-embedding-dependency-smoke-observation.v1",
        "status": "imports_passed",
        "expected_image_identity": IMAGE,
        "image_source_commit": COMMIT,
        "python_version": "3.12.3",
        "dependency_manifest_sha256": "c" * 64,
        "project_wheel_sha256": "d" * 64,
        "wheel_sha256s": {"psycopg.whl": "e" * 64},
        "imports": ["psycopg"],
        "psycopg_implementation": "binary",
        "distributions": {"psycopg": "3.2.9"},
    }
    value.update(updates)
    return canonical_json_bytes(value)


def smoke(tmp_path: Path, adapter: Adapter) -> CentralDependencySmoke:
    return CentralDependencySmoke(
        adapter=adapter,
        owner="owner",
        runtime_dataset_exact_ref="owner/runtime/12",
        image_identity=IMAGE,
        image_source_commit=COMMIT,
        project_wheel_sha256="d" * 64,
        project_wheel_relative_path="my_data_hub.whl",
        dependency_manifest_sha256="c" * 64,
        state_path=(tmp_path / "private" / "state.json").absolute(),
        receipt_path=(tmp_path / "private" / "receipt.json").absolute(),
        clock=lambda: datetime(2026, 8, 12, tzinfo=UTC),
    )


def test_smoke_launch_is_exact_offline_and_central_receipt_is_restart_safe(tmp_path: Path) -> None:
    adapter = Adapter(observation())
    first = smoke(tmp_path, adapter).run_once()
    assert first is not None and first.status == "pass"
    assert adapter.pushes == adapter.deletes == 1
    assert adapter.kwargs["enable_internet"] is False
    assert adapter.kwargs["docker_image"] == IMAGE
    assert adapter.kwargs["docker_image_pinning_type"] == "original"
    assert adapter.kwargs["dataset_sources"] == ("owner/runtime/12",)
    assert (tmp_path / "private" / "receipt.json").stat().st_mode & 0o777 == 0o600
    assert smoke(tmp_path, adapter).run_once() == first
    assert adapter.pushes == adapter.deletes == 1


def test_smoke_rejects_observation_or_receipt_tamper(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="differs"):
        smoke(tmp_path, Adapter(observation(status="PASS"))).run_once()
    adapter = Adapter(observation())
    instance = smoke(tmp_path / "other", adapter)
    instance.run_once()
    instance.receipt_path.write_bytes(b"{}")
    with pytest.raises(ValueError):
        smoke(tmp_path / "other", adapter).run_once()


def test_cleanup_response_loss_is_replayed_by_new_coordinator(tmp_path: Path) -> None:
    adapter = Adapter(observation())
    original = adapter.delete_task_created_resource
    lost = True

    def delete_then_lose(**kwargs):  # type: ignore[no-untyped-def]
        nonlocal lost
        original(**kwargs)
        if lost:
            lost = False
            raise RuntimeError("delete response lost")

    adapter.delete_task_created_resource = delete_then_lose  # type: ignore[method-assign]
    with pytest.raises(RuntimeError, match="response lost"):
        smoke(tmp_path, adapter).run_once()
    receipt = smoke(tmp_path, adapter).run_once()
    assert receipt is not None
    assert adapter.pushes == 1 and adapter.deletes == 2
