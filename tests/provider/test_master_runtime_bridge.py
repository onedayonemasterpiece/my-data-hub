from __future__ import annotations

import hashlib
from collections import Counter
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

import pytest

from my_data_hub.control_plane.ledger import ControlLedger
from my_data_hub.orchestrator.master import MasterCoordinator, MasterIntent, MasterState
from my_data_hub.providers.kaggle import (
    KaggleKernelRunIdentity,
    KaggleMasterLaunchAssets,
    KaggleMasterRuntimeProvider,
    MasterLaunchContractError,
)
from my_data_hub.runtime_sdk import KAGGLE_HARD_CAP_SECONDS, KAGGLE_PROVIDER_TIMEOUT_SECONDS


class FakeKaggleAdapter:
    """Duck-typed fake for the one KaggleProviderAdapter boundary."""

    def __init__(self) -> None:
        self.calls: Counter[str] = Counter()
        self.run: KaggleKernelRunIdentity | None = None
        self.last_notebook_kwargs: dict[str, object] | None = None

    def create_private_dataset(self, **kwargs):  # type: ignore[no-untyped-def]
        self.calls["dataset"] += 1
        return SimpleNamespace(
            identity=SimpleNamespace(
                provider_ref=kwargs["intent"].provider_ref,
                version=1,
                package_sha256="a" * 64,
            )
        )

    def push_private_notebook(self, **kwargs):  # type: ignore[no-untyped-def]
        self.calls["notebook_run"] += 1
        self.last_notebook_kwargs = kwargs
        source = kwargs["source"]
        body = __import__("json").loads(source)
        for cell in body["cells"]:
            if cell.get("cell_type") == "code":
                cell["outputs"] = []
            if isinstance(cell.get("source"), list):
                cell["source"] = "".join(cell["source"])
        source_sha = hashlib.sha256(__import__("json").dumps(body).encode()).hexdigest()
        self.run = KaggleKernelRunIdentity(
            task_run_id=kwargs["task_run_id"],
            provider_ref=kwargs["intent"].provider_ref,
            source_version=1,
            source_sha256=source_sha,
            provider_kernel_id=42,
            provider_run_ref=f"{kwargs['intent'].provider_ref}/1",
            started_at=datetime.now(UTC),
        )
        return SimpleNamespace(run=self.run)

    def read_run_status(self, run):  # type: ignore[no-untyped-def]
        assert self.run == run
        self.calls["run_reconcile"] += 1
        return SimpleNamespace(state="running")

    def reconcile_private_notebook_run(self, **kwargs):  # type: ignore[no-untyped-def]
        if self.run is None:
            return None
        if self.run.task_run_id != kwargs["task_run_id"]:
            return None
        if self.run.source_sha256 != kwargs["expected_source_sha256"]:
            return None
        return self.run


def test_concrete_bridge_launches_dataset_notebook_and_run_once(tmp_path: Path) -> None:
    launch = KaggleMasterLaunchAssets(
        source_identity="owner/postgres-master",
        source_version="git:exact",
        checkpoint_ref="owner/checkpoints",
        dataset_ref="owner/master-launch",
        notebook_ref="owner/postgres-master",
        dataset_files={"config.json": b'{"run":"{{MY_DATA_HUB_RUN_ID}}"}', "checkpoint-verifier.ipynb": b"{}"},
        notebook_source=b'{"cells":[],"metadata":{},"nbformat":4,"nbformat_minor":5}',
        callback_url="https://control.example/internal/runtime/events",
        runtime_token_secret_name="master-runtime-root",
        checkpoint_verifier_ref="owner/checkpoint-verifier",
        checkpoint_verifier_source_file="checkpoint-verifier.ipynb",
        checkpoint_probe_relations=("hub.canonical_state",),
    )
    adapter = FakeKaggleAdapter()
    provider = KaggleMasterRuntimeProvider(adapter, launch)  # type: ignore[arg-type]
    ledger = ControlLedger(tmp_path / "control.sqlite3")
    coordinator = MasterCoordinator(ledger, provider)
    intent = MasterIntent(
        idempotency_key="bridge-master",
        source_identity=launch.source_identity,
        source_version=launch.source_version,
        checkpoint_ref=launch.checkpoint_ref,
        dataset_ref=launch.dataset_ref,
        notebook_ref=launch.notebook_ref,
    )
    first = coordinator.ensure_master(intent, runtime_secret="runtime-secret-long-enough")
    second = coordinator.ensure_master(intent, runtime_secret="runtime-secret-long-enough")
    assert first.state == second.state == MasterState.REGISTERING
    assert first.run_id == second.run_id == str(UUID(first.run_id))
    assert adapter.calls == {"dataset": 1, "notebook_run": 1, "run_reconcile": 1}
    assert launch.notebook_timeout_seconds == KAGGLE_PROVIDER_TIMEOUT_SECONDS
    assert KAGGLE_PROVIDER_TIMEOUT_SECONDS < KAGGLE_HARD_CAP_SECONDS
    assert adapter.last_notebook_kwargs is not None
    assert adapter.last_notebook_kwargs["timeout_seconds"] == KAGGLE_PROVIDER_TIMEOUT_SECONDS
    with pytest.raises(MasterLaunchContractError, match="reserve"):
        replace(launch, notebook_timeout_seconds=KAGGLE_HARD_CAP_SECONDS)
