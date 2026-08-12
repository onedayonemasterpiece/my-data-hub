from __future__ import annotations

import hashlib
import json
from pathlib import Path
from uuid import UUID

import pytest

from my_data_hub.embeddings.central_launcher import CentralEmbeddingWorkerLauncher, EmbeddingWorkerLaunchConfig

TASK = UUID("22222222-2222-4222-8222-222222222222")


def _source(tmp_path: Path) -> str:
    status = tmp_path / "status-provider-renamed-v4" / "files"
    runtime = tmp_path / "runtime-provider-renamed-v12" / "dist"
    status.mkdir(parents=True)
    runtime.mkdir(parents=True)
    wheel = runtime / "project.whl"
    wheel.write_bytes(b"wheel")
    payload = {
        "schema_version": "embedding-worker-status.v1",
        "launch": {"task_run_id": str(TASK), "epoch": 7},
        "direct_access": {"epoch": 7},
        "runtime": {
            "dataset_exact_ref": "owner/runtime/12",
            "wheel_relative_path": "dist/project.whl",
            "wheel_sha256": hashlib.sha256(b"wheel").hexdigest(),
            "input_dataset_versions": ["owner/runtime/12", "owner/status/4"],
        },
    }
    (status / "embedding-worker.json").write_text(json.dumps(payload))
    (status / "execution-pins.json").write_text(
        json.dumps({"input_dataset_versions": payload["runtime"]["input_dataset_versions"]})
    )
    launcher = CentralEmbeddingWorkerLauncher(
        adapter=object(),
        access_factory=lambda *_: None,
        config=EmbeddingWorkerLaunchConfig(
            "owner",
            "owner/runtime/12",
            "image",
            "dist/project.whl",
            hashlib.sha256(b"wheel").hexdigest(),
            "https://callback",
        ),
    )
    source = (
        launcher._render_source("owner/original-status-name", TASK).decode().replace("/kaggle/input", str(tmp_path))
    )
    return source.split("observed_commit=", 1)[0]


def test_worker_mount_discovery_survives_both_provider_mount_renames(tmp_path: Path) -> None:
    scope: dict[str, object] = {}
    exec(compile(_source(tmp_path), "worker-bootstrap", "exec"), scope)
    assert scope["status_mount"] == tmp_path / "status-provider-renamed-v4" / "files"
    assert scope["runtime_mount"] == tmp_path / "runtime-provider-renamed-v12"


def test_worker_mount_discovery_rejects_ambiguous_task_status(tmp_path: Path) -> None:
    source = _source(tmp_path)
    original = tmp_path / "status-provider-renamed-v4" / "files" / "embedding-worker.json"
    duplicate = tmp_path / "another-status-mount" / "embedding-worker.json"
    duplicate.parent.mkdir()
    duplicate.write_bytes(original.read_bytes())
    with pytest.raises(RuntimeError, match="absent or ambiguous"):
        exec(compile(source, "worker-bootstrap", "exec"), {})


@pytest.mark.parametrize("unsafe", ["symlink", "oversize"])
def test_worker_mount_discovery_rejects_unsafe_status_file(tmp_path: Path, unsafe: str) -> None:
    source = _source(tmp_path)
    status = tmp_path / "status-provider-renamed-v4" / "files" / "embedding-worker.json"
    if unsafe == "symlink":
        target = tmp_path / "outside-status.json"
        status.replace(target)
        status.symlink_to(target)
    else:
        payload = json.loads(status.read_bytes())
        payload["padding"] = "x" * 262_144
        status.write_text(json.dumps(payload))
    with pytest.raises(RuntimeError, match="absent or ambiguous"):
        exec(compile(source, "worker-bootstrap", "exec"), {})
