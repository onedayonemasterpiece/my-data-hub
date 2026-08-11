from __future__ import annotations

import json
from pathlib import Path

import pytest

from my_data_hub.master_runtime.contracts import BootSource
from my_data_hub.master_runtime.notebook_entrypoint import NotebookMasterConfig, _activation_url


def _payload() -> dict[str, object]:
    return {
        "master_instance_id": "11111111-1111-4111-8111-111111111111",
        "run_id": "run-1",
        "attempt_id": "attempt-1",
        "service_instance_id": "service-1",
        "epoch": 1,
        "boot_source": "empty_baseline",
        "checkpoint_directory": None,
        "lease_seconds": 120,
        "postgres_bin": "/kaggle/input/postgresql-18/bin",
        "postgres_port": 15432,
        "tunnel_gateway_host": "gateway.example.test",
        "tunnel_gateway_port": 22,
        "tunnel_gateway_user": "mdh-tunnel",
        "tunnel_remote_port": 25432,
        "maximum_runtime_seconds": 3600,
        "source_identity": "owner/postgres-master",
        "source_version": "1",
    }


def test_master_notebook_config_requires_exact_fields_and_source_binding(tmp_path: Path) -> None:
    path = tmp_path / "master.json"
    path.write_text(json.dumps(_payload()))
    config = NotebookMasterConfig.load(path)
    assert config.boot_source is BootSource.EMPTY_BASELINE
    assert config.epoch == 1

    payload = _payload()
    payload["checkpoint_directory"] = "/kaggle/input/checkpoint"
    path.write_text(json.dumps(payload))
    with pytest.raises(ValueError, match="checkpoint source"):
        NotebookMasterConfig.load(path)


def test_activation_url_is_https_and_exact() -> None:
    assert _activation_url(
        "https://control.example/internal/runtime/events", "run-1", "attempt-1"
    ) == "https://control.example/internal/runtime/activation/run-1/attempt-1"
    with pytest.raises(ValueError, match="exact HTTPS"):
        _activation_url("http://control.example/internal/runtime/events", "run", "attempt")
