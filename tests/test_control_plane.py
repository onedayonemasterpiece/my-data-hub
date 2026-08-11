from __future__ import annotations

import os

import pytest
from fastapi.testclient import TestClient

from my_data_hub.control_plane.app import (
    DATABASE_ENVIRONMENT_NAMES,
    ControlPlaneConfigurationError,
    ControlPlaneSettings,
    create_app,
)


def test_control_plane_is_ready_while_master_is_absent(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:  # type: ignore[no-untyped-def]
    for name in set(DATABASE_ENVIRONMENT_NAMES) | {
        key for key in os.environ if key.startswith("PG") or key.endswith("_DATABASE_URL")
    }:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("MY_DATA_HUB_CONTROL_LEDGER_PATH", str(tmp_path / "control.sqlite3"))
    settings = ControlPlaneSettings.from_env()
    response = TestClient(create_app(settings)).get("/health/ready")
    assert response.status_code == 200
    assert response.json() == {
        "ok": True,
        "control_plane_ready": True,
        "data_plane_ready": False,
        "master_state": "ABSENT",
        "master_instance_id": None,
        "master_epoch": None,
        "canonical_database_runtime": "kaggle_notebook",
        "lifecycle_implementation": "durable_control_ledger_v1",
        "production_publication": False,
        "remote_mcp_writes": False,
    }


@pytest.mark.parametrize(
    "name",
    [
        *DATABASE_ENVIRONMENT_NAMES,
        "MY_DATA_HUB_FUTURE_DATABASE_URL",
        "PGHOSTADDR",
        "PGSSLKEY",
        "PGSSLCERT",
        "PGSSLROOTCERT",
        "PGOPTIONS",
    ],
)
def test_control_plane_rejects_local_master_credentials(
    monkeypatch: pytest.MonkeyPatch, name: str
) -> None:
    for candidate in set(DATABASE_ENVIRONMENT_NAMES) | {
        key for key in os.environ if key.startswith("PG") or key.endswith("_DATABASE_URL")
    }:
        monkeypatch.delenv(candidate, raising=False)
    monkeypatch.setenv(name, "postgresql://forbidden/local")
    with pytest.raises(ControlPlaneConfigurationError, match="must not receive"):
        ControlPlaneSettings.from_env()


def test_control_plane_data_operations_fail_closed() -> None:
    response = TestClient(create_app(ControlPlaneSettings())).post(
        "/intake/v1/batches", json={"unsafe": True}
    )
    assert response.status_code == 503
    assert response.json()["detail"]["code"] == "master_absent"
    read_response = TestClient(create_app(ControlPlaneSettings())).get("/mcp")
    assert read_response.status_code == 503


def test_control_plane_ensure_is_durable_idempotent_and_survives_restart(tmp_path) -> None:  # type: ignore[no-untyped-def]
    settings = ControlPlaneSettings(ledger_path=tmp_path / "control.sqlite3")
    first = TestClient(create_app(settings)).post(
        "/control/v1/master/ensure",
        json={"idempotency_key": "same-cold-start", "intent": "test"},
    )
    assert first.status_code == 200
    assert first.json()["duplicate"] is False
    restarted = TestClient(create_app(settings)).post(
        "/control/v1/master/ensure",
        json={"idempotency_key": "same-cold-start", "intent": "test"},
    )
    assert restarted.status_code == 200
    assert restarted.json()["duplicate"] is True
    assert restarted.json()["operation_id"] == first.json()["operation_id"]
    ready = restarted = TestClient(create_app(settings)).get("/health/ready")
    assert ready.json()["master_state"] == "REQUESTED"


def test_runtime_callback_is_fail_closed_without_provider_coordinator(tmp_path) -> None:  # type: ignore[no-untyped-def]
    response = TestClient(
        create_app(ControlPlaneSettings(ledger_path=tmp_path / "control.sqlite3"))
    ).post("/internal/runtime/events", content=b"{}", headers={"Authorization": "Bearer opaque"})
    assert response.status_code == 503
