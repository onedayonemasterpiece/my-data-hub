from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

import my_data_hub.api.app as api_module
from my_data_hub.api.connector_runtime import build_connector_api_runtime
from my_data_hub.config import ConfigurationError, Settings
from my_data_hub.control_plane.ledger import ControlLedger

ROOT = Path(__file__).resolve().parents[1]


class FakeRepository:
    def __init__(self, database_url: str, artifact_store) -> None:  # type: ignore[no-untyped-def]
        self.database_url = database_url
        self.artifact_store = artifact_store

    def store(self, envelope) -> dict[str, Any]:  # type: ignore[no-untyped-def]
        return {
            "result_id": str(envelope.result_id),
            "result_sha256": "f" * 64,
            "status": "received",
            "artifact_locator": "file:///fixture",
            "duplicate": False,
        }


def settings(tmp_path: Path, *, token: str | None = "worker-secret", max_bytes: int = 8192) -> Settings:
    return Settings(
        database_url="postgresql://unused",
        environment="test",
        instance_id="pytest",
        log_level="INFO",
        artifact_root=tmp_path / "artifacts",
        api_host="127.0.0.1",
        api_port=8080,
        worker_result_token=token,
        worker_result_max_bytes=max_bytes,
        scheduler_enabled=False,
        production_publish_enabled=False,
        orchestrator_interval_seconds=60,
        orchestrator_batch_size=25,
        orchestrator_lease_seconds=1800,
        mcp_remote_enabled=False,
        mcp_write_enabled=False,
        mcp_host="127.0.0.1",
        mcp_port=8765,
        mcp_allowed_origins=("http://localhost",),
        mcp_allowed_hosts=("localhost",),
        mcp_auth_mode="stdio-environment",
        mcp_development_token=None,
        mcp_scopes=frozenset({"hub:read"}),
    )


def valid_result() -> dict[str, Any]:
    return json.loads(
        (ROOT / "examples/contracts/notebook-result.v1.example.json").read_text(
            encoding="utf-8"
        )
    )


def test_liveness_and_security_headers(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(api_module, "WorkerResultRepository", FakeRepository)
    client = TestClient(api_module.create_app(settings(tmp_path)))
    response = client.get("/health/live")
    assert response.status_code == 200
    assert response.json()["ok"] is True
    assert response.headers["cache-control"] == "no-store"
    assert response.headers["x-content-type-options"] == "nosniff"


def test_worker_result_requires_token(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(api_module, "WorkerResultRepository", FakeRepository)
    client = TestClient(api_module.create_app(settings(tmp_path)))
    assert client.post("/v1/worker-results", json=valid_result()).status_code == 401
    accepted = client.post(
        "/v1/worker-results",
        json=valid_result(),
        headers={"Authorization": "Bearer worker-secret"},
    )
    assert accepted.status_code == 202
    assert accepted.json()["status"] == "received"


def test_unconfigured_intake_is_fail_closed(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(api_module, "WorkerResultRepository", FakeRepository)
    client = TestClient(api_module.create_app(settings(tmp_path, token=None)))
    response = client.post("/v1/worker-results", json=valid_result())
    assert response.status_code == 503


def test_production_api_does_not_require_static_connector_database_identity(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(api_module, "WorkerResultRepository", FakeRepository)
    production = replace(settings(tmp_path), environment="production")
    with pytest.raises(ConfigurationError, match="APPLICATION_DATABASE_URL"):
        api_module.create_app(production)
    production = replace(
        production,
        application_database_url="postgresql://application@db/hub",
        connector_intake_database_url="",
        worker_result_token=None,
    )
    with pytest.raises(ConfigurationError, match="worker result token"):
        api_module.create_app(production)
    configured = replace(production, worker_result_token="worker-secret")
    app = api_module.create_app(configured)
    assert app.title == "my-data-hub control API"


def test_connector_intake_without_active_master_runtime_is_pre_mutation_blocker(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(api_module, "WorkerResultRepository", FakeRepository)
    configured = replace(
        settings(tmp_path),
        connector_credentials=(("synthetic.daily-statistics", "connector-secret"),),
    )
    client = TestClient(api_module.create_app(configured))
    envelope = (ROOT / "examples/contracts/data-connector-envelope.v1.example.json").read_bytes()
    response = client.post(
        "/intake/v1/batches",
        content=envelope,
        headers={
            "Authorization": "Bearer connector-secret",
            "Content-Type": "application/json",
        },
    )
    assert response.status_code == 503
    assert response.json()["detail"] == {
        "code": "CONNECTOR_ACTIVE_MASTER_RUNTIME_UNAVAILABLE",
        "retryable": True,
        "mutation_started": False,
    }


def test_connector_only_production_runtime_has_no_static_database_or_worker_route(
    tmp_path: Path,
) -> None:
    configured = replace(
        settings(tmp_path),
        database_url="",
        environment="production",
        worker_result_token=None,
        connector_credentials=(("synthetic.daily-statistics", "connector-secret"),),
    )
    ledger = ControlLedger(tmp_path / "control.sqlite3")
    runtime = build_connector_api_runtime(settings=configured, ledger=ledger)
    with TestClient(runtime.app) as client:
        assert client.get("/health/ready").json() == {
            "ok": True,
            "component": "my-data-hub-connector-intake",
        }
        assert client.post("/v1/worker-results", json={}).status_code == 404
        response = client.post(
            "/intake/v1/batches",
            content=(
                ROOT / "examples/contracts/data-connector-envelope.v1.example.json"
            ).read_bytes(),
            headers={
                "Authorization": "Bearer connector-secret",
                "Content-Type": "application/json",
            },
        )
    assert response.status_code == 503
    assert response.json()["detail"] == {
        "code": "CONNECTOR_VERIFIED_CHECKPOINT_COORDINATOR_UNAVAILABLE",
        "master_state": None,
        "operation_id": None,
        "retryable": True,
        "mutation_started": False,
    }


def test_connector_only_runtime_rejects_injected_static_database_url(tmp_path: Path) -> None:
    configured = replace(
        settings(tmp_path),
        environment="production",
        connector_credentials=(("synthetic.daily-statistics", "connector-secret"),),
    )
    with pytest.raises(ConfigurationError, match="static database URL"):
        build_connector_api_runtime(
            settings=configured,
            ledger=ControlLedger(tmp_path / "control.sqlite3"),
        )


def test_chunked_or_declared_oversize_body_is_rejected(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(api_module, "WorkerResultRepository", FakeRepository)
    client = TestClient(api_module.create_app(settings(tmp_path, max_bytes=1024)))
    response = client.post(
        "/v1/worker-results",
        content=b"{" + b"x" * 2048 + b"}",
        headers={
            "Authorization": "Bearer worker-secret",
            "Content-Type": "application/json",
        },
    )
    assert response.status_code == 413


def test_invalid_result_contract_is_rejected(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(api_module, "WorkerResultRepository", FakeRepository)
    client = TestClient(api_module.create_app(settings(tmp_path)))
    response = client.post(
        "/v1/worker-results",
        json={"schema_version": "wrong"},
        headers={"Authorization": "Bearer worker-secret"},
    )
    assert response.status_code == 422
