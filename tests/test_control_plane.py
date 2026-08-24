from __future__ import annotations

import os
from types import SimpleNamespace
from uuid import UUID

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
    payload = response.json()
    assert UUID(payload.pop("control_boot_id"))
    assert payload == {
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
            "master_runtime_ready": False,
            "master_provider_status": "provider_unavailable",
            "provider_gateway_ready": False,
            "unified_bootstrap_mode": False,
        }


def test_region_talk_enabled_but_unassembled_fails_readiness_with_exact_code(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("MY_DATA_HUB_REGION_TALK_PIPELINE_ENABLED", "true")
    monkeypatch.setenv("MY_DATA_HUB_REGION_TALK_SCHEDULE_ENABLED", "false")
    monkeypatch.setenv("MY_DATA_HUB_KAGGLE_OWNER", "owner")
    monkeypatch.setenv("MY_DATA_HUB_CALLBACK_URL", "https://control.example")
    monkeypatch.setenv(
        "MY_DATA_HUB_REGION_TALK_RUNTIME_IMAGE_IDENTITY",
        "runtime@sha256:" + "d" * 64,
    )
    monkeypatch.setenv(
        "MY_DATA_HUB_REGION_TALK_RUNTIME_SOURCE_COMMIT", "e" * 40
    )
    monkeypatch.setenv(
        "MY_DATA_HUB_REGION_TALK_WHEEL_RELATIVE_PATH", "dist/my_data_hub.whl"
    )
    monkeypatch.setenv("MY_DATA_HUB_REGION_TALK_WHEEL_SHA256", "f" * 64)
    monkeypatch.setenv(
        "MY_DATA_HUB_EMBEDDING_DEPENDENCY_MANIFEST_SHA256", "8" * 64
    )
    monkeypatch.setenv(
        "MY_DATA_HUB_REGION_TALK_YDB_ENDPOINT",
        "grpcs://ydb.serverless.yandexcloud.net:2135",
    )
    monkeypatch.setenv(
        "MY_DATA_HUB_REGION_TALK_YDB_DATABASE", "/ru-central1/example/region-talk"
    )
    monkeypatch.setenv(
        "MY_DATA_HUB_REGION_TALK_YDB_VIEWER_SECRET_LABEL",
        "REGION_TALK_YDB_VIEWER_SA_JSON",
    )
    monkeypatch.setenv(
        "MY_DATA_HUB_MASTER_YDB_DEPENDENCY_MANIFEST_SHA256", "9" * 64
    )
    monkeypatch.setenv(
        "MY_DATA_HUB_REGION_TALK_CAPABILITY_DIR", str(tmp_path / "private")
    )
    response = TestClient(
        create_app(
            ControlPlaneSettings(
                ledger_path=tmp_path / "control.sqlite3",
                provider_gateway_enabled=True,
                operator_credentials_enabled=True,
            ),
            provider_gateway=SimpleNamespace(uploads=None),
            provider_gateway_token=b"g" * 48,
        )
    ).get("/health/ready")
    assert response.status_code == 503
    detail = response.json()["detail"]
    assert detail["code"] == "REGION_TALK_EXACT_RUNTIME_CLAIM_PENDING"
    assert detail["region_talk_pipeline_enabled"] is True
    assert detail["region_talk_pipeline_ready"] is False
    assert detail["region_talk_schedule_enabled"] is False


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


def test_control_plane_ensure_fails_closed_when_provider_is_unavailable(tmp_path) -> None:  # type: ignore[no-untyped-def]
    settings = ControlPlaneSettings(ledger_path=tmp_path / "control.sqlite3")
    first = TestClient(create_app(settings)).post(
        "/control/v1/master/ensure",
        json={"idempotency_key": "same-cold-start", "intent": "test"},
    )
    assert first.status_code == 503
    assert first.json()["detail"]["code"] == "provider_unavailable"
    restarted = TestClient(create_app(settings)).post(
        "/control/v1/master/ensure",
        json={"idempotency_key": "same-cold-start", "intent": "test"},
    )
    assert restarted.status_code == 503
    ready = TestClient(create_app(settings)).get("/health/ready")
    assert ready.json()["master_state"] == "ABSENT"


def test_runtime_callback_is_fail_closed_without_provider_coordinator(tmp_path) -> None:  # type: ignore[no-untyped-def]
    response = TestClient(
        create_app(ControlPlaneSettings(ledger_path=tmp_path / "control.sqlite3"))
    ).post("/internal/runtime/events", content=b"{}", headers={"Authorization": "Bearer opaque"})
    assert response.status_code == 503


def test_runtime_activation_requires_exact_nonrevoked_per_run_token(tmp_path) -> None:  # type: ignore[no-untyped-def]
    app = create_app(ControlPlaneSettings(ledger_path=tmp_path / "control.sqlite3"))
    ledger = app.state.control_ledger
    ledger.ensure_operation(
        operation_id="op-activation",
        idempotency_key="activation-key",
        operation_kind="ensure_master",
        intent={"intent": "test"},
        initial_state="REGISTERING",
        identity={
            "run_id": "run-activation",
            "attempt_id": "attempt-activation",
            "service_instance_id": "service-activation",
            "master_instance_id": "master-activation",
            "epoch": 1,
        },
    )
    ledger.record_attempt(
        attempt_id="attempt-activation",
        run_id="run-activation",
        operation_id="op-activation",
        source_identity="owner/notebook",
        source_version="1",
        service_instance_id="service-activation",
        master_instance_id="master-activation",
        epoch=1,
        state="REGISTERING",
    )
    ledger.store_runtime_token_hash("run-activation", "attempt-activation", "opaque-run-secret")
    client = TestClient(app)
    path = "/internal/runtime/activation/run-activation/attempt-activation"
    assert client.get(path).status_code == 401
    assert client.get(path, headers={"Authorization": "Bearer wrong"}).status_code == 401
    pending = client.get(path, headers={"Authorization": "Bearer opaque-run-secret"})
    assert pending.status_code == 200
    assert pending.json() == {
        "active": False,
        "state": "REGISTERING",
        "master_instance_id": "master-activation",
        "epoch": 1,
        "credential_roles": ["reader"],
    }
    ledger.revoke_runtime_token("run-activation", "attempt-activation")
    assert client.get(path, headers={"Authorization": "Bearer opaque-run-secret"}).status_code == 401
