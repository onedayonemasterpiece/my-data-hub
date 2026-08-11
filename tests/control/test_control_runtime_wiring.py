from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from my_data_hub.control_plane.app import ControlPlaneSettings, create_app
from my_data_hub.control_plane.ledger import ControlLedger
from my_data_hub.control_plane.runtime import (
    ControlPlaneMasterRuntime,
    MasterRuntimeSettings,
    build_production_runtime,
)
from my_data_hub.orchestrator.master import FakeKaggleRuntime, MasterCoordinator
from my_data_hub.providers.kaggle import KaggleMasterLaunchAssets, KaggleMasterRuntimeProvider, derive_runtime_secret
from my_data_hub.runtime_sdk import RuntimeEvent, RuntimeEventType
from my_data_hub.workloads.bloggers.master_stage import (
    BloggerImportStageReceipt,
    BloggerMigrationRequest,
)

ROOT = "runtime-root-secret-long-enough-for-tests"


def test_production_runtime_rejects_an_attacker_callback_audience(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    values = {
        "MY_DATA_HUB_KAGGLE_MASTER_SOURCE_IDENTITY": "owner/postgres-master",
        "MY_DATA_HUB_KAGGLE_MASTER_SOURCE_VERSION": "git:exact",
        "MY_DATA_HUB_KAGGLE_MASTER_CHECKPOINT_REF": "owner/checkpoints",
        "MY_DATA_HUB_KAGGLE_MASTER_DATASET_REF": "owner/launch",
        "MY_DATA_HUB_KAGGLE_MASTER_NOTEBOOK_REF": "owner/master",
        "MY_DATA_HUB_KAGGLE_MASTER_DATASET_DIR": "/does/not/matter",
        "MY_DATA_HUB_KAGGLE_MASTER_NOTEBOOK_SOURCE": "/does/not/matter.ipynb",
        "MY_DATA_HUB_CALLBACK_URL": "https://attacker.example/internal/runtime/events",
        "MY_DATA_HUB_KAGGLE_RUNTIME_TOKEN_SECRET_NAME": "runtime-token",
        "MY_DATA_HUB_KAGGLE_CHECKPOINT_VERIFIER_REF": "owner/verifier",
        "MY_DATA_HUB_KAGGLE_CHECKPOINT_VERIFIER_SOURCE_FILE": "verifier.ipynb",
        "MY_DATA_HUB_MASTER_RUNTIME_TOKEN_ROOT": ROOT,
    }
    for name, value in values.items():
        monkeypatch.setenv(name, value)
    with pytest.raises(ValueError, match="owner-approved canonical HTTPS audience"):
        MasterRuntimeSettings.from_env()


def assets() -> KaggleMasterLaunchAssets:
    return KaggleMasterLaunchAssets(
        source_identity="owner/postgres-master",
        source_version="git:0123456789abcdef",
        checkpoint_ref="owner/checkpoints",
        dataset_ref="owner/master-launch",
        notebook_ref="owner/postgres-master",
        dataset_files={"launch.txt": b"exact", "checkpoint-verifier.ipynb": b"{}"},
        notebook_source=b'{"cells":[],"metadata":{},"nbformat":4,"nbformat_minor":5}',
        callback_url="https://mcp-datahub.kenigevents.ru/internal/runtime/events",
        runtime_token_secret_name="MY_DATA_HUB_MASTER_RUNTIME_TOKEN_ROOT",
        checkpoint_verifier_ref="owner/checkpoint-verifier",
        checkpoint_verifier_source_file="checkpoint-verifier.ipynb",
        checkpoint_probe_relations=("hub.canonical_state",),
    )


def runtime(ledger: ControlLedger, provider: FakeKaggleRuntime) -> ControlPlaneMasterRuntime:
    return ControlPlaneMasterRuntime(
        ledger,
        MasterCoordinator(ledger, provider),
        MasterRuntimeSettings(assets(), ROOT),
    )


def test_production_builder_constructs_single_adapter_journal_and_bridge(monkeypatch, tmp_path: Path) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("KAGGLE_API_TOKEN", "modern-token-present")
    ledger = ControlLedger(tmp_path / "builder.sqlite3")
    adapter = object()
    seen = []
    built = build_production_runtime(
        ledger,
        MasterRuntimeSettings(assets(), ROOT),
        adapter_factory=lambda journal: seen.append(journal) or adapter,  # type: ignore[arg-type,return-value]
    )
    assert built.provider_status == "available"
    assert built.master is not None
    assert len(seen) == 1
    assert isinstance(built.master.coordinator.provider, KaggleMasterRuntimeProvider)


def test_control_ensure_runs_one_physical_launch_under_concurrency_and_restart(tmp_path: Path) -> None:
    path = tmp_path / "control.sqlite3"
    provider = FakeKaggleRuntime()
    first_ledger = ControlLedger(path)
    app = create_app(
        ControlPlaneSettings(ledger_path=path),
        ledger=first_ledger,
        master_runtime=runtime(first_ledger, provider),
    )

    def ensure() -> tuple[int, str]:
        response = TestClient(app).post(
            "/control/v1/master/ensure",
            json={"idempotency_key": "concurrent-master", "intent": "test"},
        )
        return response.status_code, response.json()["operation_id"]

    with ThreadPoolExecutor(max_workers=12) as pool:
        responses = list(pool.map(lambda _: ensure(), range(12)))
    assert {status for status, _operation_id in responses} == {200}
    assert len({operation_id for _status, operation_id in responses}) == 1
    assert provider.physical_effect_counts == {
        "ensure_dataset": 1,
        "push_notebook": 1,
        "trigger_run": 1,
    }

    restarted_ledger = ControlLedger(path)
    restarted_runtime = runtime(restarted_ledger, provider)
    restarted_runtime.reconcile_startup()
    restarted = TestClient(
        create_app(
            ControlPlaneSettings(ledger_path=path),
            ledger=restarted_ledger,
            master_runtime=restarted_runtime,
        )
    ).post(
        "/control/v1/master/ensure",
        json={"idempotency_key": "concurrent-master", "intent": "test"},
    )
    assert restarted.status_code == 200
    assert restarted.json()["duplicate"] is True
    assert provider.physical_effect_counts["trigger_run"] == 1


def test_runtime_callback_reaches_active_through_production_app_wiring(tmp_path: Path) -> None:
    path = tmp_path / "control.sqlite3"
    ledger = ControlLedger(path)
    wired = runtime(ledger, FakeKaggleRuntime())
    app = create_app(ControlPlaneSettings(ledger_path=path), ledger=ledger, master_runtime=wired)
    response = TestClient(app).post(
        "/control/v1/master/ensure",
        json={"idempotency_key": "callback-master", "intent": "test"},
    )
    assert response.status_code == 200
    operation = ledger.get_operation(response.json()["operation_id"])
    assert operation is not None
    identity = operation.identity
    now = datetime.now(UTC)
    event = RuntimeEvent(
        event_id=str(uuid4()),
        run_id=str(identity["run_id"]),
        attempt_id=str(identity["attempt_id"]),
        service_instance_id=str(identity["service_instance_id"]),
        source_identity=assets().source_identity,
        source_version=assets().source_version,
        event_type=RuntimeEventType.SERVICE_READY,
        emitted_at=now,
        local_sequence=1,
        epoch=int(identity["epoch"]),
        data={
            "service_kind": "postgres-master",
            "endpoint": "tunnel://127.0.0.1:55432",
            "protocol": "postgresql+tls",
            "tls_fingerprint": "sha256:" + "a" * 64,
            "capabilities": ["sql", "fts", "pgvector"],
            "canonical_revision": 1,
            "schema_version": "1",
            "lease_until": (now + timedelta(minutes=4)).isoformat(),
            "master_instance_id": str(identity["master_instance_id"]),
            "epoch": int(identity["epoch"]),
        },
    )
    token = derive_runtime_secret(ROOT, str(identity["run_id"]), str(identity["attempt_id"]))
    callback = TestClient(app).post(
        "/internal/runtime/events",
        content=event.model_dump_json(by_alias=True, exclude_none=True).encode(),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert callback.status_code == 200
    assert TestClient(app).get("/health/ready").json()["master_state"] == "ACTIVE"


def test_active_runtime_claims_only_its_exact_blogger_request(tmp_path: Path) -> None:
    path = tmp_path / "control.sqlite3"
    ledger = ControlLedger(path)
    wired = runtime(ledger, FakeKaggleRuntime())
    client = TestClient(create_app(ControlPlaneSettings(ledger_path=path), ledger=ledger, master_runtime=wired))
    ensured = client.post("/control/v1/master/ensure", json={"idempotency_key": "blogger-claim-master"})
    operation = ledger.get_operation(ensured.json()["operation_id"])
    assert operation is not None
    identity = operation.identity
    now = datetime.now(UTC)
    ready = RuntimeEvent(
        event_id=str(uuid4()),
        run_id=str(identity["run_id"]),
        attempt_id=str(identity["attempt_id"]),
        service_instance_id=str(identity["service_instance_id"]),
        source_identity=assets().source_identity,
        source_version=assets().source_version,
        event_type=RuntimeEventType.SERVICE_READY,
        emitted_at=now,
        local_sequence=1,
        epoch=int(identity["epoch"]),
        data={
            "service_kind": "postgres-master",
            "endpoint": "tunnel://127.0.0.1:55432",
            "protocol": "postgresql+tls",
            "tls_fingerprint": "sha256:" + "a" * 64,
            "capabilities": ["sql", "fts", "pgvector"],
            "canonical_revision": 1,
            "schema_version": "1",
            "lease_until": (now + timedelta(minutes=4)).isoformat(),
            "master_instance_id": str(identity["master_instance_id"]),
            "epoch": int(identity["epoch"]),
        },
    )
    token = derive_runtime_secret(ROOT, str(identity["run_id"]), str(identity["attempt_id"]))
    accepted = client.post(
        "/internal/runtime/events",
        content=ready.model_dump_json(by_alias=True, exclude_none=True).encode(),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert accepted.status_code == 200
    request = BloggerMigrationRequest(
        request_id=uuid4(),
        operation_id=operation.operation_id,
        project_id=uuid4(),
        snapshot_at=now,
        source_revision="a" * 40,
    )
    created = client.post("/control/v1/blogger-closure/requests", json=request.model_dump(mode="json"))
    assert created.status_code == 200
    claim = client.get(
        f"/internal/runtime/blogger-migration/{identity['run_id']}/{identity['attempt_id']}",
        headers={
            "Authorization": f"Bearer {token}",
            "X-MDH-Master-Instance-ID": str(identity["master_instance_id"]),
            "X-MDH-Epoch": str(identity["epoch"]),
        },
    )
    assert claim.status_code == 200
    assert claim.json()["available"] is True
    assert claim.json()["request_sha256"] == request.request_sha256

    import_receipt = BloggerImportStageReceipt(
        request_id=request.request_id,
        operation_id=operation.operation_id,
        master_instance_id=identity["master_instance_id"],
        run_id=identity["run_id"],
        epoch=identity["epoch"],
        request_sha256=request.request_sha256,
        export_batch_id=uuid4(),
        row_count=266,
        distinct_record_ids=266,
        source_file_count=14,
        dispositions={"imported": 266, "quarantined": 0},
        record_id_set_sha256="b" * 64,
        logical_sha256="c" * 64,
        canonical_outcome_sha256="d" * 64,
        actor_count=266,
        account_count=266,
        duplicate_group_count=0,
        replayed_count=0,
        canonical_revision=9,
    )
    imported = client.post(
        f"/internal/runtime/blogger-migration/{identity['run_id']}/{identity['attempt_id']}/import-receipt",
        json=import_receipt.model_dump(mode="json"),
        headers={
            "Authorization": f"Bearer {token}",
            "X-MDH-Master-Instance-ID": str(identity["master_instance_id"]),
            "X-MDH-Epoch": str(identity["epoch"]),
        },
    )
    assert imported.status_code == 200
    cannot_downgrade = client.post(
        f"/internal/runtime/blogger-migration/{identity['run_id']}/{identity['attempt_id']}/failed",
        json={"request_id": str(request.request_id), "failure_code": "lost_response"},
        headers={
            "Authorization": f"Bearer {token}",
            "X-MDH-Master-Instance-ID": str(identity["master_instance_id"]),
            "X-MDH-Epoch": str(identity["epoch"]),
        },
    )
    assert cannot_downgrade.status_code == 409
    assert ledger.blogger_migration_request(str(request.request_id))["state"] == "IMPORT_COMMITTED"
    checkpoint_id = str(uuid4())
    ledger.add_checkpoint_candidate(
        checkpoint_id=checkpoint_id,
        operation_id=operation.operation_id,
        dataset_ref="owner/checkpoints",
        version_ref=None,
        manifest_sha256="e" * 64,
        source_checkpoint_id=None,
        source_head_generation=0,
        master_instance_id=str(identity["master_instance_id"]),
        epoch=int(identity["epoch"]),
        manifest_payload={"canonical_revision": 9},
    )
    ledger.mark_checkpoint_uploaded(checkpoint_id, "owner/checkpoints/1")
    ledger.mark_checkpoint_readback_verified(checkpoint_id)
    ledger.mark_checkpoint_restore_verified(checkpoint_id)
    ledger.mark_checkpoint_verified(checkpoint_id)
    ledger.promote_checkpoint(
        "postgres-master",
        checkpoint_id,
        expected_generation=0,
        expected_parent_checkpoint_id=None,
    )
    status = client.get(f"/control/v1/blogger-closure/requests/{request.request_id}")
    assert status.status_code == 200
    assert status.json()["state"] == "CHECKPOINT_VERIFIED"
    coverage = ledger.connector_coverage_metadata()
    assert coverage[0]["connector_kind"] == "region-talk-ydb-bloggers-v1"
    assert coverage[0]["state"] == "COMPLETE"


class RecordingRegistrar:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.credentials = []

    def store(self, credential):  # type: ignore[no-untyped-def]
        self.credentials.append(credential)
        return self.root / f"reader-{credential.epoch}.json"


def test_runtime_can_register_bounded_reader_credential_without_echoing_secret(tmp_path: Path) -> None:
    path = tmp_path / "control.sqlite3"
    ledger = ControlLedger(path)
    wired = runtime(ledger, FakeKaggleRuntime())
    registrar = RecordingRegistrar(tmp_path)
    app = create_app(
        ControlPlaneSettings(ledger_path=path),
        ledger=ledger,
        master_runtime=wired,
        session_registrar=registrar,
    )
    ensured = TestClient(app).post(
        "/control/v1/master/ensure",
        json={"idempotency_key": "credential-master", "intent": "test"},
    )
    operation = ledger.get_operation(ensured.json()["operation_id"])
    assert operation is not None
    identity = operation.identity
    token = derive_runtime_secret(ROOT, str(identity["run_id"]), str(identity["attempt_id"]))
    secret_url = (
        "postgresql://reader:opaque-password@127.0.0.1:55432/hub"
        "?sslmode=verify-full&sslrootcert=/state/master-tls/ca.pem&connect_timeout=5"
    )
    response = TestClient(app).post(
        f"/internal/runtime/session-credentials/{identity['run_id']}/{identity['attempt_id']}",
        json={
            "master_instance_id": identity["master_instance_id"],
            "epoch": identity["epoch"],
            "credentials": [
                {
                    "role": "reader",
                    "database_url": secret_url,
                    "expires_at": (datetime.now(UTC) + timedelta(minutes=2)).isoformat(),
                }
            ],
        },
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 200
    assert response.json() == {"registered": 1, "credential_refs": ["reader-1.json"]}
    assert secret_url not in response.text
    assert registrar.credentials[0].database_url == secret_url


def test_embedding_capability_is_fail_closed_without_injected_master_runner(tmp_path: Path) -> None:
    ledger = ControlLedger(tmp_path / "capability.sqlite3")
    app = create_app(
        ControlPlaneSettings(ledger_path=ledger.path),
        ledger=ledger,
        master_runtime=runtime(ledger, FakeKaggleRuntime()),
    )
    response = TestClient(app).get("/control/v1/embedding-production/capabilities")
    assert response.status_code == 503
    assert response.json()["detail"]["code"] == "embedding_production_unavailable"


def test_embedding_capability_only_appears_with_injected_master_runner(tmp_path: Path) -> None:
    ledger = ControlLedger(tmp_path / "capability-ready.sqlite3")
    app = create_app(
        ControlPlaneSettings(ledger_path=ledger.path),
        ledger=ledger,
        master_runtime=runtime(ledger, FakeKaggleRuntime()),
        embedding_stage_runner=object(),
    )
    response = TestClient(app).get("/control/v1/embedding-production/capabilities")
    assert response.status_code == 200
    assert response.json()["ready"] is True
