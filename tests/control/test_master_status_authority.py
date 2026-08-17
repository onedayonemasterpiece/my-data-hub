from __future__ import annotations

import hashlib
import json
import threading
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from cryptography import x509
from cryptography.x509.oid import ObjectIdentifier
from fastapi.testclient import TestClient

from my_data_hub.control_plane.app import ControlPlaneSettings, create_app
from my_data_hub.control_plane.clock import DeterministicClock
from my_data_hub.control_plane.ledger import ControlLedger
from my_data_hub.control_plane.runtime import ControlPlaneMasterRuntime, MasterRuntimeSettings
from my_data_hub.orchestrator.master import MasterCoordinator, MasterState
from my_data_hub.providers.kaggle import KaggleMasterLaunchAssets, KaggleMasterRuntimeProvider
from my_data_hub.providers.kaggle.contracts import (
    DatasetMutationResult,
    EffectOutcome,
    KaggleDatasetIdentity,
    KaggleKernelRunIdentity,
    KaggleKernelSourceIdentity,
    NotebookMutationResult,
    ProviderEffectReceipt,
    TaskResourceClaim,
)
from my_data_hub.providers.kaggle.source_attestation import executable_source_sha256
from my_data_hub.providers.models import ControlClass, ProviderFingerprint, ProviderKind
from my_data_hub.runtime_sdk import RuntimeEvent, RuntimeEventType

NOW = datetime(2026, 8, 11, 12, tzinfo=UTC)


def _assets() -> KaggleMasterLaunchAssets:
    return KaggleMasterLaunchAssets(
        source_identity="owner/postgres-master",
        source_version="git:" + "a" * 40,
        checkpoint_ref="owner/checkpoints",
        dataset_ref="owner/master-assets",
        notebook_ref="owner/postgres-master",
        dataset_files={
            "checkpoint-verifier.ipynb": b"{}",
            "postgresql-18-runtime.bundle": b"fake-postgresql-18-runtime",
            "postgresql-18-runtime.json": b"""{"archive_sha256":"63a988449f3d37c9c9fd2658b14f9254918e0b0f8ac600f9b98f15ede09e912f","build_recipe_sha256":"3fbcf52450dd44e3eb0eb7b826ebdb84a4293fbc54b713408083f10b44964d61","builder_image":"ubuntu:22.04@sha256:3b06811b2afd352be909dd088a004166d665dc76d38b13eada33522a9d915c6f","pgvector_source_sha256":"10bf9938906e5d643bbc4a7eea104b6f57ba4898e5b76b20e60484ea1d5a7f8f","pgvector_source_url":"https://github.com/pgvector/pgvector/archive/refs/tags/v0.8.6.tar.gz","pgvector_version":"0.8.6","platform":"linux-x86_64","postgresql_source_sha256":"81a81ec695fb0c7901407defaa1d2f7973617154cf27ba74e3a7ab8e64436094","postgresql_source_url":"https://ftp.postgresql.org/pub/source/v18.4/postgresql-18.4.tar.bz2","postgresql_version":"18.4","schema_version":"my-data-hub-postgresql-runtime.v1"}""",
            "tunnel-known-hosts": b"|1|aaaa|bbbb ssh-ed25519 AAAA\n",
        },
        notebook_source=b"print('master')\n",
        callback_url="https://mcp-datahub.kenigevents.ru/internal/runtime/events",
        checkpoint_verifier_ref="owner/checkpoint-verifier",
        checkpoint_verifier_source_file="checkpoint-verifier.ipynb",
        checkpoint_probe_relations=("hub.canonical_state",),
        tunnel_gateway_host="gateway.example.test",
        tunnel_gateway_port=22,
        tunnel_gateway_user="mdh_tunnel",
        tunnel_remote_port=25432,
        notebook_kernel_type="script",
        notebook_timeout_seconds=3600,
    )


class StatusAdapter:
    def __init__(self, ledger: ControlLedger) -> None:
        self.ledger = ledger
        self.calls: Counter[str] = Counter()
        self.status_files: dict[str, bytes] = {}
        self.source = b""
        self.dataset_sources: tuple[str, ...] = ()
        self.run: KaggleKernelRunIdentity | None = None
        self.crash_after_status = False
        self.lose_first_delete = False
        self.absent = False

    def create_private_dataset(self, *, intent, files, control_class, disposable, **_kwargs):
        is_status = "mdh-master-status-" in intent.provider_ref
        self.calls["status_create" if is_status else "asset_create"] += 1
        fingerprint = ProviderFingerprint(value=("9" if is_status else "8") * 64)
        identity = KaggleDatasetIdentity(
            provider_ref=intent.provider_ref,
            version=1,
            privacy="private",
            package_sha256="7" * 64,
            fingerprint=fingerprint,
            observed_at=NOW,
        )
        claim = TaskResourceClaim.create(
            task_id=intent.task_id,
            effect_id=intent.effect_id,
            provider_ref=intent.provider_ref,
            kind=ProviderKind.DATASET,
            control_class=control_class,
            disposable=disposable,
            fingerprint=fingerprint,
            provider_version=1,
            registered_at=intent.requested_at,
        )
        receipt = ProviderEffectReceipt(
            operation_id=intent.operation_id,
            effect_id=intent.effect_id,
            action=intent.action,
            provider_ref=intent.provider_ref,
            outcome=EffectOutcome.APPLIED,
            attempts=1,
            observed_fingerprint=fingerprint,
            provider_version=1,
            observed_at=NOW,
            detail_code="private_dataset_exact_readback",
        )
        if is_status:
            self.status_files = dict(files)
            self.ledger.persist_provider_effect_intent(intent.model_dump(mode="json"))
            self.ledger.persist_provider_effect_receipt(str(intent.effect_id), receipt.model_dump(mode="json"))
            self.ledger.persist_provider_resource_claim(claim.model_dump(mode="json"))
            if self.crash_after_status:
                raise RuntimeError("simulated crash after status side effect")
        return DatasetMutationResult(identity=identity, claim=claim, effect=receipt)

    def push_private_master_notebook_pending_attestation(
        self, *, intent, task_run_id, source, dataset_sources, **_kwargs
    ):
        self.calls["push"] += 1
        self.source = source
        self.dataset_sources = tuple(dataset_sources)
        source_sha = executable_source_sha256(source, kernel_type="script")
        fingerprint = ProviderFingerprint(value="6" * 64)
        source_identity = KaggleKernelSourceIdentity(
            provider_ref=intent.provider_ref,
            source_version=1,
            privacy="private",
            source_sha256=source_sha,
            fingerprint=fingerprint,
            observed_at=NOW,
        )
        self.run = KaggleKernelRunIdentity(
            task_run_id=task_run_id,
            provider_ref=intent.provider_ref,
            source_version=1,
            source_sha256=source_sha,
            provider_kernel_id=42,
            provider_run_ref=f"{intent.provider_ref}/1",
            started_at=NOW,
        )
        claim = TaskResourceClaim.create(
            task_id=intent.task_id,
            effect_id=intent.effect_id,
            provider_ref=intent.provider_ref,
            kind=ProviderKind.NOTEBOOK,
            control_class=ControlClass.ORCHESTRATOR_PROTECTED,
            disposable=False,
            fingerprint=fingerprint,
            provider_version=1,
            registered_at=intent.requested_at,
        )
        # The coordinator consumes only the exact run identity from this result.
        return NotebookMutationResult(
            source=source_identity,
            run=self.run,
            claim=claim,
            effect=ProviderEffectReceipt(
                operation_id=intent.operation_id,
                effect_id=intent.effect_id,
                action=intent.action,
                provider_ref=intent.provider_ref,
                outcome=EffectOutcome.APPLIED,
                attempts=1,
                observed_fingerprint=fingerprint,
                provider_version=1,
                observed_at=NOW,
                detail_code="private_notebook_pending_runtime_attestation",
            ),
        )

    def read_attested_master_run_status(self, run):
        assert run == self.run
        return type("Status", (), {"state": "running"})()

    read_run_status = read_attested_master_run_status

    def delete_task_created_resource(self, *, intent, claim):
        self.calls["delete"] += 1
        if self.lose_first_delete and not self.absent:
            self.absent = True
            raise RuntimeError("simulated lost delete response")
        self.absent = True
        return ProviderEffectReceipt(
            operation_id=intent.operation_id,
            effect_id=intent.effect_id,
            action=intent.action,
            provider_ref=claim.provider_ref,
            outcome=EffectOutcome.ALREADY_APPLIED,
            attempts=0,
            observed_at=NOW,
            detail_code="task_created_resource_already_absent",
        )


def _runtime(tmp_path: Path) -> tuple[ControlLedger, StatusAdapter, ControlPlaneMasterRuntime]:
    ledger = ControlLedger(tmp_path / "control.sqlite3", clock=DeterministicClock(NOW))
    adapter = StatusAdapter(ledger)
    assets = _assets()
    provider = KaggleMasterRuntimeProvider(adapter, assets, status_authority=ledger)  # type: ignore[arg-type]
    runtime = ControlPlaneMasterRuntime(
        ledger,
        MasterCoordinator(ledger, provider),
        MasterRuntimeSettings(assets),
        master_tls_ca_path=tmp_path / "master-tls-ca.pem",
    )
    return ledger, adapter, runtime


def _ledger_storage_bytes(ledger: ControlLedger) -> bytes:
    candidates = (
        ledger.path,
        ledger.path.with_name(ledger.path.name + "-wal"),
        ledger.path.with_name(ledger.path.name + "-shm"),
    )
    return b"".join(path.read_bytes() for path in candidates if path.is_file())


def _promote_checkpoint(ledger: ControlLedger, *, version: int = 7) -> tuple[str, str]:
    checkpoint_id = str(uuid4())
    operation_id = str(uuid4())
    ledger.ensure_operation(
        operation_id=operation_id,
        idempotency_key=f"checkpoint-source:{checkpoint_id}",
        operation_kind="checkpoint_source_test",
        intent={"source": "test"},
        initial_state="STOPPED",
        identity={"run_id": str(uuid4()), "attempt_id": str(uuid4()), "epoch": 1},
    )
    ledger.add_checkpoint_candidate(
        checkpoint_id=checkpoint_id,
        operation_id=operation_id,
        dataset_ref="owner/checkpoints",
        version_ref=None,
        manifest_sha256="c" * 64,
        source_checkpoint_id=None,
        master_instance_id=str(uuid4()),
        epoch=1,
    )
    exact_ref = f"owner/checkpoints/{version}"
    ledger.mark_checkpoint_uploaded(checkpoint_id, exact_ref)
    ledger.mark_checkpoint_readback_verified(checkpoint_id)
    ledger.mark_checkpoint_restore_verified(checkpoint_id)
    ledger.promote_checkpoint(
        "postgres-master",
        checkpoint_id,
        expected_generation=0,
        expected_parent_checkpoint_id=None,
    )
    return checkpoint_id, exact_ref


def test_master_status_dataset_is_hash_only_exact_version_and_not_in_source(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    ledger, adapter, runtime = _runtime(tmp_path)

    handle, duplicate = runtime.ensure("status-authority")

    assert not duplicate and handle.state is MasterState.REGISTERING
    status = ledger.master_status_dataset_authority(handle.operation_id)
    assert status is not None and status["state"] == "READY"
    token = json.loads(adapter.status_files["kaggle_run.json"])["token"]
    tls_private_key = adapter.status_files["postgres-server.key"]
    tls_certificate = adapter.status_files["postgres-server.crt"]
    assert len(token) == 64
    assert status["token_sha256"] == hashlib.sha256(token.encode()).hexdigest()
    assert token not in json.dumps(status, sort_keys=True)
    assert token.encode() not in _ledger_storage_bytes(ledger)
    assert tls_private_key not in _ledger_storage_bytes(ledger)
    assert status["status_dataset"]["tls_certificate_pem"].encode() == tls_certificate
    certificate = x509.load_pem_x509_certificate(tls_certificate)
    binding = json.loads(
        certificate.extensions.get_extension_for_oid(ObjectIdentifier("1.3.6.1.4.1.57264.1.1")).value.value
    )
    assert binding == {
        "schema_version": "my-data-hub-master-tls-binding.v1",
        "operation_id": handle.operation_id,
        "run_id": handle.run_id,
        "attempt_id": handle.attempt_id,
        "master_instance_id": handle.master_instance_id,
        "epoch": handle.epoch,
    }
    assert token.encode() not in adapter.source
    assert tls_private_key not in adapter.source
    assert token not in caplog.text
    assert adapter.dataset_sources == (
        "owner/master-assets/1",
        f"owner/mdh-master-status-{UUID(handle.run_id).hex}/1",
    )
    assert ledger.runtime_token_valid(handle.run_id, handle.attempt_id, token)
    assert adapter.calls == {"status_create": 1, "asset_create": 1, "push": 1}


def test_atomic_tls_rotation_is_visible_by_shared_directory_without_private_key_leak(
    tmp_path: Path,
) -> None:
    ledger, adapter, runtime = _runtime(tmp_path)
    observer = tmp_path / "master-tls-ca.pem"
    first, _ = runtime.ensure("tls-rotation-one")
    first_certificate = observer.read_bytes()
    first_private_key = adapter.status_files["postgres-server.key"]
    ledger.transition_operation(
        first.operation_id,
        expected_state=MasterState.REGISTERING.value,
        new_state=MasterState.FAILED.value,
        metadata={"code": "TEST_ROTATE_TLS"},
    )
    runtime.reconcile_status_cleanup_once()

    second, _ = runtime.ensure("tls-rotation-two")
    second_certificate = observer.read_bytes()

    assert second.epoch == first.epoch + 1
    assert first_certificate != second_certificate
    assert first_private_key not in _ledger_storage_bytes(ledger)
    assert adapter.status_files["postgres-server.key"] not in _ledger_storage_bytes(ledger)
    first_status = ledger.master_status_dataset_authority(first.operation_id)
    second_status = ledger.master_status_dataset_authority(second.operation_id)
    assert first_status is not None and second_status is not None
    assert first_status["status_dataset"]["tls_certificate_pem"].encode() == first_certificate
    assert second_status["status_dataset"]["tls_certificate_pem"].encode() == second_certificate
    assert observer.stat().st_mode & 0o777 == 0o600


def test_verified_head_is_in_status_config_and_attached_by_exact_numeric_version(
    tmp_path: Path,
) -> None:
    ledger, adapter, runtime = _runtime(tmp_path)
    checkpoint_id, exact_ref = _promote_checkpoint(ledger)

    handle, _ = runtime.ensure("verified-head-boot")

    config = json.loads(adapter.status_files["master-config.json"])
    assert config["checkpoint_id"] == checkpoint_id
    assert config["checkpoint_exact_version_ref"] == exact_ref
    assert config["checkpoint_manifest_sha256"] == "c" * 64
    assert config["checkpoint_head_generation"] == 1
    assert config["checkpoint_directory"] is None
    assert config["postgres_bin"] == "/tmp/mdh-postgresql-runtime/pgsql/bin"
    assert adapter.dataset_sources == (
        "owner/master-assets/1",
        f"owner/mdh-master-status-{UUID(handle.run_id).hex}/1",
        exact_ref,
    )


def test_checkpoint_head_change_after_status_admission_blocks_before_notebook_push(
    tmp_path: Path,
) -> None:
    ledger, adapter, runtime = _runtime(tmp_path)
    key = "head-cas-race"
    identity = MasterCoordinator.identity_for(key)
    operation, _ = ledger.ensure_master_operation(
        operation_id=identity["operation_id"],
        idempotency_key=key,
        intent=runtime.intent(key).as_dict(),
        identity=identity,
    )
    assert runtime._status_dataset_ready(operation.operation_id, operation.identity)
    _promote_checkpoint(ledger)

    with pytest.raises(ValueError, match="HEAD advanced"):
        runtime.coordinator.ensure_master(runtime.intent(key))

    assert adapter.calls["push"] == 0


def test_master_launch_rejects_control_credentials_and_manual_tls_user_secrets() -> None:
    for binding in (
        {"KAGGLE_API_TOKEN": "MDH_KAGGLE_API_TOKEN"},
        {"KAGGLE_USERNAME": "MDH_KAGGLE_USERNAME", "KAGGLE_KEY": "MDH_KAGGLE_KEY"},
        {"MY_DATA_HUB_POSTGRES_TLS_CERT": "MDH_TLS_CERT"},
        {"MY_DATA_HUB_POSTGRES_TLS_KEY": "MDH_TLS_KEY"},
    ):
        with pytest.raises(ValueError, match="runtime secret binding is invalid"):
            replace(_assets(), runtime_secret_bindings=binding)


def test_donor_heartbeat_renews_the_exact_status_resource_lease(tmp_path: Path) -> None:
    ledger, adapter, runtime = _runtime(tmp_path)
    handle, _ = runtime.ensure("status-resource-renewal")
    token = json.loads(adapter.status_files["kaggle_run.json"])["token"]
    status = ledger.master_status_dataset_authority(handle.operation_id)
    assert status is not None
    resource = status["resource_lease"]
    ready = RuntimeEvent(
        event_id="22222222-2222-4222-8222-222222222222",
        run_id=handle.run_id,
        attempt_id=handle.attempt_id,
        service_instance_id=handle.service_instance_id,
        source_identity=_assets().source_identity,
        source_version=_assets().source_version,
        event_type=RuntimeEventType.SERVICE_READY,
        emitted_at=NOW,
        local_sequence=1,
        epoch=handle.epoch,
        phase="service",
        status="ready",
        data={
            "service_kind": "postgres-master",
            "endpoint": "tunnel://127.0.0.1:55432",
            "protocol": "postgresql+tls",
            "tls_fingerprint": "sha256:" + status["status_dataset"]["tls_certificate_sha256"],
            "capabilities": ["sql"],
            "canonical_revision": 0,
            "schema_version": "1",
            "lease_until": (NOW + timedelta(minutes=4)).isoformat(),
            "master_instance_id": handle.master_instance_id,
            "epoch": handle.epoch,
            "executed_source_sha256": executable_source_sha256(adapter.source, kernel_type="script"),
            "boot_checkpoint": status["status_dataset"]["boot_checkpoint"],
        },
    )
    runtime.coordinator.accept_runtime_event(
        ready.model_dump_json(by_alias=True, exclude_none=True).encode(), header_token=token
    )
    renewed_until = NOW + timedelta(minutes=10)
    heartbeat = RuntimeEvent(
        event_id="33333333-3333-4333-8333-333333333333",
        run_id=handle.run_id,
        attempt_id=handle.attempt_id,
        service_instance_id=handle.service_instance_id,
        source_identity=_assets().source_identity,
        source_version=_assets().source_version,
        event_type=RuntimeEventType.RUNTIME_HEARTBEAT,
        emitted_at=NOW + timedelta(seconds=30),
        local_sequence=2,
        epoch=handle.epoch,
        phase="active",
        status="healthy",
        data={"lease_until": renewed_until.isoformat(), "resource": resource},
    )

    runtime.coordinator.accept_runtime_event(
        heartbeat.model_dump_json(by_alias=True, exclude_none=True).encode(), header_token=token
    )

    lease = ledger.resource_lease(str(resource["lease_id"]))
    assert lease is not None and lease.lease_until == renewed_until


def test_twenty_same_key_ensures_create_one_status_dataset_and_one_run(tmp_path: Path, monkeypatch) -> None:
    _ledger, adapter, runtime = _runtime(tmp_path)
    barrier = threading.Barrier(20)
    candidates = iter(range(20))

    def candidate(_size: int) -> str:
        value = next(candidates)
        barrier.wait(timeout=5)
        return f"{value:064x}"

    monkeypatch.setattr("my_data_hub.control_plane.runtime.secrets.token_hex", candidate)
    with ThreadPoolExecutor(max_workers=20) as pool:
        handles = list(pool.map(lambda _: runtime.ensure("concurrent-status")[0], range(20)))

    assert {handle.operation_id for handle in handles} == {handles[0].operation_id}
    assert adapter.calls["status_create"] == 1
    assert adapter.calls["push"] == 1


def test_crash_after_status_side_effect_never_recreates_and_cleans_exact_claim(
    tmp_path: Path,
) -> None:
    ledger, adapter, runtime = _runtime(tmp_path)
    adapter.crash_after_status = True

    with pytest.raises(RuntimeError, match="after status side effect"):
        runtime.ensure("ambiguous-status")
    token = json.loads(adapter.status_files["kaggle_run.json"])["token"]
    operation = ledger.incomplete_operations("ensure_master")[0]
    assert runtime.ensure("ambiguous-status")[0].state is MasterState.REQUESTED
    assert adapter.calls["status_create"] == 1

    ledger.clock.advance(901)  # type: ignore[attr-defined]
    failed, _ = runtime.ensure("ambiguous-status")
    stored = ledger.master_status_dataset_authority(operation.operation_id)
    assert failed.state is MasterState.FAILED
    assert stored is not None and stored["state"] == "AMBIGUOUS"
    assert stored["cleanup_receipt"] is not None
    assert not ledger.runtime_token_valid(failed.run_id, failed.attempt_id, token)
    lease = ledger.resource_lease(stored["resource_lease"]["lease_id"])
    assert lease is not None and lease.released_at is not None
    assert adapter.calls["status_create"] == 1 and adapter.calls["delete"] == 1


def test_periodic_terminal_cleanup_reconciles_lost_delete_response(tmp_path: Path) -> None:
    ledger, adapter, runtime = _runtime(tmp_path)
    handle, _ = runtime.ensure("terminal-cleanup")
    ledger.transition_operation(
        handle.operation_id,
        expected_state=MasterState.REGISTERING.value,
        new_state=MasterState.FAILED.value,
        metadata={"code": "TEST_TERMINAL"},
    )
    adapter.lose_first_delete = True

    with pytest.raises(RuntimeError, match="lost delete response"):
        runtime.reconcile_status_cleanup_once()
    stored = ledger.master_status_dataset_authority(handle.operation_id)
    assert stored is not None and stored["state"] == "CLEANING"
    ledger.clock.advance(901)  # type: ignore[attr-defined]

    assert runtime.reconcile_status_cleanup_once() == handle.operation_id
    stored = ledger.master_status_dataset_authority(handle.operation_id)
    assert stored is not None and stored["state"] == "CLEANED"
    assert stored["cleanup_receipt"] is not None
    assert adapter.calls["delete"] == 2


def test_runtime_failure_callback_triggers_cleanup_without_restart(tmp_path: Path) -> None:
    ledger, adapter, runtime = _runtime(tmp_path)
    handle, _ = runtime.ensure("callback-cleanup")
    token = json.loads(adapter.status_files["kaggle_run.json"])["token"]
    ledger.transition_operation(
        handle.operation_id,
        expected_state=MasterState.REGISTERING.value,
        new_state=MasterState.FAILED.value,
        metadata={"code": "TEST_FAILURE"},
    )
    event = RuntimeEvent(
        event_id="11111111-1111-4111-8111-111111111111",
        run_id=handle.run_id,
        attempt_id=handle.attempt_id,
        service_instance_id=handle.service_instance_id,
        source_identity=_assets().source_identity,
        source_version=_assets().source_version,
        event_type=RuntimeEventType.RUNTIME_FAILED,
        emitted_at=NOW,
        local_sequence=1,
        epoch=handle.epoch,
        phase="bootstrap",
        status="failed",
        data={"failure_code": "TEST_FAILURE"},
    )
    app = create_app(
        ControlPlaneSettings(ledger_path=ledger.path),
        ledger=ledger,
        master_runtime=runtime,
    )

    response = TestClient(app).post(
        "/internal/runtime/events",
        content=event.model_dump_json(by_alias=True, exclude_none=True).encode(),
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 200
    stored = ledger.master_status_dataset_authority(handle.operation_id)
    assert stored is not None and stored["state"] == "CLEANED"
    assert adapter.calls["delete"] == 1
