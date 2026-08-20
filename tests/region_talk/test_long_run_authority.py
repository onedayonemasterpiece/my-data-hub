from __future__ import annotations

import hashlib
import threading
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

import pytest
from fastapi.testclient import TestClient

from my_data_hub.control_plane.app import ControlPlaneSettings, create_app
from my_data_hub.control_plane.ledger import ControlLedger
from my_data_hub.hashing import canonical_json_bytes
from my_data_hub.workloads.region_talk.pipeline_contracts import (
    ActiveMasterBinding,
    RegionTalkCredentialActivation,
    RegionTalkCredentialRefreshRequest,
    RegionTalkLaunchMetadata,
    RegionTalkRunRequest,
    RegionTalkTerminalReceipt,
    TaskWorkerCredentialRegistration,
)
from my_data_hub.workloads.region_talk.production_assembly import (
    DirectoryRegionTalkTaskAuthority,
    RegionTalkAssemblyUnavailable,
)

NOW = datetime(2026, 8, 19, 12, tzinfo=UTC)
MASTER = ActiveMasterBinding(
    run_id=UUID("11111111-1111-4111-8111-111111111111"),
    attempt_id=UUID("22222222-2222-4222-8222-222222222222"),
    master_instance_id=UUID("33333333-3333-4333-8333-333333333333"),
    epoch=47,
)


class _Broker:
    def __init__(self) -> None:
        self.serial = 7000
        self.revoked: list[dict[str, object]] = []

    def issue_task_worker_public_key(self, **_kwargs):  # type: ignore[no-untyped-def]
        self.serial += 1
        return SimpleNamespace(
            certificate=f"ssh-ed25519-cert-v01@openssh.com test-{self.serial}",
            account="mdh-region-talk",
            serial=self.serial,
        )

    def revoke_task_worker_certificate(self, **kwargs):  # type: ignore[no-untyped-def]
        self.revoked.append(dict(kwargs))


def _metadata() -> RegionTalkLaunchMetadata:
    request = RegionTalkRunRequest.supervised(
        idempotency_key="region-talk-long-run-authority", requested_at=NOW
    )
    return RegionTalkLaunchMetadata(
        request_id=request.request_id,
        task_run_id=UUID("55555555-5555-4555-8555-555555555555"),
        trigger=request.trigger,
        schedule_slot=request.schedule_slot,
        master=MASTER,
        runtime_dataset_exact_ref="owner/runtime/12",
        runtime_image_identity="runtime@sha256:" + "d" * 64,
        runtime_image_source_commit="e" * 40,
        wheel_relative_path="dist/my_data_hub.whl",
        wheel_sha256="f" * 64,
        ydb_endpoint="grpcs://ydb.serverless.yandexcloud.net:2135",
        ydb_database="/ru-central1/example/region-talk",
        ydb_viewer_secret_label="REGION_TALK_YDB_VIEWER_SA_JSON",
        max_cycles=1,
        max_runtime_seconds=7200,
    )


def _authority(tmp_path: Path) -> tuple[DirectoryRegionTalkTaskAuthority, _Broker]:
    root = (tmp_path / "private").absolute()
    root.mkdir(mode=0o700)
    ca = (tmp_path / "ca.pem").absolute()
    known = (tmp_path / "known-hosts").absolute()
    ca.write_text("test-ca", encoding="ascii")
    known.write_text("gateway ssh-ed25519 test", encoding="ascii")
    broker = _Broker()
    return (
        DirectoryRegionTalkTaskAuthority(
            root=root,
            broker=broker,
            tls_ca_path=ca,
            known_hosts_path=known,
            gateway_host="gateway.internal",
            gateway_port=2222,
            clock=lambda: NOW,
            wait_seconds=2,
        ),
        broker,
    )


def _register(
    authority: DirectoryRegionTalkTaskAuthority,
    command,
    *,
    credential_id: UUID,
    acknowledge: bool = True,
    expires_at: datetime | None = None,
):  # type: ignore[no-untyped-def]
    receipt = authority.register(
        TaskWorkerCredentialRegistration(
            master_instance_id=MASTER.master_instance_id,
            epoch=MASTER.epoch,
            task_run_id=command.task_run_id,
            generation=command.generation,
            credential_id=credential_id,
            database_url=(
                "postgresql://region:private@127.0.0.1:25432/postgres"
                "?sslmode=verify-ca"
            ),
            expires_at=expires_at or NOW + timedelta(minutes=4),
            task_token_sha256=command.task_token_sha256,
            command_sha256=command.command_sha256,
        )
    )
    if acknowledge:
        authority.acknowledge_registrations((receipt,))
    return receipt


def test_generation_refresh_replays_and_revokes_only_after_activation_ack(
    tmp_path: Path,
) -> None:
    authority, broker = _authority(tmp_path)
    metadata = _metadata()
    source_sha256 = "a" * 64
    command = authority.prepare(
        metadata, task_token="t" * 48, source_sha256=source_sha256
    )
    # Provider response-loss restart supplies a new random candidate token but
    # must retain the already-fsynced exact task token/command.
    replay = authority.prepare(
        metadata, task_token="x" * 48, source_sha256=source_sha256
    )
    assert replay == command
    first_receipt = _register(
        authority,
        command,
        credential_id=UUID("66666666-6666-4666-8666-666666666666"),
        acknowledge=False,
    )
    # Persisting the registration is not delivery.  Until the ACTIVE master
    # explicitly ACKs its exact response, GET replays the command and private
    # access remains unavailable.
    assert authority.batch(
        master_instance_id=MASTER.master_instance_id, epoch=MASTER.epoch
    ).commands == (command,)
    authority.acknowledge_registrations((first_receipt,))
    first = authority.await_access(metadata, command)
    request = RegionTalkCredentialRefreshRequest(
        request_id=metadata.request_id,
        task_run_id=metadata.task_run_id,
        master_instance_id=MASTER.master_instance_id,
        epoch=MASTER.epoch,
        source_sha256=source_sha256,
        image_identity=metadata.runtime_image_identity,
        image_source_commit=metadata.runtime_image_source_commit,
        previous=authority._binding(first),
        requested_at=NOW + timedelta(minutes=3),
    )
    result: list[object] = []
    error: list[BaseException] = []

    def refresh() -> None:
        try:
            result.append(authority.refresh(request))
        except BaseException as exc:  # pragma: no cover - surfaced below
            error.append(exc)

    thread = threading.Thread(target=refresh)
    thread.start()
    deadline = time.monotonic() + 1
    second_command = None
    while time.monotonic() < deadline:
        batch = authority.batch(
            master_instance_id=MASTER.master_instance_id, epoch=MASTER.epoch
        )
        if batch.commands and batch.commands[0].generation == 2:
            second_command = batch.commands[0]
            break
        time.sleep(0.01)
    assert second_command is not None
    _register(
        authority,
        second_command,
        credential_id=UUID("77777777-7777-4777-8777-777777777777"),
    )
    thread.join(timeout=2)
    assert not thread.is_alive() and error == [] and len(result) == 1
    second = result[0]
    assert second.generation == 2  # type: ignore[union-attr]
    assert broker.revoked == []
    # Lost private response returns byte-for-byte the persisted generation;
    # it cannot create a second LOGIN/certificate, including after a control
    # process restart which reconstructs only from the private sidecar.
    restarted = DirectoryRegionTalkTaskAuthority(
        root=authority.root,
        broker=broker,
        tls_ca_path=authority.tls_ca_path,
        known_hosts_path=authority.known_hosts_path,
        gateway_host=authority.gateway_host,
        gateway_port=authority.gateway_port,
        clock=lambda: NOW,
        wait_seconds=2,
    )
    assert restarted.refresh(request) == second
    authority = restarted
    assert broker.serial == 7002

    activation = RegionTalkCredentialActivation(
        request_id=metadata.request_id,
        task_run_id=metadata.task_run_id,
        master_instance_id=MASTER.master_instance_id,
        epoch=MASTER.epoch,
        source_sha256=source_sha256,
        image_identity=metadata.runtime_image_identity,
        image_source_commit=metadata.runtime_image_source_commit,
        previous=authority._binding(first),
        replacement=authority._binding(second),  # type: ignore[arg-type]
        asserted_at=NOW + timedelta(minutes=3, seconds=5),
    )
    authority.activate(activation)
    authority.activate(activation)
    assert len(broker.revoked) == 1
    assert broker.revoked[0]["serial"] == first.ssh_certificate_serial
    revocations = authority.batch(
        master_instance_id=MASTER.master_instance_id, epoch=MASTER.epoch
    ).revocations
    assert len(revocations) == 1 and revocations[0].generation == 1
    # GET is non-destructive and exact response-loss replay continues until ACK.
    assert authority.batch(
        master_instance_id=MASTER.master_instance_id, epoch=MASTER.epoch
    ).revocations == revocations
    authority.acknowledge_revocations(revocations)
    assert authority.batch(
        master_instance_id=MASTER.master_instance_id, epoch=MASTER.epoch
    ).revocations == ()
    # Even if the worker lost the activation HTTP response until after the
    # master ACK/purge, the retained non-secret binding makes replay exact.
    authority.activate(activation)
    assert len(broker.revoked) == 1
    authority.request_revocation(
        SimpleNamespace(
            task_run_id=metadata.task_run_id,
            master=MASTER,
            access=authority._binding(second),  # type: ignore[arg-type]
        )
    )
    terminal_revocations = authority.batch(
        master_instance_id=MASTER.master_instance_id, epoch=MASTER.epoch
    ).revocations
    assert len(terminal_revocations) == 1
    assert terminal_revocations[0].generation == 2
    authority.acknowledge_revocations(terminal_revocations)
    assert list(authority.root.iterdir()) == []
    assert authority.root.stat().st_mode & 0o077 == 0


def test_activation_crash_after_mailbox_is_exactly_replayable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    authority, broker = _authority(tmp_path)
    metadata = _metadata()
    command = authority.prepare(metadata, task_token="t" * 48, source_sha256="a" * 64)
    _register(
        authority,
        command,
        credential_id=UUID("66666666-6666-4666-8666-666666666666"),
    )
    first = authority.await_access(metadata, command)
    request = RegionTalkCredentialRefreshRequest(
        request_id=metadata.request_id,
        task_run_id=metadata.task_run_id,
        master_instance_id=MASTER.master_instance_id,
        epoch=MASTER.epoch,
        source_sha256="a" * 64,
        image_identity=metadata.runtime_image_identity,
        image_source_commit=metadata.runtime_image_source_commit,
        previous=authority._binding(first),
        requested_at=NOW,
    )
    results: list[object] = []
    thread = threading.Thread(target=lambda: results.append(authority.refresh(request)))
    thread.start()
    deadline = time.monotonic() + 1
    replacement_command = None
    while time.monotonic() < deadline:
        batch = authority.batch(master_instance_id=MASTER.master_instance_id, epoch=MASTER.epoch)
        if batch.commands and batch.commands[0].generation == 2:
            replacement_command = batch.commands[0]
            break
        time.sleep(0.01)
    assert replacement_command is not None
    _register(
        authority,
        replacement_command,
        credential_id=UUID("77777777-7777-4777-8777-777777777777"),
    )
    thread.join(timeout=2)
    second = results[0]
    activation = RegionTalkCredentialActivation(
        request_id=metadata.request_id,
        task_run_id=metadata.task_run_id,
        master_instance_id=MASTER.master_instance_id,
        epoch=MASTER.epoch,
        source_sha256="a" * 64,
        image_identity=metadata.runtime_image_identity,
        image_source_commit=metadata.runtime_image_source_commit,
        previous=authority._binding(first),
        replacement=authority._binding(second),  # type: ignore[arg-type]
        asserted_at=NOW,
    )
    original_revoke = DirectoryRegionTalkTaskAuthority._revoke_certificate_binding
    failed = False

    def crash_once(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        nonlocal failed
        if not failed:
            failed = True
            raise RuntimeError("crash after durable revocation mailbox")
        return original_revoke(self, *args, **kwargs)

    monkeypatch.setattr(
        DirectoryRegionTalkTaskAuthority,
        "_revoke_certificate_binding",
        crash_once,
    )
    with pytest.raises(RuntimeError, match="crash after durable"):
        authority.activate(activation)
    assert authority.batch(master_instance_id=MASTER.master_instance_id, epoch=MASTER.epoch).revocations
    monkeypatch.setattr(
        DirectoryRegionTalkTaskAuthority,
        "_revoke_certificate_binding",
        original_revoke,
    )
    authority.activate(activation)
    assert len(broker.revoked) == 1
    assert authority.active_binding(metadata.task_run_id).generation == 2


def test_expired_unactivated_generation_is_purged_and_never_reissued(
    tmp_path: Path,
) -> None:
    authority, _broker = _authority(tmp_path)
    metadata = _metadata()
    command = authority.prepare(
        metadata, task_token="t" * 48, source_sha256="a" * 64
    )
    first_receipt = _register(
        authority,
        command,
        credential_id=UUID("66666666-6666-4666-8666-666666666666"),
    )
    first = authority.await_access(metadata, command)
    request = RegionTalkCredentialRefreshRequest(
        request_id=metadata.request_id,
        task_run_id=metadata.task_run_id,
        master_instance_id=MASTER.master_instance_id,
        epoch=MASTER.epoch,
        source_sha256="a" * 64,
        image_identity=metadata.runtime_image_identity,
        image_source_commit=metadata.runtime_image_source_commit,
        previous=authority._binding(first),
        requested_at=NOW + timedelta(minutes=3),
    )
    results: list[object] = []
    thread = threading.Thread(target=lambda: results.append(authority.refresh(request)))
    thread.start()
    deadline = time.monotonic() + 1
    replacement_command = None
    while time.monotonic() < deadline:
        batch = authority.batch(
            master_instance_id=MASTER.master_instance_id, epoch=MASTER.epoch
        )
        if batch.commands and batch.commands[0].generation == 2:
            replacement_command = batch.commands[0]
            break
        time.sleep(0.01)
    assert replacement_command is not None
    _register(
        authority,
        replacement_command,
        credential_id=UUID("77777777-7777-4777-8777-777777777777"),
        expires_at=NOW + timedelta(minutes=7),
    )
    thread.join(timeout=2)
    assert len(results) == 1
    replacement = results[0]
    authority.clock = lambda: NOW + timedelta(minutes=8)

    batch = authority.batch(
        master_instance_id=MASTER.master_instance_id, epoch=MASTER.epoch
    )
    assert batch.commands == ()
    assert not authority._access_path(metadata.task_run_id, 2).exists()
    assert not authority._registration_path(metadata.task_run_id, 2).exists()
    assert first_receipt.generation == 1 and replacement.generation == 2  # type: ignore[union-attr]
    with pytest.raises(
        RegionTalkAssemblyUnavailable,
        match="REGION_TALK_CREDENTIAL_REPLACEMENT_EXPIRED",
    ):
        authority.refresh(request)


def test_terminal_callback_resolves_current_active_epoch_and_fences_stale_run(
    tmp_path: Path,
) -> None:
    ledger = ControlLedger(
        tmp_path / "control.sqlite3", clock=SimpleNamespace(now=lambda: NOW)
    )
    identity = {
        "run_id": str(MASTER.run_id),
        "attempt_id": str(MASTER.attempt_id),
        "service_instance_id": "region-talk-master-service",
        "master_instance_id": str(MASTER.master_instance_id),
        "epoch": 1,
    }
    ledger.ensure_operation(
        operation_id="region-talk-active-master",
        idempotency_key="region-talk-active-master",
        operation_kind="ensure_master",
        intent={"source": "test"},
        initial_state="READY",
        identity=identity,
    )
    ledger.record_attempt(
        attempt_id=str(MASTER.attempt_id),
        run_id=str(MASTER.run_id),
        operation_id="region-talk-active-master",
        source_identity="source",
        source_version="git:" + "a" * 40,
        service_instance_id="region-talk-master-service",
        master_instance_id=str(MASTER.master_instance_id),
        epoch=1,
        state="RUNNING",
    )
    assert ledger.allocate_epoch("postgres-master") == 1
    ledger.activate_service_operation(
        operation_id="region-talk-active-master",
        expected_state="READY",
        service_instance_id="region-talk-master-service",
        service_kind="postgres-master",
        run_id=str(MASTER.run_id),
        attempt_id=str(MASTER.attempt_id),
        master_instance_id=str(MASTER.master_instance_id),
        epoch=1,
        endpoint="tunnel://127.0.0.1:25432",
        protocol="postgresql+tls",
        tls_fingerprint="sha256:" + "b" * 64,
        capabilities=("sql",),
        canonical_revision=1,
        schema_version="1",
        lease_until=NOW + timedelta(minutes=10),
        latest_event_id="active",
    )
    metadata = _metadata()
    active = MASTER.model_copy(update={"epoch": 1})
    snapshot = SimpleNamespace(
        request=SimpleNamespace(request_id=metadata.request_id),
        task_run_id=metadata.task_run_id,
        master=active,
        source_sha256="a" * 64,
        state=SimpleNamespace(value="RUNNING"),
    )

    class _Store:
        def __init__(self) -> None:
            self.fenced = 0
            self.terminals = 0

        def expire_and_fence(self, **_kwargs):  # type: ignore[no-untyped-def]
            self.fenced += 1
            return (metadata.request_id,)

        def record_terminal(self, _receipt):  # type: ignore[no-untyped-def]
            self.terminals += 1
            raise AssertionError("stale terminal must not be recorded")

    store = _Store()
    coordinator = SimpleNamespace(
        store=store,
        status=lambda _request_id=None: snapshot,
    )
    refreshes: list[object] = []
    activations: list[object] = []
    authority = SimpleNamespace(
        validate_token=lambda task_id, supplied: {
            "request_id": str(metadata.request_id),
            "source_sha256": "a" * 64,
        }
        if task_id == metadata.task_run_id and supplied == "private-token"
        else (_ for _ in ()).throw(RuntimeError("bad token")),
        refresh=lambda value: refreshes.append(value) or object(),
        private_access_bytes=lambda _value: b'{"private":"capability"}',
        activate=lambda value: activations.append(value),
    )
    app = create_app(
        ControlPlaneSettings(ledger_path=ledger.path),
        ledger=ledger,
        region_talk_coordinator=coordinator,  # type: ignore[arg-type]
        region_talk_task_authority=authority,  # type: ignore[arg-type]
    )
    body = {
        "schema_version": "region-talk-terminal-receipt.v1",
        "request_id": str(metadata.request_id),
        "task_run_id": str(metadata.task_run_id),
        "master_instance_id": str(MASTER.master_instance_id),
        "epoch": 2,
        "status": "SUCCEEDED",
        "cycles_completed": 1,
        "rows_observed": 10,
        "rows_changed": 10,
        "queue_revision": None,
        "aggregate_receipt_sha256": "c" * 64,
        "completed_at": (NOW + timedelta(minutes=1)).isoformat(),
        "publication_dispatch": False,
    }
    response = TestClient(app).post(
        "/internal/region-talk-pipeline/terminal",
        json=body,
        headers={"Authorization": "Bearer private-token"},
    )
    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "active_epoch_mismatch"
    assert store.fenced == 1 and store.terminals == 0

    previous = {
        "credential_id": "66666666-6666-4666-8666-666666666666",
        "generation": 1,
        "command_sha256": "b" * 64,
        "task_token_sha256": "c" * 64,
        "expires_at": (NOW + timedelta(minutes=4)).isoformat(),
        "ssh_certificate_serial": 7001,
    }
    refresh_body = {
        "schema_version": "region-talk-credential-refresh.v1",
        "request_id": str(metadata.request_id),
        "task_run_id": str(metadata.task_run_id),
        "master_instance_id": str(MASTER.master_instance_id),
        "epoch": 1,
        "source_sha256": "a" * 64,
        "image_identity": metadata.runtime_image_identity,
        "image_source_commit": metadata.runtime_image_source_commit,
        "previous": previous,
        "requested_at": NOW.isoformat(),
        "publication_dispatch": False,
    }
    response = TestClient(app).post(
        "/internal/region-talk-pipeline/access/refresh",
        json=refresh_body,
        headers={"Authorization": "Bearer private-token"},
    )
    assert response.status_code == 200
    assert response.json() == {"private": "capability"}
    assert response.headers["cache-control"] == "no-store, max-age=0"
    assert response.headers["pragma"] == "no-cache"
    assert len(refreshes) == 1

    replacement = {
        **previous,
        "credential_id": "77777777-7777-4777-8777-777777777777",
        "generation": 2,
        "expires_at": (NOW + timedelta(minutes=7)).isoformat(),
        "ssh_certificate_serial": 7002,
    }
    activation_body = {
        "schema_version": "region-talk-credential-activation.v1",
        "request_id": str(metadata.request_id),
        "task_run_id": str(metadata.task_run_id),
        "master_instance_id": str(MASTER.master_instance_id),
        "epoch": 1,
        "source_sha256": "a" * 64,
        "image_identity": metadata.runtime_image_identity,
        "image_source_commit": metadata.runtime_image_source_commit,
        "previous": previous,
        "replacement": replacement,
        "asserted_at": NOW.isoformat(),
        "publication_dispatch": False,
    }
    response = TestClient(app).post(
        "/internal/region-talk-pipeline/access/activate",
        json=activation_body,
        headers={"Authorization": "Bearer private-token"},
    )
    assert response.status_code == 200
    assert response.json()["generation"] == 2
    assert len(activations) == 1


def test_exact_terminal_http_response_loss_replay_is_accepted(tmp_path: Path) -> None:
    metadata = _metadata()
    receipt = RegionTalkTerminalReceipt(
        request_id=metadata.request_id,
        task_run_id=metadata.task_run_id,
        master_instance_id=metadata.master.master_instance_id,
        epoch=metadata.master.epoch,
        status="SUCCEEDED",
        cycles_completed=1,
        rows_observed=10,
        rows_changed=10,
        aggregate_receipt_sha256="c" * 64,
        completed_at=NOW,
    )
    receipt_sha = hashlib.sha256(
        canonical_json_bytes(receipt.model_dump(mode="json"))
    ).hexdigest()
    snapshot = SimpleNamespace(
        request=SimpleNamespace(request_id=metadata.request_id),
        task_run_id=metadata.task_run_id,
        master=metadata.master,
        source_sha256="a" * 64,
        state=SimpleNamespace(value="TERMINAL"),
        terminal_status=SimpleNamespace(value="SUCCEEDED"),
        terminal_receipt_sha256=receipt_sha,
    )
    coordinator = SimpleNamespace(status=lambda _request_id=None: snapshot)
    authority = SimpleNamespace(
        validate_token=lambda task_id, supplied: {
            "request_id": str(metadata.request_id),
            "source_sha256": "a" * 64,
        }
        if task_id == metadata.task_run_id and supplied == "private-token"
        else (_ for _ in ()).throw(
            RegionTalkAssemblyUnavailable("REGION_TALK_TASK_TOKEN_INVALID")
        )
    )
    app = create_app(
        ControlPlaneSettings(ledger_path=tmp_path / "control.sqlite3"),
        region_talk_coordinator=coordinator,  # type: ignore[arg-type]
        region_talk_task_authority=authority,  # type: ignore[arg-type]
    )

    response = TestClient(app).post(
        "/internal/region-talk-pipeline/terminal",
        json=receipt.model_dump(mode="json"),
        headers={"Authorization": "Bearer private-token"},
    )

    assert response.status_code == 200
    assert response.json() == {"accepted": True, "state": "TERMINAL"}
