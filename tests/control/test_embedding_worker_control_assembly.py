from __future__ import annotations

import time
from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

from fastapi.testclient import TestClient

import my_data_hub.control_plane.app as app_module
from my_data_hub.control_plane.app import ControlPlaneSettings, create_app
from my_data_hub.control_plane.ledger import ControlLedger
from my_data_hub.control_plane.runtime import ProductionRuntimeBuild
from my_data_hub.providers.kaggle import (
    EffectOutcome,
    MutationAction,
    ProviderEffectIntent,
    ProviderEffectReceipt,
    TaskResourceClaim,
)
from my_data_hub.providers.kaggle.adapter import mapping_sha256
from my_data_hub.providers.models import ControlClass, ProviderFingerprint, ProviderKind


class _AttestationLauncher:
    ready = True

    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def attest_runtime_source(self, **kwargs: object) -> None:
        if kwargs["task_token"] != "task-token" or kwargs["source_sha256"] != "a" * 64:
            raise ValueError("binding differs")
        if kwargs["image_identity"] != "image@sha256:" + "b" * 64:
            raise ValueError("binding differs")
        if kwargs["image_source_commit"] != "c" * 40 or kwargs["epoch"] != 7:
            raise ValueError("binding differs")
        self.calls.append(kwargs)


def test_embedding_attestation_route_enforces_exact_task_binding_before_worker_access(tmp_path) -> None:  # type: ignore[no-untyped-def]
    launcher = _AttestationLauncher()
    app = create_app(
        ControlPlaneSettings(ledger_path=tmp_path / "control.sqlite3"),
        embedding_direct_plane_launcher=launcher,  # type: ignore[arg-type]
    )
    client = TestClient(app)
    body = {
        "task_run_id": str(UUID("22222222-2222-4222-8222-222222222222")),
        "source_sha256": "a" * 64,
        "image_identity": "image@sha256:" + "b" * 64,
        "image_source_commit": "c" * 40,
        "epoch": 7,
    }
    accepted = client.post(
        "/internal/runtime/events/embedding-worker-attestation",
        json=body,
        headers={"Authorization": "Bearer task-token"},
    )
    assert accepted.status_code == 200 and accepted.json() == {"accepted": True}
    assert len(launcher.calls) == 1

    assert client.post(
        "/internal/runtime/events/embedding-worker-attestation",
        json=body,
        headers={"Authorization": "Bearer wrong"},
    ).status_code == 403
    assert client.post(
        "/internal/runtime/events/embedding-worker-attestation",
        json={**body, "unexpected": "field"},
        headers={"Authorization": "Bearer task-token"},
    ).status_code == 422
    assert len(launcher.calls) == 1


def _persist_exact_asset_effect(
    ledger: ControlLedger,
    provider_ref: str,
    *,
    dataset_files: dict[str, bytes],
    content_tree_sha256: str | None = None,
    receipt_fingerprint: ProviderFingerprint | None = None,
) -> None:
    operation_id = uuid4()
    task_id = uuid4()
    key = f"{operation_id}:ensure_dataset"
    effect_id = uuid5(NAMESPACE_URL, key)
    now = datetime.now(UTC)
    content_identity = content_tree_sha256 or mapping_sha256(dataset_files)
    intent = ProviderEffectIntent.create(
        operation_id=operation_id,
        effect_id=effect_id,
        idempotency_key=key,
        task_id=task_id,
        action=MutationAction.CREATE_DATASET,
        provider_ref=provider_ref,
        arguments={
            "content_tree_sha256": content_identity,
            "control_class": "orchestrator_protected",
            "disposable": False,
        },
        requested_at=now,
    )
    fingerprint = ProviderFingerprint(value="e" * 64)
    receipt = ProviderEffectReceipt(
        operation_id=operation_id,
        effect_id=effect_id,
        action=MutationAction.CREATE_DATASET,
        provider_ref=provider_ref,
        outcome=EffectOutcome.APPLIED,
        attempts=1,
        observed_fingerprint=receipt_fingerprint or fingerprint,
        provider_version=3,
        observed_at=now,
        detail_code="private_dataset_exact_readback",
    )
    claim = TaskResourceClaim.create(
        task_id=task_id,
        effect_id=effect_id,
        provider_ref=provider_ref,
        kind=ProviderKind.DATASET,
        control_class=ControlClass.ORCHESTRATOR_PROTECTED,
        disposable=False,
        fingerprint=fingerprint,
        provider_version=3,
        registered_at=now,
    )
    ledger.persist_provider_effect_intent(intent.model_dump(mode="json"))
    ledger.persist_provider_effect_receipt(str(effect_id), receipt.model_dump(mode="json"))
    ledger.persist_provider_resource_claim(claim.model_dump(mode="json"))


def test_cold_start_lazily_assembles_same_adapter_after_exact_asset_effect(tmp_path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    ledger = ControlLedger(tmp_path / "cold.sqlite3")
    adapter = object()
    dataset_files = {"bundle-manifest.json": b'{"source_commit":"current"}', "worker.whl": b"wheel"}
    settings = SimpleNamespace(
        assets=SimpleNamespace(dataset_ref="owner/master-assets", dataset_files=dataset_files)
    )
    monkeypatch.setattr(
        app_module,
        "build_production_runtime",
        lambda *_args, **_kwargs: ProductionRuntimeBuild(
            master=SimpleNamespace(
                reconcile_requested_once=lambda: None,
                reconcile_acceptance_once=lambda: None,
                reconcile_status_cleanup_once=lambda: None,
                coordinator=SimpleNamespace(tunnel_authority=None),
                settings=settings,
                ledger=ledger,
            ),  # type: ignore[arg-type]
            provider_status="available", provider_adapter=adapter,  # type: ignore[arg-type]
        ),
    )
    assembled = SimpleNamespace(ready=True, reconcile_timeouts=lambda: ())
    calls: list[tuple[object, str | None]] = []

    def build(provider: object, **kwargs: object):
        calls.append((provider, kwargs.get("runtime_dataset_exact_ref")))
        return assembled, SimpleNamespace()

    monkeypatch.setattr(app_module, "build_embedding_production_assembly", build)
    app = create_app(
        ControlPlaneSettings(ledger_path=ledger.path, master_runtime=settings),  # type: ignore[arg-type]
        ledger=ledger,
    )
    assert app.state.embedding_direct_plane_launcher is None and calls == []
    _persist_exact_asset_effect(ledger, "owner/master-assets", dataset_files=dataset_files)
    with TestClient(app):
        deadline = time.monotonic() + 6.5
        while app.state.embedding_direct_plane_launcher is None and time.monotonic() < deadline:
            time.sleep(0.05)
    assert app.state.embedding_direct_plane_launcher is assembled
    assert calls == [(adapter, "owner/master-assets/3")]


def test_cold_start_rejects_stale_asset_content_claim(tmp_path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    ledger = ControlLedger(tmp_path / "stale.sqlite3")
    adapter = object()
    dataset_files = {"bundle-manifest.json": b'{"source_commit":"current"}', "worker.whl": b"wheel"}
    settings = SimpleNamespace(
        assets=SimpleNamespace(dataset_ref="owner/master-assets", dataset_files=dataset_files)
    )
    monkeypatch.setattr(
        app_module,
        "build_production_runtime",
        lambda *_args, **_kwargs: ProductionRuntimeBuild(
            master=SimpleNamespace(
                reconcile_requested_once=lambda: None,
                reconcile_acceptance_once=lambda: None,
                reconcile_status_cleanup_once=lambda: None,
                coordinator=SimpleNamespace(tunnel_authority=None),
                settings=settings,
                ledger=ledger,
            ),  # type: ignore[arg-type]
            provider_status="available",
            provider_adapter=adapter,  # type: ignore[arg-type]
        ),
    )
    calls: list[str] = []

    def build(*_args: object, **_kwargs: object):
        calls.append("called")
        return SimpleNamespace(ready=True, reconcile_timeouts=lambda: ()), SimpleNamespace()

    monkeypatch.setattr(app_module, "build_embedding_production_assembly", build)
    app = create_app(
        ControlPlaneSettings(ledger_path=ledger.path, master_runtime=settings),  # type: ignore[arg-type]
        ledger=ledger,
    )
    _persist_exact_asset_effect(
        ledger,
        "owner/master-assets",
        dataset_files=dataset_files,
        content_tree_sha256="d" * 64,
    )
    with TestClient(app):
        time.sleep(0.1)
    assert app.state.embedding_direct_plane_launcher is None
    assert calls == []


def test_cold_start_rejects_claim_receipt_fingerprint_mismatch(tmp_path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    ledger = ControlLedger(tmp_path / "fingerprint.sqlite3")
    adapter = object()
    dataset_files = {"bundle-manifest.json": b'{"source_commit":"current"}', "worker.whl": b"wheel"}
    settings = SimpleNamespace(
        assets=SimpleNamespace(dataset_ref="owner/master-assets", dataset_files=dataset_files)
    )
    monkeypatch.setattr(
        app_module,
        "build_production_runtime",
        lambda *_args, **_kwargs: ProductionRuntimeBuild(
            master=SimpleNamespace(
                reconcile_requested_once=lambda: None,
                reconcile_acceptance_once=lambda: None,
                reconcile_status_cleanup_once=lambda: None,
                coordinator=SimpleNamespace(tunnel_authority=None),
                settings=settings,
                ledger=ledger,
            ),  # type: ignore[arg-type]
            provider_status="available",
            provider_adapter=adapter,  # type: ignore[arg-type]
        ),
    )
    calls: list[str] = []

    def build(*_args: object, **_kwargs: object):
        calls.append("called")
        return SimpleNamespace(ready=True, reconcile_timeouts=lambda: ()), SimpleNamespace()

    monkeypatch.setattr(app_module, "build_embedding_production_assembly", build)
    app = create_app(
        ControlPlaneSettings(ledger_path=ledger.path, master_runtime=settings),  # type: ignore[arg-type]
        ledger=ledger,
    )
    _persist_exact_asset_effect(
        ledger,
        "owner/master-assets",
        dataset_files=dataset_files,
        receipt_fingerprint=ProviderFingerprint(value="f" * 64),
    )
    with TestClient(app):
        time.sleep(0.1)
    assert app.state.embedding_direct_plane_launcher is None
    assert calls == []
