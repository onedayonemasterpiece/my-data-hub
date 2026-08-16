from __future__ import annotations

import base64
import hashlib
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from uuid import NAMESPACE_URL, uuid4, uuid5

import pytest
from fastapi.testclient import TestClient

import my_data_hub.control_plane.app as app_module
from my_data_hub.control_plane.adapters import LedgerControlReader
from my_data_hub.control_plane.app import (
    ControlPlaneSettings,
    _validate_blogger_replay_source,
    create_app,
)
from my_data_hub.control_plane.ledger import ControlLedger
from my_data_hub.control_plane.runtime import (
    ControlPlaneMasterRuntime,
    MasterRuntimeSettings,
    ProductionRuntimeBuild,
    TunnelCertificate,
    build_production_runtime,
)
from my_data_hub.embeddings.production import EmbeddingProductionRequest
from my_data_hub.orchestrator.master import FakeKaggleRuntime, MasterCoordinator
from my_data_hub.providers.kaggle import (
    ControlLedgerKaggleJournal,
    EffectOutcome,
    KaggleMasterLaunchAssets,
    KaggleMasterRuntimeProvider,
    KaggleProviderAdapter,
    MutationAction,
    ProviderEffectIntent,
    ProviderEffectReceipt,
    TaskResourceClaim,
)
from my_data_hub.providers.models import ControlClass, ProviderFingerprint, ProviderKind
from my_data_hub.runtime_sdk import RuntimeEvent, RuntimeEventType
from my_data_hub.workloads.bloggers.importer import batch_identity
from my_data_hub.workloads.bloggers.master_stage import (
    BLOGGER_REPLAY_STAGE_SCHEMA,
    BloggerDuplicateDecision,
    BloggerDuplicateResolutionEnvelope,
    BloggerDuplicateReviewGroup,
    BloggerDuplicateReviewInputs,
    BloggerDuplicateReviewMember,
    BloggerImportStageReceipt,
    BloggerMigrationRequest,
    BloggerQuarantineReceipt,
)

TOKEN = "a" * 64


def test_duplicate_replay_requires_terminal_quarantine_and_exact_source_authorization() -> None:
    snapshot_at = datetime(2026, 8, 9, tzinfo=UTC)
    source = BloggerMigrationRequest(
        request_id=uuid4(),
        operation_id=uuid4(),
        project_id=uuid4(),
        snapshot_at=snapshot_at,
        expected_rows=266,
        source_revision="a" * 40,
    )
    authorized_at = datetime(2026, 8, 11, tzinfo=UTC)
    authorizer = "owner-review:test"
    canonical_actor_id = uuid4()
    envelope = BloggerDuplicateResolutionEnvelope(
        authorization_id=uuid4(),
        authorized_by=authorizer,
        authorized_at=authorized_at,
        source_request_id=source.request_id,
        source_operation_id=source.operation_id,
        source_request_sha256=source.request_sha256,
        export_batch_id=batch_identity(snapshot_at, 266),
        project_id=source.project_id,
        snapshot_at=snapshot_at,
        expected_rows=source.expected_rows,
        source_revision=source.source_revision,
        decisions=(
            BloggerDuplicateDecision(
                identity_sha256="b" * 64,
                canonical_record_id="record-1",
                canonical_actor_id=canonical_actor_id,
                member_record_ids=("record-1", "record-2"),
                decided_by=authorizer,
                reason="The exact reviewed evidence establishes one person.",
            ),
        ),
    )
    replay = BloggerMigrationRequest(
        schema_version=BLOGGER_REPLAY_STAGE_SCHEMA,
        request_id=uuid4(),
        operation_id=uuid4(),
        project_id=source.project_id,
        snapshot_at=snapshot_at,
        expected_rows=266,
        source_revision=source.source_revision,
        replay_of_request_id=source.request_id,
        duplicate_resolution=envelope,
    )
    quarantine = BloggerQuarantineReceipt(
        request_id=source.request_id,
        operation_id=source.operation_id,
        request_sha256=source.request_sha256,
        master_instance_id=uuid4(),
        run_id="run-1",
        attempt_id="attempt-1",
        epoch=1,
        export_batch_id=batch_identity(snapshot_at, 266),
        row_count=266,
        raw_count=266,
        dispositioned_count=266,
        undispositioned_count=0,
        quarantined_count=2,
        logical_sha256="d" * 64,
        record_id_set_sha256="e" * 64,
        canonical_outcome_sha256="f" * 64,
        duplicate_group_count=1,
        duplicate_groups_pending=1,
        duplicate_review_inputs=BloggerDuplicateReviewInputs(
            groups=(
                BloggerDuplicateReviewGroup(
                    identity_sha256="b" * 64,
                    members=(
                        BloggerDuplicateReviewMember(record_id="record-1", projected_actor_id=canonical_actor_id),
                        BloggerDuplicateReviewMember(record_id="record-2", projected_actor_id=uuid4()),
                    ),
                ),
            )
        ),
    )
    record = {
        "request_id": str(source.request_id),
        "operation_id": str(source.operation_id),
        "request_sha256": source.request_sha256,
        "request": source.model_dump(mode="json"),
        "state": "FAILED",
        "failure_code": "BloggerMigrationQuarantined",
        "quarantine_receipt": quarantine.model_dump(mode="json"),
        "quarantine_receipt_sha256": quarantine.receipt_sha256,
        "created_at": "2026-08-10T00:00:00Z",
        "updated_at": "2026-08-10T00:01:00Z",
    }
    assert _validate_blogger_replay_source(replay, record, now=authorized_at) is None
    assert (
        _validate_blogger_replay_source(replay, {**record, "failure_code": "RuntimeError"}, now=authorized_at)
        == "blogger_replay_source_invalid"
    )
    assert (
        _validate_blogger_replay_source(replay, {**record, "request_sha256": "c" * 64}, now=authorized_at)
        == "blogger_replay_source_invalid"
    )
    assert (
        _validate_blogger_replay_source(replay, {**record, "updated_at": "2026-08-12T00:00:00Z"}, now=authorized_at)
        == "blogger_replay_binding_invalid"
    )
    tampered_envelope = envelope.model_copy(
        update={"decisions": (envelope.decisions[0].model_copy(update={"canonical_actor_id": uuid4()}),)}
    )
    tampered_replay = replay.model_copy(update={"duplicate_resolution": tampered_envelope})
    assert (
        _validate_blogger_replay_source(tampered_replay, record, now=authorized_at) == "blogger_replay_binding_invalid"
    )


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
        "MY_DATA_HUB_KAGGLE_CHECKPOINT_VERIFIER_REF": "owner/verifier",
        "MY_DATA_HUB_KAGGLE_CHECKPOINT_VERIFIER_SOURCE_FILE": "verifier.ipynb",
        "MY_DATA_HUB_MASTER_TUNNEL_GATEWAY_HOST": "gateway.example.test",
        "MY_DATA_HUB_MASTER_TUNNEL_GATEWAY_PORT": "22",
        "MY_DATA_HUB_MASTER_TUNNEL_GATEWAY_USER": "mdh_tunnel",
        "MY_DATA_HUB_MASTER_TUNNEL_REMOTE_PORT": "25432",
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
        dataset_files={
            "launch.txt": b"exact",
            "checkpoint-verifier.ipynb": b"{}",
            "postgresql-18-runtime.bundle": b"fake-postgresql-18-runtime",
            "postgresql-18-runtime.json": b"""{"archive_sha256":"63a988449f3d37c9c9fd2658b14f9254918e0b0f8ac600f9b98f15ede09e912f","build_recipe_sha256":"3fbcf52450dd44e3eb0eb7b826ebdb84a4293fbc54b713408083f10b44964d61","builder_image":"ubuntu:22.04@sha256:3b06811b2afd352be909dd088a004166d665dc76d38b13eada33522a9d915c6f","pgvector_source_sha256":"10bf9938906e5d643bbc4a7eea104b6f57ba4898e5b76b20e60484ea1d5a7f8f","pgvector_source_url":"https://github.com/pgvector/pgvector/archive/refs/tags/v0.8.6.tar.gz","pgvector_version":"0.8.6","platform":"linux-x86_64","postgresql_source_sha256":"81a81ec695fb0c7901407defaa1d2f7973617154cf27ba74e3a7ab8e64436094","postgresql_source_url":"https://ftp.postgresql.org/pub/source/v18.4/postgresql-18.4.tar.bz2","postgresql_version":"18.4","schema_version":"my-data-hub-postgresql-runtime.v1"}""",
            "tunnel-known-hosts": b"|1|aaaa|bbbb ssh-ed25519 AAAA\n",
        },
        notebook_source=b'{"cells":[],"metadata":{},"nbformat":4,"nbformat_minor":5}',
        callback_url="https://mcp-datahub.kenigevents.ru/internal/runtime/events",
        checkpoint_verifier_ref="owner/checkpoint-verifier",
        checkpoint_verifier_source_file="checkpoint-verifier.ipynb",
        checkpoint_probe_relations=("hub.canonical_state",),
        tunnel_gateway_host="gateway.example.test",
        tunnel_gateway_port=22,
        tunnel_gateway_user="mdh_tunnel",
        tunnel_remote_port=25432,
    )


def runtime(ledger: ControlLedger, provider: FakeKaggleRuntime) -> ControlPlaneMasterRuntime:
    return ControlPlaneMasterRuntime(
        ledger,
        MasterCoordinator(ledger, provider),
        MasterRuntimeSettings(assets()),
    )


def test_production_builder_constructs_single_adapter_journal_and_bridge(monkeypatch, tmp_path: Path) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("KAGGLE_API_TOKEN", "modern-token-present")
    key = tmp_path / "checkpoint-broker.key"
    key.write_bytes(b"k" * 32)
    key.chmod(0o600)
    monkeypatch.setenv("MY_DATA_HUB_CHECKPOINT_UPLOAD_BROKER_KEY_FILE", str(key))
    ledger = ControlLedger(tmp_path / "builder.sqlite3")
    adapter = object()
    seen = []
    built = build_production_runtime(
        ledger,
        MasterRuntimeSettings(assets()),
        adapter_factory=lambda journal: seen.append(journal) or adapter,  # type: ignore[arg-type,return-value]
    )
    assert built.provider_status == "available"
    assert built.master is not None
    assert built.checkpoint_broker is not None
    assert built.provider_adapter is adapter
    assert len(seen) == 1


def test_checkpoint_verifier_factory_uses_exact_verified_master_asset_claim(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("KAGGLE_API_TOKEN", "modern-token-present")
    key = tmp_path / "checkpoint-broker.key"
    key.write_bytes(b"k" * 32)
    key.chmod(0o600)
    monkeypatch.setenv("MY_DATA_HUB_CHECKPOINT_UPLOAD_BROKER_KEY_FILE", str(key))
    base = assets()
    wheel_name = "my_data_hub-0.1.0-py3-none-any.whl"
    launch = replace(base, dataset_files={**base.dataset_files, wheel_name: b"exact-wheel"})
    ledger = ControlLedger(tmp_path / "verified-assets.sqlite3")
    journal = ControlLedgerKaggleJournal(ledger)
    operation_id = uuid4()
    task_id = uuid4()
    effect_key = f"{operation_id}:ensure_dataset"
    effect_id = uuid5(NAMESPACE_URL, effect_key)
    fingerprint = ProviderFingerprint(value="a" * 64)
    intent = ProviderEffectIntent.create(
        operation_id=operation_id,
        effect_id=effect_id,
        idempotency_key=effect_key,
        task_id=task_id,
        action=MutationAction.CREATE_DATASET,
        provider_ref=launch.dataset_ref,
        arguments={
            "content_tree_sha256": KaggleMasterRuntimeProvider._mapping_sha(launch.dataset_files),
            "control_class": ControlClass.ORCHESTRATOR_PROTECTED.value,
            "disposable": False,
        },
        requested_at=datetime(2026, 8, 12, tzinfo=UTC),
    )
    journal.persist_intent(intent)
    journal.persist_receipt(
        ProviderEffectReceipt(
            operation_id=operation_id,
            effect_id=effect_id,
            action=MutationAction.CREATE_DATASET,
            provider_ref=launch.dataset_ref,
            outcome=EffectOutcome.APPLIED,
            attempts=1,
            observed_fingerprint=fingerprint,
            provider_version=7,
            observed_at=datetime(2026, 8, 12, tzinfo=UTC),
            detail_code="dataset_created_private",
        )
    )
    journal.persist_resource_claim(
        TaskResourceClaim.create(
            task_id=task_id,
            effect_id=effect_id,
            provider_ref=launch.dataset_ref,
            kind=ProviderKind.DATASET,
            control_class=ControlClass.ORCHESTRATOR_PROTECTED,
            disposable=False,
            fingerprint=fingerprint,
            provider_version=7,
            registered_at=datetime(2026, 8, 12, tzinfo=UTC),
        )
    )
    adapter = object()
    built = build_production_runtime(
        ledger,
        MasterRuntimeSettings(launch),
        adapter_factory=lambda _journal: adapter,  # type: ignore[arg-type,return-value]
    )
    assert built.checkpoint_broker is not None
    verifier = built.checkpoint_broker.restore_verifier_factory(uuid4(), uuid4())  # type: ignore[misc]
    contract = verifier.assets.execution_contract()
    assert contract["runtime_dataset_exact_ref"] == "owner/master-launch/7"
    assert contract["runtime_image_identity"] == launch.runtime_image_identity
    assert contract["wheel_relative_path"] == wheel_name
    assert contract["wheel_sha256"] == hashlib.sha256(b"exact-wheel").hexdigest()


def test_production_builder_accepts_central_legacy_kaggle_credentials(monkeypatch, tmp_path: Path) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.delenv("KAGGLE_API_TOKEN", raising=False)
    monkeypatch.setenv("KAGGLE_CONFIG_DIR", str(tmp_path / "missing-sdk-config"))
    monkeypatch.setenv("KAGGLE_USERNAME", "automation-owner")
    monkeypatch.setenv("KAGGLE_KEY", "k" * 32)
    key = tmp_path / "checkpoint-broker.key"
    key.write_bytes(b"k" * 32)
    key.chmod(0o600)
    monkeypatch.setenv("MY_DATA_HUB_CHECKPOINT_UPLOAD_BROKER_KEY_FILE", str(key))
    ledger = ControlLedger(tmp_path / "legacy-builder.sqlite3")
    adapter = object()
    built = build_production_runtime(
        ledger,
        MasterRuntimeSettings(assets()),
        adapter_factory=lambda _journal: adapter,  # type: ignore[arg-type,return-value]
    )
    assert built.provider_status == "available"
    assert built.provider_adapter is adapter
    assert built.master is not None
    assert built.checkpoint_broker is not None


def test_provider_only_builder_constructs_one_adapter_without_master_assets_or_checkpoint(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("KAGGLE_USERNAME", "automation-owner")
    monkeypatch.setenv("KAGGLE_KEY", "k" * 32)
    monkeypatch.delenv("KAGGLE_API_TOKEN", raising=False)
    ledger = ControlLedger(tmp_path / "provider-only.sqlite3")
    adapter = object()
    journals = []

    built = build_production_runtime(
        ledger,
        None,
        provider_only=True,
        adapter_factory=lambda journal: journals.append(journal) or adapter,  # type: ignore[arg-type,return-value]
    )

    assert built.provider_status == "available"
    assert built.provider_adapter is adapter
    assert built.master is None
    assert built.checkpoint_broker is None
    assert built.session_registrar is None
    assert len(journals) == 1


def test_provider_only_control_settings_reject_master_or_acceptance_coupling(tmp_path: Path) -> None:
    with pytest.raises(Exception, match="provider-only"):
        ControlPlaneSettings(
            ledger_path=tmp_path / "invalid.sqlite3",
            provider_only_mode=True,
            operator_credentials_enabled=True,
            provider_gateway_enabled=True,
            master_runtime=MasterRuntimeSettings(assets()),
        )
    with pytest.raises(Exception, match="provider-only"):
        ControlPlaneSettings(
            ledger_path=tmp_path / "invalid-acceptance.sqlite3",
            provider_only_mode=True,
            operator_credentials_enabled=True,
            provider_gateway_enabled=True,
            acceptance_scenarios_enabled=True,
        )


def test_production_master_ensure_reports_missing_broker_configuration_before_provider_mutation(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    ledger = ControlLedger(tmp_path / "checkpoint-path-blocked.sqlite3")
    adapter = object()

    def blocked(*_args, **_kwargs):  # type: ignore[no-untyped-def]
        return ProductionRuntimeBuild(
            master=None,
            provider_status="checkpoint_upload_broker_unavailable",
            provider_adapter=adapter,  # type: ignore[arg-type]
        )

    monkeypatch.setattr(app_module, "build_production_runtime", blocked)
    app = create_app(
        ControlPlaneSettings(
            ledger_path=ledger.path,
            master_runtime=MasterRuntimeSettings(assets()),
        ),
        ledger=ledger,
    )
    response = TestClient(app).post(
        "/control/v1/master/ensure",
        json={"idempotency_key": "checkpoint-upload-not-proven"},
    )

    assert response.status_code == 503
    assert response.json() == {"detail": {"code": "checkpoint_upload_broker_unavailable"}}
    assert ledger.incomplete_operations() == []


def test_production_builder_rejects_partial_legacy_kaggle_credentials(monkeypatch, tmp_path: Path) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.delenv("KAGGLE_API_TOKEN", raising=False)
    monkeypatch.setenv("KAGGLE_CONFIG_DIR", str(tmp_path / "missing-sdk-config"))
    monkeypatch.setenv("KAGGLE_USERNAME", "automation-owner")
    monkeypatch.delenv("KAGGLE_KEY", raising=False)
    built = build_production_runtime(
        ControlLedger(tmp_path / "partial-legacy.sqlite3"),
        MasterRuntimeSettings(assets()),
        adapter_factory=lambda _journal: object(),  # type: ignore[arg-type,return-value]
    )
    assert built.provider_status == "provider_unavailable"
    assert built.master is None


def test_control_provider_gateway_requires_service_auth_and_uses_injected_single_adapter(
    tmp_path: Path,
) -> None:
    path = tmp_path / "provider-gateway.sqlite3"
    ledger = ControlLedger(path)

    class Gateway:
        def __init__(self) -> None:
            self.calls = []

        def invoke(self, tool, arguments, principal):  # type: ignore[no-untyped-def]
            self.calls.append((tool, dict(arguments), principal.subject, principal.token_id))
            return {"provider_ref": arguments["resource_ref"], "outcome": "applied"}

    gateway = Gateway()
    app = create_app(
        ControlPlaneSettings(
            ledger_path=path,
            operator_credentials_enabled=True,
            provider_gateway_enabled=True,
        ),
        ledger=ledger,
        master_runtime=runtime(ledger, FakeKaggleRuntime()),
        provider_gateway=gateway,  # type: ignore[arg-type]
        provider_gateway_token=b"g" * 32,
    )
    client = TestClient(app)
    body = {
        "tool": "provider.resources.delete",
        "arguments": {
            "resource_ref": "owner/disposable",
            "control_class": "mcp_managed",
            "private": True,
            "payload": {},
        },
        "principal": {
            "subject": "owner",
            "client_id": "owner-operator",
            "scopes": ["provider:write"],
            "audience": "mcp",
            "expires_at": int((datetime.now(UTC) + timedelta(minutes=2)).timestamp()),
            "issuer": "https://issuer.example",
            "issued_at": int((datetime.now(UTC) - timedelta(minutes=1)).timestamp()),
            "resource": "https://mcp.example/mcp",
        },
    }
    assert client.post("/internal/mcp-provider/invoke", json=body).status_code == 401
    accepted = client.post(
        "/internal/mcp-provider/invoke",
        json=body,
        headers={"Authorization": "Bearer " + "g" * 32},
    )
    assert accepted.status_code == 200
    assert accepted.json() == {"provider_ref": "owner/disposable", "outcome": "applied"}
    assert gateway.calls == [("provider.resources.delete", body["arguments"], "owner", "internal-provider-gateway")]

    for tool in ("provider.resources.list", "provider.resources.download"):
        routed = client.post(
            "/internal/mcp-provider/invoke",
            json={**body, "tool": tool},
            headers={"Authorization": "Bearer " + "g" * 32},
        )
        assert routed.status_code == 200
    assert [call[0] for call in gateway.calls] == [
        "provider.resources.delete",
        "provider.resources.list",
        "provider.resources.download",
    ]

    secret = {**body, "arguments": {**body["arguments"], "password": "forbidden"}}
    rejected = client.post(
        "/internal/mcp-provider/invoke",
        json=secret,
        headers={"Authorization": "Bearer " + "g" * 32},
    )
    assert rejected.status_code == 422
    assert rejected.json()["detail"]["code"] == "provider_gateway_secret_forbidden"
    assert len(rejected.json()["detail"]["correlation_id"]) == 36
    assert len(gateway.calls) == 3


def test_control_provider_gateway_redacts_raw_adapter_failure_and_emits_correlation(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    path = tmp_path / "provider-gateway-redaction.sqlite3"
    ledger = ControlLedger(path)

    class FailingGateway:
        def invoke(self, *_args):  # type: ignore[no-untyped-def]
            raise RuntimeError("KAGGLE_API_TOKEN=must-not-cross raw provider body")

    app = create_app(
        ControlPlaneSettings(
            ledger_path=path,
            operator_credentials_enabled=True,
            provider_gateway_enabled=True,
        ),
        ledger=ledger,
        master_runtime=runtime(ledger, FakeKaggleRuntime()),
        provider_gateway=FailingGateway(),  # type: ignore[arg-type]
        provider_gateway_token=b"g" * 32,
    )
    now = datetime.now(UTC)
    response = TestClient(app).post(
        "/internal/mcp-provider/invoke",
        headers={"Authorization": "Bearer " + "g" * 32},
        json={
            "tool": "provider.inventory.live",
            "arguments": {"limit": 100},
            "principal": {
                "subject": "owner",
                "client_id": "owner-operator",
                "scopes": ["provider:write"],
                "audience": "mcp",
                "expires_at": int((now + timedelta(minutes=2)).timestamp()),
                "issuer": "https://issuer.example",
                "issued_at": int((now - timedelta(minutes=1)).timestamp()),
                "resource": "https://mcp.example/mcp",
            },
        },
    )
    assert response.status_code == 502
    detail = response.json()["detail"]
    assert detail["code"] == "provider_gateway_internal_failure"
    assert len(detail["correlation_id"]) == 36
    assert "KAGGLE_API_TOKEN" not in response.text
    assert "KAGGLE_API_TOKEN" not in caplog.text
    assert detail["correlation_id"] in caplog.text


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

    class CertificateBroker:
        def __init__(self) -> None:
            self.calls = []

        def issue_public_key(self, **kwargs):  # type: ignore[no-untyped-def]
            self.calls.append(kwargs)
            return TunnelCertificate(
                certificate="ssh-ed25519-cert-v01@openssh.com " + base64.b64encode(b"certificate").decode(),
                serial=17,
                principal="mdh-master-tunnel",
                valid_before=kwargs["valid_before"],
                listen_host="127.0.0.1",
                listen_port=25432,
            )

        def renew(self, **kwargs):  # type: ignore[no-untyped-def]
            self.calls.append(kwargs)
            return SimpleNamespace(lease_until=kwargs["lease_until"])

    certificate_broker = CertificateBroker()
    app = create_app(
        ControlPlaneSettings(ledger_path=path),
        ledger=ledger,
        master_runtime=wired,
        operator_credential_enabled=True,
        tunnel_certificate_broker=certificate_broker,
    )
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
    token = TOKEN
    ledger.store_runtime_token_hash(str(identity["run_id"]), str(identity["attempt_id"]), token)
    callback = TestClient(app).post(
        "/internal/runtime/events",
        content=event.model_dump_json(by_alias=True, exclude_none=True).encode(),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert callback.status_code == 200
    assert TestClient(app).get("/health/ready").json()["master_state"] == "ACTIVE"
    activation = TestClient(app).get(
        f"/internal/runtime/activation/{identity['run_id']}/{identity['attempt_id']}",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert activation.status_code == 200
    assert activation.json()["credential_roles"] == ["reader", "operator"]

    key_blob = b"\x00\x00\x00\x0bssh-ed25519\x00\x00\x00\x20" + b"k" * 32
    public_key = "ssh-ed25519 " + base64.b64encode(key_blob).decode()
    valid_before = datetime.now(UTC) + timedelta(minutes=2)
    lease = TestClient(app).post(
        f"/internal/runtime/tunnel-leases/{identity['run_id']}/{identity['attempt_id']}",
        json={
            "master_instance_id": identity["master_instance_id"],
            "epoch": identity["epoch"],
            "lease_until": valid_before.isoformat(),
        },
        headers={"Authorization": f"Bearer {token}"},
    )
    assert lease.status_code == 200
    assert lease.json()["renewed"] is True
    certificate = TestClient(app).post(
        f"/internal/runtime/tunnel-certificates/{identity['run_id']}/{identity['attempt_id']}",
        json={
            "master_instance_id": identity["master_instance_id"],
            "epoch": identity["epoch"],
            "public_key": public_key,
            "valid_before": valid_before.isoformat(),
        },
        headers={"Authorization": f"Bearer {token}"},
    )
    assert certificate.status_code == 200
    assert certificate.json()["listen_host"] == "127.0.0.1"
    assert certificate.json()["listen_port"] == 25432
    assert "private" not in certificate.text.casefold()
    call = certificate_broker.calls[1]
    assert call["run_id"] == str(identity["run_id"])
    assert call["attempt_id"] == str(identity["attempt_id"])
    assert call["master_instance_id"] == str(identity["master_instance_id"])
    assert call["epoch"] == int(identity["epoch"])
    assert call["public_key"] == public_key


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
    token = TOKEN
    ledger.store_runtime_token_hash(str(identity["run_id"]), str(identity["attempt_id"]), token)
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
        expected_rows=266,
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
        export_batch_id=batch_identity(request.snapshot_at, request.expected_rows),
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
    mismatched = client.post(
        f"/internal/runtime/blogger-migration/{identity['run_id']}/{identity['attempt_id']}/import-receipt",
        json=import_receipt.model_copy(update={"request_sha256": "e" * 64}).model_dump(mode="json"),
        headers={
            "Authorization": f"Bearer {token}",
            "X-MDH-Master-Instance-ID": str(identity["master_instance_id"]),
            "X-MDH-Epoch": str(identity["epoch"]),
        },
    )
    assert mismatched.status_code == 409
    assert mismatched.json()["detail"]["code"] == "blogger_receipt_request_mismatch"
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


def test_quarantine_callback_is_durable_public_and_alteration_denied(tmp_path: Path) -> None:
    path = tmp_path / "quarantine-control.sqlite3"
    ledger = ControlLedger(path)
    wired = runtime(ledger, FakeKaggleRuntime())
    client = TestClient(create_app(ControlPlaneSettings(ledger_path=path), ledger=ledger, master_runtime=wired))
    ensured = client.post("/control/v1/master/ensure", json={"idempotency_key": "blogger-quarantine-master"})
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
    token = TOKEN
    ledger.store_runtime_token_hash(str(identity["run_id"]), str(identity["attempt_id"]), token)
    assert (
        client.post(
            "/internal/runtime/events",
            content=ready.model_dump_json(by_alias=True, exclude_none=True).encode(),
            headers={"Authorization": f"Bearer {token}"},
        ).status_code
        == 200
    )
    migration = BloggerMigrationRequest(
        request_id=uuid4(),
        operation_id=operation.operation_id,
        project_id=uuid4(),
        snapshot_at=now,
        expected_rows=266,
        source_revision="a" * 40,
    )
    assert (
        client.post("/control/v1/blogger-closure/requests", json=migration.model_dump(mode="json")).status_code == 200
    )
    runtime_headers = {
        "Authorization": f"Bearer {token}",
        "X-MDH-Master-Instance-ID": str(identity["master_instance_id"]),
        "X-MDH-Epoch": str(identity["epoch"]),
    }
    claim = client.get(
        f"/internal/runtime/blogger-migration/{identity['run_id']}/{identity['attempt_id']}",
        headers=runtime_headers,
    )
    assert claim.status_code == 200 and claim.json()["available"] is True
    actor_one, actor_two = uuid4(), uuid4()
    receipt = BloggerQuarantineReceipt(
        request_id=migration.request_id,
        operation_id=operation.operation_id,
        request_sha256=migration.request_sha256,
        master_instance_id=identity["master_instance_id"],
        run_id=identity["run_id"],
        attempt_id=identity["attempt_id"],
        epoch=identity["epoch"],
        export_batch_id=batch_identity(migration.snapshot_at, 266),
        row_count=266,
        raw_count=266,
        dispositioned_count=266,
        undispositioned_count=0,
        quarantined_count=2,
        logical_sha256="b" * 64,
        record_id_set_sha256="c" * 64,
        canonical_outcome_sha256="d" * 64,
        duplicate_group_count=1,
        duplicate_groups_pending=1,
        duplicate_review_inputs=BloggerDuplicateReviewInputs(
            groups=(
                BloggerDuplicateReviewGroup(
                    identity_sha256="e" * 64,
                    members=(
                        BloggerDuplicateReviewMember(record_id="record-1", projected_actor_id=actor_one),
                        BloggerDuplicateReviewMember(record_id="record-2", projected_actor_id=actor_two),
                    ),
                ),
            )
        ),
    )
    callback = f"/internal/runtime/blogger-migration/{identity['run_id']}/{identity['attempt_id']}/failed"
    payload = {
        "request_id": str(migration.request_id),
        "failure_code": receipt.failure_code,
        "quarantine_receipt": receipt.model_dump(mode="json"),
        "receipt_sha256": receipt.receipt_sha256,
    }
    first = client.post(callback, json=payload, headers=runtime_headers)
    assert first.status_code == 200
    assert client.post(callback, json=payload, headers=runtime_headers).json() == first.json()
    altered = receipt.model_copy(update={"logical_sha256": "9" * 64})
    conflict = client.post(
        callback,
        json={
            **payload,
            "quarantine_receipt": altered.model_dump(mode="json"),
            "receipt_sha256": altered.receipt_sha256,
        },
        headers=runtime_headers,
    )
    assert conflict.status_code == 409
    status = client.get(f"/control/v1/blogger-closure/requests/{migration.request_id}")
    assert status.status_code == 200
    value = status.json()
    assert value["state"] == "FAILED"
    assert value["quarantine_evidence"]["request_sha256"] == migration.request_sha256
    assert value["duplicate_review"]["duplicate_group_count"] == 1
    assert value["duplicate_review_inputs"]["groups"][0]["identity_sha256"] == "e" * 64
    assert "quarantine_receipt" not in value


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
        ControlPlaneSettings(ledger_path=path, connector_runtime_enabled=True),
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
    token = TOKEN
    ledger.store_runtime_token_hash(str(identity["run_id"]), str(identity["attempt_id"]), token)
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
    unauthorized_operator = TestClient(app).post(
        f"/internal/runtime/session-credentials/{identity['run_id']}/{identity['attempt_id']}",
        json={
            "master_instance_id": identity["master_instance_id"],
            "epoch": identity["epoch"],
            "credentials": [
                {
                    "role": "operator",
                    "database_url": secret_url.replace("reader:", "operator:"),
                    "expires_at": (datetime.now(UTC) + timedelta(minutes=2)).isoformat(),
                }
            ],
        },
        headers={"Authorization": f"Bearer {token}"},
    )
    assert unauthorized_operator.status_code == 422
    assert unauthorized_operator.json()["detail"]["code"] == "credential_roles_not_authorized"

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
            "schema_version": "18",
            "lease_until": (now + timedelta(minutes=4)).isoformat(),
            "master_instance_id": str(identity["master_instance_id"]),
            "epoch": int(identity["epoch"]),
        },
    )
    assert (
        TestClient(app)
        .post(
            "/internal/runtime/events",
            content=ready.model_dump_json(by_alias=True, exclude_none=True).encode(),
            headers={"Authorization": f"Bearer {token}"},
        )
        .status_code
        == 200
    )
    connector_url = secret_url.replace(
        "reader:opaque-password@127.0.0.1", "connector:opaque-password@master-tunnel.internal"
    )
    committer_url = connector_url.replace("connector:", "committer:")
    active_roles = TestClient(app).post(
        f"/internal/runtime/session-credentials/{identity['run_id']}/{identity['attempt_id']}",
        json={
            "master_instance_id": identity["master_instance_id"],
            "epoch": identity["epoch"],
            "credentials": [
                {
                    "role": "reader",
                    "database_url": secret_url,
                    "expires_at": (now + timedelta(minutes=2)).isoformat(),
                },
                {
                    "role": "connector",
                    "database_url": connector_url,
                    "expires_at": (now + timedelta(minutes=2)).isoformat(),
                },
                {
                    "role": "canonical_committer",
                    "database_url": committer_url,
                    "expires_at": (now + timedelta(minutes=2)).isoformat(),
                },
            ],
        },
        headers={"Authorization": f"Bearer {token}"},
    )
    assert active_roles.status_code == 200
    assert [credential.role for credential in registrar.credentials[-3:]] == [
        "reader",
        "connector",
        "canonical_committer",
    ]


def test_embedding_capability_does_not_claim_ready_without_active_observed_evidence(tmp_path: Path) -> None:
    ledger = ControlLedger(tmp_path / "capability.sqlite3")
    app = create_app(
        ControlPlaneSettings(ledger_path=ledger.path),
        ledger=ledger,
        master_runtime=runtime(ledger, FakeKaggleRuntime()),
    )
    response = TestClient(app).get("/control/v1/embedding-production/capabilities")
    assert response.status_code == 503
    assert response.json() == {"detail": {"code": "embedding_production_unavailable"}}


def test_injected_runner_alone_cannot_claim_embedding_readiness(tmp_path: Path) -> None:
    ledger = ControlLedger(tmp_path / "capability-ready.sqlite3")
    app = create_app(
        ControlPlaneSettings(ledger_path=ledger.path),
        ledger=ledger,
        master_runtime=runtime(ledger, FakeKaggleRuntime()),
        embedding_stage_runner=object(),
    )
    response = TestClient(app).get("/control/v1/embedding-production/capabilities")
    assert response.status_code == 503
    assert response.json() == {"detail": {"code": "embedding_production_unavailable"}}


def test_embedding_admission_accepts_first_request_and_replays_without_completion(tmp_path: Path) -> None:
    path = tmp_path / "embedding-admission.sqlite3"
    ledger = ControlLedger(path)
    wired = runtime(ledger, FakeKaggleRuntime())
    app = create_app(ControlPlaneSettings(ledger_path=path), ledger=ledger, master_runtime=wired)
    client = TestClient(app)
    ensured = client.post("/control/v1/master/ensure", json={"idempotency_key": "embedding-admission"})
    operation = ledger.get_operation(ensured.json()["operation_id"])
    assert operation is not None
    identity = operation.identity
    now = datetime.now(UTC)
    token = TOKEN
    ledger.store_runtime_token_hash(str(identity["run_id"]), str(identity["attempt_id"]), token)
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
            "canonical_revision": 9,
            "schema_version": "1",
            "lease_until": (now + timedelta(minutes=4)).isoformat(),
            "master_instance_id": str(identity["master_instance_id"]),
            "epoch": int(identity["epoch"]),
        },
    )
    accepted = client.post(
        "/internal/runtime/events",
        content=ready.model_dump_json(by_alias=True, exclude_none=True).encode(),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert accepted.status_code == 200
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
        "postgres-master", checkpoint_id, expected_generation=0, expected_parent_checkpoint_id=None
    )
    probe_query = "admission probe"
    request = EmbeddingProductionRequest(
        request_id=uuid4(),
        idempotency_key_sha256="a" * 64,
        blogger_receipt_id=uuid4(),
        blogger_receipt_sha256="b" * 64,
        blogger_canonical_revision=9,
        blogger_checkpoint_id=checkpoint_id,
        source_revision="c" * 40,
        probe_query=probe_query,
        probe_query_sha256=hashlib.sha256(probe_query.encode()).hexdigest(),
    )
    blocked = client.post("/control/v1/embedding-production/requests", json=request.model_dump(mode="json"))
    assert blocked.status_code == 409
    assert ledger.embedding_production_request(str(request.request_id)) is None

    adapter = object.__new__(KaggleProviderAdapter)
    wired.coordinator.provider = KaggleMasterRuntimeProvider(adapter, assets())

    capability = client.get("/control/v1/embedding-production/capabilities")
    assert capability.status_code == 503
    assert capability.json() == {"detail": {"code": "embedding_production_unavailable"}}
    observed = LedgerControlReader(ledger).invoke_control(
        "embedding.production.capabilities",
        {},
        object(),  # type: ignore[arg-type]
    )
    assert observed == {
        "admission_ready": False,
        "blocker_code": "EMBEDDING_DIRECT_DATA_PLANE_UNAVAILABLE",
    }
    rejected = client.post("/control/v1/embedding-production/requests", json=request.model_dump(mode="json"))
    assert rejected.status_code == 409
    assert ledger.embedding_production_request(str(request.request_id)) is None
