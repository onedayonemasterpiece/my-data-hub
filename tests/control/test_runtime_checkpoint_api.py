from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

from fastapi.testclient import TestClient

from my_data_hub.checkpoints.manifest import RestoreProbe, build_manifest
from my_data_hub.control_plane.app import ControlPlaneSettings, create_app
from my_data_hub.control_plane.ledger import ControlLedger
from my_data_hub.control_plane.runtime import ControlPlaneMasterRuntime, MasterRuntimeSettings
from my_data_hub.embeddings.production import embedding_provider_authority
from my_data_hub.orchestrator.master import FakeKaggleRuntime, MasterCoordinator
from my_data_hub.providers.kaggle import KaggleMasterLaunchAssets
from my_data_hub.providers.kaggle.contracts import (
    MutationAction,
    ProviderEffectIntent,
    TaskResourceClaim,
)
from my_data_hub.providers.models import ControlClass, ProviderFingerprint, ProviderKind

RUN = UUID("11111111-1111-4111-8111-111111111111")
ATTEMPT = UUID("22222222-2222-4222-8222-222222222222")
MASTER = UUID("33333333-3333-4333-8333-333333333333")
OPERATION = UUID("44444444-4444-4444-8444-444444444444")
SERVICE = UUID("55555555-5555-4555-8555-555555555555")
CHECKPOINT = UUID("66666666-6666-4666-8666-666666666666")
TOKEN = "runtime-token-that-is-long-enough"
RUN_2 = UUID("88888888-8888-4888-8888-888888888888")
ATTEMPT_2 = UUID("99999999-9999-4999-8999-999999999999")
MASTER_2 = UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
OPERATION_2 = UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")


def _runtime(ledger: ControlLedger) -> ControlPlaneMasterRuntime:
    assets = KaggleMasterLaunchAssets(
        source_identity="owner/postgres-master",
        source_version="git:exact",
        checkpoint_ref="owner/checkpoints",
        dataset_ref="owner/master-launch",
        notebook_ref="owner/master",
        dataset_files={
            "launch.txt": b"exact",
            "checkpoint-verifier.ipynb": b"{}",
            "postgresql-18-runtime.tar.gz": b"fake-postgresql-18-runtime",
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
    return ControlPlaneMasterRuntime(
        ledger,
        MasterCoordinator(ledger, FakeKaggleRuntime()),
        MasterRuntimeSettings(assets),
    )


def _app(tmp_path: Path):  # type: ignore[no-untyped-def]
    ledger = ControlLedger(tmp_path / "control.sqlite3")
    ledger.ensure_operation(
        operation_id=str(OPERATION),
        idempotency_key="checkpoint-api-operation",
        operation_kind="ensure_master",
        intent={"exact": True},
        initial_state="ACTIVE",
        identity={
            "run_id": str(RUN),
            "attempt_id": str(ATTEMPT),
            "service_instance_id": str(SERVICE),
            "master_instance_id": str(MASTER),
        },
        allocate_epoch_for="postgres-master",
    )
    ledger.record_attempt(
        attempt_id=str(ATTEMPT),
        run_id=str(RUN),
        operation_id=str(OPERATION),
        source_identity="owner/postgres-master",
        source_version="git:exact",
        service_instance_id=str(SERVICE),
        master_instance_id=str(MASTER),
        epoch=1,
        state="ACTIVE",
    )
    ledger.store_runtime_token_hash(str(RUN), str(ATTEMPT), TOKEN)
    app = create_app(
        ControlPlaneSettings(ledger_path=ledger.path),
        ledger=ledger,
        master_runtime=_runtime(ledger),
    )
    headers = {
        "Authorization": f"Bearer {TOKEN}",
        "X-MDH-Run-ID": str(RUN),
        "X-MDH-Attempt-ID": str(ATTEMPT),
        "X-MDH-Master-Instance-ID": str(MASTER),
        "X-MDH-Epoch": "1",
    }
    return app, ledger, headers


def _manifest(tmp_path: Path):  # type: ignore[no-untyped-def]
    package = tmp_path / "package"
    values = {
        "physical/base.tar.gz": b"base",
        "physical/backup_manifest": b"manifest",
        "physical/pg_wal.tar.gz": b"wal",
        "logical/hub.dump": b"logical",
        "receipts/verification.json": b"{}",
    }
    for relative, content in values.items():
        target = package / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
    return build_manifest(
        package_directory=package,
        checkpoint_id=CHECKPOINT,
        master_instance_id=MASTER,
        epoch=1,
        parent_checkpoint_id=None,
        postgres_version="18.0",
        pgvector_version="0.8.6",
        schema_version=13,
        canonical_revision=1,
        source_run_id=str(RUN),
        source_identity="owner/master/version/1",
        created_at=datetime(2026, 8, 11, tzinfo=UTC),
        checkpoint_lsn="0/16B6C50",
        file_kinds={
            "physical/base.tar.gz": "physical",
            "physical/backup_manifest": "postgres_backup_manifest",
            "physical/pg_wal.tar.gz": "physical",
            "logical/hub.dump": "logical",
            "receipts/verification.json": "verification_receipt",
        },
        restore_probe=RestoreProbe(13, 1, "a" * 64, {"hub.canonical_state": 1}),
    )


def test_remote_journal_requires_exact_runtime_identity(tmp_path: Path) -> None:
    app, ledger, headers = _app(tmp_path)
    intent = ProviderEffectIntent.create(
        operation_id=OPERATION,
        effect_id=UUID("77777777-7777-4777-8777-777777777777"),
        idempotency_key="remote-checkpoint-dataset",
        task_id=RUN,
        action=MutationAction.CREATE_DATASET,
        provider_ref="owner/checkpoints",
        arguments={"private": True},
        requested_at=datetime(2026, 8, 11, tzinfo=UTC),
    )
    client = TestClient(app)
    path = "/internal/provider-journal/intents"
    assert client.post(path, json={"intent": intent.model_dump(mode="json")}).status_code == 401
    assert client.post(path, json={"intent": intent.model_dump(mode="json")}, headers=headers).status_code == 200
    assert ledger.provider_effect_authority(str(intent.effect_id)) == {
        "effect_id": str(intent.effect_id),
        "operation_id": str(OPERATION),
        "task_id": str(RUN),
        "provider_ref": "owner/checkpoints",
        "action": "create_dataset",
    }
    claim = TaskResourceClaim.create(
        task_id=RUN,
        effect_id=intent.effect_id,
        provider_ref="owner/checkpoints",
        kind=ProviderKind.DATASET,
        control_class=ControlClass.ORCHESTRATOR_PROTECTED,
        disposable=False,
        fingerprint=ProviderFingerprint(value="a" * 64),
        provider_version=1,
        registered_at=datetime(2026, 8, 11, tzinfo=UTC),
    )
    persisted = client.post(
        "/internal/provider-journal/resource-claims",
        json={"claim": claim.model_dump(mode="json")},
        headers=headers,
    )
    assert persisted.status_code == 200, persisted.text
    current = client.post(
        "/internal/provider-journal/resource-claims/current",
        json={
            "provider_ref": "owner/checkpoints",
            "kind": "dataset",
            "control_class": "orchestrator_protected",
        },
        headers=headers,
    )
    assert current.status_code == 200
    assert current.json() == {"claim": claim.model_dump(mode="json")}
    fenced = {**headers, "X-MDH-Epoch": "2"}
    assert client.post(path, json={"intent": intent.model_dump(mode="json")}, headers=fenced).status_code == 409

    ledger.ensure_operation(
        operation_id=str(OPERATION_2),
        idempotency_key="checkpoint-api-operation-2",
        operation_kind="ensure_master",
        intent={"exact": True, "generation": 2},
        initial_state="ACTIVE",
        identity={
            "run_id": str(RUN_2),
            "attempt_id": str(ATTEMPT_2),
            "service_instance_id": str(UUID("cccccccc-cccc-4ccc-8ccc-cccccccccccc")),
            "master_instance_id": str(MASTER_2),
        },
        allocate_epoch_for="postgres-master",
    )
    ledger.record_attempt(
        attempt_id=str(ATTEMPT_2),
        run_id=str(RUN_2),
        operation_id=str(OPERATION_2),
        source_identity="owner/postgres-master",
        source_version="git:exact-2",
        service_instance_id=str(UUID("cccccccc-cccc-4ccc-8ccc-cccccccccccc")),
        master_instance_id=str(MASTER_2),
        epoch=2,
        state="ACTIVE",
    )
    ledger.store_runtime_token_hash(str(RUN_2), str(ATTEMPT_2), TOKEN + "-2")
    headers_2 = {
        "Authorization": f"Bearer {TOKEN}-2",
        "X-MDH-Run-ID": str(RUN_2),
        "X-MDH-Attempt-ID": str(ATTEMPT_2),
        "X-MDH-Master-Instance-ID": str(MASTER_2),
        "X-MDH-Epoch": "2",
    }
    prior_authority = client.post(
        "/internal/provider-journal/resource-claims/assert",
        json={"claim": claim.model_dump(mode="json")},
        headers=headers_2,
    )
    assert prior_authority.status_code == 200
    assert prior_authority.json() == {"authorized": True}
    version_intent = ProviderEffectIntent.create(
        operation_id=OPERATION_2,
        effect_id=UUID("dddddddd-dddd-4ddd-8ddd-dddddddddddd"),
        idempotency_key="remote-checkpoint-dataset-v2",
        task_id=claim.task_id,
        action=MutationAction.VERSION_DATASET,
        provider_ref=claim.provider_ref,
        expected_fingerprint=claim.fingerprint,
        arguments={"previous_version": 1},
        requested_at=datetime(2026, 8, 11, tzinfo=UTC),
    )
    accepted_version = client.post(
        path,
        json={"intent": version_intent.model_dump(mode="json")},
        headers=headers_2,
    )
    assert accepted_version.status_code == 200, accepted_version.text
    claim_v2 = TaskResourceClaim.create(
        task_id=claim.task_id,
        effect_id=version_intent.effect_id,
        provider_ref=claim.provider_ref,
        kind=ProviderKind.DATASET,
        control_class=ControlClass.ORCHESTRATOR_PROTECTED,
        disposable=False,
        fingerprint=ProviderFingerprint(value="b" * 64),
        provider_version=2,
        registered_at=datetime(2026, 8, 11, tzinfo=UTC),
    )
    persisted_v2 = client.post(
        "/internal/provider-journal/resource-claims",
        json={"claim": claim_v2.model_dump(mode="json")},
        headers=headers_2,
    )
    assert persisted_v2.status_code == 200, persisted_v2.text
    replayed_v2 = client.post(
        "/internal/provider-journal/resource-claims",
        json={"claim": claim_v2.model_dump(mode="json")},
        headers=headers_2,
    )
    assert replayed_v2.status_code == 200, replayed_v2.text
    conflicting_v2 = TaskResourceClaim.create(
        task_id=claim.task_id,
        effect_id=version_intent.effect_id,
        provider_ref=claim.provider_ref,
        kind=ProviderKind.DATASET,
        control_class=ControlClass.ORCHESTRATOR_PROTECTED,
        disposable=False,
        fingerprint=ProviderFingerprint(value="c" * 64),
        provider_version=2,
        registered_at=version_intent.requested_at,
    )
    rejected_conflict = client.post(
        "/internal/provider-journal/resource-claims",
        json={"claim": conflicting_v2.model_dump(mode="json")},
        headers=headers_2,
    )
    assert rejected_conflict.status_code == 403
    assert ledger.latest_provider_resource_claim(
        provider_ref=claim.provider_ref,
        resource_kind=ProviderKind.DATASET.value,
        control_class=ControlClass.ORCHESTRATOR_PROTECTED.value,
    ) == claim_v2.model_dump(mode="json")


def test_checkpoint_api_promotes_only_after_exact_stages_and_returns_numeric_head(tmp_path: Path) -> None:
    app, _ledger, headers = _app(tmp_path)
    client = TestClient(app)
    manifest = _manifest(tmp_path)
    initial = client.get("/internal/checkpoints/postgres-master/head", headers=headers)
    assert initial.json() == {"generation": 0, "current": None, "previous": None}
    candidate = client.post(
        "/internal/checkpoints/candidates",
        headers=headers,
        json={
            "operation_id": str(OPERATION),
            "dataset_ref": "owner/checkpoints",
            "service_kind": "postgres-master",
            "manifest": manifest.payload(),
        },
    )
    assert candidate.status_code == 200, candidate.text
    premature = client.post(
        f"/internal/checkpoints/{CHECKPOINT}/promote",
        headers=headers,
        json={"service_kind": "postgres-master", "expected_generation": 0},
    )
    assert premature.status_code == 409
    stages = [
        ("uploaded", {"service_kind": "postgres-master", "exact_version_ref": "owner/checkpoints/1"}),
        ("readback-verified", {"service_kind": "postgres-master"}),
        ("restore-verified", {"service_kind": "postgres-master"}),
    ]
    for path, body in stages:
        response = client.post(f"/internal/checkpoints/{CHECKPOINT}/{path}", headers=headers, json=body)
        assert response.status_code == 200, response.text
    promoted = client.post(
        f"/internal/checkpoints/{CHECKPOINT}/promote",
        headers=headers,
        json={"service_kind": "postgres-master", "expected_generation": 0},
    )
    assert promoted.status_code == 200, promoted.text
    assert promoted.json() == {
        "generation": 1,
        "current": {
            "checkpoint_id": str(CHECKPOINT),
            "dataset_ref": "owner/checkpoints",
            "exact_version_ref": "owner/checkpoints/1",
            "manifest_sha256": manifest.manifest_sha256,
        },
        "previous": None,
    }


def test_remote_journal_allows_only_exact_claimed_embedding_resources(tmp_path: Path) -> None:
    app, ledger, headers = _app(tmp_path)
    request_id = UUID("12121212-1212-4212-8212-121212121212")
    ledger.ensure_embedding_production_request(
        request_id=str(request_id),
        operation_id=str(OPERATION),
        idempotency_key_sha256="a" * 64,
        request_sha256="b" * 64,
        request={"request_id": str(request_id)},
    )
    ledger.claim_embedding_production_request(
        operation_id=str(OPERATION),
        run_id=str(RUN),
        attempt_id=str(ATTEMPT),
        master_instance_id=str(MASTER),
        epoch=1,
    )
    authority = embedding_provider_authority("owner", request_id)
    provider_ref, task_id = authority["e5_input"]
    exact = ProviderEffectIntent.create(
        operation_id=OPERATION,
        effect_id=UUID("13131313-1313-4313-8313-131313131313"),
        idempotency_key="embedding-e5-input",
        task_id=task_id,
        action=MutationAction.CREATE_DATASET,
        provider_ref=provider_ref,
        arguments={"exact": True},
        requested_at=datetime(2026, 8, 11, tzinfo=UTC),
    )
    client = TestClient(app)
    endpoint = "/internal/provider-journal/intents"
    assert client.post(endpoint, json={"intent": exact.model_dump(mode="json")}, headers=headers).status_code == 200
    wrong = ProviderEffectIntent.create(
        operation_id=OPERATION,
        effect_id=UUID("14141414-1414-4414-8414-141414141414"),
        idempotency_key="embedding-wrong-input",
        task_id=task_id,
        action=MutationAction.CREATE_DATASET,
        provider_ref="owner/mdh-embed-attacker-e5",
        arguments={"exact": True},
        requested_at=datetime(2026, 8, 11, tzinfo=UTC),
    )
    assert client.post(endpoint, json={"intent": wrong.model_dump(mode="json")}, headers=headers).status_code == 403
