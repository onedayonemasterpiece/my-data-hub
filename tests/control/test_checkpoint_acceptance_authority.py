from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta
from uuid import NAMESPACE_URL, UUID, uuid5

from fastapi.testclient import TestClient

from my_data_hub.acceptance.scenario_operator import (
    AcceptanceScenarioRequest,
    CheckpointAcceptanceLaunchCatalog,
    CheckpointDatasetInputClaim,
)
from my_data_hub.control_plane.app import ControlPlaneSettings, create_app
from my_data_hub.providers.kaggle.contracts import MutationAction, ProviderEffectIntent

TASK = UUID("11111111-1111-4111-8111-111111111111")
TOKEN = "one-time-status-dataset-token-value-1234567890"
NOW = datetime.now(UTC)


class Principal:
    subject = "owner"
    client_id = "operator"
    scopes = frozenset({"acceptance:operate"})


def _launch():
    from my_data_hub.acceptance.scenario_operator import CheckpointVerifierInputClaim

    catalog = CheckpointAcceptanceLaunchCatalog(
        provider_owner="owner",
        evidence_notebook_ref="owner/checkpoint-evidence",
        candidate_dataset_refs={
            "FM05": "owner/candidate-fm05",
            "FM14": "owner/candidate-fm14",
            "FM15": "owner/candidate-fm15",
        },
        template_input=CheckpointDatasetInputClaim(
            provider_ref="owner/template",
            exact_version_ref="owner/template/1",
            claim_sha256="1" * 64,
            manifest_sha256="2" * 64,
            content_sha256="3" * 64,
        ),
        verifier_inputs={
            scenario: CheckpointVerifierInputClaim(
                provider_ref=f"owner/verifier-{scenario.lower()}",
                exact_version_ref=f"owner/verifier-{scenario.lower()}/1",
                claim_sha256="4" * 64,
                source_sha256="5" * 64,
            )
            for scenario in ("FM05", "FM15")
        },
        verifier_notebook_refs={
            "FM05": "owner/verifier-notebook-fm05",
            "FM15": "owner/verifier-notebook-fm15",
        },
    )
    return catalog.request(
        AcceptanceScenarioRequest(
            task_id=TASK,
            scenario="FM14",
            idempotency_key="checkpoint-authority-test",
            source_revision="a" * 40,
        ),
        Principal(),
        started_at=NOW,
    )


def _headers(launch, token: str = TOKEN) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "X-MDH-Acceptance-Request-ID": str(launch.request_id),
        "X-MDH-Acceptance-Task-Run-ID": str(launch.task_run_id),
        "X-MDH-Acceptance-Attempt-ID": str(launch.control_identity.attempt_id),
    }


def test_checkpoint_task_authority_is_exact_and_does_not_borrow_master_epoch(tmp_path) -> None:
    app = create_app(ControlPlaneSettings(ledger_path=tmp_path / "control.sqlite3"))
    ledger = app.state.control_ledger
    launch = _launch()
    ledger.ensure_checkpoint_acceptance_launch(
        request=launch.model_dump(mode="json"),
        request_sha256=launch.request_sha256,
        principal_id="owner",
        client_id="operator",
        token_sha256=hashlib.sha256(TOKEN.encode()).hexdigest(),
        expires_at=NOW + timedelta(seconds=900),
        config={"schema_version": "test-only"},
        config_sha256="6" * 64,
        expected_source_sha256="8" * 64,
    )
    client = TestClient(app)

    assert client.get("/internal/checkpoints/postgres-master/head").status_code == 401
    assert client.get(
        "/internal/checkpoints/postgres-master/head", headers=_headers(launch, "wrong-token")
    ).status_code == 401
    assert client.get(
        "/internal/checkpoints/postgres-master/head", headers=_headers(launch)
    ).status_code == 409

    event = {
        "schema": "content-runtime-event/v1",
        "event_id": "33333333-3333-4333-8333-333333333333",
        "run_id": str(TASK),
        "attempt_id": str(launch.control_identity.attempt_id),
        "service_instance_id": str(TASK),
        "source_identity": launch.evidence_notebook_ref,
        "source_version": launch.source_revision,
        "event_type": "runtime.started",
        "emitted_at": NOW.isoformat(),
        "local_sequence": 1,
        "epoch": 1,
        "phase": "bootstrap",
        "status": "running",
        "data": {
            "donor_event": "kernel_started",
            "donor_event_uid": f"{TASK}:kernel_started:0",
            "progress": {
                "completed_steps": 0,
                "sequence": 0,
                "runtime_source_sha256": "8" * 64,
            },
            "donor_body_sha256": "7" * 64,
        },
        "artifact_refs": [],
        "metrics": {},
    }
    first_event = client.post(
        "/internal/acceptance/events", headers=_headers(launch), json=event
    )
    assert first_event.status_code == 200
    assert first_event.json()["duplicate"] is False
    replay_event = client.post(
        "/internal/acceptance/events", headers=_headers(launch), json=event
    )
    assert replay_event.status_code == 200
    assert replay_event.json()["duplicate"] is True
    changed = {**event, "status": "changed"}
    assert client.post(
        "/internal/acceptance/events", headers=_headers(launch), json=changed
    ).status_code == 409
    observation = ledger.checkpoint_acceptance_event_observation(str(TASK))
    assert observation is not None
    assert observation["latest_phase"] == "bootstrap"
    assert observation["event_counts"] == {"runtime.started": 1}
    assert observation["runtime_source_sha256"] == "8" * 64
    head = client.get("/internal/checkpoints/postgres-master/head", headers=_headers(launch))
    assert head.status_code == 200
    assert head.json()["generation"] == 0

    intent = ProviderEffectIntent.create(
        operation_id=launch.operation_id,
        effect_id=uuid5(NAMESPACE_URL, f"candidate:{launch.operation_id}"),
        idempotency_key=f"candidate:{launch.operation_id}",
        task_id=launch.task_run_id,
        action=MutationAction.CREATE_DATASET,
        provider_ref=launch.candidate_dataset_ref,
        arguments={"fixed": True},
        requested_at=NOW,
    )
    accepted = client.post(
        "/internal/provider-journal/intents",
        headers=_headers(launch),
        json={"intent": intent.model_dump(mode="json")},
    )
    assert accepted.status_code == 200

    forbidden = intent.model_copy(
        update={
            "provider_ref": "owner/not-authorized",
            "request_sha256": "0" * 64,
        }
    )
    denied = client.post(
        "/internal/provider-journal/intents",
        headers=_headers(launch),
        json={"intent": forbidden.model_dump(mode="json")},
    )
    assert denied.status_code in {403, 422}


def test_checkpoint_source_mismatch_is_persisted_and_fences_authority(tmp_path) -> None:
    app = create_app(ControlPlaneSettings(ledger_path=tmp_path / "control.sqlite3"))
    ledger = app.state.control_ledger
    launch = _launch()
    ledger.ensure_checkpoint_acceptance_launch(
        request=launch.model_dump(mode="json"),
        request_sha256=launch.request_sha256,
        principal_id="owner",
        client_id="operator",
        token_sha256=hashlib.sha256(TOKEN.encode()).hexdigest(),
        expires_at=NOW + timedelta(seconds=900),
        config={"schema_version": "test-only"},
        config_sha256="6" * 64,
        expected_source_sha256="8" * 64,
    )
    event = {
        "schema": "content-runtime-event/v1",
        "event_id": "44444444-4444-4444-8444-444444444444",
        "run_id": str(TASK),
        "attempt_id": str(launch.control_identity.attempt_id),
        "service_instance_id": str(TASK),
        "source_identity": launch.evidence_notebook_ref,
        "source_version": launch.source_revision,
        "event_type": "runtime.started",
        "emitted_at": NOW.isoformat(),
        "local_sequence": 1,
        "epoch": 1,
        "phase": "bootstrap",
        "status": "running",
        "data": {
            "donor_event": "kernel_started",
            "donor_event_uid": f"{TASK}:kernel_started:mismatch",
            "progress": {"runtime_source_sha256": "9" * 64},
            "donor_body_sha256": "7" * 64,
        },
        "artifact_refs": [],
        "metrics": {},
    }
    client = TestClient(app)

    assert client.post(
        "/internal/acceptance/events", headers=_headers(launch), json=event
    ).status_code == 200
    stored = ledger.checkpoint_acceptance_launch(str(TASK))
    assert stored is not None
    assert stored["observed_source_sha256"] == "9" * 64
    assert stored["source_attestation_state"] == "MISMATCH"
    assert client.get(
        "/internal/checkpoints/postgres-master/head", headers=_headers(launch)
    ).status_code == 409


def test_enabled_app_assembles_checkpoint_launcher_without_runtime_impersonation(
    tmp_path, monkeypatch
) -> None:
    import json
    from types import SimpleNamespace

    import my_data_hub.control_plane.app as app_module
    from my_data_hub.acceptance.checkpoint_launcher import ControlCheckpointAcceptanceLauncher
    from my_data_hub.control_plane.runtime import ProductionRuntimeBuild

    deployment = {
        "schema_version": "my-data-hub-checkpoint-acceptance-deployment.v1",
        "provider_owner": "owner",
        "evidence_notebook_ref": "owner/checkpoint-evidence",
        "candidate_dataset_refs": {
            "FM05": "owner/candidate-fm05",
            "FM14": "owner/candidate-fm14",
            "FM15": "owner/candidate-fm15",
        },
        "template_input": {
            "provider_ref": "owner/template",
            "exact_version_ref": "owner/template/1",
            "claim_sha256": "1" * 64,
            "manifest_sha256": "2" * 64,
            "content_sha256": "3" * 64,
        },
        "verifier_inputs": {
            scenario: {
                "provider_ref": f"owner/verifier-{scenario.lower()}",
                "exact_version_ref": f"owner/verifier-{scenario.lower()}/1",
                "claim_sha256": "4" * 64,
                "source_sha256": "5" * 64,
            }
            for scenario in ("FM05", "FM15")
        },
        "verifier_notebook_refs": {
            "FM05": "owner/verifier-notebook-fm05",
            "FM15": "owner/verifier-notebook-fm15",
        },
        "runtime_input": {
            "provider_ref": "owner/checkpoint-runtime",
            "exact_version_ref": "owner/checkpoint-runtime/1",
            "claim_sha256": "6" * 64,
            "wheel_file": "my_data_hub.whl",
            "wheel_sha256": "7" * 64,
            "entrypoint_sha256": "8" * 64,
            "docker_image": "gcr.io/kaggle-images/python@sha256:" + "a" * 64,
            "docker_image_pinning_type": "original",
            "image_source_commit": "c" * 40,
            "python_series": "3.12",
        },
        "control_base_url": "https://control.example.test",
        "brokered_checkpoint_upload": True,
    }
    deployment_path = tmp_path / "checkpoint-deployment.json"
    deployment_path.write_text(json.dumps(deployment))
    deployment_path.chmod(0o600)
    monkeypatch.setenv("MY_DATA_HUB_CHECKPOINT_ACCEPTANCE_DEPLOYMENT_FILE", str(deployment_path))

    provider_adapter = SimpleNamespace()

    def build(ledger, *_args, **_kwargs):
        runtime = SimpleNamespace(
            ledger=ledger,
            settings=SimpleNamespace(),
            coordinator=SimpleNamespace(
                tunnel_authority=SimpleNamespace(
                    acceptance_identity_snapshot=lambda **_values: {},
                    acceptance_retired_denial=lambda **_values: {},
                )
            ),
            reconcile_requested_once=lambda: None,
            reconcile_acceptance_once=lambda: None,
        )
        return ProductionRuntimeBuild(
            master=runtime,
            provider_status="available",
            provider_adapter=provider_adapter,  # type: ignore[arg-type]
        )

    monkeypatch.setattr(app_module, "build_production_runtime", build)
    app = create_app(
        ControlPlaneSettings(
            ledger_path=tmp_path / "assembled.sqlite3",
            master_runtime=SimpleNamespace(),  # type: ignore[arg-type]
            operator_credentials_enabled=True,
            provider_gateway_enabled=True,
            acceptance_scenarios_enabled=True,
        ),
        provider_gateway_token=b"x" * 32,
    )

    adapter = app.state.acceptance_scenario_adapter
    assert adapter is not None
    launcher = adapter.executor.checkpoint
    assert isinstance(launcher, ControlCheckpointAcceptanceLauncher)
    assert launcher.adapter is provider_adapter
    assert launcher.deployment.catalog.provider_owner == "owner"
    assert adapter.executor.master.host_effects.old_epoch_denials.__class__.__name__ == (
        "TaskBoundOldEpochDenialFactory"
    )
