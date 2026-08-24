from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

import pytest

from my_data_hub.providers.kaggle.contracts import (
    KaggleAmbiguousMutation,
    TaskResourceClaim,
)
from my_data_hub.providers.models import ControlClass, ProviderFingerprint, ProviderKind
from my_data_hub.workloads.region_talk.pipeline_contracts import (
    ActiveMasterBinding,
    RegionTalkAccessBinding,
    RegionTalkDirectMasterAccess,
    RegionTalkLaunchMetadata,
    RegionTalkRunRequest,
    TaskWorkerCredentialCommand,
)
from my_data_hub.workloads.region_talk.production_assembly import (
    CentralRegionTalkNotebookAdapter,
)

NOW = datetime(2026, 8, 20, 10, tzinfo=UTC)
TASK = UUID("55555555-5555-4555-8555-555555555555")
MASTER = ActiveMasterBinding(
    run_id=UUID("11111111-1111-4111-8111-111111111111"),
    attempt_id=UUID("22222222-2222-4222-8222-222222222222"),
    master_instance_id=UUID("33333333-3333-4333-8333-333333333333"),
    epoch=47,
)


def _metadata() -> RegionTalkLaunchMetadata:
    request = RegionTalkRunRequest.supervised(
        idempotency_key="region-talk-provider-response-loss", requested_at=NOW
    )
    return RegionTalkLaunchMetadata(
        request_id=request.request_id,
        task_run_id=TASK,
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
        ydb_dependency_manifest_sha256="9" * 64,
        max_cycles=1,
        max_runtime_seconds=7200,
    )


class _Authority:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.token = "private-task-token-" + "t" * 40
        self.access: RegionTalkDirectMasterAccess | None = None
        self.purges = 0

    def prepare(self, metadata, *, task_token, source_sha256, generation=1):  # type: ignore[no-untyped-def]
        del task_token, source_sha256
        command = TaskWorkerCredentialCommand.create(
            task_run_id=metadata.task_run_id,
            epoch=metadata.master.epoch,
            generation=generation,
            task_token_sha256=hashlib.sha256(self.token.encode()).hexdigest(),
        )
        if self.access is None:
            self.access = RegionTalkDirectMasterAccess(
                credential_id=UUID("66666666-6666-4666-8666-666666666666"),
                task_run_id=metadata.task_run_id,
                master_instance_id=metadata.master.master_instance_id,
                epoch=metadata.master.epoch,
                generation=1,
                command_sha256=command.command_sha256,
                task_token_sha256=command.task_token_sha256,
                database_url="postgresql://worker:private@tunnel:25432/hub",
                tls_ca_pem="test-ca",
                expires_at=NOW + timedelta(minutes=4),
                tunnel_endpoint="tunnel:25432",
                ssh_private_key="private-key",
                ssh_certificate="certificate",
                ssh_known_hosts="known-hosts",
                ssh_gateway_host="gateway.internal",
                ssh_gateway_port=2222,
                ssh_account="mdh-region-talk",
                ssh_certificate_serial=7001,
            )
        return command

    def task_token(self, _task_run_id):  # type: ignore[no-untyped-def]
        return self.token

    def await_access(self, _metadata, _command):  # type: ignore[no-untyped-def]
        assert self.access is not None
        return self.access

    def active_binding(self, _task_run_id):  # type: ignore[no-untyped-def]
        assert self.access is not None
        return RegionTalkAccessBinding(
            credential_id=self.access.credential_id,
            generation=self.access.generation,
            command_sha256=self.access.command_sha256,
            task_token_sha256=self.access.task_token_sha256,
            expires_at=self.access.expires_at,
            ssh_certificate_serial=self.access.ssh_certificate_serial,
        )

    def request_revocation(self, _run):  # type: ignore[no-untyped-def]
        return None

    def purge(self, _task_run_id):  # type: ignore[no-untyped-def]
        self.purges += 1


class _Provider:
    def __init__(self) -> None:
        self.dataset_present = False
        self.dataset_creates = 0
        self.dataset_reconciles = 0
        self.notebook_present = False
        self.notebook_reconciles = 0
        self.notebook_pushes = 0
        self.deletes = 0

    @staticmethod
    def _claim(intent, kind):  # type: ignore[no-untyped-def]
        return TaskResourceClaim.create(
            task_id=intent.task_id,
            effect_id=intent.effect_id,
            provider_ref=intent.provider_ref,
            kind=kind,
            control_class=ControlClass.ORCHESTRATOR_PROTECTED,
            disposable=kind is ProviderKind.DATASET,
            fingerprint=ProviderFingerprint(value="a" * 64),
            provider_version=1 if kind is ProviderKind.DATASET else 2,
            registered_at=intent.requested_at,
        )

    def current_private_dataset_version(self, **_kwargs):  # type: ignore[no-untyped-def]
        return 1 if self.dataset_present else None

    def create_private_dataset(self, *, intent, **_kwargs):  # type: ignore[no-untyped-def]
        self.dataset_creates += 1
        self.dataset_present = True
        raise KaggleAmbiguousMutation("response lost after exact Dataset commit")

    def reconcile_private_dataset_directory_mutation(self, *, intent, **_kwargs):  # type: ignore[no-untyped-def]
        self.dataset_reconciles += 1
        return SimpleNamespace(claim=self._claim(intent, ProviderKind.DATASET))

    def reconcile_private_notebook_mutation(self, *, intent, **_kwargs):  # type: ignore[no-untyped-def]
        if not self.notebook_present:
            return None
        self.notebook_reconciles += 1
        return SimpleNamespace(claim=self._claim(intent, ProviderKind.NOTEBOOK))

    def current_private_notebook_version(self, **_kwargs):  # type: ignore[no-untyped-def]
        return 1

    def push_private_notebook_pending_runtime_attestation(self, *, intent, **_kwargs):  # type: ignore[no-untyped-def]
        self.notebook_pushes += 1
        self.notebook_present = True
        raise KaggleAmbiguousMutation("response lost after exact Notebook push")

    def delete_task_created_resource(self, **_kwargs):  # type: ignore[no-untyped-def]
        self.deletes += 1
        return SimpleNamespace()


def test_restart_reuses_original_provider_intents_and_reconciles_without_duplicate(
    tmp_path: Path,
) -> None:
    root = (tmp_path / "private").absolute()
    root.mkdir(mode=0o700)
    authority = _Authority(root)
    provider = _Provider()
    metadata = _metadata()
    first = CentralRegionTalkNotebookAdapter(
        adapter=provider,
        authority=authority,  # type: ignore[arg-type]
        owner="owner",
        callback_base_url="https://control.example",
        clock=lambda: NOW,
    )

    with pytest.raises(KaggleAmbiguousMutation):
        first.launch(metadata)
    journal = json.loads((root / "launcher-metadata.json").read_bytes())
    original_requested_at = journal["intents"][str(TASK)]["status"]["requested_at"]

    restarted = CentralRegionTalkNotebookAdapter(
        adapter=provider,
        authority=authority,  # type: ignore[arg-type]
        owner="owner",
        callback_base_url="https://control.example",
        clock=lambda: NOW + timedelta(hours=1),
    )
    with pytest.raises(KaggleAmbiguousMutation):
        restarted.launch(metadata)
    third = CentralRegionTalkNotebookAdapter(
        adapter=provider,
        authority=authority,  # type: ignore[arg-type]
        owner="owner",
        callback_base_url="https://control.example",
        clock=lambda: NOW + timedelta(hours=2),
    )
    receipt = third.launch(metadata)

    assert receipt.provider_run_ref == "owner/mdh-region-talk-supervisor/2"
    assert provider.dataset_creates == 1
    assert provider.dataset_reconciles == 1
    assert provider.notebook_pushes == 1
    assert provider.notebook_reconciles == 1
    replayed = json.loads((root / "launcher-metadata.json").read_bytes())
    assert replayed["intents"][str(TASK)]["status"]["requested_at"] == original_requested_at
    encoded = (root / "launcher-metadata.json").read_text()
    assert "postgresql://" not in encoded
    assert authority.token not in encoded

    run = SimpleNamespace(
        task_run_id=metadata.task_run_id,
        access=receipt.access,
        master=metadata.master,
    )
    cleaned = third.cleanup(run)
    replay = CentralRegionTalkNotebookAdapter(
        adapter=provider,
        authority=authority,  # type: ignore[arg-type]
        owner="owner",
        callback_base_url="https://control.example",
        clock=lambda: NOW + timedelta(hours=2),
    ).cleanup(run)
    assert replay == cleaned
    assert provider.deletes == 1
    assert cleaned.resources_deleted == 1
