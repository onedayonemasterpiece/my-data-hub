from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

from my_data_hub.control_plane.adapters import LedgerMasterResolver
from my_data_hub.control_plane.ledger import ControlLedger
from my_data_hub.control_plane.runtime import ControlPlaneMasterRuntime, MasterRuntimeSettings
from my_data_hub.mcp.contracts import MasterState
from my_data_hub.mcp.oauth import AccessIdentity
from my_data_hub.orchestrator.master import FakeKaggleRuntime, MasterCoordinator
from my_data_hub.providers.kaggle import KaggleMasterLaunchAssets


def identity() -> AccessIdentity:
    return AccessIdentity(
        subject="owner",
        client_id="chatgpt-reader",
        scopes=frozenset({"bloggers:read"}),
        audience="https://mcp.example/mcp",
        token_id="token",
        expires_at=2**31,
        issuer="https://auth.example",
        issued_at=1,
        resource="https://mcp.example/mcp",
    )


def test_mcp_cold_start_request_is_durably_bridged_to_one_provider_run(
    tmp_path: Path,
) -> None:
    ledger = ControlLedger(tmp_path / "control.sqlite3")
    provider = FakeKaggleRuntime()
    assets = KaggleMasterLaunchAssets(
        source_identity="owner/postgres-master",
        source_version="git:exact",
        checkpoint_ref="owner/checkpoints",
        dataset_ref="owner/master-assets",
        notebook_ref="owner/master-runtime",
        dataset_files={"asset.txt": b"bounded", "checkpoint-verifier.ipynb": b"{}"},
        notebook_source=b"print('master')\n",
        callback_url="https://mcp-datahub.kenigevents.ru/internal/runtime/events",
        checkpoint_verifier_ref="owner/checkpoint-verifier",
        checkpoint_verifier_source_file="checkpoint-verifier.ipynb",
        checkpoint_probe_relations=("hub.canonical_state",),
        notebook_kernel_type="script",
    )
    runtime = ControlPlaneMasterRuntime(
        ledger,
        MasterCoordinator(ledger, provider),
        MasterRuntimeSettings(assets=assets),
    )
    resolver = LedgerMasterResolver(ledger)
    first = resolver.ensure_master(identity(), intent="mcp-read:bloggers.search")
    duplicate = resolver.ensure_master(identity(), intent="mcp-read:bloggers.search")
    assert first.operation_id == duplicate.operation_id
    assert not first.duplicate and duplicate.duplicate
    assert resolver.resolve_master(identity()).state is MasterState.REQUESTED

    handle = runtime.reconcile_requested_once()
    assert handle is not None and handle.operation_id == first.operation_id
    assert handle.state.value == MasterState.REGISTERING.value
    assert runtime.reconcile_requested_once() is None
    assert provider.physical_effect_counts == {
        "ensure_dataset": 1,
        "push_notebook": 1,
        "trigger_run": 1,
    }
    ledger.activate_service_operation(
        operation_id=handle.operation_id,
        expected_state=MasterState.REGISTERING.value,
        service_instance_id=handle.service_instance_id,
        service_kind="postgres-master",
        run_id=handle.run_id,
        attempt_id=handle.attempt_id,
        master_instance_id=handle.master_instance_id,
        epoch=handle.epoch,
        endpoint="postgres-master.internal:5432",
        protocol="postgresql+tls",
        tls_fingerprint="a" * 64,
        capabilities=("canonical-read",),
        canonical_revision=1,
        schema_version="16",
        lease_until=datetime.now(UTC) + timedelta(minutes=5),
        latest_event_id="test-ready-provider-run-projection",
    )
    active = resolver.resolve_master(identity())
    assert active.operation_id == handle.operation_id
    assert active.public()["operation_id"] == handle.operation_id
    assert active.provider_run_ref == f"owner/master-runtime/run/{handle.run_id}"
    assert active.public()["provider_run_ref"] == active.provider_run_ref
