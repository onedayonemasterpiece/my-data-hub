from __future__ import annotations

import hashlib
import sqlite3
from base64 import b64decode, b64encode
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest

from my_data_hub.control_plane.adapters import KaggleMCPProviderGateway, LedgerControlReader, LedgerWriteGate
from my_data_hub.control_plane.clock import DeterministicClock
from my_data_hub.control_plane.ledger import ControlLedger, StaleRuntimeEvent
from my_data_hub.hashing import sha256_value
from my_data_hub.mcp.contracts import MasterSnapshot, MasterState
from my_data_hub.mcp.oauth import AccessIdentity
from my_data_hub.mcp.service import HubService
from my_data_hub.providers.exchange import manifest_sha256
from my_data_hub.providers.inventory import InventoryPage
from my_data_hub.providers.kaggle import ControlLedgerKaggleJournal
from my_data_hub.providers.kaggle.contracts import (
    DatasetMutationResult,
    EffectOutcome,
    ExactDatasetBatch,
    ExactDatasetBatchFile,
    KaggleContractError,
    KaggleDatasetIdentity,
    KaggleKernelRunIdentity,
    KaggleKernelSourceIdentity,
    KaggleKernelStatus,
    KaggleNotFound,
    KernelState,
    MutationAction,
    NotebookMutationResult,
    ProviderEffectReceipt,
    TaskResourceClaim,
)
from my_data_hub.providers.models import ObservedProviderResource, ProviderFingerprint, ProviderKind


def principal(subject: str = "owner") -> AccessIdentity:
    return AccessIdentity(
        subject=subject,
        client_id="owner-operator",
        scopes=frozenset({"data:write", "provider:write", "operation:read"}),
        audience="mcp",
        token_id="token",
        expires_at=2_100_000_000,
        issuer="https://issuer.example",
        issued_at=2_000_000_000,
        resource="https://mcp.example/mcp",
    )


def checkpoint(
    ledger: ControlLedger,
    *,
    checkpoint_id: str,
    operation_id: str,
    master_instance_id: str,
    epoch: int,
    revision: int,
) -> None:
    head = ledger.checkpoint_head("postgres-master")
    ledger.add_checkpoint_candidate(
        checkpoint_id=checkpoint_id,
        operation_id=operation_id,
        dataset_ref="owner/checkpoints",
        version_ref=None,
        manifest_sha256=("a" if head is None else "b") * 64,
        source_checkpoint_id=head.current_checkpoint_id if head else None,
        source_head_generation=head.generation if head else 0,
        master_instance_id=master_instance_id,
        epoch=epoch,
        manifest_payload={"canonical_revision": revision},
    )
    version = (head.generation + 1) if head else 1
    ledger.mark_checkpoint_uploaded(checkpoint_id, f"owner/checkpoints/{version}")
    ledger.mark_checkpoint_readback_verified(checkpoint_id)
    ledger.mark_checkpoint_restore_verified(checkpoint_id)
    ledger.promote_checkpoint(
        "postgres-master",
        checkpoint_id,
        expected_generation=head.generation if head else 0,
        expected_parent_checkpoint_id=head.current_checkpoint_id if head else None,
    )


def test_write_gate_persists_preview_apply_and_exact_post_checkpoint(tmp_path: Path) -> None:
    clock = DeterministicClock(datetime(2026, 8, 11, tzinfo=UTC))
    ledger = ControlLedger(tmp_path / "control.sqlite3", clock=clock)
    source_operation = str(uuid4())
    ledger.ensure_operation(
        operation_id=source_operation,
        idempotency_key="source-master-operation",
        operation_kind="ensure_master",
        intent={"kind": "source"},
        initial_state="ACTIVE",
        identity={},
    )
    master_id = str(uuid4())
    checkpoint(
        ledger,
        checkpoint_id=str(uuid4()),
        operation_id=source_operation,
        master_instance_id=master_id,
        epoch=7,
        revision=41,
    )
    gate = LedgerWriteGate(
        ledger,
        signing_secret=b"s" * 32,
        clock=lambda: clock.now().timestamp(),
    )
    master = MasterSnapshot(
        MasterState.ACTIVE,
        instance_id=master_id,
        epoch=7,
        canonical_revision=41,
    )
    arguments = {
        "sql": "UPDATE hub.project SET name=$1 WHERE project_id=$2",
        "parameters": ["fixed", "project-1"],
        "expected_revision": 41,
        "max_affected_rows": 1,
        "idempotency_key": "operator-change-1",
    }
    preview_permit = gate.authorize_write(
        principal=principal(), tool="data.change.preview", arguments=arguments, master=master
    )
    preview = gate.record_write_result(
        permit=preview_permit,
        result={"affected_rows": 1, "master_epoch": 7, "canonical_revision": 41},
    )
    assert preview["status"] == "PREVIEWED"
    assert preview["preview_receipt"]

    apply_arguments = {**arguments, "preview_receipt": preview["preview_receipt"]}
    apply_permit = gate.authorize_write(
        principal=principal(), tool="data.change.apply", arguments=apply_arguments, master=master
    )
    committed = gate.record_write_result(
        permit=apply_permit,
        result={"affected_rows": 1, "master_epoch": 7, "canonical_revision": 41},
    )
    assert committed["status"] == "COMMITTED_PENDING_CHECKPOINT"
    assert gate.write_status(preview["operation_id"], principal())["state"] == "CHECKPOINTING"

    clock.advance(1)
    checkpoint(
        ledger,
        checkpoint_id=str(uuid4()),
        operation_id=source_operation,
        master_instance_id=master_id,
        epoch=7,
        revision=41,
    )
    status = gate.write_status(preview["operation_id"], principal())
    assert status["state"] == "DURABLE_COMPLETE"
    assert status["pre_change_checkpoint_id"] != status["post_change_checkpoint_id"]


@pytest.mark.parametrize(
    "tool",
    [
        "provider.resources.create",
        "provider.resources.version",
        "provider.resources.run",
        "provider.resources.delete",
        "provider.upload.start",
        "provider.upload.put_chunk",
        "provider.upload.finalize",
        "provider.upload.abort",
    ],
)
@pytest.mark.parametrize(
    "master",
    [
        MasterSnapshot(MasterState.ABSENT),
        MasterSnapshot(MasterState.ACTIVE, instance_id="master", epoch=7, canonical_revision=41),
    ],
)
def test_operator_write_gate_keeps_private_provider_mutations_master_independent(
    tmp_path: Path, tool: str, master: MasterSnapshot
) -> None:
    gate = LedgerWriteGate(ControlLedger(tmp_path / "control.sqlite3"), signing_secret=b"s" * 32)

    permit = gate.authorize_write(
        principal=principal(),
        tool=tool,
        arguments={"control_class": "mcp_managed", "private": True},
        master=master,
    )

    assert permit.tool == tool
    assert permit.canonical_data_independent is True
    assert permit.master_epoch == 0
    assert permit.canonical_revision == 0
    assert permit.checkpoint_lifecycle_bound is False
    assert permit.pre_change_checkpoint_verified is False
    assert permit.allowed_resource_class == "mcp_managed"
    assert permit.private_resource_only is True


def test_operator_write_gate_rejects_unscoped_or_public_provider_mutation(tmp_path: Path) -> None:
    gate = LedgerWriteGate(ControlLedger(tmp_path / "control.sqlite3"), signing_secret=b"s" * 32)
    absent = MasterSnapshot(MasterState.ABSENT)
    with pytest.raises(PermissionError, match="provider:write"):
        gate.authorize_write(
            principal=replace(principal(), scopes=frozenset({"data:write"})),
            tool="provider.upload.start",
            arguments={"control_class": "mcp_managed", "private": True},
            master=absent,
        )
    with pytest.raises(PermissionError, match="private"):
        gate.authorize_write(
            principal=principal(),
            tool="provider.upload.start",
            arguments={"control_class": "mcp_managed", "private": False},
            master=absent,
        )


@pytest.mark.asyncio
async def test_postgres_receipt_reconciles_applying_without_reexecuting_dml(tmp_path: Path) -> None:
    clock = DeterministicClock(datetime(2026, 8, 11, tzinfo=UTC))
    ledger = ControlLedger(tmp_path / "control.sqlite3", clock=clock)
    source_operation = str(uuid4())
    ledger.ensure_operation(
        operation_id=source_operation,
        idempotency_key="source-master-operation-reconcile",
        operation_kind="ensure_master",
        intent={"kind": "source"},
        initial_state="ACTIVE",
        identity={},
    )
    master_id = str(uuid4())
    checkpoint(
        ledger,
        checkpoint_id=str(uuid4()),
        operation_id=source_operation,
        master_instance_id=master_id,
        epoch=7,
        revision=41,
    )
    gate = LedgerWriteGate(
        ledger,
        signing_secret=b"s" * 32,
        clock=lambda: clock.now().timestamp(),
    )
    before = MasterSnapshot(MasterState.ACTIVE, instance_id=master_id, epoch=7, canonical_revision=41)
    arguments = {
        "sql": "UPDATE hub.project SET name=$1 WHERE project_id=$2",
        "parameters": ["fixed", "project-1"],
        "expected_revision": 41,
        "max_affected_rows": 1,
        "idempotency_key": "operator-crash-after-pg-commit",
    }
    preview_permit = gate.authorize_write(
        principal=principal(), tool="data.change.preview", arguments=arguments, master=before
    )
    preview = gate.record_write_result(
        permit=preview_permit,
        result={"affected_rows": 1, "master_epoch": 7, "canonical_revision": 41},
    )
    apply_arguments = {**arguments, "preview_receipt": preview["preview_receipt"]}
    gate.authorize_write(
        principal=principal(), tool="data.change.apply", arguments=apply_arguments, master=before
    )
    applying = ledger.mcp_write_operation(preview["operation_id"])
    assert applying is not None and applying["state"] == "APPLYING"

    canonical_receipt = {
        "found": True,
        "operation_id": preview["operation_id"],
        "request_sha256": applying["request_sha256"],
        "master_instance_id": master_id,
        "master_epoch": 7,
        "expected_revision": 41,
        "principal_id": principal().subject,
        "client_id": principal().client_id,
        "affected_rows": 1,
        "committed_revision": 42,
        "committed_at": clock.now().isoformat(),
        "canonical_revision": 42,
    }

    class Resolver:
        async def resolve_master(self, _identity):  # type: ignore[no-untyped-def]
            return MasterSnapshot(
                MasterState.ACTIVE, instance_id=master_id, epoch=7, canonical_revision=42
            )

        async def ensure_master(self, *_args, **_kwargs):  # type: ignore[no-untyped-def]
            raise AssertionError("reconciliation must not ensure another master")

    class Session:
        arguments: dict[str, object] | None = None

        async def execute(self, value):  # type: ignore[no-untyped-def]
            self.arguments = dict(value)
            return canonical_receipt

        async def close(self) -> None:
            return None

    class Broker:
        def __init__(self) -> None:
            self.requests = []
            self.session = Session()

        async def issue_session(self, request):  # type: ignore[no-untyped-def]
            self.requests.append(request)
            return self.session

    broker = Broker()
    service = HubService(
        Resolver(),
        broker=broker,  # type: ignore[arg-type]
        control=LedgerControlReader(ledger, write_gate=gate),
        write_gate=gate,
        fallback_identity=principal(),
        clock=lambda: clock.now().timestamp(),
    )
    status = await service.invoke("data.change.status", {"operation_id": preview["operation_id"]})
    assert status["state"] == "CHECKPOINTING"
    assert [request.tool for request in broker.requests] == ["data.change.reconcile"]
    assert broker.session.arguments is not None
    assert "sql" not in broker.session.arguments and "parameters" not in broker.session.arguments
    def committed_projection_events() -> int:
        with sqlite3.connect(ledger.path) as connection:
            return int(
                connection.execute(
                    "SELECT count(*) FROM mcp_write_events WHERE operation_id=? AND state=?",
                    (preview["operation_id"], "COMMITTED_PENDING_CHECKPOINT"),
                ).fetchone()[0]
            )

    assert committed_projection_events() == 1

    with pytest.raises(PermissionError, match="already admitted"):
        await service.invoke("data.change.apply", apply_arguments)
    assert [request.tool for request in broker.requests] == ["data.change.reconcile"]

    replay = gate.record_reconciled_write(
        operation_id=preview["operation_id"], receipt=canonical_receipt
    )
    assert replay["status"] == "CHECKPOINTING"
    assert committed_projection_events() == 1


class FakeAdapter:
    def __init__(self, ledger: ControlLedger) -> None:
        self.journal = ControlLedgerKaggleJournal(ledger)
        self.now = ledger.clock.now
        self.datasets: dict[tuple[str, int], KaggleDatasetIdentity] = {}
        self.dataset_files: dict[tuple[str, int], dict[str, bytes]] = {}
        self.sources: dict[tuple[str, int], KaggleKernelSourceIdentity] = {}
        self.output_bytes = b'{"accepted":true}'
        self.create_calls = 0
        self.version_calls = 0
        self.run_dataset_sources: tuple[str, ...] | None = None
        self.run_intent_arguments_sha256: str | None = None
        self.run_runtime_options: tuple[bool, str] | None = None
        self.run_calls = 0
        self.delete_calls = 0
        self.delete_receipts: dict[str, ProviderEffectReceipt] = {}

    def _dataset_result(self, intent, control_class, disposable, version):  # type: ignore[no-untyped-def]
        fingerprint = ProviderFingerprint(value=("c" if version == 1 else "d") * 64)
        identity = KaggleDatasetIdentity(
            provider_ref=intent.provider_ref,
            version=version,
            privacy="private",
            package_sha256=("e" if version == 1 else "f") * 64,
            fingerprint=fingerprint,
            observed_at=self.now(),
        )
        receipt = ProviderEffectReceipt(
            operation_id=intent.operation_id,
            effect_id=intent.effect_id,
            action=intent.action,
            provider_ref=intent.provider_ref,
            outcome=EffectOutcome.APPLIED,
            attempts=1,
            observed_fingerprint=fingerprint,
            provider_version=version,
            observed_at=self.now(),
            detail_code="fake_applied",
        )
        claim = TaskResourceClaim.create(
            task_id=intent.task_id,
            effect_id=intent.effect_id,
            provider_ref=intent.provider_ref,
            kind=ProviderKind.DATASET,
            control_class=control_class,
            disposable=disposable,
            fingerprint=fingerprint,
            provider_version=version,
            registered_at=self.now(),
        )
        self.journal.persist_intent(intent)
        self.journal.persist_receipt(receipt)
        self.journal.persist_resource_claim(claim)
        self.datasets[(identity.provider_ref, version)] = identity
        return DatasetMutationResult(identity=identity, claim=claim, effect=receipt)

    def create_private_dataset(self, *, intent, files, title, control_class, disposable):  # type: ignore[no-untyped-def]
        assert files and title
        self.create_calls += 1
        result = self._dataset_result(intent, control_class, disposable, 1)
        self.dataset_files[(intent.provider_ref, 1)] = dict(files)
        return result

    def create_private_dataset_from_directory(  # type: ignore[no-untyped-def]
        self, *, intent, source_directory, title, control_class, disposable
    ):
        files = {
            path.relative_to(source_directory).as_posix(): path.read_bytes()
            for path in source_directory.rglob("*")
            if path.is_file()
        }
        return self.create_private_dataset(
            intent=intent,
            files=files,
            title=title,
            control_class=control_class,
            disposable=disposable,
        )

    def create_private_dataset_version(self, *, intent, claim, files, version_notes):  # type: ignore[no-untyped-def]
        assert files and version_notes
        self.version_calls += 1
        version = claim.provider_version + 1
        result = self._dataset_result(intent, claim.control_class, claim.disposable, version)
        self.dataset_files[(intent.provider_ref, version)] = dict(files)
        return result

    def read_private_dataset(self, *, provider_ref, version):  # type: ignore[no-untyped-def]
        return self.datasets[(provider_ref, version)]

    def download_mcp_dataset_batch_exact(self, *, claim, max_files, max_total_bytes):  # type: ignore[no-untyped-def]
        files = self.dataset_files[(claim.provider_ref, claim.provider_version)]
        assert len(files) <= max_files
        assert sum(map(len, files.values())) <= max_total_bytes
        return ExactDatasetBatch(
            identity=self.datasets[(claim.provider_ref, claim.provider_version)],
            files=tuple(
                ExactDatasetBatchFile(
                    path=path,
                    byte_size=len(content),
                    sha256=hashlib.sha256(content).hexdigest(),
                    content=content,
                )
                for path, content in sorted(files.items())
            ),
        )

    def list_private_dataset_files_exact(self, *, claim, **_kwargs):  # type: ignore[no-untyped-def]
        return tuple(
            (path, len(content))
            for path, content in sorted(
                self.dataset_files[(claim.provider_ref, claim.provider_version)].items()
            )
        )

    def download_mcp_dataset_file_exact(  # type: ignore[no-untyped-def]
        self, *, claim, path, expected_size, expected_sha256
    ):
        content = self.dataset_files[(claim.provider_ref, claim.provider_version)][path]
        assert len(content) == expected_size
        assert hashlib.sha256(content).hexdigest() == expected_sha256
        return ExactDatasetBatchFile(
            path=path,
            byte_size=expected_size,
            sha256=expected_sha256,
            content=content,
        )

    def push_private_notebook(self, *, intent, task_run_id, source, control_class, disposable, **kwargs):  # type: ignore[no-untyped-def]
        self.run_calls += 1
        self.run_intent_arguments_sha256 = intent.arguments_sha256
        self.run_dataset_sources = tuple(kwargs.get("dataset_sources", ()))
        self.run_runtime_options = (
            bool(kwargs.get("enable_internet", False)),
            str(kwargs.get("accelerator", "none")),
        )
        source_identity = KaggleKernelSourceIdentity(
            provider_ref=intent.provider_ref,
            source_version=1,
            privacy="private",
            source_sha256="1" * 64,
            fingerprint=ProviderFingerprint(value="2" * 64),
            observed_at=self.now(),
        )
        run = KaggleKernelRunIdentity(
            task_run_id=task_run_id,
            provider_ref=intent.provider_ref,
            source_version=1,
            source_sha256=source_identity.source_sha256,
            provider_kernel_id=123,
            provider_run_ref=f"{intent.provider_ref}/1",
            started_at=self.now(),
        )
        receipt = ProviderEffectReceipt(
            operation_id=intent.operation_id,
            effect_id=intent.effect_id,
            action=MutationAction.PUSH_NOTEBOOK,
            provider_ref=intent.provider_ref,
            outcome=EffectOutcome.APPLIED,
            attempts=1,
            observed_fingerprint=source_identity.fingerprint,
            provider_version=1,
            observed_at=self.now(),
            detail_code="fake_run",
        )
        claim = TaskResourceClaim.create(
            task_id=intent.task_id,
            effect_id=intent.effect_id,
            provider_ref=intent.provider_ref,
            kind=ProviderKind.NOTEBOOK,
            control_class=control_class,
            disposable=disposable,
            fingerprint=source_identity.fingerprint,
            provider_version=1,
            registered_at=self.now(),
        )
        self.journal.persist_intent(intent)
        self.journal.persist_receipt(receipt)
        self.journal.persist_resource_claim(claim)
        self.sources[(intent.provider_ref, 1)] = source_identity
        return NotebookMutationResult(source=source_identity, run=run, claim=claim, effect=receipt)

    def read_private_notebook_source(self, *, provider_ref, source_version, expected_source_sha256):  # type: ignore[no-untyped-def]
        assert expected_source_sha256 is None
        return self.sources[(provider_ref, source_version)]

    def delete_task_created_resource(self, *, intent, claim):  # type: ignore[no-untyped-def]
        existing = self.delete_receipts.get(str(intent.effect_id))
        if existing is not None:
            return existing.model_copy(update={"outcome": EffectOutcome.ALREADY_APPLIED, "attempts": 0})
        self.delete_calls += 1
        receipt = ProviderEffectReceipt(
            operation_id=intent.operation_id,
            effect_id=intent.effect_id,
            action=intent.action,
            provider_ref=intent.provider_ref,
            outcome=EffectOutcome.APPLIED,
            attempts=1,
            observed_at=self.now(),
            detail_code="fake_deleted",
        )
        self.journal.persist_intent(intent)
        self.journal.persist_receipt(receipt)
        self.delete_receipts[str(intent.effect_id)] = receipt
        return receipt

    def poll_run(self, run, policy):  # type: ignore[no-untyped-def]
        assert policy.max_polls >= 1
        return KaggleKernelStatus(
            run=run,
            state=KernelState.COMPLETE,
            provider_status="complete",
            observed_at=self.now(),
        )

    def read_run_status(self, run):  # type: ignore[no-untyped-def]
        return KaggleKernelStatus(
            run=run,
            state=KernelState.COMPLETE,
            provider_status="complete",
            observed_at=self.now(),
        )

    def download_exact_run_output_file(  # type: ignore[no-untyped-def]
        self, run, *, destination, file_name, max_bytes
    ):
        assert len(self.output_bytes) <= max_bytes
        destination.mkdir(parents=True, exist_ok=True)
        (destination / file_name).write_bytes(self.output_bytes)
        return SimpleNamespace(output_tree_sha256="9" * 64, file_count=1)


def _dataset_input(result: dict[str, object]) -> dict[str, object]:
    return {
        "resource_ref": result["provider_ref"],
        "provider_version": result["provider_version"],
        "claim_sha256": result["claim_sha256"],
        "control_class": "mcp_managed",
    }


class InventoryOnlyAdapter:
    def __init__(self) -> None:
        self.calls: list[tuple[ProviderKind, str | None, int]] = []

    def list_resources(
        self, *, kind: ProviderKind, cursor: str | None, limit: int
    ) -> InventoryPage:
        self.calls.append((kind, cursor, limit))
        slug = "checkpoints" if kind is ProviderKind.DATASET else "master"
        return InventoryPage(
            resources=(
                ObservedProviderResource(
                    provider="kaggle",
                    provider_ref=f"owner/{slug}",
                    kind=kind,
                    owner="owner",
                    private=True,
                    fingerprint=ProviderFingerprint(value=("a" if kind is ProviderKind.DATASET else "b") * 64),
                    state="complete",
                    observed_at=datetime(2026, 8, 12, tzinfo=UTC),
                ),
            ),
            next_cursor=None,
        )


@pytest.mark.asyncio
async def test_live_inventory_uses_only_injected_control_adapter_and_operator_scope(
    tmp_path: Path,
) -> None:
    ledger = ControlLedger(tmp_path / "provider.sqlite3")
    adapter = InventoryOnlyAdapter()
    gateway = KaggleMCPProviderGateway(ledger, adapter)  # type: ignore[arg-type]
    control = LedgerControlReader(ledger, provider_gateway=gateway)
    service = HubService(
        control=control,
        fallback_identity=principal(),
        scopes=principal().scopes,
    )

    result = await service.invoke("provider.inventory.live", {"limit": 10})

    assert result["bounded"] is True
    assert result["complete"] is True
    assert result["count"] == 2
    assert [item["provider_ref"] for item in result["resources"]] == [
        "owner/checkpoints",
        "owner/master",
    ]
    assert [call[0] for call in adapter.calls] == [ProviderKind.DATASET, ProviderKind.NOTEBOOK]
    assert [call[2] for call in adapter.calls] == [20, 20]
    with pytest.raises(PermissionError, match="provider operator scope"):
        gateway.invoke(
            "provider.inventory.live",
            {"limit": 100},
            replace(principal(), scopes=frozenset({"provider:read"})),
        )


def test_ledger_control_reader_routes_every_chunked_upload_tool_to_provider_gateway(
    tmp_path: Path,
) -> None:
    ledger = ControlLedger(tmp_path / "production-dispatch.sqlite3")

    class RecordingGateway:
        def __init__(self) -> None:
            self.calls: list[tuple[str, dict[str, object], str]] = []

        def invoke(self, tool, arguments, caller):  # type: ignore[no-untyped-def]
            self.calls.append((tool, dict(arguments), caller.subject))
            return {"routed_tool": tool}

    gateway = RecordingGateway()
    control = LedgerControlReader(ledger, provider_gateway=gateway)  # type: ignore[arg-type]
    tools = (
        "provider.upload.start",
        "provider.upload.put_chunk",
        "provider.upload.status",
        "provider.upload.finalize",
        "provider.upload.abort",
    )

    for tool in tools:
        arguments = {"dispatch_marker": tool}
        assert control.invoke_control(tool, arguments, principal()) == {
            "routed_tool": tool
        }

    assert gateway.calls == [
        (tool, {"dispatch_marker": tool}, "owner") for tool in tools
    ]


def _run_request(
    *,
    task_id: object,
    dataset_inputs: list[dict[str, object]],
    notebook_ref: str = "owner/mcp-notebook",
    idempotency_key: str | None = None,
    enable_internet: bool = False,
    accelerator: str = "none",
    expected_outputs: list[dict[str, object]] | None = None,
) -> dict[str, object]:
    task_run_id = uuid4()
    return {
        "resource_ref": notebook_ref,
        "control_class": "mcp_managed",
        "private": True,
        "payload": {
            "kind": "notebook",
            "task_id": str(task_id),
            "effect_id": str(uuid4()),
            "idempotency_key": idempotency_key or f"provider-run-{uuid4()}",
            "task_run_id": str(task_run_id),
            "title": "mcp-notebook",
            "code_file": "worker.py",
            "kernel_type": "script",
            "language": "python",
            "source_utf8": f"# {task_run_id}\nprint('ok')\n",
            "dataset_inputs": dataset_inputs,
            "disposable": True,
            "enable_internet": enable_internet,
            "accelerator": accelerator,
            "expected_outputs": expected_outputs or [],
        },
    }


def test_single_provider_gateway_uses_exact_claims_and_metadata_only_ledger(tmp_path: Path) -> None:
    ledger = ControlLedger(tmp_path / "provider.sqlite3")
    adapter = FakeAdapter(ledger)
    gateway = KaggleMCPProviderGateway(ledger, adapter)  # type: ignore[arg-type]
    task_id = uuid4()
    common = {
        "resource_ref": "owner/mcp-data",
        "control_class": "mcp_managed",
        "private": True,
    }
    created = gateway.invoke(
        "provider.resources.create",
        {
            **common,
            "payload": {
                "kind": "dataset",
                "task_id": str(task_id),
                "effect_id": str(uuid4()),
                "idempotency_key": "provider-create-1",
                "title": "MCP data",
                "disposable": True,
                "files": {"payload.txt": "must-not-be-journaled"},
            },
        },
        principal(),
    )
    assert created["provider_version"] == 1
    versioned = gateway.invoke(
        "provider.resources.version",
        {
            **common,
            "payload": {
                "kind": "dataset",
                "task_id": str(task_id),
                "effect_id": str(uuid4()),
                "idempotency_key": "provider-version-1",
                "claim_sha256": created["claim_sha256"],
                "version_notes": "second exact version",
                "files": {"payload.txt": "second"},
            },
        },
        principal(),
    )
    readback = gateway.invoke(
        "provider.resources.read",
        {**common, "payload": {"kind": "dataset", "claim_sha256": versioned["claim_sha256"]}},
        principal(),
    )
    assert readback["provider_version"] == 2
    assert b"must-not-be-journaled" not in ledger.path.read_bytes()

    task_run_id = uuid4()
    run_request = _run_request(
        task_id=task_id,
        dataset_inputs=[_dataset_input(created)],
        idempotency_key="provider-run-1",
    )
    run_request["payload"]["task_run_id"] = str(task_run_id)  # type: ignore[index]
    notebook = gateway.invoke("provider.resources.run", run_request, principal())
    assert adapter.run_dataset_sources == ("owner/mcp-data/1",)
    # The gateway resolves claim-bound inputs into exact numeric Kaggle
    # sources.  The provider adapter's effect contract deliberately contains
    # only those provider-facing sources; control-plane claim metadata must
    # not be added as an extra adapter argument or every real run is rejected
    # before the mutation journal is written.
    assert adapter.run_intent_arguments_sha256 == sha256_value(
        {
            "task_run_id": str(task_run_id),
            "source_sha256": hashlib.sha256(
                str(run_request["payload"]["source_utf8"]).encode("utf-8")  # type: ignore[index]
            ).hexdigest(),
            "dataset_sources": ("owner/mcp-data/1",),
            "control_class": "mcp_managed",
            "disposable": True,
        }
    )
    notebook_read = gateway.invoke(
        "provider.resources.read",
        {
            "resource_ref": "owner/mcp-notebook",
            "control_class": "mcp_managed",
            "private": True,
            "payload": {"kind": "notebook", "claim_sha256": notebook["claim_sha256"]},
        },
        principal(),
    )
    assert notebook_read["task_run_id"] == str(task_run_id)
    assert notebook_read["provider_kernel_id"] == 123
    assert notebook_read["run_state"] == "complete"
    assert notebook_read["terminal"] is True
    deleted = gateway.invoke(
        "provider.resources.delete",
        {
            "resource_ref": "owner/mcp-notebook",
            "control_class": "mcp_managed",
            "private": True,
            "payload": {
                "kind": "notebook",
                "task_id": notebook["task_id"],
                "effect_id": str(uuid4()),
                "idempotency_key": "provider-delete-1",
                "claim_sha256": notebook["claim_sha256"],
            },
        },
        principal(),
    )
    assert deleted["outcome"] == "applied"


def test_provider_run_exposes_bounded_runtime_options_status_and_declared_outputs(
    tmp_path: Path,
) -> None:
    ledger = ControlLedger(tmp_path / "provider-runtime.sqlite3")
    adapter = FakeAdapter(ledger)
    gateway = KaggleMCPProviderGateway(ledger, adapter)  # type: ignore[arg-type]
    task_id = uuid4()
    request = _run_request(
        task_id=task_id,
        dataset_inputs=[],
        notebook_ref="owner/internet-notebook",
        idempotency_key="provider-internet-run-1",
        enable_internet=True,
        accelerator="gpu",
        expected_outputs=[
            {
                "path": "transcript.srt",
                "max_bytes": 1_048_576,
                "media_type": "application/x-subrip",
            }
        ],
    )

    notebook = gateway.invoke("provider.resources.run", request, principal())
    assert adapter.run_runtime_options == (True, "gpu")
    payload = request["payload"]
    assert isinstance(payload, dict)
    assert adapter.run_intent_arguments_sha256 == sha256_value(
        {
            "task_run_id": payload["task_run_id"],
            "source_sha256": hashlib.sha256(str(payload["source_utf8"]).encode()).hexdigest(),
            "dataset_sources": (),
            "control_class": "mcp_managed",
            "disposable": True,
            "enable_internet": True,
            "accelerator": "gpu",
        }
    )

    listing = gateway.invoke(
        "provider.resources.list",
        {
            "resource_ref": "owner/internet-notebook",
            "control_class": "mcp_managed",
            "private": True,
            "payload": {
                "kind": "notebook",
                "claim_sha256": notebook["claim_sha256"],
                "cursor": 0,
                "limit": 50,
            },
        },
        principal(),
    )
    assert listing["contract_version"] == "my-data-hub-mcp-notebook-outputs.v1"
    assert listing["run_state"] == "complete"
    assert listing["outputs"] == [
        {
            "path": "transcript.srt",
            "max_bytes": 1_048_576,
            "media_type": "application/x-subrip",
        }
    ]
    projection = ledger.provider_resource("owner/internet-notebook", "1")
    assert projection is not None
    assert projection["state"] == "complete"

    chunk = gateway.invoke(
        "provider.resources.download",
        {
            "resource_ref": "owner/internet-notebook",
            "control_class": "mcp_managed",
            "private": True,
            "payload": {
                "kind": "notebook",
                "claim_sha256": notebook["claim_sha256"],
                "path": "transcript.srt",
                "offset": 0,
                "max_bytes": 131_072,
            },
        },
        principal(),
    )
    assert chunk["contract_version"] == "my-data-hub-mcp-notebook-output-chunk.v1"
    assert b64decode(chunk["content_base64"], validate=True) == adapter.output_bytes
    assert chunk["complete"] is True


def test_provider_notebook_read_projects_terminal_status_to_control_ledger(tmp_path: Path) -> None:
    ledger = ControlLedger(tmp_path / "provider-read-status.sqlite3")
    adapter = FakeAdapter(ledger)
    gateway = KaggleMCPProviderGateway(ledger, adapter)  # type: ignore[arg-type]
    request = _run_request(
        task_id=uuid4(),
        dataset_inputs=[],
        notebook_ref="owner/read-status-notebook",
        idempotency_key="provider-read-status-1",
    )
    notebook = gateway.invoke("provider.resources.run", request, principal())
    before = ledger.provider_resource("owner/read-status-notebook", "1")
    assert before is not None and before["state"] == "running"

    observed = gateway.invoke(
        "provider.resources.read",
        {
            "resource_ref": "owner/read-status-notebook",
            "control_class": "mcp_managed",
            "private": True,
            "payload": {"kind": "notebook", "claim_sha256": notebook["claim_sha256"]},
        },
        principal(),
    )

    assert observed["run_state"] == "complete"
    after = ledger.provider_resource("owner/read-status-notebook", "1")
    assert after is not None and after["state"] == "complete"


def test_provider_delete_projects_absence_and_blocks_all_notebook_reads(tmp_path: Path) -> None:
    ledger = ControlLedger(tmp_path / "provider-delete-absence.sqlite3")
    adapter = FakeAdapter(ledger)
    gateway = KaggleMCPProviderGateway(ledger, adapter)  # type: ignore[arg-type]
    request = _run_request(
        task_id=uuid4(),
        dataset_inputs=[],
        notebook_ref="owner/deleted-notebook",
        idempotency_key="provider-deleted-run-1",
        expected_outputs=[
            {"path": "result.json", "max_bytes": 4096, "media_type": "application/json"}
        ],
    )
    notebook = gateway.invoke("provider.resources.run", request, principal())
    gateway.invoke(
        "provider.resources.delete",
        {
            "resource_ref": "owner/deleted-notebook",
            "control_class": "mcp_managed",
            "private": True,
            "payload": {
                "kind": "notebook",
                "task_id": notebook["task_id"],
                "effect_id": str(uuid4()),
                "idempotency_key": "provider-deleted-cleanup-1",
                "claim_sha256": notebook["claim_sha256"],
            },
        },
        principal(),
    )
    projection = ledger.provider_resource("owner/deleted-notebook", "1")
    assert projection is not None and projection["state"] == "absent"

    def unexpected(*_args, **_kwargs):  # type: ignore[no-untyped-def]
        raise AssertionError("absent projection must short-circuit provider reads")

    adapter.read_private_notebook_source = unexpected  # type: ignore[method-assign]
    adapter.read_run_status = unexpected  # type: ignore[method-assign]
    adapter.download_exact_run_output_file = unexpected  # type: ignore[method-assign]
    common = {
        "resource_ref": "owner/deleted-notebook",
        "control_class": "mcp_managed",
        "private": True,
    }
    requests = (
        (
            "provider.resources.read",
            {**common, "payload": {"kind": "notebook", "claim_sha256": notebook["claim_sha256"]}},
        ),
        (
            "provider.resources.list",
            {
                **common,
                "payload": {
                    "kind": "notebook",
                    "claim_sha256": notebook["claim_sha256"],
                    "cursor": 0,
                    "limit": 50,
                },
            },
        ),
        (
            "provider.resources.download",
            {
                **common,
                "payload": {
                    "kind": "notebook",
                    "claim_sha256": notebook["claim_sha256"],
                    "path": "result.json",
                    "offset": 0,
                    "max_bytes": 4096,
                },
            },
        ),
    )
    for tool, arguments in requests:
        with pytest.raises(KaggleNotFound, match="absent"):
            gateway.invoke(tool, arguments, principal())


def test_provider_resource_observations_are_exact_monotonic_and_metadata_preserving(
    tmp_path: Path,
) -> None:
    ledger = ControlLedger(tmp_path / "provider-observation.sqlite3")
    identity = {
        "provider": "kaggle",
        "resource_ref": "owner/exact-resource",
        "resource_kind": "notebook",
        "source_identity": str(uuid4()),
        "source_version": "7",
        "control_class": "mcp_managed",
        "private": True,
    }
    ledger.register_provider_resource(**identity, state="running", metadata={"immutable": "value"})
    ledger.record_provider_resource_terminal_observation(**identity, state="complete")
    ledger.record_provider_resource_terminal_observation(**identity, state="complete")
    projection = ledger.provider_resource(identity["resource_ref"], identity["source_version"])
    assert projection is not None
    assert projection["state"] == "complete"
    assert projection["metadata"] == {"immutable": "value"}
    with pytest.raises(StaleRuntimeEvent, match="terminal"):
        ledger.record_provider_resource_terminal_observation(**identity, state="failed")

    ledger.record_provider_resource_absence(**identity)
    ledger.record_provider_resource_absence(**identity)
    projection = ledger.provider_resource(identity["resource_ref"], identity["source_version"])
    assert projection is not None and projection["state"] == "absent"
    assert projection["metadata"] == {"immutable": "value"}
    with pytest.raises(StaleRuntimeEvent, match="absent"):
        ledger.record_provider_resource_terminal_observation(**identity, state="complete")


def test_provider_run_denies_internet_when_private_dataset_inputs_are_attached(
    tmp_path: Path,
) -> None:
    ledger = ControlLedger(tmp_path / "provider-internet-denied.sqlite3")
    adapter = FakeAdapter(ledger)
    gateway = KaggleMCPProviderGateway(ledger, adapter)  # type: ignore[arg-type]
    task_id = uuid4()
    created = gateway.invoke(
        "provider.resources.create",
        {
            "resource_ref": "owner/private-input",
            "control_class": "mcp_managed",
            "private": True,
            "payload": {
                "kind": "dataset",
                "task_id": str(task_id),
                "effect_id": str(uuid4()),
                "idempotency_key": "private-input-create-1",
                "title": "Private input",
                "disposable": True,
                "files": {"input.txt": "private"},
            },
        },
        principal(),
    )
    request = _run_request(
        task_id=task_id,
        dataset_inputs=[_dataset_input(created)],
        enable_internet=True,
    )
    with pytest.raises(PermissionError, match=r"internet.*dataset inputs"):
        gateway.invoke("provider.resources.run", request, principal())
    assert adapter.run_calls == 0


def test_chunked_upload_finalize_uses_single_adapter_and_durable_manifest(tmp_path: Path) -> None:
    ledger = ControlLedger(tmp_path / "chunked.sqlite3")
    adapter = FakeAdapter(ledger)
    gateway = KaggleMCPProviderGateway(
        ledger,
        adapter,  # type: ignore[arg-type]
        upload_root=tmp_path / "provider-uploads",
    )
    task_id = uuid4()
    upload_id = uuid4()
    content = b"private-provider-bytes" * 20_000
    common = {
        "resource_ref": "owner/chunked-data",
        "control_class": "mcp_managed",
        "private": True,
    }
    start = {
        **common,
        "payload": {
            "kind": "dataset",
            "upload_id": str(upload_id),
            "task_id": str(task_id),
            "effect_id": str(uuid4()),
            "idempotency_key": "provider-chunked-create-1",
            "title": "MCP chunked private dataset",
            "disposable": True,
            "files": [
                {
                    "path": "nested/payload.bin",
                    "byte_size": len(content),
                    "sha256": hashlib.sha256(content).hexdigest(),
                }
            ],
            "ttl_seconds": 3600,
        },
    }
    assert gateway.invoke("provider.upload.start", start, principal())["state"] == "OPEN"
    for offset in range(0, len(content), 24 * 1024):
        chunk = content[offset : offset + 24 * 1024]
        gateway.invoke(
            "provider.upload.put_chunk",
            {
                **common,
                "payload": {
                    "upload_id": str(upload_id),
                    "task_id": str(task_id),
                    "path": "nested/payload.bin",
                    "offset": offset,
                    "encoding": "base64",
                    "content_base64": b64encode(chunk).decode("ascii"),
                    "byte_size": len(chunk),
                    "sha256": hashlib.sha256(chunk).hexdigest(),
                },
            },
            principal(),
        )
    finalized = gateway.invoke(
        "provider.upload.finalize",
        {
            **common,
            "payload": {"upload_id": str(upload_id), "task_id": str(task_id)},
        },
        principal(),
    )
    assert finalized["state"] == "FINALIZED"
    result = finalized["result"]
    assert result["provider_ref"] == "owner/chunked-data"
    assert adapter.create_calls == 1
    assert adapter.dataset_files[("owner/chunked-data", 1)] == {"nested/payload.bin": content}
    projection = ledger.provider_resource("owner/chunked-data", "1")
    assert projection is not None
    assert projection["metadata"]["content_manifest"] == [
        {
            "path": "nested/payload.bin",
            "byte_size": len(content),
            "sha256": hashlib.sha256(content).hexdigest(),
        }
    ]
    assert content not in ledger.path.read_bytes()
    assert not any((tmp_path / "provider-uploads" / "uploads").iterdir())


def test_binary_batch_round_trip_is_claim_bound_chunked_and_json_safe(tmp_path: Path) -> None:
    ledger = ControlLedger(tmp_path / "batch.sqlite3")
    adapter = FakeAdapter(ledger)
    gateway = KaggleMCPProviderGateway(ledger, adapter)  # type: ignore[arg-type]
    task_id = uuid4()
    content = b"\x00\xffbinary\n" * 20_000
    # Stay within the explicit 128 KiB per-file request contract.
    content = content[:131_072]
    digest = hashlib.sha256(content).hexdigest()
    common = {
        "resource_ref": "owner/mcp-binary",
        "control_class": "mcp_managed",
        "private": True,
    }
    created = gateway.invoke(
        "provider.resources.create",
        {
            **common,
            "payload": {
                "kind": "dataset",
                "task_id": str(task_id),
                "effect_id": str(uuid4()),
                "idempotency_key": "provider-binary-create-1",
                "title": "MCP binary batch",
                "disposable": True,
                "files": {
                    "nested/payload.bin": {
                        "encoding": "base64",
                        "content_base64": b64encode(content).decode("ascii"),
                        "byte_size": len(content),
                        "sha256": digest,
                    },
                    "readme.txt": "compatible UTF-8",
                },
            },
        },
        principal(),
    )
    listing = gateway.invoke(
        "provider.resources.list",
        {
            **common,
            "payload": {
                "kind": "dataset",
                "claim_sha256": created["claim_sha256"],
                "cursor": 0,
                "limit": 1,
            },
        },
        principal(),
    )
    assert listing["contract_version"] == "my-data-hub-mcp-dataset-batch.v1"
    assert listing["file_count"] == 2
    assert listing["next_cursor"] == 1
    assert "content" not in listing["files"][0]

    assembled = bytearray()
    offset = 0
    while True:
        chunk = gateway.invoke(
            "provider.resources.download",
            {
                **common,
                "payload": {
                    "kind": "dataset",
                    "claim_sha256": created["claim_sha256"],
                    "path": "nested/payload.bin",
                    "offset": offset,
                    "max_bytes": 32_768,
                },
            },
            principal(),
        )
        decoded = b64decode(chunk["content_base64"], validate=True)
        assert hashlib.sha256(decoded).hexdigest() == chunk["content_sha256"]
        assembled.extend(decoded)
        if chunk["complete"]:
            assert chunk["next_offset"] is None
            break
        offset = chunk["next_offset"]
    assert bytes(assembled) == content
    assert chunk["file_sha256"] == digest
    assert content not in ledger.path.read_bytes()


@pytest.mark.parametrize(
    ("path", "message"),
    [
        ("../escape.bin", "traversal"),
        ("PG_VERSION", "canonical database"),
        ("checkpoints/base.tar", "checkpoint"),
        ("snapshot.dump", "canonical database"),
        ("my-data-hub-resource.json", "metadata paths"),
    ],
)
def test_batch_upload_denies_traversal_reserved_and_canonical_artifacts(
    tmp_path: Path, path: str, message: str
) -> None:
    ledger = ControlLedger(tmp_path / "denied.sqlite3")
    adapter = FakeAdapter(ledger)
    gateway = KaggleMCPProviderGateway(ledger, adapter)  # type: ignore[arg-type]
    with pytest.raises((ValueError, PermissionError, KaggleContractError), match=message):
        gateway.invoke(
            "provider.resources.create",
            {
                "resource_ref": "owner/mcp-denied",
                "control_class": "mcp_managed",
                "private": True,
                "payload": {
                    "kind": "dataset",
                    "task_id": str(uuid4()),
                    "effect_id": str(uuid4()),
                    "idempotency_key": "provider-denied-create-1",
                    "title": "MCP denied batch",
                    "disposable": True,
                    "files": {path: "forbidden"},
                },
            },
            principal(),
        )
    assert adapter.create_calls == 0


def test_batch_rejects_hash_tamper_and_wrong_principal_before_adapter(tmp_path: Path) -> None:
    ledger = ControlLedger(tmp_path / "tamper.sqlite3")
    adapter = FakeAdapter(ledger)
    gateway = KaggleMCPProviderGateway(ledger, adapter)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="size or sha256"):
        gateway.invoke(
            "provider.resources.create",
            {
                "resource_ref": "owner/mcp-tamper",
                "control_class": "mcp_managed",
                "private": True,
                "payload": {
                    "kind": "dataset",
                    "task_id": str(uuid4()),
                    "effect_id": str(uuid4()),
                    "idempotency_key": "provider-tamper-create-1",
                    "title": "MCP tamper batch",
                    "disposable": True,
                    "files": {
                        "payload.bin": {
                            "encoding": "base64",
                            "content_base64": "AA==",
                            "byte_size": 1,
                            "sha256": "0" * 64,
                        }
                    },
                },
            },
            principal(),
        )

    assert adapter.create_calls == 0

    created = gateway.invoke(
        "provider.resources.create",
        {
            "resource_ref": "owner/mcp-owned",
            "control_class": "mcp_managed",
            "private": True,
            "payload": {
                "kind": "dataset",
                "task_id": str(uuid4()),
                "effect_id": str(uuid4()),
                "idempotency_key": "provider-owned-create-1",
                "title": "MCP owned batch",
                "disposable": True,
                "files": {"payload.txt": "owned"},
            },
        },
        principal(),
    )
    with pytest.raises(PermissionError, match="creating task and principal"):
        gateway.invoke(
            "provider.resources.download",
            {
                "resource_ref": "owner/mcp-owned",
                "control_class": "mcp_managed",
                "private": True,
                "payload": {
                    "kind": "dataset",
                    "claim_sha256": created["claim_sha256"],
                    "path": "payload.txt",
                    "offset": 0,
                    "max_bytes": 10,
                },
            },
            principal("other"),
        )


def test_mixed_binary_limit_is_independent_of_json_file_order() -> None:
    text = "x" * (270 * 1024)
    binary = {
        "encoding": "base64",
        "content_base64": "AA==",
        "byte_size": 1,
        "sha256": hashlib.sha256(b"\x00").hexdigest(),
    }
    first = KaggleMCPProviderGateway._files(
        {"binary.bin": binary, "large.txt": text}
    )
    last = KaggleMCPProviderGateway._files(
        {"large.txt": text, "binary.bin": binary}
    )
    assert first == last == {"binary.bin": b"\x00", "large.txt": text.encode()}


def test_provider_run_rejects_legacy_raw_dataset_source_before_adapter(
    tmp_path: Path,
) -> None:
    ledger = ControlLedger(tmp_path / "provider.sqlite3")
    adapter = FakeAdapter(ledger)
    gateway = KaggleMCPProviderGateway(ledger, adapter)  # type: ignore[arg-type]

    request = _run_request(task_id=uuid4(), dataset_inputs=[])
    payload = request["payload"]
    assert isinstance(payload, dict)
    payload.pop("dataset_inputs")
    payload["dataset_sources"] = ["owner/orchestrator-checkpoints"]
    with pytest.raises(ValueError, match="exact contract"):
        gateway.invoke(
            "provider.resources.run",
            request,
            principal(),
        )

    assert adapter.run_dataset_sources is None


def test_provider_run_rejects_unregistered_and_inexact_input_claims_before_adapter(
    tmp_path: Path,
) -> None:
    ledger = ControlLedger(tmp_path / "provider.sqlite3")
    adapter = FakeAdapter(ledger)
    gateway = KaggleMCPProviderGateway(ledger, adapter)  # type: ignore[arg-type]
    task_id = uuid4()
    common = {"resource_ref": "owner/mcp-data", "control_class": "mcp_managed", "private": True}
    created = gateway.invoke(
        "provider.resources.create",
        {
            **common,
            "payload": {
                "kind": "dataset",
                "task_id": str(task_id),
                "effect_id": str(uuid4()),
                "idempotency_key": "provider-create-input-bounds",
                "title": "MCP data",
                "disposable": True,
                "files": {"payload.txt": "bounded"},
            },
        },
        principal(),
    )
    exact = _dataset_input(created)
    forbidden_inputs = [
        ({**exact, "provider_version": "latest"}, "exact numeric"),
        ({**exact, "provider_version": 2}, "exact numeric version"),
        ({**exact, "claim_sha256": "a" * 64}, "no exact registered claim"),
        ({**exact, "control_class": "orchestrator_protected"}, "control class is forbidden"),
        ({**exact, "control_class": "external_read_only"}, "control class is forbidden"),
        ({**exact, "control_class": "unknown"}, "control class is forbidden"),
    ]
    for dataset_input, message in forbidden_inputs:
        with pytest.raises(PermissionError, match=message):
            gateway.invoke(
                "provider.resources.run",
                _run_request(task_id=task_id, dataset_inputs=[dataset_input]),
                principal(),
            )

    assert adapter.run_calls == 0


def test_provider_run_enforces_managed_same_task_namespace_and_creator(
    tmp_path: Path,
) -> None:
    ledger = ControlLedger(tmp_path / "provider.sqlite3")
    adapter = FakeAdapter(ledger)
    gateway = KaggleMCPProviderGateway(ledger, adapter)  # type: ignore[arg-type]
    task_id = uuid4()
    created = gateway.invoke(
        "provider.resources.create",
        {
            "resource_ref": "owner/mcp-data",
            "control_class": "mcp_managed",
            "private": True,
            "payload": {
                "kind": "dataset",
                "task_id": str(task_id),
                "effect_id": str(uuid4()),
                "idempotency_key": "provider-create-owner-bounds",
                "title": "MCP data",
                "disposable": True,
                "files": {"payload.txt": "bounded"},
            },
        },
        principal(),
    )
    exact = _dataset_input(created)
    denied = [
        (_run_request(task_id=uuid4(), dataset_inputs=[exact]), principal()),
        (_run_request(task_id=task_id, dataset_inputs=[exact], notebook_ref="other/notebook"), principal()),
        (_run_request(task_id=task_id, dataset_inputs=[exact]), principal("different-principal")),
    ]
    for request, caller in denied:
        with pytest.raises(PermissionError, match="same task and owner"):
            gateway.invoke("provider.resources.run", request, caller)

    assert adapter.run_calls == 0


def _exchange_manifest(
    *,
    content: str,
    now: datetime,
    version: int = 1,
    creator: str = "owner",
    recipients: list[str] | None = None,
    sensitivity: str = "internal",
    path: str = "payload.txt",
    media_type: str = "text/plain",
    provider_ref: str = "owner/mcp-exchange",
) -> dict[str, object]:
    encoded = content.encode()
    payload: dict[str, object] = {
        "contract_version": "my-data-hub-kaggle-exchange.v1",
        "package_id": str(uuid4()),
        "control_class": "mcp_exchange",
        "private": True,
        "dataset_ref": provider_ref,
        "dataset_version": version,
        "created_at": now.isoformat(),
        "expires_at": (now + timedelta(days=1)).isoformat(),
        "created_by": creator,
        "purpose": "bounded provider gateway exchange",
        "target_project": "my-data-hub",
        "intended_recipients": recipients or ["recipient"],
        "sensitivity": sensitivity,
        "files": [
            {
                "path": path,
                "media_type": media_type,
                "byte_size": len(encoded),
                "sha256": hashlib.sha256(encoded).hexdigest(),
                "executable": False,
            }
        ],
    }
    payload["manifest_sha256"] = manifest_sha256(payload)
    return payload


def test_exchange_gateway_binds_creator_recipients_ttl_manifest_and_encryption(
    tmp_path: Path,
) -> None:
    clock = DeterministicClock(datetime(2026, 8, 11, 12, tzinfo=UTC))
    ledger = ControlLedger(tmp_path / "exchange.sqlite3", clock=clock)
    adapter = FakeAdapter(ledger)
    gateway = KaggleMCPProviderGateway(ledger, adapter)  # type: ignore[arg-type]
    common = {
        "resource_ref": "owner/mcp-exchange",
        "control_class": "mcp_exchange",
        "private": True,
    }
    content = "recipient-scoped content"
    manifest = _exchange_manifest(content=content, now=clock.now())
    created = gateway.invoke(
        "provider.resources.create",
        {
            **common,
            "payload": {
                "kind": "dataset",
                "task_id": str(uuid4()),
                "effect_id": str(uuid4()),
                "idempotency_key": "exchange-create-1",
                "title": "MCP exchange",
                "disposable": True,
                "files": {"payload.txt": content},
                "exchange_manifest": manifest,
            },
        },
        principal(),
    )
    stored = ledger.provider_resource("owner/mcp-exchange", "1")
    assert stored is not None
    assert stored["metadata"]["exchange_access"]["manifest_sha256"] == manifest["manifest_sha256"]
    assert content.encode() not in ledger.path.read_bytes()

    with pytest.raises(PermissionError, match="intended exchange recipient"):
        gateway.invoke(
            "provider.resources.read",
            {**common, "payload": {"kind": "dataset", "claim_sha256": created["claim_sha256"]}},
            principal(),
        )
    readback = gateway.invoke(
        "provider.resources.read",
        {**common, "payload": {"kind": "dataset", "claim_sha256": created["claim_sha256"]}},
        principal("recipient"),
    )
    assert readback["provider_version"] == 1

    with pytest.raises(PermissionError, match="only the exchange creator"):
        gateway.invoke(
            "provider.resources.delete",
            {
                **common,
                "payload": {
                    "kind": "dataset",
                    "task_id": created["task_id"],
                    "effect_id": str(uuid4()),
                    "idempotency_key": "exchange-delete-denied",
                    "claim_sha256": created["claim_sha256"],
                },
            },
            principal("recipient"),
        )

    exchange_input = {
        "resource_ref": created["provider_ref"],
        "provider_version": created["provider_version"],
        "claim_sha256": created["claim_sha256"],
        "control_class": "mcp_exchange",
    }
    gateway.invoke(
        "provider.resources.run",
        _run_request(task_id=uuid4(), dataset_inputs=[exchange_input]),
        principal("recipient"),
    )
    assert adapter.run_dataset_sources == ("owner/mcp-exchange/1",)
    assert adapter.run_calls == 1
    with pytest.raises(PermissionError, match="intended exchange recipient"):
        gateway.invoke(
            "provider.resources.run",
            _run_request(task_id=uuid4(), dataset_inputs=[exchange_input]),
            principal(),
        )
    assert adapter.run_calls == 1

    clock.advance(delta=timedelta(days=2))
    with pytest.raises(PermissionError, match="exchange resource has expired"):
        gateway.invoke(
            "provider.resources.run",
            _run_request(task_id=uuid4(), dataset_inputs=[exchange_input]),
            principal("recipient"),
        )
    assert adapter.run_calls == 1
    with pytest.raises(PermissionError, match="exchange resource has expired"):
        gateway.invoke(
            "provider.resources.read",
            {**common, "payload": {"kind": "dataset", "claim_sha256": created["claim_sha256"]}},
            principal("recipient"),
        )
    with pytest.raises(PermissionError, match="exchange resource has expired"):
        gateway.invoke(
            "provider.resources.version",
            {
                **common,
                "payload": {
                    "kind": "dataset",
                    "task_id": created["task_id"],
                    "effect_id": str(uuid4()),
                    "idempotency_key": "exchange-expired-version-denied",
                    "claim_sha256": created["claim_sha256"],
                    "version_notes": "must remain denied",
                    "files": {"payload.txt": "forbidden"},
                    "exchange_manifest": {},
                },
            },
            principal(),
        )
    assert adapter.version_calls == 0

    expired_cleanup = {
        **common,
        "payload": {
            "kind": "dataset",
            "task_id": created["task_id"],
            "effect_id": str(uuid4()),
            "idempotency_key": "exchange-expired-retention-cleanup",
            "claim_sha256": created["claim_sha256"],
        },
    }
    with pytest.raises(PermissionError, match="only the exchange creator"):
        gateway.invoke("provider.resources.delete", expired_cleanup, principal("recipient"))
    cleaned = gateway.invoke("provider.resources.delete", expired_cleanup, principal())
    replayed = gateway.invoke("provider.resources.delete", expired_cleanup, principal())
    assert cleaned["outcome"] == "applied"
    assert replayed["outcome"] == "applied"
    assert adapter.delete_calls == 1
    assert cleaned["retention_receipt"] == replayed["retention_receipt"]
    assert cleaned["retention_receipt_sha256"] == replayed["retention_receipt_sha256"]
    assert cleaned["retention_receipt"]["maximum_ttl_seconds"] == 7 * 24 * 60 * 60
    assert cleaned["retention_receipt"]["resource_state"] == "absent"

    plaintext_confidential = _exchange_manifest(
        content="plaintext",
        now=clock.now(),
        sensitivity="confidential_encrypted",
        provider_ref="owner/mcp-exchange-confidential",
    )
    with pytest.raises(ValueError, match="armored age ciphertext"):
        gateway.invoke(
            "provider.resources.create",
            {
                **{**common, "resource_ref": "owner/mcp-exchange-confidential"},
                "payload": {
                    "kind": "dataset",
                    "task_id": str(uuid4()),
                    "effect_id": str(uuid4()),
                    "idempotency_key": "exchange-confidential-1",
                    "title": "Encrypted exchange",
                    "disposable": True,
                    "files": {"payload.txt": "plaintext"},
                    "exchange_manifest": plaintext_confidential,
                },
            },
            principal(),
        )
