from __future__ import annotations

import hashlib
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest

from my_data_hub.control_plane.adapters import KaggleMCPProviderGateway, LedgerControlReader, LedgerWriteGate
from my_data_hub.control_plane.clock import DeterministicClock
from my_data_hub.control_plane.ledger import ControlLedger
from my_data_hub.mcp.contracts import MasterSnapshot, MasterState
from my_data_hub.mcp.oauth import AccessIdentity
from my_data_hub.mcp.service import HubService
from my_data_hub.providers.exchange import manifest_sha256
from my_data_hub.providers.kaggle import ControlLedgerKaggleJournal
from my_data_hub.providers.kaggle.contracts import (
    DatasetMutationResult,
    EffectOutcome,
    KaggleDatasetIdentity,
    KaggleKernelRunIdentity,
    KaggleKernelSourceIdentity,
    KaggleKernelStatus,
    KernelState,
    MutationAction,
    NotebookMutationResult,
    ProviderEffectReceipt,
    TaskResourceClaim,
)
from my_data_hub.providers.models import ProviderFingerprint, ProviderKind


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
        self.sources: dict[tuple[str, int], KaggleKernelSourceIdentity] = {}
        self.output_bytes = b'{"accepted":true}'
        self.create_calls = 0
        self.version_calls = 0
        self.run_dataset_sources: tuple[str, ...] | None = None
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
        return self._dataset_result(intent, control_class, disposable, 1)

    def create_private_dataset_version(self, *, intent, claim, files, version_notes):  # type: ignore[no-untyped-def]
        assert files and version_notes
        self.version_calls += 1
        return self._dataset_result(intent, claim.control_class, claim.disposable, claim.provider_version + 1)

    def read_private_dataset(self, *, provider_ref, version):  # type: ignore[no-untyped-def]
        return self.datasets[(provider_ref, version)]

    def push_private_notebook(self, *, intent, task_run_id, source, control_class, disposable, **kwargs):  # type: ignore[no-untyped-def]
        self.run_calls += 1
        self.run_dataset_sources = tuple(kwargs.get("dataset_sources", ()))
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


def _run_request(
    *,
    task_id: object,
    dataset_inputs: list[dict[str, object]],
    notebook_ref: str = "owner/mcp-notebook",
    idempotency_key: str | None = None,
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
                "task_id": str(uuid4()),
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
