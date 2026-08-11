from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from my_data_hub.control_plane.adapters import KaggleMCPProviderGateway, LedgerWriteGate
from my_data_hub.control_plane.clock import DeterministicClock
from my_data_hub.control_plane.ledger import ControlLedger
from my_data_hub.mcp.contracts import MasterSnapshot, MasterState
from my_data_hub.mcp.oauth import AccessIdentity
from my_data_hub.providers.kaggle import ControlLedgerKaggleJournal
from my_data_hub.providers.kaggle.contracts import (
    DatasetMutationResult,
    EffectOutcome,
    KaggleDatasetIdentity,
    KaggleKernelRunIdentity,
    KaggleKernelSourceIdentity,
    MutationAction,
    NotebookMutationResult,
    ProviderEffectReceipt,
    TaskResourceClaim,
)
from my_data_hub.providers.models import ProviderFingerprint, ProviderKind


def principal() -> AccessIdentity:
    return AccessIdentity(
        subject="owner",
        client_id="owner-operator",
        scopes=frozenset({"data:write", "provider:write"}),
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


class FakeAdapter:
    def __init__(self, ledger: ControlLedger) -> None:
        self.journal = ControlLedgerKaggleJournal(ledger)
        self.now = ledger.clock.now
        self.datasets: dict[tuple[str, int], KaggleDatasetIdentity] = {}
        self.sources: dict[tuple[str, int], KaggleKernelSourceIdentity] = {}

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
        return self._dataset_result(intent, control_class, disposable, 1)

    def create_private_dataset_version(self, *, intent, claim, files, version_notes):  # type: ignore[no-untyped-def]
        assert files and version_notes
        return self._dataset_result(intent, claim.control_class, claim.disposable, claim.provider_version + 1)

    def read_private_dataset(self, *, provider_ref, version):  # type: ignore[no-untyped-def]
        return self.datasets[(provider_ref, version)]

    def push_private_notebook(self, *, intent, task_run_id, source, control_class, disposable, **_kwargs):  # type: ignore[no-untyped-def]
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
        return receipt


def test_single_provider_gateway_uses_exact_claims_and_metadata_only_ledger(tmp_path: Path) -> None:
    ledger = ControlLedger(tmp_path / "provider.sqlite3")
    gateway = KaggleMCPProviderGateway(ledger, FakeAdapter(ledger))  # type: ignore[arg-type]
    common = {
        "resource_ref": "owner/mcp-data",
        "control_class": "mcp_exchange",
        "private": True,
    }
    created = gateway.invoke(
        "provider.resources.create",
        {
            **common,
            "payload": {
                "kind": "dataset",
                "task_id": str(uuid4()),
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
    notebook = gateway.invoke(
        "provider.resources.run",
        {
            "resource_ref": "owner/mcp-notebook",
            "control_class": "mcp_managed",
            "private": True,
            "payload": {
                "kind": "notebook",
                "task_id": str(uuid4()),
                "effect_id": str(uuid4()),
                "idempotency_key": "provider-run-1",
                "task_run_id": str(task_run_id),
                "title": "mcp-notebook",
                "code_file": "worker.py",
                "kernel_type": "script",
                "language": "python",
                "source_utf8": f"# {task_run_id}\nprint('ok')\n",
                "dataset_sources": [],
                "disposable": True,
            },
        },
        principal(),
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
