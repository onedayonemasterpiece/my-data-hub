from __future__ import annotations

import hashlib
import sqlite3
from pathlib import Path
from uuid import uuid4

import pytest
from test_mcp_operator_provider import FakeAdapter

from my_data_hub.control_plane.acceptance_evidence import AcceptanceEvidenceController
from my_data_hub.control_plane.adapters import KaggleMCPProviderGateway, LedgerControlReader
from my_data_hub.control_plane.ledger import ControlLedger
from my_data_hub.mcp.catalog import READER_PROFILE_SCOPES, TOOL_CONTRACTS
from my_data_hub.mcp.oauth import AccessIdentity


def principal() -> AccessIdentity:
    return AccessIdentity(
        subject="acceptance-owner",
        client_id="acceptance-client",
        scopes=frozenset({"provider:write", "acceptance:probe"}),
        audience="mcp",
        token_id="token",
        expires_at=2_100_000_000,
        issuer="https://issuer.example",
        issued_at=2_000_000_000,
        resource="https://mcp.example/mcp",
    )


def dataset_request(*, task_id: str | None = None) -> dict[str, object]:
    first = "synthetic-acceptance-v1"
    second = "synthetic-acceptance-v2"
    return {
        "scenario_id": "FM01",
        "task_id": task_id or str(uuid4()),
        "idempotency_key": "fm01-dataset-lifecycle",
        "resource_ref": "owner/fm01-dataset",
        "title": "FM01 dataset",
        "file_name": "acceptance.txt",
        "file_sha256": hashlib.sha256(first.encode()).hexdigest(),
        "file_utf8": first,
        "version_file_sha256": hashlib.sha256(second.encode()).hexdigest(),
        "version_file_utf8": second,
    }


def notebook_request() -> dict[str, object]:
    task_run_id = str(uuid4())
    output = b'{"accepted":true}'
    return {
        "scenario_id": "FM02",
        "task_id": str(uuid4()),
        "task_run_id": task_run_id,
        "idempotency_key": "fm02-notebook-lifecycle",
        "resource_ref": "owner/fm02-notebook",
        "title": "fm02-notebook",
        "code_file": "run.py",
        "source_utf8": f"# exact task {task_run_id}\nprint('acceptance')\n",
        "dataset_sources": [],
        "output_file_name": "operational-result.json",
        "expected_output_sha256": hashlib.sha256(output).hexdigest(),
        "max_output_bytes": 4096,
    }


def test_dataset_claim_precedes_effect_and_response_loss_reconciles_without_duplicate_create(
    tmp_path: Path,
) -> None:
    ledger = ControlLedger(tmp_path / "control.sqlite3")
    adapter = FakeAdapter(ledger)
    gateway = KaggleMCPProviderGateway(ledger, adapter)  # type: ignore[arg-type]
    request = dataset_request()

    class LoseFirstCreateResponse:
        def __init__(self) -> None:
            self.adapter = adapter
            self.lost = False

        def invoke(self, tool, arguments, identity):  # type: ignore[no-untyped-def]
            result = gateway.invoke(tool, arguments, identity)
            if tool == "provider.resources.create" and not self.lost:
                self.lost = True
                raise KeyboardInterrupt("simulated process loss after durable provider claim")
            return result

    controller = AcceptanceEvidenceController(ledger, LoseFirstCreateResponse())  # type: ignore[arg-type]
    with pytest.raises(KeyboardInterrupt):
        controller.dataset_lifecycle(request, principal())
    claimed = ledger.acceptance_evidence_task(
        scenario_id="FM01", task_id=str(request["task_id"])
    )
    assert claimed is not None
    assert claimed["state"] == "RUNNING"
    assert claimed["mutation_started"] is True

    reconciled = AcceptanceEvidenceController(ledger, gateway).dataset_lifecycle(request, principal())
    assert reconciled["state"] == "SUCCEEDED"
    assert reconciled["cleanup_state"] == "COMPLETE"
    assert adapter.create_calls == 1
    assert adapter.version_calls == 1
    assert adapter.delete_calls == 1
    replay = AcceptanceEvidenceController(ledger, gateway).dataset_lifecycle(request, principal())
    assert replay == reconciled
    assert adapter.create_calls == 1

    dump = "\n".join(sqlite3.connect(ledger.path).iterdump())
    assert "synthetic-acceptance-v1" not in dump
    assert "synthetic-acceptance-v2" not in dump


def test_notebook_claim_keeps_output_for_outer_reader_then_exact_cleanup_is_idempotent(
    tmp_path: Path,
) -> None:
    ledger = ControlLedger(tmp_path / "control.sqlite3")
    adapter = FakeAdapter(ledger)
    gateway = KaggleMCPProviderGateway(ledger, adapter)  # type: ignore[arg-type]
    controller = AcceptanceEvidenceController(ledger, gateway)
    request = notebook_request()

    result = controller.notebook_lifecycle(request, principal())
    assert result["state"] == "SUCCEEDED"
    assert result["cleanup_state"] == "PENDING"
    assert adapter.run_calls == 1
    assert adapter.delete_calls == 0
    notebook = next(item["evidence"] for item in result["evidence"] if item["event_type"] == "PROVIDER_NOTEBOOK")
    output = next(item["evidence"] for item in result["evidence"] if item["event_type"] == "OUTPUT_READ")
    assert isinstance(notebook["provider_version"], int)
    assert isinstance(notebook["provider_kernel_id"], int)
    assert notebook["provider_run_ref"].endswith("/1")
    assert notebook["fingerprint"]["value"] == "2" * 64
    assert output["output_file_sha256"] == request["expected_output_sha256"]

    cleanup = {
        "scenario_id": "FM02",
        "task_id": request["task_id"],
        "claim_sha256": notebook["claim_sha256"],
        "provider_run_ref": notebook["provider_run_ref"],
        "output_receipt_sha256": output["output_receipt_sha256"],
        "idempotency_key": "fm02-exact-cleanup",
    }
    cleaned = controller.cleanup(cleanup, principal())
    assert cleaned["cleanup_state"] == "COMPLETE"
    assert adapter.delete_calls == 1
    assert controller.cleanup(cleanup, principal()) == cleaned
    assert adapter.delete_calls == 1

    with pytest.raises(PermissionError, match="exact claim/run/output"):
        controller.cleanup({**cleanup, "output_receipt_sha256": "0" * 64}, principal())


def test_runtime_history_is_exact_bounded_metadata_and_tools_do_not_expand_reader_profile(
    tmp_path: Path,
) -> None:
    ledger = ControlLedger(tmp_path / "control.sqlite3")
    run_id, attempt_id, event_id = str(uuid4()), str(uuid4()), str(uuid4())
    connection = sqlite3.connect(ledger.path)
    connection.execute(
        "INSERT INTO runtime_event_dedup(event_id,body_sha256,first_seen_at) VALUES (?,?,?)",
        (event_id, "a" * 64, "2026-08-11T00:00:00Z"),
    )
    connection.execute(
        "INSERT INTO runtime_events(event_id,schema_version,run_id,attempt_id,service_instance_id,"
        "source_identity,source_version,epoch,event_type,emitted_at,received_at,local_sequence,body_sha256,"
        "body_bytes,sanitized_json) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            event_id,
            "content-runtime-event/v1",
            run_id,
            attempt_id,
            str(uuid4()),
            "owner/notebook",
            "1",
            7,
            "runtime.heartbeat",
            "2026-08-11T00:00:00Z",
            "2026-08-11T00:00:01Z",
            1,
            "a" * 64,
            123,
            '{"payload":"must-not-leave-control-ledger"}',
        ),
    )
    connection.commit()
    connection.close()

    reader = LedgerControlReader(ledger)
    history = reader.invoke_control(
        "runtime.events.history",
        {"run_id": run_id, "attempt_id": attempt_id, "epoch": 7, "limit": 10},
        principal(),
    )
    assert history["count"] == 1
    assert history["bounded"] is True
    assert history["events"][0]["event_type"] == "runtime.heartbeat"
    assert "sanitized_json" not in history["events"][0]
    assert reader.invoke_control(
        "runtime.events.history",
        {"run_id": run_id, "attempt_id": attempt_id, "epoch": 8, "limit": 10},
        principal(),
    )["count"] == 0

    for name in (
        "provider.acceptance.dataset.lifecycle",
        "provider.acceptance.notebook.lifecycle",
        "provider.acceptance.claim.get",
        "provider.acceptance.claim.cleanup",
    ):
        assert TOOL_CONTRACTS[name].role == "provider_operator"
        assert TOOL_CONTRACTS[name].scope == "provider:write"
    assert TOOL_CONTRACTS["runtime.events.history"].role == "operator"
    assert "provider:write" not in READER_PROFILE_SCOPES
    assert "acceptance:probe" not in READER_PROFILE_SCOPES


def test_mutation_failure_is_terminal_fail_never_blocked(tmp_path: Path) -> None:
    ledger = ControlLedger(tmp_path / "control.sqlite3")
    adapter = FakeAdapter(ledger)
    gateway = KaggleMCPProviderGateway(ledger, adapter)  # type: ignore[arg-type]
    controller = AcceptanceEvidenceController(ledger, gateway)
    request = dataset_request()
    request["resource_ref"] = "owner/missing-dataset"
    original = adapter.create_private_dataset

    def fail(**kwargs):  # type: ignore[no-untyped-def]
        raise RuntimeError("provider transport ambiguous")

    adapter.create_private_dataset = fail  # type: ignore[method-assign]
    with pytest.raises(Exception, match="terminalized as FAIL"):
        controller.dataset_lifecycle(request, principal())
    result = controller.claim_get("FM01", str(request["task_id"]))
    assert result["state"] == "FAILED"
    assert "BLOCKED" not in repr(result)
    adapter.create_private_dataset = original  # type: ignore[method-assign]
