from __future__ import annotations

import hashlib
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import pytest
from jsonschema import Draft202012Validator, FormatChecker

from my_data_hub.embeddings.production import (
    EXTERNAL_BLOCKED,
    WORKER_ASSETS,
    EmbeddingInterfacesUnavailable,
    EmbeddingProductionCapabilities,
    EmbeddingProductionConfig,
    EmbeddingProductionError,
    LocalEmbeddingProductionControl,
    embedding_provider_authority,
    run_embedding_production_closure,
)
from my_data_hub.hashing import canonical_json_bytes
from my_data_hub.workloads.bloggers.master_stage import (
    BloggerImportStageReceipt,
    BloggerMigrationRequest,
)
from scripts.embeddings.run_final_embedding_closure import main

SHA = "a" * 64
BLOGGER_CHECKPOINT = UUID("55555555-5555-4555-8555-555555555555")
EMBED_CHECKPOINT = UUID("66666666-6666-4666-8666-666666666666")


def _imported() -> BloggerImportStageReceipt:
    request = BloggerMigrationRequest(
        request_id=UUID("11111111-1111-4111-8111-111111111111"),
        operation_id=UUID("22222222-2222-4222-8222-222222222222"),
        project_id=UUID("33333333-3333-4333-8333-333333333333"),
        snapshot_at=datetime(2026, 8, 9, tzinfo=UTC),
        source_revision="b" * 40,
    )
    return BloggerImportStageReceipt(
        request_id=request.request_id,
        operation_id=request.operation_id,
        master_instance_id=UUID("44444444-4444-4444-8444-444444444444"),
        run_id="77777777-7777-4777-8777-777777777777",
        epoch=7,
        request_sha256=request.request_sha256,
        export_batch_id=UUID("88888888-8888-4888-8888-888888888888"),
        row_count=266,
        distinct_record_ids=266,
        source_file_count=14,
        dispositions={"imported": 266, "quarantined": 0},
        record_id_set_sha256=SHA,
        logical_sha256="b" * 64,
        canonical_outcome_sha256="c" * 64,
        actor_count=266,
        account_count=210,
        duplicate_group_count=0,
        replayed_count=0,
        canonical_revision=9,
    )


def _blogger_receipt() -> dict[str, object]:
    imported = _imported()
    return {
        "schema_version": "my-data-hub-blogger-closure.v1",
        "receipt_id": "99999999-9999-4999-8999-999999999999",
        "status": "DURABLE_COMPLETE",
        "started_at": "2026-08-10T00:00:00Z",
        "completed_at": "2026-08-10T01:00:00Z",
        "closure_idempotency_key_sha256": "d" * 64,
        "request_id": str(imported.request_id),
        "request_sha256": imported.request_sha256,
        "ensure_operation_id": str(imported.operation_id),
        "rotation_operation_id": "blogger-rotation",
        "import_runtime": {
            "run_id": imported.run_id,
            "attempt_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
            "master_instance_id": str(imported.master_instance_id),
            "epoch": imported.epoch,
        },
        "import_receipt": imported.model_dump(mode="json"),
        "import_receipt_sha256": hashlib.sha256(canonical_json_bytes(imported.model_dump(mode="json"))).hexdigest(),
        "checkpoint": {
            "generation": 2,
            "checkpoint_id": str(BLOGGER_CHECKPOINT),
            "exact_version_ref": "owner/blogger-checkpoints/2",
            "manifest_sha256": "e" * 64,
        },
        "cold_restore": {
            "master_instance_id": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
            "epoch": 8,
            "canonical_revision": imported.canonical_revision,
        },
        "mcp_accounting": {
            "export_batch_id": str(imported.export_batch_id),
            "expected_row_count": 266,
            "status": "accepted",
            "logical_sha256": imported.logical_sha256,
            "record_id_set_sha256": imported.record_id_set_sha256,
            "canonical_outcome_sha256": imported.canonical_outcome_sha256,
            "duplicate_groups_pending": 0,
            "imported_canonical_revision": imported.canonical_revision,
            "raw_count": 266,
            "dispositioned_count": 266,
            "undispositioned_count": 0,
            "quarantined_count": 0,
            "actor_count": 266,
            "account_count": imported.account_count,
            "checkpoint_required": True,
        },
        "mcp_statistics": {"bloggers": 266},
        "mcp_projection": {
            "listed_bloggers": 266,
            "get_found": True,
            "provenance_events": 1,
            "search_matches": 1,
            "completed_retrievers": ["exact", "fts"],
        },
    }


def _capabilities() -> dict[str, object]:
    return EmbeddingProductionCapabilities(
        ready=True,
        execution_location="active_kaggle_master",
        provider_adapter_package="kaggle",
        provider_adapter_version="2.2.4",
        provider_adapter_implementation="my_data_hub.providers.kaggle.KaggleProviderAdapter",
        single_provider_adapter=True,
        transactional_import=True,
        verified_checkpoint_restore=True,
        mcp_hybrid_search=True,
        worker_assets=WORKER_ASSETS,
    ).model_dump(mode="json")


def test_embedding_provider_authority_is_exact_and_distinct() -> None:
    request_id = UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
    first = embedding_provider_authority("owner", request_id)
    second = embedding_provider_authority("owner", request_id)
    assert first == second
    assert len(first) == 4
    assert len({task for _ref, task in first.values()}) == 2
    assert {ref for key, (ref, _task) in first.items() if key.endswith("_worker")} == {
        f"owner/{asset.notebook_slug}" for asset in WORKER_ASSETS
    }
    assert all(ref.startswith("owner/mdh-embed-") for key, (ref, _task) in first.items() if key.endswith("_input"))


def _worker(asset_index: int) -> dict[str, object]:
    asset = WORKER_ASSETS[asset_index]
    number = asset_index + 1
    return {
        "model_exact_id": asset.model.exact_id,
        "task_run_id": f"00000000-0000-4000-8000-{number:012d}",
        "provider_ref": f"owner/{asset.notebook_slug}",
        "provider_run_ref": f"owner/{asset.notebook_slug}/{number}",
        "provider_kernel_id": 100 + number,
        "source_version": number,
        "source_sha256": f"{number}" * 64,
        "primary_source_sha256": asset.primary_source_sha256,
        "provider_status": "complete",
        "privacy": "private",
        "control_class": "orchestrator_protected",
        "output_tree_sha256": f"{number + 2}" * 64,
        "artifact_sha256": f"{number + 4}" * 64,
        "artifact_id": f"10000000-0000-4000-8000-{number:012d}",
        "artifact_run_id": f"00000000-0000-4000-8000-{number:012d}",
        "input_dataset": {
            "provider_ref": f"owner/embedding-input-{number}",
            "provider_version": 1,
            "package_sha256": f"{number + 6}" * 64,
            "jobs_sha256": f"{number + 7}" * 64,
        },
    }


def _import(asset_index: int) -> dict[str, object]:
    asset = WORKER_ASSETS[asset_index]
    number = asset_index + 1
    return {
        "model_exact_id": asset.model.exact_id,
        "artifact_id": f"10000000-0000-4000-8000-{number:012d}",
        "run_id": f"00000000-0000-4000-8000-{number:012d}",
        "artifact_sha256": f"{number + 4}" * 64,
        "outbox_id": f"20000000-0000-4000-8000-{number:012d}",
        "canonical_revision": 9 + number,
        "inserted_count": 266,
        "stale_count": 0,
        "failed_count": 0,
        "replayed": False,
        "durability_state": "COMMITTED_PENDING_CHECKPOINT",
    }


def _coverage() -> list[dict[str, object]]:
    return [
        {
            "model_exact_id": asset.model.exact_id,
            "expected_documents": 266,
            "completed_documents": 266,
            "coverage": 1.0,
        }
        for asset in WORKER_ASSETS
    ]


class FakeControl:
    def __init__(self, *, available: bool = True) -> None:
        self.available = available
        self.created = 0
        self.request = None

    def capabilities(self) -> dict[str, object]:
        if not self.available:
            raise EmbeddingInterfacesUnavailable("not installed")
        return _capabilities()

    def create_request(self, request):  # type: ignore[no-untyped-def]
        self.created += 1
        self.request = request
        return {"request_sha256": request.request_sha256}

    def request_status(self, request_id: UUID) -> dict[str, object]:
        assert request_id == self.request.request_id
        return {
            "state": "CHECKPOINT_VERIFIED",
            "request_sha256": self.request.request_sha256,
            "claimed_epoch": 9,
            "claimed_run_id": "30000000-0000-4000-8000-000000000001",
            "workers": [_worker(0), _worker(1)],
            "imports": [_import(0), _import(1)],
            "coverage": _coverage(),
            "canonical_revision": 11,
            "checkpoint_receipt": {
                "checkpoint_id": str(EMBED_CHECKPOINT),
                "status": "VERIFIED",
                "canonical_revision": 11,
                "manifest_sha256": "f" * 64,
                "exact_version_ref": "owner/embedding-checkpoints/3",
            },
        }


class FakeMcp:
    def __init__(self) -> None:
        self.master_calls = 0
        self.calls: list[str] = []

    def call(self, tool: str, arguments: dict[str, object]) -> dict[str, object]:
        self.calls.append(tool)
        if tool == "embedding.production.capabilities":
            return _capabilities()
        if tool == "checkpoint.status":
            return {
                "current_checkpoint_id": str(EMBED_CHECKPOINT),
                "current": {
                    "checkpoint_id": str(EMBED_CHECKPOINT),
                    "status": "VERIFIED",
                    "canonical_revision": 11,
                    "manifest_sha256": "f" * 64,
                    "exact_version_ref": "owner/embedding-checkpoints/3",
                },
            }
        if tool == "master.status":
            self.master_calls += 1
            if self.master_calls == 1:
                return {"master_state": "STOPPED"}
            return {
                "master_state": "ACTIVE",
                "master_epoch": 10,
                "instance_id": "40000000-0000-4000-8000-000000000001",
                "canonical_revision": 11,
            }
        if tool == "master.rotation.request":
            assert arguments["expected_active_epoch"] == 9
            assert arguments["expected_canonical_revision"] == 11
            return {"operation_id": "embedding-rotation"}
        if tool == "operation.get":
            return {"state": "DURABLE_COMPLETE"}
        if tool == "embedding.coverage":
            return {"canonical_revision": 11, "models": _coverage()}
        if tool == "bloggers.search":
            return {
                "canonical_revision": 11,
                "items": [{"blogger_id": "bounded-fake-item"}],
                "complete": True,
                "retrievers": {
                    "requested": ["exact", "fts", "e5", "bge_m3"],
                    "completed": ["exact", "fts", "e5", "bge_m3"],
                    "unavailable": [],
                },
            }
        raise AssertionError(tool)


def _config() -> EmbeddingProductionConfig:
    return EmbeddingProductionConfig(
        control_url="http://127.0.0.1:8080",
        idempotency_key="final-embed-test-key",
        source_revision="a" * 40,
        probe_query="калининград культура",
        timeout_seconds=600,
        poll_seconds=1,
    )


def test_absent_live_interface_blocks_before_mutation() -> None:
    control = FakeControl(available=False)
    with pytest.raises(EmbeddingInterfacesUnavailable):
        run_embedding_production_closure(_config(), blogger_receipt=_blogger_receipt(), control=control, mcp=FakeMcp())
    assert control.created == 0


def test_fake_full_closure_binds_workers_imports_checkpoint_restore_and_hybrid_search() -> None:
    control = FakeControl()
    mcp = FakeMcp()
    receipt = run_embedding_production_closure(
        _config(),
        blogger_receipt=_blogger_receipt(),
        control=control,
        mcp=mcp,
        now=lambda: datetime(2026, 8, 11, tzinfo=UTC),
    )
    assert control.created == 1
    assert receipt["live_evidence"] is False
    assert len(receipt["workers"]) == 2
    assert len({item["provider_run_ref"] for item in receipt["workers"]}) == 2
    assert all(item["inserted_count"] == 266 for item in receipt["imports"])
    assert all(item["coverage"] == 1.0 for item in receipt["mcp_coverage"])
    assert receipt["checkpoint"]["checkpoint_id"] == str(EMBED_CHECKPOINT)
    assert receipt["cold_restore"]["canonical_revision"] == 11
    assert receipt["hybrid_search"]["completed_retrievers"] == ["bge_m3", "e5", "exact", "fts"]
    assert mcp.calls.index("embedding.production.capabilities") < mcp.calls.index("checkpoint.status")


@pytest.mark.parametrize(
    ("field", "invalid"),
    [
        ("primary_source_sha256", "1" * 64),
        ("provider_ref", "owner/wrong-worker"),
    ],
)
def test_worker_receipt_must_bind_the_pinned_generated_asset(field: str, invalid: object) -> None:
    control = FakeControl()
    original = control.request_status

    def altered(request_id: UUID) -> dict[str, object]:
        value = original(request_id)
        workers = value["workers"]
        assert isinstance(workers, list) and isinstance(workers[0], dict)
        workers[0][field] = invalid
        return value

    control.request_status = altered  # type: ignore[method-assign]
    with pytest.raises(EmbeddingProductionError, match="pinned generated asset"):
        run_embedding_production_closure(_config(), blogger_receipt=_blogger_receipt(), control=control, mcp=FakeMcp())


@pytest.mark.parametrize(
    ("path", "invalid"),
    [
        (("checkpoint", "exact_version_ref"), "owner/blogger-checkpoints/latest"),
        (("checkpoint", "manifest_sha256"), "not-a-hash"),
        (("mcp_projection",), None),
    ],
)
def test_partial_blogger_receipt_is_rejected_before_request(path: tuple[str, ...], invalid: object) -> None:
    receipt = _blogger_receipt()
    if len(path) == 1:
        receipt.pop(path[0])
    else:
        nested = receipt[path[0]]
        assert isinstance(nested, dict)
        nested[path[1]] = invalid
    control = FakeControl()
    with pytest.raises(EmbeddingProductionError):
        run_embedding_production_closure(_config(), blogger_receipt=receipt, control=control, mcp=FakeMcp())
    assert control.created == 0


def test_cli_missing_modern_token_exits_78_before_reading_prerequisite(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.delenv("KAGGLE_API_TOKEN", raising=False)
    monkeypatch.setenv("KAGGLE_CONFIG_DIR", str(tmp_path / "missing-token"))
    receipt = tmp_path / "must-not-exist.json"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_final_embedding_closure.py",
            "run",
            "--idempotency-key",
            "final-embed-test",
            "--source-revision",
            "a" * 40,
            "--blogger-receipt",
            str(tmp_path / "absent-blogger.json"),
            "--probe-query",
            "test",
            "--receipt",
            str(receipt),
        ],
    )
    assert main() == EXTERNAL_BLOCKED
    assert not receipt.exists()


def test_cli_absent_live_capability_exits_78_before_mutating_request(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("KAGGLE_API_TOKEN", "modern-token-present-for-fake-test")
    prerequisite = tmp_path / "blogger.json"
    prerequisite.write_bytes(canonical_json_bytes(_blogger_receipt()))
    receipt = tmp_path / "must-not-exist.json"
    monkeypatch.setattr(
        LocalEmbeddingProductionControl,
        "capabilities",
        lambda _self: (_ for _ in ()).throw(EmbeddingInterfacesUnavailable("not installed")),
    )
    mutation_attempted = False

    def reject_mutation(_self: object, _request: object) -> dict[str, object]:
        nonlocal mutation_attempted
        mutation_attempted = True
        raise AssertionError("mutation must not be attempted")

    monkeypatch.setattr(LocalEmbeddingProductionControl, "create_request", reject_mutation)
    monkeypatch.setattr(
        "scripts.embeddings.run_final_embedding_closure.StreamableHttpClosureMcp",
        lambda _endpoint, _token: FakeMcp(),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_final_embedding_closure.py",
            "run",
            "--idempotency-key",
            "final-embed-test",
            "--source-revision",
            "a" * 40,
            "--blogger-receipt",
            str(prerequisite),
            "--probe-query",
            "test",
            "--receipt",
            str(receipt),
        ],
    )
    assert main() == EXTERNAL_BLOCKED
    assert mutation_attempted is False
    assert not receipt.exists()


def test_cli_rejects_bearer_token_in_process_arguments(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        ["run_final_embedding_closure.py", "run", "--mcp-token", "must-not-appear-in-argv"],
    )
    with pytest.raises(SystemExit) as exc:
        main()
    assert exc.value.code == 2


@pytest.mark.parametrize(
    "stem",
    [
        "embedding-production-capabilities.v1",
        "embedding-production-request.v1",
        "embedding-production-closure.v1",
    ],
)
def test_production_contract_examples_validate(stem: str) -> None:
    root = Path(__file__).resolve().parents[2]
    schema = json.loads((root / f"schemas/embeddings/{stem}.schema.json").read_text())
    example = json.loads((root / f"examples/embeddings/{stem}.example.json").read_text())
    Draft202012Validator.check_schema(schema)
    Draft202012Validator(schema, format_checker=FormatChecker()).validate(example)


def test_worker_assets_match_generated_pinned_notebook_metadata() -> None:
    root = Path(__file__).resolve().parents[2]
    for asset in WORKER_ASSETS:
        assert (root / asset.notebook_path).is_file()
        metadata = json.loads((root / "notebooks" / asset.notebook_slug / "kernel-metadata.example.json").read_text())[
            "my_data_hub"
        ]
        assert metadata["primary_source_sha256"] == asset.primary_source_sha256
        assert metadata["model_id"] == asset.model.model_key
        assert metadata["model_revision"] == asset.model.revision
        assert metadata["resource_class"] == "orchestrator_protected"
        assert metadata["privacy"] == "private"
