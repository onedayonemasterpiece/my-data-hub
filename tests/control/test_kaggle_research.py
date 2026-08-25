from __future__ import annotations

import hashlib
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

import pytest

from my_data_hub.control_plane.clock import DeterministicClock
from my_data_hub.control_plane.ledger import ControlLedger
from my_data_hub.control_plane.research import KaggleResearchError, KaggleResearchService
from my_data_hub.hashing import canonical_json_bytes, sha256_value
from my_data_hub.mcp.oauth import AccessIdentity
from my_data_hub.providers.kaggle import (
    EffectOutcome,
    KaggleAmbiguousMutation,
    KaggleDatasetFileObservation,
    KaggleDatasetInspection,
    KaggleDatasetSummary,
    KaggleKernelRunIdentity,
    KaggleNotebookSource,
    KaggleRunLog,
    KernelState,
)
from my_data_hub.providers.kaggle.source_attestation import executable_source_sha256


def _principal() -> AccessIdentity:
    return AccessIdentity(
        subject="research-owner",
        client_id="research-tests",
        scopes=frozenset({"provider:read", "provider:write"}),
        audience="my-data-hub",
        token_id="research-token",
        expires_at=2_000_000_000,
        issuer="https://issuer.example",
        issued_at=1_900_000_000,
        resource="https://hub.example/mcp",
    )


class ResearchAdapter:
    def __init__(
        self,
        clock: DeterministicClock,
        *,
        lose_first_response: bool = False,
        terms_acceptance_required: bool = False,
    ) -> None:
        self.clock = clock
        self.lose_first_response = lose_first_response
        self.terms_acceptance_required = terms_acceptance_required
        self.pushes = 0
        self.run: KaggleKernelRunIdentity | None = None
        self.status = KernelState.RUNNING
        self.outputs: dict[str, bytes] = {}
        self.log = b"bounded provider failure log\n"
        self.owner_source = "print('safe owner source')\n"

    @staticmethod
    def provider_identity() -> SimpleNamespace:
        return SimpleNamespace(username="owner")

    def inspect_dataset(
        self, *, provider_ref: str, provider_version: int | None = None
    ) -> KaggleDatasetInspection:
        files = (
            KaggleDatasetFileObservation(path="aisles.csv", byte_size=11, provider_hash=None),
            KaggleDatasetFileObservation(path="orders.csv", byte_size=19, provider_hash=None),
        )
        return KaggleDatasetInspection(
            provider_ref=provider_ref,
            title=provider_ref.split("/", 1)[1],
            provider_version=provider_version or 9,
            visibility="owner_private" if provider_ref.startswith("owner/") else "public",
            license="CC0: Public Domain",
            total_bytes=30,
            files=files,
            files_manifest_sha256=sha256_value(
                {"files": [item.model_dump(mode="json") for item in files]}
            ),
            attach_mode="native_exact",
            terms_acceptance_required=self.terms_acceptance_required,
        )

    @staticmethod
    def search_datasets(
        *, query: str, visibility: str, cursor: str | None, limit: int
    ) -> tuple[tuple[KaggleDatasetSummary, ...], str | None]:
        assert query and cursor is None and limit >= 1
        ref = (
            "public-owner/public-input"
            if visibility == "public"
            else "owner/private-input"
        )
        return (
            (
                KaggleDatasetSummary(
                    provider_ref=ref,
                    title=ref.split("/", 1)[1],
                    provider_version=3,
                    visibility=visibility,
                    license="CC0: Public Domain",
                    total_bytes=30,
                ),
            ),
            None,
        )

    def push_private_research_notebook(self, **kwargs: object) -> SimpleNamespace:
        self.pushes += 1
        intent = kwargs["intent"]
        task_run_id = kwargs["task_run_id"]
        assert isinstance(task_run_id, UUID)
        provider_ref = intent.provider_ref
        source = kwargs["source"]
        assert isinstance(source, bytes)
        self.run = KaggleKernelRunIdentity(
            task_run_id=task_run_id,
            provider_ref=provider_ref,
            source_version=1,
            source_sha256=executable_source_sha256(source, kernel_type=str(kwargs["kernel_type"])),
            provider_kernel_id=101,
            provider_run_ref=f"{provider_ref}/1",
            started_at=self.clock.now(),
        )
        if self.lose_first_response:
            self.lose_first_response = False
            raise KaggleAmbiguousMutation("simulated response loss after provider commit")
        return SimpleNamespace(run=self.run, effect=SimpleNamespace(outcome=EffectOutcome.APPLIED))

    @staticmethod
    def assert_research_notebook_target_absent(provider_ref: str) -> None:
        assert provider_ref.startswith("owner/mdh-r-")

    def read_owner_notebook_source(
        self, *, provider_ref: str, source_version: int
    ) -> KaggleNotebookSource:
        encoded = self.owner_source.encode()
        return KaggleNotebookSource(
            provider_ref=provider_ref,
            source_version=source_version,
            code_file="research.py",
            kernel_type="script",
            language="python",
            source_utf8=self.owner_source,
            source_sha256=executable_source_sha256(encoded, kernel_type="script"),
        )

    def reconcile_private_notebook_mutation(self, **kwargs: object) -> SimpleNamespace | None:
        assert self.run is not None
        expected = str(kwargs["expected_source_sha256"])
        assert self.run.source_sha256 == expected
        return SimpleNamespace(run=self.run, effect=SimpleNamespace(outcome=EffectOutcome.ALREADY_APPLIED))

    def read_run_status(self, run: KaggleKernelRunIdentity) -> SimpleNamespace:
        assert self.run is not None and run.provider_run_ref == self.run.provider_run_ref
        return SimpleNamespace(
            state=self.status,
            provider_status=self.status.value.lower(),
            failure_message="simulated analysis failure" if self.status is KernelState.FAILED else None,
        )

    def read_exact_run_logs(self, run: KaggleKernelRunIdentity) -> KaggleRunLog:
        assert self.run is not None and run.provider_run_ref == self.run.provider_run_ref
        return KaggleRunLog(
            content=self.log,
            byte_size=len(self.log),
            sha256=hashlib.sha256(self.log).hexdigest(),
        )

    def download_exact_run_output_file(
        self,
        run: KaggleKernelRunIdentity,
        *,
        destination: Path,
        file_name: str,
        max_bytes: int,
    ) -> object:
        assert self.run is not None and run.provider_run_ref == self.run.provider_run_ref
        content = self.outputs[file_name]
        assert len(content) <= max_bytes
        destination.mkdir(parents=True, exist_ok=True)
        (destination / file_name).write_bytes(content)
        return SimpleNamespace()

    def install_outputs(self, ledger: ControlLedger, run_id: str) -> None:
        run = ledger.kaggle_run_internal(run_id)
        assert run is not None
        revision = ledger.kaggle_revision(
            owner_subject="research-owner",
            research_id=run.research_id,
            revision_id=run.revision_id,
        )
        assert revision is not None
        bodies = {
            "summary.md": b"# Result\nDurable research completed.\n",
            "metrics.json": b'{"rows":42}',
            "diagnostics.json": b'{"warnings":[]}',
            "run.log": b"completed\n",
        }
        roles = {
            "summary.md": ("summary", "text/markdown"),
            "metrics.json": ("metrics", "application/json"),
            "diagnostics.json": ("diagnostics", "application/json"),
            "run.log": ("log", "text/plain"),
        }
        artifacts = [
            {
                "path": path,
                "role": roles[path][0],
                "media_type": roles[path][1],
                "byte_size": len(body),
                "sha256": hashlib.sha256(body).hexdigest(),
            }
            for path, body in bodies.items()
        ]
        manifest = {
            "schema_version": "my-data-hub-research-output.v1",
            "research_id": run.research_id,
            "run_id": run.run_id,
            "revision_id": run.revision_id,
            "source_sha256": revision.source_sha256,
            "provider_source_sha256": run.provider_source_sha256,
            "inputs_sha256": revision.inputs_sha256,
            "artifacts": artifacts,
        }
        self.outputs = {**bodies, "research-output-manifest.json": canonical_json_bytes(manifest)}


def _created(service: KaggleResearchService) -> dict[str, object]:
    return service.research_create(
        {
            "alias": "instacart-study",
            "title": "Instacart study",
            "goal": "Validate a durable market basket research workflow.",
            "dataset_ref": "psparks/instacart-market-basket-analysis",
        },
        _principal(),
    )


def test_lost_submission_restart_recovery_and_artifact_chunks(tmp_path: Path) -> None:
    clock = DeterministicClock(datetime(2026, 8, 25, tzinfo=UTC))
    ledger_path = tmp_path / "control.db"
    ledger = ControlLedger(ledger_path, clock=clock)
    adapter = ResearchAdapter(clock, lose_first_response=True)
    service = KaggleResearchService(ledger, adapter, cache_root=tmp_path / "artifacts")

    ledger.register_provider_resource(
        provider="kaggle",
        resource_ref="owner/protected-master",
        resource_kind="notebook",
        source_identity="protected-master-source",
        source_version="1",
        control_class="orchestrator_protected",
        private=True,
        state="ready",
    )
    with pytest.raises(KaggleResearchError, match="status-only"):
        service.notebooks_get(
            {"notebook_ref": "owner/protected-master", "source_version": 1}, _principal()
        )
    owner_source = service.notebooks_get(
        {"notebook_ref": "owner/unmanaged", "source_version": 1}, _principal()
    )
    assert owner_source["source_utf8"] == "print('safe owner source')\n"
    assert owner_source["credentials_returned"] is False
    adapter.owner_source = "api_key = 'super-secret-value'\n"
    with pytest.raises(KaggleResearchError, match="credentials"):
        service.notebooks_get(
            {"notebook_ref": "owner/unmanaged", "source_version": 1}, _principal()
        )
    adapter.owner_source = "print('safe owner source')\n"
    ledger.register_provider_resource(
        provider="kaggle",
        resource_ref="owner/protected-checkpoints",
        resource_kind="dataset",
        source_identity="protected-checkpoint-source",
        source_version="7",
        control_class="orchestrator_protected",
        private=True,
        state="ready",
    )
    with pytest.raises(KaggleResearchError, match="status-only"):
        service.datasets_inspect(
            {"dataset_ref": "owner/protected-checkpoints", "provider_version": 7},
            _principal(),
        )

    public_page = service.datasets_search(
        {"query": "input", "visibility": "all", "limit": 1}, _principal()
    )
    assert [item["visibility"] for item in public_page["datasets"]] == ["public"]
    private_page = service.datasets_search(
        {
            "query": "input",
            "visibility": "all",
            "limit": 1,
            "cursor": public_page["next_cursor"],
        },
        _principal(),
    )
    assert [item["visibility"] for item in private_page["datasets"]] == ["owner_private"]
    assert private_page["next_cursor"] is None

    research = _created(service)
    run = service.runs_start({"research_id": research["research_id"]}, _principal())
    assert run["semantic_status"] == "SUBMISSION_UNKNOWN"
    assert adapter.pushes == 1
    with sqlite3.connect(ledger_path) as connection:
        assert connection.execute(
            "SELECT e.state FROM effects e JOIN kaggle_runs r ON r.effect_id=e.effect_id "
            "WHERE r.run_id=?",
            (run["run_id"],),
        ).fetchone() == ("IN_PROGRESS",)

    # A fresh ledger/service instance represents a restarted central control
    # process. It reuses the same SQLite-WAL state and never pushes blindly.
    restarted_ledger = ControlLedger(ledger_path, clock=clock)
    restarted = KaggleResearchService(
        restarted_ledger, adapter, cache_root=tmp_path / "artifacts"
    )
    clock.advance(6)
    assert restarted.reconcile_due_once() == {"observed": 1, "reconciled": 1}
    assert adapter.pushes == 1
    adapter.install_outputs(restarted_ledger, str(run["run_id"]))
    adapter.status = KernelState.COMPLETE
    assert restarted.reconcile_due_once() == {"observed": 1, "reconciled": 1}

    repeated = restarted.runs_start(
        {"research_id": research["research_id"]}, _principal()
    )
    assert repeated["run_id"] == run["run_id"]
    assert repeated["semantic_status"] == "SUCCEEDED"
    assert adapter.pushes == 1
    assert restarted.research_get(
        {"research_id": research["research_id"]}, _principal()
    )["state"] == "REVIEW_REQUIRED"
    with sqlite3.connect(ledger_path) as connection:
        assert connection.execute(
            "SELECT e.state FROM effects e JOIN kaggle_runs r ON r.effect_id=e.effect_id "
            "WHERE r.run_id=?",
            (run["run_id"],),
        ).fetchone() == ("APPLIED",)
        assert [row[0] for row in connection.execute(
            "SELECT l.state FROM effect_log l JOIN kaggle_runs r ON r.effect_id=l.effect_id "
            "WHERE r.run_id=? ORDER BY l.sequence",
            (run["run_id"],),
        )] == ["PLANNED", "IN_PROGRESS", "APPLIED"]

    with sqlite3.connect(ledger_path) as connection, pytest.raises(
        sqlite3.IntegrityError, match="frozen Kaggle revision is immutable"
    ):
        connection.execute(
            "UPDATE kaggle_notebook_revisions SET frozen_at=? WHERE revision_no=1",
            ("2099-01-01T00:00:00Z",),
        )

    listed = restarted.artifacts_list({"run_id": run["run_id"]}, _principal())
    assert {item["path"] for item in listed["artifacts"]} == {
        "research-output-manifest.json",
        "summary.md",
        "metrics.json",
        "provenance.json",
        "diagnostics.json",
        "run.log",
    }
    assert listed["compact_outputs"]["metrics.json"] == {"rows": 42}
    assert listed["compact_outputs"]["summary.md"].startswith("# Result")
    assert listed["compact_outputs"]["provenance.json"]["run_id"] == run["run_id"]
    summary = next(item for item in listed["artifacts"] if item["path"] == "summary.md")
    first = restarted.artifacts_read(
        {"artifact_id": summary["artifact_id"], "offset": 0, "max_bytes": 8}, _principal()
    )
    assert first["complete"] is False
    assert first["continuation"]["offset"] == 8
    final = restarted.artifacts_read(
        {"artifact_id": summary["artifact_id"], "offset": 8, "max_bytes": 131_072},
        _principal(),
    )
    assert final["complete"] is True
    assert final["file_sha256_verified"] is True

    saved = restarted.notebooks_save(
        {
            "research_id": research["research_id"],
            "source_utf8": "print('revision two')\n",
        },
        _principal(),
    )
    next_run = restarted.runs_start(
        {
            "research_id": research["research_id"],
            "revision_no": saved["revision"]["revision_no"],
        },
        _principal(),
    )
    assert next_run["run_id"] != run["run_id"]
    history = restarted.research_get({"research_id": research["research_id"]}, _principal())
    assert [item["revision_no"] for item in history["revisions"]] == [1, 2]
    assert [item["run_id"] for item in history["runs"]] == [run["run_id"], next_run["run_id"]]

    history_page = restarted.research_get(
        {"research_id": research["research_id"], "history_limit": 1}, _principal()
    )
    continuation = history_page["history_continuation"]
    assert history_page["history_complete"] is False
    assert isinstance(continuation["revision_cursor"], str)
    assert isinstance(continuation["run_cursor"], str)
    next_history_page = restarted.research_get(continuation, _principal())
    assert next_history_page["revisions"][0]["revision_no"] == 2
    assert next_history_page["runs"][0]["run_id"] == next_run["run_id"]


def test_failure_response_includes_status_logs_revision_and_pins(tmp_path: Path) -> None:
    clock = DeterministicClock(datetime(2026, 8, 25, tzinfo=UTC))
    ledger = ControlLedger(tmp_path / "control.db", clock=clock)
    adapter = ResearchAdapter(clock)
    service = KaggleResearchService(ledger, adapter, cache_root=tmp_path / "artifacts")
    _created(service)

    started = service.runs_start({"alias": "instacart-study"}, _principal())
    adapter.status = KernelState.FAILED
    failed = service.runs_get({"run_id": started["run_id"]}, _principal())

    assert failed["semantic_status"] == "FAILED"
    assert failed["provider_status"] == "failed"
    assert failed["failure_summary"] == "simulated analysis failure"
    assert failed["revision"]["revision_id"] == failed["revision_id"]
    assert failed["pins"][0]["provider_version"] == 9
    assert failed["logs"]["bounded"] is True
    assert failed["allowed_next_actions"] == [
        "runs.logs",
        "runs.retry",
        "notebooks.save",
        "runs.get",
    ]
    retry = service.runs_retry({"run_id": failed["run_id"]}, _principal())
    repeated_retry = service.runs_retry({"run_id": failed["run_id"]}, _principal())
    assert repeated_retry["run_id"] == retry["run_id"]
    assert adapter.pushes == 2


def test_unknown_submission_deadline_preserves_effect_uncertainty(tmp_path: Path) -> None:
    class UnobservedAdapter(ResearchAdapter):
        def reconcile_private_notebook_mutation(self, **_kwargs: object) -> None:
            return None

    clock = DeterministicClock(datetime(2026, 8, 25, tzinfo=UTC))
    ledger_path = tmp_path / "control.db"
    ledger = ControlLedger(ledger_path, clock=clock)
    service = KaggleResearchService(
        ledger,
        UnobservedAdapter(clock, lose_first_response=True),
        cache_root=tmp_path / "artifacts",
    )
    research = _created(service)
    run = service.runs_start({"research_id": research["research_id"]}, _principal())
    assert run["semantic_status"] == "SUBMISSION_UNKNOWN"

    clock.advance(2401)
    service.reconcile_run(str(run["run_id"]))
    assert service.runs_get({"run_id": run["run_id"]}, _principal())["semantic_status"] == "FAILED"
    with sqlite3.connect(ledger_path) as connection:
        assert connection.execute(
            "SELECT e.state FROM effects e JOIN kaggle_runs r ON r.effect_id=e.effect_id "
            "WHERE r.run_id=?",
            (run["run_id"],),
        ).fetchone() == ("IN_PROGRESS",)
        assert connection.execute(
            "SELECT l.state FROM effect_log l JOIN kaggle_runs r ON r.effect_id=l.effect_id "
            "WHERE r.run_id=? ORDER BY l.sequence DESC LIMIT 1",
            (run["run_id"],),
        ).fetchone() == ("SUBMISSION_UNKNOWN",)


def test_terms_guard_dataset_pagination_and_forbidden_architecture_paths(tmp_path: Path) -> None:
    clock = DeterministicClock(datetime(2026, 8, 25, tzinfo=UTC))
    service = KaggleResearchService(
        ControlLedger(tmp_path / "control.db", clock=clock),
        ResearchAdapter(clock),
        cache_root=tmp_path / "artifacts",
    )
    first = service.datasets_inspect(
        {
            "dataset_ref": "psparks/instacart-market-basket-analysis",
            "provider_version": 9,
            "file_limit": 1,
        },
        _principal(),
    )
    assert first["file_count"] == 2
    assert len(first["files"]) == 1
    assert isinstance(first["next_file_cursor"], str)
    second = service.datasets_inspect(first["continuation"], _principal())
    assert len(second["files"]) == 1
    assert second["next_file_cursor"] is None

    guarded = KaggleResearchService(
        ControlLedger(tmp_path / "terms.db", clock=clock),
        ResearchAdapter(clock, terms_acceptance_required=True),
        cache_root=tmp_path / "terms-artifacts",
    )
    with pytest.raises(KaggleResearchError, match="TERMS_ACCEPTANCE_REQUIRED"):
        _created(guarded)

    service_source = Path("src/my_data_hub/control_plane/research.py").read_text()
    for forbidden in ("master.ensure", "MasterSessionBroker", "data.query", "postgresql://", "PG DSN"):
        assert forbidden not in service_source
