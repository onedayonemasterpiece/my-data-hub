from __future__ import annotations

import hashlib
import json
import urllib.error
import urllib.request
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

import pytest
from jsonschema import Draft202012Validator, FormatChecker

from my_data_hub.providers.kaggle import KaggleIdentityError
from my_data_hub.providers.kaggle.contracts import (
    KaggleDatasetIdentity,
    KaggleKernelRunIdentity,
    KaggleKernelSourceIdentity,
    KaggleKernelStatus,
    KernelState,
    TaskResourceClaim,
)
from my_data_hub.providers.kaggle.retry import RetryClass, classify_failure
from my_data_hub.providers.models import ControlClass, ProviderFingerprint, ProviderKind
from scripts.provider.real_kaggle_matrix import (
    EXTERNAL_BLOCKED,
    MATRIX_SCENARIOS,
    AnonymousDatasetProbe,
    _AnonymousDatasetProbeError,
    _notebook_source,
    build_matrix_plan,
    modern_token_configured,
    run_dataset_canary,
    run_notebook_canary,
    run_real_matrix,
)


class _Response:
    status = 200

    def __enter__(self) -> _Response:
        return self

    def __exit__(self, *args: object) -> None:
        return None


def test_real_notebook_source_emits_exact_dynamic_identity_receipt(tmp_path: Path) -> None:
    run_id = UUID("11111111-1111-4111-8111-111111111111")
    source = _notebook_source(task_run_id=run_id, provider_ref="owner/mdh-private-smoke-11111111")
    script = tmp_path / "run.py"
    script.write_bytes(source)
    assert str(run_id).encode() in source
    assert b"Path(__file__).read_bytes()" in source
    assert b"source_sha256" in source
    assert b"is_private" not in source


def test_real_notebook_canary_fails_before_mutation_without_modern_token(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.delenv("KAGGLE_API_TOKEN", raising=False)
    monkeypatch.setenv("KAGGLE_CONFIG_DIR", str(tmp_path / "no-token"))
    receipt = tmp_path / "blocker.json"
    assert run_notebook_canary(ledger_path=tmp_path / "ledger.sqlite3", receipt_path=receipt) == EXTERNAL_BLOCKED
    payload = json.loads(receipt.read_text())
    assert payload["blocker_code"] == "KAGGLE_MODERN_API_TOKEN_REQUIRED"
    assert not (tmp_path / "ledger.sqlite3").exists()


def test_real_dataset_canary_fails_before_mutation_without_modern_token(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.delenv("KAGGLE_API_TOKEN", raising=False)
    monkeypatch.setenv("KAGGLE_CONFIG_DIR", str(tmp_path / "no-token"))
    receipt = tmp_path / "blocker.json"
    assert run_dataset_canary(ledger_path=tmp_path / "ledger.sqlite3", receipt_path=receipt) == EXTERNAL_BLOCKED
    assert json.loads(receipt.read_text())["mutations_started"] == 0
    assert not (tmp_path / "ledger.sqlite3").exists()


def test_modern_token_preflight_accepts_only_supported_nonempty_sources(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("KAGGLE_API_TOKEN", raising=False)
    monkeypatch.setenv("KAGGLE_CONFIG_DIR", str(tmp_path))
    assert modern_token_configured() is False

    token = tmp_path / "access_token"
    token.write_text("x" * 32, encoding="utf-8")
    assert modern_token_configured() is True

    token.unlink()
    target = tmp_path / "elsewhere"
    target.write_text("x" * 32, encoding="utf-8")
    token.symlink_to(target)
    assert modern_token_configured() is False

    monkeypatch.setenv("KAGGLE_API_TOKEN", "runtime-token-is-present")
    assert modern_token_configured() is True


def test_anonymous_probe_uses_exact_https_ref_and_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, object] = {}

    def open_request(request: urllib.request.Request, *, timeout: int) -> _Response:
        observed["url"] = request.full_url
        observed["timeout"] = timeout
        return _Response()

    monkeypatch.setattr("urllib.request.urlopen", open_request)
    assert AnonymousDatasetProbe().read_dataset("owner/private-dataset", 7) == {"status": 200}
    assert observed == {
        "url": ("https://www.kaggle.com/api/v1/datasets/download/owner/private-dataset?datasetVersionNumber=7"),
        "timeout": 20,
    }


def test_anonymous_probe_shapes_http_denial_for_adapter_classifier(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def deny(*_args: object, **_kwargs: object) -> object:
        raise urllib.error.HTTPError("https://www.kaggle.com", 403, "denied", {}, None)

    monkeypatch.setattr("urllib.request.urlopen", deny)
    with pytest.raises(_AnonymousDatasetProbeError) as captured:
        AnonymousDatasetProbe().read_dataset("owner/private-dataset", 1)
    failure = classify_failure(captured.value, now=datetime.now(UTC))
    assert failure.retry_class == RetryClass.AUTHORIZATION
    assert failure.http_status == 403


class _FakeMatrixAdapter:
    """Provider-shaped fake: it does not constitute real Kaggle evidence."""

    def __init__(self) -> None:
        self.dataset_files: dict[str, bytes] = {}
        self.runs: dict[str, object] = {}
        self.outputs: dict[str, bytes] = {}
        self.notebook_pushes = 0
        self.cleanups: list[str] = []

    @staticmethod
    def _fingerprint(value: str) -> ProviderFingerprint:
        return ProviderFingerprint(value=hashlib.sha256(value.encode()).hexdigest())

    def provider_identity(self) -> object:
        return SimpleNamespace(username="fake-owner")

    def create_private_dataset(self, *, intent: object, files: dict[str, bytes], **_: object) -> object:
        ref = str(intent.provider_ref)  # type: ignore[attr-defined]
        fingerprint = self._fingerprint(f"dataset:{ref}:1")
        claim = TaskResourceClaim.create(
            task_id=intent.task_id,  # type: ignore[attr-defined]
            effect_id=intent.effect_id,  # type: ignore[attr-defined]
            provider_ref=ref,
            kind=ProviderKind.DATASET,
            control_class=ControlClass.MCP_MANAGED,
            disposable=True,
            fingerprint=fingerprint,
            provider_version=1,
            registered_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        self.dataset_files = dict(files)
        return SimpleNamespace(
            identity=KaggleDatasetIdentity(
                provider_ref=ref,
                version=1,
                privacy="private",
                package_sha256=hashlib.sha256(b"fake-private-package").hexdigest(),
                fingerprint=fingerprint,
                observed_at=datetime(2026, 1, 1, tzinfo=UTC),
            ),
            claim=claim,
        )

    def prove_private_dataset_access(self, **_: object) -> object:
        return SimpleNamespace(unauthenticated_http_status=403)

    def reconcile_private_notebook_mutation(
        self, *, task_run_id: UUID, expected_source_sha256: str, **_: object
    ) -> object | None:
        result = self.runs.get(str(task_run_id))
        if result is not None and result.run.source_sha256 != expected_source_sha256:  # type: ignore[attr-defined]
            raise KaggleIdentityError("fake exact source mismatch")
        return result

    def push_private_notebook(self, *, intent: object, task_run_id: UUID, source: bytes, **_: object) -> object:
        self.notebook_pushes += 1
        ref = str(intent.provider_ref)  # type: ignore[attr-defined]
        source_sha = __import__(
            "scripts.provider.real_kaggle_matrix", fromlist=["_canonical_notebook_sha256"]
        )._canonical_notebook_sha256(source)
        fingerprint = self._fingerprint(f"notebook:{ref}:1:{source_sha}")
        run = KaggleKernelRunIdentity(
            task_run_id=task_run_id,
            provider_ref=ref,
            source_version=1,
            source_sha256=source_sha,
            provider_kernel_id=self.notebook_pushes,
            provider_run_ref=f"{ref}/1",
            started_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        claim = TaskResourceClaim.create(
            task_id=intent.task_id,  # type: ignore[attr-defined]
            effect_id=intent.effect_id,  # type: ignore[attr-defined]
            provider_ref=ref,
            kind=ProviderKind.NOTEBOOK,
            control_class=ControlClass.MCP_MANAGED,
            disposable=True,
            fingerprint=fingerprint,
            provider_version=1,
            registered_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        result = SimpleNamespace(
            source=KaggleKernelSourceIdentity(
                provider_ref=ref,
                source_version=1,
                privacy="private",
                source_sha256=source_sha,
                fingerprint=fingerprint,
                observed_at=datetime(2026, 1, 1, tzinfo=UTC),
            ),
            run=run,
            claim=claim,
        )
        manifest = json.loads(self.dataset_files[f"manifest-{task_run_id}.json"])
        item = manifest["work_items"][0]
        raw = {
            "schema_version": "my-data-hub-notebook-result.v1",
            "result_id": str(UUID(int=self.notebook_pushes)),
            "run_id": str(task_run_id),
            "workload": manifest["workload"],
            "stage": manifest["stage"],
            "stage_contract_version": manifest["stage_contract_version"],
            "input_manifest_sha256": hashlib.sha256(self.dataset_files[f"manifest-{task_run_id}.json"]).hexdigest(),
            "producer": {"code_revision": item["payload"]["commit_sha"], "runtime": "fake", "model": {}},
            "status": "succeeded",
            "items": [
                {
                    "work_item_id": item["work_item_id"],
                    "input_fingerprint": item["input_fingerprint"],
                    "output_fingerprint": hashlib.sha256(str(task_run_id).encode()).hexdigest(),
                    "status": "succeeded",
                    "result": {"payload_keys": sorted(item["payload"])},
                    "evidence": {},
                }
            ],
            "failures": [],
            "metrics": {"input_items": 1, "accounted_items": 1, "successful_items": 1, "failed_items": 0},
            "provider_usage": [],
            "artifacts": [],
            "started_at": "2026-01-01T00:00:00Z",
            "completed_at": "2026-01-01T00:00:01Z",
        }
        self.outputs[str(task_run_id)] = json.dumps(raw, sort_keys=True, separators=(",", ":")).encode()
        self.runs[str(task_run_id)] = result
        return result

    def poll_run(self, run: KaggleKernelRunIdentity, _policy: object) -> KaggleKernelStatus:
        return self.read_run_status(run)

    def read_run_status(self, run: KaggleKernelRunIdentity) -> KaggleKernelStatus:
        exact = self.runs.get(str(run.task_run_id))
        if exact is None or exact.run != run:  # type: ignore[attr-defined]
            raise KaggleIdentityError("fake stale run denied")
        return KaggleKernelStatus(
            run=run,
            state=KernelState.COMPLETE,
            provider_status="complete",
            observed_at=datetime(2026, 1, 1, tzinfo=UTC),
        )

    def download_exact_run_output_file(
        self, run: KaggleKernelRunIdentity, *, destination: Path, file_name: str, **_: object
    ) -> object:
        self.read_run_status(run)
        raw = self.outputs[str(run.task_run_id)]
        destination.mkdir(parents=True, exist_ok=True)
        (destination / file_name).write_bytes(raw)
        return SimpleNamespace(output_tree_sha256=hashlib.sha256(raw).hexdigest())

    def reconcile_private_notebook_run(
        self, *, task_run_id: UUID, provider_ref: str, expected_source_sha256: str
    ) -> KaggleKernelRunIdentity | None:
        result = self.runs.get(str(task_run_id))
        if result is None:
            return None
        run = result.run  # type: ignore[attr-defined]
        if run.provider_ref != provider_ref or run.source_sha256 != expected_source_sha256:
            raise KaggleIdentityError("fake reconciliation identity mismatch")
        return run

    def delete_task_created_resource(self, *, claim: TaskResourceClaim, **_: object) -> object:
        self.cleanups.append(claim.provider_ref)
        if claim.kind == ProviderKind.NOTEBOOK:
            for run_id, result in tuple(self.runs.items()):
                if result.run.provider_ref == claim.provider_ref:  # type: ignore[attr-defined]
                    self.runs.pop(run_id)
        return SimpleNamespace(detail_code="task_created_resource_absent")


def test_matrix_plan_has_distinct_run_identities_and_required_variants() -> None:
    plan = build_matrix_plan(
        matrix_id=UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
        commit_sha="a" * 40,
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
    )
    run_ids = [scenario["task_run_id"] for scenario in plan["scenarios"]]
    assert len(run_ids) == len(MATRIX_SCENARIOS) >= 15
    assert len(set(run_ids)) == len(run_ids)
    assert {scenario["category"] for scenario in plan["scenarios"]} >= {
        "retry",
        "soak",
        "fault",
        "resume",
        "idempotency",
        "checkpoint",
    }


def test_real_matrix_fails_before_adapter_or_ledger_without_modern_token(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.delenv("KAGGLE_API_TOKEN", raising=False)
    monkeypatch.setenv("KAGGLE_CONFIG_DIR", str(tmp_path / "no-token"))
    receipt = tmp_path / "blocked.json"
    assert (
        run_real_matrix(
            ledger_path=tmp_path / "ledger.sqlite3",
            receipt_path=receipt,
            scenario_receipt_dir=tmp_path / "scenarios",
            plan_path=tmp_path / "plan.json",
            adapter_factory=lambda _ledger: pytest.fail("adapter must not be constructed"),
        )
        == EXTERNAL_BLOCKED
    )
    assert json.loads(receipt.read_text())["mutations_started"] == 0
    assert not (tmp_path / "ledger.sqlite3").exists()
    assert not (tmp_path / "plan.json").exists()


def test_fake_matrix_proves_planning_accounting_cleanup_and_receipt_resume(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("KAGGLE_API_TOKEN", "fake-test-token-not-live-evidence")
    adapter = _FakeMatrixAdapter()
    kwargs = {
        "ledger_path": tmp_path / "ledger.sqlite3",
        "receipt_path": tmp_path / "summary.json",
        "scenario_receipt_dir": tmp_path / "scenarios",
        "plan_path": tmp_path / "plan.json",
        "matrix_id": UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
        "commit_sha": "a" * 40,
        "adapter_factory": lambda _ledger: adapter,
        "wheel_builder": lambda _root, _commit: ("my_data_hub-test.whl", b"fake wheel"),
        "root": Path(__file__).resolve().parents[2],
    }
    assert run_real_matrix(**kwargs) == EXTERNAL_BLOCKED
    summary = json.loads((tmp_path / "summary.json").read_text())
    assert summary["live_evidence"] is False
    assert summary["outcome"] == "SMOKE_PASS"
    assert summary["matrix_scope"] == "platform_smoke_only"
    assert summary["blockers"] == ["MANDATORY_OPERATIONAL_SCENARIOS_NOT_EXECUTED"]
    assert summary["completed_real_runs"] == len(MATRIX_SCENARIOS)
    assert len(summary["distinct_real_run_ids"]) == len(MATRIX_SCENARIOS)
    assert len(summary["distinct_provider_run_refs"]) == len(MATRIX_SCENARIOS)
    assert len(tuple((tmp_path / "scenarios").glob("*.json"))) == len(MATRIX_SCENARIOS)
    launch_fences = tuple((tmp_path / "scenarios").glob("*.launch"))
    assert len(launch_fences) == len(MATRIX_SCENARIOS)
    assert all(path.stat().st_mode & 0o777 == 0o600 for path in launch_fences)
    assert adapter.notebook_pushes == len(MATRIX_SCENARIOS)
    assert len(adapter.cleanups) == len(MATRIX_SCENARIOS) + 2  # scenarios + replay + input
    assert all(
        json.loads(path.read_text())["accounting"]["accounted_items"] == 1
        for path in (tmp_path / "scenarios").glob("*.json")
    )
    assert all(
        json.loads(path.read_text())["live_evidence"] is False for path in (tmp_path / "scenarios").glob("*.json")
    )

    # A process restart consumes exact completed receipts and does not launch a second run.
    cleanup_count = len(adapter.cleanups)
    assert run_real_matrix(**kwargs) == EXTERNAL_BLOCKED
    assert adapter.notebook_pushes == len(MATRIX_SCENARIOS)
    assert len(adapter.cleanups) == cleanup_count

    # If a successful cleanup committed but its final receipt was lost, the
    # durable launch fence denies a second physical run under the same run ID.
    (tmp_path / "summary.json").unlink()
    next((tmp_path / "scenarios").glob("01-*.json")).unlink()
    with pytest.raises(RuntimeError, match="durable launch fence"):
        run_real_matrix(**kwargs)
    assert adapter.notebook_pushes == len(MATRIX_SCENARIOS)


@pytest.mark.parametrize(
    ("schema_name", "example_name"),
    [
        (
            "kaggle-real-matrix-plan.v1.schema.json",
            "kaggle-real-matrix-plan.v1.example.json",
        ),
        (
            "kaggle-real-matrix-scenario-receipt.v1.schema.json",
            "kaggle-real-matrix-scenario-receipt.v1.example.json",
        ),
        (
            "kaggle-real-matrix-receipt.v1.schema.json",
            "kaggle-real-matrix-receipt.v1.example.json",
        ),
    ],
)
def test_real_matrix_contract_examples_validate(schema_name: str, example_name: str) -> None:
    root = Path(__file__).resolve().parents[2]
    schema = json.loads((root / "schemas" / schema_name).read_text())
    example = json.loads((root / "examples/contracts" / example_name).read_text())
    Draft202012Validator.check_schema(schema)
    Draft202012Validator(schema, format_checker=FormatChecker()).validate(example)


def test_provider_real_workflow_runs_matrix_after_token_preflight() -> None:
    workflow = (Path(__file__).resolve().parents[2] / ".github/workflows/provider-real.yml").read_text()
    assert workflow.index("real_kaggle_matrix.py preflight") < workflow.index("real_kaggle_matrix.py matrix")
    assert "timeout-minutes: 360" in workflow
    assert "artifacts/kaggle-matrix-scenarios/" in workflow
