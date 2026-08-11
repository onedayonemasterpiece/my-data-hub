#!/usr/bin/env python3
"""Run bounded real Kaggle gates through the repository's single adapter."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

from my_data_hub.control_plane.ledger import ControlLedger
from my_data_hub.hashing import canonical_json_bytes, sha256_value
from my_data_hub.notebooks.contracts import NotebookResult
from my_data_hub.providers.kaggle import (
    KaggleIdentityError,
    KaggleProviderAdapter,
    mapping_sha256,
)
from my_data_hub.providers.kaggle.contracts import (
    MutationAction,
    PollPolicy,
    ProviderEffectIntent,
)
from my_data_hub.providers.kaggle.control_journal import ControlLedgerKaggleJournal
from my_data_hub.providers.models import ControlClass

EXTERNAL_BLOCKED = 78
MATRIX_SCHEMA = "my-data-hub-real-kaggle-matrix.v1"
SCENARIO_SCHEMA = "my-data-hub-real-kaggle-matrix-scenario.v1"
MATRIX_MINIMUM_RUNS = 15
MATRIX_RESULT_NAME = "matrix-result.json"


def clean_repository_commit() -> str:
    root = Path(__file__).resolve().parents[2]
    commit = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    dirty = subprocess.run(
        ["git", "status", "--porcelain=v1", "-z"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    if len(commit) != 40 or any(character not in "0123456789abcdef" for character in commit):
        raise RuntimeError("real provider receipt requires an exact Git commit")
    if dirty:
        raise RuntimeError("real provider mutation requires a clean repository worktree")
    return commit


class _AnonymousDatasetProbeError(RuntimeError):
    """Shape an anonymous HTTP denial for the adapter's retry classifier."""

    def __init__(self, status_code: int) -> None:
        super().__init__(f"anonymous Kaggle dataset request denied with HTTP {status_code}")
        self.response = SimpleNamespace(status_code=status_code, headers={})


class AnonymousDatasetProbe:
    """Prove that an exact authenticated dataset version is not public."""

    def read_dataset(self, provider_ref: str, version: int) -> object:
        owner, slug = provider_ref.split("/", 1)
        quoted = "/".join(urllib.parse.quote(value, safe="") for value in (owner, slug))
        url = f"https://www.kaggle.com/api/v1/datasets/download/{quoted}?datasetVersionNumber={version}"
        request = urllib.request.Request(
            url,
            headers={"Accept": "application/zip", "User-Agent": "my-data-hub-private-proof/1"},
        )
        try:
            with urllib.request.urlopen(request, timeout=20) as response:
                # A successful response is deliberately not read or persisted.  The
                # adapter treats any success as a privacy failure.
                return {"status": response.status}
        except urllib.error.HTTPError as exc:
            raise _AnonymousDatasetProbeError(exc.code) from exc


def modern_token_configured() -> bool:
    configured = bool(os.environ.get("KAGGLE_API_TOKEN", "").strip())
    token_path = Path(os.environ.get("KAGGLE_CONFIG_DIR", "~/.kaggle")).expanduser() / "access_token"
    return configured or (token_path.is_file() and not token_path.is_symlink() and token_path.stat().st_size > 20)


def _effect(
    *,
    action: MutationAction,
    ref: str,
    task_id: UUID,
    arguments: dict[str, object],
    expected=None,  # type: ignore[no-untyped-def]
) -> ProviderEffectIntent:
    effect_id = uuid4()
    return ProviderEffectIntent.create(
        operation_id=uuid4(),
        effect_id=effect_id,
        idempotency_key=f"real-kaggle:{task_id}:{action.value}:{effect_id}",
        task_id=task_id,
        action=action,
        provider_ref=ref,
        arguments=arguments,
        expected_fingerprint=expected,
        requested_at=datetime.now(UTC),
    )


def _notebook_source(*, task_run_id: UUID, provider_ref: str) -> bytes:
    # The source hashes its exact staged bytes at runtime; no self-referential
    # digest is embedded.  Unique task slugs make the first exact source version 1.
    return f"""from __future__ import annotations
import hashlib, json
from pathlib import Path
TASK_RUN_ID = {str(task_run_id)!r}
PROVIDER_REF = {provider_ref!r}
SOURCE_VERSION = 1
source_sha256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
receipt = {{
    "schema_version": "my-data-hub-run-receipt.v1",
    "task_run_id": TASK_RUN_ID,
    "provider_ref": PROVIDER_REF,
    "source_version": SOURCE_VERSION,
    "source_sha256": source_sha256,
    "terminal_state": "complete",
}}
Path("/kaggle/working/my-data-hub-run-receipt.json").write_text(
    json.dumps(receipt, sort_keys=True, separators=(",", ":")), encoding="utf-8"
)
Path("/kaggle/working/smoke-output.json").write_text(
    json.dumps({{"ok": True, "task_run_id": TASK_RUN_ID}}, sort_keys=True), encoding="utf-8"
)
""".encode()


def run_dataset_canary(*, ledger_path: Path, receipt_path: Path) -> int:
    if not modern_token_configured():
        receipt_path.parent.mkdir(parents=True, exist_ok=True)
        receipt_path.write_bytes(
            canonical_json_bytes(
                {
                    "schema_version": "my-data-hub-real-kaggle-blocker.v1",
                    "gate": "private_dataset_exact_read_cleanup",
                    "outcome": "BLOCKED",
                    "blocker_code": "KAGGLE_MODERN_API_TOKEN_REQUIRED",
                    "mutations_started": 0,
                    "observed_at": datetime.now(UTC).isoformat(),
                }
            )
        )
        return EXTERNAL_BLOCKED
    commit_sha = clean_repository_commit()
    ledger = ControlLedger(ledger_path)
    adapter = KaggleProviderAdapter.from_environment(journal=ControlLedgerKaggleJournal(ledger))
    task_id = uuid4()
    run_id = uuid4()
    started_at = datetime.now(UTC)
    slug = f"mdh-private-canary-{str(run_id)[:8]}"
    ref = f"{adapter.provider_identity().username}/{slug}"
    content = canonical_json_bytes(
        {
            "schema_version": "my-data-hub-provider-canary.v1",
            "task_id": str(task_id),
            "run_id": str(run_id),
        }
    )
    content_tree_sha = hashlib.sha256(
        canonical_json_bytes(
            {
                "files": [
                    {
                        "path": "canary.json",
                        "sha256": hashlib.sha256(content).hexdigest(),
                        "byte_size": len(content),
                    }
                ]
            }
        )
    ).hexdigest()
    create = _effect(
        action=MutationAction.CREATE_DATASET,
        ref=ref,
        task_id=task_id,
        arguments={
            "content_tree_sha256": content_tree_sha,
            "control_class": ControlClass.MCP_MANAGED.value,
            "disposable": True,
        },
    )
    claim = None
    result = None
    proof = None
    cleanup = None
    try:
        result = adapter.create_private_dataset(
            intent=create,
            files={"canary.json": content},
            title=slug,
            control_class=ControlClass.MCP_MANAGED,
            disposable=True,
        )
        claim = result.claim
        proof = adapter.prove_private_dataset_access(
            provider_ref=ref,
            version=result.identity.version,
            unauthenticated_probe=AnonymousDatasetProbe(),
        )
    finally:
        if claim is not None:
            delete = _effect(
                action=MutationAction.DELETE_DATASET,
                ref=ref,
                task_id=task_id,
                arguments={
                    "claim_sha256": claim.claim_sha256,
                    "provider_version": claim.provider_version,
                },
                expected=claim.fingerprint,
            )
            cleanup = adapter.delete_task_created_resource(intent=delete, claim=claim)
    if result is None or proof is None or cleanup is None:
        raise RuntimeError("dataset canary has no exact create/privacy/cleanup receipt")
    receipt = {
        "schema_version": "my-data-hub-real-kaggle-canary.v2",
        "scenario": "private_dataset_create_exact_readback_unauthenticated_denial_delete",
        "task_id": str(task_id),
        "run_id": str(run_id),
        "commit_sha": commit_sha,
        "provider_ref": ref,
        "provider_version": result.identity.version,
        "package_sha256": result.identity.package_sha256,
        "claim_sha256": result.claim.claim_sha256,
        "privacy": result.identity.privacy,
        "unauthenticated_http_status": proof.unauthenticated_http_status,
        "denial_class": proof.denial_class,
        "cleanup": cleanup.detail_code,
        "cleanup_outcome": "complete",
        "counts": {"created": 1, "exact_readback": 1, "deleted": 1},
        "gate_results": [
            {"name": "private_create", "outcome": "PASS"},
            {"name": "exact_version_readback", "outcome": "PASS"},
            {"name": "unauthenticated_access_denied", "outcome": "PASS"},
            {"name": "claim_bound_cleanup", "outcome": "PASS"},
        ],
        "blockers": [],
        "started_at": started_at.isoformat(),
        "completed_at": datetime.now(UTC).isoformat(),
    }
    receipt_path.parent.mkdir(parents=True, exist_ok=True)
    receipt_path.write_bytes(canonical_json_bytes(receipt))
    return 0


def run_notebook_canary(*, ledger_path: Path, receipt_path: Path) -> int:
    if not modern_token_configured():
        receipt_path.parent.mkdir(parents=True, exist_ok=True)
        receipt_path.write_bytes(
            canonical_json_bytes(
                {
                    "schema_version": "my-data-hub-real-kaggle-blocker.v1",
                    "gate": "private_notebook_exact_read_run_output",
                    "outcome": "BLOCKED",
                    "blocker_code": "KAGGLE_MODERN_API_TOKEN_REQUIRED",
                    "credential_location": "KAGGLE_API_TOKEN or ~/.kaggle/access_token",
                    "proof_command": (
                        "kaggle auth login && python scripts/provider/real_kaggle_matrix.py notebook-canary"
                    ),
                    "observed_at": datetime.now(UTC).isoformat(),
                }
            )
        )
        return EXTERNAL_BLOCKED
    ledger = ControlLedger(ledger_path)
    adapter = KaggleProviderAdapter.from_environment(journal=ControlLedgerKaggleJournal(ledger))
    task_id = uuid4()
    task_run_id = uuid4()
    slug = f"mdh-private-smoke-{str(task_run_id)[:8]}"
    ref = f"{adapter.provider_identity().username}/{slug}"
    source = _notebook_source(task_run_id=task_run_id, provider_ref=ref)
    canonical_source = source
    source_sha = hashlib.sha256(canonical_source).hexdigest()
    create_arguments = {
        "task_run_id": str(task_run_id),
        "source_sha256": source_sha,
        "dataset_sources": (),
        "control_class": ControlClass.MCP_MANAGED.value,
        "disposable": True,
    }
    create = _effect(
        action=MutationAction.PUSH_NOTEBOOK,
        ref=ref,
        task_id=task_id,
        arguments=create_arguments,
    )
    claim = None
    result = None
    cleanup = None
    try:
        result = adapter.push_private_notebook(
            intent=create,
            task_run_id=task_run_id,
            source=source,
            title=slug,
            code_file="run.py",
            kernel_type="script",
            language="python",
            control_class=ControlClass.MCP_MANAGED,
            disposable=True,
            enable_internet=False,
            timeout_seconds=900,
        )
        claim = result.claim
        terminal = adapter.poll_run(
            result.run,
            PollPolicy(interval_seconds=15, timeout_seconds=900, max_polls=60),
        )
        output = adapter.read_exact_run_output(result.run)
        receipt = {
            "schema_version": "my-data-hub-real-kaggle-run.v1",
            "scenario": "private_notebook_exact_source_run_output_delete",
            "task_id": str(task_id),
            "task_run_id": str(task_run_id),
            "provider_ref": ref,
            "provider_kernel_id": result.run.provider_kernel_id,
            "source_version": result.run.source_version,
            "source_sha256": result.run.source_sha256,
            "provider_run_ref": result.run.provider_run_ref,
            "terminal_state": terminal.state.value,
            "output_tree_sha256": output.output_tree_sha256,
            "output_receipt_sha256": output.receipt_sha256,
            "privacy": result.source.privacy,
            "control_class": result.claim.control_class.value,
            "completed_at": datetime.now(UTC).isoformat(),
        }
    finally:
        if claim is not None:
            delete_arguments = {
                "claim_sha256": claim.claim_sha256,
                "provider_version": claim.provider_version,
            }
            delete = _effect(
                action=MutationAction.DELETE_NOTEBOOK,
                ref=ref,
                task_id=task_id,
                arguments=delete_arguments,
                expected=claim.fingerprint,
            )
            cleanup = adapter.delete_task_created_resource(intent=delete, claim=claim)
    if result is None or cleanup is None:
        raise RuntimeError("notebook canary has no exact create/cleanup receipt")
    receipt["cleanup"] = cleanup.detail_code
    receipt_path.parent.mkdir(parents=True, exist_ok=True)
    receipt_path.write_bytes(canonical_json_bytes(receipt))
    return 0


@dataclass(frozen=True, slots=True)
class MatrixScenarioSpec:
    name: str
    category: str
    variant: str
    fault_probe: str | None = None
    checkpoint_bound: bool = False


MATRIX_SCENARIOS: tuple[MatrixScenarioSpec, ...] = (
    MatrixScenarioSpec("baseline-private-runtime", "baseline", "exact-private-generated-notebook"),
    MatrixScenarioSpec("exact-source-readback", "identity", "exact-source-version-sha"),
    MatrixScenarioSpec("exact-output-readback", "identity", "exact-numeric-run-output"),
    MatrixScenarioSpec("status-retry-observation", "retry", "repeat-status-read", "repeat_status"),
    MatrixScenarioSpec("output-retry-observation", "retry", "repeat-output-read", "repeat_output"),
    MatrixScenarioSpec("resume-exact-run", "resume", "reconcile-after-push", "reconcile"),
    MatrixScenarioSpec("idempotent-reconcile", "idempotency", "repeat-reconciliation", "reconcile_twice"),
    MatrixScenarioSpec("claim-cleanup-replay", "idempotency", "repeat-claim-cleanup", "cleanup_replay"),
    MatrixScenarioSpec("stale-source-fence", "fault", "stale-source-sha-denial", "stale_source"),
    MatrixScenarioSpec("stale-run-fence", "fault", "stale-numeric-version-denial", "stale_run"),
    MatrixScenarioSpec(
        "checkpoint-current-binding",
        "checkpoint",
        "current-checkpoint-manifest",
        checkpoint_bound=True,
    ),
    MatrixScenarioSpec(
        "checkpoint-resume-binding",
        "checkpoint",
        "checkpoint-resume-idempotency",
        "reconcile",
        checkpoint_bound=True,
    ),
    MatrixScenarioSpec("runtime-accounting-binding", "contract", "typed-item-accounting"),
    MatrixScenarioSpec("short-soak-a", "soak", "sequential-soak-1"),
    MatrixScenarioSpec("short-soak-b", "soak", "sequential-soak-2"),
    MatrixScenarioSpec("short-soak-c", "soak", "sequential-soak-3"),
)


def build_matrix_plan(*, matrix_id: UUID, commit_sha: str, created_at: datetime) -> dict[str, Any]:
    """Build stable identities before any provider mutation so a run can resume exactly."""

    if len(commit_sha) != 40 or any(character not in "0123456789abcdef" for character in commit_sha):
        raise ValueError("matrix plan requires an exact source commit")
    scenarios: list[dict[str, Any]] = []
    for ordinal, spec in enumerate(MATRIX_SCENARIOS, 1):
        namespace = f"real-kaggle-matrix:{matrix_id}:{ordinal}:{spec.name}"
        run_id = uuid5(NAMESPACE_URL, f"{namespace}:run")
        checkpoint_id = uuid5(NAMESPACE_URL, f"{namespace}:checkpoint")
        checkpoint_manifest_sha256 = sha256_value(
            {
                "schema_version": "my-data-hub-matrix-checkpoint-binding.v1",
                "checkpoint_id": str(checkpoint_id),
                "matrix_id": str(matrix_id),
                "scenario": spec.name,
                "commit_sha": commit_sha,
            }
        )
        scenarios.append(
            {
                "ordinal": ordinal,
                "name": spec.name,
                "category": spec.category,
                "variant": spec.variant,
                "fault_probe": spec.fault_probe,
                "checkpoint_bound": spec.checkpoint_bound,
                "task_id": str(uuid5(NAMESPACE_URL, f"{namespace}:task")),
                "task_run_id": str(run_id),
                "work_item_id": str(uuid5(NAMESPACE_URL, f"{namespace}:work-item")),
                "subject_id": str(uuid5(NAMESPACE_URL, f"{namespace}:subject")),
                "checkpoint_id": str(checkpoint_id) if spec.checkpoint_bound else None,
                "checkpoint_manifest_sha256": (checkpoint_manifest_sha256 if spec.checkpoint_bound else None),
            }
        )
    return {
        "schema_version": "my-data-hub-real-kaggle-matrix-plan.v1",
        "matrix_id": str(matrix_id),
        "commit_sha": commit_sha,
        "created_at": created_at.astimezone(UTC).isoformat(),
        "minimum_real_runs": MATRIX_MINIMUM_RUNS,
        "scenarios": scenarios,
    }


def _load_or_create_plan(*, plan_path: Path, matrix_id: UUID | None, commit_sha: str, now: datetime) -> dict[str, Any]:
    if plan_path.is_file():
        plan = json.loads(plan_path.read_bytes())
        expected_names = [spec.name for spec in MATRIX_SCENARIOS]
        if (
            not isinstance(plan, dict)
            or plan.get("schema_version") != "my-data-hub-real-kaggle-matrix-plan.v1"
            or plan.get("commit_sha") != commit_sha
            or [item.get("name") for item in plan.get("scenarios", [])] != expected_names
            or (matrix_id is not None and plan.get("matrix_id") != str(matrix_id))
        ):
            raise RuntimeError("matrix resume plan differs from the exact source/scenario contract")
        return plan
    plan = build_matrix_plan(matrix_id=matrix_id or uuid4(), commit_sha=commit_sha, created_at=now)
    plan_path.parent.mkdir(parents=True, exist_ok=True)
    plan_path.write_bytes(canonical_json_bytes(plan))
    return plan


def _persist_launch_fence(path: Path, payload: dict[str, Any]) -> None:
    """Durably fence a task run before push so restart never launches it twice."""

    encoded = canonical_json_bytes(payload)
    if path.is_file():
        if path.is_symlink() or path.read_bytes() != encoded:
            raise RuntimeError("matrix launch fence differs from the exact planned provider run")
        return
    if path.exists():
        raise RuntimeError("matrix launch fence path is not a regular file")
    temporary = path.with_name(f".{path.name}.{uuid4()}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        os.chmod(path, 0o600)
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if temporary.exists():
            temporary.unlink()


def _matrix_effect(
    *,
    matrix_id: UUID,
    identity: str,
    action: MutationAction,
    ref: str,
    task_id: UUID,
    arguments: dict[str, object],
    expected=None,  # type: ignore[no-untyped-def]
) -> ProviderEffectIntent:
    effect_id = uuid5(NAMESPACE_URL, f"real-kaggle-matrix:{matrix_id}:{identity}:{action.value}:effect")
    return ProviderEffectIntent.create(
        operation_id=uuid5(NAMESPACE_URL, f"real-kaggle-matrix:{matrix_id}:{identity}:operation"),
        effect_id=effect_id,
        idempotency_key=f"real-kaggle-matrix:{matrix_id}:{identity}:{action.value}",
        task_id=task_id,
        action=action,
        provider_ref=ref,
        arguments=arguments,
        expected_fingerprint=expected,
        requested_at=datetime(2000, 1, 1, tzinfo=UTC),
    )


def _build_exact_wheel(*, root: Path, commit_sha: str) -> tuple[str, bytes]:
    with tempfile.TemporaryDirectory(prefix="my-data-hub-matrix-wheel-") as temporary:
        destination = Path(temporary)
        environment = dict(os.environ)
        environment.update({"SOURCE_DATE_EPOCH": "315532800", "PYTHONHASHSEED": "0"})
        subprocess.run(
            [
                sys.executable,
                "-m",
                "pip",
                "wheel",
                ".",
                "--no-deps",
                "--disable-pip-version-check",
                "--wheel-dir",
                str(destination),
            ],
            cwd=root,
            env=environment,
            check=True,
            capture_output=True,
        )
        wheels = tuple(destination.glob("*.whl"))
        if len(wheels) != 1:
            raise RuntimeError("matrix build did not produce one exact my-data-hub wheel")
        wheel = wheels[0]
        # The commit is bound in every manifest/bootstrap and matrix receipt;
        # deterministic build inputs make a resumed package byte-identical.
        if not commit_sha:
            raise RuntimeError("matrix wheel is not source-commit bound")
        return wheel.name, wheel.read_bytes()


def _scenario_payload(scenario: dict[str, Any], *, matrix_id: UUID, commit_sha: str) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "matrix_id": str(matrix_id),
        "scenario": scenario["name"],
        "category": scenario["category"],
        "variant": scenario["variant"],
        "commit_sha": commit_sha,
    }
    if scenario["checkpoint_bound"]:
        payload["checkpoint"] = {
            "checkpoint_id": scenario["checkpoint_id"],
            "manifest_sha256": scenario["checkpoint_manifest_sha256"],
            "current_checkpoint_id": scenario["checkpoint_id"],
        }
    return payload


def _scenario_manifest(
    scenario: dict[str, Any], *, matrix_id: UUID, commit_sha: str, created_at: str
) -> tuple[dict[str, Any], str]:
    payload = _scenario_payload(scenario, matrix_id=matrix_id, commit_sha=commit_sha)
    fingerprint = sha256_value(
        {
            "schema_version": "my-data-hub-matrix-work-item.v1",
            "work_item_id": scenario["work_item_id"],
            "payload": payload,
        }
    )
    manifest = {
        "schema_version": "my-data-hub-notebook-input.v1",
        "run_id": scenario["task_run_id"],
        "workload": "real-kaggle-acceptance",
        "stage": "platform_smoke",
        "stage_contract_version": "my-data-hub.platform-smoke.v1",
        "canonical_revision": 0,
        "work_items": [
            {
                "work_item_id": scenario["work_item_id"],
                "subject_type": "real_kaggle_matrix_scenario",
                "subject_id": scenario["subject_id"],
                "input_fingerprint": fingerprint,
                "payload": payload,
            }
        ],
        "artifacts": [],
        "model": {
            "provider": "none",
            "name": "contract-smoke",
            "version": "v1",
            "task": "validation",
            "configuration": {"scenario": scenario["name"]},
        },
        "policy_versions": {"real_kaggle_matrix": "v1"},
        "limits": {"max_runtime_seconds": 600, "max_output_bytes": 1024 * 1024, "max_items": 1},
        "created_at": created_at,
    }
    return manifest, fingerprint


def _render_generated_matrix_notebook(
    *,
    root: Path,
    scenario: dict[str, Any],
    input_slug: str,
    wheel_name: str,
    wheel_sha256: str,
    manifest_name: str,
    manifest_sha256: str,
    commit_sha: str,
) -> bytes:
    source_path = root / "notebooks/00-platform-smoke/worker.ipynb"
    body = json.loads(source_path.read_bytes())
    if not isinstance(body, dict) or not isinstance(body.get("cells"), list):
        raise RuntimeError("generated platform smoke notebook is invalid")
    input_root = f"/kaggle/input/{input_slug}"
    bootstrap = f"""from __future__ import annotations
import hashlib, os, subprocess, sys
from pathlib import Path
MATRIX_TASK_RUN_ID = {scenario["task_run_id"]!r}
MATRIX_MANIFEST_FILE = {manifest_name!r}
wheel = Path({f"{input_root}/{wheel_name}"!r})
manifest = Path({f"{input_root}/{manifest_name}"!r})
if not wheel.is_file() or hashlib.sha256(wheel.read_bytes()).hexdigest() != {wheel_sha256!r}:
    raise RuntimeError("exact private matrix wheel is absent or mismatched")
if not manifest.is_file() or hashlib.sha256(manifest.read_bytes()).hexdigest() != {manifest_sha256!r}:
    raise RuntimeError("exact private matrix manifest is absent or mismatched")
subprocess.run(
    [sys.executable, "-m", "pip", "install", "--no-deps", "--disable-pip-version-check", str(wheel)],
    check=True,
)
os.environ["MY_DATA_HUB_NOTEBOOK_INPUT_MANIFEST"] = str(manifest)
os.environ["MY_DATA_HUB_NOTEBOOK_RESULT_PATH"] = "/kaggle/working/{MATRIX_RESULT_NAME}"
os.environ["MY_DATA_HUB_CODE_REVISION"] = {commit_sha!r}
"""
    body["cells"].insert(
        1,
        {
            "cell_type": "code",
            "execution_count": None,
            "id": f"matrix-{scenario['ordinal']:02d}-bootstrap",
            "metadata": {},
            "outputs": [],
            "source": bootstrap,
        },
    )
    metadata = body.setdefault("metadata", {}).setdefault("my_data_hub", {})
    metadata.update(
        {
            "matrix_scenario": scenario["name"],
            "matrix_task_run_id": scenario["task_run_id"],
            "primary_source": "notebooks/00-platform-smoke/worker.ipynb",
        }
    )
    return json.dumps(body, sort_keys=True, separators=(",", ":")).encode()


def _canonical_notebook_sha256(source: bytes) -> str:
    body = json.loads(source)
    for cell in body.get("cells", []):
        if isinstance(cell, dict):
            if cell.get("cell_type") == "code" and "outputs" in cell:
                cell["outputs"] = []
            if isinstance(cell.get("source"), list):
                cell["source"] = "".join(str(item) for item in cell["source"])
    return hashlib.sha256(json.dumps(body).encode()).hexdigest()


def _delete_claimed_resource(*, adapter: Any, matrix_id: UUID, identity: str, task_id: UUID, claim: Any) -> Any:
    action = MutationAction.DELETE_DATASET if claim.kind.value == "dataset" else MutationAction.DELETE_NOTEBOOK
    intent = _matrix_effect(
        matrix_id=matrix_id,
        identity=f"{identity}:cleanup",
        action=action,
        ref=claim.provider_ref,
        task_id=task_id,
        arguments={
            "claim_sha256": claim.claim_sha256,
            "provider_version": claim.provider_version,
        },
        expected=claim.fingerprint,
    )
    return adapter.delete_task_created_resource(intent=intent, claim=claim)


def _run_matrix_fault_probe(
    *, adapter: Any, scenario: dict[str, Any], result: Any, output_tree_sha256: str
) -> dict[str, Any]:
    probe = scenario.get("fault_probe")
    if probe is None:
        return {"name": "none", "outcome": "NOT_REQUESTED"}
    if probe == "repeat_status":
        first = adapter.read_run_status(result.run)
        second = adapter.read_run_status(result.run)
        if first.state.value != "complete" or second.state.value != "complete":
            raise RuntimeError("repeat exact status observation was nonterminal")
    elif probe == "repeat_output":
        with tempfile.TemporaryDirectory(prefix="my-data-hub-matrix-output-repeat-") as folder:
            repeated = adapter.download_exact_run_output_file(
                result.run,
                destination=Path(folder),
                file_name=MATRIX_RESULT_NAME,
                max_bytes=1024 * 1024,
            )
        if repeated.output_tree_sha256 != output_tree_sha256:
            raise RuntimeError("repeat exact output readback changed identity")
    elif probe in {"reconcile", "reconcile_twice"}:
        repetitions = 2 if probe == "reconcile_twice" else 1
        for _ in range(repetitions):
            reconciled = adapter.reconcile_private_notebook_run(
                task_run_id=result.run.task_run_id,
                provider_ref=result.run.provider_ref,
                expected_source_sha256=result.run.source_sha256,
            )
            if reconciled is None or reconciled.provider_run_ref != result.run.provider_run_ref:
                raise RuntimeError("exact Notebook reconciliation did not return the same physical run")
    elif probe in {"stale_source", "stale_run"}:
        updates: dict[str, Any]
        if probe == "stale_source":
            updates = {"source_sha256": "0" * 64}
        else:
            stale_version = result.run.source_version + 1
            updates = {
                "source_version": stale_version,
                "provider_run_ref": f"{result.run.provider_ref}/{stale_version}",
            }
        stale = result.run.model_copy(update=updates)
        try:
            adapter.read_run_status(stale)
        except KaggleIdentityError:
            pass
        else:
            raise RuntimeError("stale exact run/source evidence was not denied")
    elif probe == "cleanup_replay":
        # Executed after the first cleanup because it requires an absent resource.
        return {"name": probe, "outcome": "DEFERRED_UNTIL_CLEANUP"}
    else:  # pragma: no cover - constant plan is validated by tests
        raise RuntimeError(f"unsupported matrix fault probe: {probe}")
    return {"name": probe, "outcome": "PASS"}


def _validate_completed_result(
    *, raw: bytes, scenario: dict[str, Any], manifest_sha256: str, input_fingerprint: str
) -> NotebookResult:
    result = NotebookResult.model_validate_json(raw)
    if (
        str(result.run_id) != scenario["task_run_id"]
        or result.status != "succeeded"
        or result.input_manifest_sha256 != manifest_sha256
        or len(result.items) != 1
        or result.failures
        or str(result.items[0].work_item_id) != scenario["work_item_id"]
        or result.items[0].input_fingerprint != input_fingerprint
        or result.items[0].result.get("payload_keys")
        != sorted(
            _scenario_payload(
                scenario,
                matrix_id=UUID(str(scenario["matrix_id"])),
                commit_sha=str(scenario["commit_sha"]),
            )
        )
    ):
        raise RuntimeError("typed Notebook output differs from the exact scenario manifest/accounting")
    return result


def run_real_matrix(
    *,
    ledger_path: Path,
    receipt_path: Path,
    scenario_receipt_dir: Path,
    plan_path: Path,
    matrix_id: UUID | None = None,
    commit_sha: str | None = None,
    adapter_factory: Callable[[ControlLedger], Any] | None = None,
    wheel_builder: Callable[[Path, str], tuple[str, bytes]] | None = None,
    root: Path | None = None,
) -> int:
    """Run the real disposable matrix; never instantiate the adapter without a modern token."""

    live_evidence = commit_sha is None and adapter_factory is None and wheel_builder is None and root is None
    if not modern_token_configured():
        receipt_path.parent.mkdir(parents=True, exist_ok=True)
        receipt_path.write_bytes(
            canonical_json_bytes(
                {
                    "schema_version": "my-data-hub-real-kaggle-matrix-blocker.v1",
                    "outcome": "BLOCKED",
                    "blocker_code": "KAGGLE_MODERN_API_TOKEN_REQUIRED",
                    "planned_runs": len(MATRIX_SCENARIOS),
                    "mutations_started": 0,
                    "observed_at": datetime.now(UTC).isoformat(),
                }
            )
        )
        return EXTERNAL_BLOCKED
    repository_root = root or Path(__file__).resolve().parents[2]
    exact_commit = commit_sha or clean_repository_commit()
    plan = _load_or_create_plan(
        plan_path=plan_path,
        matrix_id=matrix_id,
        commit_sha=exact_commit,
        now=datetime.now(UTC),
    )
    matrix_uuid = UUID(str(plan["matrix_id"]))
    if len(plan["scenarios"]) < MATRIX_MINIMUM_RUNS:
        raise RuntimeError("real Kaggle matrix plan has fewer than 15 distinct runs")
    run_ids = [str(item["task_run_id"]) for item in plan["scenarios"]]
    if len(set(run_ids)) != len(run_ids):
        raise RuntimeError("real Kaggle matrix plan reused a task run identity")
    if receipt_path.is_file():
        existing_summary = json.loads(receipt_path.read_bytes())
        exact_receipts: list[dict[str, Any]] = []
        for planned in plan["scenarios"]:
            scenario_path = scenario_receipt_dir / f"{planned['ordinal']:02d}-{planned['name']}.json"
            if not scenario_path.is_file():
                break
            scenario_receipt = json.loads(scenario_path.read_bytes())
            if (
                scenario_receipt.get("schema_version") != SCENARIO_SCHEMA
                or scenario_receipt.get("matrix_id") != str(matrix_uuid)
                or scenario_receipt.get("commit_sha") != exact_commit
                or scenario_receipt.get("task_run_id") != planned["task_run_id"]
                or scenario_receipt.get("outcome") != "PASS"
                or scenario_receipt.get("live_evidence") is not live_evidence
                or scenario_receipt.get("cleanup", {}).get("outcome") != "complete"
            ):
                raise RuntimeError(f"unsafe or stale scenario resume receipt: {scenario_path}")
            exact_receipts.append(scenario_receipt)
        if len(exact_receipts) == len(plan["scenarios"]):
            if (
                existing_summary.get("schema_version") == MATRIX_SCHEMA
                and existing_summary.get("matrix_id") == str(matrix_uuid)
                and existing_summary.get("commit_sha") == exact_commit
                and existing_summary.get("outcome") == "SMOKE_PASS"
                and existing_summary.get("matrix_scope") == "platform_smoke_only"
                and existing_summary.get("live_evidence") is live_evidence
                and set(existing_summary.get("distinct_real_run_ids", [])) == set(run_ids)
                and set(existing_summary.get("distinct_provider_run_refs", []))
                == {item["provider_run_ref"] for item in exact_receipts}
            ):
                return EXTERNAL_BLOCKED
            raise RuntimeError("unsafe or stale completed matrix summary receipt")
    scenario_receipt_dir.mkdir(parents=True, exist_ok=True)
    ledger = ControlLedger(ledger_path)
    factory = adapter_factory or (
        lambda value: KaggleProviderAdapter.from_environment(journal=ControlLedgerKaggleJournal(value))
    )
    adapter = factory(ledger)
    username = adapter.provider_identity().username
    build = wheel_builder or (lambda path, commit: _build_exact_wheel(root=path, commit_sha=commit))
    wheel_name, wheel_bytes = build(repository_root, exact_commit)
    wheel_sha256 = hashlib.sha256(wheel_bytes).hexdigest()
    input_slug = f"mdh-matrix-input-{str(matrix_uuid)[:8]}"
    input_ref = f"{username}/{input_slug}"
    dataset_task_id = uuid5(NAMESPACE_URL, f"real-kaggle-matrix:{matrix_uuid}:input-dataset")
    dataset_files: dict[str, bytes] = {wheel_name: wheel_bytes}
    scenario_runtime: dict[str, dict[str, Any]] = {}
    for planned in plan["scenarios"]:
        scenario = dict(planned)
        scenario.update({"matrix_id": str(matrix_uuid), "commit_sha": exact_commit})
        manifest, input_fingerprint = _scenario_manifest(
            scenario,
            matrix_id=matrix_uuid,
            commit_sha=exact_commit,
            created_at=str(plan["created_at"]),
        )
        manifest_name = f"manifest-{scenario['task_run_id']}.json"
        manifest_bytes = canonical_json_bytes(manifest)
        dataset_files[manifest_name] = manifest_bytes
        scenario_runtime[scenario["name"]] = {
            "scenario": scenario,
            "manifest_name": manifest_name,
            "manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
            "input_fingerprint": input_fingerprint,
        }
    input_arguments = {
        "content_tree_sha256": mapping_sha256(dataset_files),
        "control_class": ControlClass.MCP_MANAGED.value,
        "disposable": True,
    }
    input_intent = _matrix_effect(
        matrix_id=matrix_uuid,
        identity="input-dataset",
        action=MutationAction.CREATE_DATASET,
        ref=input_ref,
        task_id=dataset_task_id,
        arguments=input_arguments,
    )
    input_result = None
    input_cleanup = None
    summary_scenarios: list[dict[str, Any]] = []
    started_at = datetime.now(UTC)
    try:
        input_result = adapter.create_private_dataset(
            intent=input_intent,
            files=dataset_files,
            title=input_slug,
            control_class=ControlClass.MCP_MANAGED,
            disposable=True,
        )
        privacy = adapter.prove_private_dataset_access(
            provider_ref=input_ref,
            version=input_result.identity.version,
            unauthenticated_probe=AnonymousDatasetProbe(),
        )
        if input_result.identity.privacy != "private" or privacy.unauthenticated_http_status not in {401, 403, 404}:
            raise RuntimeError("matrix input Dataset privacy was not exactly proven")
        for planned in plan["scenarios"]:
            runtime = scenario_runtime[str(planned["name"])]
            scenario = runtime["scenario"]
            scenario_path = scenario_receipt_dir / f"{scenario['ordinal']:02d}-{scenario['name']}.json"
            if scenario_path.is_file():
                existing = json.loads(scenario_path.read_bytes())
                if (
                    existing.get("schema_version") == SCENARIO_SCHEMA
                    and existing.get("outcome") == "PASS"
                    and existing.get("live_evidence") is live_evidence
                    and existing.get("matrix_id") == str(matrix_uuid)
                    and existing.get("ordinal") == scenario["ordinal"]
                    and existing.get("scenario") == scenario["name"]
                    and existing.get("commit_sha") == exact_commit
                    and existing.get("task_run_id") == scenario["task_run_id"]
                    and existing.get("input_dataset")
                    == {
                        "provider_ref": input_ref,
                        "provider_version": input_result.identity.version,
                        "package_sha256": input_result.identity.package_sha256,
                    }
                    and existing.get("cleanup", {}).get("outcome") == "complete"
                ):
                    summary_scenarios.append(existing)
                    continue
                raise RuntimeError(f"unsafe or stale scenario resume receipt: {scenario_path}")
            task_id = UUID(str(scenario["task_id"]))
            task_run_id = UUID(str(scenario["task_run_id"]))
            slug = f"mdh-matrix-{scenario['ordinal']:02d}-{str(task_run_id)[:8]}"
            ref = f"{username}/{slug}"
            source = _render_generated_matrix_notebook(
                root=repository_root,
                scenario=scenario,
                input_slug=input_slug,
                wheel_name=wheel_name,
                wheel_sha256=wheel_sha256,
                manifest_name=runtime["manifest_name"],
                manifest_sha256=runtime["manifest_sha256"],
                commit_sha=exact_commit,
            )
            source_sha256 = _canonical_notebook_sha256(source)
            push_arguments = {
                "task_run_id": str(task_run_id),
                "source_sha256": source_sha256,
                "dataset_sources": (input_ref,),
                "control_class": ControlClass.MCP_MANAGED.value,
                "disposable": True,
            }
            push = _matrix_effect(
                matrix_id=matrix_uuid,
                identity=f"scenario:{scenario['ordinal']}:push",
                action=MutationAction.PUSH_NOTEBOOK,
                ref=ref,
                task_id=task_id,
                arguments=push_arguments,
            )
            launch_path = scenario_receipt_dir / (f"{scenario['ordinal']:02d}-{scenario['name']}.launch")
            launch_fence = {
                "schema_version": "my-data-hub-real-kaggle-matrix-launch.v1",
                "matrix_id": str(matrix_uuid),
                "commit_sha": exact_commit,
                "task_run_id": str(task_run_id),
                "provider_ref": ref,
                "source_sha256": source_sha256,
                "effect_id": str(push.effect_id),
            }
            result = None
            cleanup = None
            fault = {"name": scenario.get("fault_probe") or "none", "outcome": "NOT_RUN"}
            scenario_started = datetime.now(UTC)
            try:
                result = adapter.reconcile_private_notebook_mutation(
                    intent=push,
                    task_run_id=task_run_id,
                    expected_source_sha256=source_sha256,
                    dataset_sources=(input_ref,),
                    control_class=ControlClass.MCP_MANAGED,
                    disposable=True,
                )
                resumed = result is not None
                if result is None:
                    if launch_path.exists():
                        _persist_launch_fence(launch_path, launch_fence)
                        raise RuntimeError(
                            "exact provider run is absent after its durable launch fence; "
                            "use a new matrix identity rather than launching it twice"
                        )
                    _persist_launch_fence(launch_path, launch_fence)
                    result = adapter.push_private_notebook(
                        intent=push,
                        task_run_id=task_run_id,
                        source=source,
                        title=slug,
                        code_file="worker.ipynb",
                        kernel_type="notebook",
                        language="python",
                        control_class=ControlClass.MCP_MANAGED,
                        disposable=True,
                        dataset_sources=(input_ref,),
                        enable_internet=False,
                        timeout_seconds=900,
                    )
                terminal = adapter.poll_run(
                    result.run,
                    PollPolicy(interval_seconds=15, timeout_seconds=900, max_polls=60),
                )
                with tempfile.TemporaryDirectory(prefix="my-data-hub-matrix-output-") as folder:
                    output_identity = adapter.download_exact_run_output_file(
                        result.run,
                        destination=Path(folder),
                        file_name=MATRIX_RESULT_NAME,
                        max_bytes=1024 * 1024,
                    )
                    raw_result = (Path(folder) / MATRIX_RESULT_NAME).read_bytes()
                typed_result = _validate_completed_result(
                    raw=raw_result,
                    scenario=scenario,
                    manifest_sha256=runtime["manifest_sha256"],
                    input_fingerprint=runtime["input_fingerprint"],
                )
                fault = _run_matrix_fault_probe(
                    adapter=adapter,
                    scenario=scenario,
                    result=result,
                    output_tree_sha256=output_identity.output_tree_sha256,
                )
                if terminal.state.value != "complete" or result.source.privacy != "private":
                    raise RuntimeError("matrix run lacks exact private terminal provider evidence")
            finally:
                if result is not None:
                    cleanup = _delete_claimed_resource(
                        adapter=adapter,
                        matrix_id=matrix_uuid,
                        identity=f"scenario:{scenario['ordinal']}",
                        task_id=task_id,
                        claim=result.claim,
                    )
            if result is None or cleanup is None:
                raise RuntimeError(f"scenario {scenario['name']} lacks exact run/cleanup evidence")
            if scenario.get("fault_probe") == "cleanup_replay":
                replay = _delete_claimed_resource(
                    adapter=adapter,
                    matrix_id=matrix_uuid,
                    identity=f"scenario:{scenario['ordinal']}:replay",
                    task_id=task_id,
                    claim=result.claim,
                )
                fault = {"name": "cleanup_replay", "outcome": "PASS", "detail_code": replay.detail_code}
            scenario_receipt = {
                "schema_version": SCENARIO_SCHEMA,
                "matrix_id": str(matrix_uuid),
                "ordinal": scenario["ordinal"],
                "scenario": scenario["name"],
                "category": scenario["category"],
                "variant": scenario["variant"],
                "outcome": "PASS",
                "live_evidence": live_evidence,
                "commit_sha": exact_commit,
                "task_id": str(task_id),
                "task_run_id": str(task_run_id),
                "provider_ref": ref,
                "provider_run_ref": result.run.provider_run_ref,
                "provider_kernel_id": result.run.provider_kernel_id,
                "source_version": result.run.source_version,
                "source_sha256": result.run.source_sha256,
                "provider_status": terminal.state.value,
                "privacy": result.source.privacy,
                "input_dataset": {
                    "provider_ref": input_ref,
                    "provider_version": input_result.identity.version,
                    "package_sha256": input_result.identity.package_sha256,
                },
                "manifest_sha256": runtime["manifest_sha256"],
                "input_fingerprint": runtime["input_fingerprint"],
                "result_sha256": hashlib.sha256(raw_result).hexdigest(),
                "output_tree_sha256": output_identity.output_tree_sha256,
                "checkpoint_binding": (
                    {
                        "checkpoint_id": scenario["checkpoint_id"],
                        "manifest_sha256": scenario["checkpoint_manifest_sha256"],
                        "current_checkpoint_id": scenario["checkpoint_id"],
                    }
                    if scenario["checkpoint_bound"]
                    else None
                ),
                "retry_policy": {
                    "interval_seconds": 15,
                    "timeout_seconds": 900,
                    "max_polls": 60,
                },
                "resume": {"reused_exact_run": resumed},
                "fault_probe": fault,
                "accounting": {
                    "input_items": typed_result.metrics["input_items"],
                    "accounted_items": typed_result.metrics["accounted_items"],
                    "successful_items": typed_result.metrics["successful_items"],
                    "failed_items": typed_result.metrics["failed_items"],
                },
                "cleanup": {"outcome": "complete", "detail_code": cleanup.detail_code},
                "started_at": scenario_started.isoformat(),
                "completed_at": datetime.now(UTC).isoformat(),
            }
            scenario_path.write_bytes(canonical_json_bytes(scenario_receipt))
            summary_scenarios.append(scenario_receipt)
    finally:
        if input_result is not None:
            input_cleanup = _delete_claimed_resource(
                adapter=adapter,
                matrix_id=matrix_uuid,
                identity="input-dataset",
                task_id=dataset_task_id,
                claim=input_result.claim,
            )
    distinct_run_ids = {item["task_run_id"] for item in summary_scenarios if item.get("outcome") == "PASS"}
    distinct_provider_run_refs = {
        item["provider_run_ref"] for item in summary_scenarios if item.get("outcome") == "PASS"
    }
    if input_cleanup is None:
        raise RuntimeError("matrix input Dataset lacks claim-bound cleanup evidence")
    summary = {
        "schema_version": MATRIX_SCHEMA,
        "matrix_id": str(matrix_uuid),
        "commit_sha": exact_commit,
        "outcome": (
            "SMOKE_PASS"
            if len(distinct_run_ids) >= MATRIX_MINIMUM_RUNS and len(distinct_provider_run_refs) >= MATRIX_MINIMUM_RUNS
            else "FAIL"
        ),
        "matrix_scope": "platform_smoke_only",
        "minimum_real_runs": MATRIX_MINIMUM_RUNS,
        "planned_runs": len(plan["scenarios"]),
        "completed_real_runs": len(distinct_run_ids),
        "distinct_real_run_ids": sorted(distinct_run_ids),
        "distinct_provider_run_refs": sorted(distinct_provider_run_refs),
        "input_dataset": {
            "provider_ref": input_ref,
            "provider_version": input_result.identity.version,
            "privacy": input_result.identity.privacy,
            "cleanup": input_cleanup.detail_code,
        },
        "scenario_receipts": [
            {
                "ordinal": item["ordinal"],
                "scenario": item["scenario"],
                "task_run_id": item["task_run_id"],
                "provider_run_ref": item["provider_run_ref"],
                "outcome": item["outcome"],
                "receipt": f"{item['ordinal']:02d}-{item['scenario']}.json",
            }
            for item in sorted(summary_scenarios, key=lambda value: int(value["ordinal"]))
        ],
        "coverage": sorted({str(item["category"]) for item in summary_scenarios}),
        "started_at": started_at.isoformat(),
        "completed_at": datetime.now(UTC).isoformat(),
        "live_evidence": live_evidence,
        "blockers": (
            ["MANDATORY_OPERATIONAL_SCENARIOS_NOT_EXECUTED"]
            if len(distinct_run_ids) >= MATRIX_MINIMUM_RUNS and len(distinct_provider_run_refs) >= MATRIX_MINIMUM_RUNS
            else ["PLATFORM_SMOKE_RUNS_INCOMPLETE"]
        ),
    }
    receipt_path.parent.mkdir(parents=True, exist_ok=True)
    receipt_path.write_bytes(canonical_json_bytes(summary))
    return EXTERNAL_BLOCKED if summary["outcome"] == "SMOKE_PASS" else 1


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=("preflight", "dataset-canary", "notebook-canary", "matrix"))
    parser.add_argument(
        "--ledger",
        type=Path,
        default=Path("~/.local/state/my-data-hub/provider-effects.sqlite3").expanduser(),
    )
    parser.add_argument(
        "--receipt",
        type=Path,
        default=None,
    )
    parser.add_argument("--scenario-receipts", type=Path, default=Path("artifacts/kaggle-matrix-scenarios"))
    parser.add_argument("--plan", type=Path, default=Path("artifacts/kaggle-matrix-plan.json"))
    parser.add_argument("--matrix-id", type=UUID, default=None)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.command == "preflight":
        payload = {
            "modern_token_configured": modern_token_configured(),
            "legacy_credentials_present": (Path("~/.kaggle/kaggle.json").expanduser().is_file()),
            "private_notebook_exact_read_ready": modern_token_configured(),
        }
        print(json.dumps(payload, sort_keys=True))
        return 0 if payload["private_notebook_exact_read_ready"] else EXTERNAL_BLOCKED
    if args.command == "dataset-canary":
        receipt = args.receipt or Path(".codex/operational-mvp/evidence/real-kaggle/dataset-canary.json")
        return run_dataset_canary(ledger_path=args.ledger, receipt_path=receipt)
    if args.command == "matrix":
        receipt = args.receipt or Path("artifacts/kaggle-matrix.json")
        return run_real_matrix(
            ledger_path=args.ledger,
            receipt_path=receipt,
            scenario_receipt_dir=args.scenario_receipts,
            plan_path=args.plan,
            matrix_id=args.matrix_id,
        )
    receipt = args.receipt or Path(".codex/operational-mvp/evidence/real-kaggle/notebook-canary.json")
    return run_notebook_canary(ledger_path=args.ledger, receipt_path=receipt)


if __name__ == "__main__":
    raise SystemExit(main())
