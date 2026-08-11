#!/usr/bin/env python3
"""Execute the 24-scenario operational Kaggle acceptance matrix.

The older :mod:`real_kaggle_matrix` command is intentionally a platform-smoke
probe.  This runner accepts only scenario-specific operational Notebook output,
reconciles every claimed run through the repository's single
``KaggleProviderAdapter``, and counts exact numeric Kaggle run references rather
than caller-generated UUIDs.

The deployed control/host fault injector is deliberately an external bounded
interface.  It is not present in this repository.  Until it is configured, all
24 scenarios are emitted as typed ``BLOCKED`` receipts and the command exits
78; a missing interface is never converted into a passing smoke run.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

from pydantic import BaseModel, ConfigDict, Field, model_validator

from my_data_hub.acceptance.master_lifecycle import (
    CallbackLossEvidence,
    LeaseExpiryEvidence,
    MasterAcceptanceReceipt,
    OldEpochEvidence,
    RotationSoakEvidence,
)
from my_data_hub.acceptance.scenario_operator import CheckpointAcceptanceOperationalResult
from my_data_hub.hashing import canonical_json_bytes

EXTERNAL_BLOCKED = 78
FAIL = 1
PASS = 0
PLAN_SCHEMA = "my-data-hub-operational-kaggle-matrix-plan.v1"
SCENARIO_SCHEMA = "my-data-hub-operational-kaggle-scenario-receipt.v1"
SUMMARY_SCHEMA = "my-data-hub-operational-kaggle-matrix-receipt.v1"
RESULT_FILE = "operational-result.json"
MINIMUM_DISTINCT_PROVIDER_RUNS = 15
MINIMUM_SOAK_SECONDS = 60 * 60
MAXIMUM_SOAK_SECONDS = 90 * 60
MAX_RESULT_BYTES = 2 * 1024 * 1024
RESUMABLE_OWNER_BLOCKER = "FM16_AWAITING_OWNER_AUTHORIZATION"
TRUSTED_DRIVER_PATH = Path(__file__).with_name("operational_kaggle_driver.py")
CHECKPOINT_REQUIREMENTS = frozenset({"FM05", "FM14", "FM15"})
CHECKPOINT_SCENARIO_NAMES = frozenset(
    {
        "verified-empty-checkpoint-roundtrip",
        "corrupt-candidate-head-preserved",
        "restore-smoke-failure-head-preserved",
    }
)


@dataclass(frozen=True, slots=True)
class OperationalScenario:
    requirement_id: str
    name: str
    category: str
    assertions: tuple[str, ...]
    integration_dependency: str
    lifecycle_gates: tuple[str, ...] = ()


SCENARIOS: tuple[OperationalScenario, ...] = (
    OperationalScenario(
        "FM01",
        "private-dataset-create-readback-delete",
        "provider",
        ("private_create", "exact_readback", "claim_bound_delete"),
        "operational driver must create/read/delete a disposable private Dataset",
    ),
    OperationalScenario(
        "FM02",
        "private-notebook-source-run-output-delete",
        "provider",
        ("exact_source", "terminal_complete", "exact_output", "claim_bound_delete"),
        "operational driver must run and clean a disposable private Notebook",
    ),
    OperationalScenario(
        "FM03",
        "runtime-callback-heartbeat-terminal",
        "runtime",
        ("callback_bound", "heartbeat_observed", "terminal_event_bound"),
        "runtime event observation API with exact run and epoch identity",
    ),
    OperationalScenario(
        "FM04",
        "empty-postgresql-master-bootstrap",
        "master",
        ("empty_checkpoint_selected", "postgres_bootstrapped", "master_active"),
        "disposable empty-master bootstrap action and status API",
        ("master_boot",),
    ),
    OperationalScenario(
        "FM05",
        "verified-empty-checkpoint-roundtrip",
        "checkpoint",
        ("candidate_uploaded", "exact_readback", "restore_verified", "head_advanced"),
        "checkpoint publish/readback/restore action API",
    ),
    OperationalScenario(
        "FM06",
        "cold-master-restore",
        "master",
        ("verified_checkpoint_selected", "cold_restore_complete", "revision_equal"),
        "cold-restore action API with exact checkpoint identity",
        ("master_boot",),
    ),
    OperationalScenario(
        "FM07",
        "concurrent-ensure-master-single-run",
        "concurrency",
        ("twenty_requests", "one_operation", "one_provider_run", "all_resolved_same_epoch"),
        "20-way concurrent master.ensure client and provider inventory",
    ),
    OperationalScenario(
        "FM08",
        "callback-loss-status-output-recovery",
        "recovery",
        ("callback_withheld", "status_reconciled", "output_identity_reconciled"),
        "callback-loss fault injector and restart-safe reconciliation",
        ("abrupt_master_termination", "control_plane_restart"),
    ),
    OperationalScenario(
        "FM09",
        "duplicate-stale-callback-output-rejection",
        "fencing",
        ("duplicate_callback_noop", "stale_callback_rejected", "stale_output_rejected"),
        "callback and output replay fault injector",
    ),
    OperationalScenario(
        "FM10",
        "lease-expiry-write-gate-closes",
        "fencing",
        ("lease_expired", "write_denied", "credentials_invalidated"),
        "lease clock/fault control and real write admission probe",
    ),
    OperationalScenario(
        "FM11",
        "old-epoch-return-fenced",
        "fencing",
        ("epoch_advanced", "old_renew_denied", "old_register_denied", "old_write_denied", "registry_resolves_new"),
        "split-brain old-run resume injector and admission probes",
        ("clean_rotation",),
    ),
    OperationalScenario(
        "FM12",
        "clean-drain-checkpoint-stop",
        "master",
        ("drain_started", "checkpoint_verified", "terminal_stopped"),
        "clean drain/checkpoint/stop action API",
    ),
    OperationalScenario(
        "FM13",
        "forced-rotation-new-run-epoch",
        "master",
        ("old_run_stopped", "new_run_distinct", "epoch_incremented", "checkpoint_bound"),
        "forced rotation action API",
        ("clean_rotation", "master_boot"),
    ),
    OperationalScenario(
        "FM14",
        "corrupt-candidate-head-preserved",
        "checkpoint",
        ("corrupt_candidate_uploaded", "hash_mismatch_detected", "old_head_unchanged"),
        "checkpoint corruption fault injector",
    ),
    OperationalScenario(
        "FM15",
        "restore-smoke-failure-head-preserved",
        "checkpoint",
        ("restore_smoke_forced_failure", "candidate_rejected", "old_head_unchanged"),
        "restore-smoke failure injector",
    ),
    OperationalScenario(
        "FM16",
        "full-ydb-blogger-import-checkpoint",
        "migration",
        ("full_export_accounted", "transactional_import", "quarantine_accounted", "checkpoint_verified"),
        "production YDB export plus migration-operator preview/apply",
    ),
    OperationalScenario(
        "FM17",
        "post-import-cold-restore-equality",
        "migration",
        ("cold_restore_complete", "row_count_equal", "logical_hash_equal"),
        "post-import checkpoint restore and logical equality query",
        ("master_boot",),
    ),
    OperationalScenario(
        "FM18",
        "e5-corpus-worker-transactional-import",
        "embedding",
        ("exact_e5_model", "corpus_accounted", "transactional_import", "checkpoint_required"),
        "E5 worker submission and embedding import status interface",
    ),
    OperationalScenario(
        "FM19",
        "bge-m3-corpus-worker-transactional-import",
        "embedding",
        ("exact_bge_m3_model", "corpus_accounted", "transactional_import", "checkpoint_required"),
        "BGE-M3 worker submission and embedding import status interface",
    ),
    OperationalScenario(
        "FM20",
        "remote-mcp-cold-start-blogger-search",
        "mcp",
        ("master_initially_absent", "ensure_triggered", "search_completed", "bounded_result"),
        "remote reader MCP plus host reboot controller",
        ("host_reboot",),
    ),
    OperationalScenario(
        "FM21",
        "owner-preview-apply-checkpoint-receipt",
        "mcp",
        ("disposable_row", "preview_bound", "apply_bound", "post_checkpoint_verified", "durable_receipt"),
        "controlled business row and owner/operator MCP credential",
    ),
    OperationalScenario(
        "FM22",
        "mcp-managed-provider-lifecycle-cleanup",
        "mcp",
        ("private_dataset_lifecycle", "private_notebook_lifecycle", "exact_readback", "claim_cleanup"),
        "provider-operator MCP credential and lifecycle tools",
    ),
    OperationalScenario(
        "FM23",
        "protected-kaggle-mutation-denial",
        "security",
        ("protected_resource_selected", "mutation_denied", "mutation_not_attempted"),
        "operator acceptance probe for protected provider resources",
    ),
    OperationalScenario(
        "FM24",
        "accelerated-session-rotation-soak",
        "soak",
        ("duration_in_range", "heartbeats_continuous", "reads_succeeded", "checkpoint_verified", "recovery_succeeded"),
        "60-90 minute soak controller with session rotation",
        ("soak",),
    ),
)

if len(SCENARIOS) != 24 or tuple(item.requirement_id for item in SCENARIOS) != tuple(
    f"FM{ordinal:02d}" for ordinal in range(1, 25)
):  # pragma: no cover - import-time authoring invariant
    raise RuntimeError("operational matrix must contain the exact ordered FM01-FM24 contract")


class AssertionResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    name: str = Field(pattern=r"^[a-z0-9_]+$", max_length=100)
    outcome: Literal["PASS", "FAIL"]
    evidence_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")


class LifecycleEvent(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    gate: Literal[
        "master_boot",
        "clean_rotation",
        "abrupt_master_termination",
        "control_plane_restart",
        "host_reboot",
        "soak",
    ]
    event_id: UUID
    started_at: datetime
    completed_at: datetime
    old_provider_run_ref: str | None = None
    new_provider_run_ref: str | None = None
    old_epoch: int | None = Field(default=None, ge=1)
    new_epoch: int | None = Field(default=None, ge=1)
    operation_id: str | None = Field(default=None, min_length=1, max_length=300)
    before_identity: str | None = Field(default=None, min_length=1, max_length=300)
    after_identity: str | None = Field(default=None, min_length=1, max_length=300)
    duration_seconds: int | None = Field(default=None, ge=1)
    heartbeat_count: int | None = Field(default=None, ge=0)
    read_query_count: int | None = Field(default=None, ge=0)
    checkpoint_count: int | None = Field(default=None, ge=0)
    recovery_count: int | None = Field(default=None, ge=0)

    @model_validator(mode="after")
    def exact_gate_shape(self) -> LifecycleEvent:
        if self.completed_at < self.started_at:
            raise ValueError("lifecycle event ends before it starts")
        if self.gate == "master_boot" and not (self.new_provider_run_ref and self.new_epoch):
            raise ValueError("master boot must bind the new provider run and epoch")
        if self.gate == "clean_rotation" and not (
            self.old_provider_run_ref
            and self.new_provider_run_ref
            and self.old_provider_run_ref != self.new_provider_run_ref
            and self.old_epoch
            and self.new_epoch == self.old_epoch + 1
        ):
            raise ValueError("clean rotation must bind distinct runs and consecutive epochs")
        if self.gate == "abrupt_master_termination" and not (
            self.operation_id
            and self.old_provider_run_ref
            and self.new_provider_run_ref
            and self.old_provider_run_ref != self.new_provider_run_ref
        ):
            raise ValueError("abrupt termination must bind the operation and old/recovery runs")
        if self.gate in {"control_plane_restart", "host_reboot"} and not (
            self.operation_id
            and self.before_identity
            and self.after_identity
            and self.before_identity != self.after_identity
        ):
            raise ValueError("restart/reboot must bind the operation and distinct before/after identities")
        if self.gate == "soak" and not (
            self.operation_id
            and self.duration_seconds
            and MINIMUM_SOAK_SECONDS <= self.duration_seconds <= MAXIMUM_SOAK_SECONDS
            and (self.heartbeat_count or 0) > 0
            and (self.read_query_count or 0) > 0
            and (self.checkpoint_count or 0) > 0
            and (self.recovery_count or 0) > 0
        ):
            raise ValueError("soak must be 60-90 minutes with heartbeat/read/checkpoint/recovery")
        return self


class DriverCapabilityCheck(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    name: str = Field(pattern=r"^[a-z0-9_.-]+$", max_length=120)
    outcome: Literal["PASS", "BLOCKED", "FAIL"]
    evidence_sha256: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")
    detail_code: str = Field(pattern=r"^[A-Z0-9_]+$", max_length=120)


class DriverCleanupBinding(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    claim_task_id: UUID | None = None
    claim_sha256: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")
    provider_ref: str = Field(pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
    provider_run_ref: str = Field(pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/[1-9][0-9]*$")
    provider_kernel_id: int = Field(ge=1)
    source_version: int = Field(ge=1)
    source_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    output_receipt_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    output_file_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    output_tree_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")

    @model_validator(mode="after")
    def exact_claim(self) -> DriverCleanupBinding:
        if (self.claim_task_id is None) != (self.claim_sha256 is None):
            raise ValueError("driver claim identity must be wholly present or absent")
        return self


class DriverControlLifecycle(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    gate: Literal["abrupt_master_termination", "control_plane_restart", "clean_rotation"]
    operation_id: UUID
    old_provider_run_ref: str | None = None
    new_provider_run_ref: str | None = None
    old_epoch: int | None = Field(default=None, ge=1)
    new_epoch: int | None = Field(default=None, ge=1)
    before_identity: UUID | None = None
    after_identity: UUID | None = None

    @model_validator(mode="after")
    def exact_gate(self) -> DriverControlLifecycle:
        if self.gate in {"abrupt_master_termination", "clean_rotation"} and not (
            self.old_provider_run_ref
            and self.new_provider_run_ref
            and self.old_provider_run_ref != self.new_provider_run_ref
        ):
            raise ValueError("control lifecycle requires distinct provider runs")
        if self.gate == "clean_rotation" and not (
            self.old_epoch and self.new_epoch == self.old_epoch + 1
        ):
            raise ValueError("control clean rotation requires consecutive epochs")
        if self.gate == "control_plane_restart" and not (
            self.before_identity and self.after_identity and self.before_identity != self.after_identity
        ):
            raise ValueError("control restart requires distinct identities")
        return self


class DriverResult(BaseModel):
    """Control-owned locator/receipt returned by the pinned operational driver."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    schema_version: Literal["my-data-hub-operational-kaggle-driver-result.v2"]
    phase: Literal["EXECUTE", "RECONCILE", "CLEANUP"]
    outcome: Literal["READY", "PASS", "FAIL", "BLOCKED"]
    scenario: str
    task_run_id: UUID
    provider_ref: str | None = Field(default=None, pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
    provider_run_ref: str | None = Field(default=None, pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/[1-9][0-9]*$")
    provider_kernel_id: int | None = Field(default=None, ge=1)
    source_version: int | None = Field(default=None, ge=1)
    source_sha256: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")
    blocker_code: str | None = Field(default=None, pattern=r"^[A-Z0-9_]+$", max_length=100)
    integration_dependency: str | None = Field(default=None, min_length=1, max_length=500)
    mutations_started: int = Field(ge=0)
    capability_checks: tuple[DriverCapabilityCheck, ...]
    observation_sha256: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")
    claim_task_id: UUID | None = None
    claim_sha256: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")
    output_receipt_sha256: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")
    output_file_sha256: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")
    output_tree_sha256: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")
    cleanup_state: Literal["NOT_REQUIRED", "PENDING", "COMPLETE"]
    scenario_output: dict[str, Any] | None = None
    control_receipt: MasterAcceptanceReceipt | None = None
    control_lifecycle: tuple[DriverControlLifecycle, ...] = ()

    @model_validator(mode="after")
    def pass_has_exact_provider_locator(self) -> DriverResult:
        locator = (
            self.provider_ref,
            self.provider_run_ref,
            self.provider_kernel_id,
            self.source_version,
            self.source_sha256,
        )
        cleanup = (
            self.claim_task_id,
            self.claim_sha256,
            self.output_receipt_sha256,
            self.output_file_sha256,
            self.output_tree_sha256,
        )
        if self.outcome in {"READY", "PASS"} and any(value is None for value in locator):
            raise ValueError("successful driver result lacks exact provider run identity")
        if self.phase == "EXECUTE" and self.outcome in {"READY", "PASS"} and (
            (self.scenario_output is None) == (self.control_receipt is None)
        ):
            raise ValueError("successful execution requires exactly one trusted evidence payload")
        if self.outcome in {"READY", "PASS"} and self.mutations_started < 1:
            raise ValueError("successful driver result lacks a real evidence Notebook mutation")
        if self.outcome == "READY" and (
            self.phase != "EXECUTE" or self.cleanup_state != "PENDING" or any(value is None for value in cleanup)
        ):
            raise ValueError("READY driver result lacks exact pending cleanup evidence")
        if self.phase == "CLEANUP" and self.outcome == "PASS" and self.cleanup_state != "COMPLETE":
            raise ValueError("cleanup PASS requires COMPLETE cleanup evidence")
        if (
            self.phase == "EXECUTE"
            and self.outcome == "PASS"
            and self.scenario in CHECKPOINT_SCENARIO_NAMES
            and (
                self.output_receipt_sha256 is None
                or self.output_file_sha256 is None
                or self.output_tree_sha256 is None
                or self.claim_task_id is None
                or self.claim_sha256 is None
                or self.cleanup_state != "NOT_REQUIRED"
            )
        ):
            raise ValueError("checkpoint evidence locator lacks exact output receipts")
        if self.outcome == "BLOCKED" and not (self.blocker_code and self.integration_dependency):
            raise ValueError("BLOCKED driver result lacks a concrete integration dependency")
        if self.outcome == "BLOCKED" and self.mutations_started != 0:
            raise ValueError("BLOCKED driver result cannot leave an unreceipted mutation")
        return self


class OperationalOutput(BaseModel):
    """Exact output produced inside the operational Kaggle Notebook."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    schema_version: Literal["my-data-hub-operational-kaggle-output.v1"]
    matrix_id: UUID
    scenario: str
    task_run_id: UUID
    outcome: Literal["PASS", "FAIL"]
    assertions: tuple[AssertionResult, ...]
    lifecycle_events: tuple[LifecycleEvent, ...] = ()
    operation_ids: tuple[str, ...] = ()
    completed_at: datetime


def _trusted_driver_command() -> tuple[str, str]:
    path = TRUSTED_DRIVER_PATH.resolve(strict=True)
    if TRUSTED_DRIVER_PATH.is_symlink() or not path.is_file() or path.parent != Path(__file__).resolve().parent:
        raise RuntimeError("checked-in operational driver path is not an exact regular sibling")
    return sys.executable, str(path)


def _exact_commit(root: Path) -> str:
    commit = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=root, check=True, capture_output=True, text=True
    ).stdout.strip()
    dirty = subprocess.run(["git", "status", "--porcelain=v1", "-z"], cwd=root, check=True, capture_output=True).stdout
    if len(commit) != 40 or any(value not in "0123456789abcdef" for value in commit):
        raise RuntimeError("operational matrix requires an exact Git commit")
    if dirty:
        raise RuntimeError("operational provider mutation requires a clean worktree")
    return commit


def build_plan(*, matrix_id: UUID, commit_sha: str, created_at: datetime) -> dict[str, Any]:
    if len(commit_sha) != 40 or any(value not in "0123456789abcdef" for value in commit_sha):
        raise ValueError("plan requires an exact commit SHA")
    rows = []
    for ordinal, spec in enumerate(SCENARIOS, 1):
        namespace = f"operational-kaggle:{matrix_id}:{spec.requirement_id}:{spec.name}"
        rows.append(
            {
                "ordinal": ordinal,
                "requirement_id": spec.requirement_id,
                "name": spec.name,
                "category": spec.category,
                "planned_task_run_id": str(uuid5(NAMESPACE_URL, f"{namespace}:run")),
                "required_assertions": list(spec.assertions),
                "lifecycle_gates": list(spec.lifecycle_gates),
                "integration_dependency": spec.integration_dependency,
            }
        )
    return {
        "schema_version": PLAN_SCHEMA,
        "matrix_id": str(matrix_id),
        "commit_sha": commit_sha,
        "created_at": created_at.astimezone(UTC).isoformat(),
        "minimum_distinct_provider_runs": MINIMUM_DISTINCT_PROVIDER_RUNS,
        "scenarios": rows,
    }


def _atomic_write(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = canonical_json_bytes(dict(payload))
    temporary = path.with_name(f".{path.name}.{uuid4()}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        os.chmod(path, 0o600)
    finally:
        if temporary.exists():
            temporary.unlink()


def _load_or_create_plan(path: Path, *, matrix_id: UUID | None, commit_sha: str) -> dict[str, Any]:
    if path.is_file():
        value = json.loads(path.read_bytes())
        expected = build_plan(
            matrix_id=UUID(str(value.get("matrix_id"))),
            commit_sha=commit_sha,
            created_at=datetime.fromisoformat(str(value.get("created_at"))),
        )
        if value != expected or (matrix_id is not None and value["matrix_id"] != str(matrix_id)):
            raise RuntimeError("existing operational plan differs from the exact scenario contract")
        return value
    value = build_plan(matrix_id=matrix_id or uuid4(), commit_sha=commit_sha, created_at=datetime.now(UTC))
    _atomic_write(path, value)
    return value


def _blocker_receipt(
    *, plan: Mapping[str, Any], scenario: Mapping[str, Any], code: str, dependency: str
) -> dict[str, Any]:
    now = datetime.now(UTC).isoformat()
    return {
        "schema_version": SCENARIO_SCHEMA,
        "matrix_id": plan["matrix_id"],
        "commit_sha": plan["commit_sha"],
        "ordinal": scenario["ordinal"],
        "requirement_id": scenario["requirement_id"],
        "scenario": scenario["name"],
        "outcome": "BLOCKED",
        "live_evidence": False,
        "planned_task_run_id": scenario["planned_task_run_id"],
        "real_run_identity": None,
        "assertions": [],
        "lifecycle_events": [],
        "operation_ids": [],
        "driver_mutations_started": 0,
        "driver_capability_checks": [],
        "driver_observation_sha256": None,
        "blocker": {"code": code, "integration_dependency": dependency},
        "started_at": now,
        "completed_at": now,
    }


def _scenario_path(directory: Path, row: Mapping[str, Any]) -> Path:
    return directory / f"{int(row['ordinal']):02d}-{row['name']}.json"


def _write_all_blocked(
    *, plan: Mapping[str, Any], directory: Path, code: str, dependency: str | None = None
) -> list[dict[str, Any]]:
    receipts = []
    for row in plan["scenarios"]:
        receipt = _blocker_receipt(
            plan=plan,
            scenario=row,
            code=code,
            dependency=dependency or str(row["integration_dependency"]),
        )
        _atomic_write(_scenario_path(directory, row), receipt)
        receipts.append(receipt)
    return receipts


def _driver_request(plan: Mapping[str, Any], row: Mapping[str, Any], *, resume_only: bool) -> dict[str, Any]:
    return {
        "schema_version": "my-data-hub-operational-kaggle-driver-request.v2",
        "phase": "EXECUTE",
        "matrix_id": plan["matrix_id"],
        "commit_sha": plan["commit_sha"],
        "ordinal": row["ordinal"],
        "requirement_id": row["requirement_id"],
        "scenario": row["name"],
        "task_run_id": row["planned_task_run_id"],
        "required_assertions": row["required_assertions"],
        "lifecycle_gates": row["lifecycle_gates"],
        "minimum_soak_seconds": MINIMUM_SOAK_SECONDS if "soak" in row["lifecycle_gates"] else None,
        "maximum_soak_seconds": MAXIMUM_SOAK_SECONDS if "soak" in row["lifecycle_gates"] else None,
        "result_file": RESULT_FILE,
        "resume_only": resume_only,
        "evidence_issued_at": plan["created_at"],
        "cleanup": None,
    }


def _cleanup_request(
    execute_request: Mapping[str, Any], binding: DriverCleanupBinding
) -> dict[str, Any]:
    return {
        **dict(execute_request),
        "phase": "CLEANUP",
        "resume_only": True,
        "cleanup": binding.model_dump(mode="json"),
    }


def _reconcile_request(
    execute_request: Mapping[str, Any], binding: DriverCleanupBinding
) -> dict[str, Any]:
    return {
        **dict(execute_request),
        "phase": "RECONCILE",
        "resume_only": True,
        "cleanup": binding.model_dump(mode="json"),
    }


def _driver_binding(locator: DriverResult) -> DriverCleanupBinding:
    if any(
        value is None
        for value in (
            locator.provider_ref,
            locator.provider_run_ref,
            locator.provider_kernel_id,
            locator.source_version,
            locator.source_sha256,
            locator.output_receipt_sha256,
            locator.output_file_sha256,
            locator.output_tree_sha256,
        )
    ):
        raise RuntimeError("successful driver execution lacks exact reconciliation fields")
    return DriverCleanupBinding(
        claim_task_id=locator.claim_task_id,
        claim_sha256=locator.claim_sha256,
        provider_ref=locator.provider_ref,  # type: ignore[arg-type]
        provider_run_ref=locator.provider_run_ref,  # type: ignore[arg-type]
        provider_kernel_id=locator.provider_kernel_id,  # type: ignore[arg-type]
        source_version=locator.source_version,  # type: ignore[arg-type]
        source_sha256=locator.source_sha256,  # type: ignore[arg-type]
        output_receipt_sha256=locator.output_receipt_sha256,  # type: ignore[arg-type]
        output_file_sha256=locator.output_file_sha256,  # type: ignore[arg-type]
        output_tree_sha256=locator.output_tree_sha256,  # type: ignore[arg-type]
    )


def _reconcile_driver_result(
    execute_request: Mapping[str, Any],
    locator: DriverResult,
    *,
    timeout_seconds: int,
) -> tuple[DriverResult, DriverCleanupBinding]:
    binding = _driver_binding(locator)
    result = _invoke_driver(
        _reconcile_request(execute_request, binding), timeout_seconds=timeout_seconds
    )
    if (
        result.phase != "RECONCILE"
        or result.outcome != "PASS"
        or result.scenario != locator.scenario
        or result.task_run_id != locator.task_run_id
        or _driver_binding(result) != binding
        or result.control_receipt != locator.control_receipt
        or result.control_lifecycle != locator.control_lifecycle
    ):
        raise RuntimeError("independent control reconciliation differs from execution evidence")
    return result, binding


def _invoke_driver(request: Mapping[str, Any], *, timeout_seconds: int) -> DriverResult:
    with tempfile.TemporaryDirectory(prefix="my-data-hub-operational-driver-") as folder:
        request_path = Path(folder) / "request.json"
        result_path = Path(folder) / "result.json"
        request_path.write_bytes(canonical_json_bytes(dict(request)))
        completed = subprocess.run(
            [*_trusted_driver_command(), "--request", str(request_path), "--result", str(result_path)],
            check=False,
            timeout=timeout_seconds,
            env=os.environ.copy(),
        )
        if completed.returncode not in {PASS, FAIL, EXTERNAL_BLOCKED}:
            raise RuntimeError(f"operational driver failed with exit {completed.returncode}")
        if not result_path.is_file() or result_path.is_symlink() or result_path.stat().st_size > 256 * 1024:
            raise RuntimeError("operational driver did not emit a bounded regular result")
        result = DriverResult.model_validate_json(result_path.read_bytes())
        if completed.returncode == EXTERNAL_BLOCKED and result.outcome != "BLOCKED":
            raise RuntimeError("driver exit 78 must carry a BLOCKED result")
        if completed.returncode == FAIL and result.outcome != "FAIL":
            raise RuntimeError("driver exit 1 must carry a FAIL result")
        if completed.returncode == PASS and result.outcome not in {"READY", "PASS"}:
            raise RuntimeError("driver exit 0 must carry READY or PASS")
        return result


_CHECKPOINT_OUTPUT_STAGES: dict[str, tuple[tuple[str, str, str], ...]] = {
    "FM05": (
        ("empty_candidate", "EMPTY_CANDIDATE_CREATED", "succeeded"),
        ("private_upload", "PRIVATE_CANDIDATE_UPLOADED", "succeeded"),
        ("exact_readback", "EXACT_READBACK_VERIFIED", "succeeded"),
        ("independent_restore", "INDEPENDENT_RESTORE_VERIFIED", "succeeded"),
        ("cas_promotion", "HEAD_CAS_PROMOTED", "succeeded"),
    ),
    "FM14": (
        ("corrupted_candidate", "TASK_OWNED_CORRUPTION_CANDIDATE_CREATED", "succeeded"),
        ("hash_mismatch_rejection", "EXACT_READBACK_HASH_MISMATCH_REJECTED", "rejected_expected"),
    ),
    "FM15": (
        ("restore_failure_candidate", "TASK_OWNED_RESTORE_FAILURE_CANDIDATE_CREATED", "succeeded"),
        ("exact_readback", "EXACT_READBACK_VERIFIED", "succeeded"),
        ("forced_restore_rejection", "FORCED_DISPOSABLE_RESTORE_FAILURE_REJECTED", "rejected_expected"),
    ),
}


def _checkpoint_operational_output(
    raw: bytes,
    *,
    plan: Mapping[str, Any],
    row: Mapping[str, Any],
    locator: DriverResult,
) -> OperationalOutput:
    result = CheckpointAcceptanceOperationalResult.model_validate_json(raw)
    if (
        result.scenario != row["requirement_id"]
        or str(result.task_run_id) != row["planned_task_run_id"]
        or result.source_revision != plan["commit_sha"]
        or result.outcome != "LIVE_EVIDENCE_READY"
        or result.live_evidence is not True
        or result.verdict != "LIVE_PASS"
        or result.evidence_class != "live"
        or result.receipt is None
        or result.receipt_sha256 is None
        or result.locator.evidence_notebook_ref != locator.provider_ref
    ):
        raise RuntimeError("checkpoint operational result differs from the exact matrix task")
    expected = _CHECKPOINT_OUTPUT_STAGES[str(row["requirement_id"])]
    actual = tuple((item.stage, item.detail_code, item.outcome) for item in result.stages)
    if actual != expected or any(
        item.candidate_checkpoint_id != result.candidate_checkpoint_id for item in result.stages
    ):
        raise RuntimeError("checkpoint operational result has unexpected stages")
    stage = {item.stage: item.model_dump(mode="json") for item in result.stages}
    head = {
        "initial": result.initial_head.model_dump(mode="json") if result.initial_head else None,
        "final": result.final_head.model_dump(mode="json") if result.final_head else None,
        "receipt_sha256": result.receipt_sha256,
    }
    requirement = str(row["requirement_id"])
    if requirement == "FM05":
        if (
            result.head_unchanged is not False
            or result.initial_head is None
            or result.final_head is None
            or result.candidate_checkpoint_id is None
            or result.final_head.generation != result.initial_head.generation + 1
            or result.final_head.current_checkpoint_id != result.candidate_checkpoint_id
            or result.final_head.previous_checkpoint_id != result.initial_head.current_checkpoint_id
        ):
            raise RuntimeError("FM05 result did not advance exact HEAD")
        proofs = {
            "candidate_uploaded": {
                "empty_candidate": stage["empty_candidate"],
                "private_upload": stage["private_upload"],
            },
            "exact_readback": stage["exact_readback"],
            "restore_verified": stage["independent_restore"],
            "head_advanced": {**head, "cas_promotion": stage["cas_promotion"]},
        }
    elif requirement == "FM14":
        if result.head_unchanged is not True or result.initial_head != result.final_head:
            raise RuntimeError("FM14 result changed checkpoint HEAD")
        proofs = {
            "corrupt_candidate_uploaded": stage["corrupted_candidate"],
            "hash_mismatch_detected": stage["hash_mismatch_rejection"],
            "old_head_unchanged": head,
        }
    else:
        if result.head_unchanged is not True or result.initial_head != result.final_head:
            raise RuntimeError("FM15 result changed checkpoint HEAD")
        proofs = {
            "restore_smoke_forced_failure": stage["forced_restore_rejection"],
            "candidate_rejected": {
                "candidate": stage["restore_failure_candidate"],
                "rejection": stage["forced_restore_rejection"],
            },
            "old_head_unchanged": head,
        }
    if set(proofs) != set(row["required_assertions"]):
        raise RuntimeError("checkpoint proofs differ from the exact assertion catalog")
    return OperationalOutput(
        schema_version="my-data-hub-operational-kaggle-output.v1",
        matrix_id=plan["matrix_id"],
        scenario=row["name"],
        task_run_id=row["planned_task_run_id"],
        outcome="PASS",
        assertions=tuple(
            AssertionResult(
                name=name,
                outcome="PASS",
                evidence_sha256=hashlib.sha256(canonical_json_bytes(proofs[name])).hexdigest(),
            )
            for name in row["required_assertions"]
        ),
        lifecycle_events=(),
        operation_ids=(str(result.operation_id),),
        completed_at=result.completed_at,
    )


def _typed_master_output(
    *,
    plan: Mapping[str, Any],
    row: Mapping[str, Any],
    locator: DriverResult,
) -> OperationalOutput:
    receipt = locator.control_receipt
    if receipt is None or str(receipt.task_id) != row["planned_task_run_id"]:
        raise RuntimeError("master scenario lacks its exact typed control receipt")
    evidence = receipt.evidence
    proofs: dict[str, object]
    lifecycle: list[dict[str, Any]] = []
    if row["requirement_id"] == "FM08":
        if not isinstance(evidence, CallbackLossEvidence):
            raise RuntimeError("FM08 control receipt has the wrong typed evidence")
        proofs = {
            "callback_withheld": {
                "callback_suppressed_once": evidence.callback_suppressed_once,
                "exact_event_id": str(evidence.exact_event_id),
                "exact_body_sha256": evidence.exact_body_sha256,
            },
            "status_reconciled": {
                "replay_disposition": evidence.replay_disposition,
                "service_active_after_recovery": evidence.service_active_after_recovery,
            },
            "output_identity_reconciled": {
                "provider_run_ref": locator.provider_run_ref,
                "source_sha256": locator.source_sha256,
                "output_receipt_sha256": locator.output_receipt_sha256,
            },
        }
    elif row["requirement_id"] == "FM10":
        if not isinstance(evidence, LeaseExpiryEvidence):
            raise RuntimeError("FM10 control receipt has the wrong typed evidence")
        proofs = {
            "lease_expired": {
                "observed_wait_seconds": evidence.observed_wait_seconds,
                "lease_expired": evidence.lease_expired,
            },
            "write_denied": {
                "bounded_operator_dml_denied": evidence.bounded_operator_dml_denied,
                "transaction_state": evidence.transaction_state,
                "denial_code": evidence.denial_code,
                "operator_operation_id": str(evidence.operator_operation_id),
                "operator_receipt_sha256": evidence.operator_receipt_sha256,
                "canonical_revision_before": evidence.canonical_revision_before,
                "canonical_revision_after": evidence.canonical_revision_after,
            },
            "credentials_invalidated": {
                "lease_expired": evidence.lease_expired,
                "denial_code": evidence.denial_code,
                "transaction_state": evidence.transaction_state,
            },
        }
    elif row["requirement_id"] == "FM11":
        if not isinstance(evidence, OldEpochEvidence):
            raise RuntimeError("FM11 control receipt has the wrong typed evidence")
        proofs = {
            "epoch_advanced": {
                "old_epoch": evidence.old_epoch,
                "new_epoch": evidence.new_epoch,
                "old_operation_id": str(evidence.old_operation_id),
                "new_operation_id": str(evidence.new_operation_id),
            },
            "old_renew_denied": {"renew_denied": evidence.renew_denied},
            "old_register_denied": {"register_denied": evidence.register_denied},
            "old_write_denied": {
                "bounded_write_denied": evidence.bounded_write_denied,
                "tunnel_denied": evidence.tunnel_denied,
                "write_denial_receipt_sha256": evidence.write_denial_receipt_sha256,
                "tunnel_denial_receipt_sha256": evidence.tunnel_denial_receipt_sha256,
            },
            "registry_resolves_new": {
                "new_epoch_active": evidence.new_epoch_active,
                "new_epoch": evidence.new_epoch,
                "new_operation_id": str(evidence.new_operation_id),
            },
        }
    elif row["requirement_id"] == "FM24":
        if not isinstance(evidence, RotationSoakEvidence):
            raise RuntimeError("FM24 control receipt has the wrong typed evidence")
        if any(
            value is None
            for value in (
                evidence.heartbeats_continuous,
                evidence.heartbeat_count,
                evidence.heartbeat_receipt_sha256s,
                evidence.reads_succeeded,
                evidence.read_query_count,
                evidence.bounded_read_receipt_sha256s,
                evidence.checkpoint_verified,
                evidence.recovery_succeeded,
                evidence.checkpoint_id,
                evidence.exact_version_ref,
                evidence.manifest_sha256,
                evidence.checkpoint_receipt_sha256,
                evidence.recovery_receipt_sha256,
            )
        ):
            raise RuntimeError(
                "FM24 typed receipt lacks heartbeat/read/checkpoint/recovery observations"
            )
        proofs = {
            "duration_in_range": {
                "observed_duration_seconds": evidence.observed_duration_seconds,
                "monotonic_started_ns": evidence.monotonic_started_ns,
                "monotonic_finished_ns": evidence.monotonic_finished_ns,
            },
            "heartbeats_continuous": {
                "heartbeat_count": evidence.heartbeat_count,
                "receipt_sha256s": evidence.heartbeat_receipt_sha256s,
            },
            "reads_succeeded": {
                "read_query_count": evidence.read_query_count,
                "receipt_sha256s": evidence.bounded_read_receipt_sha256s,
            },
            "checkpoint_verified": {
                "checkpoint_id": str(evidence.checkpoint_id),
                "exact_version_ref": evidence.exact_version_ref,
                "manifest_sha256": evidence.manifest_sha256,
                "receipt_sha256": evidence.checkpoint_receipt_sha256,
            },
            "recovery_succeeded": {
                "recovery_receipt_sha256": evidence.recovery_receipt_sha256,
                "service_active_at_end": evidence.service_active_at_end,
            },
        }
    else:
        raise RuntimeError("control receipt is not admitted for this matrix scenario")
    if set(proofs) != set(row["required_assertions"]):
        raise RuntimeError("typed control receipt differs from fixed scenario assertions")
    for observed in locator.control_lifecycle:
        lifecycle.append(
            {
                "gate": observed.gate,
                "event_id": str(uuid5(NAMESPACE_URL, f"operational:{receipt.task_id}:{observed.gate}")),
                "started_at": receipt.completed_at.isoformat(),
                "completed_at": receipt.completed_at.isoformat(),
                "old_provider_run_ref": observed.old_provider_run_ref,
                "new_provider_run_ref": observed.new_provider_run_ref,
                "old_epoch": observed.old_epoch,
                "new_epoch": observed.new_epoch,
                "operation_id": str(observed.operation_id),
                "before_identity": str(observed.before_identity) if observed.before_identity else None,
                "after_identity": str(observed.after_identity) if observed.after_identity else None,
            }
        )
    if isinstance(evidence, RotationSoakEvidence):
        lifecycle.append(
            {
                "gate": "soak",
                "event_id": str(uuid5(NAMESPACE_URL, f"operational:{receipt.task_id}:soak")),
                "started_at": receipt.completed_at.isoformat(),
                "completed_at": receipt.completed_at.isoformat(),
                "operation_id": str(receipt.binding.operation_id),
                "duration_seconds": evidence.observed_duration_seconds,
                "heartbeat_count": evidence.heartbeat_count,
                "read_query_count": evidence.read_query_count,
                "checkpoint_count": 1,
                "recovery_count": 1,
            }
        )
    return OperationalOutput(
        schema_version="my-data-hub-operational-kaggle-output.v1",
        matrix_id=UUID(str(plan["matrix_id"])),
        scenario=str(row["name"]),
        task_run_id=UUID(str(row["planned_task_run_id"])),
        outcome="PASS",
        assertions=tuple(
            AssertionResult(
                name=name,
                outcome="PASS",
                evidence_sha256=hashlib.sha256(canonical_json_bytes(proofs[name])).hexdigest(),
            )
            for name in row["required_assertions"]
        ),
        lifecycle_events=tuple(LifecycleEvent.model_validate(item) for item in lifecycle),
        operation_ids=(str(receipt.binding.operation_id),),
        completed_at=receipt.completed_at,
    )


def _trusted_control_receipt(
    *,
    plan: Mapping[str, Any],
    row: Mapping[str, Any],
    locator: DriverResult,
    started_at: datetime,
) -> tuple[dict[str, Any], DriverCleanupBinding | None]:
    """Build a receipt only from the pinned driver's control-reconciled metadata.

    Provider output bytes and credentials never enter this process. For generated
    evidence outputs, the canonical document must hash to the control-owned
    OUTPUT_READ receipt. Master acceptance scenarios are derived separately from
    their typed control receipt and exact provider carrier projection.
    """

    if locator.scenario != row["name"] or str(locator.task_run_id) != row["planned_task_run_id"]:
        raise RuntimeError("driver result differs from planned scenario identity")
    if locator.control_receipt is not None:
        if locator.scenario_output is not None:
            raise RuntimeError("driver result mixes typed control receipt and scenario output")
        output = _typed_master_output(plan=plan, row=row, locator=locator)
        if locator.output_file_sha256 is None:
            raise RuntimeError("typed control receipt lacks exact carrier output identity")
        result_sha256 = locator.output_file_sha256
    else:
        if locator.scenario_output is None:
            raise RuntimeError("driver result lacks trusted scenario output metadata")
        output = OperationalOutput.model_validate(locator.scenario_output)
        raw = canonical_json_bytes(output.model_dump(mode="json"))
        result_sha256 = hashlib.sha256(raw).hexdigest()
        if locator.output_file_sha256 is not None and locator.output_file_sha256 != result_sha256:
            raise RuntimeError("trusted scenario output differs from the control-owned output receipt")
    expected_assertions = set(row["required_assertions"])
    actual_assertions = {item.name for item in output.assertions}
    if (
        str(output.matrix_id) != plan["matrix_id"]
        or output.scenario != row["name"]
        or str(output.task_run_id) != row["planned_task_run_id"]
        or output.outcome != "PASS"
        or actual_assertions != expected_assertions
        or any(item.outcome != "PASS" for item in output.assertions)
        or {item.gate for item in output.lifecycle_events} != set(row["lifecycle_gates"])
    ):
        raise RuntimeError("trusted scenario evidence does not satisfy the exact contract")
    assert (
        locator.provider_ref
        and locator.provider_run_ref
        and locator.provider_kernel_id
        and locator.source_version
        and locator.source_sha256
    )
    receipt = {
        "schema_version": SCENARIO_SCHEMA,
        "matrix_id": plan["matrix_id"],
        "commit_sha": plan["commit_sha"],
        "ordinal": row["ordinal"],
        "requirement_id": row["requirement_id"],
        "scenario": row["name"],
        "outcome": "PASS",
        "live_evidence": True,
        "planned_task_run_id": row["planned_task_run_id"],
        "real_run_identity": {
            "provider_ref": locator.provider_ref,
            "provider_run_ref": locator.provider_run_ref,
            "provider_kernel_id": locator.provider_kernel_id,
            "source_version": locator.source_version,
            "source_sha256": locator.source_sha256,
            "provider_claim_sha256": locator.claim_sha256,
            "provider_status": "control_reconciled",
            "output_tree_sha256": locator.output_tree_sha256,
            "result_sha256": result_sha256,
        },
        "assertions": [item.model_dump(mode="json") for item in output.assertions],
        "lifecycle_events": [item.model_dump(mode="json") for item in output.lifecycle_events],
        "operation_ids": list(output.operation_ids),
        "driver_mutations_started": locator.mutations_started,
        "driver_capability_checks": [item.model_dump(mode="json") for item in locator.capability_checks],
        "driver_observation_sha256": locator.observation_sha256,
        "blocker": None,
        "started_at": started_at.isoformat(),
        "completed_at": datetime.now(UTC).isoformat(),
    }
    if locator.outcome != "READY":
        return receipt, None
    assert (
        locator.claim_task_id
        and locator.claim_sha256
        and locator.output_receipt_sha256
        and locator.output_file_sha256
        and locator.output_tree_sha256
    )
    return receipt, DriverCleanupBinding(
        claim_task_id=locator.claim_task_id,
        claim_sha256=locator.claim_sha256,
        provider_ref=locator.provider_ref,
        provider_run_ref=locator.provider_run_ref,
        provider_kernel_id=locator.provider_kernel_id,
        source_version=locator.source_version,
        source_sha256=locator.source_sha256,
        output_receipt_sha256=locator.output_receipt_sha256,
        output_file_sha256=locator.output_file_sha256,
        output_tree_sha256=locator.output_tree_sha256,
    )


def _aggregate_lifecycle(receipts: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    events = [event for receipt in receipts for event in receipt.get("lifecycle_events", [])]
    by_gate = {
        gate: [event for event in events if event.get("gate") == gate]
        for gate in (
            "master_boot",
            "clean_rotation",
            "abrupt_master_termination",
            "control_plane_restart",
            "host_reboot",
            "soak",
        )
    }
    boot_refs = {
        event.get("new_provider_run_ref") for event in by_gate["master_boot"] if event.get("new_provider_run_ref")
    }
    soak = by_gate["soak"]
    return {
        "master_boots": len(boot_refs),
        "clean_rotations": len(by_gate["clean_rotation"]),
        "abrupt_master_terminations": len(by_gate["abrupt_master_termination"]),
        "control_plane_restarts": len(by_gate["control_plane_restart"]),
        "host_reboots": len(by_gate["host_reboot"]),
        "soak_runs": len(soak),
        "soak_duration_seconds": int(soak[0]["duration_seconds"]) if len(soak) == 1 else 0,
    }


def _summary(plan: Mapping[str, Any], receipts: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    passed = [receipt for receipt in receipts if receipt["outcome"] == "PASS"]
    failed = [receipt for receipt in receipts if receipt["outcome"] == "FAIL"]
    blocked = [receipt for receipt in receipts if receipt["outcome"] == "BLOCKED"]
    run_refs = {
        str(receipt["real_run_identity"]["provider_run_ref"])
        for receipt in passed
        if isinstance(receipt.get("real_run_identity"), Mapping)
    }
    kernel_ids = {
        int(receipt["real_run_identity"]["provider_kernel_id"])
        for receipt in passed
        if isinstance(receipt.get("real_run_identity"), Mapping)
    }
    lifecycle = _aggregate_lifecycle(passed)
    lifecycle_complete = (
        lifecycle["master_boots"] >= 3
        and lifecycle["clean_rotations"] >= 2
        and lifecycle["abrupt_master_terminations"] >= 1
        and lifecycle["control_plane_restarts"] >= 1
        and lifecycle["host_reboots"] >= 1
        and lifecycle["soak_runs"] == 1
        and MINIMUM_SOAK_SECONDS <= lifecycle["soak_duration_seconds"] <= MAXIMUM_SOAK_SECONDS
    )
    complete = (
        len(passed) == 24
        and not failed
        and not blocked
        and len(run_refs) >= MINIMUM_DISTINCT_PROVIDER_RUNS
        and len(kernel_ids) >= MINIMUM_DISTINCT_PROVIDER_RUNS
        and lifecycle_complete
    )
    outcome = "PASS" if complete else ("FAIL" if failed else "BLOCKED")
    return {
        "schema_version": SUMMARY_SCHEMA,
        "matrix_id": plan["matrix_id"],
        "commit_sha": plan["commit_sha"],
        "matrix_scope": "operational_24_scenario",
        "outcome": outcome,
        "live_evidence": complete,
        "minimum_distinct_provider_runs": MINIMUM_DISTINCT_PROVIDER_RUNS,
        "planned_scenarios": 24,
        "passed_scenarios": len(passed),
        "failed_scenarios": len(failed),
        "blocked_scenarios": len(blocked),
        "distinct_provider_run_refs": sorted(run_refs),
        "distinct_provider_kernel_ids": sorted(kernel_ids),
        "lifecycle_gates": lifecycle,
        "scenario_receipts": [
            {
                "ordinal": receipt["ordinal"],
                "requirement_id": receipt["requirement_id"],
                "scenario": receipt["scenario"],
                "outcome": receipt["outcome"],
                "receipt": f"{int(receipt['ordinal']):02d}-{receipt['scenario']}.json",
            }
            for receipt in receipts
        ],
        "blockers": [receipt["blocker"] for receipt in blocked],
        "completed_at": datetime.now(UTC).isoformat(),
    }


def run_operational_matrix(
    *,
    ledger_path: Path,
    plan_path: Path,
    receipt_path: Path,
    scenario_directory: Path,
    matrix_id: UUID | None = None,
    commit_sha: str | None = None,
    root: Path | None = None,
    driver_timeout_seconds: int = 7200,
) -> int:
    """Run or resume the exact operational matrix.

    The checked-in driver is the only executable boundary. Provider credentials,
    mutations, status, output reads, and cleanup remain inside the deployed
    control-owned gateway; this process never constructs a Kaggle adapter.
    """

    repository_root = root or Path(__file__).resolve().parents[2]
    exact_commit = commit_sha or _exact_commit(repository_root)
    plan = _load_or_create_plan(plan_path, matrix_id=matrix_id, commit_sha=exact_commit)
    live_execution = commit_sha is None and root is None
    if not live_execution:
        blocked_receipts = _write_all_blocked(
            plan=plan,
            directory=scenario_directory,
            code="TEST_INJECTION_CANNOT_CREATE_LIVE_EVIDENCE",
            dependency="uninjected CLI execution with the checked-in driver and control-owned provider gateway",
        )
        _atomic_write(receipt_path, _summary(plan, blocked_receipts))
        return EXTERNAL_BLOCKED
    del ledger_path  # retained CLI compatibility; no local provider journal is opened
    receipts: list[dict[str, Any]] = []
    for row in plan["scenarios"]:
        scenario_path = _scenario_path(scenario_directory, row)
        owner_pause_path = scenario_path.with_suffix(".owner-pause.json")
        if scenario_path.is_file():
            existing = json.loads(scenario_path.read_bytes())
            if (
                existing.get("schema_version") != SCENARIO_SCHEMA
                or existing.get("matrix_id") != plan["matrix_id"]
                or existing.get("commit_sha") != plan["commit_sha"]
                or existing.get("planned_task_run_id") != row["planned_task_run_id"]
            ):
                raise RuntimeError(f"stale operational scenario receipt: {scenario_path}")
            blocker = existing.get("blocker")
            resumable_owner_pause = (
                row["requirement_id"] == "FM16"
                and existing.get("outcome") == "BLOCKED"
                and isinstance(blocker, Mapping)
                and blocker.get("code") == RESUMABLE_OWNER_BLOCKER
                and existing.get("driver_mutations_started") == 0
            )
            if not resumable_owner_pause:
                receipts.append(existing)
                continue
        if owner_pause_path.exists():
            owner_pause = json.loads(owner_pause_path.read_bytes())
            blocker = owner_pause.get("blocker")
            if (
                owner_pause.get("schema_version") != SCENARIO_SCHEMA
                or owner_pause.get("matrix_id") != plan["matrix_id"]
                or owner_pause.get("commit_sha") != plan["commit_sha"]
                or owner_pause.get("planned_task_run_id") != row["planned_task_run_id"]
                or row["requirement_id"] != "FM16"
                or owner_pause.get("outcome") != "BLOCKED"
                or not isinstance(blocker, Mapping)
                or blocker.get("code") != RESUMABLE_OWNER_BLOCKER
                or owner_pause.get("driver_mutations_started") != 0
            ):
                raise RuntimeError("durable FM16 owner-pause fence differs from the plan")
        launch_path = scenario_path.with_suffix(".launch.json")
        reconciliation_path = scenario_path.with_suffix(".reconciled.json")
        reconciliation_loaded = reconciliation_path.exists()
        resume_only = launch_path.exists()
        request = _driver_request(plan, row, resume_only=resume_only)
        if not resume_only:
            _atomic_write(launch_path, request)
        elif json.loads(launch_path.read_bytes()) != request | {"resume_only": False}:
            raise RuntimeError("durable operational launch fence differs from planned identity")
        if reconciliation_path.exists():
            reconciliation = json.loads(reconciliation_path.read_bytes())
            reconciliation_payload = {
                key: value
                for key, value in reconciliation.items()
                if key != "reconciliation_sha256"
            }
            if (
                set(reconciliation)
                != {
                    "schema_version",
                    "matrix_id",
                    "commit_sha",
                    "planned_task_run_id",
                    "started_at",
                    "execute_result",
                    "reconcile_result",
                    "cleanup_request",
                    "validated_receipt",
                    "reconciliation_sha256",
                }
                or reconciliation.get("reconciliation_sha256")
                != hashlib.sha256(canonical_json_bytes(reconciliation_payload)).hexdigest()
                or
                reconciliation.get("schema_version")
                != "my-data-hub-operational-kaggle-reconciliation.v1"
                or reconciliation.get("matrix_id") != plan["matrix_id"]
                or reconciliation.get("commit_sha") != plan["commit_sha"]
                or reconciliation.get("planned_task_run_id") != row["planned_task_run_id"]
            ):
                raise RuntimeError("durable operational reconciliation fence differs from the plan")
            locator = DriverResult.model_validate(reconciliation.get("execute_result"))
            reconciled = DriverResult.model_validate(reconciliation.get("reconcile_result"))
            cleanup_request = dict(reconciliation.get("cleanup_request", {}))
            receipt = dict(reconciliation.get("validated_receipt", {}))
            if (
                receipt.get("schema_version") != SCENARIO_SCHEMA
                or receipt.get("matrix_id") != plan["matrix_id"]
                or receipt.get("commit_sha") != plan["commit_sha"]
                or receipt.get("planned_task_run_id") != row["planned_task_run_id"]
                or receipt.get("outcome") != "PASS"
                or receipt.get("live_evidence") is not True
            ):
                raise RuntimeError("durable outer-reconciled receipt differs from the exact plan")
            started = datetime.fromisoformat(str(reconciliation["started_at"]))
        else:
            started = datetime.now(UTC)
            locator = _invoke_driver(request, timeout_seconds=driver_timeout_seconds)
            reconciled = None
            cleanup_request = {}
        if locator.scenario != row["name"] or str(locator.task_run_id) != row["planned_task_run_id"]:
            raise RuntimeError("driver returned a different scenario identity")
        cleanup_binding: DriverCleanupBinding | None = None
        if locator.outcome in {"PASS", "READY"}:
            execute_request = {**dict(request), "resume_only": False}
            if reconciliation_loaded:
                binding = _driver_binding(locator)
                assert reconciled is not None
                if (
                    reconciled.phase != "RECONCILE"
                    or reconciled.outcome != "PASS"
                    or _driver_binding(reconciled) != binding
                    or reconciled.control_receipt != locator.control_receipt
                    or reconciled.control_lifecycle != locator.control_lifecycle
                ):
                    raise RuntimeError("durable control reconciliation differs from execution evidence")
                cleanup_binding = binding if locator.outcome == "READY" else None
            else:
                reconciled, binding = _reconcile_driver_result(
                    execute_request,
                    locator,
                    timeout_seconds=driver_timeout_seconds,
                )
                receipt, trusted_cleanup = _trusted_control_receipt(
                    plan=plan,
                    row=row,
                    locator=locator,
                    started_at=started,
                )
                cleanup_binding = trusted_cleanup
                if locator.outcome == "READY":
                    if cleanup_binding is None or cleanup_binding != binding:
                        raise RuntimeError("driver READY cleanup differs from reconciled provider claim")
                    cleanup_request = _cleanup_request(execute_request, cleanup_binding)
                elif cleanup_binding is not None:
                    raise RuntimeError("driver PASS unexpectedly requires pending cleanup")
                reconciliation_payload = {
                    "schema_version": "my-data-hub-operational-kaggle-reconciliation.v1",
                    "matrix_id": plan["matrix_id"],
                    "commit_sha": plan["commit_sha"],
                    "planned_task_run_id": row["planned_task_run_id"],
                    "started_at": started.isoformat(),
                    "execute_result": locator.model_dump(mode="json"),
                    "reconcile_result": reconciled.model_dump(mode="json"),
                    "cleanup_request": cleanup_request,
                    "validated_receipt": receipt,
                }
                _atomic_write(
                    reconciliation_path,
                    {
                        **reconciliation_payload,
                        "reconciliation_sha256": hashlib.sha256(
                            canonical_json_bytes(reconciliation_payload)
                        ).hexdigest(),
                    },
                )
        if locator.outcome == "BLOCKED":
            receipt = _blocker_receipt(
                plan=plan,
                scenario=row,
                code=str(locator.blocker_code),
                dependency=str(locator.integration_dependency),
            )
            receipt["driver_mutations_started"] = locator.mutations_started
            receipt["driver_capability_checks"] = [item.model_dump(mode="json") for item in locator.capability_checks]
            receipt["driver_observation_sha256"] = locator.observation_sha256
        elif locator.outcome == "FAIL":
            receipt = _blocker_receipt(
                plan=plan,
                scenario=row,
                code="OPERATIONAL_ASSERTION_FAILED",
                dependency="inspect exact operational Notebook output and driver fault-injection logs",
            )
            receipt["outcome"] = "FAIL"
            receipt["blocker"] = None
            receipt["driver_mutations_started"] = locator.mutations_started
            receipt["driver_capability_checks"] = [item.model_dump(mode="json") for item in locator.capability_checks]
            receipt["driver_observation_sha256"] = locator.observation_sha256
        elif locator.outcome == "PASS":
            if cleanup_binding is not None:
                raise RuntimeError("driver PASS unexpectedly requires pending cleanup")
        else:
            if locator.outcome != "READY":
                raise RuntimeError("driver returned an unsupported execution outcome")
            if cleanup_binding is None:
                raise RuntimeError("driver READY lacks a cleanup binding")
            if reconciliation_loaded:
                expected_cleanup = _cleanup_request(
                    {**dict(request), "resume_only": False}, cleanup_binding
                )
                if cleanup_request != expected_cleanup:
                    raise RuntimeError("durable cleanup request differs from outer reconciliation")
            cleanup_result = _invoke_driver(cleanup_request, timeout_seconds=driver_timeout_seconds)
            if (
                cleanup_result.phase != "CLEANUP"
                or cleanup_result.outcome != "PASS"
                or cleanup_result.cleanup_state != "COMPLETE"
                or cleanup_result.provider_ref != cleanup_binding.provider_ref
                or cleanup_result.provider_run_ref != cleanup_binding.provider_run_ref
                or cleanup_result.claim_task_id != cleanup_binding.claim_task_id
                or cleanup_result.claim_sha256 != cleanup_binding.claim_sha256
                or cleanup_result.output_receipt_sha256 != cleanup_binding.output_receipt_sha256
                or cleanup_result.output_file_sha256 != cleanup_binding.output_file_sha256
                or cleanup_result.output_tree_sha256 != cleanup_binding.output_tree_sha256
            ):
                receipt["outcome"] = "FAIL"
                receipt["live_evidence"] = False
                receipt["blocker"] = None
            receipt["driver_mutations_started"] = cleanup_result.mutations_started
            receipt["driver_capability_checks"] = [
                *[item.model_dump(mode="json") for item in locator.capability_checks],
                *[item.model_dump(mode="json") for item in cleanup_result.capability_checks],
            ]
            receipt["driver_observation_sha256"] = hashlib.sha256(
                canonical_json_bytes(
                    {
                        "execute": locator.observation_sha256,
                        "cleanup": cleanup_result.observation_sha256,
                    }
                )
            ).hexdigest()
        blocker = receipt.get("blocker")
        if (
            row["requirement_id"] == "FM16"
            and receipt.get("outcome") == "BLOCKED"
            and isinstance(blocker, Mapping)
            and blocker.get("code") == RESUMABLE_OWNER_BLOCKER
        ):
            # Do not launch dependent FM17--FM21 rows until the exact same
            # FM16 task/state resumes with an owner-provided mode-0600 envelope.
            # The pause fence is append-only; it is never overwritten by the
            # later final scenario receipt.
            if not owner_pause_path.exists():
                _atomic_write(owner_pause_path, receipt)
            receipts.append(receipt)
            _atomic_write(receipt_path, _summary(plan, receipts))
            return EXTERNAL_BLOCKED
        _atomic_write(scenario_path, receipt)
        receipts.append(receipt)
    summary = _summary(plan, receipts)
    _atomic_write(receipt_path, summary)
    return PASS if summary["outcome"] == "PASS" else (FAIL if summary["outcome"] == "FAIL" else EXTERNAL_BLOCKED)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("preflight", "run"))
    parser.add_argument("--ledger", type=Path, default=Path("artifacts/operational-provider-effects.sqlite3"))
    parser.add_argument("--plan", type=Path, default=Path("artifacts/operational-kaggle-plan.json"))
    parser.add_argument("--receipt", type=Path, default=Path("artifacts/operational-kaggle-matrix.json"))
    parser.add_argument("--scenario-receipts", type=Path, default=Path("artifacts/operational-kaggle-scenarios"))
    parser.add_argument("--matrix-id", type=UUID)
    parser.add_argument("--driver-timeout-seconds", type=int, default=7200)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.command == "preflight":
        try:
            _trusted_driver_command()
            driver_ready = True
        except (OSError, RuntimeError):
            driver_ready = False
        payload = {
            "local_kaggle_credentials_used": False,
            "checked_in_operational_driver_ready": driver_ready,
            "scenario_count": len(SCENARIOS),
            "minimum_distinct_provider_runs": MINIMUM_DISTINCT_PROVIDER_RUNS,
            "soak_seconds": [MINIMUM_SOAK_SECONDS, MAXIMUM_SOAK_SECONDS],
        }
        print(json.dumps(payload, sort_keys=True))
        return (
            PASS if payload["checked_in_operational_driver_ready"] else EXTERNAL_BLOCKED
        )
    if not 60 <= args.driver_timeout_seconds <= 7200:
        print("driver timeout must be between 60 and 7200 seconds", file=sys.stderr)
        return FAIL
    return run_operational_matrix(
        ledger_path=args.ledger,
        plan_path=args.plan,
        receipt_path=args.receipt,
        scenario_directory=args.scenario_receipts,
        matrix_id=args.matrix_id,
        driver_timeout_seconds=args.driver_timeout_seconds,
    )


if __name__ == "__main__":
    raise SystemExit(main())
