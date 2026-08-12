#!/usr/bin/env python3
"""Trusted, fail-closed production driver for the operational Kaggle matrix.

This executable binds each FM01--FM24 scenario to the production MCP/control
surface that exists today.  It performs non-mutating capability/status probes
unless an owner-issued provider claim first reconciles an already-running exact
evidence Notebook for the matrix-planned task.  Only that claim-gated path may
issue an idempotent durable restore/rotation request and poll its receipt. Every
unresolved scenario has a named internal API gap; there is no generic fallback
and no synthetic PASS.

The matrix runner invokes this file with ``--request`` and ``--result``. Exact
acceptance-evidence scenarios first return READY. The outer runner independently
reconciles/downloads the run and then invokes CLEANUP; only a durable COMPLETE
claim can return cleanup PASS.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import hashlib
import json
import os
import re
import stat
import subprocess
import sys
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Literal, Protocol
from urllib.parse import urlsplit
from uuid import NAMESPACE_URL, UUID, uuid5

from pydantic import BaseModel, ConfigDict, Field, model_validator

from my_data_hub.acceptance.data_production import (
    AtomicJsonStateStore,
    ProductionDataWorkloadConfig,
    ProductionDataWorkloadReceipt,
)
from my_data_hub.acceptance.data_workloads import (
    BGE_EXACT_ID,
    E5_EXACT_ID,
    DataPhase,
    DataWorkloadPlan,
    DataWorkloadState,
)
from my_data_hub.acceptance.master_lifecycle import (
    CallbackLossEvidence,
    CleanDrainEvidence,
    ConcurrentEnsureEvidence,
    EmptyBootstrapEvidence,
    LeaseExpiryEvidence,
    MasterAcceptanceReceipt,
    MasterProviderCarrierObservation,
    OldEpochEvidence,
    RotationSoakEvidence,
    StaleReplayEvidence,
)
from my_data_hub.acceptance.scenario_operator import CheckpointAcceptanceLaunchStatus
from my_data_hub.auth.oauth_credentials import (
    BearerSource,
    OAuthCredentialError,
    StaticBearerSource,
    bearer_source_from_environment,
    validate_oauth_credential_file,
)
from my_data_hub.hashing import canonical_json_bytes

if not __package__:
    # Direct execution starts with scripts/provider on sys.path. Add the exact
    # repository root so the shared signed-evidence verifier is still imported
    # from this trusted checkout rather than reimplemented here.
    sys.path.insert(1, str(Path(__file__).resolve().parents[2]))

from scripts.verify_post_deploy import validate_deployment_evidence_v2

if __package__:
    from scripts.provider.operational_kaggle_matrix import EXTERNAL_BLOCKED, SCENARIOS
else:  # Direct repository script execution places this directory on sys.path.
    from operational_kaggle_matrix import EXTERNAL_BLOCKED, SCENARIOS

FAIL = 1
MAX_REQUEST_BYTES = 256 * 1024
MAX_RESULT_BYTES = 256 * 1024
DEFAULT_ENDPOINT = "https://mcp-datahub.kenigevents.ru/mcp"
MAX_EVIDENCE_CLAIMS_BYTES = 64 * 1024
ACTION_POLL_SECONDS = 5.0
ACTION_TERMINAL_STATES = frozenset({"DURABLE_COMPLETE", "FAILED", "FENCED", "ORPHANED"})
CHECKPOINT_ACCEPTANCE_TERMINAL_STATES = frozenset({"LIVE_EVIDENCE_READY", "BLOCKED", "FAIL"})
CHECKPOINT_ACCEPTANCE_POLL_SECONDS = 5.0
CHECKPOINT_ACCEPTANCE_TIMEOUT_SECONDS = 900
MASTER_ACCEPTANCE_TERMINAL_STATES = frozenset({"PASSED", "FAILED"})
MASTER_ACCEPTANCE_POLL_SECONDS = 5.0
MASTER_ACCEPTANCE_TIMEOUT_SECONDS = 1800
MASTER_ACCEPTANCE_SOAK_TIMEOUT_SECONDS = 5700
MASTER_ACCEPTANCE_REQUIREMENTS = frozenset(
    {"FM04", "FM07", "FM08", "FM09", "FM10", "FM11", "FM12", "FM24"}
)
PREBOOT_MASTER_ACCEPTANCE_REQUIREMENTS = frozenset({"FM04", "FM07"})
TERMINAL_OUTPUT_MASTER_ACCEPTANCE_REQUIREMENTS = frozenset({"FM11", "FM12"})
FM20_MASTER_TIMEOUT_SECONDS = 1800
FM20_MASTER_POLL_SECONDS = 5.0
EXPECTED_SOURCE_IDENTITY = "onedayonemasterpiece/my-data-hub"
SERVICE_NAMES = frozenset({"control-plane", "oauth-server", "remote-mcp"})
CHECKPOINT_SCENARIO_NAMES = frozenset(
    {
        "verified-empty-checkpoint-roundtrip",
        "corrupt-candidate-head-preserved",
        "restore-smoke-failure-head-preserved",
    }
)


class CleanupBinding(BaseModel):
    """Exact outer-reconciled evidence required for destructive cleanup."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    claim_task_id: UUID | None = None
    claim_sha256: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")
    provider_ref: str = Field(pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
    provider_run_ref: str = Field(pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/[1-9][0-9]*$")
    provider_kernel_id: int = Field(ge=1)
    source_version: int = Field(ge=1)
    source_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    output_receipt_sha256: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")
    output_file_sha256: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")
    output_tree_sha256: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")

    @model_validator(mode="after")
    def exact_cleanup_claim(self) -> CleanupBinding:
        if (self.claim_task_id is None) != (self.claim_sha256 is None):
            raise ValueError("cleanup claim identity must be wholly present or absent")
        outputs = (self.output_receipt_sha256, self.output_file_sha256, self.output_tree_sha256)
        if any(value is not None for value in outputs) != all(value is not None for value in outputs):
            raise ValueError("reconciliation output identity must be wholly present or absent")
        return self


class DriverRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    schema_version: Literal["my-data-hub-operational-kaggle-driver-request.v2"]
    phase: Literal["EXECUTE", "RECONCILE", "CLEANUP"]
    matrix_id: UUID
    commit_sha: str = Field(pattern=r"^[a-f0-9]{40}$")
    ordinal: int = Field(ge=1, le=24)
    requirement_id: str = Field(pattern=r"^FM(0[1-9]|1[0-9]|2[0-4])$")
    scenario: str = Field(pattern=r"^[a-z0-9-]+$", max_length=100)
    task_run_id: UUID
    required_assertions: tuple[str, ...] = Field(min_length=2)
    lifecycle_gates: tuple[str, ...]
    minimum_soak_seconds: int | None = Field(default=None, ge=3600, le=3600)
    maximum_soak_seconds: int | None = Field(default=None, ge=5400, le=5400)
    result_file: Literal["operational-result.json"]
    resume_only: bool
    evidence_issued_at: datetime
    cleanup: CleanupBinding | None = None

    @model_validator(mode="after")
    def exact_catalog_entry(self) -> DriverRequest:
        spec = SCENARIOS[self.ordinal - 1]
        if (
            self.requirement_id != spec.requirement_id
            or self.scenario != spec.name
            or self.required_assertions != spec.assertions
            or self.lifecycle_gates != spec.lifecycle_gates
        ):
            raise ValueError("driver request differs from the exact FM01-FM24 catalog")
        if ("soak" in self.lifecycle_gates) != bool(
            self.minimum_soak_seconds == 3600 and self.maximum_soak_seconds == 5400
        ):
            raise ValueError("driver request has inconsistent soak bounds")
        if (self.phase == "EXECUTE") == (self.cleanup is not None):
            raise ValueError("driver reconcile/cleanup phase requires one exact binding")
        if self.phase == "CLEANUP" and self.cleanup is not None and self.cleanup.claim_task_id is None:
            raise ValueError("driver cleanup phase requires an exact disposable claim")
        if self.phase == "CLEANUP" and self.cleanup is not None and any(
            value is None
            for value in (
                self.cleanup.output_receipt_sha256,
                self.cleanup.output_file_sha256,
                self.cleanup.output_tree_sha256,
            )
        ):
            raise ValueError("driver cleanup phase requires an exact output receipt")
        return self


class CapabilityCheck(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    name: str = Field(pattern=r"^[a-z0-9_.-]+$", max_length=120)
    outcome: Literal["PASS", "BLOCKED", "FAIL"]
    evidence_sha256: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")
    detail_code: str = Field(pattern=r"^[A-Z0-9_]+$", max_length=120)


class DriverAssertionEvidence(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    name: str = Field(pattern=r"^[a-z0-9_]+$", max_length=100)
    outcome: Literal["PASS"]
    evidence_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")


class DriverScenarioOutput(BaseModel):
    """Trusted, source-generated output metadata; provider bytes stay control-side."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["my-data-hub-operational-kaggle-output.v1"]
    matrix_id: UUID
    scenario: str
    task_run_id: UUID
    outcome: Literal["PASS"]
    assertions: tuple[DriverAssertionEvidence, ...]
    lifecycle_events: tuple[dict[str, Any], ...] = ()
    operation_ids: tuple[str, ...] = ()
    completed_at: datetime

    @model_validator(mode="after")
    def exact_assertions(self) -> DriverScenarioOutput:
        if self.completed_at.tzinfo is None or len({item.name for item in self.assertions}) != len(self.assertions):
            raise ValueError("driver scenario output is not exact and timezone-bound")
        return self


class MasterCommandProjection(BaseModel):
    """Terminal command projection returned by acceptance.scenario.status."""

    model_config = ConfigDict(extra="allow", frozen=True)

    command_id: UUID
    command_kind: str
    command_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    state: Literal["SUCCEEDED"]
    receipt_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    receipt: MasterAcceptanceReceipt

    @model_validator(mode="after")
    def exact_receipt(self) -> MasterCommandProjection:
        if (
            self.command_id != self.receipt.command_id
            or self.command_kind != self.receipt.command_kind.value
            or self.command_sha256 != self.receipt.command_sha256
            or self.receipt_sha256 != self.receipt.receipt_sha256
        ):
            raise ValueError("master command projection differs from its typed receipt")
        return self


class DriverControlLifecycle(BaseModel):
    """Exact lifecycle identities independently projected by control status."""

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
            raise ValueError("master lifecycle requires distinct provider runs")
        if self.gate == "clean_rotation" and not (
            self.old_epoch and self.new_epoch == self.old_epoch + 1
        ):
            raise ValueError("clean rotation requires consecutive epochs")
        if self.gate == "control_plane_restart" and not (
            self.before_identity
            and self.after_identity
            and self.before_identity != self.after_identity
        ):
            raise ValueError("control restart requires distinct boot identities")
        return self


class TrustedDriverResult(BaseModel):
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
    blocker_code: str | None = Field(default=None, pattern=r"^[A-Z0-9_]+$", max_length=120)
    integration_dependency: str | None = Field(default=None, min_length=1, max_length=500)
    mutations_started: int = Field(ge=0)
    capability_checks: tuple[CapabilityCheck, ...]
    observation_sha256: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")
    claim_task_id: UUID | None = None
    claim_sha256: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")
    output_receipt_sha256: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")
    output_file_sha256: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")
    output_tree_sha256: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")
    cleanup_state: Literal["NOT_REQUIRED", "PENDING", "COMPLETE"] = "NOT_REQUIRED"
    scenario_output: DriverScenarioOutput | None = None
    control_receipt: MasterAcceptanceReceipt | None = None
    control_lifecycle: tuple[DriverControlLifecycle, ...] = ()

    @model_validator(mode="after")
    def exact_outcome_shape(self) -> TrustedDriverResult:
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
            raise ValueError("successful driver result lacks an exact provider run locator")
        if self.phase == "EXECUTE" and self.outcome in {"READY", "PASS"} and (
            (self.scenario_output is None) == (self.control_receipt is None)
        ):
            raise ValueError("successful execution requires exactly one trusted evidence payload")
        if self.outcome == "READY" and (
            self.phase != "EXECUTE" or self.cleanup_state != "PENDING" or any(value is None for value in cleanup)
        ):
            raise ValueError("driver READY lacks an exact pending cleanup binding")
        if self.phase == "CLEANUP" and self.outcome == "PASS" and self.cleanup_state != "COMPLETE":
            raise ValueError("cleanup PASS requires a COMPLETE durable claim")
        if self.phase == "RECONCILE" and self.outcome == "PASS":
            if self.cleanup_state == "PENDING" and any(value is None for value in cleanup):
                raise ValueError("reconciliation PASS lacks its exact pending cleanup binding")
            if self.cleanup_state == "NOT_REQUIRED" and any(value is not None for value in cleanup[:2]):
                raise ValueError("protected carrier reconciliation cannot contain a cleanup claim")
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
            raise ValueError("driver BLOCKED lacks a named integration dependency")
        if self.outcome == "BLOCKED" and self.mutations_started != 0:
            raise ValueError("driver BLOCKED cannot follow a possible mutation")
        return self


class RuntimeEventIdentity(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    run_id: UUID
    attempt_id: UUID
    epoch: int = Field(ge=1)


class DataWorkloadDriverConfig(BaseModel):
    """Owner-fixed files for the single matrix-wide FM16--FM21 workflow."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    plan_path: str = Field(min_length=1, max_length=4096)
    production_config_path: str = Field(min_length=1, max_length=4096)
    state_path: str = Field(min_length=1, max_length=4096)
    owner_envelope_path: str | None = Field(default=None, min_length=1, max_length=4096)


class EvidenceDriverConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    schema_version: Literal["my-data-hub-operational-kaggle-evidence-driver.v1"]
    provider_owner: str = Field(pattern=r"^[A-Za-z0-9_.-]+$", max_length=80)
    fm03_runtime: RuntimeEventIdentity | None = None
    data_workload: DataWorkloadDriverConfig | None = None


class EvidenceClaim(BaseModel):
    """Owner-issued claim for an already-running disposable evidence Notebook.

    The claim is not accepted as a run locator.  The driver sends it to the
    production provider control gateway and accepts only the exact identity
    returned by that read path.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)
    requirement_id: str = Field(pattern=r"^FM(06|13|20)$")
    task_id: UUID
    resource_ref: str = Field(pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$", max_length=300)
    claim_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    operation_id: str | None = Field(
        default=None,
        pattern=r"^(?:[a-f0-9]{64}|[a-f0-9]{8}-[a-f0-9]{4}-[1-5][a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12})$",
    )


class EvidenceClaimsDocument(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    schema_version: Literal["my-data-hub-operational-kaggle-evidence-claims.v1"]
    claims: dict[Literal["FM06", "FM13", "FM20"], EvidenceClaim] = Field(max_length=3)
    fm20_evidence: dict[str, Any] | None = None

    @model_validator(mode="after")
    def keyed_requirements_match(self) -> EvidenceClaimsDocument:
        if any(key != claim.requirement_id for key, claim in self.claims.items()):
            raise ValueError("evidence claim key differs from requirement_id")
        return self


class FM20EvidenceBundle(BaseModel):
    """Owner-supplied signed host receipt and exact cold-search policy."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    schema_version: Literal["my-data-hub-operational-kaggle-fm20-evidence.v1"]
    deployment_evidence: dict[str, Any]
    public_key_pem: str = Field(min_length=1, max_length=8192)
    expected_key_id: str = Field(pattern=r"^[A-Za-z0-9._:-]{1,128}$")
    expected_source_identity: Literal["onedayonemasterpiece/my-data-hub"]
    expected_source_tree_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    expected_service_image_ids: dict[
        Literal["control-plane", "oauth-server", "remote-mcp"], str
    ] = Field(min_length=3, max_length=3)
    blogger_query: str = Field(min_length=1, max_length=500)

    @model_validator(mode="after")
    def exact_policy(self) -> FM20EvidenceBundle:
        if set(self.expected_service_image_ids) != SERVICE_NAMES or any(
            re.fullmatch(r"sha256:[a-f0-9]{64}", value) is None
            for value in self.expected_service_image_ids.values()
        ):
            raise ValueError("FM20 expected image identities differ from policy")
        if self.blogger_query != self.blogger_query.strip():
            raise ValueError("FM20 blogger query must not contain surrounding whitespace")
        return self


class EvidenceRunLocator(BaseModel):
    """Exact locator observed from ``provider.resources.read``."""

    model_config = ConfigDict(extra="ignore", frozen=True)
    claim_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    task_id: UUID
    task_run_id: UUID
    provider_ref: str = Field(pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
    provider_run_ref: str = Field(pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/[1-9][0-9]*$")
    provider_kernel_id: int = Field(ge=1)
    source_version: int = Field(ge=1)
    source_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    fingerprint: dict[Literal["algorithm", "value"], str]
    private: Literal[True]

    @model_validator(mode="after")
    def exact_fingerprint(self) -> EvidenceRunLocator:
        if set(self.fingerprint) != {"algorithm", "value"}:
            raise ValueError("provider fingerprint has an unexpected shape")
        if self.fingerprint["algorithm"] != "sha256" or re.fullmatch(
            r"[a-f0-9]{64}", self.fingerprint["value"]
        ) is None:
            raise ValueError("provider fingerprint is not an exact SHA-256")
        return self


class McpGateway(Protocol):
    async def catalog(self, profile: str) -> frozenset[str]: ...

    async def call(self, profile: str, tool: str, arguments: Mapping[str, Any]) -> dict[str, Any]: ...


def _structured_result(result: object) -> dict[str, Any]:
    structured = getattr(result, "structured_content", None)
    if isinstance(structured, Mapping):
        return dict(structured)
    for block in getattr(result, "content", ()):
        text = getattr(block, "text", None)
        if not isinstance(text, str) or len(text.encode()) > MAX_RESULT_BYTES:
            continue
        try:
            value = json.loads(text)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            return value
    raise RuntimeError("MCP tool returned no bounded structured object")


def _result_is_error(result: object) -> bool:
    return getattr(result, "isError", getattr(result, "is_error", False)) is True


class RemoteMcpGateway:
    """Bounded MCP client with profile-specific bearer credentials."""

    def __init__(self, endpoint: str, tokens: Mapping[str, str] | BearerSource) -> None:
        parsed = urlsplit(endpoint)
        if (
            parsed.scheme != "https"
            or not parsed.hostname
            or parsed.username
            or parsed.password
            or parsed.query
            or parsed.fragment
            or parsed.path.rstrip("/") != "/mcp"
        ):
            raise ValueError("operational MCP endpoint must be a bounded HTTPS /mcp URL")
        self.endpoint = endpoint
        self.bearers = StaticBearerSource(tokens) if isinstance(tokens, Mapping) else tokens

    async def _token(self, profile: str) -> str:
        try:
            return await self.bearers.token(profile)
        except OAuthCredentialError as exc:
            raise MissingCredential(profile) from exc

    async def _invoke(self, profile: str, tool: str | None, arguments: Mapping[str, Any]) -> object:
        import httpx2
        from mcp import ClientSession
        from mcp.client.streamable_http import streamable_http_client

        timeout = httpx2.Timeout(25.0, connect=5.0)
        token = await self._token(profile)
        async with (
            httpx2.AsyncClient(
                headers={"Authorization": f"Bearer {token}"},
                follow_redirects=False,
                timeout=timeout,
            ) as client,
            streamable_http_client(self.endpoint, http_client=client) as streams,
        ):
            read_stream, write_stream = streams
            async with ClientSession(read_stream, write_stream, read_timeout_seconds=25) as session:
                await session.initialize()
                if tool is None:
                    return await session.list_tools()
                return await session.call_tool(tool, dict(arguments))

    async def catalog(self, profile: str) -> frozenset[str]:
        result = await self._invoke(profile, None, {})
        return frozenset(str(tool.name) for tool in result.tools)  # type: ignore[attr-defined]

    async def call(self, profile: str, tool: str, arguments: Mapping[str, Any]) -> dict[str, Any]:
        result = await self._invoke(profile, tool, arguments)
        if _result_is_error(result):
            raise RuntimeError(f"MCP {tool} returned an error")
        return _structured_result(result)


class MissingCredential(RuntimeError):
    def __init__(self, profile: str) -> None:
        super().__init__(profile)
        self.profile = profile


class MissingPreActionEvidence(RuntimeError):
    """No durable claim exists, so a resume must not create a new mutation."""


class AmbiguousEvidenceMutation(RuntimeError):
    """A lifecycle call may have mutated but could not be reconciled."""


@dataclass(frozen=True, slots=True)
class ExecutorSpec:
    requirement_id: str
    profiles: tuple[str, ...]
    tools: tuple[tuple[str, str], ...]
    probe: str
    gap_code: str
    gap_dependency: str
    action_tool: str | None = None
    action_target: Literal["current", "previous"] | None = None


# Every scenario has an explicit production surface and a concrete remaining
# gap.  There is intentionally no catch-all executor.
EXECUTORS: tuple[ExecutorSpec, ...] = (
    ExecutorSpec(
        "FM01",
        ("provider",),
        (("provider", "provider.acceptance.dataset.lifecycle"),
         ("provider", "provider.acceptance.notebook.lifecycle"),
         ("provider", "provider.acceptance.claim.get"),
         ("provider", "provider.acceptance.claim.cleanup")),
        "catalog_only",
        "FM01_EVIDENCE_PROVIDER_CONFIGURATION_REQUIRED",
        "exact disposable provider owner configuration",
    ),
    ExecutorSpec(
        "FM02",
        ("provider",),
        (("provider", "provider.acceptance.notebook.lifecycle"),
         ("provider", "provider.acceptance.claim.get"),
         ("provider", "provider.acceptance.claim.cleanup")),
        "catalog_only",
        "FM02_EVIDENCE_PROVIDER_CONFIGURATION_REQUIRED",
        "exact disposable provider owner configuration",
    ),
    ExecutorSpec(
        "FM03",
        ("operator", "provider"),
        (("operator", "runtime.events.history"),
         ("provider", "provider.acceptance.notebook.lifecycle"),
         ("provider", "provider.acceptance.claim.get"),
         ("provider", "provider.acceptance.claim.cleanup")),
        "catalog_only",
        "FM03_RUNTIME_IDENTITY_CONFIGURATION_REQUIRED",
        "exact runtime run_id, attempt_id, epoch and disposable provider owner configuration",
    ),
    ExecutorSpec(
        "FM04",
        ("reader", "operator"),
        (("reader", "master.status"),
         ("operator", "acceptance.scenario.request"),
         ("operator", "acceptance.scenario.status")),
        "master_status",
        "FM04_ACCEPTANCE_SCENARIO_EXECUTOR_MISSING",
        "owner-only typed empty-master bootstrap acceptance executor",
    ),
    ExecutorSpec(
        "FM05",
        ("operator",),
        (("operator", "acceptance.scenario.request"),
         ("operator", "acceptance.scenario.status")),
        "catalog_only",
        "FM05_CHECKPOINT_ACCEPTANCE_HOST_EXECUTOR_MISSING",
        "owner-only acceptance.scenario request/status host executor and fixed checkpoint assets",
    ),
    ExecutorSpec(
        "FM06",
        ("reader", "operator", "provider"),
        (
            ("reader", "master.status"),
            ("reader", "checkpoint.status"),
            ("operator", "checkpoint.restore.request"),
            ("operator", "operation.get"),
            ("provider", "provider.resources.read"),
            ("provider", "provider.acceptance.notebook.lifecycle"),
            ("provider", "provider.acceptance.claim.get"),
            ("provider", "provider.acceptance.claim.cleanup"),
        ),
        "checkpoint_status",
        "FM06_EVIDENCE_PROVIDER_CONFIGURATION_REQUIRED",
        "exact disposable provider owner configuration",
        "checkpoint.restore.request",
        "current",
    ),
    ExecutorSpec(
        "FM07",
        ("reader", "operator"),
        (("reader", "master.status"),
         ("operator", "acceptance.scenario.request"),
         ("operator", "acceptance.scenario.status")),
        "master_status",
        "FM07_ACCEPTANCE_SCENARIO_EXECUTOR_MISSING",
        "owner-only typed concurrent ensure acceptance executor",
    ),
    ExecutorSpec(
        "FM08",
        ("reader", "operator"),
        (("reader", "master.status"),
         ("operator", "acceptance.scenario.request"),
         ("operator", "acceptance.scenario.status")),
        "master_status",
        "FM08_ACCEPTANCE_SCENARIO_EXECUTOR_MISSING",
        "owner-only typed callback-loss/restart acceptance executor",
    ),
    ExecutorSpec(
        "FM09",
        ("reader", "operator"),
        (("reader", "master.status"),
         ("operator", "acceptance.scenario.request"),
         ("operator", "acceptance.scenario.status")),
        "master_status",
        "FM09_ACCEPTANCE_SCENARIO_EXECUTOR_MISSING",
        "owner-only typed duplicate/stale replay acceptance executor",
    ),
    ExecutorSpec(
        "FM10",
        ("reader", "operator"),
        (("reader", "master.status"),
         ("operator", "acceptance.scenario.request"),
         ("operator", "acceptance.scenario.status")),
        "master_status",
        "FM10_ACCEPTANCE_SCENARIO_EXECUTOR_MISSING",
        "owner-only typed lease-expiry acceptance executor",
    ),
    ExecutorSpec(
        "FM11",
        ("reader", "operator"),
        (("reader", "master.status"),
         ("operator", "acceptance.scenario.request"),
         ("operator", "acceptance.scenario.status")),
        "master_status",
        "FM11_ACCEPTANCE_SCENARIO_EXECUTOR_MISSING",
        "owner-only typed old-epoch return acceptance executor",
    ),
    ExecutorSpec(
        "FM12",
        ("reader", "operator"),
        (("reader", "master.status"),
         ("reader", "checkpoint.status"),
         ("operator", "acceptance.scenario.request"),
         ("operator", "acceptance.scenario.status")),
        "master_checkpoint_status",
        "FM12_ACCEPTANCE_SCENARIO_EXECUTOR_MISSING",
        "owner-only typed clean drain/checkpoint/stop acceptance executor",
    ),
    ExecutorSpec(
        "FM13",
        ("reader", "operator", "provider"),
        (
            ("reader", "master.status"),
            ("reader", "checkpoint.status"),
            ("operator", "master.rotation.request"),
            ("operator", "operation.get"),
            ("provider", "provider.resources.read"),
        ),
        "master_checkpoint_status",
        "ROTATION_EVIDENCE_RUN_LOCATOR_MISSING",
        "owner-issued claim for an already-running exact verifier Notebook, followed by a durable rotation receipt",
        "master.rotation.request",
    ),
    ExecutorSpec(
        "FM14",
        ("operator",),
        (("operator", "acceptance.scenario.request"),
         ("operator", "acceptance.scenario.status")),
        "catalog_only",
        "FM14_CHECKPOINT_ACCEPTANCE_HOST_EXECUTOR_MISSING",
        "owner-only acceptance.scenario request/status host executor and fixed FM14 task assets",
    ),
    ExecutorSpec(
        "FM15",
        ("operator",),
        (("operator", "acceptance.scenario.request"),
         ("operator", "acceptance.scenario.status")),
        "catalog_only",
        "FM15_CHECKPOINT_ACCEPTANCE_HOST_EXECUTOR_MISSING",
        "owner-only acceptance.scenario request/status host executor and fixed FM15 task assets",
    ),
    ExecutorSpec(
        "FM16",
        ("reader", "operator", "provider"),
        (
            ("reader", "bloggers.migration.accounting"),
            ("reader", "operation.get"),
            ("reader", "master.status"),
            ("reader", "checkpoint.status"),
            ("reader", "data.change.status"),
            ("operator", "master.rotation.request"),
            ("operator", "data.change.preview"),
            ("operator", "data.change.apply"),
            ("provider", "provider.acceptance.notebook.lifecycle"),
            ("provider", "provider.acceptance.claim.get"),
            ("provider", "provider.acceptance.claim.cleanup"),
        ),
        "catalog_only",
        "FM16_DATA_WORKLOAD_CONFIGURATION_REQUIRED",
        "owner-fixed production data-workload plan/config/state and duplicate-resolution envelope",
    ),
    ExecutorSpec(
        "FM17",
        ("reader", "operator", "provider"),
        (
            ("reader", "master.status"),
            ("reader", "bloggers.migration.accounting"),
            ("reader", "checkpoint.status"),
            ("reader", "operation.get"),
            ("reader", "data.change.status"),
            ("operator", "master.rotation.request"),
            ("operator", "data.change.preview"),
            ("operator", "data.change.apply"),
            ("provider", "provider.acceptance.notebook.lifecycle"),
            ("provider", "provider.acceptance.claim.get"),
            ("provider", "provider.acceptance.claim.cleanup"),
        ),
        "catalog_only",
        "FM17_DATA_WORKLOAD_CONFIGURATION_REQUIRED",
        "same exact matrix-wide production data-workload state used by FM16",
    ),
    ExecutorSpec(
        "FM18",
        ("reader", "operator", "provider"),
        (("reader", "bloggers.migration.accounting"), ("reader", "operation.get"),
         ("reader", "master.status"), ("reader", "checkpoint.status"),
         ("reader", "data.change.status"), ("operator", "master.rotation.request"),
         ("operator", "data.change.preview"), ("operator", "data.change.apply"),
         ("provider", "provider.acceptance.notebook.lifecycle"),
         ("provider", "provider.acceptance.claim.get"),
         ("provider", "provider.acceptance.claim.cleanup")),
        "catalog_only",
        "FM18_DATA_WORKLOAD_CONFIGURATION_REQUIRED",
        "same exact two-model production request and matrix-wide data-workload state",
    ),
    ExecutorSpec(
        "FM19",
        ("reader", "operator", "provider"),
        (("reader", "bloggers.migration.accounting"), ("reader", "operation.get"),
         ("reader", "master.status"), ("reader", "checkpoint.status"),
         ("reader", "data.change.status"), ("operator", "master.rotation.request"),
         ("operator", "data.change.preview"), ("operator", "data.change.apply"),
         ("provider", "provider.acceptance.notebook.lifecycle"),
         ("provider", "provider.acceptance.claim.get"),
         ("provider", "provider.acceptance.claim.cleanup")),
        "catalog_only",
        "FM19_DATA_WORKLOAD_CONFIGURATION_REQUIRED",
        "same exact two-model production request and matrix-wide data-workload state",
    ),
    ExecutorSpec(
        "FM20",
        ("reader", "operator", "provider"),
        (
            ("reader", "master.status"),
            ("reader", "bloggers.search"),
            ("operator", "master.ensure"),
            ("operator", "operation.get"),
            ("provider", "provider.resources.read"),
        ),
        "catalog_only",
        "FM20_SIGNED_HOST_OR_EVIDENCE_NOTEBOOK_MISSING",
        "fresh signed v2 host reboot receipt plus an exact already-running FM20 evidence Notebook claim",
    ),
    ExecutorSpec(
        "FM21",
        ("reader", "operator", "provider"),
        (
            ("reader", "checkpoint.status"),
            ("reader", "bloggers.migration.accounting"),
            ("reader", "operation.get"),
            ("reader", "master.status"),
            ("reader", "data.change.status"),
            ("operator", "master.rotation.request"),
            ("operator", "data.change.preview"),
            ("operator", "data.change.apply"),
            ("provider", "provider.acceptance.notebook.lifecycle"),
            ("provider", "provider.acceptance.claim.get"),
            ("provider", "provider.acceptance.claim.cleanup"),
        ),
        "catalog_only",
        "FM21_DATA_WORKLOAD_CONFIGURATION_REQUIRED",
        "owner-fixed production data-workload state with fixed "
        "insert/checkpoint/delete/checkpoint/zero-preview fixture",
    ),
    ExecutorSpec(
        "FM22",
        ("provider",),
        (
            ("provider", "provider.acceptance.dataset.lifecycle"),
            ("provider", "provider.acceptance.notebook.lifecycle"),
            ("provider", "provider.acceptance.claim.get"),
            ("provider", "provider.acceptance.claim.cleanup"),
        ),
        "catalog_only",
        "FM22_EVIDENCE_PROVIDER_CONFIGURATION_REQUIRED",
        "exact disposable provider owner configuration",
    ),
    ExecutorSpec(
        "FM23",
        ("reader", "operator", "provider"),
        (("reader", "provider.resources.status"),
         ("operator", "provider.protected_resource.probe"),
         ("provider", "provider.acceptance.notebook.lifecycle"),
         ("provider", "provider.acceptance.claim.get"),
         ("provider", "provider.acceptance.claim.cleanup")),
        "protected_probe",
        "FM23_EVIDENCE_PROVIDER_CONFIGURATION_REQUIRED",
        "exact disposable provider owner configuration after protected denial observation",
    ),
    ExecutorSpec(
        "FM24",
        ("reader", "operator"),
        (("reader", "master.status"),
         ("operator", "acceptance.scenario.request"),
         ("operator", "acceptance.scenario.status")),
        "master_status",
        "FM24_ACCEPTANCE_SCENARIO_EXECUTOR_MISSING",
        "owner-only typed 60-90 minute session-rotation soak executor",
    ),
)

if len(EXECUTORS) != 24 or tuple(item.requirement_id for item in EXECUTORS) != tuple(
    f"FM{ordinal:02d}" for ordinal in range(1, 25)
):  # pragma: no cover
    raise RuntimeError("trusted driver requires one exact executor for every FM01-FM24 scenario")


def _sha(value: object) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def _evidence_driver_config() -> EvidenceDriverConfig | None:
    raw = os.environ.get("MY_DATA_HUB_OPERATIONAL_EVIDENCE_DRIVER_JSON", "").strip()
    if not raw:
        return None
    if len(raw.encode()) > MAX_EVIDENCE_CLAIMS_BYTES:
        raise ValueError("operational evidence driver configuration is too large")
    return EvidenceDriverConfig.model_validate_json(raw)


def _owner_path(value: str, *, label: str) -> Path:
    path = Path(value)
    if not path.is_absolute() or path != path.resolve(strict=False):
        raise ValueError(f"{label} must be an absolute path without symlink components")
    return path


def _read_owner_model(path: Path, model: Any) -> Any:
    if path.is_symlink() or not path.is_file():
        raise ValueError(f"{path.name} must be a regular non-symlink file")
    raw = path.read_bytes()
    if not 1 <= len(raw) <= MAX_REQUEST_BYTES:
        raise ValueError(f"{path.name} is empty or exceeds 256 KiB")
    return model.model_validate_json(raw)


def _valid_secret(name: str, *, optional: bool = False) -> bool:
    value = os.environ.get(name, "")
    if not value and optional:
        return True
    return 24 <= len(value) <= 4096 and not any(char.isspace() for char in value)


@dataclass(frozen=True, slots=True)
class PreparedDataWorkload:
    plan: DataWorkloadPlan
    production: ProductionDataWorkloadConfig
    store: AtomicJsonStateStore
    plan_path: Path
    production_config_path: Path
    state_path: Path
    owner_envelope_path: Path | None


def _prepare_data_workload(
    request: DriverRequest, config: DataWorkloadDriverConfig
) -> PreparedDataWorkload:
    plan_path = _owner_path(config.plan_path, label="data workload plan")
    production_path = _owner_path(
        config.production_config_path, label="data workload production config"
    )
    state_path = _owner_path(config.state_path, label="data workload state")
    owner_path = (
        _owner_path(config.owner_envelope_path, label="FM16 owner envelope")
        if config.owner_envelope_path is not None
        else None
    )
    plan = _read_owner_model(plan_path, DataWorkloadPlan)
    production = _read_owner_model(production_path, ProductionDataWorkloadConfig)
    if plan.matrix_id != request.matrix_id or plan.source_commit != request.commit_sha:
        raise ValueError("data workload plan differs from the exact operational matrix")
    if production.timeout_seconds > 6900:
        raise ValueError("data workload deadline exceeds the outer driver safety budget")
    if not _valid_secret(
        "MY_DATA_HUB_DATA_CONTROL_TOKEN",
        optional=production.control_base_url.startswith("http://127.0.0.1:8080"),
    ):
        raise ValueError("data workload control credential is absent or invalid")
    oauth_path = os.environ.get("MY_DATA_HUB_MCP_OAUTH_CREDENTIAL_FILE", "").strip()
    if oauth_path:
        validate_oauth_credential_file(
            _owner_path(oauth_path, label="MCP OAuth credential file"),
            required_profiles=frozenset({"reader", "operator"}),
        )
    else:
        for name in (
            "MY_DATA_HUB_DATA_MCP_READER_TOKEN",
            "MY_DATA_HUB_DATA_MCP_OPERATOR_TOKEN",
        ):
            if not _valid_secret(name):
                raise ValueError(f"required data workload credential {name} is absent or invalid")
    store = AtomicJsonStateStore(state_path)
    existed = state_path.exists()
    if request.resume_only and not existed:
        raise MissingPreActionEvidence("resume has no durable data-workload state")
    if existed:
        info = state_path.stat(follow_symlinks=False)
        if (
            state_path.is_symlink()
            or not stat.S_ISREG(info.st_mode)
            or stat.S_IMODE(info.st_mode) != 0o600
            or info.st_uid != os.getuid()
        ):
            raise ValueError("data workload state must be an owner-owned mode-0600 regular file")
    state = store.load(plan)
    if not existed:
        # This metadata-only matrix claim is durable before the production
        # entrypoint is allowed to persist any scenario action intent.
        store.persist(state)
    return PreparedDataWorkload(
        plan=plan,
        production=production,
        store=store,
        plan_path=plan_path,
        production_config_path=production_path,
        state_path=state_path,
        owner_envelope_path=owner_path,
    )


def _invoke_data_workload_entrypoint(prepared: PreparedDataWorkload) -> ProductionDataWorkloadReceipt:
    entrypoint = Path(__file__).with_name("data_workload_evidence.py")
    if entrypoint.is_symlink() or not entrypoint.is_file():
        raise ValueError("trusted data-workload production entrypoint is unavailable")
    with tempfile.TemporaryDirectory(prefix="my-data-hub-data-workload-") as folder:
        output = Path(folder) / "receipt.json"
        command = [
            sys.executable,
            str(entrypoint),
            "--plan",
            str(prepared.plan_path),
            "--production-config",
            str(prepared.production_config_path),
            "--state",
            str(prepared.state_path),
            "--output",
            str(output),
        ]
        if prepared.owner_envelope_path is not None and prepared.owner_envelope_path.exists():
            command.extend(("--owner-envelope", str(prepared.owner_envelope_path)))
        completed = subprocess.run(
            command,
            check=False,
            timeout=prepared.production.timeout_seconds + 30,
            env=os.environ.copy(),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        if completed.returncode not in {0, 2}:
            raise RuntimeError(f"data-workload entrypoint exited unexpectedly: {completed.returncode}")
        if output.is_symlink() or not output.is_file() or not 1 <= output.stat().st_size <= MAX_RESULT_BYTES:
            raise RuntimeError("data-workload entrypoint emitted no bounded receipt")
        return ProductionDataWorkloadReceipt.model_validate_json(output.read_bytes())


def _data_requirement_proof(
    request: DriverRequest,
    receipt: ProductionDataWorkloadReceipt,
    state: DataWorkloadState,
) -> tuple[dict[str, object], tuple[str, ...], str]:
    evidence = receipt.evidence
    if (
        evidence is None
        or evidence.matrix_id != request.matrix_id
        or evidence.source_commit != request.commit_sha
        or evidence.live_evidence is not False
    ):
        raise ValueError("data workload evidence differs from the exact matrix")
    requirements = {item.requirement_id: item for item in evidence.requirements}
    fm18 = requirements["FM18"]
    fm19 = requirements["FM19"]
    if (
        len(fm18.operation_ids) != 2
        or len(fm19.operation_ids) != 2
        or fm18.operation_ids[0] != fm19.operation_ids[0]
        or fm18.operation_ids[1] == fm19.operation_ids[1]
    ):
        raise ValueError("FM18/FM19 do not split one exact two-model request")
    if (
        state.quarantine is None
        or state.blogger_terminal is None
        or requirements["FM16"].operation_ids
        != (str(state.quarantine.operation_id), str(state.blogger_terminal.operation_id))
        or state.restore_operation_id is None
        or requirements["FM17"].operation_ids != (state.restore_operation_id,)
        or state.embedding_request_id is None
        or state.embedding_terminal is None
        or fm18.operation_ids[0] != str(state.embedding_request_id)
    ):
        raise ValueError("data evidence operation IDs differ from the durable production state")
    model_task_ids = {
        item.model_exact_id: str(item.task_run_id) for item in state.embedding_terminal.models
    }
    if (
        fm18.operation_ids[1] != model_task_ids.get(E5_EXACT_ID)
        or fm19.operation_ids[1] != model_task_ids.get(BGE_EXACT_ID)
        or state.insert_status is None
        or state.delete_status is None
        or state.final_zero_preview is None
        or requirements["FM21"].operation_ids
        != (state.insert_status.operation_id, state.delete_status.operation_id)
        or state.final_zero_preview.action != "delete"
        or state.final_zero_preview.affected_rows != 0
        or state.delete_status.post_change_checkpoint is None
        or state.final_zero_preview.pre_change_checkpoint_id
        != state.delete_status.post_change_checkpoint.checkpoint_id
    ):
        raise ValueError("split worker or fixed FM21 evidence differs from durable production state")
    selected = next(
        item for item in evidence.requirements if item.requirement_id == request.requirement_id
    )
    if set(selected.assertion_evidence_sha256) != set(request.required_assertions):
        raise ValueError("data workload assertion evidence differs from the catalog")
    bundle_sha256 = evidence.bundle_sha256
    proofs = {
        name: {
            "production_evidence_sha256": selected.assertion_evidence_sha256[name],
            "data_workload_bundle_sha256": bundle_sha256,
        }
        for name in request.required_assertions
    }
    return proofs, tuple(selected.operation_ids), str(bundle_sha256)


def _subtask_id(request: DriverRequest, kind: Literal["dataset", "notebook"]) -> UUID:
    identity = (
        f"operational:{request.matrix_id}:{request.requirement_id}:"
        f"{request.task_run_id}:{kind}"
    )
    return uuid5(NAMESPACE_URL, identity)


def _resource_ref(owner: str, request: DriverRequest, kind: Literal["dataset", "notebook"]) -> str:
    digest = hashlib.sha256(
        f"{request.matrix_id}:{request.requirement_id}:{request.task_run_id}:{kind}".encode()
    ).hexdigest()[:12]
    return f"{owner}/mdh-{request.requirement_id.casefold()}-{kind[:2]}-{digest}"


def _one_claim_evidence(claim: Mapping[str, Any], event_type: str) -> dict[str, Any]:
    evidence = claim.get("evidence")
    if not isinstance(evidence, list):
        raise ValueError("acceptance claim evidence is not a bounded list")
    matches = [item for item in evidence if isinstance(item, Mapping) and item.get("event_type") == event_type]
    if len(matches) != 1:
        raise ValueError(f"acceptance claim lacks one exact {event_type} receipt")
    item = matches[0]
    value = item.get("evidence")
    if not isinstance(value, Mapping) or item.get("evidence_sha256") != _sha(dict(value)):
        raise ValueError(f"acceptance {event_type} evidence hash differs")
    return dict(value)


def _exact_claim(
    response: Mapping[str, Any], *, request: DriverRequest, task_id: UUID, cleanup_state: str
) -> dict[str, Any]:
    if (
        response.get("found") is not True
        or response.get("scenario_id") != request.requirement_id
        or response.get("task_id") != str(task_id)
        or response.get("state") != "SUCCEEDED"
        or response.get("failure_code") is not None
        or response.get("mutation_started") is not True
        or response.get("cleanup_state") != cleanup_state
        or response.get("bounded") is not True
    ):
        raise ValueError("acceptance claim is not an exact successful durable task")
    return dict(response)


def _output_document(
    request: DriverRequest,
    proofs: Mapping[str, object],
    *,
    lifecycle_events: tuple[Mapping[str, Any], ...] = (),
    operation_ids: tuple[str, ...] = (),
) -> dict[str, Any]:
    if set(proofs) != set(request.required_assertions):
        raise ValueError("evidence proofs differ from the exact assertion catalog")
    return {
        "schema_version": "my-data-hub-operational-kaggle-output.v1",
        "matrix_id": str(request.matrix_id),
        "scenario": request.scenario,
        "task_run_id": str(request.task_run_id),
        "outcome": "PASS",
        "assertions": [
            {"name": name, "outcome": "PASS", "evidence_sha256": _sha(proofs[name])}
            for name in request.required_assertions
        ],
        "lifecycle_events": [dict(item) for item in lifecycle_events],
        "operation_ids": list(operation_ids),
        "completed_at": request.evidence_issued_at.isoformat(),
    }


def _notebook_source(request: DriverRequest, output: Mapping[str, Any]) -> tuple[str, str]:
    raw = canonical_json_bytes(dict(output))
    encoded = base64.b64encode(raw).decode("ascii")
    source = (
        "import base64\n"
        f"TASK_RUN_ID = {str(request.task_run_id)!r}\n"
        f"payload = base64.b64decode({encoded!r})\n"
        "with open('operational-result.json', 'wb') as handle:\n"
        "    handle.write(payload)\n"
        "print(TASK_RUN_ID)\n"
    )
    return source, hashlib.sha256(raw).hexdigest()


def _dataset_arguments(request: DriverRequest, owner: str) -> tuple[UUID, dict[str, Any]]:
    task_id = _subtask_id(request, "dataset")
    resource_ref = _resource_ref(owner, request, "dataset")
    created = canonical_json_bytes(
        {"matrix_id": str(request.matrix_id), "scenario": request.scenario, "stage": "create"}
    ).decode()
    versioned = canonical_json_bytes(
        {"matrix_id": str(request.matrix_id), "scenario": request.scenario, "stage": "version"}
    ).decode()
    return task_id, {
        "scenario_id": request.requirement_id,
        "task_id": str(task_id),
        "idempotency_key": f"operational:{request.matrix_id}:{request.requirement_id}:dataset",
        "resource_ref": resource_ref,
        "title": resource_ref.split("/", 1)[1],
        "file_name": "acceptance.json",
        "file_sha256": hashlib.sha256(created.encode()).hexdigest(),
        "file_utf8": created,
        "version_file_sha256": hashlib.sha256(versioned.encode()).hexdigest(),
        "version_file_utf8": versioned,
    }


def _notebook_arguments(
    request: DriverRequest, owner: str, output: Mapping[str, Any]
) -> tuple[UUID, dict[str, Any]]:
    task_id = _subtask_id(request, "notebook")
    resource_ref = _resource_ref(owner, request, "notebook")
    source, expected_output_sha256 = _notebook_source(request, output)
    return task_id, {
        "scenario_id": request.requirement_id,
        "task_id": str(task_id),
        "task_run_id": str(request.task_run_id),
        "idempotency_key": f"operational:{request.matrix_id}:{request.requirement_id}:notebook",
        "resource_ref": resource_ref,
        "title": resource_ref.split("/", 1)[1],
        "code_file": "scenario.py",
        "source_utf8": source,
        "dataset_inputs": [],
        "output_file_name": "operational-result.json",
        "expected_output_sha256": expected_output_sha256,
        "max_output_bytes": MAX_RESULT_BYTES,
    }


def _tokens_from_environment() -> dict[str, str]:
    return {
        "reader": os.environ.get("MY_DATA_HUB_MCP_CANARY_TOKEN", "").strip(),
        "operator": os.environ.get("MY_DATA_HUB_MCP_ACCEPTANCE_OPERATOR_TOKEN", "").strip(),
        "migration": os.environ.get("MY_DATA_HUB_MCP_MIGRATION_OPERATOR_TOKEN", "").strip(),
        "provider": os.environ.get("MY_DATA_HUB_MCP_PROVIDER_OPERATOR_TOKEN", "").strip(),
    }


def _check(
    name: str, outcome: Literal["PASS", "BLOCKED", "FAIL"], detail: str, evidence: object | None = None
) -> CapabilityCheck:
    return CapabilityCheck(
        name=name,
        outcome=outcome,
        detail_code=detail,
        evidence_sha256=_sha(evidence) if evidence is not None else None,
    )


async def _safe_probe(gateway: McpGateway, spec: ExecutorSpec) -> tuple[list[CapabilityCheck], dict[str, Any]]:
    checks: list[CapabilityCheck] = []
    observations: dict[str, Any] = {}
    if spec.probe == "catalog_only":
        return checks, observations
    if spec.probe in {"master_status", "master_checkpoint_status", "blogger_checkpoint_status"}:
        observations["master"] = await gateway.call("reader", "master.status", {})
    if spec.probe in {"checkpoint_status", "master_checkpoint_status", "blogger_checkpoint_status"}:
        observations["checkpoint"] = await gateway.call("reader", "checkpoint.status", {})
    if spec.probe == "provider_status":
        observations["provider"] = await gateway.call(spec.profiles[0], "provider.resources.status", {"limit": 100})
    if spec.probe == "embedding_status":
        observations["embedding"] = await gateway.call("reader", "embedding.coverage", {})
    if spec.probe == "blogger_accounting":
        batch_id = os.environ.get("MY_DATA_HUB_OPERATIONAL_BLOGGER_BATCH_ID", "").strip()
        if not batch_id:
            checks.append(_check("blogger-batch", "BLOCKED", "BLOGGER_BATCH_ID_MISSING"))
        else:
            observations["blogger_accounting"] = await gateway.call(
                "reader", "bloggers.migration.accounting", {"export_batch_id": batch_id}
            )
    if spec.probe == "blogger_checkpoint_status":
        observations["blogger_statistics"] = await gateway.call("reader", "bloggers.statistics", {})
    if spec.probe == "stale_epoch":
        master = await gateway.call("reader", "master.status", {})
        observations["master"] = master
        epoch = master.get("master_epoch", master.get("epoch"))
        if isinstance(epoch, int) and not isinstance(epoch, bool) and epoch > 1:
            observations["stale_epoch"] = await gateway.call(
                "operator",
                "runtime.stale_epoch.probe",
                {"expected_active_epoch": epoch, "submitted_epoch": epoch - 1},
            )
        else:
            checks.append(_check("stale-epoch-precondition", "BLOCKED", "ACTIVE_EPOCH_GT_ONE_REQUIRED"))
    if spec.probe == "protected_probe":
        provider = await gateway.call("reader", "provider.resources.status", {"limit": 100})
        observations["provider"] = provider
        rows = provider.get("resources")
        resource_ref = (
            next(
                (
                    row.get("resource_ref")
                    for row in rows
                    if isinstance(rows, list)
                    and isinstance(row, Mapping)
                    and row.get("control_class") == "orchestrator_protected"
                    and isinstance(row.get("resource_ref"), str)
                ),
                None,
            )
            if isinstance(rows, list)
            else None
        )
        if resource_ref is None:
            checks.append(_check("protected-resource", "BLOCKED", "PROTECTED_RESOURCE_NOT_REGISTERED"))
        else:
            observations["protected_resource_ref"] = resource_ref
            observations["protected_probe"] = await gateway.call(
                "operator", "provider.protected_resource.probe", {"resource_ref": resource_ref}
            )
    for name, value in observations.items():
        checks.append(_check(f"probe.{name}", "PASS", "SAFE_OBSERVATION_COMPLETE", value))
    return checks, observations


async def _claim_get(
    gateway: McpGateway, request: DriverRequest, task_id: UUID
) -> dict[str, Any]:
    return await gateway.call(
        "provider",
        "provider.acceptance.claim.get",
        {"scenario_id": request.requirement_id, "task_id": str(task_id)},
    )


async def _run_lifecycle(
    gateway: McpGateway,
    request: DriverRequest,
    *,
    tool: Literal[
        "provider.acceptance.dataset.lifecycle", "provider.acceptance.notebook.lifecycle"
    ],
    task_id: UUID,
    arguments: Mapping[str, Any],
    allow_create_on_resume: bool = False,
) -> dict[str, Any]:
    existing = await _claim_get(gateway, request, task_id)
    if existing.get("found") is True:
        if existing.get("state") in {"SUCCEEDED", "FAILED"}:
            return existing
    elif request.resume_only and not allow_create_on_resume:
        raise MissingPreActionEvidence("resume has no durable acceptance claim")
    try:
        response = await gateway.call("provider", tool, arguments)
    except Exception as exc:
        # Once the mutating call is issued, only an exact durable claim can
        # classify the outcome. Absence/transport ambiguity is never BLOCKED/0.
        try:
            reconciled = await _claim_get(gateway, request, task_id)
        except Exception as reconcile_exc:
            raise AmbiguousEvidenceMutation("lifecycle response and claim reconciliation were lost") from reconcile_exc
        if reconciled.get("found") is not True:
            raise AmbiguousEvidenceMutation("lifecycle response was lost before a claim could be reconciled") from exc
        response = reconciled
    if (
        response.get("found") is True
        and response.get("state") in {"CLAIMED", "RUNNING"}
    ):
        try:
            response = await gateway.call("provider", tool, arguments)
        except Exception as exc:
            raise AmbiguousEvidenceMutation("durable lifecycle claim did not reconcile terminally") from exc
    return dict(response)


def _notebook_binding(
    request: DriverRequest,
    task_id: UUID,
    claim: Mapping[str, Any],
    *,
    cleanup_state: Literal["PENDING", "COMPLETE"] = "PENDING",
) -> tuple[EvidenceRunLocator, dict[str, Any]]:
    exact = _exact_claim(claim, request=request, task_id=task_id, cleanup_state=cleanup_state)
    notebook = _one_claim_evidence(exact, "PROVIDER_NOTEBOOK")
    output = _one_claim_evidence(exact, "OUTPUT_READ")
    if notebook.get("task_run_id") != str(request.task_run_id):
        raise ValueError("acceptance Notebook task_run_id differs from the matrix")
    if (
        notebook.get("terminal_state") != "complete"
        or output.get("provider_run_ref") != notebook.get("provider_run_ref")
        or output.get("output_file_name") != request.result_file
        or output.get("file_count") != 1
    ):
        raise ValueError("acceptance Notebook terminal/output receipt differs")
    expected_receipt = {
        key: value for key, value in output.items() if key != "output_receipt_sha256"
    }
    if output.get("output_receipt_sha256") != _sha(expected_receipt):
        raise ValueError("acceptance output-read receipt hash differs")
    fingerprint = notebook.get("fingerprint")
    if not isinstance(fingerprint, Mapping):
        raise ValueError("acceptance Notebook fingerprint is absent")
    locator = EvidenceRunLocator.model_validate(
        {
            "claim_sha256": notebook.get("claim_sha256"),
            "task_id": str(task_id),
            "task_run_id": notebook.get("task_run_id"),
            "provider_ref": notebook.get("provider_ref"),
            "provider_run_ref": notebook.get("provider_run_ref"),
            "provider_kernel_id": notebook.get("provider_kernel_id"),
            "source_version": notebook.get("source_version"),
            "source_sha256": notebook.get("source_sha256"),
            "fingerprint": dict(fingerprint),
            "private": True,
        }
    )
    return locator, output


def _ready(
    request: DriverRequest,
    task_id: UUID,
    locator: EvidenceRunLocator,
    output: Mapping[str, Any],
    scenario_output: Mapping[str, Any],
    *,
    checks: list[CapabilityCheck],
    observations: Mapping[str, Any],
    mutations_started: int,
) -> TrustedDriverResult:
    return TrustedDriverResult(
        schema_version="my-data-hub-operational-kaggle-driver-result.v2",
        phase="EXECUTE",
        outcome="READY",
        scenario=request.scenario,
        task_run_id=request.task_run_id,
        provider_ref=locator.provider_ref,
        provider_run_ref=locator.provider_run_ref,
        provider_kernel_id=locator.provider_kernel_id,
        source_version=locator.source_version,
        source_sha256=locator.source_sha256,
        mutations_started=mutations_started,
        capability_checks=tuple(checks),
        observation_sha256=_sha(dict(observations)),
        claim_task_id=task_id,
        claim_sha256=locator.claim_sha256,
        output_receipt_sha256=str(output["output_receipt_sha256"]),
        output_file_sha256=str(output["output_file_sha256"]),
        output_tree_sha256=str(output["output_tree_sha256"]),
        cleanup_state="PENDING",
        scenario_output=DriverScenarioOutput.model_validate(scenario_output),
    )


def _validate_runtime_history(
    response: Mapping[str, Any], identity: RuntimeEventIdentity
) -> dict[str, Any]:
    if set(response) != {"events", "count", "bounded"} or response.get("bounded") is not True:
        raise ValueError("runtime event history response differs from the exact bounded contract")
    events = response.get("events")
    if not isinstance(events, list) or response.get("count") != len(events) or not 2 <= len(events) <= 200:
        raise ValueError("runtime event history has an invalid count")
    expected_keys = {
        "event_id", "schema_version", "run_id", "attempt_id", "service_instance_id",
        "source_identity", "source_version", "epoch", "event_type", "emitted_at",
        "received_at", "local_sequence", "body_sha256", "body_bytes",
    }
    normalized: list[dict[str, Any]] = []
    event_ids: set[str] = set()
    sequences: set[int] = set()
    for event in events:
        if not isinstance(event, Mapping) or set(event) != expected_keys:
            raise ValueError("runtime event metadata differs from the exact projection")
        if (
            event.get("run_id") != str(identity.run_id)
            or event.get("attempt_id") != str(identity.attempt_id)
            or event.get("epoch") != identity.epoch
            or not isinstance(event.get("local_sequence"), int)
            or isinstance(event.get("local_sequence"), bool)
            or int(event["local_sequence"]) < 1
            or re.fullmatch(r"[a-f0-9]{64}", str(event.get("body_sha256", ""))) is None
            or not isinstance(event.get("body_bytes"), int)
            or isinstance(event.get("body_bytes"), bool)
            or int(event["body_bytes"]) < 1
        ):
            raise ValueError("runtime event metadata is not bound to the exact run/attempt/epoch")
        event_id = str(UUID(str(event["event_id"])))
        sequence = int(event["local_sequence"])
        if event_id in event_ids or sequence in sequences:
            raise ValueError("runtime event history contains duplicate identity/sequence")
        event_ids.add(event_id)
        sequences.add(sequence)
        normalized.append(dict(event))
    event_types = [str(item["event_type"]) for item in normalized]
    if "runtime.heartbeat" not in event_types or event_types.count("runtime.terminal") != 1:
        raise ValueError("runtime event history lacks heartbeat or one terminal callback")
    terminal = next(item for item in normalized if item["event_type"] == "runtime.terminal")
    if int(terminal["local_sequence"]) != max(sequences):
        raise ValueError("runtime terminal callback is not the final local event")
    return {
        "run_id": str(identity.run_id),
        "attempt_id": str(identity.attempt_id),
        "epoch": identity.epoch,
        "event_count": len(normalized),
        "heartbeat_count": event_types.count("runtime.heartbeat"),
        "terminal_event_id": terminal["event_id"],
        "history_sha256": _sha(normalized),
    }


async def _execute_evidence_scenario(
    request: DriverRequest,
    spec: ExecutorSpec,
    gateway: McpGateway,
    *,
    checks: list[CapabilityCheck],
    observations: Mapping[str, Any],
) -> TrustedDriverResult:
    try:
        config = _evidence_driver_config()
    except Exception as exc:
        if request.resume_only:
            return _failed(
                request,
                checks=[*checks, _check("evidence-config", "FAIL", "EVIDENCE_CONFIGURATION_INVALID")],
                observations={**observations, "failure_type": type(exc).__name__},
                mutations_started=1,
            )
        return _blocked(
            request,
            checks=[*checks, _check("evidence-config", "FAIL", "EVIDENCE_CONFIGURATION_INVALID")],
            code=f"{request.requirement_id}_EVIDENCE_CONFIGURATION_INVALID",
            dependency=f"valid bounded evidence driver configuration ({type(exc).__name__})",
            observations=observations,
        )
    if config is None or (request.requirement_id == "FM03" and config.fm03_runtime is None):
        return _blocked(
            request,
            checks=[*checks, _check("evidence-config", "BLOCKED", "EVIDENCE_CONFIGURATION_REQUIRED")],
            code=spec.gap_code,
            dependency=spec.gap_dependency,
            observations=observations,
        )

    proof: dict[str, object]
    mutable_observations = dict(observations)
    mutations_started = 0
    try:
        if request.requirement_id in {"FM01", "FM22"}:
            dataset_task, dataset_arguments = _dataset_arguments(request, config.provider_owner)
            dataset_claim = await _run_lifecycle(
                gateway,
                request,
                tool="provider.acceptance.dataset.lifecycle",
                task_id=dataset_task,
                arguments=dataset_arguments,
            )
            mutations_started = 1
            dataset_claim = _exact_claim(
                dataset_claim, request=request, task_id=dataset_task, cleanup_state="COMPLETE"
            )
            dataset = _one_claim_evidence(dataset_claim, "PROVIDER_DATASET")
            dataset_cleanup = _one_claim_evidence(dataset_claim, "CLEANUP")
            mutable_observations["dataset_claim_sha256"] = _sha(dataset_claim)
        if request.requirement_id == "FM01":
            proof = {
                "private_create": {"provider_ref": dataset["provider_ref"], "private": True},
                "exact_readback": {
                    "provider_version": dataset["provider_version"],
                    "package_sha256": dataset["package_sha256"],
                    "fingerprint": dataset["fingerprint"],
                },
                "claim_bound_delete": dataset_cleanup,
            }
        elif request.requirement_id == "FM02":
            proof = {
                "exact_source": {"matrix_id": str(request.matrix_id), "task_run_id": str(request.task_run_id)},
                "terminal_complete": {"controller": "provider.acceptance.notebook.lifecycle", "terminal": "complete"},
                "exact_output": {"file": request.result_file, "task_run_id": str(request.task_run_id)},
                "claim_bound_delete": {"phase": "outer-reconciled-cleanup", "task_run_id": str(request.task_run_id)},
            }
        elif request.requirement_id == "FM03":
            assert config.fm03_runtime is not None
            history_raw = await gateway.call(
                "operator",
                "runtime.events.history",
                {**config.fm03_runtime.model_dump(mode="json"), "limit": 200},
            )
            history = _validate_runtime_history(history_raw, config.fm03_runtime)
            mutable_observations["runtime_history"] = history
            proof = {
                "callback_bound": {key: history[key] for key in ("run_id", "attempt_id", "epoch", "history_sha256")},
                "heartbeat_observed": {
                    "heartbeat_count": history["heartbeat_count"],
                    "history_sha256": history["history_sha256"],
                },
                "terminal_event_bound": {
                    "terminal_event_id": history["terminal_event_id"],
                    "history_sha256": history["history_sha256"],
                },
            }
        elif request.requirement_id == "FM22":
            proof = {
                "private_dataset_lifecycle": dataset,
                "private_notebook_lifecycle": {
                    "controller": "provider.acceptance.notebook.lifecycle",
                    "task_run_id": str(request.task_run_id),
                },
                "exact_readback": {"dataset_claim_sha256": _sha(dataset), "result_file": request.result_file},
                "claim_cleanup": {"phase": "outer-reconciled-cleanup", "task_run_id": str(request.task_run_id)},
            }
        elif request.requirement_id == "FM23":
            probe = mutable_observations.get("protected_probe")
            provider = mutable_observations.get("provider")
            if not isinstance(probe, Mapping) or not isinstance(provider, Mapping) or (
                probe.get("evaluated") is not True
                or probe.get("protected") is not True
                or probe.get("denied") is not True
                or probe.get("reason_code") != "PROTECTED_RESOURCE_DENIED"
                or probe.get("mutation_attempted") is not False
            ):
                return _blocked(
                    request,
                    checks=[*checks, _check("protected-denial", "BLOCKED", "PROTECTED_DENIAL_PRECONDITION_UNMET")],
                    code="FM23_PROTECTED_DENIAL_PRECONDITION_UNMET",
                    dependency="registered orchestrator-protected resource and exact non-mutating denial probe",
                    observations=mutable_observations,
                )
            rows = provider.get("resources")
            selected = next(
                (
                    item
                    for item in rows
                    if isinstance(item, Mapping)
                    and item.get("control_class") == "orchestrator_protected"
                ),
                None,
            ) if isinstance(rows, list) else None
            if not isinstance(selected, Mapping):
                raise ValueError("protected resource selection is absent")
            if selected.get("resource_ref") != mutable_observations.get("protected_resource_ref"):
                raise ValueError("protected resource selection differs from the probed exact reference")
            proof = {
                "protected_resource_selected": {
                    "resource_ref": selected.get("resource_ref"),
                    "control_class": selected.get("control_class"),
                },
                "mutation_denied": dict(probe),
                "mutation_not_attempted": {"mutation_attempted": False, "reason_code": probe.get("reason_code")},
            }
        else:  # pragma: no cover - dispatch invariant
            raise ValueError("unsupported exact evidence scenario")

        output_document = _output_document(request, proof)
        notebook_task, notebook_arguments = _notebook_arguments(
            request, config.provider_owner, output_document
        )
        notebook_claim = await _run_lifecycle(
            gateway,
            request,
            tool="provider.acceptance.notebook.lifecycle",
            task_id=notebook_task,
            arguments=notebook_arguments,
        )
        mutations_started += 1
        locator, output = _notebook_binding(request, notebook_task, notebook_claim)
        expected_output_sha256 = notebook_arguments["expected_output_sha256"]
        if output.get("output_file_sha256") != expected_output_sha256:
            raise ValueError("durable output receipt differs from the exact generated scenario result")
        mutable_observations["notebook_claim_sha256"] = _sha(notebook_claim)
        checks.append(_check("evidence-notebook", "PASS", "EXACT_EVIDENCE_NOTEBOOK_READY", notebook_claim))
        return _ready(
            request,
            notebook_task,
            locator,
            output,
            output_document,
            checks=checks,
            observations=mutable_observations,
            mutations_started=mutations_started,
        )
    except MissingPreActionEvidence as exc:
        return _blocked(
            request,
            checks=[*checks, _check("evidence-resume", "BLOCKED", "DURABLE_EVIDENCE_CLAIM_MISSING")],
            code=f"{request.requirement_id}_DURABLE_EVIDENCE_CLAIM_MISSING",
            dependency=f"existing durable acceptance claim for resume ({type(exc).__name__})",
            observations=mutable_observations,
        )
    except Exception as exc:
        possible_mutations = (
            max(mutations_started, 1)
            if isinstance(exc, AmbiguousEvidenceMutation)
            else mutations_started
        )
        return _failed(
            request,
            checks=[*checks, _check("evidence-execution", "FAIL", "EVIDENCE_EXECUTION_OR_RECONCILIATION_FAILED")],
            observations={**mutable_observations, "failure_type": type(exc).__name__},
            mutations_started=possible_mutations,
        )


async def _execute_data_workload_scenario(
    request: DriverRequest,
    spec: ExecutorSpec,
    gateway: McpGateway,
    *,
    checks: list[CapabilityCheck],
) -> TrustedDriverResult:
    try:
        config = _evidence_driver_config()
    except Exception as exc:
        return _blocked(
            request,
            checks=[*checks, _check("data.config", "FAIL", "DATA_WORKLOAD_CONFIGURATION_INVALID")],
            code=f"{request.requirement_id}_DATA_WORKLOAD_CONFIGURATION_INVALID",
            dependency=f"valid bounded production data-workload configuration ({type(exc).__name__})",
        )
    if config is None or config.data_workload is None:
        return _blocked(
            request,
            checks=[*checks, _check("data.config", "BLOCKED", "DATA_WORKLOAD_CONFIGURATION_REQUIRED")],
            code=spec.gap_code,
            dependency=spec.gap_dependency,
        )
    try:
        prepared = _prepare_data_workload(request, config.data_workload)
    except MissingPreActionEvidence as exc:
        return _blocked(
            request,
            checks=[*checks, _check("data.state", "BLOCKED", "DURABLE_DATA_STATE_MISSING")],
            code=f"{request.requirement_id}_DURABLE_DATA_STATE_MISSING",
            dependency=f"same exact matrix-wide data-workload state ({type(exc).__name__})",
        )
    except Exception as exc:
        return _blocked(
            request,
            checks=[*checks, _check("data.preflight", "FAIL", "DATA_WORKLOAD_PREFLIGHT_INVALID")],
            code=f"{request.requirement_id}_DATA_WORKLOAD_PREFLIGHT_INVALID",
            dependency=(
                "owner-fixed paths, exact plan binding, and required production credentials "
                f"({type(exc).__name__})"
            ),
        )

    before = prepared.store.load(prepared.plan)
    checks.append(
        _check(
            "data.intent",
            "PASS",
            "DURABLE_MATRIX_DATA_INTENT_BOUND",
            {
                "matrix_id": str(before.matrix_id),
                "plan_sha256": before.plan_sha256,
                "phase": before.phase.value,
            },
        )
    )
    try:
        receipt = await asyncio.to_thread(_invoke_data_workload_entrypoint, prepared)
    except Exception as exc:
        after = prepared.store.load(prepared.plan)
        possible = max(
            after.mutations_started,
            1 if after.phase is not DataPhase.INITIAL else 0,
        )
        if possible == 0:
            return _blocked(
                request,
                checks=[*checks, _check("data.entrypoint", "BLOCKED", "DATA_WORKLOAD_ENTRYPOINT_UNAVAILABLE")],
                code=f"{request.requirement_id}_DATA_WORKLOAD_ENTRYPOINT_UNAVAILABLE",
                dependency=f"trusted production data-workload entrypoint ({type(exc).__name__})",
                observations={"state_sha256": _sha(after.model_dump(mode="json"))},
            )
        return _failed(
            request,
            checks=[*checks, _check("data.entrypoint", "FAIL", "DATA_WORKLOAD_RESPONSE_AMBIGUOUS")],
            observations={
                "state_sha256": _sha(after.model_dump(mode="json")),
                "phase": after.phase.value,
                "failure_type": type(exc).__name__,
            },
            mutations_started=possible,
        )

    after = prepared.store.load(prepared.plan)
    state_sha256 = _sha(after.model_dump(mode="json"))
    observations: dict[str, Any] = {
        "production_receipt_sha256": _sha(receipt.model_dump(mode="json", exclude_none=True)),
        "state_sha256": state_sha256,
        "phase": after.phase.value,
    }
    if receipt.matrix_id != request.matrix_id or receipt.state_sha256 != state_sha256:
        return _failed(
            request,
            checks=[*checks, _check("data.receipt", "FAIL", "DATA_WORKLOAD_RECEIPT_BINDING_INVALID")],
            observations=observations,
            mutations_started=max(after.mutations_started, 1),
        )
    checks.append(_check("data.receipt", "PASS", "DATA_WORKLOAD_RECEIPT_RECONCILED", observations))

    if receipt.outcome == "AWAITING_OWNER_AUTHORIZATION":
        if (
            after.phase is not DataPhase.AWAITING_OWNER_AUTHORIZATION
            or after.quarantine is None
            or after.duplicate_review is None
        ):
            return _failed(
                request,
                checks=[*checks, _check("data.owner", "FAIL", "OWNER_AUTHORIZATION_PAUSE_INVALID")],
                observations=observations,
                mutations_started=max(after.mutations_started, 1),
            )
        # This is an explicitly resumable, durably reconciled owner boundary.
        # BLOCKED/0 describes this invocation: the prior quarantine is fully
        # receipted in the shared state and no next mutation was attempted.
        return _blocked(
            request,
            checks=[*checks, _check("data.owner", "BLOCKED", "FM16_OWNER_AUTHORIZATION_REQUIRED")],
            code="FM16_AWAITING_OWNER_AUTHORIZATION",
            dependency="mode-0600 owner duplicate-resolution envelope; resume the same matrix and state",
            observations=observations,
        )
    if receipt.outcome == "BLOCKED":
        if after.phase is DataPhase.INITIAL and after.mutations_started == 0:
            return _blocked(
                request,
                checks=[*checks, _check("data.capability", "BLOCKED", "DATA_WORKLOAD_CAPABILITY_BLOCKED")],
                code=str(receipt.blocker_code),
                dependency="production data-workload capability before the first mutation",
                observations=observations,
            )
        return _failed(
            request,
            checks=[*checks, _check("data.capability", "FAIL", "POST_MUTATION_CAPABILITY_LOST")],
            observations={**observations, "blocker_code": receipt.blocker_code},
            mutations_started=max(after.mutations_started, 1),
        )
    if receipt.outcome != "EVIDENCE_READY":
        return _failed(
            request,
            checks=[*checks, _check("data.terminal", "FAIL", "DATA_WORKLOAD_NOT_EVIDENCE_READY")],
            observations={**observations, "failure_code": receipt.failure_code},
            mutations_started=max(after.mutations_started, 1),
        )
    if after.phase is not DataPhase.EVIDENCE_READY or after.mutations_started < 1:
        return _failed(
            request,
            checks=[*checks, _check("data.terminal", "FAIL", "DATA_WORKLOAD_TERMINAL_STATE_INVALID")],
            observations=observations,
            mutations_started=max(after.mutations_started, 1),
        )
    try:
        proofs, operation_ids, bundle_sha256 = _data_requirement_proof(request, receipt, after)
        output = _output_document(request, proofs, operation_ids=operation_ids)
    except Exception as exc:
        return _failed(
            request,
            checks=[*checks, _check("data.evidence", "FAIL", "DATA_WORKLOAD_EVIDENCE_INVALID")],
            observations={**observations, "failure_type": type(exc).__name__},
            mutations_started=after.mutations_started,
        )
    observations["data_workload_bundle_sha256"] = bundle_sha256
    checks.append(
        _check(
            "data.evidence",
            "PASS",
            "DATA_WORKLOAD_LIVE_TERMINAL_BOUND",
            receipt.evidence.model_dump(mode="json"),
        )
    )
    return await _launch_post_action_evidence(
        request,
        gateway,
        owner=config.provider_owner,
        output_document=output,
        checks=checks,
        observations=observations,
        mutations_started=after.mutations_started,
    )


_CHECKPOINT_STAGE_CONTRACT: dict[str, tuple[tuple[str, str, str], ...]] = {
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


def _bound_checkpoint_status(
    request: DriverRequest, status: CheckpointAcceptanceLaunchStatus
) -> CheckpointAcceptanceLaunchStatus:
    if (
        status.request_id != request.task_run_id
        or status.task_run_id != request.task_run_id
        or status.scenario != request.requirement_id
    ):
        raise ValueError("checkpoint status differs from the exact matrix task")
    return status


def _checkpoint_assertion_proofs(
    request: DriverRequest, status: CheckpointAcceptanceLaunchStatus
) -> dict[str, object]:
    result = status.result
    output = status.provider_output
    if (
        status.state != "LIVE_EVIDENCE_READY"
        or result is None
        or output is None
        or status.result_sha256 is None
        or result.scenario != request.requirement_id
        or result.task_run_id != request.task_run_id
        or result.source_revision != request.commit_sha
        or result.outcome != "LIVE_EVIDENCE_READY"
        or result.live_evidence is not True
        or result.verdict != "LIVE_PASS"
        or result.evidence_class != "live"
        or result.receipt is None
        or result.receipt_sha256 is None
        or output.provider_ref != result.locator.evidence_notebook_ref
        or output.output_file_sha256 != status.result_sha256
        or output.output_file_name != request.result_file
        or not status.official_adapter_observed
    ):
        raise ValueError("checkpoint launch status is not exact live evidence")
    expected = _CHECKPOINT_STAGE_CONTRACT[request.requirement_id]
    actual = tuple((item.stage, item.detail_code, item.outcome) for item in result.stages)
    if actual != expected or any(
        item.candidate_checkpoint_id != result.candidate_checkpoint_id for item in result.stages
    ):
        raise ValueError("checkpoint acceptance stages differ from the fixed scenario")
    receipt_hash = result.receipt_sha256
    stage = {item.stage: item.model_dump(mode="json") for item in result.stages}
    head = {
        "initial": result.initial_head.model_dump(mode="json") if result.initial_head else None,
        "final": result.final_head.model_dump(mode="json") if result.final_head else None,
        "receipt_sha256": receipt_hash,
    }
    if request.requirement_id == "FM05":
        if (
            result.head_unchanged is not False
            or result.initial_head is None
            or result.final_head is None
            or result.candidate_checkpoint_id is None
            or result.final_head.generation != result.initial_head.generation + 1
            or result.final_head.current_checkpoint_id != result.candidate_checkpoint_id
            or result.final_head.previous_checkpoint_id != result.initial_head.current_checkpoint_id
        ):
            raise ValueError("FM05 did not advance the exact checkpoint HEAD")
        proofs = {
            "candidate_uploaded": {
                "empty_candidate": stage["empty_candidate"],
                "private_upload": stage["private_upload"],
            },
            "exact_readback": stage["exact_readback"],
            "restore_verified": stage["independent_restore"],
            "head_advanced": {**head, "cas_promotion": stage["cas_promotion"]},
        }
    elif request.requirement_id == "FM14":
        if result.head_unchanged is not True or result.initial_head != result.final_head:
            raise ValueError("FM14 changed checkpoint HEAD")
        proofs = {
            "corrupt_candidate_uploaded": stage["corrupted_candidate"],
            "hash_mismatch_detected": stage["hash_mismatch_rejection"],
            "old_head_unchanged": head,
        }
    else:
        if result.head_unchanged is not True or result.initial_head != result.final_head:
            raise ValueError("FM15 changed checkpoint HEAD")
        proofs = {
            "restore_smoke_forced_failure": stage["forced_restore_rejection"],
            "candidate_rejected": {
                "candidate": stage["restore_failure_candidate"],
                "rejection": stage["forced_restore_rejection"],
            },
            "old_head_unchanged": head,
        }
    if set(proofs) != set(request.required_assertions):
        raise ValueError("checkpoint evidence proofs differ from the scenario catalog")
    return proofs


def _checkpoint_driver_pass(
    request: DriverRequest,
    status: CheckpointAcceptanceLaunchStatus,
    *,
    checks: list[CapabilityCheck],
    observations: Mapping[str, Any],
    proofs: Mapping[str, object],
) -> TrustedDriverResult:
    output = status.provider_output
    result = status.result
    assert output is not None and result is not None
    scenario_output = _output_document(
        request,
        proofs,
        operation_ids=(str(result.operation_id),),
    )
    scenario_output["completed_at"] = result.completed_at.isoformat()
    return TrustedDriverResult(
        schema_version="my-data-hub-operational-kaggle-driver-result.v2",
        phase="EXECUTE",
        outcome="PASS",
        scenario=request.scenario,
        task_run_id=request.task_run_id,
        provider_ref=output.provider_ref,
        provider_run_ref=output.provider_run_ref,
        provider_kernel_id=output.provider_kernel_id,
        source_version=output.source_version,
        source_sha256=output.source_sha256,
        mutations_started=result.mutations_started,
        capability_checks=tuple(checks),
        observation_sha256=_sha(dict(observations)),
        claim_task_id=request.task_run_id,
        claim_sha256=output.provider_claim_sha256,
        output_receipt_sha256=output.output_receipt_sha256,
        output_file_sha256=output.output_file_sha256,
        output_tree_sha256=output.output_tree_sha256,
        cleanup_state="NOT_REQUIRED",
        scenario_output=DriverScenarioOutput.model_validate(scenario_output),
    )


def _master_status_identity(
    request: DriverRequest,
    value: Mapping[str, Any],
    *,
    target_operation_id: UUID | None,
) -> None:
    common_invalid = (
        value.get("found") is not True
        or value.get("bounded") is not True
        or value.get("task_id") != str(request.task_run_id)
        or value.get("scenario_id") != request.requirement_id
        or value.get("source_revision") != request.commit_sha
        or value.get("state") not in {"PENDING", "BOUND", "CLAIMED", "PASSED", "FAILED"}
    )
    if common_invalid:
        raise ValueError("master acceptance status differs from its exact task binding")
    raw_operation_id = value.get("operation_id")
    raw_target_operation_id = value.get("target_operation_id")
    if request.requirement_id in PREBOOT_MASTER_ACCEPTANCE_REQUIREMENTS:
        if raw_operation_id != raw_target_operation_id:
            raise ValueError("preboot acceptance status has inconsistent operation identities")
        if raw_operation_id is not None:
            UUID(str(raw_operation_id))
        if target_operation_id is not None and raw_operation_id != str(target_operation_id):
            raise ValueError("preboot acceptance status differs from its bound operation")
    elif (
        target_operation_id is None
        or raw_operation_id != str(target_operation_id)
        or raw_target_operation_id != str(target_operation_id)
    ):
        raise ValueError("master acceptance status differs from its active target")


def _terminal_master_evidence(
    request: DriverRequest,
    value: Mapping[str, Any],
    *,
    target_operation_id: UUID | None,
) -> tuple[MasterAcceptanceReceipt, MasterProviderCarrierObservation]:
    _master_status_identity(request, value, target_operation_id=target_operation_id)
    if value.get("state") != "PASSED" or value.get("failure_code") is not None:
        raise ValueError("master acceptance status is not a successful terminal task")
    command = MasterCommandProjection.model_validate(value.get("command"))
    receipt = command.receipt
    bound_operation_id = UUID(str(value.get("target_operation_id")))
    if (
        receipt.task_id != request.task_run_id
        or receipt.scenario.value != request.requirement_id
        or receipt.binding.operation_id != bound_operation_id
        or (target_operation_id is not None and bound_operation_id != target_operation_id)
    ):
        raise ValueError("typed master acceptance receipt differs from the matrix task")
    carrier = MasterProviderCarrierObservation.model_validate(value.get("provider_carrier"))
    output_identity = (
        carrier.output_file_name,
        carrier.output_file_sha256,
        carrier.output_tree_sha256,
        carrier.output_receipt_sha256,
    )
    if (
        request.requirement_id in TERMINAL_OUTPUT_MASTER_ACCEPTANCE_REQUIREMENTS
        and any(value is None for value in output_identity)
    ):
        raise ValueError("stopped master lacks its terminal output receipt")
    if request.requirement_id not in TERMINAL_OUTPUT_MASTER_ACCEPTANCE_REQUIREMENTS and any(
        value is not None for value in output_identity
    ):
        raise ValueError("non-terminal carrier unexpectedly contains terminal output")
    return receipt, carrier


def _master_evidence_is_exact(request: DriverRequest, receipt: MasterAcceptanceReceipt) -> None:
    evidence = receipt.evidence
    if request.requirement_id == "FM04":
        if not isinstance(evidence, EmptyBootstrapEvidence):
            raise ValueError("FM04 receipt lacks typed empty-bootstrap evidence")
    elif request.requirement_id == "FM07":
        if not isinstance(evidence, ConcurrentEnsureEvidence):
            raise ValueError("FM07 receipt lacks typed concurrent-ensure evidence")
    elif request.requirement_id == "FM08":
        if not isinstance(evidence, CallbackLossEvidence):
            raise ValueError("FM08 receipt lacks typed callback-loss evidence")
    elif request.requirement_id == "FM09":
        if not isinstance(evidence, StaleReplayEvidence):
            raise ValueError("FM09 receipt lacks typed stale-replay evidence")
    elif request.requirement_id == "FM10":
        if not isinstance(evidence, LeaseExpiryEvidence):
            raise ValueError("FM10 receipt lacks typed lease-expiry evidence")
    elif request.requirement_id == "FM11":
        if not isinstance(evidence, OldEpochEvidence):
            raise ValueError("FM11 receipt lacks typed old-epoch evidence")
    elif request.requirement_id == "FM12":
        if not isinstance(evidence, CleanDrainEvidence):
            raise ValueError("FM12 receipt lacks typed clean-drain evidence")
    elif request.requirement_id == "FM24":
        if not isinstance(evidence, RotationSoakEvidence):
            raise ValueError("FM24 receipt lacks typed soak evidence")
        required = (
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
        if any(value is None for value in required):
            raise ValueError("FM24 receipt lacks exact continuity/read/checkpoint/recovery evidence")
    else:  # pragma: no cover - closed dispatch invariant
        raise ValueError("unsupported master acceptance scenario")


def _master_driver_pass(
    request: DriverRequest,
    receipt: MasterAcceptanceReceipt,
    carrier: MasterProviderCarrierObservation,
    *,
    checks: list[CapabilityCheck],
    observations: Mapping[str, Any],
    terminal_master: Mapping[str, Any],
    terminal_checkpoint: Mapping[str, Any] | None = None,
    phase: Literal["EXECUTE", "RECONCILE"] = "EXECUTE",
) -> TrustedDriverResult:
    _master_evidence_is_exact(request, receipt)
    evidence = receipt.evidence
    if isinstance(evidence, CleanDrainEvidence):
        if not _exact_absent_master(terminal_master):
            raise ValueError("FM12 terminal master projection is not exact ABSENT state")
        if (
            not isinstance(terminal_checkpoint, Mapping)
            or terminal_checkpoint.get("current_checkpoint_id") != str(evidence.checkpoint_id)
            or terminal_checkpoint.get("current_exact_version_ref") != evidence.exact_version_ref
        ):
            raise ValueError("FM12 verified checkpoint differs from the typed clean-drain receipt")
        terminal_operation_id = receipt.binding.operation_id
        terminal_ref = carrier.provider_run_ref
        terminal_epoch = receipt.binding.epoch
    else:
        if not _exact_active_master(terminal_master):
            raise ValueError("terminal master projection is not exact ACTIVE state")
        terminal_operation_id = UUID(str(terminal_master.get("operation_id")))
        terminal_ref = terminal_master.get("provider_run_ref")
        terminal_epoch = terminal_master.get("master_epoch")
        terminal_kernel_id = terminal_master.get("provider_kernel_id")
        if not isinstance(terminal_ref, str) or not terminal_ref:
            raise ValueError("terminal master lacks exact provider run")
        if isinstance(evidence, CallbackLossEvidence):
            if (
                receipt.binding.operation_id != evidence.old_operation_id
                or carrier.provider_run_ref != evidence.old_provider_run_ref
                or carrier.provider_kernel_id != evidence.old_provider_kernel_id
                or terminal_operation_id != evidence.new_operation_id
                or terminal_epoch != evidence.new_epoch
                or terminal_ref != evidence.new_provider_run_ref
                or terminal_kernel_id != evidence.new_provider_kernel_id
            ):
                raise ValueError("FM08 terminal master/carrier differs from recovery evidence")
        elif isinstance(evidence, OldEpochEvidence):
            if (
                receipt.binding.operation_id != evidence.old_operation_id
                or carrier.provider_run_ref != evidence.old_provider_run_ref
                or carrier.provider_kernel_id != evidence.old_provider_kernel_id
                or terminal_operation_id != evidence.new_operation_id
                or terminal_epoch != evidence.new_epoch
                or terminal_ref != evidence.new_provider_run_ref
                or terminal_kernel_id != evidence.new_provider_kernel_id
            ):
                raise ValueError("FM11 terminal master differs from replacement evidence")
        else:
            if (
                terminal_operation_id != receipt.binding.operation_id
                or terminal_epoch != receipt.binding.epoch
                or terminal_ref != carrier.provider_run_ref
                or terminal_kernel_id != carrier.provider_kernel_id
            ):
                raise ValueError("terminal ACTIVE master differs from its typed carrier binding")
            if isinstance(evidence, EmptyBootstrapEvidence) and (
                terminal_master.get("canonical_revision") != evidence.canonical_revision
                or evidence.canonical_row_count != 0
                or evidence.service_active is not True
            ):
                raise ValueError("FM04 ACTIVE master differs from empty-bootstrap evidence")
            if isinstance(evidence, ConcurrentEnsureEvidence) and (
                set(evidence.operation_ids) != {receipt.binding.operation_id}
                or set(evidence.provider_run_refs) != {carrier.provider_run_ref}
                or set(evidence.provider_kernel_ids) != {carrier.provider_kernel_id}
                or set(evidence.epochs) != {receipt.binding.epoch}
            ):
                raise ValueError("FM07 convergence evidence differs from its exact carrier binding")
    lifecycle: tuple[DriverControlLifecycle, ...] = ()
    if isinstance(evidence, CallbackLossEvidence):
        lifecycle = (
            DriverControlLifecycle(
                gate="abrupt_master_termination",
                operation_id=evidence.old_operation_id,
                old_provider_run_ref=evidence.old_provider_run_ref,
                new_provider_run_ref=terminal_ref,
            ),
            DriverControlLifecycle(
                gate="control_plane_restart",
                operation_id=evidence.old_operation_id,
                before_identity=evidence.control_boot_id_before,
                after_identity=evidence.control_boot_id_after,
            ),
        )
    elif isinstance(evidence, OldEpochEvidence):
        lifecycle = (
            DriverControlLifecycle(
                gate="clean_rotation",
                operation_id=evidence.new_operation_id,
                old_provider_run_ref=evidence.old_provider_run_ref,
                new_provider_run_ref=evidence.new_provider_run_ref,
                old_epoch=evidence.old_epoch,
                new_epoch=evidence.new_epoch,
            ),
        )
    return TrustedDriverResult(
        schema_version="my-data-hub-operational-kaggle-driver-result.v2",
        phase=phase,
        outcome="PASS",
        scenario=request.scenario,
        task_run_id=request.task_run_id,
        provider_ref=carrier.provider_ref,
        provider_run_ref=carrier.provider_run_ref,
        provider_kernel_id=carrier.provider_kernel_id,
        source_version=carrier.source_version,
        source_sha256=carrier.source_sha256,
        mutations_started=1,
        capability_checks=tuple(checks),
        observation_sha256=_sha(dict(observations)),
        output_receipt_sha256=carrier.output_receipt_sha256,
        output_file_sha256=carrier.output_file_sha256,
        output_tree_sha256=carrier.output_tree_sha256,
        cleanup_state="NOT_REQUIRED",
        control_receipt=receipt,
        control_lifecycle=lifecycle,
    )


async def _execute_master_acceptance_scenario(
    request: DriverRequest,
    spec: ExecutorSpec,
    gateway: McpGateway,
    *,
    checks: list[CapabilityCheck],
    observations: Mapping[str, Any],
) -> TrustedDriverResult:
    raw_master = observations.get("master")
    preboot = request.requirement_id in PREBOOT_MASTER_ACCEPTANCE_REQUIREMENTS
    target_operation_id: UUID | None = None
    task_id = request.task_run_id
    try:
        observed = await gateway.call(
            "operator", "acceptance.scenario.status", {"task_id": str(task_id)}
        )
    except Exception as exc:
        if request.resume_only:
            return _failed(
                request,
                checks=[*checks, _check("acceptance.status", "FAIL", "ACCEPTANCE_STATUS_UNAVAILABLE")],
                observations={**observations, "failure_type": type(exc).__name__},
                mutations_started=1,
            )
        return _blocked(
            request,
            checks=[*checks, _check("acceptance.status", "BLOCKED", "ACCEPTANCE_STATUS_UNAVAILABLE")],
            code=spec.gap_code,
            dependency=f"{spec.gap_dependency} ({type(exc).__name__})",
            observations=observations,
        )
    status: Mapping[str, Any] | None = None
    if observed == {"found": False}:
        if request.resume_only:
            return _failed(
                request,
                checks=[*checks, _check("acceptance.resume", "FAIL", "ACCEPTANCE_TASK_MISSING_ON_RESUME")],
                observations={**observations, "status_sha256": _sha(observed)},
                mutations_started=1,
            )
        precondition_satisfied = isinstance(raw_master, Mapping) and (
            _exact_absent_master(raw_master) if preboot else _exact_active_master(raw_master)
        )
        if not precondition_satisfied:
            state = "ABSENT" if preboot else "ACTIVE"
            return _blocked(
                request,
                checks=[*checks, _check("master.target", "BLOCKED", f"{state}_MASTER_TARGET_REQUIRED")],
                code=f"{request.requirement_id}_{state}_MASTER_TARGET_REQUIRED",
                dependency=f"exact {state} master.status precondition before acceptance mutation",
                observations=observations,
            )
        if not preboot:
            try:
                target_operation_id = UUID(str(raw_master.get("operation_id")))
            except (TypeError, ValueError, AttributeError):
                return _blocked(
                    request,
                    checks=[*checks, _check("master.target", "BLOCKED", "ACTIVE_MASTER_OPERATION_ID_MISSING")],
                    code=f"{request.requirement_id}_ACTIVE_MASTER_OPERATION_ID_MISSING",
                    dependency="ACTIVE master.status with exact operation_id",
                    observations=observations,
                )
    elif observed.get("found") is True:
        try:
            if not preboot:
                target_operation_id = UUID(str(observed.get("target_operation_id")))
            _master_status_identity(request, observed, target_operation_id=target_operation_id)
        except Exception as exc:
            return _failed(
                request,
                checks=[*checks, _check("acceptance.status", "FAIL", "ACCEPTANCE_STATUS_INVALID")],
                observations={**observations, "status_sha256": _sha(observed), "failure_type": type(exc).__name__},
                mutations_started=1,
            )
        status = observed
    else:
        return _failed(
            request,
            checks=[*checks, _check("acceptance.status", "FAIL", "ACCEPTANCE_STATUS_INVALID")],
            observations={**observations, "status_sha256": _sha(observed)},
            mutations_started=1,
        )
    if status is None:
        arguments = {
            "task_id": str(task_id),
            "scenario": request.requirement_id,
            "idempotency_key": f"operational:{request.matrix_id}:{request.requirement_id}:master",
            "source_revision": request.commit_sha,
        }
        if target_operation_id is not None:
            arguments["target_operation_id"] = str(target_operation_id)
        try:
            launched = await gateway.call("operator", "acceptance.scenario.request", arguments)
        except Exception as exc:
            try:
                launched = await gateway.call(
                    "operator", "acceptance.scenario.status", {"task_id": str(task_id)}
                )
                _master_status_identity(request, launched, target_operation_id=target_operation_id)
            except Exception as reconcile_exc:
                return _failed(
                    request,
                    checks=[*checks, _check("acceptance.request", "FAIL", "ACCEPTANCE_REQUEST_AMBIGUOUS")],
                    observations={
                        **observations,
                        "failure_type": type(exc).__name__,
                        "reconcile_failure_type": type(reconcile_exc).__name__,
                    },
                    mutations_started=1,
                )
        else:
            try:
                _master_status_identity(request, launched, target_operation_id=target_operation_id)
            except Exception as exc:
                return _failed(
                    request,
                    checks=[*checks, _check("acceptance.request", "FAIL", "ACCEPTANCE_REQUEST_INVALID")],
                    observations={
                        **observations,
                        "response_sha256": _sha(launched),
                        "failure_type": type(exc).__name__,
                    },
                    mutations_started=1,
                )
        status = launched
    deadline_seconds = (
        MASTER_ACCEPTANCE_SOAK_TIMEOUT_SECONDS
        if request.requirement_id == "FM24"
        else MASTER_ACCEPTANCE_TIMEOUT_SECONDS
    )
    deadline = asyncio.get_running_loop().time() + deadline_seconds
    assert status is not None
    while status.get("state") not in MASTER_ACCEPTANCE_TERMINAL_STATES:
        if asyncio.get_running_loop().time() >= deadline:
            return _failed(
                request,
                checks=[*checks, _check("acceptance.poll", "FAIL", "ACCEPTANCE_SCENARIO_TIMEOUT")],
                observations={**observations, "task_id": str(task_id), "state": status.get("state")},
                mutations_started=1,
            )
        await asyncio.sleep(MASTER_ACCEPTANCE_POLL_SECONDS)
        try:
            status = await gateway.call(
                "operator", "acceptance.scenario.status", {"task_id": str(task_id)}
            )
            _master_status_identity(request, status, target_operation_id=target_operation_id)
        except Exception as exc:
            return _failed(
                request,
                checks=[*checks, _check("acceptance.poll", "FAIL", "ACCEPTANCE_STATUS_RECONCILIATION_FAILED")],
                observations={**observations, "failure_type": type(exc).__name__},
                mutations_started=1,
            )
    if status.get("state") == "FAILED":
        return _failed(
            request,
            checks=[*checks, _check("acceptance.terminal", "FAIL", "ACCEPTANCE_SCENARIO_FAILED")],
            observations={**observations, "failure_code": status.get("failure_code")},
            mutations_started=1,
        )
    try:
        receipt, carrier = _terminal_master_evidence(
            request, status, target_operation_id=target_operation_id
        )
        terminal_master = await gateway.call("reader", "master.status", {})
        terminal_checkpoint = (
            await gateway.call("reader", "checkpoint.status", {})
            if request.requirement_id == "FM12"
            else None
        )
        return _master_driver_pass(
            request,
            receipt,
            carrier,
            checks=[*checks, _check("acceptance.terminal", "PASS", "TYPED_CONTROL_RECEIPT_BOUND", status)],
            observations={
                **observations,
                "terminal_status_sha256": _sha(status),
                "terminal_master_sha256": _sha(terminal_master),
            },
            terminal_master=terminal_master,
            terminal_checkpoint=terminal_checkpoint,
        )
    except Exception as exc:
        return _failed(
            request,
            checks=[*checks, _check("acceptance.terminal", "FAIL", "TYPED_CONTROL_RECEIPT_INVALID")],
            observations={**observations, "status_sha256": _sha(status), "failure_type": type(exc).__name__},
            mutations_started=1,
        )


async def _execute_checkpoint_acceptance_scenario(
    request: DriverRequest,
    spec: ExecutorSpec,
    gateway: McpGateway,
    *,
    checks: list[CapabilityCheck],
) -> TrustedDriverResult:
    task_id = request.task_run_id
    try:
        observed = await gateway.call(
            "operator", "acceptance.scenario.status", {"task_id": str(task_id)}
        )
    except Exception:
        return _blocked(
            request,
            checks=[*checks, _check("checkpoint.status", "BLOCKED", "CHECKPOINT_STATUS_UNAVAILABLE")],
            code=spec.gap_code,
            dependency=spec.gap_dependency,
        )
    status: CheckpointAcceptanceLaunchStatus | None = None
    if observed.get("found") is True:
        try:
            status = _bound_checkpoint_status(
                request, CheckpointAcceptanceLaunchStatus.model_validate(observed)
            )
        except Exception as exc:
            return _failed(
                request,
                checks=[*checks, _check("checkpoint.status", "FAIL", "CHECKPOINT_STATUS_INVALID")],
                observations={"failure_type": type(exc).__name__, "status_sha256": _sha(observed)},
                mutations_started=1,
            )
    elif observed != {"found": False}:
        return _failed(
            request,
            checks=[*checks, _check("checkpoint.status", "FAIL", "CHECKPOINT_STATUS_INVALID")],
            observations={"status_sha256": _sha(observed)},
            mutations_started=1,
        )
    elif request.resume_only:
        return _failed(
            request,
            checks=[*checks, _check("checkpoint.resume", "FAIL", "CHECKPOINT_TASK_MISSING_ON_RESUME")],
            observations={"status_sha256": _sha(observed)},
            mutations_started=1,
        )
    if status is None:
        arguments = {
            "task_id": str(task_id),
            "scenario": request.requirement_id,
            "idempotency_key": f"operational:{request.matrix_id}:{request.requirement_id}:checkpoint",
            "source_revision": request.commit_sha,
        }
        try:
            launched = await gateway.call("operator", "acceptance.scenario.request", arguments)
        except Exception as exc:
            try:
                reconciled = await gateway.call(
                    "operator", "acceptance.scenario.status", {"task_id": str(task_id)}
                )
                status = _bound_checkpoint_status(
                    request, CheckpointAcceptanceLaunchStatus.model_validate(reconciled)
                )
            except Exception as reconcile_exc:
                return _failed(
                    request,
                    checks=[*checks, _check("checkpoint.request", "FAIL", "CHECKPOINT_REQUEST_AMBIGUOUS")],
                    observations={
                        "failure_type": type(exc).__name__,
                        "reconcile_failure_type": type(reconcile_exc).__name__,
                    },
                    mutations_started=1,
                )
        else:
            try:
                status = _bound_checkpoint_status(
                    request, CheckpointAcceptanceLaunchStatus.model_validate(launched)
                )
            except Exception as exc:
                return _failed(
                    request,
                    checks=[*checks, _check("checkpoint.request", "FAIL", "CHECKPOINT_REQUEST_INVALID")],
                    observations={"response_sha256": _sha(launched), "failure_type": type(exc).__name__},
                    mutations_started=1,
                )
    assert status is not None
    deadline = asyncio.get_running_loop().time() + CHECKPOINT_ACCEPTANCE_TIMEOUT_SECONDS
    while status.state not in CHECKPOINT_ACCEPTANCE_TERMINAL_STATES:
        if asyncio.get_running_loop().time() >= deadline:
            return _failed(
                request,
                checks=[*checks, _check("checkpoint.poll", "FAIL", "CHECKPOINT_ACCEPTANCE_TIMEOUT")],
                observations={"task_id": str(task_id), "state": status.state},
                mutations_started=1,
            )
        await asyncio.sleep(CHECKPOINT_ACCEPTANCE_POLL_SECONDS)
        try:
            polled = await gateway.call(
                "operator", "acceptance.scenario.status", {"task_id": str(task_id)}
            )
            status = _bound_checkpoint_status(
                request, CheckpointAcceptanceLaunchStatus.model_validate(polled)
            )
        except Exception as exc:
            return _failed(
                request,
                checks=[*checks, _check("checkpoint.poll", "FAIL", "CHECKPOINT_STATUS_RECONCILIATION_FAILED")],
                observations={"task_id": str(task_id), "failure_type": type(exc).__name__},
                mutations_started=1,
            )
    observations = {
        "task_id": str(task_id),
        "operation_id": str(status.operation_id),
        "request_sha256": status.request_sha256,
        "config_sha256": status.config_sha256,
        "state": status.state,
    }
    if status.state == "BLOCKED":
        return _blocked(
            request,
            checks=[*checks, _check("checkpoint.terminal", "BLOCKED", "CHECKPOINT_PREFLIGHT_BLOCKED")],
            code=str(status.blocker_code),
            dependency="fixed owner checkpoint assets and official launch capability",
            observations=observations,
        )
    if status.state == "FAIL":
        mutations = status.result.mutations_started if status.result is not None else 1
        return _failed(
            request,
            checks=[*checks, _check("checkpoint.terminal", "FAIL", "CHECKPOINT_ACCEPTANCE_FAILED")],
            observations={**observations, "failure_code": status.failure_code},
            mutations_started=max(mutations, 1),
        )
    try:
        proofs = _checkpoint_assertion_proofs(request, status)
    except Exception as exc:
        return _failed(
            request,
            checks=[*checks, _check("checkpoint.evidence", "FAIL", "CHECKPOINT_EVIDENCE_INVALID")],
            observations={**observations, "failure_type": type(exc).__name__},
            mutations_started=max(status.result.mutations_started if status.result else 0, 1),
        )
    observations["assertion_proofs_sha256"] = _sha(proofs)
    checks.append(_check("checkpoint.evidence", "PASS", "CHECKPOINT_LIVE_EVIDENCE_READY", proofs))
    return _checkpoint_driver_pass(
        request,
        status,
        checks=checks,
        observations=observations,
        proofs=proofs,
    )


async def _launch_post_action_evidence(
    request: DriverRequest,
    gateway: McpGateway,
    *,
    owner: str,
    output_document: Mapping[str, Any],
    checks: list[CapabilityCheck],
    observations: Mapping[str, Any],
    mutations_started: int,
) -> TrustedDriverResult:
    notebook_task, notebook_arguments = _notebook_arguments(request, owner, output_document)
    try:
        notebook_claim = await _run_lifecycle(
            gateway,
            request,
            tool="provider.acceptance.notebook.lifecycle",
            task_id=notebook_task,
            arguments=notebook_arguments,
            allow_create_on_resume=True,
        )
        locator, output = _notebook_binding(request, notebook_task, notebook_claim)
        if output.get("output_file_sha256") != notebook_arguments["expected_output_sha256"]:
            raise ValueError("durable output receipt differs from the generated post-action result")
    except Exception as exc:
        return _failed(
            request,
            checks=[*checks, _check("evidence-notebook", "FAIL", "POST_ACTION_EVIDENCE_RECONCILIATION_FAILED")],
            observations={**dict(observations), "failure_type": type(exc).__name__},
            mutations_started=max(mutations_started, 1),
        )
    checks.append(_check("evidence-notebook", "PASS", "EXACT_POST_ACTION_EVIDENCE_READY", notebook_claim))
    return _ready(
        request,
        notebook_task,
        locator,
        output,
        output_document,
        checks=checks,
        observations={**dict(observations), "notebook_claim_sha256": _sha(notebook_claim)},
        mutations_started=mutations_started + 1,
    )


def _binding_matches_result(binding: CleanupBinding, result: TrustedDriverResult) -> bool:
    return (
        binding.claim_task_id == result.claim_task_id
        and binding.claim_sha256 == result.claim_sha256
        and binding.provider_ref == result.provider_ref
        and binding.provider_run_ref == result.provider_run_ref
        and binding.provider_kernel_id == result.provider_kernel_id
        and binding.source_version == result.source_version
        and binding.source_sha256 == result.source_sha256
        and binding.output_receipt_sha256 == result.output_receipt_sha256
        and binding.output_file_sha256 == result.output_file_sha256
        and binding.output_tree_sha256 == result.output_tree_sha256
    )


async def _execute_reconcile(
    request: DriverRequest, gateway: McpGateway
) -> TrustedDriverResult:
    binding = request.cleanup
    assert binding is not None
    if request.requirement_id in MASTER_ACCEPTANCE_REQUIREMENTS:
        try:
            catalog = await gateway.catalog("operator")
            if "acceptance.scenario.status" not in catalog:
                raise ValueError("acceptance scenario status tool is absent")
            reader_catalog = await gateway.catalog("reader")
            if "master.status" not in reader_catalog:
                raise ValueError("master status tool is absent")
            if request.requirement_id == "FM12" and "checkpoint.status" not in reader_catalog:
                raise ValueError("checkpoint status tool is absent")
            status = await gateway.call(
                "operator", "acceptance.scenario.status", {"task_id": str(request.task_run_id)}
            )
            raw_operation_id = status.get("target_operation_id")
            target_operation_id = UUID(str(raw_operation_id))
            receipt, carrier = _terminal_master_evidence(
                request, status, target_operation_id=target_operation_id
            )
            terminal_master = await gateway.call("reader", "master.status", {})
            terminal_checkpoint = (
                await gateway.call("reader", "checkpoint.status", {})
                if request.requirement_id == "FM12"
                else None
            )
            result = _master_driver_pass(
                request,
                receipt,
                carrier,
                checks=[_check("reconcile.acceptance", "PASS", "TYPED_CONTROL_RECEIPT_RECONCILED", status)],
                observations={"status_sha256": _sha(status)},
                terminal_master=terminal_master,
                terminal_checkpoint=terminal_checkpoint,
                phase="RECONCILE",
            )
            if not _binding_matches_result(binding, result):
                raise ValueError("reconciled master carrier differs from the execute binding")
            return result
        except Exception as exc:
            return _failed(
                request,
                checks=[_check("reconcile.acceptance", "FAIL", "CONTROL_RECEIPT_RECONCILIATION_FAILED")],
                observations={"failure_type": type(exc).__name__},
                mutations_started=1,
            )

    try:
        catalog = await gateway.catalog("provider")
        if "provider.acceptance.claim.get" not in catalog:
            raise ValueError("provider claim status tool is absent")
        if binding.claim_task_id is None or binding.claim_sha256 is None:
            raise ValueError("provider reconciliation lacks a claim identity")
        claim = await _claim_get(gateway, request, binding.claim_task_id)
        cleanup_state = claim.get("cleanup_state")
        if cleanup_state not in {"PENDING", "COMPLETE"}:
            raise ValueError("provider claim lacks an exact cleanup state")
        locator, output = _notebook_binding(
            request, binding.claim_task_id, claim, cleanup_state=cleanup_state
        )
        result = TrustedDriverResult(
            schema_version="my-data-hub-operational-kaggle-driver-result.v2",
            phase="RECONCILE",
            outcome="PASS",
            scenario=request.scenario,
            task_run_id=request.task_run_id,
            provider_ref=locator.provider_ref,
            provider_run_ref=locator.provider_run_ref,
            provider_kernel_id=locator.provider_kernel_id,
            source_version=locator.source_version,
            source_sha256=locator.source_sha256,
            mutations_started=1,
            capability_checks=(
                _check("reconcile.provider", "PASS", "PROVIDER_CLAIM_RECONCILED", claim),
            ),
            observation_sha256=_sha(claim),
            claim_task_id=binding.claim_task_id,
            claim_sha256=locator.claim_sha256,
            output_receipt_sha256=str(output["output_receipt_sha256"]),
            output_file_sha256=str(output["output_file_sha256"]),
            output_tree_sha256=str(output["output_tree_sha256"]),
            cleanup_state="PENDING" if cleanup_state == "PENDING" else "COMPLETE",
        )
        if not _binding_matches_result(binding, result):
            raise ValueError("reconciled provider claim differs from the execute binding")
        return result
    except Exception as exc:
        return _failed(
            request,
            checks=[_check("reconcile.provider", "FAIL", "PROVIDER_CLAIM_RECONCILIATION_FAILED")],
            observations={"failure_type": type(exc).__name__},
            mutations_started=1,
        )


async def _execute_cleanup(
    request: DriverRequest, gateway: McpGateway
) -> TrustedDriverResult:
    binding = request.cleanup
    assert binding is not None
    assert binding.claim_task_id is not None and binding.claim_sha256 is not None
    checks: list[CapabilityCheck] = []
    try:
        catalog = await gateway.catalog("provider")
    except MissingCredential:
        return _failed(
            request,
            checks=[_check("credential.provider", "FAIL", "PROVIDER_MCP_TOKEN_MISSING_AFTER_MUTATION")],
            observations={"claim_task_id": str(binding.claim_task_id)},
            mutations_started=1,
        )
    except Exception as exc:
        return _failed(
            request,
            checks=[_check("credential.provider", "FAIL", "PROVIDER_MCP_CATALOG_UNAVAILABLE_AFTER_MUTATION")],
            observations={"failure_type": type(exc).__name__},
            mutations_started=1,
        )
    required = {"provider.acceptance.claim.get", "provider.acceptance.claim.cleanup"}
    if not required <= catalog:
        return _failed(
            request,
            checks=[_check("catalog.provider", "FAIL", "CLEANUP_MCP_TOOLSET_INCOMPLETE", sorted(catalog))],
            observations={"missing": sorted(required - catalog)},
            mutations_started=1,
        )
    try:
        before = await _claim_get(gateway, request, binding.claim_task_id)
        before_cleanup_state = before.get("cleanup_state")
        if before_cleanup_state not in {"PENDING", "COMPLETE"}:
            raise ValueError("durable acceptance claim has no exact cleanup state")
        locator, output = _notebook_binding(
            request,
            binding.claim_task_id,
            before,
            cleanup_state=before_cleanup_state,
        )
        expected = CleanupBinding(
            claim_task_id=binding.claim_task_id,
            claim_sha256=locator.claim_sha256,
            provider_ref=locator.provider_ref,
            provider_run_ref=locator.provider_run_ref,
            provider_kernel_id=locator.provider_kernel_id,
            source_version=locator.source_version,
            source_sha256=locator.source_sha256,
            output_receipt_sha256=str(output["output_receipt_sha256"]),
            output_file_sha256=str(output["output_file_sha256"]),
            output_tree_sha256=str(output["output_tree_sha256"]),
        )
        if expected != binding:
            raise ValueError("outer reconciliation binding differs from the durable acceptance claim")
        cleanup_arguments = {
            "scenario_id": request.requirement_id,
            "task_id": str(binding.claim_task_id),
            "claim_sha256": binding.claim_sha256,
            "provider_run_ref": binding.provider_run_ref,
            "output_receipt_sha256": binding.output_receipt_sha256,
            "idempotency_key": f"operational:{request.matrix_id}:{request.requirement_id}:cleanup",
        }
        try:
            after = await gateway.call(
                "provider", "provider.acceptance.claim.cleanup", cleanup_arguments
            )
        except Exception:
            # The cleanup effect is deterministic and append-only. A lost
            # response is accepted only if claim.get independently observes
            # the exact COMPLETE claim; otherwise the phase is FAIL.
            after = await _claim_get(gateway, request, binding.claim_task_id)
        exact = _exact_claim(
            after, request=request, task_id=binding.claim_task_id, cleanup_state="COMPLETE"
        )
        cleanup = _one_claim_evidence(exact, "CLEANUP")
        if cleanup.get("claim_sha256") != binding.claim_sha256:
            raise ValueError("cleanup receipt differs from the exact provider claim")
    except Exception as exc:
        return _failed(
            request,
            checks=[*checks, _check("evidence-cleanup", "FAIL", "EVIDENCE_CLEANUP_RECONCILIATION_FAILED")],
            observations={"failure_type": type(exc).__name__, "claim_task_id": str(binding.claim_task_id)},
            mutations_started=2,
        )
    return TrustedDriverResult(
        schema_version="my-data-hub-operational-kaggle-driver-result.v2",
        phase="CLEANUP",
        outcome="PASS",
        scenario=request.scenario,
        task_run_id=request.task_run_id,
        provider_ref=binding.provider_ref,
        provider_run_ref=binding.provider_run_ref,
        provider_kernel_id=binding.provider_kernel_id,
        source_version=binding.source_version,
        source_sha256=binding.source_sha256,
        mutations_started=2,
        capability_checks=(_check("evidence-cleanup", "PASS", "EXACT_EVIDENCE_CLEANUP_COMPLETE", after),),
        observation_sha256=_sha({"claim": _sha(after), "cleanup": cleanup}),
        claim_task_id=binding.claim_task_id,
        claim_sha256=binding.claim_sha256,
        output_receipt_sha256=binding.output_receipt_sha256,
        output_file_sha256=binding.output_file_sha256,
        output_tree_sha256=binding.output_tree_sha256,
        cleanup_state="COMPLETE",
    )


def _evidence_claim_for(requirement_id: str) -> EvidenceClaim | None:
    document = _evidence_claims_document()
    return document.claims.get(requirement_id) if document is not None else None  # type: ignore[arg-type]


def _evidence_claims_document() -> EvidenceClaimsDocument | None:
    raw = os.environ.get("MY_DATA_HUB_OPERATIONAL_EVIDENCE_CLAIMS_JSON", "").strip()
    if not raw:
        return None
    if len(raw.encode()) > MAX_EVIDENCE_CLAIMS_BYTES:
        raise ValueError("operational evidence claims document is too large")
    return EvidenceClaimsDocument.model_validate_json(raw)


def _evidence_read_arguments(claim: EvidenceClaim) -> dict[str, Any]:
    return {
        "resource_ref": claim.resource_ref,
        "control_class": "mcp_managed",
        "private": True,
        "payload": {"kind": "notebook", "claim_sha256": claim.claim_sha256},
    }


def _evidence_locator(
    request: DriverRequest, claim: EvidenceClaim, response: Mapping[str, Any]
) -> EvidenceRunLocator:
    locator = EvidenceRunLocator.model_validate(response)
    if (
        locator.claim_sha256 != claim.claim_sha256
        or locator.task_id != claim.task_id
        or locator.provider_ref != claim.resource_ref
        or locator.task_run_id != request.task_run_id
    ):
        raise ValueError("provider evidence identity differs from the exact matrix claim")
    return locator


def _fm20_evidence_bundle() -> FM20EvidenceBundle | None:
    document = _evidence_claims_document()
    if document is None or document.fm20_evidence is None:
        return None
    return FM20EvidenceBundle.model_validate(document.fm20_evidence)


def _verify_fm20_host_evidence(
    request: DriverRequest, bundle: FM20EvidenceBundle
) -> dict[str, object]:
    return validate_deployment_evidence_v2(
        json.dumps(bundle.deployment_evidence, ensure_ascii=False, separators=(",", ":"), sort_keys=True),
        bundle.public_key_pem,
        expected_commit=request.commit_sha,
        expected_source_identity=EXPECTED_SOURCE_IDENTITY,
        expected_key_id=bundle.expected_key_id,
        expected_source_tree_sha256=bundle.expected_source_tree_sha256,
        expected_service_image_ids=bundle.expected_service_image_ids,
    )


def _exact_absent_master(status: Mapping[str, Any]) -> bool:
    return (
        status.get("master_state") == "ABSENT"
        and status.get("operation_id") is None
        and status.get("instance_id") is None
        and status.get("master_epoch") is None
        and status.get("canonical_revision") is None
        and status.get("lease_expires_at") is None
        and status.get("capabilities") == []
    )


def _exact_active_master(status: Mapping[str, Any]) -> bool:
    epoch = status.get("master_epoch")
    revision = status.get("canonical_revision")
    return (
        status.get("master_state") == "ACTIVE"
        and isinstance(status.get("instance_id"), str)
        and 1 <= len(status["instance_id"]) <= 300
        and isinstance(epoch, int)
        and not isinstance(epoch, bool)
        and epoch >= 1
        and isinstance(revision, int)
        and not isinstance(revision, bool)
        and revision >= 0
        and isinstance(status.get("capabilities"), list)
    )


async def _poll_fm20_master(
    gateway: McpGateway, operation_id: str
) -> dict[str, Any]:
    deadline = asyncio.get_running_loop().time() + FM20_MASTER_TIMEOUT_SECONDS
    while True:
        status = await gateway.call("reader", "master.status", {})
        if _exact_active_master(status):
            return status
        if (
            status.get("master_state") not in {"REQUESTED", "STARTING", "RESTORING", "REGISTERING"}
            or status.get("operation_id") != operation_id
        ):
            raise ValueError("FM20 master status is not bound to the accepted ensure operation")
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            raise TimeoutError("FM20 master did not become ACTIVE within the bound")
        await asyncio.sleep(min(FM20_MASTER_POLL_SECONDS, remaining))


def _exact_checkpoint(observations: Mapping[str, Any]) -> dict[str, Any]:
    checkpoint = observations.get("checkpoint")
    if not isinstance(checkpoint, Mapping):
        raise ValueError("checkpoint observation is absent")
    checkpoint_id = checkpoint.get("current_checkpoint_id")
    version_ref = checkpoint.get("current_exact_version_ref")
    generation = checkpoint.get("generation")
    if not isinstance(checkpoint_id, str) or not checkpoint_id:
        raise ValueError("current checkpoint identity is absent")
    if (
        not isinstance(version_ref, str)
        or len(version_ref) > 512
        or re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/[1-9][0-9]*", version_ref) is None
    ):
        raise ValueError("current checkpoint exact numeric version is absent")
    if not isinstance(generation, int) or isinstance(generation, bool) or generation < 1:
        raise ValueError("current checkpoint generation is absent")
    return {
        "checkpoint_id": checkpoint_id,
        "exact_version_ref": version_ref,
        "head_generation": generation,
        "current": checkpoint.get("current"),
    }


def _action_request(
    request: DriverRequest, spec: ExecutorSpec, observations: Mapping[str, Any]
) -> tuple[dict[str, Any], str, int]:
    checkpoint = _exact_checkpoint(observations)
    timeout_seconds = 1200 if spec.action_tool == "checkpoint.restore.request" else 1800
    request_key = f"operational-matrix:{request.matrix_id}:{request.requirement_id}:{request.task_run_id}"
    arguments: dict[str, Any] = {
        "idempotency_key": request_key,
        "checkpoint_id": checkpoint["checkpoint_id"],
        "exact_version_ref": checkpoint["exact_version_ref"],
        "timeout_seconds": timeout_seconds,
    }
    intent: dict[str, Any] = {
        "tool": spec.action_tool,
        "target": spec.action_target or "current",
        "checkpoint_id": checkpoint["checkpoint_id"],
        "exact_version_ref": checkpoint["exact_version_ref"],
        "head_generation": checkpoint["head_generation"],
        "timeout_seconds": timeout_seconds,
    }
    if spec.action_tool == "checkpoint.restore.request":
        arguments["target"] = spec.action_target or "current"
    elif spec.action_tool == "master.rotation.request":
        current = checkpoint.get("current")
        master = observations.get("master")
        if not isinstance(current, Mapping) or current.get("source_state") != "STOPPED":
            raise ValueError("rotation checkpoint source master is not durably stopped")
        if not isinstance(master, Mapping) or master.get("master_state", master.get("state")) not in {
            "ABSENT",
            "STOPPED",
        }:
            raise ValueError("rotation requires no ACTIVE master")
        epoch = current.get("source_epoch")
        revision = current.get("canonical_revision")
        if (
            not isinstance(epoch, int)
            or isinstance(epoch, bool)
            or epoch < 1
            or not isinstance(revision, int)
            or isinstance(revision, bool)
            or revision < 0
        ):
            raise ValueError("rotation checkpoint lacks exact epoch/revision binding")
        arguments.update(expected_active_epoch=epoch, expected_canonical_revision=revision)
        intent.update(expected_active_epoch=epoch, expected_canonical_revision=revision)
    else:  # pragma: no cover - authoring invariant
        raise ValueError("unsupported operational action tool")
    operation_id = hashlib.sha256(
        json.dumps(intent, sort_keys=True, separators=(",", ":")).encode() + b":" + request_key.encode()
    ).hexdigest()
    return arguments, operation_id, timeout_seconds


async def _poll_operation(
    gateway: McpGateway, operation_id: str, *, timeout_seconds: int
) -> dict[str, Any]:
    deadline = asyncio.get_running_loop().time() + timeout_seconds
    while True:
        status = await gateway.call("operator", "operation.get", {"operation_id": operation_id})
        if status.get("found") is not True or status.get("operation_id") != operation_id:
            return status
        if status.get("state") in ACTION_TERMINAL_STATES:
            return status
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            return status
        await asyncio.sleep(min(ACTION_POLL_SECONDS, remaining))


def _pass(
    request: DriverRequest,
    locator: EvidenceRunLocator,
    *,
    checks: list[CapabilityCheck],
    observations: Mapping[str, Any],
) -> TrustedDriverResult:
    # Legacy action paths have an exact launch locator but no control-owned
    # terminal output/receipt projection. Returning PASS here would let the
    # driver bless its own assertions without independent reconciliation.
    return TrustedDriverResult(
        schema_version="my-data-hub-operational-kaggle-driver-result.v2",
        phase=request.phase,
        outcome="FAIL",
        scenario=request.scenario,
        task_run_id=request.task_run_id,
        mutations_started=1,
        capability_checks=tuple(
            [*checks, _check("control-reconciliation", "FAIL", "CONTROL_OUTPUT_RECEIPT_MISSING")]
        ),
        observation_sha256=_sha(
            {**dict(observations), "untrusted_locator_sha256": _sha(locator.model_dump(mode="json"))}
        ),
    )


def _failed(
    request: DriverRequest,
    *,
    checks: list[CapabilityCheck],
    observations: Mapping[str, Any],
    mutations_started: int,
) -> TrustedDriverResult:
    return TrustedDriverResult(
        schema_version="my-data-hub-operational-kaggle-driver-result.v2",
        phase=request.phase,
        outcome="FAIL",
        scenario=request.scenario,
        task_run_id=request.task_run_id,
        mutations_started=mutations_started,
        capability_checks=tuple(checks),
        observation_sha256=_sha(dict(observations)),
    )


async def _execute_claimed_action(
    request: DriverRequest,
    spec: ExecutorSpec,
    gateway: McpGateway,
    *,
    checks: list[CapabilityCheck],
    observations: dict[str, Any],
) -> TrustedDriverResult:
    evidence_config: EvidenceDriverConfig | None = None
    if request.requirement_id == "FM06":
        try:
            evidence_config = _evidence_driver_config()
        except Exception as exc:
            return _blocked(
                request,
                checks=[*checks, _check("evidence-config", "FAIL", "EVIDENCE_CONFIGURATION_INVALID")],
                code="FM06_EVIDENCE_CONFIGURATION_INVALID",
                dependency=f"valid bounded evidence driver configuration ({type(exc).__name__})",
                observations=observations,
            )
        if evidence_config is None:
            return _blocked(
                request,
                checks=[*checks, _check("evidence-config", "BLOCKED", "EVIDENCE_CONFIGURATION_REQUIRED")],
                code="FM06_EVIDENCE_PROVIDER_CONFIGURATION_REQUIRED",
                dependency="exact disposable provider owner configuration",
                observations=observations,
            )
    try:
        claim = _evidence_claim_for(request.requirement_id)
    except Exception as exc:
        return _blocked(
            request,
            checks=[*checks, _check("evidence-claim", "FAIL", "EVIDENCE_CLAIMS_INVALID")],
            code=f"{request.requirement_id}_EVIDENCE_CLAIM_INVALID",
            dependency=f"valid bounded evidence claims document ({type(exc).__name__})",
            observations=observations,
        )
    if claim is None:
        return _blocked(
            request,
            checks=[*checks, _check("evidence-claim", "BLOCKED", "EVIDENCE_NOTEBOOK_CLAIM_MISSING")],
            code=spec.gap_code,
            dependency=spec.gap_dependency,
            observations=observations,
        )
    resume_initial: dict[str, Any] | None = None
    mutations_started = 0
    if request.resume_only:
        if claim.operation_id is None:
            return _failed(
                request,
                checks=[*checks, _check("action-resume", "FAIL", "DURABLE_ACTION_IDENTITY_MISSING")],
                observations=observations,
                mutations_started=0,
            )
        try:
            resume_initial = await gateway.call(
                "operator", "operation.get", {"operation_id": claim.operation_id}
            )
        except Exception as exc:
            return _failed(
                request,
                checks=[*checks, _check("action-resume", "FAIL", "DURABLE_ACTION_OBSERVATION_FAILED")],
                observations={**observations, "failure_type": type(exc).__name__},
                mutations_started=0,
            )
        expected_kind = (
            "checkpoint_restore_smoke"
            if spec.action_tool == "checkpoint.restore.request"
            else "forced_master_rotation"
        )
        if (
            resume_initial.get("found") is not True
            or resume_initial.get("operation_id") != claim.operation_id
            or resume_initial.get("operation_kind") != expected_kind
        ):
            return _failed(
                request,
                checks=[*checks, _check("action-resume", "FAIL", "DURABLE_ACTION_IDENTITY_NOT_FOUND")],
                observations={**observations, "action_resume": resume_initial},
                mutations_started=0,
            )
        observations["action_resume"] = resume_initial
        mutations_started = 1
    try:
        provider_observation = await gateway.call(
            "provider", "provider.resources.read", _evidence_read_arguments(claim)
        )
        locator = _evidence_locator(request, claim, provider_observation)
        if request.resume_only:
            assert claim.operation_id is not None
            arguments: dict[str, Any] = {}
            expected_operation_id = claim.operation_id
            timeout_seconds = 1200 if spec.action_tool == "checkpoint.restore.request" else 1800
        else:
            arguments, expected_operation_id, timeout_seconds = _action_request(request, spec, observations)
    except Exception as exc:
        if mutations_started:
            return _failed(
                request,
                checks=[*checks, _check("evidence-precondition", "FAIL", "EVIDENCE_ACTION_RECONCILIATION_FAILED")],
                observations={**observations, "failure_type": type(exc).__name__},
                mutations_started=mutations_started,
            )
        return _blocked(
            request,
            checks=[*checks, _check("evidence-precondition", "FAIL", "EVIDENCE_ACTION_PRECONDITION_UNMET")],
            code=f"{request.requirement_id}_EVIDENCE_ACTION_PRECONDITION_UNMET",
            dependency=f"exact provider claim and stopped/checkpoint action binding ({type(exc).__name__})",
            observations=observations,
        )
    observations["evidence_locator"] = locator.model_dump(mode="json")
    checks.append(_check("evidence-locator", "PASS", "EXACT_EVIDENCE_NOTEBOOK_RECONCILED", provider_observation))
    try:
        if request.resume_only:
            assert resume_initial is not None
            initial = resume_initial
        else:
            assert spec.action_tool is not None
            initial = await gateway.call("operator", spec.action_tool, arguments)
            if initial.get("accepted") is not True or initial.get("execution_supported") is not True:
                raw_blocker = initial.get("blocker_code")
                valid_blocker = (
                    isinstance(raw_blocker, str)
                    and len(raw_blocker) <= 120
                    and re.fullmatch(r"[A-Z0-9_]+", raw_blocker) is not None
                )
                exact_negative = (
                    initial.get("accepted") is False
                    and initial.get("execution_supported") is False
                    and not initial.get("operation_id")
                    and valid_blocker
                )
                if not exact_negative:
                    return _failed(
                        request,
                        checks=[
                            *checks,
                            _check("action-request", "FAIL", "DURABLE_ACTION_ACCEPTANCE_AMBIGUOUS", initial),
                        ],
                        observations={**observations, "action_request": initial},
                        mutations_started=1,
                    )
                assert isinstance(raw_blocker, str)
                return _blocked(
                    request,
                    checks=[*checks, _check("action-request", "BLOCKED", "DURABLE_ACTION_NOT_ACCEPTED", initial)],
                    code=raw_blocker,
                    dependency=f"durable consumer for exact {spec.action_tool} request",
                    observations={**observations, "action_request": initial},
                )
            if initial.get("operation_id") != expected_operation_id:
                return _failed(
                    request,
                    checks=[*checks, _check("action-request", "FAIL", "DURABLE_ACTION_IDENTITY_MISMATCH", initial)],
                    observations={**observations, "action_request": initial},
                    mutations_started=1,
                )
            mutations_started = 1
        terminal = await _poll_operation(gateway, expected_operation_id, timeout_seconds=timeout_seconds)
    except Exception as exc:
        return _failed(
            request,
            checks=[*checks, _check("action-execution", "FAIL", "DURABLE_ACTION_OBSERVATION_FAILED")],
            observations={**observations, "failure_type": type(exc).__name__},
            mutations_started=mutations_started or 1,
        )
    observations["action_terminal"] = terminal
    if (
        terminal.get("found") is not True
        or terminal.get("operation_id") != expected_operation_id
        or terminal.get("state") != "DURABLE_COMPLETE"
    ):
        return _failed(
            request,
            checks=[*checks, _check("action-terminal", "FAIL", "DURABLE_ACTION_NOT_COMPLETE", terminal)],
            observations=observations,
            mutations_started=mutations_started,
        )
    checks.append(_check("action-terminal", "PASS", "DURABLE_ACTION_COMPLETE", terminal))
    if request.requirement_id == "FM06":
        assert evidence_config is not None
        try:
            active = await gateway.call("reader", "master.status", {})
            provider_run_ref = active.get("provider_run_ref")
            provider_kernel_id = active.get("provider_kernel_id")
            checkpoint = _exact_checkpoint(observations)
            current = checkpoint.get("current")
            if (
                not _exact_active_master(active)
                or re.fullmatch(
                    r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/[1-9][0-9]*",
                    str(provider_run_ref or ""),
                ) is None
                or not isinstance(provider_kernel_id, int)
                or isinstance(provider_kernel_id, bool)
                or provider_kernel_id < 1
                or not isinstance(current, Mapping)
                or active.get("canonical_revision") != current.get("canonical_revision")
            ):
                raise ValueError("restored ACTIVE master identity/revision differs from the selected checkpoint")
            completed_at = terminal.get("updated_at")
            if not isinstance(completed_at, str):
                raise ValueError("restore terminal has no exact completion timestamp")
            lifecycle = ({
                "gate": "master_boot",
                "event_id": str(uuid5(NAMESPACE_URL, f"{request.task_run_id}:fm06-master-boot")),
                "started_at": request.evidence_issued_at.isoformat(),
                "completed_at": completed_at,
                "old_provider_run_ref": None,
                "new_provider_run_ref": provider_run_ref,
                "old_epoch": None,
                "new_epoch": active["master_epoch"],
                "operation_id": expected_operation_id,
                "before_identity": None,
                "after_identity": None,
                "duration_seconds": None,
                "heartbeat_count": None,
                "read_query_count": None,
                "checkpoint_count": None,
                "recovery_count": None,
            },)
            output = _output_document(
                request,
                {
                    "verified_checkpoint_selected": checkpoint,
                    "cold_restore_complete": {"operation": terminal, "active_master": active},
                    "revision_equal": {
                        "checkpoint_revision": current["canonical_revision"],
                        "active_revision": active["canonical_revision"],
                    },
                },
                lifecycle_events=lifecycle,
                operation_ids=(expected_operation_id,),
            )
        except Exception as exc:
            return _failed(
                request,
                checks=[*checks, _check("restored-master", "FAIL", "RESTORED_MASTER_IDENTITY_INVALID")],
                observations={**observations, "failure_type": type(exc).__name__},
                mutations_started=mutations_started,
            )
        observations["restored_master"] = active
        checks.append(_check("restored-master", "PASS", "RESTORED_MASTER_EXACT_IDENTITY_BOUND", active))
        return await _launch_post_action_evidence(
            request,
            gateway,
            owner=evidence_config.provider_owner,
            output_document=output,
            checks=checks,
            observations=observations,
            mutations_started=mutations_started,
        )
    return _pass(request, locator, checks=checks, observations=observations)


async def _execute_fm20(
    request: DriverRequest,
    spec: ExecutorSpec,
    gateway: McpGateway,
    *,
    checks: list[CapabilityCheck],
) -> TrustedDriverResult:
    observations: dict[str, Any] = {}
    mutations_started = 0
    try:
        claim = _evidence_claim_for("FM20")
    except Exception as exc:
        return _blocked(
            request,
            checks=[*checks, _check("evidence-claim", "FAIL", "EVIDENCE_CLAIMS_INVALID")],
            code="FM20_EVIDENCE_CLAIM_INVALID",
            dependency=f"valid bounded FM20 evidence Notebook claim ({type(exc).__name__})",
        )
    if claim is None:
        return _blocked(
            request,
            checks=[*checks, _check("evidence-claim", "BLOCKED", "EVIDENCE_NOTEBOOK_CLAIM_MISSING")],
            code=spec.gap_code,
            dependency=spec.gap_dependency,
        )

    operation_id = claim.operation_id
    if not request.resume_only and operation_id is not None:
        return _failed(
            request,
            checks=[*checks, _check("master-ensure", "FAIL", "UNEXPECTED_PRIOR_ACTION_IDENTITY")],
            observations=observations,
            mutations_started=0,
        )
    if request.resume_only:
        if operation_id is None:
            return _failed(
                request,
                checks=[*checks, _check("ensure-resume", "FAIL", "DURABLE_ACTION_IDENTITY_MISSING")],
                observations=observations,
                mutations_started=0,
            )
        try:
            resume = await gateway.call("operator", "operation.get", {"operation_id": operation_id})
        except Exception as exc:
            return _failed(
                request,
                checks=[*checks, _check("ensure-resume", "FAIL", "DURABLE_ACTION_OBSERVATION_FAILED")],
                observations={"failure_type": type(exc).__name__},
                mutations_started=0,
            )
        if (
            resume.get("found") is not True
            or resume.get("operation_id") != operation_id
            or resume.get("operation_kind") != "ensure_master"
        ):
            return _failed(
                request,
                checks=[*checks, _check("ensure-resume", "FAIL", "DURABLE_ACTION_IDENTITY_NOT_FOUND")],
                observations={"ensure_resume": resume},
                mutations_started=0,
            )
        mutations_started = 1
        observations["ensure_resume"] = resume
        if resume.get("state") not in {"REQUESTED", "STARTING", "RESTORING", "REGISTERING", "ACTIVE"}:
            return _failed(
                request,
                checks=[*checks, _check("ensure-resume", "FAIL", "ENSURE_OPERATION_NOT_ACTIVE")],
                observations=observations,
                mutations_started=1,
            )
        checks.append(_check("ensure-resume", "PASS", "DURABLE_ENSURE_IDENTITY_RECONCILED", resume))

    try:
        bundle = _fm20_evidence_bundle()
        if bundle is None:
            raise LookupError("FM20 signed evidence document is absent")
        host_evidence = _verify_fm20_host_evidence(request, bundle)
        provider_observation = await gateway.call(
            "provider", "provider.resources.read", _evidence_read_arguments(claim)
        )
        locator = _evidence_locator(request, claim, provider_observation)
    except Exception as exc:
        failure_observations = {**observations, "failure_type": type(exc).__name__}
        if mutations_started:
            return _failed(
                request,
                checks=[*checks, _check("fm20-evidence", "FAIL", "EVIDENCE_RECONCILIATION_FAILED")],
                observations=failure_observations,
                mutations_started=1,
            )
        return _blocked(
            request,
            checks=[*checks, _check("fm20-evidence", "FAIL", "SIGNED_FM20_EVIDENCE_INVALID")],
            code=spec.gap_code,
            dependency=f"fresh signed v2 host receipt and exact FM20 Notebook claim ({type(exc).__name__})",
            observations=failure_observations,
        )
    observations["host_evidence"] = host_evidence
    observations["evidence_locator"] = locator.model_dump(mode="json")
    checks.extend(
        (
            _check("host-evidence", "PASS", "SIGNED_HOST_REBOOT_EVIDENCE_VERIFIED", host_evidence),
            _check("evidence-locator", "PASS", "EXACT_EVIDENCE_NOTEBOOK_RECONCILED", provider_observation),
        )
    )

    if request.resume_only:
        assert operation_id is not None
    else:
        try:
            initial_master = await gateway.call("reader", "master.status", {})
        except Exception as exc:
            return _blocked(
                request,
                checks=[*checks, _check("master-initial", "FAIL", "INITIAL_MASTER_OBSERVATION_FAILED")],
                code="FM20_INITIAL_MASTER_OBSERVATION_UNAVAILABLE",
                dependency=f"exact reader master.status ABSENT observation ({type(exc).__name__})",
                observations=observations,
            )
        observations["master_initial"] = initial_master
        if not _exact_absent_master(initial_master):
            return _blocked(
                request,
                checks=[*checks, _check("master-initial", "FAIL", "MASTER_NOT_EXACTLY_ABSENT")],
                code="FM20_MASTER_NOT_INITIALLY_ABSENT",
                dependency="post-reboot reader master.status with exact ABSENT state",
                observations=observations,
            )
        checks.append(_check("master-initial", "PASS", "MASTER_INITIALLY_ABSENT", initial_master))
        try:
            ensure = await gateway.call("operator", "master.ensure", {})
        except Exception as exc:
            return _failed(
                request,
                checks=[*checks, _check("master-ensure", "FAIL", "ENSURE_ACCEPTANCE_AMBIGUOUS")],
                observations={**observations, "failure_type": type(exc).__name__},
                mutations_started=1,
            )
        observations["master_ensure"] = ensure
        raw_operation_id = ensure.get("operation_id")
        try:
            parsed_operation_id = UUID(str(raw_operation_id))
        except (TypeError, ValueError, AttributeError):
            parsed_operation_id = None
        if (
            parsed_operation_id is None
            or str(parsed_operation_id) != raw_operation_id
            or ensure.get("master_state") != "REQUESTED"
            or ensure.get("duplicate") is not False
            or ensure.get("intent") != "explicit-mcp-request"
            or ensure.get("terminal") is not False
        ):
            return _failed(
                request,
                checks=[*checks, _check("master-ensure", "FAIL", "ENSURE_ACCEPTANCE_AMBIGUOUS", ensure)],
                observations=observations,
                mutations_started=1,
            )
        operation_id = raw_operation_id
        mutations_started = 1
        checks.append(_check("master-ensure", "PASS", "MASTER_ENSURE_ACCEPTED", ensure))

    assert operation_id is not None
    try:
        active_master = await _poll_fm20_master(gateway, operation_id)
    except Exception as exc:
        return _failed(
            request,
            checks=[*checks, _check("master-active", "FAIL", "ENSURED_MASTER_NOT_ACTIVE")],
            observations={**observations, "failure_type": type(exc).__name__},
            mutations_started=mutations_started or 1,
        )
    observations["master_active"] = active_master
    checks.append(_check("master-active", "PASS", "ENSURED_MASTER_ACTIVE", active_master))

    try:
        search = await gateway.call(
            "reader",
            "bloggers.search",
            {"query": bundle.blogger_query, "cursor": None, "limit": 1},
        )
    except Exception as exc:
        return _failed(
            request,
            checks=[*checks, _check("blogger-search", "FAIL", "BOUNDED_BLOGGER_SEARCH_FAILED")],
            observations={**observations, "failure_type": type(exc).__name__},
            mutations_started=mutations_started,
        )
    items = search.get("items")
    if (
        not isinstance(items, list)
        or len(items) != 1
        or search.get("cursor") is not None
        or search.get("master_epoch") != active_master.get("master_epoch")
        or search.get("canonical_revision") != active_master.get("canonical_revision")
    ):
        return _failed(
            request,
            checks=[*checks, _check("blogger-search", "FAIL", "BOUNDED_BLOGGER_RESULT_INVALID", search)],
            observations={**observations, "search_response_sha256": _sha(search)},
            mutations_started=mutations_started,
        )
    search_summary = {
        "item_count": 1,
        "master_epoch": search["master_epoch"],
        "canonical_revision": search["canonical_revision"],
        "response_sha256": _sha(search),
    }
    observations["search"] = search_summary
    checks.append(_check("blogger-search", "PASS", "BOUNDED_BLOGGER_SEARCH_COMPLETE", search_summary))
    return _pass(request, locator, checks=checks, observations=observations)


async def execute(request: DriverRequest, gateway: McpGateway) -> TrustedDriverResult:
    if request.phase == "CLEANUP":
        return await _execute_cleanup(request, gateway)
    if request.phase == "RECONCILE":
        return await _execute_reconcile(request, gateway)
    spec = EXECUTORS[request.ordinal - 1]
    checks: list[CapabilityCheck] = []
    for profile in spec.profiles:
        try:
            catalog = await gateway.catalog(profile)
        except MissingCredential:
            if request.resume_only:
                return _failed(
                    request,
                    checks=[*checks, _check(f"credential.{profile}", "FAIL", f"{profile.upper()}_MCP_TOKEN_MISSING")],
                    observations={"resume_only": True, "profile": profile},
                    mutations_started=1,
                )
            return _blocked(
                request,
                checks=[*checks, _check(f"credential.{profile}", "BLOCKED", f"{profile.upper()}_MCP_TOKEN_MISSING")],
                code=f"{profile.upper()}_MCP_TOKEN_MISSING",
                dependency=f"configured {profile} MCP OAuth credential",
            )
        except Exception as exc:
            if request.resume_only:
                return _failed(
                    request,
                    checks=[*checks, _check(f"credential.{profile}", "FAIL", "MCP_PROFILE_CATALOG_UNAVAILABLE")],
                    observations={"resume_only": True, "profile": profile, "failure_type": type(exc).__name__},
                    mutations_started=1,
                )
            return _blocked(
                request,
                checks=[
                    *checks,
                    _check(f"credential.{profile}", "FAIL", "MCP_PROFILE_CATALOG_UNAVAILABLE"),
                ],
                code=f"{profile.upper()}_MCP_CATALOG_UNAVAILABLE",
                dependency=f"bounded {profile} MCP catalog observation ({type(exc).__name__})",
            )
        checks.append(_check(f"credential.{profile}", "PASS", "MCP_PROFILE_AUTHENTICATED", sorted(catalog)))
        required = {tool for tool_profile, tool in spec.tools if tool_profile == profile}
        missing = sorted(required - catalog)
        if missing:
            if request.resume_only:
                return _failed(
                    request,
                    checks=[*checks, _check(f"catalog.{profile}", "FAIL", "REQUIRED_MCP_TOOL_MISSING", missing)],
                    observations={"resume_only": True, "profile": profile, "missing": missing},
                    mutations_started=1,
                )
            return _blocked(
                request,
                checks=[*checks, _check(f"catalog.{profile}", "BLOCKED", "REQUIRED_MCP_TOOL_MISSING", missing)],
                code=f"{request.requirement_id}_MCP_TOOLSET_INCOMPLETE",
                dependency=f"{profile} MCP catalog tools: {', '.join(missing)}",
            )
        checks.append(_check(f"catalog.{profile}", "PASS", "REQUIRED_MCP_TOOLS_PRESENT", sorted(required)))
    try:
        probe_checks, observations = await _safe_probe(gateway, spec)
    except Exception as exc:
        if request.resume_only:
            return _failed(
                request,
                checks=[*checks, _check("safe-probe", "FAIL", "SAFE_PROBE_UNAVAILABLE")],
                observations={"resume_only": True, "failure_type": type(exc).__name__},
                mutations_started=1,
            )
        return _blocked(
            request,
            checks=[*checks, _check("safe-probe", "FAIL", "SAFE_PROBE_UNAVAILABLE")],
            code=f"{request.requirement_id}_SAFE_PROBE_UNAVAILABLE",
            dependency=f"bounded {spec.probe} production observation ({type(exc).__name__})",
        )
    checks.extend(probe_checks)
    if request.requirement_id in MASTER_ACCEPTANCE_REQUIREMENTS:
        return await _execute_master_acceptance_scenario(
            request,
            spec,
            gateway,
            checks=checks,
            observations=observations,
        )
    if request.requirement_id == "FM20":
        return await _execute_fm20(request, spec, gateway, checks=checks)
    if request.requirement_id in {"FM01", "FM02", "FM03", "FM22", "FM23"}:
        return await _execute_evidence_scenario(
            request,
            spec,
            gateway,
            checks=checks,
            observations=observations,
        )
    if request.requirement_id in {"FM16", "FM17", "FM18", "FM19", "FM21"}:
        return await _execute_data_workload_scenario(
            request,
            spec,
            gateway,
            checks=checks,
        )
    if request.requirement_id in {"FM05", "FM14", "FM15"}:
        return await _execute_checkpoint_acceptance_scenario(
            request,
            spec,
            gateway,
            checks=checks,
        )
    if spec.action_tool is not None:
        return await _execute_claimed_action(
            request,
            spec,
            gateway,
            checks=checks,
            observations=observations,
        )
    return _blocked(
        request,
        checks=checks,
        code=spec.gap_code,
        dependency=spec.gap_dependency,
        observations=observations,
    )


def _blocked(
    request: DriverRequest,
    *,
    checks: list[CapabilityCheck],
    code: str,
    dependency: str,
    observations: Mapping[str, Any] | None = None,
) -> TrustedDriverResult:
    return TrustedDriverResult(
        schema_version="my-data-hub-operational-kaggle-driver-result.v2",
        phase=request.phase,
        outcome="BLOCKED",
        scenario=request.scenario,
        task_run_id=request.task_run_id,
        blocker_code=code,
        integration_dependency=dependency,
        mutations_started=0,
        capability_checks=tuple(checks),
        observation_sha256=_sha(dict(observations)) if observations else None,
    )


def _bounded_json(path: Path, *, maximum: int) -> bytes:
    if path.is_symlink() or not path.is_file() or not 1 <= path.stat().st_size <= maximum:
        raise ValueError(f"unsafe bounded JSON path: {path}")
    return path.read_bytes()


def _atomic_write(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(canonical_json_bytes(dict(payload)))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        os.chmod(path, 0o600)
    finally:
        if temporary.exists():
            temporary.unlink()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--request", type=Path, required=True)
    parser.add_argument("--result", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        request = DriverRequest.model_validate_json(_bounded_json(args.request, maximum=MAX_REQUEST_BYTES))
        gateway = RemoteMcpGateway(
            os.environ.get("MY_DATA_HUB_MCP_CANARY_ENDPOINT", DEFAULT_ENDPOINT),
            bearer_source_from_environment(_tokens_from_environment()),
        )
        result = asyncio.run(execute(request, gateway))
        _atomic_write(args.result, result.model_dump(mode="json"))
    except Exception as exc:
        print(f"trusted operational driver failed closed: {type(exc).__name__}", file=sys.stderr)
        return FAIL
    return EXTERNAL_BLOCKED if result.outcome == "BLOCKED" else (FAIL if result.outcome == "FAIL" else 0)


if __name__ == "__main__":
    raise SystemExit(main())
