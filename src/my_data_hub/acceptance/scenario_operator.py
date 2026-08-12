"""Unified owner-only acceptance scenario request and status surface.

Master lifecycle scenarios execute in the control runtime.  Checkpoint
scenarios execute in a separate, private, task-owned Kaggle Notebook.  The
control host carries metadata only: exact numeric Dataset claims, provider run
locators, hashes, and the terminal checkpoint receipt.  It never materializes
checkpoint or verifier bytes and never treats a ``/kaggle`` path as local.
"""
from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Literal, Protocol
from urllib.parse import urlsplit
from uuid import NAMESPACE_URL, UUID, uuid5

from pydantic import BaseModel, ConfigDict, Field, model_validator

from my_data_hub.checkpoints.acceptance import Scenario as CheckpointScenario
from my_data_hub.checkpoints.acceptance_runtime import CheckpointAcceptanceOperationalResult
from my_data_hub.hashing import canonical_json_bytes

from .master_lifecycle import (
    AcceptancePrincipal,
    MasterAcceptanceRequest,
    MasterAcceptanceScenario,
    require_acceptance_operator,
)
from .master_production import ControlMasterAcceptanceExecutor

CHECKPOINT_SCENARIOS = frozenset({"FM05", "FM14", "FM15"})
MASTER_SCENARIOS = frozenset(item.value for item in MasterAcceptanceScenario)
ALL_ACCEPTANCE_SCENARIOS = MASTER_SCENARIOS | CHECKPOINT_SCENARIOS
CHECKPOINT_TIMEOUT_SECONDS = 900
CHECKPOINT_RESULT_FILE = "operational-result.json"
CHECKPOINT_STATUS_CONFIG_FILE = "kaggle_run.json"
CHECKPOINT_STATUS_HELPER_FILE = "kaggle_status_client.py"


class AcceptanceScenarioId(StrEnum):
    FM04 = "FM04"
    FM05 = "FM05"
    FM07 = "FM07"
    FM08 = "FM08"
    FM09 = "FM09"
    FM10 = "FM10"
    FM11 = "FM11"
    FM12 = "FM12"
    FM14 = "FM14"
    FM15 = "FM15"
    FM24 = "FM24"


class AcceptanceScenarioRequest(BaseModel):
    """The complete public mutation contract; no action/fault selector."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    task_id: UUID
    scenario: AcceptanceScenarioId
    idempotency_key: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]{7,199}$")
    source_revision: str = Field(pattern=r"^[a-f0-9]{40}$")
    target_operation_id: UUID | None = None

    @model_validator(mode="after")
    def exact_target(self) -> AcceptanceScenarioRequest:
        preboot = self.scenario.value in {"FM04", "FM07"}
        checkpoint = self.scenario.value in CHECKPOINT_SCENARIOS
        if checkpoint and self.target_operation_id is not None:
            raise ValueError("checkpoint acceptance cannot select a master operation")
        if not checkpoint and preboot == (self.target_operation_id is not None):
            raise ValueError("active master scenarios require one exact target operation")
        return self


class CheckpointDatasetInputClaim(BaseModel):
    """Exact protected Dataset input; bytes remain provider-side."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    provider_ref: str = Field(
        pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$", max_length=300
    )
    exact_version_ref: str = Field(
        pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/[1-9][0-9]*$",
        max_length=320,
    )
    claim_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    manifest_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    content_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")

    @model_validator(mode="after")
    def exact_ref_matches(self) -> CheckpointDatasetInputClaim:
        if self.exact_version_ref.rsplit("/", 1)[0] != self.provider_ref:
            raise ValueError("checkpoint input numeric version differs from provider_ref")
        return self


class CheckpointVerifierInputClaim(BaseModel):
    """Exact owner-reviewed verifier source Dataset; no source bytes."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    provider_ref: str = Field(
        pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$", max_length=300
    )
    exact_version_ref: str = Field(
        pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/[1-9][0-9]*$",
        max_length=320,
    )
    claim_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    source_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    code_file: Literal["worker.py"] = "worker.py"

    @model_validator(mode="after")
    def exact_ref_matches(self) -> CheckpointVerifierInputClaim:
        if self.exact_version_ref.rsplit("/", 1)[0] != self.provider_ref:
            raise ValueError("checkpoint verifier numeric version differs from provider_ref")
        return self


class CheckpointAcceptanceServiceIdentity(BaseModel):
    """Launch-bound acceptance identity, deliberately not runtime authority."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    request_id: UUID
    task_run_id: UUID
    attempt_id: UUID
    principal_id: str = Field(min_length=1, max_length=200)
    client_id: str = Field(min_length=1, max_length=200)
    scope: Literal["acceptance:operate"] = "acceptance:operate"


class CheckpointAcceptanceLaunchRequest(BaseModel):
    """Exact metadata persisted by the launch port before provider effects."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["my-data-hub-checkpoint-acceptance-launch-request.v1"] = (
        "my-data-hub-checkpoint-acceptance-launch-request.v1"
    )
    request_id: UUID
    scenario: CheckpointScenario
    operation_id: UUID
    task_run_id: UUID
    idempotency_key: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]{7,199}$")
    source_revision: str = Field(pattern=r"^[a-f0-9]{40}$")
    started_at: datetime
    provider_owner: str = Field(pattern=r"^[A-Za-z0-9_.-]{1,80}$")
    evidence_notebook_ref: str = Field(
        pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$", max_length=300
    )
    candidate_dataset_ref: str = Field(
        pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$", max_length=300
    )
    verifier_notebook_ref: str | None = Field(
        default=None,
        pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$",
        max_length=300,
    )
    template_input: CheckpointDatasetInputClaim
    verifier_input: CheckpointVerifierInputClaim | None = None
    control_identity: CheckpointAcceptanceServiceIdentity
    result_file: Literal["operational-result.json"] = CHECKPOINT_RESULT_FILE
    timeout_seconds: Literal[900] = CHECKPOINT_TIMEOUT_SECONDS
    status_config_file: Literal["kaggle_run.json"] = CHECKPOINT_STATUS_CONFIG_FILE
    status_helper_file: Literal["kaggle_status_client.py"] = CHECKPOINT_STATUS_HELPER_FILE

    @model_validator(mode="after")
    def exact_binding(self) -> CheckpointAcceptanceLaunchRequest:
        if self.started_at.tzinfo is None or self.started_at.utcoffset() is None:
            raise ValueError("checkpoint launch time must be timezone-aware")
        if (
            self.control_identity.request_id != self.request_id
            or self.control_identity.task_run_id != self.task_run_id
            or self.task_run_id != self.request_id
            or any(
                ref.split("/", 1)[0] != self.provider_owner
                for ref in (
                    self.evidence_notebook_ref,
                    self.candidate_dataset_ref,
                    self.template_input.provider_ref,
                )
            )
            or (self.scenario in {"FM05", "FM15"})
            != (self.verifier_input is not None and self.verifier_notebook_ref is not None)
        ):
            raise ValueError("checkpoint launch metadata differs from its fixed task binding")
        if self.verifier_input is not None and (
            self.verifier_input.provider_ref.split("/", 1)[0] != self.provider_owner
            or self.verifier_notebook_ref is None
            or self.verifier_notebook_ref.split("/", 1)[0] != self.provider_owner
        ):
            raise ValueError("checkpoint verifier metadata has another provider owner")
        return self

    @property
    def request_sha256(self) -> str:
        return hashlib.sha256(canonical_json_bytes(self.model_dump(mode="json"))).hexdigest()


class CheckpointProviderRunOutput(BaseModel):
    """Official-adapter locator and bounded output-file receipt."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    provider_ref: str = Field(
        pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$", max_length=300
    )
    provider_run_ref: str = Field(
        pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/[1-9][0-9]*$",
        max_length=320,
    )
    provider_kernel_id: int = Field(ge=1)
    source_version: int = Field(ge=1)
    source_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    provider_claim_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    output_file_name: Literal["operational-result.json"] = CHECKPOINT_RESULT_FILE
    output_file_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    output_tree_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    output_receipt_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    private: Literal[True] = True

    @model_validator(mode="after")
    def exact_run_ref(self) -> CheckpointProviderRunOutput:
        if self.provider_run_ref.rsplit("/", 1)[0] != self.provider_ref:
            raise ValueError("checkpoint provider run differs from its Notebook")
        if int(self.provider_run_ref.rsplit("/", 1)[1]) != self.source_version:
            raise ValueError("checkpoint provider run version differs from source_version")
        return self


class CheckpointResourceLeaseObservation(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    lease_id: UUID
    resource_kind: Literal["kaggle_notebook"]
    resource_ref: str = Field(
        pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$", max_length=300
    )
    holder_id: UUID
    lease_until: datetime
    epoch: int = Field(ge=1)
    released: bool

    @model_validator(mode="after")
    def aware_lease(self) -> CheckpointResourceLeaseObservation:
        if self.lease_until.tzinfo is None:
            raise ValueError("checkpoint resource lease deadline must be timezone-aware")
        return self


class CheckpointStatusDatasetObservation(BaseModel):
    """Exact provider-only callback bootstrap and its cleanup proof."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    provider_ref: str = Field(
        pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$", max_length=300
    )
    exact_version_ref: str = Field(
        pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/[1-9][0-9]*$", max_length=320
    )
    claim_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    content_tree_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    status_config_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    status_helper_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    cleanup_receipt_sha256: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")
    cleaned: bool = False
    resource_lease: CheckpointResourceLeaseObservation

    @model_validator(mode="after")
    def exact_observation(self) -> CheckpointStatusDatasetObservation:
        if self.exact_version_ref.rsplit("/", 1)[0] != self.provider_ref:
            raise ValueError("checkpoint status Dataset exact version differs")
        if self.cleaned != (self.cleanup_receipt_sha256 is not None):
            raise ValueError("checkpoint status Dataset cleanup proof is incomplete")
        return self


class CheckpointRuntimeObservation(BaseModel):
    """Bounded durable custom-phase/event projection from the task Notebook."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    latest_phase: str | None = Field(default=None, min_length=1, max_length=100)
    latest_status: str | None = Field(default=None, min_length=1, max_length=100)
    latest_progress: dict[str, Any]
    event_counts: dict[
        Literal[
            "runtime.started", "runtime.progress", "runtime.heartbeat", "runtime.failed",
            "runtime.terminal", "resource.acquire", "resource.release", "job.result_available",
        ],
        int,
    ]
    event_uids: tuple[str, ...] = Field(min_length=1, max_length=100)
    event_receipt_sha256s: tuple[str, ...] = Field(min_length=1, max_length=100)
    last_local_sequence: int = Field(ge=1)
    runtime_source_sha256: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")

    @model_validator(mode="after")
    def exact_projection(self) -> CheckpointRuntimeObservation:
        if (
            len(self.event_uids) != len(self.event_receipt_sha256s)
            or len(set(self.event_uids)) != len(self.event_uids)
            or any(
                re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]{7,199}", value) is None
                for value in self.event_uids
            )
            or any(re.fullmatch(r"[a-f0-9]{64}", value) is None for value in self.event_receipt_sha256s)
        ):
            raise ValueError("checkpoint runtime event projection is invalid")
        return self

    @property
    def terminal_complete(self) -> bool:
        required = {
            "runtime.started": 1,
            "runtime.progress": 1,
            "runtime.heartbeat": 1,
            "resource.acquire": 1,
            "job.result_available": 1,
            "resource.release": 1,
            "runtime.terminal": 1,
        }
        return all(self.event_counts.get(key, 0) >= count for key, count in required.items())


class CheckpointAcceptanceLaunchStatus(BaseModel):
    """Metadata-only reconciliation result; never an operational-matrix PASS."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["my-data-hub-checkpoint-acceptance-launch-status.v1"] = (
        "my-data-hub-checkpoint-acceptance-launch-status.v1"
    )
    found: Literal[True] = True
    request_id: UUID
    scenario: CheckpointScenario
    operation_id: UUID
    task_run_id: UUID
    principal_id: str = Field(min_length=1, max_length=200)
    client_id: str = Field(min_length=1, max_length=200)
    request_persisted: Literal[True] = True
    state: Literal["REQUESTED", "RUNNING", "LIVE_EVIDENCE_READY", "BLOCKED", "FAIL"]
    request_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    config_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    result_sha256: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")
    provider_output: CheckpointProviderRunOutput | None = None
    status_input: CheckpointStatusDatasetObservation | None = None
    runtime_observation: CheckpointRuntimeObservation | None = None
    result: CheckpointAcceptanceOperationalResult | None = None
    blocker_code: str | None = Field(default=None, pattern=r"^[A-Z][A-Z0-9_]{2,119}$")
    failure_code: str | None = Field(default=None, pattern=r"^[A-Z][A-Z0-9_]{2,119}$")
    official_adapter_observed: bool = False

    @model_validator(mode="after")
    def exact_state(self) -> CheckpointAcceptanceLaunchStatus:
        if self.request_id != self.task_run_id:
            raise ValueError("checkpoint launch status is not task-bound")
        if self.state in {"REQUESTED", "RUNNING"}:
            if any(
                value is not None
                for value in (
                    self.result_sha256,
                    self.provider_output,
                    self.result,
                    self.blocker_code,
                    self.failure_code,
                )
            ) or self.official_adapter_observed:
                raise ValueError("nonterminal checkpoint status contains terminal evidence")
            if self.status_input is not None and self.status_input.cleaned:
                raise ValueError("running checkpoint status cannot report cleaned input")
            return self
        if self.state == "LIVE_EVIDENCE_READY":
            if (
                self.result is None
                or self.result.outcome != "LIVE_EVIDENCE_READY"
                or self.provider_output is None
                or self.result_sha256 is None
                or self.config_sha256 != self.result.config_sha256
                or self.result.scenario != self.scenario
                or self.result.operation_id != self.operation_id
                or self.result.task_run_id != self.task_run_id
                or self.blocker_code is not None
                or self.failure_code is not None
                or not self.official_adapter_observed
                or self.provider_output.provider_ref != self.result.locator.evidence_notebook_ref
                or self.status_input is None
                or not self.status_input.cleaned
                or self.runtime_observation is None
                or not self.runtime_observation.terminal_complete
            ):
                raise ValueError("ready checkpoint status lacks exact provider/result reconciliation")
            calculated = hashlib.sha256(
                canonical_json_bytes(self.result.model_dump(mode="json"))
            ).hexdigest()
            if calculated != self.result_sha256:
                raise ValueError("checkpoint operational result hash differs")
            return self
        if self.state == "BLOCKED":
            if not self.blocker_code or self.failure_code is not None or self.official_adapter_observed:
                raise ValueError("blocked checkpoint status lacks blocker code")
        elif not self.failure_code or self.blocker_code is not None:
            raise ValueError("failed checkpoint status lacks failure code")
        ambiguous_push = self.state == "FAIL" and self.failure_code in {
            "CHECKPOINT_PUSH_RESPONSE_AMBIGUOUS",
            "CHECKPOINT_RUNTIME_SOURCE_MISMATCH",
        }
        if self.status_input is not None and not self.status_input.cleaned and not ambiguous_push:
            raise ValueError("terminal checkpoint status retains its status Dataset")
        if self.result is not None and self.result.outcome != self.state:
            raise ValueError("checkpoint terminal result outcome differs from launch state")
        return self


class CheckpointAcceptanceLaunchPort(Protocol):
    """Single official-adapter launch/reconcile boundary.

    Implementations MUST persist the exact request before a provider push,
    attach only the exact numeric ``dataset_sources``, require a private
    task-owned Notebook and the named launch-bound User Secret, and reconcile
    the exact provider run/output before returning ``LIVE_EVIDENCE_READY``.
    """

    def launch_checkpoint_acceptance(
        self, request: CheckpointAcceptanceLaunchRequest
    ) -> CheckpointAcceptanceLaunchStatus: ...

    def checkpoint_acceptance_status(
        self, request_id: UUID
    ) -> CheckpointAcceptanceLaunchStatus | None: ...


@dataclass(frozen=True, slots=True)
class CheckpointAcceptanceLaunchCatalog:
    """Owner-fixed metadata used to build launch requests."""

    provider_owner: str
    evidence_notebook_ref: str
    candidate_dataset_refs: dict[CheckpointScenario, str]
    template_input: CheckpointDatasetInputClaim
    verifier_inputs: dict[Literal["FM05", "FM15"], CheckpointVerifierInputClaim]
    verifier_notebook_refs: dict[Literal["FM05", "FM15"], str]

    def __post_init__(self) -> None:
        if set(self.candidate_dataset_refs) != {"FM05", "FM14", "FM15"}:
            raise ValueError("checkpoint candidate Dataset catalog must cover FM05/FM14/FM15")
        if set(self.verifier_inputs) != {"FM05", "FM15"} or set(
            self.verifier_notebook_refs
        ) != {"FM05", "FM15"}:
            raise ValueError("checkpoint verifier catalog must cover only FM05/FM15")
        refs = [self.evidence_notebook_ref, self.template_input.provider_ref]
        refs.extend(self.candidate_dataset_refs.values())
        refs.extend(item.provider_ref for item in self.verifier_inputs.values())
        refs.extend(self.verifier_notebook_refs.values())
        if any(ref.split("/", 1)[0] != self.provider_owner for ref in refs):
            raise ValueError("checkpoint launch catalog has mixed provider owners")

    def request(
        self,
        public: AcceptanceScenarioRequest,
        principal: AcceptancePrincipal,
        *,
        started_at: datetime,
    ) -> CheckpointAcceptanceLaunchRequest:
        if public.scenario.value not in CHECKPOINT_SCENARIOS:
            raise ValueError("master scenario cannot use checkpoint launch catalog")
        scenario = public.scenario.value
        operation_id = uuid5(
            NAMESPACE_URL,
            f"checkpoint-acceptance:{scenario}:{public.task_id}:{public.idempotency_key}:{public.source_revision}",
        )
        attempt_id = uuid5(NAMESPACE_URL, f"checkpoint-acceptance-attempt:{operation_id}")
        return CheckpointAcceptanceLaunchRequest(
            request_id=public.task_id,
            scenario=scenario,  # type: ignore[arg-type]
            operation_id=operation_id,
            task_run_id=public.task_id,
            idempotency_key=public.idempotency_key,
            source_revision=public.source_revision,
            started_at=started_at,
            provider_owner=self.provider_owner,
            evidence_notebook_ref=self.evidence_notebook_ref,
            candidate_dataset_ref=self.candidate_dataset_refs[scenario],  # type: ignore[index]
            verifier_notebook_ref=self.verifier_notebook_refs.get(scenario),  # type: ignore[arg-type]
            template_input=self.template_input,
            verifier_input=self.verifier_inputs.get(scenario),  # type: ignore[arg-type]
            control_identity=CheckpointAcceptanceServiceIdentity(
                request_id=public.task_id,
                task_run_id=public.task_id,
                attempt_id=attempt_id,
                principal_id=principal.subject,
                client_id=principal.client_id,
            ),
        )


@dataclass(slots=True)
class UnifiedAcceptanceScenarioExecutor:
    """Dispatch only the fixed enum to master or checkpoint execution."""

    master: ControlMasterAcceptanceExecutor
    checkpoint: CheckpointAcceptanceLaunchPort
    checkpoint_catalog: CheckpointAcceptanceLaunchCatalog

    def request(
        self, request: AcceptanceScenarioRequest, principal: AcceptancePrincipal
    ) -> dict[str, Any]:
        require_acceptance_operator(principal)
        if request.scenario.value in MASTER_SCENARIOS:
            value = self.master.request(
                MasterAcceptanceRequest(
                    task_id=request.task_id,
                    scenario=MasterAcceptanceScenario(request.scenario.value),
                    idempotency_key=request.idempotency_key,
                    source_revision=request.source_revision,
                    target_operation_id=request.target_operation_id,
                ),
                principal,
            )
            return {"found": True, **value}
        launch = self.checkpoint_catalog.request(
            request, principal, started_at=datetime.now(UTC)
        )
        status = self.checkpoint.launch_checkpoint_acceptance(launch)
        self._assert_checkpoint_status(launch, status, principal)
        return status.model_dump(mode="json")

    def status(self, task_id: UUID, principal: AcceptancePrincipal) -> dict[str, Any]:
        require_acceptance_operator(principal)
        checkpoint = self.checkpoint.checkpoint_acceptance_status(task_id)
        if checkpoint is not None:
            if (
                checkpoint.request_id != task_id
                or checkpoint.principal_id != principal.subject
                or checkpoint.client_id != principal.client_id
            ):
                raise ValueError("checkpoint status returned another owner-bound task")
            return checkpoint.model_dump(mode="json")
        return self.master.status(task_id, principal)

    @staticmethod
    def _assert_checkpoint_status(
        request: CheckpointAcceptanceLaunchRequest,
        status: CheckpointAcceptanceLaunchStatus,
        principal: AcceptancePrincipal,
    ) -> None:
        require_acceptance_operator(principal)
        if (
            status.request_id != request.request_id
            or status.scenario != request.scenario
            or status.operation_id != request.operation_id
            or status.task_run_id != request.task_run_id
            or status.request_sha256 != request.request_sha256
            or status.principal_id != request.control_identity.principal_id
            or status.client_id != request.control_identity.client_id
            or status.principal_id != principal.subject
            or status.client_id != principal.client_id
        ):
            raise ValueError("checkpoint launch response differs from persisted request")
        if status.result is not None:
            locator = status.result.locator
            verifier_ref = (
                request.verifier_input.exact_version_ref
                if request.verifier_input is not None
                else None
            )
            verifier_claim = (
                request.verifier_input.claim_sha256
                if request.verifier_input is not None
                else None
            )
            if (
                locator.provider_owner != request.provider_owner
                or locator.dataset_ref != request.candidate_dataset_ref
                or locator.evidence_notebook_ref != request.evidence_notebook_ref
                or locator.verifier_notebook_ref != request.verifier_notebook_ref
                or locator.template_dataset_version_ref
                != request.template_input.exact_version_ref
                or locator.template_claim_sha256 != request.template_input.claim_sha256
                or locator.verifier_dataset_version_ref != verifier_ref
                or locator.verifier_claim_sha256 != verifier_claim
            ):
                raise ValueError("checkpoint result locator differs from exact launch inputs")


@dataclass(frozen=True, slots=True)
class AcceptanceScenarioOperatorAdapter:
    """The only owner-facing request/status tools; there is no list catalog."""

    executor: UnifiedAcceptanceScenarioExecutor

    REQUEST_TOOL = "acceptance.scenario.request"
    STATUS_TOOL = "acceptance.scenario.status"

    @classmethod
    def tool_schemas(cls) -> dict[str, dict[str, Any]]:
        return {
            cls.REQUEST_TOOL: AcceptanceScenarioRequest.model_json_schema(),
            cls.STATUS_TOOL: {
                "type": "object",
                "additionalProperties": False,
                "required": ["task_id"],
                "properties": {"task_id": {"type": "string", "format": "uuid"}},
            },
        }

    def call(
        self,
        tool: str,
        arguments: dict[str, Any],
        principal: AcceptancePrincipal,
    ) -> dict[str, Any]:
        require_acceptance_operator(principal)
        if tool == self.REQUEST_TOOL:
            return self.executor.request(AcceptanceScenarioRequest.model_validate(arguments), principal)
        if tool == self.STATUS_TOOL:
            if set(arguments) != {"task_id"}:
                raise ValueError("acceptance scenario status arguments differ from the exact contract")
            return self.executor.status(UUID(str(arguments["task_id"])), principal)
        raise ValueError("unknown acceptance scenario operator tool")


def checkpoint_launch_request_sha256(request: CheckpointAcceptanceLaunchRequest) -> str:
    return request.request_sha256


def validate_checkpoint_control_origin(value: str) -> str:
    """Validate metadata-only control origin without accepting credentials."""

    parsed = urlsplit(value)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
        or parsed.path not in {"", "/"}
    ):
        raise ValueError("checkpoint control origin must be credential-free HTTPS")
    return value.rstrip("/")
