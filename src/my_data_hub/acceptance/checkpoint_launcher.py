"""Control-owned FM05/FM14/FM15 launcher with provider-only status bootstrap.

The control process persists an owner-task authority, creates one private,
disposable status Dataset containing bounded ``kaggle_run.json`` plus a fixed
helper, and attaches its exact numeric version to the protected evidence
Notebook.  Only the callback token hash enters the control ledger.  Provider
API credential *names* may be bound as Kaggle User Secrets for the narrowly
reviewed data-local checkpoint adapter; their values never enter source,
Datasets, callbacks, logs, or receipts.
"""
from __future__ import annotations

import hashlib
import json
import os
import secrets
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Literal
from uuid import NAMESPACE_URL, UUID, uuid5

from pydantic import BaseModel, ConfigDict, Field, model_validator

from my_data_hub.checkpoints.acceptance_runtime import (
    CheckpointAcceptanceControlIdentity,
    CheckpointAcceptanceOperationalResult,
    CheckpointAcceptanceProductionConfig,
    CheckpointAcceptanceVerifierAsset,
)
from my_data_hub.control_plane.ledger import ControlLedger
from my_data_hub.hashing import canonical_json_bytes
from my_data_hub.providers.kaggle import KaggleProviderAdapter
from my_data_hub.providers.kaggle.adapter import mapping_sha256
from my_data_hub.providers.kaggle.contracts import (
    KaggleKernelRunIdentity,
    KernelState,
    MutationAction,
    ProviderEffectIntent,
    ProviderEffectReceipt,
    TaskResourceClaim,
)
from my_data_hub.providers.models import ControlClass

from .scenario_operator import (
    CHECKPOINT_RESULT_FILE,
    CheckpointAcceptanceLaunchCatalog,
    CheckpointAcceptanceLaunchRequest,
    CheckpointAcceptanceLaunchStatus,
    CheckpointDatasetInputClaim,
    CheckpointProviderRunOutput,
    CheckpointResourceLeaseObservation,
    CheckpointRuntimeObservation,
    CheckpointStatusDatasetObservation,
    CheckpointVerifierInputClaim,
)

_AUTHORITY_TTL_SECONDS = 900
_MAX_SOURCE_BYTES = 512 * 1024
_MAX_RESULT_BYTES = 256 * 1024
_MAX_STATUS_BYTES = 16 * 1024

# Provider-side bootstrap only. RuntimeClient remains the authenticated
# Bearer/header transport and owns redacted JSONL fallback/event_uid replay.
_STATUS_HELPER = (
    b'"""Fixed provider-only status bootstrap; never logs token values."""\n'
    b"import json, os, pathlib\n\n"
    b"def load_run_config(path, *, request_id, attempt_id, notebook):\n"
    b"    raw = pathlib.Path(path).read_bytes()\n"
    b"    if not 1 <= len(raw) <= 16384:\n"
    b'        raise RuntimeError("status input size invalid")\n'
    b"    value = json.loads(raw)\n"
    b'    expected = {"schema_version","run_id","attempt_id","kind","notebook",'
    b'"callback_url","token","resource_leases"}\n'
    b"    if set(value) != expected:\n"
    b'        raise RuntimeError("status input shape invalid")\n'
    b'    if value["schema_version"] != "my-data-hub-kaggle-run.v1":\n'
    b'        raise RuntimeError("status input schema invalid")\n'
    b'    if value["kind"] != "checkpoint-acceptance":\n'
    b'        raise RuntimeError("status input kind invalid")\n'
    b'    if value["run_id"] != request_id or value["attempt_id"] != attempt_id:\n'
    b'        raise RuntimeError("status input binding invalid")\n'
    b'    if value["notebook"] != notebook:\n'
    b'        raise RuntimeError("status input Notebook invalid")\n'
    b'    token = value.pop("token")\n'
    b"    if not isinstance(token, str) or len(token) != 64:\n"
    b'        raise RuntimeError("status input token invalid")\n'
    b'    os.environ["MY_DATA_HUB_RUN_SECRET"] = token\n'
    b"    return value\n"
)


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


class CheckpointRuntimeInput(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    provider_ref: str = Field(pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$", max_length=300)
    exact_version_ref: str = Field(
        pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/[1-9][0-9]*$", max_length=320
    )
    claim_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    wheel_file: str = Field(pattern=r"^[A-Za-z0-9_.-]+\.whl$", max_length=200)
    wheel_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    entrypoint_file: Literal["checkpoint_acceptance_evidence.py"] = (
        "checkpoint_acceptance_evidence.py"
    )
    entrypoint_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")

    @model_validator(mode="after")
    def exact_ref(self) -> CheckpointRuntimeInput:
        if self.exact_version_ref.rsplit("/", 1)[0] != self.provider_ref:
            raise ValueError("checkpoint runtime input is not an exact version of provider_ref")
        return self


class CheckpointAcceptanceDeployment(BaseModel):
    """Validated owner asset catalog loaded once by production assembly."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["my-data-hub-checkpoint-acceptance-deployment.v1"]
    provider_owner: str = Field(pattern=r"^[A-Za-z0-9_.-]{1,80}$")
    evidence_notebook_ref: str
    candidate_dataset_refs: dict[Literal["FM05", "FM14", "FM15"], str]
    template_input: CheckpointDatasetInputClaim
    verifier_inputs: dict[Literal["FM05", "FM15"], CheckpointVerifierInputClaim]
    verifier_notebook_refs: dict[Literal["FM05", "FM15"], str]
    runtime_input: CheckpointRuntimeInput
    control_base_url: str
    kaggle_secret_bindings: dict[str, str]

    @model_validator(mode="after")
    def exact_deployment(self) -> CheckpointAcceptanceDeployment:
        _catalog = self.catalog  # validate the complete fixed catalog
        if self.runtime_input.provider_ref.split("/", 1)[0] != self.provider_owner:
            raise ValueError("checkpoint runtime input has another owner")
        keys = set(self.kaggle_secret_bindings)
        if keys not in ({"KAGGLE_API_TOKEN"}, {"KAGGLE_USERNAME", "KAGGLE_KEY"}):
            raise ValueError("checkpoint runtime requires access token OR complete legacy pair")
        if any(not value or len(value) > 200 for value in self.kaggle_secret_bindings.values()):
            raise ValueError("checkpoint runtime User Secret names are invalid")
        if len(set(self.kaggle_secret_bindings.values())) != len(self.kaggle_secret_bindings):
            raise ValueError("checkpoint runtime User Secret names must be distinct")
        return self

    @property
    def catalog(self) -> CheckpointAcceptanceLaunchCatalog:
        return CheckpointAcceptanceLaunchCatalog(
            provider_owner=self.provider_owner,
            evidence_notebook_ref=self.evidence_notebook_ref,
            candidate_dataset_refs=self.candidate_dataset_refs,  # type: ignore[arg-type]
            template_input=self.template_input,
            verifier_inputs=self.verifier_inputs,  # type: ignore[arg-type]
            verifier_notebook_refs=self.verifier_notebook_refs,  # type: ignore[arg-type]
        )


@dataclass(slots=True)
class ControlCheckpointAcceptanceLauncher:
    ledger: ControlLedger
    adapter: KaggleProviderAdapter
    deployment: CheckpointAcceptanceDeployment

    def launch_checkpoint_acceptance(
        self, request: CheckpointAcceptanceLaunchRequest
    ) -> CheckpointAcceptanceLaunchStatus:
        existing = self.ledger.checkpoint_acceptance_launch(str(request.request_id))
        if existing is not None:
            if (
                existing["request_sha256"] != request.request_sha256
                or existing["principal_id"] != request.control_identity.principal_id
                or existing["client_id"] != request.control_identity.client_id
            ):
                raise ValueError("checkpoint acceptance request identity changed")
            config = CheckpointAcceptanceProductionConfig.model_validate(existing["config"])
            if existing["status_dataset"] is None:
                if self._creator_claim_fresh(existing):
                    return self._status(request, config, existing, state="REQUESTED")
                return self._terminal(
                    self._status(
                        request,
                        config,
                        existing,
                        state="FAIL",
                        failure="CHECKPOINT_STATUS_INPUT_RESPONSE_AMBIGUOUS",
                    )
                )
            if existing["provider_run"] is None:
                if self._creator_claim_fresh(existing):
                    return self._status(request, config, existing, state="REQUESTED")
                return self._terminal(
                    self._status(
                        request,
                        config,
                        existing,
                        state="FAIL",
                        failure="CHECKPOINT_PUSH_RESPONSE_AMBIGUOUS",
                    )
                )
            return self._reconcile(existing, request, config)

        token = secrets.token_hex(32)
        status_files = self._status_files(request, token)
        config = self._config(request, status_files)
        source = self._render_source(request, config)
        stored, created = self.ledger.ensure_checkpoint_acceptance_launch(
            request=request.model_dump(mode="json"),
            request_sha256=request.request_sha256,
            principal_id=request.control_identity.principal_id,
            client_id=request.control_identity.client_id,
            token_sha256=_sha256(token.encode()),
            expires_at=request.started_at + timedelta(seconds=_AUTHORITY_TTL_SECONDS),
            config=config.model_dump(mode="json"),
            config_sha256=config.config_sha256,
            expected_source_sha256=_sha256(source),
        )
        if not created:
            config = CheckpointAcceptanceProductionConfig.model_validate(stored["config"])
            if stored["status_dataset"] is None:
                if self._creator_claim_fresh(stored):
                    return self._status(request, config, stored, state="REQUESTED")
                return self._terminal(
                    self._status(
                        request,
                        config,
                        stored,
                        state="FAIL",
                        failure="CHECKPOINT_STATUS_INPUT_RESPONSE_AMBIGUOUS",
                    )
                )
        if stored["provider_run"] is not None:
            return self._reconcile(stored, request, config)
        if not created and stored["status_dataset"] is not None:
            # A persisted status input with no run may be a lost push response.
            # Never repeat or delete an input that an unknown live run may use.
            if self._creator_claim_fresh(stored):
                return self._status(request, config, stored, state="REQUESTED")
            return self._terminal(
                self._status(
                    request,
                    config,
                    stored,
                    state="FAIL",
                    failure="CHECKPOINT_PUSH_RESPONSE_AMBIGUOUS",
                )
            )
        if stored["status_dataset"] is None:
            lease_payload = self._resource_lease_payload(request)
            lease = self.ledger.acquire_resource_lease(
                lease_id=lease_payload["lease_id"],
                resource_kind=lease_payload["resource_kind"],
                resource_ref=lease_payload["resource_ref"],
                holder_id=lease_payload["holder_id"],
                lease_until=request.started_at + timedelta(seconds=_AUTHORITY_TTL_SECONDS),
            )
            status_result = self.adapter.create_private_dataset(
                intent=self._status_create_intent(request, status_files),
                files=status_files,
                title=self._status_dataset_ref(request).split("/", 1)[1],
                control_class=ControlClass.MCP_EXCHANGE,
                disposable=True,
            )
            stored = self.ledger.record_checkpoint_acceptance_status_dataset(
                request_id=str(request.request_id),
                status_dataset={
                    "provider_ref": status_result.claim.provider_ref,
                    "exact_version_ref": (
                        f"{status_result.claim.provider_ref}/{status_result.claim.provider_version}"
                    ),
                    "claim": status_result.claim.model_dump(mode="json"),
                    "content_tree_sha256": mapping_sha256(status_files),
                    "status_config_sha256": _sha256(status_files["kaggle_run.json"]),
                    "status_helper_sha256": _sha256(_STATUS_HELPER),
                    "resource_lease": {
                        **lease_payload,
                        "epoch": lease.epoch,
                    },
                },
            )
        result = self.adapter.push_private_notebook_pending_runtime_attestation(
            intent=self._notebook_intent(request, source),
            task_run_id=request.task_run_id,
            source=source,
            title=request.evidence_notebook_ref.split("/", 1)[1],
            code_file="worker.py",
            kernel_type="script",
            language="python",
            control_class=ControlClass.ORCHESTRATOR_PROTECTED,
            disposable=False,
            dataset_sources=self._dataset_sources(request),
            enable_internet=True,
            timeout_seconds=request.timeout_seconds,
        )
        if result.run.task_run_id != request.task_run_id or (
            result.run.provider_ref != request.evidence_notebook_ref
        ):
            raise ValueError("checkpoint provider launch differs from exact task")
        stored = self.ledger.record_checkpoint_acceptance_provider_run(
            request_id=str(request.request_id), provider_run=result.run.model_dump(mode="json")
        )
        return self._reconcile(stored, request, config)

    def checkpoint_acceptance_status(self, request_id: UUID) -> CheckpointAcceptanceLaunchStatus | None:
        stored = self.ledger.checkpoint_acceptance_launch(str(request_id))
        if stored is None:
            return None
        request = CheckpointAcceptanceLaunchRequest.model_validate(stored["request"])
        return self._reconcile(
            stored,
            request,
            CheckpointAcceptanceProductionConfig.model_validate(stored["config"]),
        )

    def _creator_claim_fresh(self, stored: Mapping[str, Any]) -> bool:
        value = datetime.fromisoformat(str(stored["creator_claim_until"]).replace("Z", "+00:00"))
        return value > self.ledger.clock.now()

    def _reconcile(
        self,
        stored: Mapping[str, Any],
        request: CheckpointAcceptanceLaunchRequest,
        config: CheckpointAcceptanceProductionConfig,
    ) -> CheckpointAcceptanceLaunchStatus:
        if stored["result"] is not None:
            return CheckpointAcceptanceLaunchStatus.model_validate(stored["result"])
        if stored["source_attestation_state"] == "MISMATCH":
            return self._terminal(
                self._status(
                    request,
                    config,
                    stored,
                    state="FAIL",
                    failure="CHECKPOINT_RUNTIME_SOURCE_MISMATCH",
                )
            )
        run_payload = stored["provider_run"]
        if run_payload is None:
            if stored["status_dataset"] is not None:
                if self._creator_claim_fresh(stored):
                    return self._status(request, config, stored, state="REQUESTED")
                return self._terminal(
                    self._status(
                        request,
                        config,
                        stored,
                        state="FAIL",
                        failure="CHECKPOINT_PUSH_RESPONSE_AMBIGUOUS",
                    )
                )
            if not self._creator_claim_fresh(stored):
                return self._terminal(
                    self._status(
                        request,
                        config,
                        stored,
                        state="FAIL",
                        failure="CHECKPOINT_STATUS_INPUT_RESPONSE_AMBIGUOUS",
                    )
                )
            return self._status(request, config, stored, state="REQUESTED")
        run = KaggleKernelRunIdentity.model_validate(run_payload)
        status = self.adapter.read_run_status(run)
        if status.state in {KernelState.QUEUED, KernelState.RUNNING}:
            return self._status(request, config, stored, state="RUNNING")
        if status.state is not KernelState.COMPLETE:
            stored = self._cleanup(stored, request)
            return self._terminal(
                self._status(
                    request,
                    config,
                    stored,
                    state="FAIL",
                    failure="CHECKPOINT_PROVIDER_RUN_FAILED",
                )
            )
        runtime_observation = self._runtime_observation(request)
        if (
            stored["source_attestation_state"] != "MATCHED"
            or runtime_observation is None
            or runtime_observation.runtime_source_sha256 != run.source_sha256
            or not runtime_observation.terminal_complete
        ):
            return self._status(request, config, stored, state="RUNNING")
        with tempfile.TemporaryDirectory(prefix="mdh-checkpoint-acceptance-output-") as raw:
            destination = Path(raw)
            output = self.adapter.download_exact_run_output_file(
                run=run,
                file_name=CHECKPOINT_RESULT_FILE,
                destination=destination,
                max_bytes=_MAX_RESULT_BYTES,
            )
            result = CheckpointAcceptanceOperationalResult.model_validate_json(
                (destination / CHECKPOINT_RESULT_FILE).read_bytes()
            )
            if (
                result.scenario != request.scenario
                or result.operation_id != request.operation_id
                or result.task_run_id != request.task_run_id
                or result.source_revision != request.source_revision
                or result.config_sha256 != config.config_sha256
                or result.locator.provider_owner != request.provider_owner
                or result.locator.dataset_ref != request.candidate_dataset_ref
                or result.locator.evidence_notebook_ref != request.evidence_notebook_ref
            ):
                raise ValueError("checkpoint operational result differs from exact launch")
            result_sha = _sha256(canonical_json_bytes(result.model_dump(mode="json")))
            provider = CheckpointProviderRunOutput(
                provider_ref=run.provider_ref,
                provider_run_ref=run.provider_run_ref,
                provider_kernel_id=run.provider_kernel_id,
                source_version=run.source_version,
                source_sha256=run.source_sha256,
                provider_claim_sha256=_sha256(
                    canonical_json_bytes(
                        {"request_sha256": request.request_sha256, "run": run.model_dump(mode="json")}
                    )
                ),
                output_file_sha256=result_sha,
                output_tree_sha256=output.output_tree_sha256,
                output_receipt_sha256=output.receipt_sha256,
            )
        stored = self._cleanup(stored, request)
        state = result.outcome
        candidate = CheckpointAcceptanceLaunchStatus(
            request_id=request.request_id,
            scenario=request.scenario,
            operation_id=request.operation_id,
            task_run_id=request.task_run_id,
            principal_id=request.control_identity.principal_id,
            client_id=request.control_identity.client_id,
            state=state,
            request_sha256=request.request_sha256,
            config_sha256=config.config_sha256,
            result_sha256=result_sha,
            provider_output=provider if state == "LIVE_EVIDENCE_READY" else None,
            status_input=self._status_input(stored),
            runtime_observation=runtime_observation,
            result=result,
            blocker_code=result.blocker_code,
            failure_code=result.failure_code,
            official_adapter_observed=state == "LIVE_EVIDENCE_READY",
        )
        return self._terminal(candidate)

    def _cleanup(
        self, stored: Mapping[str, Any], request: CheckpointAcceptanceLaunchRequest
    ) -> Mapping[str, Any]:
        if stored["cleanup_receipt"] is not None:
            return stored
        status_payload = stored["status_dataset"]
        if not isinstance(status_payload, Mapping):
            raise ValueError("checkpoint status Dataset is absent")
        claim = TaskResourceClaim.model_validate(status_payload["claim"])
        receipt = self.adapter.delete_task_created_resource(
            intent=ProviderEffectIntent.create(
                operation_id=request.operation_id,
                effect_id=uuid5(NAMESPACE_URL, f"checkpoint-status-delete:{request.operation_id}"),
                idempotency_key=f"checkpoint-status-delete:{request.operation_id}",
                task_id=request.task_run_id,
                action=MutationAction.DELETE_DATASET,
                provider_ref=claim.provider_ref,
                expected_fingerprint=claim.fingerprint,
                arguments={
                    "claim_sha256": claim.claim_sha256,
                    "provider_version": claim.provider_version,
                },
                requested_at=request.started_at,
            ),
            claim=claim,
        )
        lease = status_payload.get("resource_lease")
        if not isinstance(lease, Mapping):
            raise ValueError("checkpoint status Dataset lacks its resource lease")
        self.ledger.release_resource_lease_exact(
            str(lease["lease_id"]), str(lease["holder_id"]), int(lease["epoch"])
        )
        return self.ledger.record_checkpoint_acceptance_cleanup(
            request_id=str(request.request_id),
            cleanup_receipt=receipt.model_dump(mode="json"),
        )

    def _terminal(self, status: CheckpointAcceptanceLaunchStatus) -> CheckpointAcceptanceLaunchStatus:
        self.ledger.complete_checkpoint_acceptance_launch(
            request_id=str(status.request_id),
            state=status.state,
            result=status.model_dump(mode="json"),
            result_sha256=_sha256(canonical_json_bytes(status.model_dump(mode="json"))),
        )
        return status

    def _status(
        self,
        request: CheckpointAcceptanceLaunchRequest,
        config: CheckpointAcceptanceProductionConfig,
        stored: Mapping[str, Any],
        *,
        state: Literal["REQUESTED", "RUNNING", "BLOCKED", "FAIL"],
        blocker: str | None = None,
        failure: str | None = None,
    ) -> CheckpointAcceptanceLaunchStatus:
        return CheckpointAcceptanceLaunchStatus(
            request_id=request.request_id,
            scenario=request.scenario,
            operation_id=request.operation_id,
            task_run_id=request.task_run_id,
            principal_id=request.control_identity.principal_id,
            client_id=request.control_identity.client_id,
            state=state,
            request_sha256=request.request_sha256,
            config_sha256=config.config_sha256,
            status_input=self._status_input(stored),
            runtime_observation=self._runtime_observation(request),
            blocker_code=blocker,
            failure_code=failure,
        )

    def _runtime_observation(
        self, request: CheckpointAcceptanceLaunchRequest
    ) -> CheckpointRuntimeObservation | None:
        value = self.ledger.checkpoint_acceptance_event_observation(str(request.request_id))
        return CheckpointRuntimeObservation.model_validate(value) if value is not None else None

    def _status_input(self, stored: Mapping[str, Any]) -> CheckpointStatusDatasetObservation | None:
        value = stored.get("status_dataset")
        if not isinstance(value, Mapping):
            return None
        cleanup = stored.get("cleanup_receipt")
        cleanup_sha = None
        if isinstance(cleanup, Mapping):
            ProviderEffectReceipt.model_validate(cleanup)
            cleanup_sha = _sha256(canonical_json_bytes(cleanup))
        lease = value.get("resource_lease")
        if not isinstance(lease, Mapping):
            raise ValueError("checkpoint status Dataset lacks its resource lease")
        return CheckpointStatusDatasetObservation(
            provider_ref=value["provider_ref"],
            exact_version_ref=value["exact_version_ref"],
            claim_sha256=value["claim"]["claim_sha256"],
            content_tree_sha256=value["content_tree_sha256"],
            status_config_sha256=value["status_config_sha256"],
            status_helper_sha256=value["status_helper_sha256"],
            cleanup_receipt_sha256=cleanup_sha,
            cleaned=cleanup_sha is not None,
            resource_lease=CheckpointResourceLeaseObservation(
                **lease,
                released=cleanup_sha is not None,
            ),
        )

    def _status_dataset_ref(self, request: CheckpointAcceptanceLaunchRequest) -> str:
        return f"{request.provider_owner}/mdh-acc-status-{request.request_id.hex}"

    def _resource_lease_payload(self, request: CheckpointAcceptanceLaunchRequest) -> dict[str, str]:
        return {
            "lease_id": str(uuid5(NAMESPACE_URL, f"checkpoint-evidence-lease:{request.operation_id}")),
            "resource_kind": "kaggle_notebook",
            "resource_ref": request.evidence_notebook_ref,
            "holder_id": str(request.task_run_id),
            "lease_until": (
                request.started_at + timedelta(seconds=_AUTHORITY_TTL_SECONDS)
            ).isoformat(),
        }

    def _status_files(self, request: CheckpointAcceptanceLaunchRequest, token: str) -> dict[str, bytes]:
        value = {
            "schema_version": "my-data-hub-kaggle-run.v1",
            "run_id": str(request.request_id),
            "attempt_id": str(request.control_identity.attempt_id),
            "kind": "checkpoint-acceptance",
            "notebook": request.evidence_notebook_ref,
            "callback_url": self.deployment.control_base_url,
            "token": token,
            "resource_leases": [self._resource_lease_payload(request)],
        }
        encoded = canonical_json_bytes(value)
        if len(encoded) > _MAX_STATUS_BYTES:
            raise ValueError("checkpoint status config exceeds 16 KiB")
        return {"kaggle_run.json": encoded, "kaggle_status_client.py": _STATUS_HELPER}

    def _status_create_intent(
        self, request: CheckpointAcceptanceLaunchRequest, files: Mapping[str, bytes]
    ) -> ProviderEffectIntent:
        return ProviderEffectIntent.create(
            operation_id=request.operation_id,
            effect_id=uuid5(NAMESPACE_URL, f"checkpoint-status-create:{request.operation_id}"),
            idempotency_key=f"checkpoint-status-create:{request.operation_id}",
            task_id=request.task_run_id,
            action=MutationAction.CREATE_DATASET,
            provider_ref=self._status_dataset_ref(request),
            arguments={
                "content_tree_sha256": mapping_sha256(files),
                "control_class": ControlClass.MCP_EXCHANGE.value,
                "disposable": True,
            },
            requested_at=request.started_at,
        )

    def _notebook_intent(
        self, request: CheckpointAcceptanceLaunchRequest, source: bytes
    ) -> ProviderEffectIntent:
        return ProviderEffectIntent.create(
            operation_id=request.operation_id,
            effect_id=uuid5(NAMESPACE_URL, f"checkpoint-acceptance-launch:{request.operation_id}"),
            idempotency_key=f"checkpoint-acceptance-launch:{request.operation_id}",
            task_id=request.task_run_id,
            action=MutationAction.PUSH_NOTEBOOK,
            provider_ref=request.evidence_notebook_ref,
            arguments={
                "task_run_id": str(request.task_run_id),
                "source_sha256": _sha256(source),
                "dataset_sources": self._dataset_sources(request),
                "control_class": ControlClass.ORCHESTRATOR_PROTECTED.value,
                "disposable": False,
            },
            requested_at=request.started_at,
        )

    def _config(
        self, request: CheckpointAcceptanceLaunchRequest, status_files: Mapping[str, bytes]
    ) -> CheckpointAcceptanceProductionConfig:
        verifier = request.verifier_input
        return CheckpointAcceptanceProductionConfig(
            schema_version="my-data-hub-checkpoint-acceptance-production-config.v1",
            scenario=request.scenario,
            operation_id=request.operation_id,
            task_run_id=request.task_run_id,
            source_revision=request.source_revision,
            started_at=request.started_at,
            provider_owner=request.provider_owner,
            dataset_ref=request.candidate_dataset_ref,
            evidence_notebook_ref=request.evidence_notebook_ref,
            verifier_notebook_ref=request.verifier_notebook_ref,
            template_dataset_version_ref=request.template_input.exact_version_ref,
            template_claim_sha256=request.template_input.claim_sha256,
            template_directory=Path("/kaggle/working/checkpoint-template"),
            template_manifest_sha256=request.template_input.manifest_sha256,
            template_content_sha256=request.template_input.content_sha256,
            verifier=(
                CheckpointAcceptanceVerifierAsset(
                    dataset_version_ref=verifier.exact_version_ref,
                    claim_sha256=verifier.claim_sha256,
                    path=Path("/kaggle/working/checkpoint-verifier/worker.py"),
                    source_sha256=verifier.source_sha256,
                )
                if verifier is not None
                else None
            ),
            status_dataset_version_ref=f"{self._status_dataset_ref(request)}/1",
            status_config_sha256=_sha256(status_files["kaggle_run.json"]),
            status_helper_sha256=_sha256(status_files["kaggle_status_client.py"]),
            working_directory=Path("/kaggle/working/checkpoint-acceptance"),
            control_base_url=self.deployment.control_base_url,
            control_identity=CheckpointAcceptanceControlIdentity(
                request_id=request.request_id,
                task_run_id=request.task_run_id,
                attempt_id=request.control_identity.attempt_id,
            ),
            timeout_seconds=900,
        )

    def _dataset_sources(self, request: CheckpointAcceptanceLaunchRequest) -> tuple[str, ...]:
        refs = [
            self.deployment.runtime_input.exact_version_ref,
            request.template_input.exact_version_ref,
            f"{self._status_dataset_ref(request)}/1",
        ]
        if request.verifier_input is not None:
            refs.append(request.verifier_input.exact_version_ref)
        if len(refs) != len(set(refs)):
            raise ValueError("checkpoint acceptance inputs must be distinct exact versions")
        return tuple(refs)

    def _render_source(
        self, request: CheckpointAcceptanceLaunchRequest, config: CheckpointAcceptanceProductionConfig
    ) -> bytes:
        runtime = self.deployment.runtime_input
        runtime_mount = f"/kaggle/input/{runtime.provider_ref.split('/', 1)[1]}"
        template_mount = f"/kaggle/input/{request.template_input.provider_ref.split('/', 1)[1]}"
        status_mount = f"/kaggle/input/{self._status_dataset_ref(request).split('/', 1)[1]}"
        verifier_mount = (
            f"/kaggle/input/{request.verifier_input.provider_ref.split('/', 1)[1]}"
            if request.verifier_input is not None
            else None
        )
        bindings = json.dumps(self.deployment.kaggle_secret_bindings, sort_keys=True)
        payload = canonical_json_bytes(config.model_dump(mode="json"))
        lines = [
            "import hashlib, importlib.util, json, os, pathlib, shutil, subprocess, sys, time",
            "from kaggle_secrets import UserSecretsClient",
            f"TASK_RUN_ID = {str(request.task_run_id)!r}",
            f"ATTEMPT_ID = {str(request.control_identity.attempt_id)!r}",
            f"NOTEBOOK_REF = {request.evidence_notebook_ref!r}",
            "EXECUTED_SOURCE_SHA256 = hashlib.sha256(pathlib.Path(__file__).read_bytes()).hexdigest()",
            f"status_root = pathlib.Path({status_mount!r})",
            "status_config = status_root / 'kaggle_run.json'",
            "status_helper = status_root / 'kaggle_status_client.py'",
            (
                "if hashlib.sha256(status_config.read_bytes()).hexdigest() != "
                f"{config.status_config_sha256!r}: raise RuntimeError('status config hash mismatch')"
            ),
            (
                "if hashlib.sha256(status_helper.read_bytes()).hexdigest() != "
                f"{config.status_helper_sha256!r}: raise RuntimeError('status helper hash mismatch')"
            ),
            "spec = importlib.util.spec_from_file_location('mdh_status_bootstrap', status_helper)",
            "module = importlib.util.module_from_spec(spec)",
            "spec.loader.exec_module(module)",
            (
                "status = module.load_run_config(status_config, request_id=TASK_RUN_ID, "
                "attempt_id=ATTEMPT_ID, notebook=NOTEBOOK_REF)"
            ),
            "secrets = UserSecretsClient()",
            f"for env_name, secret_name in {bindings}.items():",
            "    os.environ[env_name] = secrets.get_secret(secret_name)",
            f"runtime_root = pathlib.Path({runtime_mount!r})",
            f"wheel = runtime_root / {runtime.wheel_file!r}",
            f"entrypoint = runtime_root / {runtime.entrypoint_file!r}",
            (
                "if hashlib.sha256(wheel.read_bytes()).hexdigest() != "
                f"{runtime.wheel_sha256!r}: raise RuntimeError('runtime wheel hash mismatch')"
            ),
            (
                "if hashlib.sha256(entrypoint.read_bytes()).hexdigest() != "
                f"{runtime.entrypoint_sha256!r}: raise RuntimeError('entrypoint hash mismatch')"
            ),
            "subprocess.run([sys.executable, '-m', 'pip', 'install', '--no-deps', str(wheel)], check=True)",
            "from uuid import UUID",
            "from my_data_hub.runtime_sdk import AcceptanceCallbackIdentity, RuntimeClient",
            "working = pathlib.Path('/kaggle/working/checkpoint-acceptance')",
            "working.mkdir(mode=0o700)",
            "status_client = RuntimeClient(",
            "    callback_url=status['callback_url'].rstrip('/') + '/internal/acceptance/events',",
            "    run_secret=os.environ['MY_DATA_HUB_RUN_SECRET'],",
            "    run_id=TASK_RUN_ID, attempt_id=ATTEMPT_ID, service_instance_id=TASK_RUN_ID,",
            f"    source_identity=NOTEBOOK_REF, source_version={request.source_revision!r}, epoch=1,",
            "    spool_path=working / 'kaggle_status_events.jsonl',",
            "    acceptance_identity=AcceptanceCallbackIdentity(",
            "        request_id=UUID(TASK_RUN_ID), task_run_id=UUID(TASK_RUN_ID),",
            "        attempt_id=UUID(ATTEMPT_ID)),",
            ")",
            "def emit(name, phase, state, progress):",
            "    return status_client.emit_donor_envelope({",
            "        'run_id': TASK_RUN_ID, 'event': name,",
            "        'event_uid': f'{TASK_RUN_ID}:{name}:{progress.get(\"sequence\", 0)}',",
            "        'phase': phase, 'status': state, 'progress': progress})",
            "emit('kernel_started', 'bootstrap', 'running', {'sequence': 0, 'completed_steps': 0,",
            "     'runtime_source_sha256': EXECUTED_SOURCE_SHA256})",
            "template = pathlib.Path('/kaggle/working/checkpoint-template')",
            f"shutil.copytree(pathlib.Path({template_mount!r}), template, dirs_exist_ok=False)",
        ]
        if verifier_mount is not None:
            lines.extend(
                [
                    "verifier_dir = pathlib.Path('/kaggle/working/checkpoint-verifier')",
                    "verifier_dir.mkdir(mode=0o700)",
                    (
                        f"shutil.copyfile(pathlib.Path({verifier_mount!r}) / 'worker.py', "
                        "verifier_dir / 'worker.py')"
                    ),
                ]
            )
        lines.extend(
            [
                "config_path = working / 'checkpoint-acceptance-config.json'",
                f"config_path.write_bytes({payload!r})",
                "os.chmod(config_path, 0o600)",
                "emit('preflight_ok', 'preflight', 'ready', {'sequence': 0, 'completed_steps': 1})",
                (
                    "status_client.emit_donor_envelope({'run_id': TASK_RUN_ID, "
                    "'event': 'resource_acquire', 'event_uid': f'{TASK_RUN_ID}:resource_acquire:0', "
                    "'phase': 'execute', 'status': 'acquired', 'progress': {'sequence': 0}, "
                    "'resource': status['resource_leases'][0]})"
                ),
                "started = time.monotonic()",
                (
                    "process = subprocess.Popen([sys.executable, str(entrypoint), '--config', "
                    "str(config_path), '--output', str(working / 'operational-result.json')])"
                ),
                "heartbeat = 1",
                "emit('alive', 'execute', 'running', {'sequence': heartbeat, 'heartbeat_count': heartbeat, "
                "     'elapsed_seconds': 0, 'completed_steps': 1})",
                "while process.poll() is None:",
                "    try:",
                "        process.wait(timeout=30)",
                "    except subprocess.TimeoutExpired:",
                "        heartbeat += 1",
                "        emit('alive', 'execute', 'running', {'sequence': heartbeat, "
                "             'heartbeat_count': heartbeat,",
                "             'elapsed_seconds': int(time.monotonic() - started), 'completed_steps': 1})",
                "if process.returncode != 0:",
                "    emit('failed', 'execute', 'failed', {'sequence': heartbeat + 1, "
                "         'heartbeat_count': heartbeat, 'completed_steps': 1})",
                "    raise SystemExit(process.returncode)",
                "result_path = working / 'operational-result.json'",
                "result_sha = hashlib.sha256(result_path.read_bytes()).hexdigest()",
                "emit('report_written', 'evidence', 'ready', {'sequence': heartbeat + 1, "
                "     'heartbeat_count': heartbeat, 'completed_steps': 2, 'result_sha256': result_sha})",
                (
                    "status_client.emit_donor_envelope({'run_id': TASK_RUN_ID, "
                    "'event': 'resource_release', 'event_uid': f'{TASK_RUN_ID}:resource_release:0', "
                    "'phase': 'cleanup', 'status': 'released', "
                    "'progress': {'sequence': 0, 'completed_steps': 3}, "
                    "'resource': status['resource_leases'][0]})"
                ),
                "emit('terminal', 'complete', 'completed', {'sequence': heartbeat + 2, "
                "     'heartbeat_count': heartbeat, 'completed_steps': 4, 'result_sha256': result_sha})",
                "if not status_client.flush_pending(max_events=100):",
                "    raise RuntimeError('checkpoint acceptance callbacks remain pending')",
            ]
        )
        encoded = ("\n".join(lines) + "\n").encode()
        if len(encoded) > _MAX_SOURCE_BYTES:
            raise ValueError("checkpoint acceptance launch source exceeds 512 KiB")
        return encoded


def checkpoint_acceptance_deployment_from_environment() -> CheckpointAcceptanceDeployment | None:
    path_value = os.getenv("MY_DATA_HUB_CHECKPOINT_ACCEPTANCE_DEPLOYMENT_FILE", "").strip()
    if not path_value:
        return None
    path = Path(path_value).expanduser()
    if not path.is_absolute() or path.is_symlink() or not path.is_file() or path.stat().st_mode & 0o077:
        raise ValueError("checkpoint acceptance deployment file must be absolute/private/regular")
    if not 1 <= path.stat().st_size <= 256 * 1024:
        raise ValueError("checkpoint acceptance deployment file exceeds its bound")
    return CheckpointAcceptanceDeployment.model_validate_json(path.read_bytes())
