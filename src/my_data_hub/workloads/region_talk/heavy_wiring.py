"""Private DB-work to evidence-heavy runtime wiring.

Only the child Notebook sees these envelopes.  The immutable sparse work hash is
kept as dispatch identity while the server-computed enrichment hash binds the
larger evidence pack.  Neither object is suitable for a control-plane journal.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import Field, model_validator

from .heavy_contracts import (
    SHA256_PATTERN,
    FinalVerifierInput,
    HeavyRuntimeUnavailable,
    ImageScoringInput,
    StrictModel,
    WriterInput,
    canonical_sha256,
    validate_heavy_result_against_input,
    validate_heavy_stage_input,
)
from .heavy_dag_bridge import (
    DagFinalVerifierWorkInput,
    DagImageWorkInput,
    DagWriterWorkInput,
    final_guard_metrics,
    image_guard_metrics,
    writer_guard_metrics,
)
from .heavy_runtimes import (
    DeterministicFinalVerifierRuntime,
    DeterministicImageRuntime,
    DeterministicWriterRuntime,
)


class HeavyStageInputReceipt(StrictModel):
    schema_version: Literal["region-talk-heavy-stage-input-receipt.v1"]
    status: Literal["READY", "UNAVAILABLE"]
    stage: Literal["image_scoring", "final_verifier", "writer"]
    work_input_fingerprint: str = Field(pattern=SHA256_PATTERN)
    enrichment_sha256: str | None = Field(default=None, pattern=SHA256_PATTERN)
    dag_input: dict[str, Any]
    heavy_input: dict[str, Any] | None = None
    reason_code: str = Field(default="", pattern=r"^(?:|[A-Z][A-Z0-9_]{0,99})$")
    publication_dispatch: Literal[False] = False
    notification_dispatch: Literal[False] = False
    receipt_sha256: str = Field(pattern=SHA256_PATTERN)

    @model_validator(mode="after")
    def exact_receipt(self) -> HeavyStageInputReceipt:
        ready = self.status == "READY"
        if ready != (self.heavy_input is not None and self.enrichment_sha256 is not None):
            raise ValueError("heavy input readiness differs from its evidence")
        if ready == bool(self.reason_code):
            raise ValueError("heavy input reason differs from readiness")
        if self.heavy_input is not None:
            if self.heavy_input.get("work_input_fingerprint") != self.work_input_fingerprint:
                raise ValueError("rich input differs from immutable DB work")
            if self.heavy_input.get("enrichment_sha256") != self.enrichment_sha256:
                raise ValueError("rich input differs from server enrichment hash")
        payload = self.model_dump(mode="json")
        payload.pop("receipt_sha256")
        if canonical_sha256(payload) != self.receipt_sha256:
            raise ValueError("heavy input receipt_sha256 differs")
        return self


class HeavyStagePrivateResult(StrictModel):
    schema_version: Literal["region-talk-heavy-stage-private-result.v1"] = (
        "region-talk-heavy-stage-private-result.v1"
    )
    stage: Literal["image_scoring", "final_verifier", "writer"]
    work_input_fingerprint: str = Field(pattern=SHA256_PATTERN)
    enrichment_sha256: str = Field(pattern=SHA256_PATTERN)
    input_fingerprint: str = Field(pattern=SHA256_PATTERN)
    result_sha256: str = Field(pattern=SHA256_PATTERN)
    result_data: dict[str, Any]
    publication_dispatch: Literal[False] = False
    notification_dispatch: Literal[False] = False

    @model_validator(mode="after")
    def exact_result(self) -> HeavyStagePrivateResult:
        if self.result_data.get("input_fingerprint") != self.input_fingerprint:
            raise ValueError("private result input fingerprint differs")
        if self.result_data.get("result_sha256") != self.result_sha256:
            raise ValueError("private result hash differs")
        return self


class AttachedHeavyStageExecution(StrictModel):
    metrics: dict[str, Any]
    artifact_sha256: str = Field(pattern=SHA256_PATTERN)
    private_result: HeavyStagePrivateResult


class HeavyAttachedStageRuntime:
    """Adapt one exact R20 runtime to the generic private Notebook seam."""

    def __init__(
        self,
        *,
        stage: str,
        runtime: DeterministicImageRuntime | DeterministicFinalVerifierRuntime | DeterministicWriterRuntime,
    ) -> None:
        if stage not in {"image_scoring", "final_verifier", "writer"}:
            raise ValueError("heavy runtime stage is not supported")
        self.stage = stage
        self.runtime = runtime

    @property
    def producer_exact_id(self) -> str:
        return self.runtime.producer_exact_id

    def execute(self, *, stage: str, input_fingerprint: str, payload: Any, **_: Any) -> AttachedHeavyStageExecution:
        if stage != self.stage:
            raise ValueError("heavy runtime differs from requested stage")
        receipt = HeavyStageInputReceipt.model_validate(payload.input_data)
        if receipt.stage != stage or receipt.work_input_fingerprint != input_fingerprint:
            raise ValueError("heavy evidence receipt differs from DB work")
        if receipt.status != "READY" or receipt.heavy_input is None or receipt.enrichment_sha256 is None:
            raise HeavyRuntimeUnavailable(receipt.reason_code or "heavy evidence is unavailable")
        request = validate_heavy_stage_input(
            stage,
            receipt.heavy_input,
            expected_work_input_fingerprint=input_fingerprint,
        )
        if isinstance(request, ImageScoringInput) and isinstance(self.runtime, DeterministicImageRuntime):
            result = self.runtime.execute(request)
            metrics = image_guard_metrics(result, DagImageWorkInput.model_validate(receipt.dag_input))
        elif isinstance(request, FinalVerifierInput) and isinstance(
            self.runtime, DeterministicFinalVerifierRuntime
        ):
            result = self.runtime.execute(request)
            metrics = final_guard_metrics(result, DagFinalVerifierWorkInput.model_validate(receipt.dag_input))
        elif isinstance(request, WriterInput) and isinstance(self.runtime, DeterministicWriterRuntime):
            result = self.runtime.execute(request)
            metrics = writer_guard_metrics(result, DagWriterWorkInput.model_validate(receipt.dag_input))
        else:
            raise ValueError("heavy runtime implementation differs from typed input")
        validate_heavy_result_against_input(request, result)
        private = HeavyStagePrivateResult(
            stage=stage,
            work_input_fingerprint=input_fingerprint,
            enrichment_sha256=receipt.enrichment_sha256,
            input_fingerprint=request.input_fingerprint,
            result_sha256=result.result_sha256,
            result_data=result.model_dump(mode="json"),
        )
        return AttachedHeavyStageExecution(
            metrics=metrics,
            artifact_sha256=result.result_sha256,
            private_result=private,
        )


__all__ = [
    "AttachedHeavyStageExecution",
    "HeavyAttachedStageRuntime",
    "HeavyStageInputReceipt",
    "HeavyStagePrivateResult",
]
