from __future__ import annotations

from datetime import datetime
from typing import Any, Final, Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from my_data_hub.voice_intake.contracts import SESSION_ID_PATTERN, SHA256_PATTERN, ModelUsage

API_VERSION: Final = "2.0"
CAPTURE_POLICIES = ("continuous_v1", "voice_activity_auto_pause_v1")
SESSION_STATES = (
    "receiving",
    "queued",
    "normalizing",
    "transcribing",
    "summarizing",
    "publishing",
    "verifying",
    "waiting_quota",
    "retryable_error",
    "reconciliation_required",
    "published_verified",
)
type VoiceSessionState = Literal[
    "receiving",
    "queued",
    "normalizing",
    "transcribing",
    "summarizing",
    "publishing",
    "verifying",
    "waiting_quota",
    "retryable_error",
    "reconciliation_required",
    "published_verified",
]


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


def _aware_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("timestamp must include an explicit UTC offset")
    return parsed


class AudioFormat(StrictModel):
    container: Literal["mp4"]
    codec: Literal["aac_lc"]
    mime_type: Literal["audio/mp4"]
    sample_rate_hz: Literal[16000]
    channels: Literal[1]
    target_bitrate_bps: int = Field(ge=16_000, le=64_000)


class VadMetadata(StrictModel):
    engine: str = Field(min_length=1, max_length=64)
    engine_version: str = Field(min_length=1, max_length=128)
    mode: int = Field(ge=0, le=3)
    frame_ms: Literal[10, 20, 30]
    config_version: str = Field(min_length=1, max_length=128)


class SessionCreateRequest(StrictModel):
    session_id: str = Field(pattern=SESSION_ID_PATTERN)
    started_at: str = Field(min_length=10, max_length=64)
    timezone: str = Field(min_length=1, max_length=128)
    device_label: str = Field(min_length=1, max_length=128)
    client_version: str = Field(min_length=1, max_length=32)
    capture_policy: Literal["continuous_v1", "voice_activity_auto_pause_v1"]
    audio_format: AudioFormat
    vad: VadMetadata | None = None

    @model_validator(mode="after")
    def validate_vad(self) -> SessionCreateRequest:
        if self.capture_policy == "voice_activity_auto_pause_v1" and self.vad is None:
            raise ValueError("VAD metadata is required for automatic pause")
        started = _aware_datetime(self.started_at)
        try:
            zone = ZoneInfo(self.timezone)
        except ZoneInfoNotFoundError as exc:
            raise ValueError("timezone must be an IANA timezone") from exc
        if started.utcoffset() != started.astimezone(zone).utcoffset():
            raise ValueError("started_at offset does not match timezone")
        return self


class ChunkManifestItem(StrictModel):
    chunk_index: int = Field(ge=0, le=10_000)
    sha256: str = Field(pattern=SHA256_PATTERN)
    duration_ms: int = Field(gt=0)
    audio_start_ms: int = Field(ge=0)
    audio_end_ms: int = Field(gt=0)
    wall_start_ms: int = Field(ge=0)
    wall_end_ms: int = Field(gt=0)

    @model_validator(mode="after")
    def validate_ranges(self) -> ChunkManifestItem:
        if self.audio_end_ms <= self.audio_start_ms or self.wall_end_ms <= self.wall_start_ms:
            raise ValueError("chunk ranges must increase")
        return self


class SessionCompleteRequest(StrictModel):
    ended_at: str = Field(min_length=10, max_length=64)
    wall_elapsed_ms: int = Field(gt=0)
    manual_pause_ms: int = Field(ge=0)
    recorded_audio_ms: int = Field(gt=0)
    auto_silence_skipped_ms: int = Field(ge=0)
    chunk_count: int = Field(ge=1, le=10_000)
    chunks: list[ChunkManifestItem] = Field(min_length=1, max_length=10_000)

    @model_validator(mode="after")
    def validate_manifest(self) -> SessionCompleteRequest:
        _aware_datetime(self.ended_at)
        if self.chunk_count != len(self.chunks):
            raise ValueError("chunk_count does not match chunks")
        if [item.chunk_index for item in self.chunks] != list(range(self.chunk_count)):
            raise ValueError("chunks must be contiguous and ordered from zero")
        if self.chunks[0].audio_start_ms != 0:
            raise ValueError("audio timeline must start at zero")
        for previous, current in zip(self.chunks, self.chunks[1:], strict=False):
            if current.audio_start_ms != previous.audio_end_ms:
                raise ValueError("audio timeline must be contiguous")
            if current.wall_start_ms < previous.wall_end_ms:
                raise ValueError("wall timeline must not overlap")
        if abs(self.chunks[-1].audio_end_ms - self.recorded_audio_ms) > 2_000:
            raise ValueError("audio timeline does not match recorded duration")
        if self.chunks[-1].wall_end_ms > self.wall_elapsed_ms + 2_000:
            raise ValueError("wall timeline exceeds elapsed duration")
        accounted_wall_ms = self.recorded_audio_ms + self.manual_pause_ms + self.auto_silence_skipped_ms
        if abs(accounted_wall_ms - self.wall_elapsed_ms) > 2_000:
            raise ValueError("wall elapsed does not match recorded, manual pause, and silence durations")
        return self


class InferenceReceipt(StrictModel):
    value: dict[str, Any]
    request_uid: str = Field(min_length=1, max_length=128)
    limiter: dict[str, Any] = Field(default_factory=dict)
    finish_reason: Literal["STOP"] = "STOP"
    usage: ModelUsage | None = None


class SegmentTranscriptPayload(StrictModel):
    transcript: str = Field(min_length=1, max_length=200_000)
    language: str = Field(min_length=2, max_length=32)
    uncertain_fragments: list[str] = Field(default_factory=list, max_length=100)
    coverage_start_ms: int = Field(ge=0)
    coverage_end_ms: int = Field(gt=0)

    @field_validator("uncertain_fragments")
    @classmethod
    def validate_uncertain(cls, values: list[str]) -> list[str]:
        if any(not value.strip() or len(value) > 2_000 for value in values):
            raise ValueError("uncertain fragments must be bounded non-empty strings")
        return values

    @model_validator(mode="after")
    def validate_coverage_range(self) -> SegmentTranscriptPayload:
        if self.coverage_end_ms <= self.coverage_start_ms:
            raise ValueError("coverage range must increase")
        return self


class SegmentPlausibilityEvidence(StrictModel):
    """Bounded, content-free evidence used to reject suspicious segment output."""

    expected_speech_ms: int = Field(gt=0)
    transcript_characters: int = Field(ge=0)
    transcript_words: int = Field(ge=0)
    alphanumeric_characters: int = Field(ge=0)
    alphanumeric_per_speech_second: float = Field(ge=0)
    words_per_speech_minute: float = Field(ge=0)


class SegmentInferenceReceipt(StrictModel):
    """Immutable provider/content receipt for one original source segment."""

    schema_version: Literal["2.0.0"] = "2.0.0"
    chunk_index: int = Field(ge=0, le=10_000)
    source_sha256: str = Field(pattern=SHA256_PATTERN)
    input_audio_sha256: str = Field(pattern=SHA256_PATTERN)
    input_audio_mime_type: Literal["audio/mpeg"]
    source_audio_start_ms: int = Field(ge=0)
    source_audio_end_ms: int = Field(gt=0)
    coverage_start_ms: int = Field(ge=0)
    coverage_end_ms: int = Field(gt=0)
    coverage_ms: int = Field(gt=0)
    coverage_ratio: float = Field(ge=0, le=1)
    finish_reason: Literal["STOP"]
    transcript_receipt_sha256: str = Field(pattern=SHA256_PATTERN)
    value: dict[str, Any]
    request_uid: str = Field(min_length=1, max_length=128)
    limiter: dict[str, Any] = Field(default_factory=dict)
    usage: ModelUsage
    plausibility: SegmentPlausibilityEvidence

    @field_validator("usage")
    @classmethod
    def validate_bounded_usage(cls, usage: ModelUsage) -> ModelUsage:
        if any(count > 10_000_000 for count in usage.model_dump().values()):
            raise ValueError("segment usage is outside the bounded range")
        return usage

    @model_validator(mode="after")
    def validate_coverage(self) -> SegmentInferenceReceipt:
        source_ms = self.source_audio_end_ms - self.source_audio_start_ms
        if source_ms <= 0:
            raise ValueError("source audio range must increase")
        if (
            self.coverage_start_ms != self.source_audio_start_ms
            or self.coverage_end_ms != self.source_audio_end_ms
            or self.coverage_ms != source_ms
            or self.coverage_ratio != 1.0
        ):
            raise ValueError("segment receipt must cover the complete source range")
        if self.plausibility.expected_speech_ms != source_ms:
            raise ValueError("plausibility evidence must account for the complete source range")
        return self


class PublicationReceipt(StrictModel):
    github_url: str
    github_commit_sha: str = Field(pattern=r"^[0-9a-f]{40}$")
    github_verified: bool


class StatusResponse(StrictModel):
    api_version: Literal["2.0"] = "2.0"
    session_id: str = Field(pattern=SESSION_ID_PATTERN)
    state: VoiceSessionState
    recording_finished: bool = False
    chunks_expected: int | None = None
    chunks_received: int = 0
    bytes_received: int = 0
    recorded_audio_ms: int | None = None
    auto_silence_skipped_ms: int | None = None
    # Preserve the historical two-stage projection when an old store omits
    # these fields; the v2 segment ledger supplies the dynamic N+1 values.
    inference_batches_total: int = Field(default=2, ge=0)
    inference_batches_completed: int = Field(default=0, ge=0)
    gemini_requests_total: int = Field(default=2, ge=0)
    gemini_requests_completed: int = Field(default=0, ge=0)
    transcription_complete: bool = False
    summary_complete: bool = False
    github_verified: bool = False
    server_audio_purged: bool = False
    github_url: str | None = None
    github_commit_sha: str | None = None
    retryable: bool = False
    retry_at: str | None = None
    error_code: str | None = None
    reconciliation_required: bool = False
    transcription_request_uid: str | None = None
    summary_request_uid: str | None = None
    transcription_limiter: dict[str, Any] | None = None
    summary_limiter: dict[str, Any] | None = None
    transcription_segments_total: int = Field(default=0, ge=0)
    transcription_segments_completed: int = Field(default=0, ge=0)
    transcription_coverage_complete: bool = False
    content_verification_status: Literal["pending", "failed", "passed"] = "pending"
    content_verified: bool = False
    publication_verified: bool = False
    purge_authorized: bool = False
    audio_purged: bool = False
    client_audio_purge_allowed: bool = False
    legacy_unverified_purge: bool = False

    @model_validator(mode="after")
    def validate_safe_progress_projection(self) -> StatusResponse:
        if self.inference_batches_completed > self.inference_batches_total:
            raise ValueError("completed inference batch count exceeds expected batches")
        if self.gemini_requests_completed > self.gemini_requests_total:
            raise ValueError("completed Gemini request count exceeds expected requests")
        if self.transcription_segments_completed > self.transcription_segments_total:
            raise ValueError("completed segment count exceeds expected segments")
        if self.content_verified != (self.content_verification_status == "passed"):
            raise ValueError("content verification status and flag disagree")
        if self.content_verified and not self.transcription_coverage_complete:
            raise ValueError("content verification requires complete transcription coverage")
        if self.purge_authorized and not (self.content_verified and self.publication_verified):
            raise ValueError("purge authorization requires content and publication verification")
        if self.audio_purged != self.server_audio_purged:
            raise ValueError("physical audio purge flags must agree")
        if self.legacy_unverified_purge:
            if not (self.audio_purged and self.server_audio_purged):
                raise ValueError("legacy purge marker requires truthful historical physical purge")
            if self.content_verified or self.purge_authorized or self.client_audio_purge_allowed:
                raise ValueError("legacy unverified purge cannot authorize new deletion")
        elif self.audio_purged and not (
            self.content_verified
            and self.publication_verified
            and self.purge_authorized
            and self.client_audio_purge_allowed
        ):
            raise ValueError("audio purge requires every durable verification gate")
        if self.client_audio_purge_allowed and not (
            self.content_verified and self.purge_authorized and self.audio_purged and self.server_audio_purged
        ):
            raise ValueError("client purge permission requires verified physical server purge")
        return self
