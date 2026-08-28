from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from my_data_hub.voice_intake.contracts import SESSION_ID_PATTERN, SHA256_PATTERN

API_VERSION = "2.0"
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


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


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
        return self


class InferenceReceipt(StrictModel):
    value: dict[str, Any]
    request_uid: str = Field(min_length=1, max_length=128)
    limiter: dict[str, Any] = Field(default_factory=dict)


class PublicationReceipt(StrictModel):
    github_url: str
    github_commit_sha: str = Field(pattern=r"^[0-9a-f]{40}$")
    github_verified: bool


class StatusResponse(StrictModel):
    api_version: Literal["2.0"] = API_VERSION
    session_id: str = Field(pattern=SESSION_ID_PATTERN)
    state: Literal[*SESSION_STATES]
    recording_finished: bool = False
    chunks_expected: int | None = None
    chunks_received: int = 0
    bytes_received: int = 0
    recorded_audio_ms: int | None = None
    auto_silence_skipped_ms: int | None = None
    inference_batches_total: int = 2
    inference_batches_completed: int = 0
    gemini_requests_total: int = 2
    gemini_requests_completed: int = 0
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
