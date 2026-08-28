from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

SESSION_ID_PATTERN = r"^voice-[0-9]{8}-[0-9]{6}-[a-f0-9]{8}$"
SHA256_PATTERN = r"^[0-9a-f]{64}$"


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class SessionCreateRequest(StrictModel):
    session_id: str = Field(pattern=SESSION_ID_PATTERN)
    started_at: str = Field(min_length=10, max_length=64)
    timezone: str = Field(min_length=1, max_length=128)
    device_label: str = Field(min_length=1, max_length=128)


class TranscriptPayload(StrictModel):
    transcript: str = Field(min_length=1, max_length=200_000)
    language: str = Field(default="ru-RU", min_length=2, max_length=32)
    uncertain_fragments: list[str] = Field(default_factory=list, max_length=100)

    @field_validator("uncertain_fragments")
    @classmethod
    def validate_uncertain(cls, values: list[str]) -> list[str]:
        if any(not value.strip() or len(value) > 2_000 for value in values):
            raise ValueError("uncertain fragments must be bounded non-empty strings")
        return values


class VoiceTask(StrictModel):
    text: str = Field(min_length=1, max_length=10_000)
    owner: str | None = Field(default=None, max_length=500)
    deadline: str | None = Field(default=None, max_length=200)
    explicitly_stated: bool = True


class SummaryPayload(StrictModel):
    title: str = Field(min_length=1, max_length=240)
    short_summary: str = Field(min_length=1, max_length=20_000)
    detailed_summary: str = Field(min_length=1, max_length=120_000)
    theses: list[str] = Field(default_factory=list, max_length=300)
    ideas: list[str] = Field(default_factory=list, max_length=300)
    decisions: list[str] = Field(default_factory=list, max_length=300)
    tasks: list[VoiceTask] = Field(default_factory=list, max_length=300)
    facts: list[str] = Field(default_factory=list, max_length=300)
    entities: list[str] = Field(default_factory=list, max_length=300)
    related_projects: list[str] = Field(default_factory=list, max_length=300)
    open_questions: list[str] = Field(default_factory=list, max_length=300)
    contradictions: list[str] = Field(default_factory=list, max_length=300)
    uncertain_fragments: list[str] = Field(default_factory=list, max_length=300)
    tags: list[str] = Field(default_factory=list, max_length=100)

    @field_validator(
        "theses",
        "ideas",
        "decisions",
        "facts",
        "entities",
        "related_projects",
        "open_questions",
        "contradictions",
        "uncertain_fragments",
        "tags",
    )
    @classmethod
    def validate_strings(cls, values: list[str]) -> list[str]:
        if any(not value.strip() or len(value) > 10_000 for value in values):
            raise ValueError("summary collections must contain bounded non-empty strings")
        return values


class TranscriptChunk(StrictModel):
    chunk_index: int = Field(ge=0, le=10_000)
    start_ms: int = Field(ge=0)
    end_ms: int = Field(gt=0)
    sha256: str = Field(pattern=SHA256_PATTERN)
    transcript: TranscriptPayload

    @model_validator(mode="after")
    def validate_range(self) -> TranscriptChunk:
        if self.end_ms <= self.start_ms:
            raise ValueError("chunk end must be after start")
        if self.end_ms - self.start_ms > 15 * 60 * 1000:
            raise ValueError("chunk duration is too large")
        return self


class SessionCompleteRequest(StrictModel):
    started_at: str = Field(min_length=10, max_length=64)
    ended_at: str = Field(min_length=10, max_length=64)
    timezone: str = Field(min_length=1, max_length=128)
    device_label: str = Field(min_length=1, max_length=128)
    duration_ms: int = Field(ge=5_000, le=24 * 60 * 60 * 1000)
    chunk_count: int = Field(ge=1, le=10_000)
    chunks: list[TranscriptChunk] = Field(min_length=1, max_length=10_000)

    @model_validator(mode="after")
    def validate_chunks(self) -> SessionCompleteRequest:
        if self.chunk_count != len(self.chunks):
            raise ValueError("chunk_count does not match chunks")
        indices = [chunk.chunk_index for chunk in self.chunks]
        if indices != list(range(len(indices))):
            raise ValueError("chunks must be contiguous and ordered from zero")
        if self.chunks[-1].end_ms > self.duration_ms + 2_000:
            raise ValueError("chunk timeline exceeds session duration")
        total_chars = sum(len(chunk.transcript.transcript) for chunk in self.chunks)
        if total_chars > 1_500_000:
            raise ValueError("combined transcript is too large")
        return self


class ModelUsage(StrictModel):
    input_tokens: int = Field(ge=0)
    output_tokens: int = Field(ge=0)
    thought_tokens: int = Field(ge=0)
    total_tokens: int = Field(ge=0)


class ChunkTranscriptResponse(StrictModel):
    schema_version: str = "1.0.0"
    session_id: str = Field(pattern=SESSION_ID_PATTERN)
    chunk_index: int = Field(ge=0)
    model: str
    prompt_version: str
    transcript: TranscriptPayload
    usage: ModelUsage
    request_uid: str
    limiter: dict[str, Any]


class SessionSummaryResponse(StrictModel):
    schema_version: str = "1.0.0"
    model: str
    prompt_version: str
    summary: SummaryPayload
    usage: ModelUsage
    request_uid: str
    limiter: dict[str, Any]


class RemoteProgress(StrictModel):
    state: str
    recording_finished: bool
    chunks_expected: int | None = None
    chunks_uploaded: int = Field(default=0, ge=0)
    chunks_transcribed: int = Field(default=0, ge=0)
    github_verified: bool = False
    github_url: str | None = None
    github_commit_sha: str | None = None
    last_error: str | None = None
    retry_after_seconds: int | None = None


TRANSCRIPT_JSON_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "transcript": {"type": "string"},
        "language": {"type": "string"},
        "uncertain_fragments": {"type": "array", "items": {"type": "string"}},
    },
    "required": ["transcript", "language", "uncertain_fragments"],
    "additionalProperties": False,
}

SUMMARY_JSON_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "title": {"type": "string"},
        "short_summary": {"type": "string"},
        "detailed_summary": {"type": "string"},
        "theses": {"type": "array", "items": {"type": "string"}},
        "ideas": {"type": "array", "items": {"type": "string"}},
        "decisions": {"type": "array", "items": {"type": "string"}},
        "tasks": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "text": {"type": "string"},
                    "owner": {"type": ["string", "null"]},
                    "deadline": {"type": ["string", "null"]},
                    "explicitly_stated": {"type": "boolean"},
                },
                "required": ["text", "owner", "deadline", "explicitly_stated"],
                "additionalProperties": False,
            },
        },
        "facts": {"type": "array", "items": {"type": "string"}},
        "entities": {"type": "array", "items": {"type": "string"}},
        "related_projects": {"type": "array", "items": {"type": "string"}},
        "open_questions": {"type": "array", "items": {"type": "string"}},
        "contradictions": {"type": "array", "items": {"type": "string"}},
        "uncertain_fragments": {"type": "array", "items": {"type": "string"}},
        "tags": {"type": "array", "items": {"type": "string"}},
    },
    "required": [
        "title",
        "short_summary",
        "detailed_summary",
        "theses",
        "ideas",
        "decisions",
        "tasks",
        "facts",
        "entities",
        "related_projects",
        "open_questions",
        "contradictions",
        "uncertain_fragments",
        "tags",
    ],
    "additionalProperties": False,
}
