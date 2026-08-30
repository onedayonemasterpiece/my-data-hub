# ruff: noqa: RUF001
from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from typing import Any, cast

import yaml

from my_data_hub.voice_intake.contracts import (
    SessionCompleteRequest as LegacySessionCompleteRequest,
)
from my_data_hub.voice_intake.contracts import SummaryPayload, TranscriptChunk, TranscriptPayload
from my_data_hub.voice_intake.errors import VoiceIntakeError
from my_data_hub.voice_intake.markdown import build_registry_entry, paths_for

from .store import PublicationProjection


@dataclass(frozen=True, slots=True)
class RenderedPublication:
    source_path: str
    detail_path: str
    source: str
    detail: str
    registry_entry: dict[str, Any]
    registered_at: str


_SHA256_RE = re.compile(r"[0-9a-f]{64}")


def require_content_verified_projection(
    projection: PublicationProjection,
) -> tuple[dict[str, Any], ...]:
    """Reject any projection that lacks the independent coverage receipt.

    The store is the authority that creates the receipt.  This second boundary
    prevents a caller, reconciliation path, or future renderer refactor from
    treating a transcript-shaped value or GitHub readback as content proof.
    Only bounded, content-free source provenance is returned.
    """
    receipt = projection.content_verification_receipt_sha256
    manifest = projection.complete.get("chunks")
    descriptor = projection.transcription_limiter
    if (
        not isinstance(receipt, str)
        or _SHA256_RE.fullmatch(receipt) is None
        or not isinstance(manifest, list)
        or not manifest
        or projection.complete.get("chunk_count") != len(manifest)
        or len(projection.transport_chunks) != len(manifest)
        or not isinstance(descriptor, dict)
        or descriptor.get("mode") != "per_chunk"
        or descriptor.get("segment_count") != len(manifest)
    ):
        raise VoiceIntakeError(
            "content_verification_required", retryable=False, status_code=409
        )

    provenance: list[dict[str, Any]] = []
    previous_end: int | None = None
    for expected_index, (source, transport) in enumerate(
        zip(manifest, projection.transport_chunks, strict=True)
    ):
        if not isinstance(source, dict) or not isinstance(transport, dict):
            raise VoiceIntakeError(
                "content_verification_required", retryable=False, status_code=409
            )
        start = source.get("audio_start_ms")
        end = source.get("audio_end_ms")
        duration = source.get("duration_ms")
        digest = source.get("sha256")
        if (
            source.get("chunk_index") != expected_index
            or transport.get("chunk_index") != expected_index
            or transport.get("sha256") != digest
            or transport.get("duration_ms") != duration
            or not isinstance(start, int)
            or not isinstance(end, int)
            or not isinstance(duration, int)
            or start < 0
            or end <= start
            or duration != end - start
            or (expected_index == 0 and start != 0)
            or (previous_end is not None and start != previous_end)
            or not isinstance(digest, str)
            or _SHA256_RE.fullmatch(digest) is None
        ):
            raise VoiceIntakeError(
                "content_verification_required", retryable=False, status_code=409
            )
        provenance.append(
            {
                "chunk_index": expected_index,
                "source_sha256": digest,
                "audio_start_ms": start,
                "audio_end_ms": end,
                "duration_ms": duration,
            }
        )
        previous_end = end
    if previous_end != projection.complete.get("recorded_audio_ms"):
        raise VoiceIntakeError(
            "content_verification_required", retryable=False, status_code=409
        )
    return tuple(provenance)


def _section(title: str, values: list[str]) -> str:
    if not values:
        return f"## {title}\n\n_Не выделено моделью._"
    return f"## {title}\n\n" + "\n".join(f"- {value.strip()}" for value in values)


def _task_lines(summary: SummaryPayload) -> list[str]:
    result: list[str] = []
    for task in summary.tasks:
        qualifiers = []
        if task.owner:
            qualifiers.append(f"ответственный: {task.owner}")
        if task.deadline:
            qualifiers.append(f"срок: {task.deadline}")
        if not task.explicitly_stated:
            qualifiers.append("выведено моделью")
        suffix = f" ({'; '.join(qualifiers)})" if qualifiers else ""
        result.append(task.text.strip() + suffix)
    return result


def _legacy_registry_entry(
    projection: PublicationProjection,
    *,
    summary: SummaryPayload,
    source_path: str,
    detail_path: str,
    registered_at: str,
) -> dict[str, Any]:
    """Adapt verified v2 data to the established IdeaHub registry helper.

    The typed v1 helper does not publish its chunk values.  Its bounded chunks
    are derived from the verified source timeline only; content completeness
    is represented by the separate v2 verification flag below.
    """
    transcript = TranscriptPayload.model_validate(projection.transcript)
    source_segments = require_content_verified_projection(projection)
    recorded_ms = int(projection.complete["recorded_audio_ms"])
    legacy_chunks: list[TranscriptChunk] = []
    for segment in source_segments:
        start_ms = int(segment["audio_start_ms"])
        source_end_ms = int(segment["audio_end_ms"])
        while start_ms < source_end_ms:
            end_ms = min(source_end_ms, start_ms + 15 * 60 * 1000)
            digest = hashlib.sha256(
                f"{segment['source_sha256']}:{start_ms}:{end_ms}".encode()
            ).hexdigest()
            legacy_chunks.append(
                TranscriptChunk(
                    chunk_index=len(legacy_chunks),
                    start_ms=start_ms,
                    end_ms=end_ms,
                    sha256=digest,
                    transcript=transcript,
                )
            )
            start_ms = end_ms
    request = LegacySessionCompleteRequest(
        started_at=str(projection.create["started_at"]),
        ended_at=str(projection.complete["ended_at"]),
        timezone=str(projection.create["timezone"]),
        device_label=str(projection.create["device_label"]),
        duration_ms=max(recorded_ms, 5_000),
        chunk_count=len(legacy_chunks),
        chunks=legacy_chunks,
    )
    entry = cast(
        dict[str, Any],
        build_registry_entry(
            session_id=projection.session_id,
            request=request,
            summary=summary,
            source_path=source_path,
            detail_path=detail_path,
            registered_at=registered_at,
        ),
    )
    entry["quality_flags"].extend(
        [
            "voice_intake_v2_durable_transport",
            "per_source_chunk_transcription",
            "content_coverage_verified",
            "aggregate_summary_single_request",
        ]
    )
    return entry


def render_publication(projection: PublicationProjection) -> RenderedPublication:
    source_segments = require_content_verified_projection(projection)
    summary = SummaryPayload.model_validate(projection.summary)
    transcript = TranscriptPayload.model_validate(projection.transcript)
    source_path, detail_path = paths_for(projection.session_id)
    # Completion time is immutable session input, so GitHub reconciliation
    # renders byte-identical artifacts after any process restart.
    registered_at = str(projection.complete["ended_at"])
    create = projection.create
    complete = projection.complete
    terminology = projection.terminology
    frontmatter: dict[str, Any] = {
        "schema_version": "1.0.0",
        "packet_id": projection.session_id,
        "intake_session_id": projection.session_id,
        "source": "record-idea-hub-android",
        "api_contract": "voice-intake-v2",
        "client_version": create["client_version"],
        "device_label": create["device_label"],
        "processing_status": "pending",
        "captured_at": {
            "start": create["started_at"],
            "end": complete["ended_at"],
            "timezone": create["timezone"],
        },
        "capture_policy": create["capture_policy"],
        "audio_format": create["audio_format"],
        "vad": create.get("vad"),
        "wall_elapsed_ms": complete["wall_elapsed_ms"],
        "manual_pause_ms": complete["manual_pause_ms"],
        "recorded_audio_ms": complete["recorded_audio_ms"],
        "auto_silence_skipped_ms": complete["auto_silence_skipped_ms"],
        "transport_chunk_count": complete["chunk_count"],
        "transport_chunk_sha256": [item["sha256"] for item in projection.transport_chunks],
        "language": transcript.language,
        "transcription_model": projection.model,
        "synthesis_model": projection.model,
        "transcription_mode": "bounded_per_source_chunk",
        "transcription_prompt_version": "voice-transcribe-v2-segment",
        "synthesis_prompt_version": "voice-summary-v2-aggregate",
        # Nullable legacy aggregate field: a per-source-chunk pipeline has no
        # truthful single provider request identity.
        "transcription_request_uid": projection.transcription_request_uid,
        "transcription_segment_count": len(source_segments),
        "transcription_segments": list(source_segments),
        "content_verification_status": "passed",
        "content_verification_receipt_sha256": (
            projection.content_verification_receipt_sha256
        ),
        "summary_request_uid": projection.summary_request_uid,
        "transcription_limiter": projection.transcription_limiter,
        "summary_limiter": projection.summary_limiter,
        "terminology_card_version": terminology["schema_version"],
        "terminology_card_path": terminology["source_path"],
        "terminology_card_commit": terminology["source_commit_sha"],
        "terminology_card_blob_sha": terminology["source_blob_sha"],
        "terminology_card_status": terminology["status"],
        "registered_at": registered_at,
        "server_audio_retention": (
            "until_content_and_publication_verified_purge_authorized"
        ),
        "tags": summary.tags,
    }
    header = yaml.safe_dump(frontmatter, allow_unicode=True, sort_keys=False).strip()
    body = [
        "---",
        header,
        "---",
        "",
        f"# {summary.title.strip()}",
        "",
        "> **Новая необработанная голосовая сессия Voice Intake API v2.** Требуется маршрутизация и "
        "материализация в owning canonical artifacts IdeaHub.",
        "",
        "## Кратко",
        "",
        summary.short_summary.strip(),
        "",
        "## Подробная выжимка",
        "",
        summary.detailed_summary.strip(),
        "",
        _section("Основные тезисы", summary.theses),
        "",
        _section("Идеи и гипотезы", summary.ideas),
        "",
        _section("Решения", summary.decisions),
        "",
        _section("Задачи", _task_lines(summary)),
        "",
        _section("Факты и конкретика", summary.facts),
        "",
        _section("Упомянутые сущности", summary.entities),
        "",
        _section("Связанные проекты", summary.related_projects),
        "",
        _section("Открытые вопросы", summary.open_questions),
        "",
        _section("Противоречия", summary.contradictions),
        "",
        _section("Неопределённые фрагменты", summary.uncertain_fragments),
        "",
        "## Полная расшифровка",
        "",
        transcript.transcript.strip(),
        "",
        "## Техническая фиксация",
        "",
        "- API-контракт: `voice-intake-v2`",
        f"- Сессия: `{projection.session_id}`",
        f"- Транспортных сегментов: {complete['chunk_count']}",
        f"- Верифицированных сегментов транскрипции: {len(source_segments)}.",
        f"- Content-verification receipt: "
        f"`{projection.content_verification_receipt_sha256}`.",
        f"- Gemini-запрос выжимки: `{projection.summary_request_uid}`.",
        "- Аудиофайлы не сохраняются в GitHub.",
        "- GitHub readback подтверждает публикацию, но сам по себе не разрешает удаление аудио.",
        "- Сервер удаляет аудио только после content verification, publication readback и "
        "отдельной durable purge authorization.",
        "- Все голосовые записи: [актуальный индекс ветки `main`](../../README.md).",
        "",
    ]
    source = "\n".join(body).rstrip() + "\n"

    detail_header = yaml.safe_dump(
        {"session_id": projection.session_id, "overall_status": "open", "api_contract": "voice-intake-v2"},
        allow_unicode=True,
        sort_keys=False,
    ).strip()
    audio = create["audio_format"]
    detail = (
        f"---\n{detail_header}\n---\n\n"
        f"# Intake-сессия: {summary.title.strip()}\n\n"
        "## Источник\n\n"
        "- Платформа: `record-idea-hub-android`\n"
        "- API-контракт: `voice-intake-v2`\n"
        f"- Client: `{create['client_version']}`; устройство: `{create['device_label']}`\n"
        f"- Capture policy: `{create['capture_policy']}`\n"
        f"- Audio: `{audio['mime_type']}`, `{audio['codec']}`, {audio['sample_rate_hz']} Hz, "
        f"{audio['channels']} channel(s), target {audio['target_bitrate_bps']} bps\n"
        f"- Начало: `{create['started_at']}`; завершение: `{complete['ended_at']}`\n"
        f"- Зарегистрировано: `{registered_at}`\n\n"
        "## Маршрут\n\n"
        f"- Source packet: [`{source_path}`](../../../../{source_path})\n"
        "- Текущий статус: `open`; routing/processing/materialization ожидают следующего агента.\n"
        "- Аудио отсутствует в Git; readback сам по себе не разрешает удаление из server spool.\n"
    )
    entry = _legacy_registry_entry(
        projection,
        summary=summary,
        source_path=source_path,
        detail_path=detail_path,
        registered_at=registered_at,
    )
    return RenderedPublication(source_path, detail_path, source, detail, entry, registered_at)


__all__ = [
    "RenderedPublication",
    "render_publication",
    "require_content_verified_projection",
]
