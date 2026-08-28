# ruff: noqa: RUF001
from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any, cast

import yaml

from my_data_hub.voice_intake.contracts import (
    SessionCompleteRequest as LegacySessionCompleteRequest,
)
from my_data_hub.voice_intake.contracts import SummaryPayload, TranscriptChunk, TranscriptPayload
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
    """Adapt v2 aggregate data to the established IdeaHub registry helper.

    The synthetic legacy chunk is used only to satisfy the typed helper. Its
    digest never leaves the registry because ``build_registry_entry`` consumes
    session metadata, not chunk data.
    """
    transcript = TranscriptPayload.model_validate(projection.transcript)
    recorded_ms = int(projection.complete["recorded_audio_ms"])
    legacy_chunks: list[TranscriptChunk] = []
    start_ms = 0
    while start_ms < recorded_ms:
        end_ms = min(recorded_ms, start_ms + 15 * 60 * 1000)
        digest = hashlib.sha256(
            f"{start_ms}:{end_ms}:{transcript.transcript}".encode()
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
    entry = build_registry_entry(
        session_id=projection.session_id,
        request=request,
        summary=summary,
        source_path=source_path,
        detail_path=detail_path,
        registered_at=registered_at,
    )
    entry["quality_flags"].extend(
        [
            "voice_intake_v2_durable_transport",
            "aggregate_transcription_single_request",
            "aggregate_summary_single_request",
        ]
    )
    return cast(dict[str, Any], entry)


def render_publication(projection: PublicationProjection) -> RenderedPublication:
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
        "transcription_prompt_version": "voice-transcribe-v2-aggregate",
        "synthesis_prompt_version": "voice-summary-v2-aggregate",
        "transcription_request_uid": projection.transcription_request_uid,
        "summary_request_uid": projection.summary_request_uid,
        "transcription_limiter": projection.transcription_limiter,
        "summary_limiter": projection.summary_limiter,
        "terminology_card_version": terminology["schema_version"],
        "terminology_card_path": terminology["source_path"],
        "terminology_card_commit": terminology["source_commit_sha"],
        "terminology_card_blob_sha": terminology["source_blob_sha"],
        "terminology_card_status": terminology["status"],
        "registered_at": registered_at,
        "server_audio_retention": "temporary_until_github_readback",
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
        f"- Gemini-запросов: транскрипция `{projection.transcription_request_uid}`, "
        f"выжимка `{projection.summary_request_uid}`.",
        "- Аудиофайлы не сохраняются в GitHub.",
        "- Сервер удаляет временное аудио только после exact-commit и current-main readback.",
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
        "- Аудио отсутствует в Git и удаляется из server spool только после readback.\n"
    )
    entry = _legacy_registry_entry(
        projection,
        summary=summary,
        source_path=source_path,
        detail_path=detail_path,
        registered_at=registered_at,
    )
    return RenderedPublication(source_path, detail_path, source, detail, entry, registered_at)


__all__ = ["RenderedPublication", "render_publication"]
