from __future__ import annotations

import re
from datetime import UTC, datetime
from typing import Any

import yaml

from .contracts import SessionCompleteRequest, SummaryPayload


def _yaml_scalar(value: str) -> str:
    return yaml.safe_dump(value, allow_unicode=True, default_flow_style=True).strip()


def _format_ms(value: int) -> str:
    seconds = max(0, value // 1000)
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def _section(title: str, values: list[str]) -> str:
    if not values:
        return f"## {title}\n\n_Не выделено моделью._\n"
    return f"## {title}\n\n" + "\n".join(f"- {value.strip()}" for value in values) + "\n"


def _markdown_cell(value: object) -> str:
    return str(value or "").replace("\n", " ").replace("|", "\\|").strip()


def paths_for(session_id: str) -> tuple[str, str]:
    stamp = session_id.removeprefix("voice-").split("-", 1)[0]
    year = stamp[:4]
    month = stamp[4:6]
    source = f"inbox/voice/{year}/{month}/{session_id}.md"
    detail = f"registry/sessions/{year}/{month}/{session_id}.md"
    return source, detail


def render_source_packet(
    *,
    session_id: str,
    request: SessionCompleteRequest,
    summary: SummaryPayload,
    model: str,
    registered_at: str,
    terminology_version: str = "unavailable",
    terminology_path: str = "config/voice-terminology.yaml",
    terminology_commit_sha: str | None = None,
    terminology_blob_sha: str | None = None,
    terminology_status: str = "error",
) -> str:
    frontmatter: dict[str, Any] = {
        "schema_version": "1.0.0",
        "packet_id": session_id,
        "intake_session_id": session_id,
        "source": "record-idea-hub-android",
        "processing_status": "pending",
        "captured_at": {
            "start": request.started_at,
            "end": request.ended_at,
            "timezone": request.timezone,
        },
        "recorded_duration_seconds": round(request.duration_ms / 1000, 3),
        "chunk_count": request.chunk_count,
        "language": "ru-RU",
        "transcription_model": model,
        "synthesis_model": model,
        "transcription_prompt_version": "voice-transcribe-v2",
        "synthesis_prompt_version": "voice-summary-v2",
        "terminology_card_version": terminology_version,
        "terminology_card_path": terminology_path,
        "terminology_card_commit": terminology_commit_sha,
        "terminology_card_blob_sha": terminology_blob_sha,
        "terminology_card_status": terminology_status,
        "registered_at": registered_at,
        "audio_retention": "phone_deletes_after_github_readback",
        "chunk_sha256": [chunk.sha256 for chunk in request.chunks],
        "tags": summary.tags,
    }
    header = yaml.safe_dump(frontmatter, allow_unicode=True, sort_keys=False).strip()
    task_lines = []
    for task in summary.tasks:
        qualifiers = []
        if task.owner:
            qualifiers.append(f"ответственный: {task.owner}")
        if task.deadline:
            qualifiers.append(f"срок: {task.deadline}")
        if not task.explicitly_stated:
            qualifiers.append("выведено моделью, не сформулировано как прямая задача")
        suffix = f" ({'; '.join(qualifiers)})" if qualifiers else ""
        task_lines.append(task.text.strip() + suffix)

    body = [
        "---",
        header,
        "---",
        "",
        f"# {summary.title.strip()}",
        "",
        "> **Новая необработанная голосовая сессия.** Требуется маршрутизация и "
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
        _section("Основные тезисы", summary.theses).rstrip(),
        "",
        _section("Идеи и гипотезы", summary.ideas).rstrip(),
        "",
        _section("Решения", summary.decisions).rstrip(),
        "",
        _section("Задачи", task_lines).rstrip(),
        "",
        _section("Факты и конкретика", summary.facts).rstrip(),
        "",
        _section("Упомянутые сущности", summary.entities).rstrip(),
        "",
        _section("Связанные проекты", summary.related_projects).rstrip(),
        "",
        _section("Открытые вопросы", summary.open_questions).rstrip(),
        "",
        _section("Противоречия", summary.contradictions).rstrip(),
        "",
        _section("Неопределённые фрагменты", summary.uncertain_fragments).rstrip(),
        "",
        "## Полная расшифровка",
        "",
    ]
    for chunk in request.chunks:
        body.extend(
            [
                f"### {_format_ms(chunk.start_ms)}–{_format_ms(chunk.end_ms)}",
                "",
                chunk.transcript.transcript.strip(),
                "",
            ]
        )
    body.extend(
        [
            "## Техническая фиксация",
            "",
            f"- Сессия: `{session_id}`",
            f"- Чанков: {request.chunk_count}",
            f"- Модель: `{model}`",
            "- Аудиофайлы не сохраняются в GitHub и не удерживаются my-data-hub после ответа.",
            "- Телефон удаляет локальные чанки только после подтверждённого GitHub readback.",
            "- Текст идеи отсутствует в сообщении commit.",
            "- Все голосовые записи: [актуальный индекс ветки `main`](../../README.md).",
            "",
        ]
    )
    return "\n".join(body).rstrip() + "\n"


def render_voice_index(registry_text: str) -> str:
    parsed = yaml.safe_load(registry_text)
    if not isinstance(parsed, dict) or not isinstance(parsed.get("sessions"), list):
        raise ValueError("registry/intake-sessions.yaml has an unexpected shape")
    records: list[dict[str, str]] = []
    for entry in parsed["sessions"]:
        if not isinstance(entry, dict):
            continue
        source = entry.get("source")
        if not isinstance(source, dict) or source.get("platform") != "record-idea-hub-android":
            continue
        routes = entry.get("routes")
        destinations = routes.get("destinations") if isinstance(routes, dict) else None
        source_path = ""
        if isinstance(destinations, list):
            for destination in destinations:
                if (
                    isinstance(destination, dict)
                    and destination.get("role") == "source_packet"
                    and isinstance(destination.get("path"), str)
                ):
                    source_path = destination["path"]
                    break
        if not source_path.startswith("inbox/voice/") or not source_path.endswith(".md"):
            continue
        occurred = entry.get("occurred_at")
        status = entry.get("status")
        records.append(
            {
                "registered_at": str(entry.get("registered_at") or ""),
                "captured_at": str(occurred.get("start") or "")
                if isinstance(occurred, dict)
                else "",
                "title": str(entry.get("title") or entry.get("session_id") or ""),
                "session_id": str(entry.get("session_id") or ""),
                "path": source_path.removeprefix("inbox/voice/"),
                "status": str(status.get("overall") or "") if isinstance(status, dict) else "",
            }
        )
    records.sort(key=lambda item: (item["registered_at"], item["session_id"]), reverse=True)
    lines = [
        "# Голосовые записи IdeaHub",
        "",
        "> Стабильный хронологический индекс записей из актуальной ветки `main`. "
        "Он обновляется атомарно вместе с каждой новой записью.",
        "",
        "| Записано | Заголовок | Сессия | Статус |",
        "|---|---|---|---|",
    ]
    lines.extend(
        "| {captured_at} | [{title}]({path}) | `{session_id}` | `{status}` |".format(
            captured_at=_markdown_cell(item["captured_at"]),
            title=_markdown_cell(item["title"]),
            path=item["path"],
            session_id=_markdown_cell(item["session_id"]),
            status=_markdown_cell(item["status"]),
        )
        for item in records
    )
    return "\n".join(lines).rstrip() + "\n"


def render_session_detail(
    *,
    session_id: str,
    request: SessionCompleteRequest,
    summary: SummaryPayload,
    source_path: str,
    registered_at: str,
) -> str:
    frontmatter = {
        "session_id": session_id,
        "overall_status": "open",
    }
    header = yaml.safe_dump(frontmatter, allow_unicode=True, sort_keys=False).strip()
    return (
        f"---\n{header}\n---\n\n"
        f"# Intake-сессия: {summary.title.strip()}\n\n"
        "## Источник\n\n"
        f"- Платформа: `record-idea-hub-android`\n"
        f"- Сессия: `{session_id}`\n"
        f"- Начало: `{request.started_at}`\n"
        f"- Завершение: `{request.ended_at}`\n"
        f"- Часовой пояс: `{request.timezone}`\n"
        f"- Устройство: `{request.device_label}`\n"
        f"- Зарегистрировано: `{registered_at}`\n\n"
        "## Маршрут\n\n"
        f"- Source packet: [`{source_path}`](../../../../{source_path})\n"
        "- Текущий статус: `open`; routing/processing/materialization ожидают следующего агента.\n"
        "- Аудио не хранится в репозитории.\n\n"
        "## Следующий продуктовый шаг\n\n"
        "Прочитать source packet, маршрутизировать каждый логический элемент в owning canonical "
        "artifact и закрыть сессию только после проверки инварианта счётчиков.\n"
    )


def build_registry_entry(
    *,
    session_id: str,
    request: SessionCompleteRequest,
    summary: SummaryPayload,
    source_path: str,
    detail_path: str,
    registered_at: str,
) -> dict[str, Any]:
    return {
        "session_id": session_id,
        "title": summary.title.strip(),
        "session_kind": "idea_intake",
        "detail_path": detail_path,
        "occurred_at": {
            "start": request.started_at,
            "end": request.ended_at,
            "timezone": request.timezone,
        },
        "registered_at": registered_at,
        "source": {
            "platform": "record-idea-hub-android",
            "source_kind": "android_voice_session",
            "media": ["voice", "transcription"],
            "direct_url": None,
            "locator": f"Samsung Android voice session {session_id}",
            "link_status": "local_device_no_stable_url",
            "authorization": "owner_device",
        },
        "routes": {
            "primary_contexts": ["portfolio.inbox"],
            "destinations": [{"role": "source_packet", "path": source_path}],
        },
        "status": {
            "overall": "open",
            "capture": "complete",
            "transcription": "complete",
            "normalization": "complete",
            "routing": "pending",
            "processing": "pending",
            "materialization": "pending",
            "verification": "complete",
        },
        "counts": {
            "unit": "sessions",
            "observed": 1,
            "transcribed": 1,
            "normalized": 1,
            "routed": 0,
            "processed": 0,
            "excluded": 0,
            "pending": 1,
        },
        "quality_flags": [
            "source_packet_github_readback_verified",
            "single_session_not_per_chunk",
        ],
        "open_items": [
            "Route extracted items and materialize them into owning canonical documents."
        ],
    }


def insert_registry_entry(current_text: str, *, entry: dict[str, Any], updated_at: str) -> str:
    parsed = yaml.safe_load(current_text)
    if not isinstance(parsed, dict) or not isinstance(parsed.get("sessions"), list):
        raise ValueError("registry/intake-sessions.yaml has an unexpected shape")
    session_id = str(entry["session_id"])
    if any(isinstance(item, dict) and item.get("session_id") == session_id for item in parsed["sessions"]):
        return current_text
    block = yaml.safe_dump(
        [entry],
        allow_unicode=True,
        sort_keys=False,
        default_flow_style=False,
        width=100,
    ).rstrip()
    updated = re.sub(
        r"(?m)^updated_at:\s*.*$",
        f"updated_at: {_yaml_scalar(updated_at)}",
        current_text,
        count=1,
    )
    if "sessions:\n" in updated:
        updated = updated.replace("sessions:\n", f"sessions:\n{block}\n\n", 1)
    elif re.search(r"(?m)^sessions:\s*\[\]\s*$", updated):
        updated = re.sub(r"(?m)^sessions:\s*\[\]\s*$", f"sessions:\n{block}", updated, count=1)
    else:
        raise ValueError("registry is missing a writable sessions list")
    check = yaml.safe_load(updated)
    if not any(
        isinstance(item, dict) and item.get("session_id") == session_id
        for item in check.get("sessions", [])
    ):
        raise ValueError("generated registry does not contain the session")
    return updated


def utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")
