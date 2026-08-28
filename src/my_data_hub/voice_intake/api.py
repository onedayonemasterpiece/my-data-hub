from __future__ import annotations

import hashlib
import hmac
from pathlib import Path
from typing import Any

from fastapi import APIRouter, FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import ValidationError

from .contracts import (
    SESSION_ID_PATTERN,
    ChunkTranscriptResponse,
    RemoteProgress,
    SessionCompleteRequest,
    SessionCreateRequest,
)
from .errors import VoiceIntakeError
from .gemini import GeminiVoiceService
from .github import IdeaHubPublisher
from .settings import VoiceIntakeSettings
from .terminology import SessionTerminologySnapshots, TerminologyContext


def _safe_error(exc: VoiceIntakeError) -> JSONResponse:
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "detail": {
                "code": exc.code,
                "retryable": exc.retryable,
                "retry_after_seconds": exc.retry_after_seconds,
                "reconciliation_required": exc.reconciliation_required,
            }
        },
    )


async def _bounded_body(request: Request, limit: int) -> bytes:
    raw_length = request.headers.get("content-length")
    if raw_length:
        try:
            if int(raw_length) > limit:
                raise HTTPException(status_code=413, detail={"code": "request_too_large"})
        except ValueError as exc:
            raise HTTPException(status_code=400, detail={"code": "content_length_invalid"}) from exc
    body = bytearray()
    async for chunk in request.stream():
        body.extend(chunk)
        if len(body) > limit:
            raise HTTPException(status_code=413, detail={"code": "request_too_large"})
    return bytes(body)


def _require_token(settings: VoiceIntakeSettings, authorization: str | None) -> None:
    if not settings.enabled:
        raise HTTPException(status_code=503, detail={"code": "voice_intake_disabled"})
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail={"code": "device_token_required"})
    token = authorization.removeprefix("Bearer ").strip()
    if not hmac.compare_digest(token, settings.device_token):
        raise HTTPException(status_code=401, detail={"code": "device_token_invalid"})


def _validate_wav(audio: bytes) -> None:
    if len(audio) < 44 or audio[:4] != b"RIFF" or audio[8:12] != b"WAVE":
        raise HTTPException(status_code=422, detail={"code": "wav_invalid"})
    declared = int.from_bytes(audio[4:8], "little", signed=False) + 8
    if declared not in {len(audio), 0xFFFFFFFF + 8}:
        raise HTTPException(status_code=422, detail={"code": "wav_length_mismatch"})
    if b"fmt " not in audio[:128] or b"data" not in audio[:256]:
        raise HTTPException(status_code=422, detail={"code": "wav_chunks_missing"})


def _receiving_progress(snapshot: TerminologyContext) -> RemoteProgress:
    return RemoteProgress(
        state="receiving",
        recording_finished=False,
        chunks_uploaded=0,
        chunks_transcribed=0,
        terminology_card_status=snapshot.status,
        terminology_card_path=snapshot.source_path,
        terminology_card_version=snapshot.schema_version,
        terminology_card_commit=snapshot.source_commit_sha,
        terminology_card_blob_sha=snapshot.source_blob_sha,
    )


def attach_voice_intake_routes(
    app: FastAPI,
    *,
    settings: VoiceIntakeSettings | None = None,
    service: GeminiVoiceService | None = None,
    publisher: IdeaHubPublisher | None = None,
) -> FastAPI:
    runtime = settings or VoiceIntakeSettings.from_env()
    voice_service = service or (GeminiVoiceService(runtime) if runtime.enabled else None)
    idea_hub = publisher or (IdeaHubPublisher(runtime) if runtime.enabled else None)
    terminology_snapshots = SessionTerminologySnapshots(
        state_path=Path(runtime.terminology_state_path)
        if runtime.terminology_state_path
        else None
    )
    router = APIRouter(prefix="/voice-intake/v1", tags=["record-idea-hub"])

    @router.get("/health")
    async def health(authorization: str | None = Header(default=None)) -> dict[str, Any]:
        _require_token(runtime, authorization)
        return {
            "status": "ready",
            "enabled": runtime.enabled,
            "model": runtime.model,
            "github_repository": runtime.github_repository,
            "github_branch": runtime.github_branch,
            "server_audio_persistence": False,
        }

    @router.post("/sessions", response_model=RemoteProgress)
    async def create_session(
        request: Request,
        authorization: str | None = Header(default=None),
    ) -> RemoteProgress | JSONResponse:
        _require_token(runtime, authorization)
        body = await _bounded_body(request, 64 * 1024)
        try:
            payload = SessionCreateRequest.model_validate_json(body)
        except ValidationError as exc:
            raise HTTPException(status_code=422, detail={"code": "session_invalid"}) from exc
        if idea_hub is None:
            raise HTTPException(status_code=503, detail={"code": "voice_intake_disabled"})
        try:
            snapshot = await terminology_snapshots.begin(
                payload.session_id,
                idea_hub.resolve_terminology,
            )
            return _receiving_progress(snapshot)
        except VoiceIntakeError as exc:
            return _safe_error(exc)

    @router.put(
        "/sessions/{session_id}/chunks/{chunk_index}",
        response_model=ChunkTranscriptResponse,
    )
    async def transcribe_chunk(
        session_id: str,
        chunk_index: int,
        request: Request,
        authorization: str | None = Header(default=None),
        chunk_sha256: str | None = Header(default=None, alias="X-Chunk-SHA256"),
        duration_ms: int | None = Header(default=None, alias="X-Chunk-Duration-Ms"),
    ) -> ChunkTranscriptResponse | JSONResponse:
        _require_token(runtime, authorization)
        if voice_service is None:
            raise HTTPException(status_code=503, detail={"code": "voice_intake_disabled"})
        try:
            import re

            if not re.fullmatch(SESSION_ID_PATTERN, session_id):
                raise ValueError
            if not 0 <= chunk_index <= 10_000:
                raise ValueError
            if duration_ms is None or not 1_000 <= duration_ms <= 15 * 60 * 1000:
                raise ValueError
            if chunk_sha256 is None or not re.fullmatch(r"[0-9a-f]{64}", chunk_sha256):
                raise ValueError
        except ValueError as exc:
            raise HTTPException(status_code=422, detail={"code": "chunk_metadata_invalid"}) from exc
        if request.headers.get("content-type", "").split(";", 1)[0].strip().lower() != "audio/wav":
            raise HTTPException(status_code=415, detail={"code": "audio_content_type_invalid"})
        audio = await _bounded_body(request, runtime.max_audio_bytes)
        _validate_wav(audio)
        if not hmac.compare_digest(hashlib.sha256(audio).hexdigest(), chunk_sha256):
            raise HTTPException(status_code=409, detail={"code": "chunk_sha256_mismatch"})
        try:
            if idea_hub is None:
                raise HTTPException(status_code=503, detail={"code": "voice_intake_disabled"})
            # Deployed clients created before the explicit session-start contract
            # contact the backend for the first time when uploading chunk zero.
            # Treat that first authenticated upload as session initialization so
            # they still get one freshly resolved, durable snapshot rather than a
            # stale fallback or a permanently retrying 409.
            terminology = await terminology_snapshots.begin(
                session_id,
                idea_hub.resolve_terminology,
            )
            return await voice_service.transcribe(
                session_id=session_id,
                chunk_index=chunk_index,
                duration_ms=duration_ms,
                audio=audio,
                terminology=terminology.prompt,
            )
        except VoiceIntakeError as exc:
            return _safe_error(exc)

    @router.post("/sessions/{session_id}/complete", response_model=RemoteProgress)
    async def complete_session(
        session_id: str,
        request: Request,
        authorization: str | None = Header(default=None),
    ) -> RemoteProgress | JSONResponse:
        _require_token(runtime, authorization)
        if voice_service is None or idea_hub is None:
            raise HTTPException(status_code=503, detail={"code": "voice_intake_disabled"})
        import re

        if not re.fullmatch(SESSION_ID_PATTERN, session_id):
            raise HTTPException(status_code=422, detail={"code": "session_id_invalid"})
        body = await _bounded_body(request, runtime.max_json_bytes)
        try:
            payload = SessionCompleteRequest.model_validate_json(body)
        except ValidationError as exc:
            raise HTTPException(status_code=422, detail={"code": "session_complete_invalid"}) from exc
        try:
            existing = await idea_hub.status(session_id)
            if existing.github_verified:
                await terminology_snapshots.discard(session_id)
                existing.chunks_expected = payload.chunk_count
                existing.chunks_uploaded = payload.chunk_count
                existing.chunks_transcribed = payload.chunk_count
                return existing
            terminology = await terminology_snapshots.require(session_id)
            summary = await voice_service.summarize(
                payload.chunks,
                terminology=terminology.prompt,
            )
            receipt = await idea_hub.publish(
                session_id=session_id,
                request=payload,
                summary=summary.summary,
                model=runtime.model,
                terminology=terminology,
            )
            await terminology_snapshots.discard(session_id)
            return RemoteProgress(
                state="published_verified",
                recording_finished=True,
                chunks_expected=payload.chunk_count,
                chunks_uploaded=payload.chunk_count,
                chunks_transcribed=payload.chunk_count,
                github_verified=True,
                github_url=receipt.github_url,
                github_commit_sha=receipt.commit_sha,
            )
        except VoiceIntakeError as exc:
            return _safe_error(exc)

    @router.get("/sessions/{session_id}", response_model=RemoteProgress)
    async def session_status(
        session_id: str,
        authorization: str | None = Header(default=None),
    ) -> RemoteProgress | JSONResponse:
        _require_token(runtime, authorization)
        if idea_hub is None:
            raise HTTPException(status_code=503, detail={"code": "voice_intake_disabled"})
        import re

        if not re.fullmatch(SESSION_ID_PATTERN, session_id):
            raise HTTPException(status_code=422, detail={"code": "session_id_invalid"})
        try:
            return await idea_hub.status(session_id)
        except VoiceIntakeError as exc:
            return _safe_error(exc)

    before = len(app.router.routes)
    app.include_router(router)
    added = app.router.routes[before:]
    del app.router.routes[before:]
    catch_index = next(
        (
            index
            for index, route in enumerate(app.router.routes)
            if getattr(route, "path", None) == "/{data_path:path}"
        ),
        len(app.router.routes),
    )
    app.router.routes[catch_index:catch_index] = added
    app.state.voice_intake_settings = runtime
    app.state.voice_terminology_snapshots = terminology_snapshots
    return app


__all__ = ["attach_voice_intake_routes"]
