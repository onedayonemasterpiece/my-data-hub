from __future__ import annotations

import hashlib
import hmac
import os
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any
from uuid import uuid4

from fastapi import APIRouter, FastAPI, Header, Request
from fastapi.responses import JSONResponse
from pydantic import ValidationError

from my_data_hub.voice_intake.settings import VoiceIntakeSettings

from .contracts import API_VERSION, SessionCompleteRequest, SessionCreateRequest
from .media import BoundedMediaTools, MediaError
from .settings import VoiceIntakeV2Settings
from .store import ChunkReceipt, StoreError, VoiceIntakeV2Store
from .worker import VoiceIntakeV2Worker

TerminologyResolver = Callable[[], Awaitable[dict[str, Any]]]


def _error(
    status: int,
    code: str,
    *,
    retryable: bool = False,
    retry_after_seconds: int | None = None,
    reconciliation_required: bool = False,
) -> JSONResponse:
    return JSONResponse(
        status_code=status,
        content={
            "api_version": API_VERSION,
            "detail": {
                "code": code,
                "retryable": retryable,
                "retry_after_seconds": retry_after_seconds,
                "reconciliation_required": reconciliation_required,
            },
        },
    )


def _require_token(settings: VoiceIntakeSettings, authorization: str | None) -> JSONResponse | None:
    if not settings.enabled:
        return _error(503, "voice_intake_disabled")
    if not authorization or not authorization.startswith("Bearer "):
        return _error(401, "device_token_required")
    if not hmac.compare_digest(authorization.removeprefix("Bearer ").strip(), settings.device_token):
        return _error(401, "device_token_invalid")
    return None


async def _bounded_json(request: Request, limit: int) -> bytes | JSONResponse:
    length = request.headers.get("content-length")
    try:
        if length is not None and int(length) > limit:
            return _error(413, "request_too_large")
    except ValueError:
        return _error(400, "content_length_invalid")
    value = bytearray()
    async for part in request.stream():
        value.extend(part)
        if len(value) > limit:
            return _error(413, "request_too_large")
    return bytes(value)


def _parse_integer(value: str | None, *, minimum: int = 0) -> int:
    if value is None:
        raise ValueError
    result = int(value)
    if result < minimum:
        raise ValueError
    return result


def attach_voice_intake_v2_routes(
    app: FastAPI,
    *,
    auth_settings: VoiceIntakeSettings | None = None,
    settings: VoiceIntakeV2Settings | None = None,
    store: VoiceIntakeV2Store | None = None,
    media: BoundedMediaTools | None = None,
    terminology_resolver: TerminologyResolver | None = None,
    worker: VoiceIntakeV2Worker | None = None,
    require_worker: bool = True,
) -> FastAPI:
    auth = auth_settings or VoiceIntakeSettings.from_env()
    config = settings or VoiceIntakeV2Settings.from_env()
    ledger = store or (VoiceIntakeV2Store(config.spool_root) if config.enabled else None)
    tools = media or BoundedMediaTools(
        ffprobe_timeout=config.ffprobe_timeout_seconds,
        ffmpeg_timeout=config.ffmpeg_timeout_seconds,
    )

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        if worker is not None:
            await worker.start()
        try:
            yield
        finally:
            if worker is not None:
                await worker.stop()

    router = APIRouter(prefix="/voice-intake/v2", tags=["record-idea-hub-v2"], lifespan=lifespan)

    def admission(authorization: str | None) -> JSONResponse | None:
        denied = _require_token(auth, authorization)
        if denied is not None:
            return denied
        if not config.enabled or ledger is None:
            return _error(503, "voice_intake_v2_disabled")
        if require_worker and worker is None:
            return _error(503, "voice_intake_v2_worker_unavailable")
        return None

    @router.get("/capabilities", response_model=None)
    async def capabilities(authorization: str | None = Header(default=None)) -> dict[str, Any] | JSONResponse:
        if denied := admission(authorization):
            return denied
        return {
            "api_version": API_VERSION,
            "status": "ready",
            "accepted_audio": [{
                "container": "mp4", "codec": "aac_lc", "mime_type": "audio/mp4",
                "sample_rate_hz": 16000, "channels": 1, "target_bitrate_bps": 32000,
            }],
            "capture_policies": ["continuous_v1", "voice_activity_auto_pause_v1"],
            "typical_gemini_requests": 2,
            "max_session_seconds": config.max_session_seconds,
            "server_audio_persistence": "temporary_until_github_readback",
        }

    @router.post("/sessions", response_model=None)
    async def create_session(
        request: Request, authorization: str | None = Header(default=None)
    ) -> dict[str, Any] | JSONResponse:
        if denied := admission(authorization):
            return denied
        body = await _bounded_json(request, 64 * 1024)
        if isinstance(body, JSONResponse):
            return body
        try:
            payload = SessionCreateRequest.model_validate_json(body)
            assert ledger is not None
            existing = ledger.existing_session(payload)
            if existing is not None:
                return {**existing.model_dump(mode="json"), "duplicate": True}
            if terminology_resolver is None:
                return _error(503, "terminology_resolver_unavailable", retryable=True)
            terminology = await terminology_resolver()
            status, duplicate = ledger.create_session(payload, terminology=terminology, model=auth.model)
            return {**status.model_dump(mode="json"), "duplicate": duplicate}
        except ValidationError:
            return _error(422, "session_invalid")
        except StoreError as exc:
            return _error(exc.status_code, exc.code)
        except Exception:
            return _error(503, "terminology_unavailable", retryable=True)

    @router.put("/sessions/{session_id}/chunks/{chunk_index}", response_model=None)
    async def upload_chunk(
        session_id: str,
        chunk_index: int,
        request: Request,
        authorization: str | None = Header(default=None),
        chunk_sha256: str | None = Header(default=None, alias="X-Chunk-SHA256"),
        duration_ms: str | None = Header(default=None, alias="X-Chunk-Duration-Ms"),
        audio_start_ms: str | None = Header(default=None, alias="X-Audio-Start-Ms"),
        audio_end_ms: str | None = Header(default=None, alias="X-Audio-End-Ms"),
        wall_start_ms: str | None = Header(default=None, alias="X-Wall-Start-Ms"),
        wall_end_ms: str | None = Header(default=None, alias="X-Wall-End-Ms"),
    ) -> dict[str, Any] | JSONResponse:
        if denied := admission(authorization):
            return denied
        assert ledger is not None
        try:
            if not __import__("re").fullmatch(r"voice-[0-9]{8}-[0-9]{6}-[a-f0-9]{8}", session_id):
                raise ValueError
            if not 0 <= chunk_index <= 10_000:
                raise ValueError
            duration = _parse_integer(duration_ms, minimum=1)
            audio_start = _parse_integer(audio_start_ms)
            audio_end = _parse_integer(audio_end_ms, minimum=1)
            wall_start = _parse_integer(wall_start_ms)
            wall_end = _parse_integer(wall_end_ms, minimum=1)
            if audio_end <= audio_start or wall_end <= wall_start or duration != audio_end - audio_start:
                raise ValueError
            if chunk_sha256 is None or not __import__("re").fullmatch(r"[0-9a-f]{64}", chunk_sha256):
                raise ValueError
        except (TypeError, ValueError):
            return _error(422, "chunk_metadata_invalid")
        try:
            ledger.status(session_id)
        except StoreError as exc:
            return _error(409 if exc.status_code == 404 else exc.status_code, "session_not_created")
        if request.headers.get("content-type", "").split(";", 1)[0].strip().lower() != "audio/mp4":
            return _error(415, "audio_content_type_invalid")
        raw_length = request.headers.get("content-length")
        try:
            if raw_length is not None and int(raw_length) > config.max_chunk_bytes:
                return _error(413, "request_too_large")
        except ValueError:
            return _error(400, "content_length_invalid")
        directory = ledger.session_directory(session_id) / "chunks"
        directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        temporary = directory / f".{chunk_index}.{uuid4().hex}.upload"
        actual = hashlib.sha256()
        size = 0
        created_final: Path | None = None
        receipt_persisted = False
        try:
            with temporary.open("xb") as handle:
                os.chmod(temporary, 0o600)
                async for part in request.stream():
                    size += len(part)
                    if size > config.max_chunk_bytes:
                        return _error(413, "request_too_large")
                    actual.update(part)
                    handle.write(part)
                handle.flush()
                os.fsync(handle.fileno())
            if not hmac.compare_digest(actual.hexdigest(), chunk_sha256):
                return _error(409, "chunk_sha256_mismatch")
            try:
                probe = await tools.probe(temporary)
            except MediaError as exc:
                return _error(422, exc.code)
            if abs(probe.duration_ms - duration) > config.duration_tolerance_ms:
                return _error(422, "audio_duration_mismatch")
            final = directory / f"{chunk_index:05d}-{chunk_sha256}.m4a"
            if final.exists():
                temporary.unlink()
            else:
                os.replace(temporary, final)
                created_final = final
                descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
                try:
                    os.fsync(descriptor)
                finally:
                    os.close(descriptor)
            receipt, status = ledger.record_chunk(ChunkReceipt(
                session_id=session_id, chunk_index=chunk_index, sha256=chunk_sha256,
                duration_ms=duration, audio_start_ms=audio_start, audio_end_ms=audio_end,
                wall_start_ms=wall_start, wall_end_ms=wall_end, size_bytes=size, path=str(final),
            ))
            receipt_persisted = True
            return {
                "api_version": API_VERSION, "session_id": session_id, "chunk_index": chunk_index,
                "accepted": True, "duplicate": receipt.duplicate, "sha256": receipt.sha256,
                "duration_ms": receipt.duration_ms, "size_bytes": receipt.size_bytes,
                "chunks_received": status.chunks_received, "bytes_received": status.bytes_received,
            }
        except StoreError as exc:
            return _error(exc.status_code, exc.code)
        finally:
            temporary.unlink(missing_ok=True)
            if created_final is not None and not receipt_persisted:
                created_final.unlink(missing_ok=True)

    @router.post("/sessions/{session_id}/complete", status_code=202, response_model=None)
    async def complete_session(
        session_id: str, request: Request, authorization: str | None = Header(default=None)
    ) -> dict[str, Any] | JSONResponse:
        if denied := admission(authorization):
            return denied
        body = await _bounded_json(request, config.max_json_bytes)
        if isinstance(body, JSONResponse):
            return body
        try:
            payload = SessionCompleteRequest.model_validate_json(body)
            if payload.recorded_audio_ms > config.max_session_seconds * 1000:
                return _error(413, "session_audio_limit_exceeded")
            assert ledger is not None
            status, duplicate = ledger.complete(session_id, payload)
            return {**status.model_dump(mode="json"), "duplicate": duplicate}
        except ValidationError:
            return _error(422, "complete_manifest_invalid")
        except StoreError as exc:
            return _error(exc.status_code, exc.code)

    @router.get("/sessions/{session_id}", response_model=None)
    async def session_status(
        session_id: str, authorization: str | None = Header(default=None)
    ) -> dict[str, Any] | JSONResponse:
        if denied := admission(authorization):
            return denied
        try:
            assert ledger is not None
            return ledger.status(session_id).model_dump(mode="json")
        except StoreError as exc:
            return _error(exc.status_code, exc.code)

    before = len(app.router.routes)
    app.include_router(router)
    added = app.router.routes[before:]
    del app.router.routes[before:]
    catch_index = next(
        (
            index for index, route in enumerate(app.router.routes)
            if getattr(route, "path", None) == "/{data_path:path}"
        ),
        len(app.router.routes),
    )
    app.router.routes[catch_index:catch_index] = added
    app.state.voice_intake_v2_store = ledger
    app.state.voice_intake_v2_worker = worker
    return app


__all__ = ["attach_voice_intake_v2_routes"]
