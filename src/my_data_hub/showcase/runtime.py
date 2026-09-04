from __future__ import annotations

import hashlib
import hmac
import inspect
import json
import os
import re
import secrets
import shutil
import stat
import threading
import time
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from my_data_hub.showcase.gateway import (
    SHOWCASE_GATEWAY_PATH,
    SHOWCASE_TOOLS,
    SHOWCASE_WRITE_TOOLS,
)
from my_data_hub.showcase.manager import ShowcaseManager
from my_data_hub.showcase.requests import (
    MAX_ARGUMENT_BYTES,
    MAX_REQUEST_BYTES,
    ShowcaseRequestError,
    ShowcaseSourceError,
    resolve_mode,
)
from my_data_hub.showcase.source import ShowcaseSourceNotFoundError

_IDEMPOTENCY = re.compile(r"^[A-Za-z0-9._:-]{8,200}$")
_SECRET_SLUG = re.compile(r"^[A-Za-z0-9_-]{24,160}$")
_READ_TOOLS = SHOWCASE_TOOLS - SHOWCASE_WRITE_TOOLS


class ShowcaseRuntimeError(RuntimeError):
    code = "SHOWCASE_RUNTIME_ERROR"


class ShowcaseRuntimeAuthenticationError(ShowcaseRuntimeError):
    code = "SHOWCASE_RUNTIME_UNAUTHENTICATED"


class ShowcaseRuntimePermissionError(ShowcaseRuntimeError):
    code = "SHOWCASE_RUNTIME_FORBIDDEN"


class ShowcaseRuntimeConflictError(ShowcaseRuntimeError):
    code = "SHOWCASE_RUNTIME_CONFLICT"


class ShowcaseRuntimeRequestError(ShowcaseRuntimeError):
    code = "SHOWCASE_RUNTIME_INVALID_REQUEST"


class PrincipalDocument(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    subject: str = Field(min_length=1, max_length=256)
    client_id: str = Field(min_length=1, max_length=256)
    scopes: list[str] = Field(min_length=1, max_length=64)
    audience: str = Field(min_length=1, max_length=1024)
    token_id: str = Field(min_length=1, max_length=512)
    expires_at: int
    issuer: str = Field(min_length=1, max_length=1024)
    issued_at: int
    resource: str = Field(min_length=1, max_length=1024)


class ShowcaseInvocation(BaseModel):
    model_config = ConfigDict(extra="forbid")

    tool: str
    arguments: dict[str, Any] = Field(default_factory=dict)
    principal: PrincipalDocument


@dataclass(frozen=True, slots=True)
class ShowcaseRuntimeSettings:
    token_file: Path
    operation_journal: Path
    host: str = "127.0.0.1"
    port: int = 8790
    max_request_bytes: int = MAX_REQUEST_BYTES
    site_template_dir: Path | None = None
    site_runtime_dir: Path | None = None

    @classmethod
    def from_env(cls) -> ShowcaseRuntimeSettings:
        raw_token = os.getenv(
            "MY_DATA_HUB_SHOWCASE_RUNTIME_TOKEN_FILE",
            os.getenv("MY_DATA_HUB_SHOWCASE_GATEWAY_TOKEN_FILE", ""),
        ).strip()
        if not raw_token:
            raise ShowcaseRuntimeAuthenticationError("MY_DATA_HUB_SHOWCASE_RUNTIME_TOKEN_FILE is required")
        artifact_root = Path(os.getenv("MY_DATA_HUB_ARTIFACT_ROOT", "./artifacts")).expanduser().resolve()
        operation_journal = (
            Path(
                os.getenv(
                    "MY_DATA_HUB_SHOWCASE_OPERATION_JOURNAL",
                    str(artifact_root / "showcase-operations.json"),
                )
            )
            .expanduser()
            .resolve()
        )
        host = os.getenv("MY_DATA_HUB_SHOWCASE_RUNTIME_HOST", "127.0.0.1").strip()
        if host not in {"127.0.0.1", "localhost", "::1"}:
            raise ShowcaseRuntimeRequestError("showcase runtime must bind to a loopback address")
        try:
            port = int(os.getenv("MY_DATA_HUB_SHOWCASE_RUNTIME_PORT", "8790"))
            max_request_bytes = int(os.getenv("MY_DATA_HUB_SHOWCASE_MAX_REQUEST_BYTES", str(MAX_REQUEST_BYTES)))
        except ValueError as exc:
            raise ShowcaseRuntimeRequestError("showcase runtime numeric configuration is invalid") from exc
        if not 1 <= port <= 65535:
            raise ShowcaseRuntimeRequestError("showcase runtime port is invalid")
        if not 4096 <= max_request_bytes <= 262_144:
            raise ShowcaseRuntimeRequestError("showcase runtime request limit must be 4 KiB..256 KiB")
        template_raw = os.getenv("MY_DATA_HUB_SHOWCASE_SITE_TEMPLATE_DIR", "").strip()
        runtime_raw = os.getenv("MY_DATA_HUB_SHOWCASE_SITE_DIR", "").strip()
        settings = cls(
            token_file=Path(raw_token).expanduser().resolve(),
            operation_journal=operation_journal,
            host=host,
            port=port,
            max_request_bytes=max_request_bytes,
            site_template_dir=(Path(template_raw).expanduser().resolve() if template_raw else None),
            site_runtime_dir=(Path(runtime_raw).expanduser().resolve() if runtime_raw else None),
        )
        read_runtime_token(settings.token_file)
        return settings


def read_runtime_token(path: Path) -> str:
    try:
        file_stat = path.stat()
    except OSError as exc:
        raise ShowcaseRuntimeAuthenticationError("showcase runtime token file is unavailable") from exc
    if not stat.S_ISREG(file_stat.st_mode):
        raise ShowcaseRuntimeAuthenticationError("showcase runtime token path must be a regular file")
    if stat.S_IMODE(file_stat.st_mode) & 0o077:
        raise ShowcaseRuntimeAuthenticationError("showcase runtime token file must not be readable by group or others")
    token = path.read_text(encoding="utf-8").strip()
    if not 32 <= len(token) <= 512 or any(ord(char) < 33 for char in token):
        raise ShowcaseRuntimeAuthenticationError("showcase runtime token is malformed")
    return token


def prepare_site_runtime(settings: ShowcaseRuntimeSettings) -> None:
    template = settings.site_template_dir
    runtime = settings.site_runtime_dir
    if template is None or runtime is None or template == runtime:
        return
    if not template.is_dir():
        raise ShowcaseRuntimeRequestError("showcase site template is unavailable")
    node_modules = template / "node_modules"
    if not node_modules.is_dir():
        raise ShowcaseRuntimeRequestError("showcase renderer dependencies are unavailable")
    if runtime.exists():
        shutil.rmtree(runtime)
    runtime.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(template, runtime, ignore=shutil.ignore_patterns("node_modules", "dist"))
    # copytree preserves the immutable template root mode. Restore owner write
    # permission on the private runtime copy before adding its dependency link.
    runtime.chmod(stat.S_IMODE(runtime.stat().st_mode) | stat.S_IWUSR)
    (runtime / "node_modules").symlink_to(node_modules, target_is_directory=True)


@contextmanager
def _advisory_lock(path: Path) -> Iterator[None]:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        try:
            import fcntl

            fcntl.flock(descriptor, fcntl.LOCK_EX)
        except ImportError:  # pragma: no cover - Linux is the deployment target
            pass
        yield
    finally:
        try:
            import fcntl

            fcntl.flock(descriptor, fcntl.LOCK_UN)
        except ImportError:  # pragma: no cover
            pass
        os.close(descriptor)


class ShowcaseOperationJournal:
    """Small durable control-metadata journal; never a business-data store."""

    def __init__(self, path: Path, *, completed_limit: int = 256) -> None:
        self.path = path
        self.lock_path = path.with_suffix(path.suffix + ".lock")
        self.completed_limit = completed_limit
        self._thread_lock = threading.RLock()

    def load(self) -> dict[str, Any]:
        with self._thread_lock, _advisory_lock(self.lock_path):
            if not self.path.exists():
                return {"version": 1, "completed": {}, "rotations": {}}
            try:
                document = json.loads(self.path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                raise ShowcaseRuntimeError("showcase operation journal is corrupt") from exc
            if not isinstance(document, dict) or document.get("version") != 1:
                raise ShowcaseRuntimeError("unsupported showcase operation journal")
            document.setdefault("completed", {})
            document.setdefault("rotations", {})
            return document

    def save(self, document: Mapping[str, Any]) -> None:
        with self._thread_lock, _advisory_lock(self.lock_path):
            self.path.parent.mkdir(parents=True, exist_ok=True)
            payload = json.dumps(document, ensure_ascii=False, indent=2, sort_keys=True)
            temporary = self.path.with_suffix(self.path.suffix + ".tmp")
            descriptor = os.open(temporary, os.O_CREAT | os.O_TRUNC | os.O_WRONLY, 0o600)
            try:
                with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                    handle.write(payload)
                    handle.write("\n")
                    handle.flush()
                    os.fsync(handle.fileno())
                os.replace(temporary, self.path)
                os.chmod(self.path, 0o600)
            finally:
                if temporary.exists():
                    temporary.unlink(missing_ok=True)

    def remember(
        self,
        document: dict[str, Any],
        operation_key: str,
        result: Mapping[str, Any] | list[Any],
        fingerprint: str | None = None,
    ) -> None:
        completed = document.setdefault("completed", {})
        completed[operation_key] = {
            "completed_at": int(time.time()),
            "result": result,
            "fingerprint": fingerprint,
        }
        overflow = len(completed) - self.completed_limit
        if overflow > 0:
            oldest = sorted(
                completed,
                key=lambda key: int(completed[key].get("completed_at", 0)),
            )[:overflow]
            for key in oldest:
                completed.pop(key, None)


def _operation_key(tool: str, view_id: str, idempotency_key: str) -> str:
    material = f"{tool}\0{view_id}\0{idempotency_key}".encode()
    return hashlib.sha256(material).hexdigest()


def _validate_view_id(value: Any) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[a-z0-9][a-z0-9-]{1,62}[a-z0-9]", value):
        raise ShowcaseRequestError("INVALID_FIELD", "view_id")
    return value


def _validate_idempotency_key(value: Any) -> str:
    if not isinstance(value, str) or not _IDEMPOTENCY.fullmatch(value):
        raise ShowcaseRequestError("IDEMPOTENCY_REQUIRED", "idempotency_key")
    return value


def _call_method(target: Any, name: str, view_id: str, **kwargs: Any) -> Any:
    method = getattr(target, name, None)
    if not callable(method):
        raise ShowcaseRuntimeError(f"showcase manager does not implement {name}")
    parameters = inspect.signature(method).parameters
    if "mode" in kwargs and "mode" not in parameters:
        selected = kwargs.pop("mode")
        kwargs.update(dry_run=selected == "preview", publish=selected == "publish")
    accepted = {key: value for key, value in kwargs.items() if key in parameters}
    return method(view_id, **accepted)


def _url_from_result(value: Any) -> str:
    if isinstance(value, str) and value.startswith(("http://", "https://")):
        return value
    if isinstance(value, Mapping):
        for key in ("url", "link", "active_url"):
            candidate = value.get(key)
            if isinstance(candidate, str) and candidate.startswith(("http://", "https://")):
                return candidate
        for child in value.values():
            try:
                return _url_from_result(child)
            except ShowcaseRuntimeError:
                continue
    raise ShowcaseRuntimeError("showcase manager result does not contain an active URL")


def _slug_from_url(url: str) -> str:
    parts = [part for part in urlsplit(url).path.split("/") if part]
    if not parts:
        raise ShowcaseRuntimeError("showcase URL has no secret slug")
    slug = parts[-1]
    if not _SECRET_SLUG.fullmatch(slug):
        raise ShowcaseRuntimeError("showcase URL contains an invalid secret slug")
    return slug


def _mask_url(url: str) -> str:
    parts = urlsplit(url)
    path_parts = [part for part in parts.path.split("/") if part]
    if not path_parts:
        return urlunsplit((parts.scheme, parts.netloc, "/", "", ""))
    slug = path_parts[-1]
    masked = f"{slug[:4]}…{slug[-4:]}" if len(slug) > 10 else "…"
    path_parts[-1] = masked
    return urlunsplit((parts.scheme, parts.netloc, "/" + "/".join(path_parts) + "/", "", ""))


def _sanitize_list_result(value: Any) -> Any:
    if isinstance(value, list):
        return [_sanitize_list_result(item) for item in value]
    if not isinstance(value, Mapping):
        return value
    sanitized: dict[str, Any] = {}
    for raw_key, child in value.items():
        key = str(raw_key)
        lowered = key.lower()
        if (
            isinstance(child, str)
            and child.startswith(("http://", "https://"))
            and ("url" in lowered or "link" in lowered)
        ):
            sanitized[f"{key}_masked" if not lowered.endswith("masked") else key] = _mask_url(child)
            continue
        if isinstance(child, str) and "slug" in lowered:
            sanitized[f"{key}_sha256"] = hashlib.sha256(child.encode()).hexdigest()
            continue
        sanitized[key] = _sanitize_list_result(child)
    return sanitized


def _with_duplicate(result: Any, duplicate: bool) -> Any:
    if isinstance(result, Mapping):
        return {**dict(result), "duplicate": duplicate}
    return {"result": result, "duplicate": duplicate}


def _publisher_revoke(manager: Any, *, view_id: str, slug: str) -> None:
    publisher = None
    for attribute in ("publisher", "_publisher"):
        candidate = getattr(manager, attribute, None)
        if candidate is not None:
            publisher = candidate
            break
    if publisher is None:
        raise ShowcaseRuntimeError("showcase publisher is unavailable for recovery")
    method = getattr(publisher, "revoke", None)
    if not callable(method):
        raise ShowcaseRuntimeError("showcase publisher cannot revoke a stale slug")
    parameters = inspect.signature(method).parameters
    if "view_id" in parameters and "slug" in parameters:
        method(view_id=view_id, slug=slug)
    elif len(parameters) <= 1:
        method(slug)
    else:
        method(view_id, slug)


class ShowcaseOperationController:
    """Exact semantic command surface around the internal showcase manager."""

    def __init__(self, manager: Any, journal: ShowcaseOperationJournal) -> None:
        self.manager = manager
        self.journal = journal
        self._lock = threading.RLock()

    def invoke(
        self,
        tool: str,
        arguments: Mapping[str, Any],
        principal: PrincipalDocument,
    ) -> dict[str, Any] | list[Any]:
        if tool not in SHOWCASE_TOOLS:
            raise ShowcaseRuntimeRequestError("unknown showcase tool")
        required_scope = (
            "showcase:write" if tool in SHOWCASE_WRITE_TOOLS or tool == "showcase.get_link" else "showcase:read"
        )
        if required_scope not in set(principal.scopes):
            raise ShowcaseRuntimePermissionError("principal lacks the required showcase scope")
        if principal.expires_at < int(time.time()) - 30:
            raise ShowcaseRuntimeAuthenticationError("principal access token has expired")
        bounded = json.dumps(arguments, ensure_ascii=False).encode()
        if len(bounded) > MAX_ARGUMENT_BYTES:
            raise ShowcaseRequestError("REQUEST_TOO_LARGE")
        with self._lock:
            if tool == "showcase.list":
                return _sanitize_list_result(self.manager.list_surfaces())
            view_id = _validate_view_id(arguments.get("view_id"))
            if tool == "showcase.get_link":
                result = self.manager.get_link(view_id)
                if not isinstance(result, (dict, list)):
                    raise ShowcaseRuntimeError("showcase manager returned an invalid link result")
                return result
            if tool == "showcase.get_source":
                result = self.manager.get_source(view_id)
                if not isinstance(result, dict):
                    raise ShowcaseRuntimeError("showcase manager returned an invalid source result")
                return result
            mutation_arguments = {}
            if tool in {"showcase.apply", "showcase.create_view"}:
                legacy_registration = (
                    tool == "showcase.create_view"
                    and arguments.get("view") is None
                    and arguments.get("mode") is None
                    and arguments.get("dry_run") is None
                )
                if legacy_registration:
                    selected = "save" if arguments.get("publish") is False else "publish"
                    mutation_arguments = {"publish": arguments.get("publish")}
                else:
                    selected = resolve_mode(arguments.get("mode"), arguments.get("dry_run"), arguments.get("publish"))
                    mutation_arguments = {
                        "view": arguments.get("view"),
                        "items": arguments.get("items") or [],
                        "mode": selected,
                    }
                    if tool == "showcase.apply":
                        mutation_arguments["expected_source_revision"] = arguments.get("expected_source_revision")
                if selected == "preview":
                    # Pure preview never reads/writes the idempotency journal or consumes a key.
                    return _call_method(
                        self.manager,
                        "apply" if tool == "showcase.apply" else "create_view",
                        view_id,
                        **mutation_arguments,
                    )
            idempotency_key = _validate_idempotency_key(arguments.get("idempotency_key"))
            operation_key = _operation_key(tool, view_id, idempotency_key)
            journal = self.journal.load()
            completed = journal.get("completed", {}).get(operation_key)
            fingerprint = hashlib.sha256(
                json.dumps(arguments, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
            ).hexdigest()
            if isinstance(completed, Mapping) and isinstance(completed.get("result"), (dict, list)):
                if completed.get("fingerprint") not in {None, fingerprint}:
                    raise ShowcaseRequestError("IDEMPOTENCY_CONFLICT", "idempotency_key")
                return _with_duplicate(completed["result"], True)
            if tool == "showcase.rotate_link":
                result = self._rotate(
                    view_id=view_id,
                    idempotency_key=idempotency_key,
                    operation_key=operation_key,
                    journal=journal,
                )
            else:
                if tool in {"showcase.apply", "showcase.create_view"}:
                    result = _call_method(
                        self.manager,
                        "apply" if tool == "showcase.apply" else "create_view",
                        view_id,
                        idempotency_key=idempotency_key,
                        **mutation_arguments,
                    )
                    if not isinstance(result, (dict, list)):
                        raise ShowcaseRuntimeError("showcase manager returned an invalid result")
                    self.journal.remember(journal, operation_key, result, fingerprint)
                    self.journal.save(journal)
                    return _with_duplicate(result, False)
                method_name = {
                    "showcase.rebuild": "rebuild",
                    "showcase.apply": "apply",
                    "showcase.create_view": "create_view",
                    "showcase.revoke_link": "revoke_link",
                }[tool]
                result = _call_method(
                    self.manager,
                    method_name,
                    view_id,
                    idempotency_key=idempotency_key,
                )
                if not isinstance(result, (dict, list)):
                    raise ShowcaseRuntimeError("showcase manager returned an invalid result")
                self.journal.remember(journal, operation_key, result)
                self.journal.save(journal)
            return _with_duplicate(result, False)

    def _rotate(
        self,
        *,
        view_id: str,
        idempotency_key: str,
        operation_key: str,
        journal: dict[str, Any],
    ) -> dict[str, Any] | list[Any]:
        rotations = journal.setdefault("rotations", {})
        pending = rotations.get(view_id)
        if pending is not None and not isinstance(pending, Mapping):
            raise ShowcaseRuntimeError("showcase rotation journal is corrupt")
        if pending and pending.get("idempotency_key") != idempotency_key:
            raise ShowcaseRuntimeConflictError("another rotation for this view is awaiting recovery")
        current = self.manager.get_link(view_id)
        current_slug = _slug_from_url(_url_from_result(current))
        if not pending:
            pending = {
                "idempotency_key": idempotency_key,
                "operation_key": operation_key,
                "old_slug": current_slug,
                "new_slug": secrets.token_urlsafe(32).rstrip("="),
                "phase": "prepared",
                "started_at": int(time.time()),
            }
            rotations[view_id] = pending
            self.journal.save(journal)
        old_slug = str(pending.get("old_slug", ""))
        new_slug = str(pending.get("new_slug", ""))
        if not _SECRET_SLUG.fullmatch(old_slug) or not _SECRET_SLUG.fullmatch(new_slug):
            raise ShowcaseRuntimeError("showcase rotation journal contains invalid slugs")
        current = self.manager.get_link(view_id)
        current_slug = _slug_from_url(_url_from_result(current))
        if current_slug != new_slug:
            result = _call_method(
                self.manager,
                "rotate_link",
                view_id,
                slug=new_slug,
                idempotency_key=idempotency_key,
            )
            if not isinstance(result, (dict, list)):
                raise ShowcaseRuntimeError("showcase manager returned an invalid rotation result")
            current = self.manager.get_link(view_id)
            current_slug = _slug_from_url(_url_from_result(current))
            if current_slug != new_slug:
                raise ShowcaseRuntimeError("showcase rotation did not activate the prepared slug")
        else:
            result = current
        pending = dict(pending)
        pending["phase"] = "active_switched"
        rotations[view_id] = pending
        self.journal.save(journal)
        if old_slug != new_slug:
            _publisher_revoke(self.manager, view_id=view_id, slug=old_slug)
        rotations.pop(view_id, None)
        self.journal.remember(journal, operation_key, result)
        self.journal.save(journal)
        return result


def create_app(
    *,
    controller: ShowcaseOperationController,
    token: str,
    max_request_bytes: int = MAX_REQUEST_BYTES,
) -> FastAPI:
    app = FastAPI(
        title="my-data-hub showcase runtime",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )

    @app.middleware("http")
    async def request_bounds(request: Request, call_next):  # type: ignore[no-untyped-def]
        raw_length = request.headers.get("content-length")
        if raw_length:
            try:
                if int(raw_length) > max_request_bytes:
                    return JSONResponse(
                        status_code=413,
                        content={"ok": False, "code": "SHOWCASE_RUNTIME_REQUEST_TOO_LARGE"},
                    )
            except ValueError:
                return JSONResponse(
                    status_code=400,
                    content={"ok": False, "code": "SHOWCASE_RUNTIME_INVALID_LENGTH"},
                )
        return await call_next(request)

    @app.get("/health/ready")
    async def health_ready() -> dict[str, bool]:
        return {"ready": True}

    @app.post(SHOWCASE_GATEWAY_PATH)
    async def invoke(request: Request) -> JSONResponse:
        authorization = request.headers.get("authorization", "")
        scheme, _, supplied = authorization.partition(" ")
        if scheme.lower() != "bearer" or not hmac.compare_digest(supplied, token):
            return JSONResponse(
                status_code=401,
                content={"ok": False, "code": ShowcaseRuntimeAuthenticationError.code},
                headers={"Cache-Control": "no-store"},
            )
        try:
            body = bytearray()
            async for chunk in request.stream():
                body.extend(chunk)
                if len(body) > max_request_bytes:
                    raise ShowcaseRequestError("REQUEST_TOO_LARGE")
            invocation = ShowcaseInvocation.model_validate_json(bytes(body))
            result = controller.invoke(
                invocation.tool,
                invocation.arguments,
                invocation.principal,
            )
        except ShowcaseRequestError as exc:
            return JSONResponse(
                status_code=exc.http_status, content={"ok": False, "code": exc.code, "error": exc.payload()}
            )
        except ShowcaseSourceNotFoundError:
            error = ShowcaseRequestError("VIEW_NOT_FOUND", "view_id")
            return JSONResponse(status_code=404, content={"ok": False, "code": error.code, "error": error.payload()})
        except ValidationError as exc:
            first = exc.errors(include_url=False, include_input=False)[0]
            field = ".".join(str(part) for part in first.get("loc", []))[:160]
            error = ShowcaseRequestError("INVALID_FIELD", field or None)
            return JSONResponse(status_code=400, content={"ok": False, "code": error.code, "error": error.payload()})
        except ShowcaseSourceError:
            error = ShowcaseRequestError("SOURCE_UNAVAILABLE")
            return JSONResponse(status_code=503, content={"ok": False, "code": error.code, "error": error.payload()})
        except ShowcaseRuntimeAuthenticationError as exc:
            return JSONResponse(status_code=401, content={"ok": False, "code": exc.code})
        except ShowcaseRuntimePermissionError as exc:
            return JSONResponse(status_code=403, content={"ok": False, "code": exc.code})
        except ShowcaseRuntimeConflictError as exc:
            return JSONResponse(status_code=409, content={"ok": False, "code": exc.code})
        except (ShowcaseRuntimeRequestError, ValueError, TypeError, json.JSONDecodeError) as exc:
            code = exc.code if isinstance(exc, ShowcaseRuntimeError) else "SHOWCASE_RUNTIME_INVALID_REQUEST"
            return JSONResponse(status_code=400, content={"ok": False, "code": code})
        except Exception:
            return JSONResponse(
                status_code=500,
                content={"ok": False, "code": ShowcaseRuntimeError.code},
            )
        return JSONResponse(
            status_code=200,
            content={"ok": True, "result": result},
            headers={"Cache-Control": "no-store"},
        )

    return app


def build_runtime() -> tuple[ShowcaseRuntimeSettings, FastAPI]:
    settings = ShowcaseRuntimeSettings.from_env()
    prepare_site_runtime(settings)
    manager = ShowcaseManager.from_env()
    journal = ShowcaseOperationJournal(settings.operation_journal)
    app = create_app(
        controller=ShowcaseOperationController(manager, journal),
        token=read_runtime_token(settings.token_file),
        max_request_bytes=settings.max_request_bytes,
    )
    return settings, app


def main() -> None:
    import uvicorn

    settings, app = build_runtime()
    uvicorn.run(
        app,
        host=settings.host,
        port=settings.port,
        access_log=False,
        log_level=os.getenv("MY_DATA_HUB_LOG_LEVEL", "INFO").lower(),
        proxy_headers=False,
        server_header=False,
    )


if __name__ == "__main__":
    main()
