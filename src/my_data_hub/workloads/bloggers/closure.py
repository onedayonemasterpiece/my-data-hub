"""Production FINAL-BLOGGER closure orchestration.

This module carries only bounded metadata.  The 266 source rows stream from YDB
inside the ACTIVE Kaggle master directly into its local PostgreSQL process.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import urlsplit
from uuid import UUID, uuid5

from my_data_hub.hashing import canonical_json_bytes

from .master_stage import (
    EXPECTED_BLOGGER_ROWS,
    BloggerImportStageReceipt,
    BloggerMigrationRequest,
)

EXTERNAL_BLOCKED = 78
FINAL_RECEIPT_SCHEMA = "my-data-hub-blogger-closure.v1"
FINAL_RECEIPT_MAX_BYTES = 256 * 1024
LOCAL_CONTROL_URL = "http://127.0.0.1:8080"
CANONICAL_MCP_URL = "https://mcp-datahub.kenigevents.ru/mcp"
_CLOSURE_NAMESPACE = UUID("650bb578-a4b2-54f2-8783-8c104265017c")


class BloggerClosureError(RuntimeError):
    pass


def modern_kaggle_token_configured() -> bool:
    if os.environ.get("KAGGLE_API_TOKEN", "").strip():
        return True
    path = Path(os.environ.get("KAGGLE_CONFIG_DIR", "~/.kaggle")).expanduser() / "access_token"
    return path.is_file() and not path.is_symlink() and path.stat().st_size > 20


class ClosureControl(Protocol):
    def ensure_master(self, idempotency_key: str) -> dict[str, Any]: ...
    def master_status(self) -> dict[str, Any]: ...
    def create_request(self, request: BloggerMigrationRequest) -> dict[str, Any]: ...
    def request_status(self, request_id: UUID) -> dict[str, Any]: ...


class ClosureMcp(Protocol):
    def call(self, tool: str, arguments: dict[str, Any]) -> dict[str, Any]: ...


@dataclass(frozen=True, slots=True)
class ClosureConfig:
    control_url: str
    idempotency_key: str
    project_id: UUID
    snapshot_at: datetime
    source_revision: str
    timeout_seconds: int = 43_000
    poll_seconds: float = 10.0

    def __post_init__(self) -> None:
        parsed = urlsplit(self.control_url)
        if (
            self.control_url != LOCAL_CONTROL_URL
            or parsed.scheme != "http"
            or parsed.hostname != "127.0.0.1"
            or parsed.port != 8080
            or parsed.path not in {"", "/"}
            or parsed.username
            or parsed.password
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError("control URL must be the exact loopback-only control endpoint")
        if not 8 <= len(self.idempotency_key) <= 200:
            raise ValueError("blogger closure idempotency key is invalid")
        if self.snapshot_at.tzinfo is None:
            raise ValueError("blogger closure snapshot timestamp must be timezone-aware")
        if len(self.source_revision) != 40 or any(c not in "0123456789abcdef" for c in self.source_revision):
            raise ValueError("source revision must be an exact lowercase commit SHA")
        if not 600 <= self.timeout_seconds <= 43_000:
            raise ValueError("blogger closure timeout must be 600..43000 seconds")
        if not 1 <= self.poll_seconds <= 60:
            raise ValueError("blogger closure polling interval is invalid")


class LocalClosureControl:
    def __init__(self, config: ClosureConfig) -> None:
        self.config = config

    def _call(self, method: str, path: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        body = canonical_json_bytes(payload) if payload is not None else None
        request = urllib.request.Request(
            self.config.control_url.rstrip("/") + path,
            data=body,
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
            method=method,
        )
        try:
            with urllib.request.urlopen(request, timeout=20) as response:
                raw = response.read(256 * 1024 + 1)
        except (OSError, urllib.error.HTTPError) as exc:
            raise BloggerClosureError("bounded control-plane request failed") from exc
        if len(raw) > 256 * 1024:
            raise BloggerClosureError("control-plane response exceeds 256 KiB")
        try:
            value = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise BloggerClosureError("control-plane response is not JSON") from exc
        if not isinstance(value, dict):
            raise BloggerClosureError("control-plane response is not an object")
        return value

    def ensure_master(self, idempotency_key: str) -> dict[str, Any]:
        return self._call("POST", "/control/v1/master/ensure", {"idempotency_key": idempotency_key})

    def master_status(self) -> dict[str, Any]:
        return self._call("GET", "/control/v1/master")

    def create_request(self, request: BloggerMigrationRequest) -> dict[str, Any]:
        return self._call("POST", "/control/v1/blogger-closure/requests", request.model_dump(mode="json"))

    def request_status(self, request_id: UUID) -> dict[str, Any]:
        return self._call("GET", f"/control/v1/blogger-closure/requests/{request_id}")


class StreamableHttpClosureMcp:
    def __init__(self, endpoint: str, token: str) -> None:
        parsed = urlsplit(endpoint)
        if (
            endpoint != CANONICAL_MCP_URL
            or parsed.scheme != "https"
            or parsed.hostname != "mcp-datahub.kenigevents.ru"
            or parsed.port is not None
            or parsed.username
            or parsed.password
            or parsed.path != "/mcp"
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError("MCP endpoint must be the owner-approved canonical HTTPS resource")
        if not 24 <= len(token) <= 4096 or any(c.isspace() for c in token):
            raise ValueError("MCP bearer token is invalid")
        self.endpoint = endpoint
        self._token = token

    async def _call(self, tool: str, arguments: dict[str, Any]) -> dict[str, Any]:
        import httpx2
        from mcp import ClientSession
        from mcp.client.streamable_http import streamable_http_client

        async with (
            httpx2.AsyncClient(
                headers={"Authorization": f"Bearer {self._token}"},
                follow_redirects=False,
                timeout=httpx2.Timeout(30.0, connect=5.0),
            ) as client,
            streamable_http_client(self.endpoint, http_client=client) as streams,
        ):
            read_stream, write_stream = streams
            async with ClientSession(read_stream, write_stream, read_timeout_seconds=30) as session:
                await session.initialize()
                result = await session.call_tool(tool, arguments)
                if bool(getattr(result, "is_error", getattr(result, "isError", False))):
                    raise BloggerClosureError(f"MCP {tool} returned an error")
                structured = getattr(result, "structured_content", getattr(result, "structuredContent", None))
                if isinstance(structured, dict):
                    return structured
                for content in getattr(result, "content", ()):
                    text = getattr(content, "text", None)
                    if isinstance(text, str):
                        value = json.loads(text)
                        if isinstance(value, dict):
                            return value
                raise BloggerClosureError(f"MCP {tool} returned no structured object")

    def call(self, tool: str, arguments: dict[str, Any]) -> dict[str, Any]:
        return asyncio.run(self._call(tool, arguments))


def _wait(deadline: float, interval: float, fn: Any, predicate: Any) -> dict[str, Any]:
    while time.monotonic() < deadline:
        value = fn()
        if predicate(value):
            return value
        time.sleep(min(interval, max(0.0, deadline - time.monotonic())))
    raise BloggerClosureError("blogger closure exceeded its bounded deadline")


def _require_checkpoint(status: dict[str, Any], request_status: dict[str, Any]) -> dict[str, Any]:
    receipt = request_status.get("checkpoint_receipt")
    current = status.get("current")
    if not isinstance(receipt, dict) or not isinstance(current, dict):
        raise BloggerClosureError("verified blogger checkpoint evidence is absent")
    if (
        status.get("current_checkpoint_id") != receipt.get("checkpoint_id")
        or current.get("checkpoint_id") != receipt.get("checkpoint_id")
        or current.get("manifest_sha256") != receipt.get("manifest_sha256")
        or current.get("status") != "VERIFIED"
        or not isinstance(current.get("exact_version_ref"), str)
    ):
        raise BloggerClosureError("checkpoint HEAD differs from the exact blogger promotion")
    return current


def _require_accounting(value: dict[str, Any], imported: BloggerImportStageReceipt) -> dict[str, Any]:
    if value.get("found") is not True or not isinstance(value.get("accounting"), dict):
        raise BloggerClosureError("bounded MCP migration accounting is absent")
    row = value["accounting"]
    exact = {
        "export_batch_id": str(imported.export_batch_id),
        "expected_row_count": EXPECTED_BLOGGER_ROWS,
        "status": "accepted",
        "logical_sha256": imported.logical_sha256,
        "record_id_set_sha256": imported.record_id_set_sha256,
        "canonical_outcome_sha256": imported.canonical_outcome_sha256,
        "duplicate_groups_pending": 0,
        "imported_canonical_revision": imported.canonical_revision,
        "raw_count": EXPECTED_BLOGGER_ROWS,
        "dispositioned_count": EXPECTED_BLOGGER_ROWS,
        "undispositioned_count": 0,
        "quarantined_count": 0,
        "actor_count": EXPECTED_BLOGGER_ROWS,
        "account_count": imported.account_count,
        "checkpoint_required": True,
    }
    if any(row.get(key) != expected for key, expected in exact.items()):
        raise BloggerClosureError("MCP accounting differs from the committed 266-row receipt")
    if value.get("canonical_revision") != imported.canonical_revision:
        raise BloggerClosureError("cold-restored master revision differs from the import")
    return row


def run_blogger_closure(
    config: ClosureConfig,
    *,
    control: ClosureControl,
    mcp: ClosureMcp,
    now: Any = lambda: datetime.now(UTC),
) -> dict[str, Any]:
    """Run the single fail-closed production closure state machine."""

    started_at = now()
    deadline = time.monotonic() + config.timeout_seconds
    ensure = control.ensure_master(f"{config.idempotency_key}:master")
    operation_id = UUID(str(ensure.get("operation_id")))
    _wait(
        deadline,
        config.poll_seconds,
        control.master_status,
        lambda value: (
            value.get("master_state") == "ACTIVE" and value.get("master_instance_id") and value.get("master_epoch")
        ),
    )
    request_id = uuid5(_CLOSURE_NAMESPACE, config.idempotency_key)
    request = BloggerMigrationRequest(
        request_id=request_id,
        operation_id=operation_id,
        project_id=config.project_id,
        snapshot_at=config.snapshot_at,
        source_revision=config.source_revision,
    )
    created = control.create_request(request)
    if created.get("request_sha256") != request.request_sha256:
        raise BloggerClosureError("control plane stored a different blogger request")
    request_status = _wait(
        deadline,
        config.poll_seconds,
        lambda: control.request_status(request_id),
        lambda value: value.get("state") in {"CHECKPOINT_VERIFIED", "FAILED"},
    )
    if request_status.get("state") != "CHECKPOINT_VERIFIED":
        raise BloggerClosureError("blogger import did not reach verified checkpoint")
    imported = BloggerImportStageReceipt.model_validate(request_status.get("import_receipt"))
    if imported.request_sha256 != request.request_sha256 or imported.operation_id != operation_id:
        raise BloggerClosureError("import receipt does not bind the exact request/operation")
    checkpoint_status = mcp.call("checkpoint.status", {})
    checkpoint = _require_checkpoint(checkpoint_status, request_status)
    rotation = mcp.call(
        "master.rotation.request",
        {
            "idempotency_key": f"{config.idempotency_key}:cold-restore",
            "checkpoint_id": checkpoint["checkpoint_id"],
            "exact_version_ref": checkpoint["exact_version_ref"],
            "expected_active_epoch": int(request_status["claimed_epoch"]),
            "expected_canonical_revision": imported.canonical_revision,
            "timeout_seconds": min(1800, config.timeout_seconds),
        },
    )
    rotation_operation_id = str(rotation.get("operation_id", ""))
    if not rotation_operation_id:
        raise BloggerClosureError("rotation did not return a durable operation identity")
    rotation_status = _wait(
        deadline,
        config.poll_seconds,
        lambda: mcp.call("operation.get", {"operation_id": rotation_operation_id}),
        lambda value: value.get("state") in {"DURABLE_COMPLETE", "FAILED", "FENCED", "ORPHANED"},
    )
    if rotation_status.get("state") != "DURABLE_COMPLETE":
        raise BloggerClosureError("cold-restore rotation did not become DURABLE_COMPLETE")
    cold_master = _wait(
        deadline,
        config.poll_seconds,
        lambda: mcp.call("master.status", {}),
        lambda value: value.get("master_state") == "ACTIVE" and int(value.get("master_epoch") or 0) > imported.epoch,
    )
    accounting = _require_accounting(
        mcp.call("bloggers.migration.accounting", {"export_batch_id": str(imported.export_batch_id)}),
        imported,
    )
    statistics = mcp.call("bloggers.statistics", {})
    if statistics.get("canonical_revision") != imported.canonical_revision:
        raise BloggerClosureError("blogger statistics revision differs from the restored import")
    if (
        not isinstance(statistics.get("statistics"), dict)
        or statistics["statistics"].get("bloggers") != EXPECTED_BLOGGER_ROWS
    ):
        raise BloggerClosureError("blogger statistics do not prove exactly 266 canonical bloggers")
    receipt_id = uuid5(_CLOSURE_NAMESPACE, f"receipt:{request_id}")
    receipt: dict[str, Any] = {
        "schema_version": FINAL_RECEIPT_SCHEMA,
        "receipt_id": str(receipt_id),
        "status": "DURABLE_COMPLETE",
        "started_at": started_at.astimezone(UTC).isoformat().replace("+00:00", "Z"),
        "completed_at": now().astimezone(UTC).isoformat().replace("+00:00", "Z"),
        "closure_idempotency_key_sha256": hashlib.sha256(config.idempotency_key.encode()).hexdigest(),
        "request_id": str(request_id),
        "request_sha256": request.request_sha256,
        "ensure_operation_id": str(operation_id),
        "rotation_operation_id": rotation_operation_id,
        "import_runtime": {
            "run_id": request_status["claimed_run_id"],
            "attempt_id": request_status["claimed_attempt_id"],
            "master_instance_id": request_status["claimed_master_instance_id"],
            "epoch": request_status["claimed_epoch"],
        },
        "import_receipt": imported.model_dump(mode="json"),
        "import_receipt_sha256": imported.receipt_sha256,
        "checkpoint": {
            "generation": checkpoint_status["generation"],
            "checkpoint_id": checkpoint["checkpoint_id"],
            "exact_version_ref": checkpoint["exact_version_ref"],
            "manifest_sha256": checkpoint["manifest_sha256"],
        },
        "cold_restore": {
            "master_instance_id": cold_master.get("instance_id"),
            "epoch": cold_master.get("master_epoch"),
            "canonical_revision": cold_master.get("canonical_revision"),
        },
        "mcp_accounting": accounting,
        "mcp_statistics": statistics["statistics"],
    }
    encoded = canonical_json_bytes(receipt)
    if len(encoded) > FINAL_RECEIPT_MAX_BYTES:
        raise BloggerClosureError("final blogger closure receipt exceeds 256 KiB")
    return receipt
