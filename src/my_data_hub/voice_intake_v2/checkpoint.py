"""Private, manifest-bound stage receipts; never an authority to replay inference."""
from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Callable
from pathlib import Path
from typing import Any
from uuid import uuid4

from .contracts import InferenceReceipt


class CheckpointError(RuntimeError):
    pass


class AccountingPending(RuntimeError):
    """A provider result is durable; only its limiter accounting may be retried."""


def fingerprint(value: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode()).hexdigest()


def atomic_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        # The file is private from creation, not only after all content is written.
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(value, handle, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    finally:
        temporary.unlink(missing_ok=True)


class StageCheckpoint:
    def __init__(
        self, directory: Path, session_id: str, stage: str, manifest_sha: str,
        before_send: Callable[[], None] | None = None,
    ) -> None:
        if stage not in {"transcript", "summary"}:
            raise ValueError("invalid checkpoint stage")
        self.path = directory / f"{stage}.receipt.json"
        self.identity = {
            "version": 1, "session_id": session_id, "stage": stage,
            "manifest_sha256": manifest_sha,
        }
        self._before_send = before_send

    def dispatch(self) -> None:
        if self._before_send is not None:
            self._before_send()

    def save(self, receipt: InferenceReceipt, accounting: dict[str, Any] | None = None) -> None:
        payload = {
            **self.identity, "receipt": receipt.model_dump(mode="json"), "accounting": accounting,
        }
        atomic_json(self.path, {**payload, "sha256": fingerprint(payload)})

    def load(self) -> tuple[InferenceReceipt, dict[str, Any] | None] | None:
        try:
            if not self.path.exists():
                return None
            if self.path.is_symlink() or self.path.stat().st_size > 16 * 1024 * 1024:
                raise CheckpointError("checkpoint_invalid")
            payload = json.loads(self.path.read_text(encoding="utf-8"))
            if not isinstance(payload, dict):
                raise CheckpointError("checkpoint_invalid")
            digest = payload.pop("sha256", None)
            if digest != fingerprint(payload) or any(
                payload.get(key) != value for key, value in self.identity.items()
            ):
                raise CheckpointError("checkpoint_identity_mismatch")
            receipt = InferenceReceipt.model_validate(payload["receipt"])
            accounting = payload["accounting"]
            if accounting is not None and (
                not isinstance(accounting, dict)
                or accounting.get("lease", {}).get("request_uid") != receipt.request_uid
            ):
                raise CheckpointError("checkpoint_accounting_mismatch")
            return receipt, accounting
        except (OSError, ValueError, KeyError, TypeError, AttributeError) as exc:
            raise CheckpointError("checkpoint_unreadable") from exc
