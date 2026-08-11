"""Consistent physical plus portable logical PostgreSQL checkpoint creation."""

from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol


class ArchiveError(RuntimeError):
    """A backup command or artifact contract failed."""


class Runner(Protocol):
    def run(self, arguments: list[str], *, timeout_seconds: int) -> None: ...


class SafeRunner:
    def run(self, arguments: list[str], *, timeout_seconds: int) -> None:
        result = subprocess.run(
            arguments,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )
        if result.returncode:
            raise ArchiveError(f"{Path(arguments[0]).name} failed with exit {result.returncode}")


@dataclass(frozen=True, slots=True)
class BackupTools:
    pg_basebackup: Path
    pg_dump: Path


class ArchiveCreator:
    """Create a WAL-consistent tar base backup and no-owner custom logical dump."""

    def __init__(self, tools: BackupTools, runner: Runner | None = None) -> None:
        self.tools = tools
        self.runner = runner or SafeRunner()

    def create(self, *, database_url: str, package: Path, timeout_seconds: int = 1800) -> dict[str, str]:
        if not database_url.startswith(("postgresql://", "postgres://")):
            raise ValueError("database_url must use the PostgreSQL protocol")
        if not 60 <= timeout_seconds <= 7200:
            raise ValueError("checkpoint timeout is outside the bounded contract")
        physical = package / "physical"
        logical = package / "logical"
        receipts = package / "receipts"
        for directory in (physical, logical, receipts):
            directory.mkdir(parents=True, exist_ok=False, mode=0o700)
        self.runner.run(
            [
                str(self.tools.pg_basebackup),
                "--dbname",
                database_url,
                "--pgdata",
                str(physical),
                "--format=tar",
                "--gzip",
                "--wal-method=stream",
                "--checkpoint=fast",
                "--no-password",
            ],
            timeout_seconds=timeout_seconds,
        )
        logical_dump = logical / "hub.dump"
        self.runner.run(
            [
                str(self.tools.pg_dump),
                "--dbname",
                database_url,
                "--file",
                str(logical_dump),
                "--format=custom",
                "--compress=9",
                "--no-owner",
                "--no-privileges",
                "--no-password",
            ],
            timeout_seconds=timeout_seconds,
        )
        base_candidates = sorted(physical.glob("base.tar*"))
        if len(base_candidates) != 1 or not base_candidates[0].is_file() or not logical_dump.is_file():
            raise ArchiveError("physical/logical checkpoint artifacts are incomplete")
        return {
            base_candidates[0].relative_to(package).as_posix(): "physical",
            logical_dump.relative_to(package).as_posix(): "logical",
        }


def write_probe_receipt(path: Path, payload: dict[str, object]) -> None:
    """Write a bounded non-secret verification/restore receipt."""

    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    lowered = encoded.lower()
    if len(encoded.encode("utf-8")) > 64 * 1024:
        raise ArchiveError("checkpoint receipt exceeds 64 KiB")
    if any(marker in lowered for marker in ("postgresql://", "password", "secret", "token")):
        raise ArchiveError("checkpoint receipt appears to contain a credential")
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.write_text(encoded + "\n", encoding="utf-8")
    path.chmod(0o600)
