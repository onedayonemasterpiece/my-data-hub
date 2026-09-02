from __future__ import annotations

import json
import os
import shlex
import shutil
import subprocess
from pathlib import Path
from tempfile import mkdtemp
from typing import Protocol

from .models import BuildReceipt


class ShowcasePublisherError(RuntimeError):
    """Raised when a checked artifact cannot be published or revoked."""


class ShowcasePublisher(Protocol):
    def publish(self, source: Path, receipt: BuildReceipt) -> str: ...
    def revoke(self, *, view_id: str, slug: str) -> None: ...


class LocalDirectoryPublisher:
    """Development publisher that mirrors the final `/v/<slug>/` object layout."""

    def __init__(self, *, root: Path, origin: str) -> None:
        self.root = root.expanduser().resolve()
        self.origin = origin.rstrip("/")

    def publish(self, source: Path, receipt: BuildReceipt) -> str:
        target = self.root / "v" / receipt.slug
        target.parent.mkdir(parents=True, exist_ok=True)
        staging = Path(mkdtemp(prefix=f".{receipt.slug}.", dir=target.parent))
        try:
            shutil.copytree(source, staging, dirs_exist_ok=True)
            shutil.rmtree(target, ignore_errors=True)
            os.replace(staging, target)
        finally:
            shutil.rmtree(staging, ignore_errors=True)
        return f"{self.origin}/v/{receipt.slug}/"

    def revoke(self, *, view_id: str, slug: str) -> None:
        del view_id
        shutil.rmtree(self.root / "v" / slug, ignore_errors=True)


class CommandPublisher:
    """Runs deployment-provided argv templates without a shell.

    Templates are JSON arrays and support `{source}`, `{prefix}`, `{slug}` and
    `{view_id}` placeholders. This keeps the product code independent from the
    selected S3-compatible client while preserving deterministic inputs.
    """

    def __init__(
        self,
        *,
        publish_argv: list[str],
        revoke_argv: list[str],
        origin: str,
        timeout_seconds: int = 180,
    ) -> None:
        if not publish_argv or not revoke_argv:
            raise ValueError("publish and revoke commands are required")
        self.publish_argv = publish_argv
        self.revoke_argv = revoke_argv
        self.origin = origin.rstrip("/")
        self.timeout_seconds = timeout_seconds

    @classmethod
    def from_json_env(cls, *, origin: str) -> CommandPublisher:
        try:
            publish = json.loads(os.environ["MY_DATA_HUB_SHOWCASE_PUBLISH_COMMAND_JSON"])
            revoke = json.loads(os.environ["MY_DATA_HUB_SHOWCASE_REVOKE_COMMAND_JSON"])
        except (KeyError, json.JSONDecodeError) as exc:
            raise ShowcasePublisherError("publisher command JSON is missing or invalid") from exc
        if not isinstance(publish, list) or not all(isinstance(value, str) for value in publish):
            raise ShowcasePublisherError("publish command must be a JSON array of strings")
        if not isinstance(revoke, list) or not all(isinstance(value, str) for value in revoke):
            raise ShowcasePublisherError("revoke command must be a JSON array of strings")
        return cls(publish_argv=publish, revoke_argv=revoke, origin=origin)

    @staticmethod
    def _expand(template: list[str], *, source: Path | None, view_id: str, slug: str) -> list[str]:
        values = {
            "source": str(source) if source else "",
            "prefix": f"v/{slug}/",
            "slug": slug,
            "view_id": view_id,
        }
        return [part.format_map(values) for part in template]

    def _run(self, argv: list[str]) -> None:
        process = subprocess.run(
            argv,
            text=True,
            capture_output=True,
            timeout=self.timeout_seconds,
            check=False,
        )
        if process.returncode != 0:
            printable = shlex.join(argv)
            detail = "\n".join(part for part in (process.stdout, process.stderr) if part).strip()
            raise ShowcasePublisherError(
                f"publisher command failed ({process.returncode}): {printable}\n{detail[-4000:]}"
            )

    def publish(self, source: Path, receipt: BuildReceipt) -> str:
        self._run(
            self._expand(
                self.publish_argv,
                source=source,
                view_id=receipt.view_id,
                slug=receipt.slug,
            )
        )
        return f"{self.origin}/v/{receipt.slug}/"

    def revoke(self, *, view_id: str, slug: str) -> None:
        self._run(self._expand(self.revoke_argv, source=None, view_id=view_id, slug=slug))
