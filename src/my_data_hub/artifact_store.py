from __future__ import annotations

import os
import tempfile
from dataclasses import dataclass
from pathlib import Path

from my_data_hub.hashing import sha256_file


class ArtifactStoreError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class StoredArtifact:
    locator: str
    path: Path
    sha256: str
    byte_size: int


@dataclass(slots=True)
class LocalArtifactStore:
    root: Path

    def __post_init__(self) -> None:
        self.root = self.root.expanduser().resolve()
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)

    def _resolve(self, relative_path: str) -> Path:
        candidate = (self.root / relative_path).resolve()
        try:
            candidate.relative_to(self.root)
        except ValueError as exc:
            raise ArtifactStoreError("artifact path escapes configured root") from exc
        return candidate

    def write_bytes(self, relative_path: str, payload: bytes) -> StoredArtifact:
        destination = self._resolve(relative_path)
        destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        fd, temporary_name = tempfile.mkstemp(
            prefix=f".{destination.name}.", suffix=".tmp", dir=destination.parent
        )
        temporary = Path(temporary_name)
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.chmod(temporary, 0o600)
            os.replace(temporary, destination)
        finally:
            temporary.unlink(missing_ok=True)
        return StoredArtifact(
            locator=f"file://{destination}",
            path=destination,
            sha256=sha256_file(destination),
            byte_size=destination.stat().st_size,
        )
