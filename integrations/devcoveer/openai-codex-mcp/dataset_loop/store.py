from __future__ import annotations

import fcntl
import json
import os
import re
import tempfile
from contextlib import contextmanager, suppress
from pathlib import Path
from typing import Any


class AtomicFileStore:
    """Private, lock-protected JSON state with atomic 0600 writes."""

    def __init__(self, root: Path) -> None:
        self.root = root
        root.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(root, 0o700)
        self.lock_path = root / ".lock"
        self.lock_path.touch(mode=0o600, exist_ok=True)
        os.chmod(self.lock_path, 0o600)

    def _path(self, key: str) -> Path:
        if re.fullmatch(r"[A-Za-z0-9_.-]{1,180}", key) is None:
            raise ValueError("unsafe state key")
        return self.root / f"{key}.json"

    @contextmanager
    def locked(self):
        with self.lock_path.open("r+") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    def write(self, key: str, value: dict[str, Any]) -> None:
        encoded = (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode()
        if len(encoded) > 4_000_000:
            raise ValueError("state document too large")
        with self.locked():
            fd, temporary = tempfile.mkstemp(prefix=".tmp-", dir=self.root)
            try:
                os.fchmod(fd, 0o600)
                with os.fdopen(fd, "wb") as handle:
                    handle.write(encoded)
                    handle.flush()
                    os.fsync(handle.fileno())
                target = self._path(key)
                os.replace(temporary, target)
                os.chmod(target, 0o600)
            finally:
                with suppress(FileNotFoundError):
                    os.unlink(temporary)

    def read(self, key: str) -> dict[str, Any] | None:
        path = self._path(key)
        with self.locked():
            if not path.is_file() or path.is_symlink() or path.stat().st_size > 4_000_000:
                return None
            try:
                value = json.loads(path.read_text("utf-8"))
            except (OSError, UnicodeError, json.JSONDecodeError):
                return None
        return value if isinstance(value, dict) else None

    def keys(self, prefix: str = "") -> list[str]:
        with self.locked():
            return sorted(path.stem for path in self.root.glob(f"{prefix}*.json") if not path.is_symlink())
