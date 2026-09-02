from __future__ import annotations

import os
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from tempfile import NamedTemporaryFile

from .models import RegistryState


class ShowcaseStateStore:
    def __init__(self, path: Path) -> None:
        self.path = path.expanduser().resolve()
        self._lock = threading.RLock()

    def load(self) -> RegistryState:
        with self._lock:
            if not self.path.exists():
                return RegistryState()
            return RegistryState.model_validate_json(self.path.read_text(encoding="utf-8"))

    def save(self, state: RegistryState) -> None:
        with self._lock:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with NamedTemporaryFile(
                "w",
                encoding="utf-8",
                dir=self.path.parent,
                prefix=f".{self.path.name}.",
                delete=False,
            ) as handle:
                handle.write(state.model_dump_json(indent=2))
                handle.write("\n")
                temp_path = Path(handle.name)
            os.chmod(temp_path, 0o600)
            os.replace(temp_path, self.path)

    @contextmanager
    def transaction(self) -> Iterator[RegistryState]:
        with self._lock:
            state = self.load()
            yield state
            self.save(state)
