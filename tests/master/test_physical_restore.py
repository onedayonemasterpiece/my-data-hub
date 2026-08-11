from __future__ import annotations

import io
import tarfile
from pathlib import Path

import pytest

from my_data_hub.checkpoints.restore import PhysicalRestoreError, restore_physical_archive


class _File:
    kind = "physical"
    path = "physical/base.tar.gz"


class _Manifest:
    files = (_File(),)


def _archive(path: Path, name: str) -> None:
    path.parent.mkdir(parents=True)
    with tarfile.open(path, "w:gz") as stream:
        payload = b"18\n"
        item = tarfile.TarInfo(name)
        item.size = len(payload)
        stream.addfile(item, io.BytesIO(payload))


def test_physical_restore_extracts_regular_members_with_restrictive_modes(tmp_path: Path) -> None:
    package = tmp_path / "package"
    _archive(package / "physical/base.tar.gz", "PG_VERSION")
    target = tmp_path / "pgdata"
    restore_physical_archive(package, _Manifest(), target)  # type: ignore[arg-type]
    assert (target / "PG_VERSION").read_text() == "18\n"
    assert (target / "PG_VERSION").stat().st_mode & 0o077 == 0


def test_physical_restore_rejects_traversal(tmp_path: Path) -> None:
    package = tmp_path / "package"
    _archive(package / "physical/base.tar.gz", "../escape")
    with pytest.raises(PhysicalRestoreError, match="unsafe"):
        restore_physical_archive(package, _Manifest(), tmp_path / "pgdata")  # type: ignore[arg-type]
