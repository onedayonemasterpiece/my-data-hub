"""Safe physical checkpoint extraction for an isolated PostgreSQL runtime."""

from __future__ import annotations

import os
import tarfile
from pathlib import Path, PurePosixPath

from .manifest import CheckpointManifest


class PhysicalRestoreError(RuntimeError):
    """A physical archive was unsafe or could not be restored exactly."""


def restore_physical_archive(package: Path, manifest: CheckpointManifest, pgdata: Path) -> None:
    physical = {PurePosixPath(item.path): item for item in manifest.files if item.kind == "physical"}
    required = {PurePosixPath("physical/base.tar.gz"), PurePosixPath("physical/pg_wal.tar.gz")}
    if set(physical) != required:
        raise PhysicalRestoreError("checkpoint must contain exact base and streamed WAL archives")
    pgdata.mkdir(parents=True, exist_ok=False, mode=0o700)
    _extract_archive(package, PurePosixPath("physical/base.tar.gz"), pgdata)
    wal = pgdata / "pg_wal"
    wal.mkdir(parents=True, exist_ok=True, mode=0o700)
    _extract_archive(package, PurePosixPath("physical/pg_wal.tar.gz"), wal)
    _restrict_modes(pgdata)


def _extract_archive(package: Path, relative: PurePosixPath, destination_root: Path) -> None:
    archive = package.joinpath(*relative.parts)
    if archive.is_symlink() or not archive.is_file():
        raise PhysicalRestoreError("physical archive is not a regular file")
    root = destination_root.resolve()
    try:
        with tarfile.open(archive, mode="r:*") as stream:
            members = stream.getmembers()
            if not members or len(members) > 100_000:
                raise PhysicalRestoreError("physical archive member count is invalid")
            for member in members:
                name = PurePosixPath(member.name)
                if name.is_absolute() or ".." in name.parts or member.issym() or member.islnk() or member.isdev():
                    raise PhysicalRestoreError("physical archive contains an unsafe member")
                destination = root.joinpath(*name.parts).resolve()
                if destination != root and root not in destination.parents:
                    raise PhysicalRestoreError("physical archive member escapes PGDATA")
                if not (member.isdir() or member.isfile()):
                    raise PhysicalRestoreError("physical archive contains an unsupported member type")
            stream.extractall(root, members=members, filter="data")
    except (tarfile.TarError, OSError) as exc:
        raise PhysicalRestoreError("physical archive extraction failed") from exc


def _restrict_modes(root: Path) -> None:
    for current, directories, files in os.walk(root):
        Path(current).chmod(0o700)
        for name in directories:
            Path(current, name).chmod(0o700)
        for name in files:
            Path(current, name).chmod(0o600)
