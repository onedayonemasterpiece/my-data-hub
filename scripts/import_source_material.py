#!/usr/bin/env python3
"""Import immutable source material from an authenticated local Git clone.

This script never fetches credentials and never guesses source bytes. It extracts the
exact Git object requested by the provenance manifest, writes it atomically, verifies
SHA-256 and updates only the matching manifest entry.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import subprocess
import tempfile
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
MANIFEST_PATH = ROOT / "docs/source-material/source-manifest.yaml"


class SourceImportError(RuntimeError):
    pass


def _git(repo: Path, *args: str) -> bytes:
    process = subprocess.run(
        ["git", "-C", str(repo), *args],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if process.returncode != 0:
        message = process.stderr.decode("utf-8", errors="replace").strip()
        raise SourceImportError(f"git {' '.join(args)} failed: {message}")
    return process.stdout


def _atomic_write(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def import_entry(
    *,
    source_repo: Path,
    source_repository: str,
    source_path: str | None = None,
    source_commit: str | None = None,
) -> dict[str, str]:
    manifest = yaml.safe_load(MANIFEST_PATH.read_text(encoding="utf-8"))
    if not isinstance(manifest, dict) or not isinstance(manifest.get("sources"), list):
        raise SourceImportError("invalid source provenance manifest")
    candidates = [
        entry
        for entry in manifest["sources"]
        if entry.get("source_repository") == source_repository
        and (source_path is None or entry.get("source_path") == source_path)
    ]
    if len(candidates) != 1:
        raise SourceImportError(
            f"expected exactly one manifest entry, found {len(candidates)}"
        )
    entry = candidates[0]
    commit = source_commit or str(entry.get("source_commit") or "").strip()
    path = source_path or str(entry.get("source_path") or "").strip()
    destination_value = str(entry.get("destination_path") or "").strip()
    if not commit or not path or not destination_value:
        raise SourceImportError("manifest entry is missing commit/path/destination")

    source_repo = source_repo.expanduser().resolve()
    if not (source_repo / ".git").exists():
        raise SourceImportError(f"not a Git working tree: {source_repo}")
    resolved_commit = _git(source_repo, "rev-parse", f"{commit}^{{commit}}").decode().strip()
    payload = _git(source_repo, "show", f"{resolved_commit}:{path}")
    if not payload:
        raise SourceImportError("source object is empty; refusing silent import")

    destination = (ROOT / destination_value).resolve()
    try:
        destination.relative_to(ROOT)
    except ValueError as exc:
        raise SourceImportError("destination escapes repository") from exc
    _atomic_write(destination, payload)
    digest = hashlib.sha256(payload).hexdigest()
    if hashlib.sha256(destination.read_bytes()).hexdigest() != digest:
        raise SourceImportError("destination SHA-256 readback failed")

    entry["source_commit"] = resolved_commit
    entry["sha256"] = digest
    entry["status"] = "verified_import"
    rendered = yaml.safe_dump(
        manifest,
        allow_unicode=True,
        sort_keys=False,
        width=1000,
    ).encode("utf-8")
    _atomic_write(MANIFEST_PATH, rendered)
    return {
        "source_repository": source_repository,
        "source_path": path,
        "source_commit": resolved_commit,
        "destination_path": destination.relative_to(ROOT).as_posix(),
        "sha256": digest,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-repo", type=Path, required=True)
    parser.add_argument("--source-repository", default="onedayonemasterpiece/idea-hub")
    parser.add_argument("--source-path")
    parser.add_argument("--source-commit")
    args = parser.parse_args()
    result = import_entry(
        source_repo=args.source_repo,
        source_repository=args.source_repository,
        source_path=args.source_path,
        source_commit=args.source_commit,
    )
    print(yaml.safe_dump(result, allow_unicode=True, sort_keys=False), end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
