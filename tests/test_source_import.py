from __future__ import annotations

import hashlib
import subprocess
from pathlib import Path

import yaml

import scripts.import_source_material as importer


def git(repo: Path, *args: str) -> str:
    process = subprocess.run(
        ["git", "-C", str(repo), *args],
        text=True,
        capture_output=True,
        check=True,
    )
    return process.stdout.strip()


def test_exact_source_import_uses_git_object_and_updates_manifest(
    tmp_path: Path, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    source = tmp_path / "source"
    source.mkdir()
    git(source, "init")
    git(source, "config", "user.email", "test@example.test")
    git(source, "config", "user.name", "Test")
    relative = Path("ideas/portfolio.inbox/idea.md")
    (source / relative).parent.mkdir(parents=True)
    payload = b"canonical target vision\n"
    (source / relative).write_bytes(payload)
    git(source, "add", ".")
    git(source, "commit", "-m", "source")
    commit = git(source, "rev-parse", "HEAD")

    root = tmp_path / "destination"
    manifest_path = root / "docs/source-material/source-manifest.yaml"
    manifest_path.parent.mkdir(parents=True)
    manifest = {
        "schema_version": "my-data-hub-source-manifest-v1",
        "sources": [
            {
                "source_repository": "example/source",
                "source_path": relative.as_posix(),
                "source_commit": commit,
                "destination_path": "docs/source-material/exact/idea.md",
                "sha256": None,
                "status": "pending_authenticated_import",
                "role": "canonical_target_vision",
            }
        ],
    }
    manifest_path.write_text(yaml.safe_dump(manifest, sort_keys=False), encoding="utf-8")
    monkeypatch.setattr(importer, "ROOT", root)
    monkeypatch.setattr(importer, "MANIFEST_PATH", manifest_path)

    result = importer.import_entry(
        source_repo=source,
        source_repository="example/source",
    )
    destination = root / result["destination_path"]
    assert destination.read_bytes() == payload
    assert result["sha256"] == hashlib.sha256(payload).hexdigest()
    updated = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
    entry = updated["sources"][0]
    assert entry["status"] == "verified_import"
    assert entry["source_commit"] == commit
