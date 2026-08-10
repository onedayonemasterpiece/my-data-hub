from __future__ import annotations

import hashlib
import subprocess
from pathlib import Path

import yaml

import scripts.import_source_material as importer

ROOT = Path(__file__).resolve().parents[1]
TARGET_REPOSITORY = "onedayonemasterpiece/idea-hub"
TARGET_PATH = "ideas/portfolio.inbox/idea-20260809-content-platform-current-design.md"
TARGET_COMMIT = "0c3fcf71b2ee8ba8afa49624bef4b779873802f7"
TARGET_SHA256 = "c7efb28231223caa6fd02fcc001a38e0f16bcc3fa4c4cd53e744721b2eac0852"


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


def test_committed_target_vision_matches_verified_provenance() -> None:
    manifest = yaml.safe_load(
        (ROOT / "docs/source-material/source-manifest.yaml").read_text(encoding="utf-8")
    )
    entries = [
        entry
        for entry in manifest["sources"]
        if entry["source_repository"] == TARGET_REPOSITORY
        and entry["source_path"] == TARGET_PATH
    ]
    assert len(entries) == 1
    entry = entries[0]
    assert entry["source_commit"] == TARGET_COMMIT
    assert entry["sha256"] == TARGET_SHA256
    assert entry["status"] == "verified_import"
    assert entry["role"] == "canonical_target_vision"

    imported = ROOT / entry["destination_path"]
    payload = imported.read_bytes()
    assert len(payload) == 65_507
    assert hashlib.sha256(payload).hexdigest() == TARGET_SHA256

    dedicated_region_talk = next(
        item
        for item in manifest["sources"]
        if item["source_repository"] == "onedayonemasterpiece/region-talk"
    )
    assert dedicated_region_talk["status"] == "pending_curated_import"
    assert dedicated_region_talk["source_commit"] is None
    assert dedicated_region_talk["sha256"] is None


def test_alias_and_region_talk_safety_claims_remain_bounded() -> None:
    source_readme = (ROOT / "docs/source-material/README.md").read_text(encoding="utf-8")
    assert "Canonical project name: my-data-hub" in source_readme
    assert "Historical alias (not a separate project): content-platform" in source_readme

    provenance = (ROOT / "docs/migrations/region-talk/source-provenance.md").read_text(
        encoding="utf-8"
    )
    assert "does not prove access to either Region Talk donor repository" in provenance
    assert "Region Talk remains `paused`" in provenance
    assert "Production publication remains disabled" in provenance
