from pathlib import Path

import pytest

from my_data_hub.showcase.source import (
    FilesystemShowcaseSource,
    GitSshShowcaseSource,
    GitSshShowcaseWriter,
    ShowcaseSourceError,
)

FIXTURES = Path(__file__).parent / "fixtures"


def test_filesystem_source_builds_exact_public_bundle() -> None:
    bundle = FilesystemShowcaseSource(FIXTURES).load_bundle("main")
    published = bundle.published()
    assert bundle.view.id == "main"
    assert len(bundle.items) == 5
    assert len(bundle.source_revision) == 64
    assert published["view"]["title"] == "Что уже умеем и что можем сделать"
    assert published["items"][0]["id"] == "voice-cloning-audioguides"
    assert "visibility" not in published["items"][0]
    assert "publish_state" not in published["items"][0]
    assert "source_ref" not in str(published)


def test_source_fails_closed_for_missing_view() -> None:
    with pytest.raises(ShowcaseSourceError):
        FilesystemShowcaseSource(FIXTURES).load_bundle("missing-view")


def test_git_ssh_source_rejects_non_private_key_permissions(tmp_path: Path) -> None:
    key = tmp_path / "deploy-key"
    key.write_text("not-a-real-key", encoding="utf-8")
    key.chmod(0o644)
    known_hosts = tmp_path / "known-hosts"
    known_hosts.write_text("github.com ssh-ed25519 placeholder\n", encoding="utf-8")
    with pytest.raises(ValueError, match="mode 0600"):
        GitSshShowcaseSource(key_file=key, known_hosts_file=known_hosts)


def test_git_ssh_source_uses_sparse_blobless_checkout() -> None:
    source = (Path(__file__).resolve().parents[2] / "src/my_data_hub/showcase/source.py").read_text(encoding="utf-8")
    assert '["git", "sparse-checkout", "set", "--no-cone", f"/{self.root}/"]' in source
    assert '"--filter=blob:none"' in source


def test_capability_type_is_backward_compatible_and_serialized() -> None:
    bundle = FilesystemShowcaseSource(FIXTURES).load_bundle("main")
    assert all(item.capability_type is None for item in bundle.items)
    assert bundle.published()["items"][0]["capability_type"] is None




def test_git_ssh_writer_is_repo_ref_root_bounded(tmp_path: Path) -> None:
    key = tmp_path / "writer"
    key.write_text("key", encoding="utf-8")
    key.chmod(0o600)
    known = tmp_path / "known"
    known.write_text("github.com", encoding="utf-8")
    with pytest.raises(ValueError, match="limited"):
        GitSshShowcaseWriter(key_file=key, known_hosts_file=known, repository="other/repo")


def test_git_ssh_writer_rejects_traversal_and_keeps_lease_contract(tmp_path: Path) -> None:
    key = tmp_path / "writer"
    key.write_text("key", encoding="utf-8")
    key.chmod(0o600)
    known = tmp_path / "known"
    known.write_text("github.com", encoding="utf-8")
    writer = GitSshShowcaseWriter(key_file=key, known_hosts_file=known)
    with pytest.raises(ShowcaseSourceError, match="unsafe"):
        writer._safe_files({"views/../secret.yaml": "x"})
    source = (Path(__file__).resolve().parents[2] / "src/my_data_hub/showcase/source.py").read_text(encoding="utf-8")
    assert "--force-with-lease=refs/heads/{self.ref}:{expected_revision}" in source
    assert "StrictHostKeyChecking=yes" in source
