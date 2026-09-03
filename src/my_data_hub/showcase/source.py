from __future__ import annotations

import base64
import hashlib
import json
import os
import shlex
import stat
import subprocess
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

import yaml

from .models import ShowcaseBundle, ShowcaseItem, ShowcaseView


class ShowcaseSourceError(RuntimeError):
    """Raised when a showcase source cannot produce one consistent snapshot."""


class ShowcaseSource(Protocol):
    def load_bundle(self, view_id: str) -> ShowcaseBundle: ...


def _parse_yaml(raw: str, *, label: str) -> dict[str, object]:
    try:
        value = yaml.safe_load(raw)
    except yaml.YAMLError as exc:
        raise ShowcaseSourceError(f"invalid YAML in {label}: {exc}") from exc
    if not isinstance(value, dict):
        raise ShowcaseSourceError(f"{label} must contain a YAML object")
    return value


class FilesystemShowcaseSource:
    def __init__(self, root: Path) -> None:
        self.root = root.expanduser().resolve()

    def _read(self, relative: str) -> str:
        path = (self.root / relative).resolve()
        try:
            path.relative_to(self.root)
        except ValueError as exc:
            raise ShowcaseSourceError("source path escaped the configured root") from exc
        try:
            return path.read_text(encoding="utf-8")
        except OSError as exc:
            raise ShowcaseSourceError(f"cannot read {relative}: {exc}") from exc

    def _revision(self, paths: list[str]) -> str:
        digest = hashlib.sha256()
        for relative in sorted(paths):
            digest.update(relative.encode())
            digest.update(b"\0")
            digest.update(self._read(relative).encode())
            digest.update(b"\n")
        return digest.hexdigest()

    def load_bundle(self, view_id: str) -> ShowcaseBundle:
        view_path = f"views/{view_id}.yaml"
        view = ShowcaseView.model_validate(_parse_yaml(self._read(view_path), label=view_path))
        if view.id != view_id:
            raise ShowcaseSourceError(f"view id mismatch: requested {view_id}, found {view.id}")
        items: list[ShowcaseItem] = []
        paths = [view_path]
        for item_id in view.item_ids:
            path = f"items/{item_id}.yaml"
            item = ShowcaseItem.model_validate(_parse_yaml(self._read(path), label=path))
            if item.id != item_id:
                raise ShowcaseSourceError(f"item id mismatch: requested {item_id}, found {item.id}")
            items.append(item)
            paths.append(path)
        return ShowcaseBundle(source_revision=self._revision(paths), view=view, items=items)


class GitHubShowcaseSource:
    def __init__(
        self,
        *,
        token: str,
        repository: str = "onedayonemasterpiece/idea-hub",
        ref: str = "main",
        root: str = "showcase",
        timeout_seconds: int = 20,
    ) -> None:
        if "/" not in repository:
            raise ValueError("repository must be owner/name")
        if not token:
            raise ValueError("GitHub token is required")
        self.token = token
        self.repository = repository
        self.ref = ref
        self.root = root.strip("/")
        self.timeout_seconds = timeout_seconds

    def _request_json(self, url: str) -> dict[str, object]:
        request = Request(
            url,
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {self.token}",
                "User-Agent": "my-data-hub-ideahub-showcase/1",
                "X-GitHub-Api-Version": "2022-11-28",
            },
        )
        try:
            with urlopen(request, timeout=self.timeout_seconds) as response:
                payload = json.load(response)
        except HTTPError as exc:
            raise ShowcaseSourceError(f"GitHub returned HTTP {exc.code}") from exc
        except (URLError, TimeoutError, OSError, json.JSONDecodeError) as exc:
            raise ShowcaseSourceError(f"GitHub source request failed: {exc}") from exc
        if not isinstance(payload, dict):
            raise ShowcaseSourceError("GitHub returned an unexpected response")
        return payload

    def _revision(self) -> str:
        url = f"https://api.github.com/repos/{self.repository}/commits/{quote(self.ref, safe='')}"
        payload = self._request_json(url)
        sha = payload.get("sha")
        if not isinstance(sha, str) or len(sha) != 40:
            raise ShowcaseSourceError("GitHub did not return an exact commit SHA")
        return sha

    def _read_at_revision(self, relative: str, revision: str) -> str:
        path = "/".join(part for part in (self.root, relative.strip("/")) if part)
        url = (
            f"https://api.github.com/repos/{self.repository}/contents/"
            f"{quote(path, safe='/')}?ref={quote(revision, safe='')}"
        )
        payload = self._request_json(url)
        if payload.get("encoding") != "base64" or not isinstance(payload.get("content"), str):
            raise ShowcaseSourceError(f"GitHub content response is invalid for {relative}")
        try:
            return base64.b64decode(str(payload["content"]), validate=False).decode("utf-8")
        except (ValueError, UnicodeDecodeError) as exc:
            raise ShowcaseSourceError(f"GitHub file is not UTF-8 text: {relative}") from exc

    def load_bundle(self, view_id: str) -> ShowcaseBundle:
        revision = self._revision()
        view_path = f"views/{view_id}.yaml"
        view = ShowcaseView.model_validate(_parse_yaml(self._read_at_revision(view_path, revision), label=view_path))
        if view.id != view_id:
            raise ShowcaseSourceError(f"view id mismatch: requested {view_id}, found {view.id}")
        items: list[ShowcaseItem] = []
        for item_id in view.item_ids:
            item_path = f"items/{item_id}.yaml"
            item = ShowcaseItem.model_validate(
                _parse_yaml(self._read_at_revision(item_path, revision), label=item_path)
            )
            if item.id != item_id:
                raise ShowcaseSourceError(f"item id mismatch: requested {item_id}, found {item.id}")
            items.append(item)
        return ShowcaseBundle(source_revision=revision, view=view, items=items)


class GitSshShowcaseSource:
    """Read an exact private-repository revision through a read-only deploy key."""

    def __init__(
        self,
        *,
        key_file: Path,
        known_hosts_file: Path,
        repository: str = "onedayonemasterpiece/idea-hub",
        ref: str = "main",
        root: str = "showcase",
        timeout_seconds: int = 60,
    ) -> None:
        if "/" not in repository or any(char.isspace() for char in repository):
            raise ValueError("repository must be owner/name")
        self.key_file = key_file.expanduser().resolve()
        self.known_hosts_file = known_hosts_file.expanduser().resolve()
        for path, private in ((self.key_file, True), (self.known_hosts_file, False)):
            try:
                file_stat = path.stat()
            except OSError as exc:
                raise ValueError(f"Git SSH credential file is unavailable: {path.name}") from exc
            if not stat.S_ISREG(file_stat.st_mode):
                raise ValueError(f"Git SSH credential path must be a regular file: {path.name}")
            if private and stat.S_IMODE(file_stat.st_mode) & 0o077:
                raise ValueError("Git SSH private key must have mode 0600")
        self.repository = repository
        self.ref = ref
        self.root = root.strip("/")
        self.timeout_seconds = timeout_seconds

    def _run(self, argv: list[str], *, cwd: Path, env: dict[str, str]) -> str:
        try:
            result = subprocess.run(
                argv,
                cwd=cwd,
                env=env,
                text=True,
                capture_output=True,
                timeout=self.timeout_seconds,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise ShowcaseSourceError("Git SSH source command failed") from exc
        if result.returncode != 0:
            raise ShowcaseSourceError(f"Git SSH source command failed: {argv[0]} {argv[1]}")
        return result.stdout.strip()

    def load_bundle(self, view_id: str) -> ShowcaseBundle:
        ssh_command = shlex.join(
            [
                "ssh",
                "-i",
                str(self.key_file),
                "-o",
                "BatchMode=yes",
                "-o",
                "IdentitiesOnly=yes",
                "-o",
                "StrictHostKeyChecking=yes",
                "-o",
                f"UserKnownHostsFile={self.known_hosts_file}",
            ]
        )
        env = {**os.environ, "GIT_SSH_COMMAND": ssh_command}
        with TemporaryDirectory(prefix="showcase-git-source-") as temp:
            checkout = Path(temp)
            self._run(["git", "init", "--quiet"], cwd=checkout, env=env)
            self._run(
                ["git", "remote", "add", "origin", f"git@github.com:{self.repository}.git"],
                cwd=checkout,
                env=env,
            )
            # The private repository can contain large unrelated visual assets.
            # Keep the bounded runtime tmpfs focused on the Showcase subtree and
            # fetch other blobs lazily rather than checking out the whole tree.
            self._run(
                ["git", "sparse-checkout", "set", "--no-cone", f"/{self.root}/"],
                cwd=checkout,
                env=env,
            )
            self._run(
                ["git", "fetch", "--quiet", "--filter=blob:none", "--depth=1", "origin", self.ref],
                cwd=checkout,
                env=env,
            )
            revision = self._run(["git", "rev-parse", "FETCH_HEAD"], cwd=checkout, env=env)
            if len(revision) != 40 or any(char not in "0123456789abcdef" for char in revision):
                raise ShowcaseSourceError("Git SSH source did not resolve an exact commit SHA")
            self._run(["git", "checkout", "--quiet", "--detach", revision], cwd=checkout, env=env)
            bundle = FilesystemShowcaseSource(checkout / self.root).load_bundle(view_id)
            return bundle.model_copy(update={"source_revision": revision})
