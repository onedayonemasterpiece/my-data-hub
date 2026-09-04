from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import shlex
import stat
import subprocess
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

import yaml

from .models import _ID_PATTERN, ShowcaseBundle, ShowcaseItem, ShowcaseView
from .requests import ShowcaseRequestError, ShowcaseSourceError


class ShowcaseSourceNotFoundError(ShowcaseSourceError):
    """Raised only when the requested bounded Showcase source object is absent."""


class ShowcaseSource(Protocol):
    def load_bundle(self, view_id: str) -> ShowcaseBundle: ...

    def get_source(self, view_id: str) -> ShowcaseBundle: ...


def _parse_yaml(raw: str, *, label: str) -> dict[str, object]:
    try:
        value = yaml.safe_load(raw)
    except yaml.YAMLError as exc:
        raise ShowcaseSourceError(f"invalid YAML in {label}: {exc}") from exc
    if not isinstance(value, dict):
        raise ShowcaseSourceError(f"{label} must contain a YAML object")
    return value


class ShowcaseSnapshot:
    """One immutable revision; lazily read only requested curated records."""

    def __init__(self, revision: str, read: Callable[[str], str], views: Callable[[], list[str]]) -> None:
        self.revision = revision
        self._read_impl = read
        self._views = views
        self._cache: dict[str, str] = {}
        self._users: dict[str, list[str]] | None = None

    def read(self, path: str) -> str:
        if path not in self._cache:
            self._cache[path] = self._read_impl(path)
        return self._cache[path]

    def view_exists(self, view_id: str) -> bool:
        self._id(view_id)
        try:
            self.read(f"views/{view_id}.yaml")
        except ShowcaseSourceNotFoundError:
            return False
        return True

    @staticmethod
    def _id(value: str) -> None:
        if not re.fullmatch(_ID_PATTERN, value):
            raise ShowcaseRequestError("INVALID_FIELD", "view_id")

    def get_item(self, item_id: str) -> ShowcaseItem:
        self._id(item_id)
        path = f"items/{item_id}.yaml"
        item = ShowcaseItem.model_validate(_parse_yaml(self.read(path), label=path))
        if item.id != item_id:
            raise ShowcaseRequestError("INVALID_FIELD", "items.id")
        return item

    def get_source(self, view_id: str, *, allow_drafts: bool = True) -> ShowcaseBundle:
        self._id(view_id)
        path = f"views/{view_id}.yaml"
        view = ShowcaseView.model_validate(_parse_yaml(self.read(path), label=path))
        if view.id != view_id:
            raise ShowcaseRequestError("VIEW_ID_MISMATCH", "view.id")
        items = []
        for index, item_id in enumerate(view.item_ids):
            try:
                items.append(self.get_item(item_id))
            except ShowcaseSourceNotFoundError as exc:
                raise ShowcaseRequestError("ITEM_NOT_FOUND", f"view.item_ids[{index}]") from exc
        return ShowcaseBundle.model_validate(
            {"source_revision": self.revision, "view": view, "items": items},
            context={"allow_drafts": allow_drafts},
        )

    def users(self, item_id: str) -> list[str]:
        if self._users is None:
            self._users = {}
            for path in self._views():
                doc = _parse_yaml(self.read(path), label=path)
                for ref in doc.get("item_ids", []):
                    self._users.setdefault(ref, []).append(Path(path).stem)
        return self._users.get(item_id, [])


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
        except FileNotFoundError as exc:
            raise ShowcaseSourceNotFoundError(f"showcase source is absent: {relative}") from exc
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

    def get_source(self, view_id: str, *, revision: str | None = None) -> ShowcaseBundle:
        with self.snapshot(revision) as snapshot:
            return snapshot.get_source(view_id, allow_drafts=True)

    def load_bundle(self, view_id: str, *, revision: str | None = None) -> ShowcaseBundle:
        with self.snapshot(revision) as snapshot:
            return snapshot.get_source(view_id, allow_drafts=False)

    @contextmanager
    def snapshot(self, at_revision: str | None = None) -> Iterator[ShowcaseSnapshot]:
        files: dict[str, str] = {}
        for directory in ("views", "items"):
            for path in sorted((self.root / directory).glob("*.yaml")):
                if path.is_symlink():
                    raise ShowcaseSourceError("unsafe Showcase symlink")
                relative = f"{directory}/{path.name}"
                files[relative] = self._read(relative)
        digest = hashlib.sha256()
        for relative, raw in sorted(files.items()):
            digest.update(relative.encode() + b"\0" + raw.encode() + b"\n")
        revision = digest.hexdigest()
        if at_revision is not None and revision != at_revision:
            raise ShowcaseRequestError("REVISION_CONFLICT")

        def read(relative: str) -> str:
            try:
                return files[relative]
            except KeyError as exc:
                raise ShowcaseSourceNotFoundError("showcase source is absent") from exc

        yield ShowcaseSnapshot(revision, read, lambda: [p for p in files if p.startswith("views/")])


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
            if exc.code == 404:
                raise ShowcaseSourceNotFoundError("showcase source is absent") from exc
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

    def load_bundle(self, view_id: str, *, revision: str | None = None) -> ShowcaseBundle:
        with self.snapshot(revision) as snapshot:
            return snapshot.get_source(view_id, allow_drafts=False)

    def get_source(self, view_id: str, *, revision: str | None = None) -> ShowcaseBundle:
        with self.snapshot(revision) as snapshot:
            return snapshot.get_source(view_id, allow_drafts=True)

    @contextmanager
    def snapshot(self, at_revision: str | None = None) -> Iterator[ShowcaseSnapshot]:
        revision = at_revision or self._revision()
        if not re.fullmatch(r"[0-9a-f]{40}", revision):
            raise ShowcaseRequestError("REVISION_CONFLICT")

        def views() -> list[str]:
            commit = self._request_json(f"https://api.github.com/repos/{self.repository}/git/commits/{revision}")
            tree_sha = commit["tree"]["sha"]
            tree = self._request_json(
                f"https://api.github.com/repos/{self.repository}/git/trees/{tree_sha}?recursive=1"
            )
            if tree.get("truncated"):
                raise ShowcaseSourceError("cannot verify shared cards from a truncated tree")
            prefix = f"{self.root}/views/"
            return [
                entry["path"][len(self.root) + 1 :]
                for entry in tree["tree"]
                if entry.get("type") == "blob" and entry["path"].startswith(prefix) and entry["path"].endswith(".yaml")
            ]

        yield ShowcaseSnapshot(revision, lambda path: self._read_at_revision(path, revision), views)


class GitHubShowcaseWriter:
    """Bounded GitHub Git-Data writer for only showcase views and items."""

    def __init__(
        self, *, token: str, repository: str, ref: str = "main", root: str = "showcase", timeout_seconds: int = 20
    ) -> None:
        if not token:
            raise ShowcaseSourceError("Showcase source write credential is unavailable")
        self.token, self.repository, self.ref, self.root, self.timeout_seconds = (
            token,
            repository,
            ref,
            root.strip("/"),
            timeout_seconds,
        )

    def _request(self, url: str, *, method: str = "GET", payload: dict[str, object] | None = None) -> dict[str, object]:
        body = None if payload is None else json.dumps(payload).encode("utf-8")
        request = Request(
            url,
            data=body,
            method=method,
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json",
                "User-Agent": "my-data-hub-ideahub-showcase/1",
                "X-GitHub-Api-Version": "2022-11-28",
            },
        )
        try:
            with urlopen(request, timeout=self.timeout_seconds) as response:
                result = json.load(response)
        except HTTPError as exc:
            raise ShowcaseSourceError(f"GitHub source write returned HTTP {exc.code}") from exc
        except (URLError, TimeoutError, OSError, json.JSONDecodeError) as exc:
            raise ShowcaseSourceError("GitHub source write request failed") from exc
        if not isinstance(result, dict):
            raise ShowcaseSourceError("GitHub source write returned an unexpected response")
        return result

    def apply(self, *, expected_revision: str, files: dict[str, str], message: str) -> str:
        base = f"https://api.github.com/repos/{self.repository}/git"
        refdoc = self._request(f"{base}/ref/heads/{quote(self.ref, safe='')}")
        current = str(refdoc.get("object", {}).get("sha", "")) if isinstance(refdoc.get("object"), dict) else ""
        if current != expected_revision:
            raise ShowcaseRequestError("REVISION_CONFLICT")
        commit = self._request(f"{base}/commits/{current}")
        tree = str(commit.get("tree", {}).get("sha", "")) if isinstance(commit.get("tree"), dict) else ""
        entries = []
        for relative, content in sorted(files.items()):
            if (
                not relative.startswith(("views/", "items/"))
                or "/" in relative.split("/", 1)[1]
                or not relative.endswith(".yaml")
            ):
                raise ShowcaseSourceError("unsafe showcase source path")
            entries.append({"path": f"{self.root}/{relative}", "mode": "100644", "type": "blob", "content": content})
        newtree = self._request(f"{base}/trees", method="POST", payload={"base_tree": tree, "tree": entries})
        tree_sha = str(newtree.get("sha", ""))
        newcommit = self._request(
            f"{base}/commits", method="POST", payload={"message": message, "tree": tree_sha, "parents": [current]}
        )
        new_sha = str(newcommit.get("sha", ""))
        # CAS update; GitHub rejects a non-fast-forward update.
        self._request(
            f"{base}/refs/heads/{quote(self.ref, safe='')}", method="PATCH", payload={"sha": new_sha, "force": False}
        )
        return new_sha

    def create(self, *, view_id: str, files: dict[str, str], message: str, expected_revision: str | None = None) -> str:
        """Create a bounded view only if its view path is absent at the CAS base."""
        base = f"https://api.github.com/repos/{self.repository}/git"
        refdoc = self._request(f"{base}/ref/heads/{quote(self.ref, safe='')}")
        current = str(refdoc.get("object", {}).get("sha", "")) if isinstance(refdoc.get("object"), dict) else ""
        if len(current) != 40:
            raise ShowcaseSourceError("GitHub did not return an exact source revision")
        if expected_revision is not None and current != expected_revision:
            raise ShowcaseRequestError("REVISION_CONFLICT")
        commit = self._request(f"{base}/commits/{current}")
        tree = str(commit.get("tree", {}).get("sha", "")) if isinstance(commit.get("tree"), dict) else ""
        listing = self._request(f"{base}/trees/{quote(tree, safe='')}?recursive=1")
        paths = (
            {entry.get("path") for entry in listing.get("tree", []) if isinstance(entry, dict)}
            if isinstance(listing.get("tree"), list)
            else set()
        )
        target = f"{self.root}/views/{view_id}.yaml"
        if listing.get("truncated"):
            raise ShowcaseSourceError("cannot verify create paths from a truncated tree")
        if target in paths:
            raise ShowcaseRequestError("VIEW_EXISTS")
        if any(f"{self.root}/{path}" in paths for path in files if path.startswith("items/")):
            raise ShowcaseRequestError("ITEM_ID_CONFLICT", "items")
        return self.apply(expected_revision=current, files=files, message=message)


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

    def get_source(self, view_id: str, *, revision: str | None = None) -> ShowcaseBundle:
        with self.snapshot(revision) as snapshot:
            return snapshot.get_source(view_id, allow_drafts=True)

    @contextmanager
    def snapshot(self, at_revision: str | None = None) -> Iterator[ShowcaseSnapshot]:
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
                ["git", "fetch", "--quiet", "--filter=blob:none", "--depth=1", "origin", at_revision or self.ref],
                cwd=checkout,
                env=env,
            )
            revision = self._run(["git", "rev-parse", "FETCH_HEAD"], cwd=checkout, env=env)
            if len(revision) != 40 or any(char not in "0123456789abcdef" for char in revision):
                raise ShowcaseSourceError("Git SSH source did not resolve an exact commit SHA")
            self._run(["git", "checkout", "--quiet", "--detach", revision], cwd=checkout, env=env)
            with FilesystemShowcaseSource(checkout / self.root).snapshot() as snapshot:
                snapshot.revision = revision
                yield snapshot

    def load_bundle(self, view_id: str, *, revision: str | None = None) -> ShowcaseBundle:
        with self.snapshot(revision) as snapshot:
            return snapshot.get_source(view_id, allow_drafts=False)


class GitSshShowcaseWriter:
    """Repo/ref/root-bounded Showcase writer using a dedicated SSH deploy key."""

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
        if repository != "onedayonemasterpiece/idea-hub" or ref != "main" or root.strip("/") != "showcase":
            raise ValueError("Git SSH Showcase writer is limited to the configured idea-hub main showcase root")
        self._source = GitSshShowcaseSource(
            key_file=key_file,
            known_hosts_file=known_hosts_file,
            repository=repository,
            ref=ref,
            root=root,
            timeout_seconds=timeout_seconds,
        )
        self.key_file = self._source.key_file
        self.known_hosts_file = self._source.known_hosts_file
        self.repository = repository
        self.ref = ref
        self.root = "showcase"
        self.timeout_seconds = timeout_seconds

    def _env(self) -> dict[str, str]:
        return {
            **os.environ,
            "GIT_SSH_COMMAND": shlex.join(
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
            ),
        }

    def _run(self, argv: list[str], *, cwd: Path) -> str:
        try:
            result = subprocess.run(
                argv,
                cwd=cwd,
                env=self._env(),
                text=True,
                capture_output=True,
                timeout=self.timeout_seconds,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise ShowcaseSourceError("Git SSH Showcase write command failed") from exc
        if result.returncode:
            raise ShowcaseSourceError("Git SSH Showcase write conflict or command failure")
        return result.stdout.strip()

    @staticmethod
    def _safe_files(files: dict[str, str]) -> dict[str, str]:
        if not files:
            raise ShowcaseSourceError("Showcase write requires files")
        safe: dict[str, str] = {}
        for relative, content in files.items():
            path = Path(relative)
            if (
                path.is_absolute()
                or ".." in path.parts
                or len(path.parts) != 2
                or path.parts[0] not in {"views", "items"}
                or not path.name.endswith(".yaml")
                or not path.stem
                or not isinstance(content, str)
            ):
                raise ShowcaseSourceError("unsafe showcase source path")
            safe[relative] = content
        return safe

    def _checkout(self, checkout: Path, expected_revision: str) -> None:
        self._run(["git", "init", "--quiet"], cwd=checkout)
        self._run(["git", "remote", "add", "origin", f"git@github.com:{self.repository}.git"], cwd=checkout)
        self._run(["git", "fetch", "--quiet", "--depth=1", "origin", self.ref], cwd=checkout)
        head = self._run(["git", "rev-parse", "FETCH_HEAD"], cwd=checkout)
        if head != expected_revision:
            raise ShowcaseRequestError("REVISION_CONFLICT")
        self._run(["git", "checkout", "--quiet", "--detach", head], cwd=checkout)

    def _commit(
        self, *, expected_revision: str, files: dict[str, str], message: str, create_view_id: str | None
    ) -> str:
        if len(expected_revision) != 40 or any(c not in "0123456789abcdef" for c in expected_revision):
            raise ShowcaseRequestError("REVISION_CONFLICT")
        safe = self._safe_files(files)
        with TemporaryDirectory(prefix="showcase-git-writer-") as temp:
            checkout = Path(temp)
            self._checkout(checkout, expected_revision)
            root = (checkout / self.root).resolve()
            if not root.is_dir() or root.is_symlink():
                raise ShowcaseSourceError("configured Showcase root is unavailable")
            if create_view_id is not None:
                target = root / "views" / f"{create_view_id}.yaml"
                if target.exists() or target.is_symlink():
                    raise ShowcaseRequestError("VIEW_EXISTS")
                if any((root / p).exists() or (root / p).is_symlink() for p in safe if p.startswith("items/")):
                    raise ShowcaseRequestError("ITEM_ID_CONFLICT", "items")
            for relative, content in sorted(safe.items()):
                target = (root / relative).resolve()
                if target.parent != (root / Path(relative).parent).resolve() or not target.is_relative_to(root):
                    raise ShowcaseSourceError("unsafe showcase source path")
                if target.exists() and target.is_symlink():
                    raise ShowcaseSourceError("unsafe showcase source path")
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text(content, encoding="utf-8", newline="\n")
            self._run(["git", "add", "--", *sorted(str(Path(self.root) / p) for p in safe)], cwd=checkout)
            self._run(
                [
                    "git",
                    "-c",
                    "user.name=my-data-hub Showcase",
                    "-c",
                    "user.email=showcase@localhost",
                    "commit",
                    "--quiet",
                    "-m",
                    message,
                ],
                cwd=checkout,
            )
            revision = self._run(["git", "rev-parse", "HEAD"], cwd=checkout)
            self._run(
                [
                    "git",
                    "push",
                    "--quiet",
                    "origin",
                    f"HEAD:refs/heads/{self.ref}",
                    f"--force-with-lease=refs/heads/{self.ref}:{expected_revision}",
                ],
                cwd=checkout,
            )
            return revision

    def apply(self, *, expected_revision: str, files: dict[str, str], message: str) -> str:
        return self._commit(expected_revision=expected_revision, files=files, message=message, create_view_id=None)

    def create(self, *, view_id: str, files: dict[str, str], message: str, expected_revision: str | None = None) -> str:
        if not view_id or "/" in view_id or view_id in {".", ".."}:
            raise ShowcaseSourceError("unsafe showcase view id")
        # The fetched branch head is the create CAS base; path absence is checked there.
        with TemporaryDirectory(prefix="showcase-git-create-head-") as temp:
            checkout = Path(temp)
            self._run(["git", "init", "--quiet"], cwd=checkout)
            self._run(["git", "remote", "add", "origin", f"git@github.com:{self.repository}.git"], cwd=checkout)
            self._run(["git", "fetch", "--quiet", "--depth=1", "origin", self.ref], cwd=checkout)
            head = self._run(["git", "rev-parse", "FETCH_HEAD"], cwd=checkout)
        if expected_revision is not None and head != expected_revision:
            raise ShowcaseRequestError("REVISION_CONFLICT")
        return self._commit(expected_revision=head, files=files, message=message, create_view_id=view_id)
