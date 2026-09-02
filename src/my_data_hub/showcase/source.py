from __future__ import annotations

import base64
import hashlib
import json
from pathlib import Path
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
            with urlopen(request, timeout=self.timeout_seconds) as response:  # noqa: S310 - fixed GitHub host
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
        view = ShowcaseView.model_validate(
            _parse_yaml(self._read_at_revision(view_path, revision), label=view_path)
        )
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
