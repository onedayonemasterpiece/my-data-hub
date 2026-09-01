from __future__ import annotations

import base64
import hashlib
import json
import re
import unicodedata
import urllib.parse
import urllib.request
from collections.abc import Iterable
from pathlib import PurePosixPath
from typing import Any, Protocol

import yaml

from .models import MANIFEST_SCHEMA, DatasetSelector


class ResolutionError(ValueError):
    """A required immutable remote input is missing, invalid, or ambiguous."""


class RemoteRepositoryReader(Protocol):
    def default_branch_sha(self) -> str: ...
    def list_paths(self, sha: str) -> list[str]: ...
    def read_text(self, sha: str, path: str) -> str: ...


def _hash(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _normal(value: str) -> str:
    return " ".join(unicodedata.normalize("NFKC", value).split()).casefold()


def _path(value: str) -> str:
    candidate = PurePosixPath(value)
    if not value or candidate.is_absolute() or ".." in candidate.parts or "\\" in value:
        raise ResolutionError("unsafe remote path")
    return candidate.as_posix()


class GitHubGitObjectReader:
    """Credential-free immutable GitHub object reader; it never reads a checkout."""

    def __init__(self, repository: str, api_base: str = "https://api.github.com") -> None:
        if re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository) is None:
            raise ValueError("repository must be owner/name")
        self.repository = repository
        self.api_base = api_base.rstrip("/")
        self._trees: dict[str, dict[str, str]] = {}

    def _get(self, path: str) -> Any:
        request = urllib.request.Request(
            f"{self.api_base}/repos/{self.repository}{path}",
            headers={"Accept": "application/vnd.github+json", "User-Agent": "my-data-hub-stage1a"},
        )
        try:
            with urllib.request.urlopen(request, timeout=20) as response:
                raw = response.read(4_000_000)
            return json.loads(raw)
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise ResolutionError("GitHub object read failed") from exc

    def default_branch_sha(self) -> str:
        repository = self._get("")
        branch = repository.get("default_branch") if isinstance(repository, dict) else None
        ref = self._get(f"/git/ref/heads/{urllib.parse.quote(branch, safe='')}") if isinstance(branch, str) else None
        sha = ref.get("object", {}).get("sha") if isinstance(ref, dict) else None
        if not isinstance(sha, str) or re.fullmatch(r"[0-9a-f]{40,64}", sha) is None:
            raise ResolutionError("remote default branch SHA unavailable")
        return sha

    def _tree(self, sha: str) -> dict[str, str]:
        if sha not in self._trees:
            response = self._get(f"/git/trees/{sha}?recursive=1")
            entries = response.get("tree") if isinstance(response, dict) else None
            if not isinstance(entries, list) or response.get("truncated"):
                raise ResolutionError("remote tree unavailable or truncated")
            tree: dict[str, str] = {}
            for item in entries:
                if not isinstance(item, dict) or item.get("type") != "blob":
                    continue
                if not isinstance(item.get("path"), str) or not isinstance(item.get("sha"), str):
                    continue
                tree[_path(item["path"])] = item["sha"]
            self._trees[sha] = tree
        return self._trees[sha]

    def list_paths(self, sha: str) -> list[str]:
        return sorted(self._tree(sha))

    def read_text(self, sha: str, path: str) -> str:
        blob = self._tree(sha).get(_path(path))
        if not blob:
            raise ResolutionError(f"remote path is missing: {path}")
        response = self._get(f"/git/blobs/{blob}")
        valid = isinstance(response, dict) and response.get("encoding") == "base64"
        if not valid or not isinstance(response.get("content"), str):
            raise ResolutionError("remote blob is invalid")
        try:
            return base64.b64decode(response["content"], validate=True).decode()
        except (ValueError, UnicodeError) as exc:
            raise ResolutionError("remote blob is not UTF-8") from exc


class DatasetResolver:
    def __init__(self, reader: RemoteRepositoryReader) -> None:
        self.reader = reader

    @staticmethod
    def _refs(document: dict[str, Any]) -> dict[str, str]:
        values = {
            "record_schema": document.get("record_schema") or document.get("schema"),
            "pipeline": document.get("pipeline"),
            "current_state": document.get("current_state") or document.get("state"),
        }
        if any(not isinstance(value, str) for value in values.values()):
            raise ResolutionError("dataset.yaml missing references")
        return {name: _path(value) for name, value in values.items() if isinstance(value, str)}

    def resolve(self, selectors: Iterable[DatasetSelector]) -> dict[str, Any]:
        sha = self.reader.default_branch_sha()
        entries: list[tuple[str, dict[str, Any], str]] = []
        for path in (item for item in self.reader.list_paths(sha) if item.endswith("dataset.yaml")):
            raw = self.reader.read_text(sha, path)
            try:
                document = yaml.safe_load(raw)
            except yaml.YAMLError as exc:
                raise ResolutionError("invalid dataset.yaml") from exc
            if not isinstance(document, dict) or not isinstance(document.get("title"), str):
                continue
            dataset_id = document.get("dataset_id", document.get("id"))
            if "dataset_id" in document and "id" in document and document["dataset_id"] != document["id"]:
                raise ResolutionError("dataset.yaml has conflicting dataset_id and id")
            if isinstance(dataset_id, str):
                entries.append((path, {**document, "dataset_id": dataset_id}, raw))
        frozen: list[dict[str, Any]] = []
        for selector in selectors:
            if not any((selector.dataset_id, selector.title, selector.path)):
                raise ResolutionError("dataset selector is empty")
            candidates = entries
            if selector.dataset_id is not None:
                candidates = [entry for entry in candidates if entry[1]["dataset_id"] == selector.dataset_id]
            if selector.title is not None:
                candidates = [entry for entry in candidates if _normal(entry[1]["title"]) == _normal(selector.title)]
            if selector.path is not None:
                candidates = [entry for entry in candidates if entry[0] == _path(selector.path)]
            if not candidates:
                raise ResolutionError("requested dataset is missing")
            if len(candidates) != 1:
                raise ResolutionError("requested dataset is ambiguous")
            path, document, raw = candidates[0]
            refs = {
                name: {"path": ref, "sha256": _hash(self.reader.read_text(sha, ref))}
                for name, ref in self._refs(document).items()
            }
            dataset_id = document["dataset_id"]
            frozen.append(
                {
                    "id": dataset_id,
                    "title": document["title"],
                    "path": path,
                    "dataset_yaml_sha256": _hash(raw),
                    "references": refs,
                    "contract_sha256": _hash(_json({"id": dataset_id, "title": document["title"], "references": refs})),
                }
            )
        if len({item["id"] for item in frozen}) != len(frozen):
            raise ResolutionError("dataset selection contains a duplicate")
        manifest = {
            "schema": MANIFEST_SCHEMA,
            "source_sha": sha,
            "datasets": sorted(frozen, key=lambda item: item["id"]),
        }
        manifest["manifest_sha256"] = _hash(_json(manifest))
        return manifest
