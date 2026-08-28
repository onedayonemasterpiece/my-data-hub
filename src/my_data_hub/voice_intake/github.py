from __future__ import annotations

import base64
import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote

from jsonschema import Draft202012Validator, FormatChecker

from my_data_hub.google_ai.http import (
    AiohttpBoundedJSONRequester,
    BoundedHTTPError,
    BoundedHTTPResponse,
    BoundedJSONRequester,
)

from .contracts import RemoteProgress, SessionCompleteRequest, SummaryPayload
from .errors import GitHubPublicationConflict, VoiceIntakeError
from .markdown import (
    build_registry_entry,
    insert_registry_entry,
    paths_for,
    render_session_detail,
    render_source_packet,
    utc_now,
)
from .settings import VoiceIntakeSettings


@dataclass(frozen=True, slots=True)
class PublicationReceipt:
    source_path: str
    detail_path: str
    commit_sha: str
    github_url: str


class IdeaHubPublisher:
    REGISTRY_PATH = "registry/intake-sessions.yaml"
    REGISTRY_SCHEMA_PATH = "schemas/intake-session.schema.json"

    def __init__(
        self,
        settings: VoiceIntakeSettings,
        *,
        requester: BoundedJSONRequester | None = None,
    ) -> None:
        self._settings = settings
        self._requester = requester or AiohttpBoundedJSONRequester()
        self._origin = f"https://api.github.com/repos/{settings.github_repository}"
        self._headers = {
            "Authorization": f"Bearer {settings.github_token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "my-data-hub-record-idea-hub/1.0",
        }

    async def status(self, session_id: str) -> RemoteProgress:
        source_path, _detail_path = paths_for(session_id)
        head = await self._head()
        source = await self._content_optional(source_path, head[0])
        registry = await self._content_optional(self.REGISTRY_PATH, head[0])
        verified = bool(
            source
            and registry
            and f"packet_id: {session_id}" in source[0]
            and f"session_id: {session_id}" in registry[0]
        )
        return RemoteProgress(
            state="published_verified" if verified else "processing",
            recording_finished=True,
            github_verified=verified,
            github_commit_sha=head[0] if verified else None,
            github_url=(
                f"https://github.com/{self._settings.github_repository}/blob/"
                f"{head[0]}/{source_path}"
                if verified
                else None
            ),
        )

    async def publish(
        self,
        *,
        session_id: str,
        request: SessionCompleteRequest,
        summary: SummaryPayload,
        model: str,
    ) -> PublicationReceipt:
        source_path, detail_path = paths_for(session_id)
        existing = await self.status(session_id)
        if existing.github_verified:
            assert existing.github_commit_sha and existing.github_url
            return PublicationReceipt(
                source_path=source_path,
                detail_path=detail_path,
                commit_sha=existing.github_commit_sha,
                github_url=existing.github_url,
            )

        registered_at = utc_now()
        source = render_source_packet(
            session_id=session_id,
            request=request,
            summary=summary,
            model=model,
            registered_at=registered_at,
        )
        detail = render_session_detail(
            session_id=session_id,
            request=request,
            summary=summary,
            source_path=source_path,
            registered_at=registered_at,
        )
        entry = build_registry_entry(
            session_id=session_id,
            request=request,
            summary=summary,
            source_path=source_path,
            detail_path=detail_path,
            registered_at=registered_at,
        )

        for _attempt in range(4):
            head_sha, tree_sha = await self._head()
            registry_text, _ = await self._content(self.REGISTRY_PATH, head_sha)
            schema_text, _ = await self._content(self.REGISTRY_SCHEMA_PATH, head_sha)
            if f"session_id: {session_id}" in registry_text:
                reconciled = await self.status(session_id)
                if reconciled.github_verified:
                    assert reconciled.github_commit_sha and reconciled.github_url
                    return PublicationReceipt(
                        source_path=source_path,
                        detail_path=detail_path,
                        commit_sha=reconciled.github_commit_sha,
                        github_url=reconciled.github_url,
                    )
                raise GitHubPublicationConflict("registry_source_readback_mismatch")

            updated_registry = insert_registry_entry(
                registry_text, entry=entry, updated_at=registered_at
            )
            self._validate_registry(updated_registry, schema_text)
            source_blob = await self._blob(source)
            detail_blob = await self._blob(detail)
            registry_blob = await self._blob(updated_registry)
            tree = await self._json(
                "POST",
                "/git/trees",
                {
                    "base_tree": tree_sha,
                    "tree": [
                        {"path": source_path, "mode": "100644", "type": "blob", "sha": source_blob},
                        {"path": detail_path, "mode": "100644", "type": "blob", "sha": detail_blob},
                        {
                            "path": self.REGISTRY_PATH,
                            "mode": "100644",
                            "type": "blob",
                            "sha": registry_blob,
                        },
                    ],
                },
            )
            commit = await self._json(
                "POST",
                "/git/commits",
                {
                    "message": f"intake(voice): register {session_id}",
                    "tree": str(tree["sha"]),
                    "parents": [head_sha],
                },
            )
            commit_sha = str(commit["sha"])
            try:
                response = await self._request(
                    "PATCH",
                    f"/git/refs/heads/{quote(self._settings.github_branch, safe='/-._~')}",
                    {"sha": commit_sha, "force": False},
                )
            except VoiceIntakeError as network_error:
                if network_error.code != "github_network_error":
                    raise
                try:
                    reconciled = await self.status(session_id)
                except VoiceIntakeError:
                    reconciled = None
                if reconciled is not None and reconciled.github_verified:
                    assert reconciled.github_commit_sha and reconciled.github_url
                    return PublicationReceipt(
                        source_path=source_path,
                        detail_path=detail_path,
                        commit_sha=reconciled.github_commit_sha,
                        github_url=reconciled.github_url,
                    )
                raise VoiceIntakeError(
                    "github_outcome_ambiguous",
                    retryable=True,
                    status_code=503,
                    reconciliation_required=True,
                ) from network_error
            if response.status in {409, 422}:
                continue
            self._require_success(response, "github_ref_update_failed")
            await self._verify_commit(
                commit_sha=commit_sha,
                source_path=source_path,
                detail_path=detail_path,
                expected_source=source,
                expected_detail=detail,
                session_id=session_id,
            )
            return PublicationReceipt(
                source_path=source_path,
                detail_path=detail_path,
                commit_sha=commit_sha,
                github_url=(
                    f"https://github.com/{self._settings.github_repository}/blob/"
                    f"{commit_sha}/{source_path}"
                ),
            )
        raise GitHubPublicationConflict("idea_hub_main_moved_repeatedly")

    async def _verify_commit(
        self,
        *,
        commit_sha: str,
        source_path: str,
        detail_path: str,
        expected_source: str,
        expected_detail: str,
        session_id: str,
    ) -> None:
        source, _ = await self._content(source_path, commit_sha)
        detail, _ = await self._content(detail_path, commit_sha)
        registry, _ = await self._content(self.REGISTRY_PATH, commit_sha)
        if hashlib.sha256(source.encode()).digest() != hashlib.sha256(expected_source.encode()).digest():
            raise GitHubPublicationConflict("github_source_readback_hash_mismatch")
        if hashlib.sha256(detail.encode()).digest() != hashlib.sha256(expected_detail.encode()).digest():
            raise GitHubPublicationConflict("github_detail_readback_hash_mismatch")
        if f"session_id: {session_id}" not in registry:
            raise GitHubPublicationConflict("github_registry_readback_missing_session")
        current_sha, _ = await self._head()
        current_source, _ = await self._content(source_path, current_sha)
        current_registry, _ = await self._content(self.REGISTRY_PATH, current_sha)
        if f"packet_id: {session_id}" not in current_source or f"session_id: {session_id}" not in current_registry:
            raise GitHubPublicationConflict("github_main_readback_missing_session")

    @staticmethod
    def _validate_registry(registry_text: str, schema_text: str) -> None:
        import yaml

        registry = yaml.safe_load(registry_text)
        schema = json.loads(schema_text)
        errors = sorted(
            Draft202012Validator(schema, format_checker=FormatChecker()).iter_errors(registry),
            key=lambda item: list(item.path),
        )
        if errors:
            first = errors[0]
            location = ".".join(str(part) for part in first.absolute_path) or "<root>"
            raise GitHubPublicationConflict(f"idea_hub_registry_invalid_at_{location}")

    async def _head(self) -> tuple[str, str]:
        ref = await self._json(
            "GET",
            f"/git/ref/heads/{quote(self._settings.github_branch, safe='/-._~')}",
        )
        obj = ref.get("object")
        if not isinstance(obj, Mapping):
            raise VoiceIntakeError("github_ref_invalid", retryable=True, status_code=503)
        head_sha = str(obj.get("sha") or "")
        commit = await self._json("GET", f"/git/commits/{head_sha}")
        tree = commit.get("tree")
        if not isinstance(tree, Mapping):
            raise VoiceIntakeError("github_commit_invalid", retryable=True, status_code=503)
        return head_sha, str(tree.get("sha") or "")

    async def _content(self, path: str, ref: str) -> tuple[str, str]:
        response = await self._request(
            "GET",
            f"/contents/{quote(path, safe='/-._~')}?ref={quote(ref, safe='-._~')}",
            None,
        )
        self._require_success(response, "github_content_read_failed")
        value = response.json_body
        if not isinstance(value, Mapping):
            raise VoiceIntakeError("github_content_invalid", retryable=True, status_code=503)
        encoded = str(value.get("content") or "").replace("\n", "")
        try:
            content = base64.b64decode(encoded, validate=True).decode("utf-8")
        except (ValueError, UnicodeDecodeError) as exc:
            raise VoiceIntakeError("github_content_invalid", retryable=True, status_code=503) from exc
        return content, str(value.get("sha") or "")

    async def _content_optional(self, path: str, ref: str) -> tuple[str, str] | None:
        response = await self._request(
            "GET",
            f"/contents/{quote(path, safe='/-._~')}?ref={quote(ref, safe='-._~')}",
            None,
        )
        if response.status == 404:
            return None
        self._require_success(response, "github_content_read_failed")
        value = response.json_body
        if not isinstance(value, Mapping):
            return None
        encoded = str(value.get("content") or "").replace("\n", "")
        return base64.b64decode(encoded).decode("utf-8"), str(value.get("sha") or "")

    async def _blob(self, content: str) -> str:
        result = await self._json("POST", "/git/blobs", {"content": content, "encoding": "utf-8"})
        return str(result["sha"])

    async def _json(
        self, method: str, path: str, body: Mapping[str, Any] | None = None
    ) -> dict[str, Any]:
        response = await self._request(method, path, body)
        self._require_success(response, "github_api_failed")
        if not isinstance(response.json_body, dict):
            raise VoiceIntakeError("github_response_invalid", retryable=True, status_code=503)
        return response.json_body

    async def _request(
        self, method: str, path: str, body: Mapping[str, Any] | None
    ) -> BoundedHTTPResponse:
        try:
            return await self._requester.request_json(
                method,
                self._origin + path,
                headers={**self._headers, "Content-Type": "application/json"},
                json_body=body,
                timeout_seconds=30.0,
                max_response_bytes=self._settings.max_json_bytes,
            )
        except BoundedHTTPError as exc:
            raise VoiceIntakeError(
                "github_network_error", retryable=True, status_code=503
            ) from exc

    @staticmethod
    def _require_success(response: BoundedHTTPResponse, code: str) -> None:
        if 200 <= response.status < 300:
            return
        raise VoiceIntakeError(
            code,
            retryable=response.status in {409, 422, 429} or response.status >= 500,
            status_code=503 if response.status >= 500 else 409,
        )
