from __future__ import annotations

import hashlib
from dataclasses import replace
from typing import Any

import pytest

from my_data_hub.google_ai.http import BoundedHTTPResponse
from my_data_hub.voice_intake.errors import VoiceIntakeError
from my_data_hub.voice_intake_v2.publisher import V2IdeaHubPublisher
from my_data_hub.voice_intake_v2.store import PublicationProjection

from .conftest import SESSION_ID, SHA, summary_value


def projection(create_request, complete_request, terminology) -> PublicationProjection:
    return PublicationProjection(
        session_id=SESSION_ID,
        create=create_request.model_dump(mode="json"),
        complete=complete_request.model_dump(mode="json"),
        terminology=terminology,
        transport_chunks=({"chunk_index": 0, "sha256": SHA, "duration_ms": 240000},),
        transcript={"transcript": "Полная запись", "language": "ru-RU", "uncertain_fragments": []},
        summary=summary_value(),
        transcription_request_uid=None,
        content_verification_receipt_sha256="d" * 64,
        summary_request_uid="summary-uid",
        transcription_limiter={"mode": "per_chunk", "segment_count": 1},
        summary_limiter={"request_uid": "summary-uid"},
        model="gemini-3.1-flash-lite",
    )


class MemoryPublisher(V2IdeaHubPublisher):
    def __init__(self, settings, *, patch_mode: str = "success") -> None:
        super().__init__(settings)
        self.patch_mode = patch_mode
        self.head_sha = "1" * 40
        self.commits: dict[str, dict[str, str]] = {
            self.head_sha: {
                self.REGISTRY_PATH: (
                    "schema_version: 1.0.0\nregistry_id: test\n"
                    "updated_at: '2026-01-01T00:00:00Z'\nsessions: []\n"
                ),
                self.REGISTRY_SCHEMA_PATH: '{"type":"object"}',
                self.VOICE_INDEX_PATH: "# old\n",
                self.TERMINOLOGY_PATH: (
                    "schema_version: 1.0.0\n"
                    "card_id: idea-hub-voice-terminology\n"
                    "rules: [Use canonical terms.]\n"
                    "entries:\n- canonical: IdeaHub Map\n  kind: project\n"
                ),
            }
        }
        self.blobs: dict[str, str] = {}
        self.trees: dict[str, dict[str, str]] = {}
        self.created_commits: list[str] = []
        self.commit_paths: dict[str, list[str]] = {}
        self.tree_paths: list[list[str]] = []

    async def _head(self) -> tuple[str, str]:
        return self.head_sha, "tree-" + self.head_sha

    async def _content(self, path: str, ref: str) -> tuple[str, str]:
        if path not in self.commits[ref]:
            raise VoiceIntakeError("github_content_read_failed", status_code=404)
        value = self.commits[ref][path]
        return value, hashlib.sha1(value.encode()).hexdigest()

    async def _content_optional(self, path: str, ref: str) -> tuple[str, str] | None:
        value = self.commits[ref].get(path)
        if value is None:
            return None
        return value, hashlib.sha1(value.encode()).hexdigest()

    async def _blob(self, content: str) -> str:
        sha = hashlib.sha256(content.encode()).hexdigest()
        self.blobs[sha] = content
        return sha

    async def _json(self, method: str, path: str, body=None) -> dict[str, Any]:
        assert method == "POST"
        assert isinstance(body, dict)
        if path == "/git/trees":
            base_sha = str(body["base_tree"]).removeprefix("tree-")
            files = dict(self.commits[base_sha])
            paths = []
            for item in body["tree"]:
                paths.append(item["path"])
                files[item["path"]] = self.blobs[item["sha"]]
            tree_sha = "tree-v2-" + str(len(self.trees))
            self.trees[tree_sha] = files
            self.tree_paths.append(paths)
            return {"sha": tree_sha}
        if path == "/git/commits":
            commit_sha = f"{len(self.created_commits) + 2:040x}"
            self.commits[commit_sha] = dict(self.trees[body["tree"]])
            self.created_commits.append(commit_sha)
            self.commit_paths[commit_sha] = list(self.tree_paths[-1])
            return {"sha": commit_sha}
        raise AssertionError(path)

    async def _request(self, method: str, path: str, body) -> BoundedHTTPResponse:
        if method == "GET" and path.startswith("/commits?sha="):
            history = [{"sha": sha} for sha in reversed(self.created_commits)]
            return BoundedHTTPResponse(200, history, None, "application/json")
        if method == "GET" and path.startswith("/commits/"):
            sha = path.removeprefix("/commits/")
            return BoundedHTTPResponse(
                200,
                {"sha": sha, "files": [{"filename": item} for item in self.commit_paths[sha]]},
                None,
                "application/json",
            )
        assert method == "PATCH"
        commit_sha = body["sha"]
        if self.patch_mode == "conflict":
            return BoundedHTTPResponse(409, {}, None, "application/json")
        if self.patch_mode in {"success", "ambiguous_applied"}:
            self.head_sha = commit_sha
        if self.patch_mode.startswith("ambiguous"):
            raise VoiceIntakeError("github_network_error", retryable=True, status_code=503)
        return BoundedHTTPResponse(200, {}, None, "application/json")


@pytest.mark.asyncio
async def test_atomic_four_file_publication_and_repeat_reconciles_without_new_commit(
    auth_settings, create_request, complete_request, terminology
) -> None:
    publisher = MemoryPublisher(auth_settings)
    item = projection(create_request, complete_request, terminology)
    receipt = await publisher.publish_and_verify(item)

    assert receipt.github_verified
    assert receipt.github_commit_sha == publisher.head_sha
    assert publisher.tree_paths == [[
        f"inbox/voice/2026/08/{SESSION_ID}.md",
        f"registry/sessions/2026/08/{SESSION_ID}.md",
        publisher.REGISTRY_PATH,
        publisher.VOICE_INDEX_PATH,
    ]]
    snapshot = publisher.commits[receipt.github_commit_sha]
    assert "api_contract: voice-intake-v2" in snapshot[publisher.tree_paths[0][0]]
    assert f"session_id: {SESSION_ID}" in snapshot[publisher.REGISTRY_PATH]
    assert f"`{SESSION_ID}`" in snapshot[publisher.VOICE_INDEX_PATH]

    later_main = "f" * 40
    publisher.commits[later_main] = {
        **publisher.commits[receipt.github_commit_sha],
        "unrelated.md": "later main change\n",
    }
    publisher.head_sha = later_main
    repeated = await publisher.publish_and_verify(item)
    assert repeated.github_verified
    assert repeated.github_commit_sha == receipt.github_commit_sha
    assert len(publisher.created_commits) == 1


@pytest.mark.asyncio
async def test_runtime_terminology_resolver_returns_durable_json_value(auth_settings) -> None:
    publisher = MemoryPublisher(auth_settings)
    value = await publisher.resolve_terminology_snapshot()
    assert value["status"] == "current"
    assert value["source_path"] == publisher.TERMINOLOGY_PATH
    assert value["source_commit_sha"] == publisher.head_sha
    assert "IdeaHub Map" in value["prompt"]


@pytest.mark.asyncio
async def test_patch_network_error_reconciles_candidate_commit_when_ref_was_applied(
    auth_settings, create_request, complete_request, terminology
) -> None:
    publisher = MemoryPublisher(auth_settings, patch_mode="ambiguous_applied")
    receipt = await publisher.publish_and_verify(projection(create_request, complete_request, terminology))
    assert receipt.github_verified
    assert receipt.github_commit_sha == publisher.created_commits[0]


@pytest.mark.asyncio
async def test_patch_network_error_without_readback_is_explicitly_ambiguous(
    auth_settings, create_request, complete_request, terminology
) -> None:
    publisher = MemoryPublisher(auth_settings, patch_mode="ambiguous_not_applied")
    with pytest.raises(VoiceIntakeError) as caught:
        await publisher.publish_and_verify(projection(create_request, complete_request, terminology))
    assert caught.value.code == "github_outcome_ambiguous"
    assert caught.value.reconciliation_required


@pytest.mark.asyncio
async def test_current_main_detail_mutation_is_not_accepted_as_verified(
    auth_settings, create_request, complete_request, terminology
) -> None:
    publisher = MemoryPublisher(auth_settings)
    item = projection(create_request, complete_request, terminology)
    await publisher.publish_and_verify(item)
    detail_path = f"registry/sessions/2026/08/{SESSION_ID}.md"
    publisher.commits[publisher.head_sha][detail_path] += "mutated\n"
    with pytest.raises(VoiceIntakeError) as caught:
        await publisher.publish_and_verify(item)
    assert caught.value.code == "github_existing_publication_mismatch"


@pytest.mark.asyncio
async def test_current_markers_without_atomic_origin_require_reconciliation(
    auth_settings, create_request, complete_request, terminology
) -> None:
    publisher = MemoryPublisher(auth_settings)
    item = projection(create_request, complete_request, terminology)
    await publisher.publish_and_verify(item)
    publisher.created_commits.clear()
    with pytest.raises(VoiceIntakeError) as caught:
        await publisher.publish_and_verify(item)
    assert caught.value.code == "github_publication_reconciliation_required"
    assert caught.value.reconciliation_required


@pytest.mark.asyncio
async def test_github_transport_is_never_started_without_content_verification_receipt(
    auth_settings, create_request, complete_request, terminology
) -> None:
    publisher = MemoryPublisher(auth_settings)
    item = replace(
        projection(create_request, complete_request, terminology),
        content_verification_receipt_sha256="",
    )
    with pytest.raises(VoiceIntakeError) as caught:
        await publisher.publish_and_verify(item)
    assert caught.value.code == "content_verification_required"
    assert publisher.created_commits == []
    assert publisher.head_sha == "1" * 40


@pytest.mark.asyncio
async def test_exact_github_readback_receipt_contains_no_purge_authority(
    auth_settings, create_request, complete_request, terminology
) -> None:
    publisher = MemoryPublisher(auth_settings)
    receipt = await publisher.publish_and_verify(
        projection(create_request, complete_request, terminology)
    )
    assert receipt.github_verified is True
    assert set(receipt.model_dump()) == {
        "github_url",
        "github_commit_sha",
        "github_verified",
    }
