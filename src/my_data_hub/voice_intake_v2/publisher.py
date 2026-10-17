from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import asdict
from typing import Any, NoReturn
from urllib.parse import quote

from my_data_hub.voice_intake.errors import GitHubPublicationConflict, VoiceIntakeError
from my_data_hub.voice_intake.github import IdeaHubPublisher
from my_data_hub.voice_intake.markdown import insert_registry_entry, render_voice_index
from my_data_hub.voice_intake.settings import VoiceIntakeSettings

from .contracts import PublicationReceipt
from .markdown import RenderedPublication, render_publication
from .store import PublicationProjection


def _same(left: str, right: str) -> bool:
    return hashlib.sha256(left.encode("utf-8")).digest() == hashlib.sha256(
        right.encode("utf-8")
    ).digest()


def _raise_retryable_readback(exc: VoiceIntakeError) -> NoReturn:
    """Keep read-only GitHub failures out of the manual-reconciliation lane.

    A successful ref update may become visible through the Contents API a
    moment later. Re-running the v2 publication stage is safe: it first
    reconciles the deterministic paths on current main and never repeats a
    durable Gemini stage.
    """
    transient_read_codes = {
        "github_content_read_failed",
        "github_head_read_failed",
        "github_commit_history_read_failed",
        "github_commit_read_failed",
    }
    if (
        exc.retryable
        or exc.reconciliation_required
        or exc.code not in transient_read_codes
    ):
        raise exc
    raise VoiceIntakeError(
        "github_readback_retryable",
        retryable=True,
        retry_after_seconds=exc.retry_after_seconds,
        status_code=503,
    ) from exc


class V2IdeaHubPublisher(IdeaHubPublisher):
    """V2 projection adapter over the already bounded v1 GitHub transport.

    Inference artifacts are durable before this object is invoked. Therefore
    optimistic-ref retries and readback reconciliation can never issue another
    Gemini request.
    """

    def __init__(self, settings: VoiceIntakeSettings, **kwargs: Any) -> None:
        super().__init__(settings, **kwargs)

    async def resolve_terminology_snapshot(self) -> dict[str, Any]:
        """Convert the verified v1 transport result into the durable v2 value."""
        return asdict(await super().resolve_terminology())

    async def publish_and_verify(self, projection: PublicationProjection) -> PublicationReceipt:
        if projection.terminology.get("status") != "current":
            raise VoiceIntakeError(
                "idea_hub_terminology_not_current", retryable=True, status_code=503
            )
        rendered = render_publication(projection)
        try:
            reconciled = await self._reconcile_current(rendered)
        except VoiceIntakeError as exc:
            _raise_retryable_readback(exc)
        if reconciled is not None:
            return reconciled

        for _attempt in range(4):
            head_sha, tree_sha = await self._head()
            registry_text, _ = await self._content(self.REGISTRY_PATH, head_sha)
            schema_text, _ = await self._content(self.REGISTRY_SCHEMA_PATH, head_sha)
            if f"session_id: {projection.session_id}" in registry_text:
                reconciled = await self._reconcile_current(rendered)
                if reconciled is not None:
                    return reconciled
                raise GitHubPublicationConflict("github_existing_publication_mismatch")

            updated_registry = insert_registry_entry(
                registry_text,
                entry=rendered.registry_entry,
                updated_at=rendered.registered_at,
            )
            self._validate_registry(updated_registry, schema_text)
            voice_index = render_voice_index(updated_registry)
            expected = {
                rendered.source_path: rendered.source,
                rendered.detail_path: rendered.detail,
                self.REGISTRY_PATH: updated_registry,
                self.VOICE_INDEX_PATH: voice_index,
            }
            blobs = {path: await self._blob(content) for path, content in expected.items()}
            tree = await self._json(
                "POST",
                "/git/trees",
                {
                    "base_tree": tree_sha,
                    "tree": [
                        {"path": path, "mode": "100644", "type": "blob", "sha": blob_sha}
                        for path, blob_sha in blobs.items()
                    ],
                },
            )
            commit = await self._json(
                "POST",
                "/git/commits",
                {
                    "message": f"intake(voice-v2): register {projection.session_id}",
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
            except VoiceIntakeError as exc:
                if exc.code != "github_network_error":
                    raise
                # The ref update may have succeeded even though its response
                # was lost. Prove both the candidate atomic commit and current
                # main before acknowledging it.
                try:
                    await self._verify_publication(
                        commit_sha=commit_sha,
                        expected=expected,
                        rendered=rendered,
                    )
                except VoiceIntakeError as readback_error:
                    raise VoiceIntakeError(
                        "github_outcome_ambiguous",
                        retryable=False,
                        status_code=503,
                        reconciliation_required=True,
                    ) from readback_error
                return self._receipt(rendered, commit_sha)
            if response.status in {409, 422}:
                continue
            self._require_success(response, "github_ref_update_failed")
            try:
                await self._verify_publication(
                    commit_sha=commit_sha,
                    expected=expected,
                    rendered=rendered,
                )
            except VoiceIntakeError as exc:
                _raise_retryable_readback(exc)
            return self._receipt(rendered, commit_sha)
        raise GitHubPublicationConflict("idea_hub_main_moved_repeatedly")

    async def _reconcile_current(
        self, rendered: RenderedPublication
    ) -> PublicationReceipt | None:
        head_sha, _ = await self._head()
        source = await self._content_optional(rendered.source_path, head_sha)
        detail = await self._content_optional(rendered.detail_path, head_sha)
        registry = await self._content_optional(self.REGISTRY_PATH, head_sha)
        voice_index = await self._content_optional(self.VOICE_INDEX_PATH, head_sha)
        if source is None and detail is None:
            if registry and f"session_id: {rendered.registry_entry['session_id']}" in registry[0]:
                raise GitHubPublicationConflict("github_existing_publication_mismatch")
            return None
        session_id = str(rendered.registry_entry["session_id"])
        if source is None or detail is None or registry is None or voice_index is None:
            raise GitHubPublicationConflict("github_existing_publication_mismatch")
        if (
            f"session_id: {session_id}" not in registry[0]
            or f"`{session_id}`" not in voice_index[0]
        ):
            raise GitHubPublicationConflict("github_existing_publication_mismatch")
        # Source and detail are allowed to evolve after intake (for example,
        # when an IdeaHub workflow reconciles or closes the captured idea).
        # The immutable proof is the original atomic four-path commit below,
        # not byte equality with a later main revision.
        commit_sha = await self._publication_commit_from_history(rendered)
        return self._receipt(rendered, commit_sha)

    async def _publication_commit_from_history(self, rendered: RenderedPublication) -> str:
        response = await self._request(
            "GET",
            "/commits?sha="
            f"{quote(self._settings.github_branch, safe='-._~')}"
            f"&path={quote(rendered.source_path, safe='/-._~')}&per_page=10",
            None,
        )
        self._require_success(response, "github_commit_history_read_failed")
        history = response.json_body
        if not isinstance(history, list):
            raise VoiceIntakeError(
                "github_commit_history_invalid", retryable=True, status_code=503
            )
        expected_paths = {
            rendered.source_path,
            rendered.detail_path,
            self.REGISTRY_PATH,
            self.VOICE_INDEX_PATH,
        }
        session_id = str(rendered.registry_entry["session_id"])
        for item in history:
            if not isinstance(item, Mapping):
                continue
            commit_sha = str(item.get("sha") or "")
            if len(commit_sha) != 40:
                continue
            commit_response = await self._request(
                "GET", f"/commits/{quote(commit_sha, safe='-._~')}", None
            )
            self._require_success(commit_response, "github_commit_read_failed")
            commit_value = commit_response.json_body
            if not isinstance(commit_value, Mapping) or not isinstance(
                commit_value.get("files"), list
            ):
                continue
            changed = {
                str(file.get("filename") or "")
                for file in commit_value["files"]
                if isinstance(file, Mapping)
            }
            if changed != expected_paths:
                continue
            try:
                source, _ = await self._content(rendered.source_path, commit_sha)
                detail, _ = await self._content(rendered.detail_path, commit_sha)
                registry, _ = await self._content(self.REGISTRY_PATH, commit_sha)
                index, _ = await self._content(self.VOICE_INDEX_PATH, commit_sha)
            except VoiceIntakeError:
                continue
            if (
                _same(source, rendered.source)
                and _same(detail, rendered.detail)
                and f"session_id: {session_id}" in registry
                and f"`{session_id}`" in index
            ):
                return commit_sha
        raise VoiceIntakeError(
            "github_publication_reconciliation_required",
            retryable=False,
            status_code=503,
            reconciliation_required=True,
        )

    async def _verify_publication(
        self,
        *,
        commit_sha: str,
        expected: dict[str, str],
        rendered: RenderedPublication,
    ) -> None:
        # The atomic candidate commit must contain byte-identical versions of
        # all four artifacts.
        for path, expected_content in expected.items():
            actual, _ = await self._content(path, commit_sha)
            if not _same(actual, expected_content):
                raise GitHubPublicationConflict(
                    "github_exact_commit_readback_hash_mismatch"
                )

        # Source and detail are immutable on current main. Registry and index
        # are append/update artifacts, so their durable session markers are
        # checked without rejecting later unrelated intake commits.
        current_sha, _ = await self._head()
        source, _ = await self._content(rendered.source_path, current_sha)
        detail, _ = await self._content(rendered.detail_path, current_sha)
        registry, _ = await self._content(self.REGISTRY_PATH, current_sha)
        index, _ = await self._content(self.VOICE_INDEX_PATH, current_sha)
        session_id = str(rendered.registry_entry["session_id"])
        if (
            not _same(source, rendered.source)
            or not _same(detail, rendered.detail)
            or f"session_id: {session_id}" not in registry
            or f"`{session_id}`" not in index
        ):
            raise GitHubPublicationConflict("github_main_readback_missing_session")

    @staticmethod
    def _analysis_transcript(content: str) -> str:
        heading = "\n## Полная расшифровка\n\n"
        ending = "\n\n## Техническая фиксация\n"
        if heading not in content:
            raise GitHubPublicationConflict("analysis_source_structure_changed")
        remaining = content.split(heading, 1)[1]
        if ending not in remaining:
            raise GitHubPublicationConflict("analysis_source_structure_changed")
        # Ignore later machine projection headings when finding the original
        # technical footer. Raw text and its hash remain immutable evidence.
        remaining = remaining.split("<!-- voice-analysis:", 1)[0]
        return remaining.rsplit(ending, 1)[0].strip()

    async def project_analysis(
        self, *, intent_id: str, source: dict[str, Any], revision: str, markdown: str,
        expected_previous_revision: str | None = None,
    ) -> str:
        """CAS-write only an owned analysis block, preserving the source revision."""
        import re
        if (source["repository"] != self._settings.github_repository
                or not re.fullmatch(r"voice:[0-9a-f]{64}", intent_id)
                or not re.fullmatch(r"[0-9a-f]{64}", revision)
                or len(markdown.encode()) > 256 * 1024
                or hashlib.sha256(markdown.encode()).hexdigest() != revision):
            raise ValueError("analysis_projection_invalid")
        path = source["path"]
        original, _ = await self._content(path, source["commit_sha"])
        transcript = self._analysis_transcript(original)
        if hashlib.sha256(transcript.encode()).hexdigest() != source["transcript_sha256"]:
            raise GitHubPublicationConflict("analysis_source_hash_mismatch")
        start = f"<!-- voice-analysis:{intent_id}:start -->"
        end = f"<!-- voice-analysis:{intent_id}:end -->"
        if "<!-- voice-analysis:" in markdown:
            raise ValueError("analysis_projection_marker_invalid")
        block = (f"{start}\n<!-- revision:{revision} -->\n"
                 f"<!-- original-source:{source['commit_sha']} -->\n{markdown}\n{end}")
        head, tree_sha = await self._head()
        current, _ = await self._content(path, head)
        current_transcript = self._analysis_transcript(current)
        if hashlib.sha256(current_transcript.encode()).hexdigest() != source["transcript_sha256"]:
            raise GitHubPublicationConflict("analysis_source_changed")
        if current.count(start) != current.count(end) or current.count(start) > 1:
            raise GitHubPublicationConflict("analysis_projection_markers_conflict")
        if start in current:
            before, remaining = current.split(start)
            old, after = remaining.split(end)
            if start + old + end == block:
                return head
            # Preserve user edits to an owned section instead of silently
            # replacing them with the next machine revision.
            lines = old[1:-1].split("\n", 2)
            old_revision = lines[0].removeprefix("<!-- revision:").removesuffix(" -->")
            old_markdown = lines[2] if len(lines) == 3 else ""
            if old_revision != expected_previous_revision:
                raise GitHubPublicationConflict("analysis_projection_revision_conflict")
            # Preserve the exact body including any final newline.
            if hashlib.sha256(old_markdown.encode()).hexdigest() != old_revision:
                raise GitHubPublicationConflict("analysis_projection_manually_changed")
            updated = before + block + after
        else:
            if expected_previous_revision is not None:
                raise GitHubPublicationConflict("analysis_projection_block_missing")
            updated = current.rstrip() + "\n\n" + block + "\n"
        blob = await self._blob(updated)
        tree = await self._json("POST", "/git/trees", {"base_tree": tree_sha,
            "tree": [{"path": path, "mode": "100644", "type": "blob", "sha": blob}]})
        commit = await self._json("POST", "/git/commits", {
            "message": "voice: project verified analysis section", "tree": tree["sha"], "parents": [head]})
        sha = str(commit["sha"])
        try:
            response = await self._request("PATCH",
                f"/git/refs/heads/{quote(self._settings.github_branch, safe='/-._~')}",
                {"sha": sha, "force": False})
            self._require_success(response, "analysis_projection_ref_update_failed")
        except VoiceIntakeError:
            # Never retry a possibly-applied write inside this call. Readback
            # below can reconcile it; the next cycle also compares the block.
            current_head, _ = await self._head()
            actual, _ = await self._content(path, current_head)
            if block not in actual:
                raise
            return current_head
        exact, _ = await self._content(path, sha)
        current_head, _ = await self._head()
        actual, _ = await self._content(path, current_head)
        if exact != updated or block not in actual:
            raise GitHubPublicationConflict("analysis_projection_readback_failed")
        return sha

    def _receipt(self, rendered: RenderedPublication, commit_sha: str) -> PublicationReceipt:
        return PublicationReceipt(
            github_url=self._branch_url(rendered.source_path),
            github_commit_sha=commit_sha,
            github_verified=True,
        )


__all__ = ["V2IdeaHubPublisher"]
