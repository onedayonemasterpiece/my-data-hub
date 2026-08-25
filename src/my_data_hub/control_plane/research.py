from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import tempfile
from collections.abc import Mapping
from contextlib import contextmanager, suppress
from datetime import UTC, datetime, timedelta
from pathlib import Path, PurePosixPath
from typing import Any
from uuid import UUID, uuid4

from my_data_hub.control_plane.ledger import (
    ControlLedger,
    KaggleArtifactRecord,
    KaggleResearchRecord,
    KaggleRevisionRecord,
    KaggleRunRecord,
    KaggleRunState,
    LeaseRejected,
)
from my_data_hub.hashing import canonical_json_bytes, sha256_value
from my_data_hub.mcp.oauth import AccessIdentity
from my_data_hub.providers.kaggle import (
    EffectOutcome,
    KaggleAmbiguousMutation,
    KaggleDatasetInspection,
    KaggleKernelRunIdentity,
    KagglePolicyError,
    KaggleProviderAdapter,
    KernelState,
    MutationAction,
    ProviderEffectIntent,
)
from my_data_hub.providers.kaggle.source_attestation import executable_source_sha256
from my_data_hub.providers.models import ControlClass

_REF = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
_ALIAS = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,62}[a-z0-9])?$")
_SHA256 = re.compile(r"^[a-f0-9]{64}$")
_TERMINAL = {KaggleRunState.SUCCEEDED, KaggleRunState.FAILED}
_STANDARD_MEDIA = {
    "research-output-manifest.json": ("manifest", "application/json"),
    "summary.md": ("summary", "text/markdown"),
    "metrics.json": ("metrics", "application/json"),
    "provenance.json": ("provenance", "application/json"),
    "diagnostics.json": ("diagnostics", "application/json"),
    "run.log": ("log", "text/plain"),
}
_PROVIDER_REQUIRED = frozenset({"summary.md", "metrics.json", "diagnostics.json", "run.log"})
_MAX_SOURCE_BYTES = 262_144
_MAX_OUTPUT_FILE_BYTES = 64 * 1024 * 1024
_MAX_OUTPUT_TOTAL_BYTES = 128 * 1024 * 1024
_MAX_CHUNK_BYTES = 131_072
_SECRET_SOURCE = re.compile(
    r"(?i)(-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----|"
    r"(?:api[_-]?key|password|secret|token)\s*=\s*['\"][^'\"]{8,})"
)


class KaggleResearchError(RuntimeError):
    """Bounded semantic failure; provider details and credentials are never included."""

    def __init__(self, message: str, *, code: str | None = None) -> None:
        self.code = code or (
            message
            if message in {"DATASET_VERSION_CHANGED", "TERMS_ACCEPTANCE_REQUIRED"}
            else "research_request_invalid"
        )
        super().__init__(message)


def _now() -> datetime:
    return datetime.now(UTC)


def _public_time(value: datetime | None) -> str | None:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z") if value else None


def _safe_path(value: str) -> str:
    if not value or len(value) > 1000 or "\\" in value or value.startswith("/") or "\x00" in value:
        raise KaggleResearchError("artifact path is not a bounded relative POSIX path")
    parts = value.split("/")
    if any(part in {"", ".", ".."} for part in parts) or PurePosixPath(value).as_posix() != value:
        raise KaggleResearchError("artifact path is not normalized")
    return value


def _encode_offset_cursor(kind: str, offset: int, binding: str) -> str:
    payload = {"kind": kind, "offset": offset, "binding": binding}
    return base64.urlsafe_b64encode(canonical_json_bytes(payload)).decode().rstrip("=")


def _decode_offset_cursor(value: object, *, kind: str, binding: str) -> int:
    if value is None:
        return 0
    try:
        payload = json.loads(base64.urlsafe_b64decode(str(value) + "===").decode())
        if (
            not isinstance(payload, dict)
            or set(payload) != {"kind", "offset", "binding"}
            or payload["kind"] != kind
            or payload["binding"] != binding
            or isinstance(payload["offset"], bool)
            or not isinstance(payload["offset"], int)
            or payload["offset"] < 0
        ):
            raise ValueError("cursor payload differs")
        return int(payload["offset"])
    except Exception as exc:
        raise KaggleResearchError("continuation cursor is invalid or belongs to another result") from exc


class KaggleResearchService:
    """Small durable research service over the existing ledger and Kaggle adapter."""

    def __init__(
        self,
        ledger: ControlLedger,
        adapter: KaggleProviderAdapter,
        *,
        cache_root: Path | None = None,
        clock: Any | None = None,
        lease_seconds: int = 120,
    ) -> None:
        self.ledger = ledger
        self.adapter = adapter
        self.clock = clock or ledger.clock.now
        self.lease_seconds = lease_seconds
        self.cache_root = (cache_root or ledger.path.parent / "research-artifacts").resolve()
        if self.cache_root.is_symlink():
            raise ValueError("research artifact cache must not be a symlink")
        self.cache_root.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(self.cache_root, 0o700)

    # Dataset discovery/read -------------------------------------------------

    def datasets_search(self, arguments: Mapping[str, Any], principal: AccessIdentity) -> dict[str, Any]:
        query = str(arguments["query"])
        visibility = str(arguments.get("visibility", "all"))
        limit = int(arguments.get("limit", 20))
        cursor = arguments.get("cursor")
        scopes = [visibility] if visibility != "all" else ["public", "owner_private"]
        results: list[dict[str, Any]] = []
        decoded: dict[str, Any] = {}
        query_sha = hashlib.sha256(query.encode()).hexdigest()
        if cursor:
            try:
                decoded = json.loads(base64.urlsafe_b64decode(str(cursor) + "===").decode())
                if not isinstance(decoded, dict):
                    raise ValueError("cursor payload is not an object")
            except Exception as exc:
                raise KaggleResearchError("Dataset search cursor is invalid") from exc
            if decoded.get("query_sha256") != query_sha or decoded.get("visibility") != visibility:
                raise KaggleResearchError("Dataset search cursor belongs to a different query")
        try:
            start_index = int(decoded.get("scope_index", 0))
        except (TypeError, ValueError) as exc:
            raise KaggleResearchError("Dataset search cursor scope is invalid") from exc
        if not 0 <= start_index < len(scopes):
            raise KaggleResearchError("Dataset search cursor scope is invalid")
        continuation_state: dict[str, Any] | None = None
        provider_cursor = str(decoded["provider_cursor"]) if decoded.get("provider_cursor") else None
        for index in range(start_index, len(scopes)):
            scope = scopes[index]
            page, next_cursor = self.adapter.search_datasets(
                query=query,
                visibility=scope,
                cursor=provider_cursor if index == start_index else None,
                limit=max(1, limit - len(results)),
            )
            for item in page:
                projection = self.ledger.latest_provider_resource(item.provider_ref)
                results.append(
                    {
                        **item.model_dump(mode="json"),
                        "status_only": bool(
                            projection is not None
                            and projection["control_class"] == "orchestrator_protected"
                        ),
                    }
                )
            if next_cursor:
                continuation_state = {
                    "query_sha256": query_sha,
                    "visibility": visibility,
                    "scope_index": index,
                    "provider_cursor": next_cursor,
                }
                break
            if len(results) >= limit:
                if index + 1 < len(scopes):
                    continuation_state = {
                        "query_sha256": query_sha,
                        "visibility": visibility,
                        "scope_index": index + 1,
                        "provider_cursor": None,
                    }
                break
        next_value = (
            base64.urlsafe_b64encode(canonical_json_bytes(continuation_state)).decode().rstrip("=")
            if continuation_state
            else None
        )
        return {
            "datasets": results[:limit],
            "next_cursor": next_value,
            "bounded": True,
            "continuation": ({"tool": "datasets.search", "cursor": next_value} if next_value else None),
            "allowed_next_actions": ["datasets.inspect"],
        }

    def datasets_inspect(self, arguments: Mapping[str, Any], principal: AccessIdentity) -> dict[str, Any]:
        inspection = self._inspect_dataset(
            provider_ref=str(arguments["dataset_ref"]),
            provider_version=(int(arguments["provider_version"]) if arguments.get("provider_version") else None),
        )
        file_cursor = _decode_offset_cursor(
            arguments.get("file_cursor"),
            kind="dataset-files",
            binding=f"{inspection.provider_ref}/{inspection.provider_version}",
        )
        file_limit = int(arguments.get("file_limit", 100))
        files = [item.model_dump(mode="json") for item in inspection.files]
        page = files[file_cursor : file_cursor + file_limit]
        next_offset = file_cursor + len(page) if file_cursor + len(page) < len(files) else None
        next_cursor = (
            _encode_offset_cursor(
                "dataset-files",
                next_offset,
                f"{inspection.provider_ref}/{inspection.provider_version}",
            )
            if next_offset is not None
            else None
        )
        metadata = inspection.model_dump(mode="json", exclude={"files"})
        return {
            **metadata,
            "files": page,
            "file_count": len(files),
            "next_file_cursor": next_cursor,
            "continuation": (
                {
                    "tool": "datasets.inspect",
                    "dataset_ref": inspection.provider_ref,
                    "provider_version": inspection.provider_version,
                    "file_cursor": next_cursor,
                    "file_limit": file_limit,
                }
                if next_cursor is not None
                else None
            ),
            "mutation_allowed": False,
            "credentials_returned": False,
            "allowed_next_actions": ["research.create", "datasets.file.read", "notebooks.inputs.set"],
        }

    def datasets_file_read(self, arguments: Mapping[str, Any], principal: AccessIdentity) -> dict[str, Any]:
        self._assert_not_protected(str(arguments["dataset_ref"]), resource_kind="Dataset")
        item = self.adapter.read_dataset_file_exact(
            provider_ref=str(arguments["dataset_ref"]),
            provider_version=int(arguments["provider_version"]),
            path=str(arguments["path"]),
        )
        offset = int(arguments.get("offset", 0))
        max_bytes = int(arguments.get("max_bytes", _MAX_CHUNK_BYTES))
        if offset > item.byte_size or not 1 <= max_bytes <= _MAX_CHUNK_BYTES:
            raise KaggleResearchError("Dataset file chunk bounds are invalid")
        content = item.content[offset : offset + max_bytes]
        next_offset = offset + len(content)
        complete = next_offset == item.byte_size
        return {
            "dataset_ref": item.provider_ref,
            "provider_version": item.provider_version,
            "path": item.path,
            "file_byte_size": item.byte_size,
            "file_sha256": item.sha256,
            "encoding": "base64",
            "offset": offset,
            "content_base64": base64.b64encode(content).decode("ascii"),
            "content_byte_size": len(content),
            "content_sha256": hashlib.sha256(content).hexdigest(),
            "next_offset": None if complete else next_offset,
            "complete": complete,
            "bounded": True,
            "continuation": (
                None
                if complete
                else {
                    "tool": "datasets.file.read",
                    "dataset_ref": item.provider_ref,
                    "provider_version": item.provider_version,
                    "path": item.path,
                    "offset": next_offset,
                }
            ),
            "allowed_next_actions": ["datasets.file.read"] if not complete else ["research.create"],
        }

    # Research/revisions -----------------------------------------------------

    def research_create(self, arguments: Mapping[str, Any], principal: AccessIdentity) -> dict[str, Any]:
        alias = str(arguments["alias"]).lower() if arguments.get("alias") else None
        if alias and not _ALIAS.fullmatch(alias):
            raise KaggleResearchError("research alias is invalid")
        inspection = self._inspect_dataset(provider_ref=str(arguments["dataset_ref"]))
        if inspection.terms_acceptance_required:
            raise KaggleResearchError("TERMS_ACCEPTANCE_REQUIRED")
        research_id = str(uuid4())
        revision_id = str(uuid4())
        pin = self._pin(inspection)
        scaffold = (
            "# Managed Kaggle research draft\n"
            "# Save a new revision with notebooks.save before production use.\n"
        )
        source_sha = executable_source_sha256(scaffold.encode(), kernel_type="script")
        research = self.ledger.create_kaggle_research(
            research_id=research_id,
            owner_subject=principal.subject,
            alias=alias,
            title=str(arguments["title"]),
            goal=str(arguments["goal"]),
            primary_dataset_ref=inspection.provider_ref,
            revision_id=revision_id,
            code_file="research.py",
            kernel_type="script",
            source_utf8=scaffold,
            source_sha256=source_sha,
            runtime={"accelerator": "none", "enable_internet": False, "timeout_seconds": 1800},
            inputs=[pin],
            inputs_sha256=sha256_value([pin]),
        )
        return self._research_response(research, include_history=True)

    def research_list(self, arguments: Mapping[str, Any], principal: AccessIdentity) -> dict[str, Any]:
        cursor = _decode_offset_cursor(
            arguments.get("cursor"), kind="research-list", binding=principal.subject
        )
        limit = int(arguments.get("limit", 20))
        rows = self.ledger.list_kaggle_researches(owner_subject=principal.subject, cursor=cursor, limit=limit + 1)
        page, more = rows[:limit], len(rows) > limit
        next_cursor = (
            _encode_offset_cursor("research-list", cursor + limit, principal.subject) if more else None
        )
        return {
            "researches": [self._research_response(row) for row in page],
            "next_cursor": next_cursor,
            "continuation": ({"tool": "research.list", "cursor": next_cursor} if more else None),
            "allowed_next_actions": ["research.get", "research.create"],
        }

    def research_get(self, arguments: Mapping[str, Any], principal: AccessIdentity) -> dict[str, Any]:
        research = self._research(arguments, principal)
        return self._research_response(
            research,
            include_history=True,
            revision_cursor=_decode_offset_cursor(
                arguments.get("revision_cursor"),
                kind="research-revisions",
                binding=research.research_id,
            ),
            run_cursor=_decode_offset_cursor(
                arguments.get("run_cursor"), kind="research-runs", binding=research.research_id
            ),
            history_limit=int(arguments.get("history_limit", 20)),
        )

    def notebooks_find(self, arguments: Mapping[str, Any], principal: AccessIdentity) -> dict[str, Any]:
        research = self._research(arguments, principal) if self._has_selector(arguments) else None
        query = str(arguments.get("query") or "") or None
        if research is not None:
            query = research.research_id.replace("-", "")[:12]
        rows, next_cursor = self.adapter.find_owner_notebooks(
            query=query, cursor=str(arguments.get("cursor")) if arguments.get("cursor") else None,
            limit=int(arguments.get("limit", 20)),
        )
        linked_runs = (
            self.ledger.kaggle_runs(owner_subject=principal.subject, research_id=research.research_id)
            if research
            else []
        )
        linked_refs = {
            run.provider_run_ref.rsplit("/", 1)[0]
            for run in linked_runs
            if run.provider_run_ref
        }
        notebooks: list[dict[str, Any]] = []
        for row in rows:
            projection = self.ledger.latest_provider_resource(row.provider_ref)
            status_only = bool(
                projection is not None and projection["control_class"] == "orchestrator_protected"
            )
            notebooks.append(
                {
                    **row.model_dump(mode="json"),
                    "managed": row.provider_ref in linked_refs,
                    "mutable": False,
                    "status_only": status_only,
                    "source_readable": not status_only,
                }
            )
        return {
            "notebooks": notebooks,
            "next_cursor": next_cursor,
            "continuation": ({"tool": "notebooks.find", "cursor": next_cursor} if next_cursor else None),
            "allowed_next_actions": ["notebooks.get", "notebooks.save"],
        }

    def notebooks_get(self, arguments: Mapping[str, Any], principal: AccessIdentity) -> dict[str, Any]:
        if arguments.get("notebook_ref"):
            notebook_ref = str(arguments["notebook_ref"])
            projection = self.ledger.latest_provider_resource(notebook_ref)
            if projection is not None and projection["control_class"] == "orchestrator_protected":
                raise KaggleResearchError("orchestrator-protected Notebooks are status-only")
            source = self.adapter.read_owner_notebook_source(
                provider_ref=notebook_ref,
                source_version=int(arguments["source_version"]),
            )
            self._validate_source(source.source_utf8, source.kernel_type)
            return {
                **source.model_dump(mode="json"),
                "managed": False,
                "mutable": False,
                "credentials_returned": False,
                "allowed_next_actions": ["research.create", "notebooks.save"],
            }
        research = self._research(arguments, principal)
        revision = self._revision(arguments, principal, research)
        return {
            "research_id": research.research_id,
            "notebook_ref": research.notebook_ref,
            "revision": self._revision_public(revision, include_source=True),
            "allowed_next_actions": self._revision_actions(revision),
        }

    def notebooks_save(self, arguments: Mapping[str, Any], principal: AccessIdentity) -> dict[str, Any]:
        research = self._research(arguments, principal)
        parent = self._revision(arguments, principal, research, current_default=True)
        source = str(arguments["source_utf8"])
        kernel_type = str(arguments.get("kernel_type", "script"))
        self._validate_source(source, kernel_type)
        runtime = self._runtime(arguments.get("runtime"))
        revision = self.ledger.save_kaggle_revision(
            owner_subject=principal.subject,
            research_id=research.research_id,
            revision_id=str(uuid4()),
            parent_revision_id=parent.revision_id,
            code_file=str(arguments.get("code_file", "research.py")),
            kernel_type=kernel_type,
            source_utf8=source,
            source_sha256=executable_source_sha256(source.encode(), kernel_type=kernel_type),
            runtime=runtime,
            inputs=parent.inputs,
            inputs_sha256=parent.inputs_sha256,
        )
        return {
            "research_id": research.research_id,
            "revision": self._revision_public(revision, include_source=False),
            "allowed_next_actions": ["notebooks.inputs.set", "runs.start", "notebooks.get"],
        }

    def notebooks_inputs_set(self, arguments: Mapping[str, Any], principal: AccessIdentity) -> dict[str, Any]:
        research = self._research(arguments, principal)
        revision = self._revision(arguments, principal, research, current_default=True)
        pins = [
            self._pin(
                self._inspect_dataset(
                    provider_ref=str(item["dataset_ref"]),
                    provider_version=(int(item["provider_version"]) if item.get("provider_version") else None),
                )
            )
            for item in arguments["inputs"]
        ]
        if not pins or len(pins) > 16:
            raise KaggleResearchError("research input count is outside 1..16")
        updated = self.ledger.set_kaggle_revision_inputs(
            owner_subject=principal.subject,
            research_id=research.research_id,
            revision_id=revision.revision_id,
            inputs=pins,
            inputs_sha256=sha256_value(pins),
        )
        return {
            "research_id": research.research_id,
            "revision": self._revision_public(updated, include_source=False),
            "allowed_next_actions": ["runs.start", "notebooks.get"],
        }

    # Runs/recovery ----------------------------------------------------------

    def runs_start(self, arguments: Mapping[str, Any], principal: AccessIdentity) -> dict[str, Any]:
        research = self._research(arguments, principal)
        revision = self._revision(arguments, principal, research, current_default=True)
        existing = next(
            (
                item
                for item in self.ledger.kaggle_runs(owner_subject=principal.subject, research_id=research.research_id)
                if item.revision_id == revision.revision_id and item.retry_of_run_id is None
            ),
            None,
        )
        if existing is not None:
            if existing.state in {KaggleRunState.SUBMITTING, KaggleRunState.SUBMISSION_UNKNOWN}:
                with suppress(LeaseRejected):
                    self._reconcile_submission(existing)
                existing = self.ledger.kaggle_run(owner_subject=principal.subject, run_id=existing.run_id) or existing
            return self._run_response(existing, principal, revision=revision)
        run, created = self._prepare_run(principal, research, revision, retry_of=None)
        if created:
            self._submit_prepared(run)
        current = self.ledger.kaggle_run(owner_subject=principal.subject, run_id=run.run_id)
        assert current is not None
        return self._run_response(current, principal, revision=revision)

    def runs_retry(self, arguments: Mapping[str, Any], principal: AccessIdentity) -> dict[str, Any]:
        failed = self.ledger.kaggle_run(owner_subject=principal.subject, run_id=str(arguments["run_id"]))
        if failed is None or failed.state is not KaggleRunState.FAILED:
            raise KaggleResearchError("only an exact terminal failed run can be retried")
        research = self.ledger.kaggle_research(owner_subject=principal.subject, research_id=failed.research_id)
        revision = self.ledger.kaggle_revision(
            owner_subject=principal.subject, research_id=failed.research_id, revision_id=failed.revision_id
        )
        if research is None or revision is None:
            raise KaggleResearchError("failed run history is incomplete")
        run, created = self._prepare_run(principal, research, revision, retry_of=failed.run_id)
        if created:
            self._submit_prepared(run)
        elif run.state in {KaggleRunState.SUBMITTING, KaggleRunState.SUBMISSION_UNKNOWN}:
            with suppress(LeaseRejected):
                self._reconcile_submission(run)
        current = self.ledger.kaggle_run(owner_subject=principal.subject, run_id=run.run_id)
        assert current is not None
        return self._run_response(current, principal, revision=revision)

    def runs_get(self, arguments: Mapping[str, Any], principal: AccessIdentity) -> dict[str, Any]:
        run = self.ledger.kaggle_run(owner_subject=principal.subject, run_id=str(arguments["run_id"]))
        if run is None:
            raise KaggleResearchError("research run was not found")
        revision = self.ledger.kaggle_revision(
            owner_subject=principal.subject, research_id=run.research_id, revision_id=run.revision_id
        )
        if revision is None:
            raise KaggleResearchError("research revision was not found")
        if run.state not in _TERMINAL:
            with suppress(LeaseRejected):
                self.reconcile_run(run.run_id)
            run = self.ledger.kaggle_run(owner_subject=principal.subject, run_id=run.run_id) or run
        return self._run_response(run, principal, revision=revision)

    def runs_logs(self, arguments: Mapping[str, Any], principal: AccessIdentity) -> dict[str, Any]:
        run = self.ledger.kaggle_run(owner_subject=principal.subject, run_id=str(arguments["run_id"]))
        if run is None or not run.provider_run_ref or not run.provider_source_version or not run.provider_source_sha256:
            raise KaggleResearchError("exact provider run logs are not available yet")
        identity = self._provider_run_identity(run)
        log = self.adapter.read_exact_run_logs(identity)
        offset = int(arguments.get("offset", 0))
        max_bytes = int(arguments.get("max_bytes", 65_536))
        if offset > log.byte_size or not 1 <= max_bytes <= _MAX_CHUNK_BYTES:
            raise KaggleResearchError("run log chunk bounds are invalid")
        content = log.content[offset : offset + max_bytes]
        next_offset = offset + len(content)
        complete = next_offset == log.byte_size
        return {
            "run_id": run.run_id,
            "semantic_status": run.state.value,
            "provider_status": run.last_provider_status,
            "encoding": "base64",
            "offset": offset,
            "content_base64": base64.b64encode(content).decode("ascii"),
            "content_byte_size": len(content),
            "content_sha256": hashlib.sha256(content).hexdigest(),
            "log_byte_size": log.byte_size,
            "log_sha256": log.sha256,
            "next_offset": None if complete else next_offset,
            "complete": complete,
            "bounded": True,
            "continuation": (
                {"tool": "runs.logs", "run_id": run.run_id, "offset": next_offset}
                if not complete
                else None
            ),
            "allowed_next_actions": ["runs.logs"] if not complete else ["runs.get"],
        }

    def reconcile_due_once(self, *, limit: int = 10) -> dict[str, int]:
        observed = self.ledger.due_kaggle_runs(limit=limit)
        reconciled = 0
        for run in observed:
            try:
                self.reconcile_run(run.run_id)
                reconciled += 1
            except Exception:
                # Durable next_poll_at/state remains available for the next bounded pass.
                continue
        return {"observed": len(observed), "reconciled": reconciled}

    def reconcile_run(self, run_id: str) -> None:
        run = self.ledger.kaggle_run_internal(run_id)
        if run is None or run.state in _TERMINAL:
            return
        revision = self._revision_internal(run)
        timeout_seconds = int(revision.runtime.get("timeout_seconds", 1800)) + 600
        if self.clock() >= run.created_at + timedelta(seconds=timeout_seconds):
            with self._run_lease(run):
                self.ledger.transition_kaggle_run(
                    run_id=run.run_id,
                    expected_states={run.state.value},
                    new_state="FAILED",
                    provider_status=run.last_provider_status or "deadline_exceeded",
                    failure_summary="research run exceeded its bounded semantic deadline",
                    finished=True,
                )
            return
        if run.state is KaggleRunState.PREPARED:
            self._submit_prepared(run)
            return
        if run.state in {KaggleRunState.SUBMITTING, KaggleRunState.SUBMISSION_UNKNOWN}:
            self._reconcile_submission(run)
            return
        with self._run_lease(run):
            if not run.provider_run_ref:
                self._schedule(run, "provider identity pending")
                return
            identity = self._provider_run_identity(run)
            status = self.adapter.read_run_status(identity)
            if status.state is KernelState.FAILED:
                self.ledger.transition_kaggle_run(
                    run_id=run.run_id,
                    expected_states={run.state.value},
                    new_state="FAILED",
                    provider_status=status.provider_status,
                    failure_summary=(status.failure_message or status.provider_status)[:2000],
                    finished=True,
                )
                return
            if status.state is KernelState.COMPLETE:
                collecting = self.ledger.transition_kaggle_run(
                    run_id=run.run_id,
                    expected_states={run.state.value},
                    new_state="COLLECTING",
                    provider_status=status.provider_status,
                    next_poll_at=self.clock(),
                )
                try:
                    self._collect_outputs(collecting)
                except KaggleResearchError as exc:
                    self.ledger.transition_kaggle_run(
                        run_id=collecting.run_id,
                        expected_states={"COLLECTING"},
                        new_state="FAILED",
                        provider_status=status.provider_status,
                        failure_summary=str(exc)[:2000],
                        finished=True,
                    )
                except Exception:
                    self.ledger.transition_kaggle_run(
                        run_id=collecting.run_id,
                        expected_states={"COLLECTING"},
                        new_state="COLLECTING",
                        provider_status=status.provider_status,
                        failure_summary="exact output collection is pending provider readback",
                        next_poll_at=self._next_poll(collecting.poll_attempts),
                    )
                return
            target = "RUNNING" if status.state is KernelState.RUNNING else "QUEUED"
            self.ledger.transition_kaggle_run(
                run_id=run.run_id,
                expected_states={run.state.value},
                new_state=target,
                provider_status=status.provider_status,
                next_poll_at=self._next_poll(run.poll_attempts),
            )

    @contextmanager
    def _run_lease(self, run: KaggleRunRecord):  # type: ignore[no-untyped-def]
        lease_id = str(uuid4())
        holder_id = f"research-reconcile:{run.run_id}:{lease_id}"
        lease = self.ledger.acquire_resource_lease(
            lease_id=lease_id,
            resource_kind="kaggle_research",
            resource_ref=run.research_id,
            holder_id=holder_id,
            lease_until=self.clock() + timedelta(seconds=self.lease_seconds),
        )
        try:
            yield lease
        finally:
            self.ledger.release_resource_lease_exact(str(lease.lease_id), lease.holder_id, lease.epoch)

    def _submit_prepared(self, run: KaggleRunRecord) -> None:
        with self._run_lease(run):
            self._submit_prepared_unleased(run)

    def _reconcile_submission(self, run: KaggleRunRecord) -> None:
        with self._run_lease(run):
            self._reconcile_submission_unleased(run)

    def _submit_prepared_unleased(self, run: KaggleRunRecord) -> None:
        intent_payload = self.ledger.kaggle_provider_intent(run.run_id)
        if intent_payload is None:
            raise KaggleResearchError("run lacks a durable provider intent")
        intent = ProviderEffectIntent.model_validate(intent_payload)
        revision = self._revision_internal(run)
        materialized = self._materialize_source(self._research_internal(run), revision, run.run_id)
        self.adapter.assert_research_notebook_target_absent(intent.provider_ref)
        self.ledger.transition_kaggle_run(
            run_id=run.run_id,
            expected_states={"PREPARED"},
            new_state="SUBMITTING",
            next_poll_at=self._next_poll(0),
        )
        try:
            result = self.adapter.push_private_research_notebook(
                intent=intent,
                task_run_id=UUID(run.run_id),
                source=materialized,
                title=intent.provider_ref.split("/", 1)[1],
                code_file=revision.code_file,
                kernel_type=revision.kernel_type,
                language=revision.language,
                dataset_sources=[
                    f"{pin['provider_ref']}/{pin['provider_version']}"
                    if pin["attach_mode"] == "native_exact"
                    else str(pin["provider_ref"])
                    for pin in revision.inputs
                ],
                enable_internet=bool(revision.runtime.get("enable_internet", False)),
                accelerator=str(revision.runtime.get("accelerator", "none")),
                timeout_seconds=int(revision.runtime.get("timeout_seconds", 1800)),
            )
        except KaggleAmbiguousMutation:
            self.ledger.transition_kaggle_run(
                run_id=run.run_id,
                expected_states={"SUBMITTING"},
                new_state="SUBMISSION_UNKNOWN",
                failure_summary="submission response was lost; exact reconciliation is pending",
                next_poll_at=self._next_poll(0),
            )
            return
        self._record_submission(run.run_id, result.run, result.effect.outcome)

    def _reconcile_submission_unleased(self, run: KaggleRunRecord) -> None:
        intent_payload = self.ledger.kaggle_provider_intent(run.run_id)
        if intent_payload is None or not run.provider_source_sha256:
            raise KaggleResearchError("unknown submission lacks exact durable identity")
        intent = ProviderEffectIntent.model_validate(intent_payload)
        revision = self._revision_internal(run)
        result = self.adapter.reconcile_private_notebook_mutation(
            intent=intent,
            task_run_id=UUID(run.run_id),
            expected_source_sha256=run.provider_source_sha256,
            dataset_sources=[
                f"{pin['provider_ref']}/{pin['provider_version']}"
                if pin["attach_mode"] == "native_exact"
                else str(pin["provider_ref"])
                for pin in revision.inputs
            ],
            control_class=ControlClass.MCP_MANAGED,
            disposable=False,
            enable_internet=bool(revision.runtime.get("enable_internet", False)),
            accelerator=str(revision.runtime.get("accelerator", "none")),
        )
        if result is None:
            self._schedule(run, "exact submission not observed")
            return
        self._record_submission(run.run_id, result.run, result.effect.outcome)

    # Artifacts --------------------------------------------------------------

    def artifacts_list(self, arguments: Mapping[str, Any], principal: AccessIdentity) -> dict[str, Any]:
        run_id = str(arguments["run_id"])
        run = self.ledger.kaggle_run(owner_subject=principal.subject, run_id=run_id)
        if run is None:
            raise KaggleResearchError("research run was not found")
        artifacts = self.ledger.kaggle_artifacts(owner_subject=principal.subject, run_id=run_id)
        compact = self._compact_outputs(run, artifacts) if run.state is KaggleRunState.SUCCEEDED else {}
        return {
            "run_id": run_id,
            "semantic_status": run.state.value,
            "manifest_sha256": run.output_manifest_sha256,
            "artifacts": [self._artifact_public(item) for item in artifacts],
            "compact_outputs": compact,
            "complete": run.state is KaggleRunState.SUCCEEDED,
            "allowed_next_actions": ["artifacts.read", "notebooks.save"],
        }

    def artifacts_read(self, arguments: Mapping[str, Any], principal: AccessIdentity) -> dict[str, Any]:
        artifact = self.ledger.kaggle_artifact(
            owner_subject=principal.subject, artifact_id=str(arguments["artifact_id"])
        )
        if artifact is None:
            raise KaggleResearchError("research artifact was not found")
        run = self.ledger.kaggle_run(owner_subject=principal.subject, run_id=artifact.run_id)
        if run is None:
            raise KaggleResearchError("research run was not found")
        content = self._artifact_bytes(artifact, run)
        if len(content) != artifact.byte_size or hashlib.sha256(content).hexdigest() != artifact.sha256:
            raise KaggleResearchError("artifact final SHA-256 verification failed")
        offset = int(arguments.get("offset", 0))
        max_bytes = int(arguments.get("max_bytes", _MAX_CHUNK_BYTES))
        if offset > len(content) or not 1 <= max_bytes <= _MAX_CHUNK_BYTES:
            raise KaggleResearchError("artifact chunk bounds are invalid")
        chunk = content[offset : offset + max_bytes]
        next_offset = offset + len(chunk)
        complete = next_offset == len(content)
        return {
            **self._artifact_public(artifact),
            "encoding": "base64",
            "offset": offset,
            "content_base64": base64.b64encode(chunk).decode("ascii"),
            "content_byte_size": len(chunk),
            "content_sha256": hashlib.sha256(chunk).hexdigest(),
            "next_offset": None if complete else next_offset,
            "complete": complete,
            "bounded": True,
            "file_sha256_verified": True,
            "continuation": (
                None
                if complete
                else {"tool": "artifacts.read", "artifact_id": artifact.artifact_id, "offset": next_offset}
            ),
            "allowed_next_actions": ["artifacts.read"] if not complete else ["artifacts.list"],
        }

    # Internal helpers -------------------------------------------------------

    def _prepare_run(
        self,
        principal: AccessIdentity,
        research: KaggleResearchRecord,
        revision: KaggleRevisionRecord,
        *,
        retry_of: str | None,
    ) -> tuple[KaggleRunRecord, bool]:
        for pin in revision.inputs:
            current = self._inspect_dataset(
                provider_ref=str(pin["provider_ref"]), provider_version=int(pin["provider_version"])
            )
            if current.files_manifest_sha256 != pin["files_manifest_sha256"]:
                raise KaggleResearchError("DATASET_VERSION_CHANGED")
        attempt = len(self.ledger.kaggle_runs(owner_subject=principal.subject, research_id=research.research_id)) + 1
        run_id = str(uuid4())
        provider_ref = self._provider_notebook_ref(research.research_id, revision.revision_no, attempt)
        materialized = self._materialize_source(research, revision, run_id)
        provider_source_sha = executable_source_sha256(materialized, kernel_type=revision.kernel_type)
        operation_id, effect_id = uuid4(), uuid4()
        sources = tuple(
            f"{pin['provider_ref']}/{pin['provider_version']}"
            if pin["attach_mode"] == "native_exact"
            else str(pin["provider_ref"])
            for pin in revision.inputs
        )
        arguments: dict[str, Any] = {
            "task_run_id": run_id,
            "source_sha256": provider_source_sha,
            "dataset_sources": sources,
            "control_class": ControlClass.MCP_MANAGED.value,
            "disposable": False,
        }
        if revision.runtime.get("enable_internet"):
            arguments["enable_internet"] = True
        if revision.runtime.get("accelerator") != "none":
            arguments["accelerator"] = str(revision.runtime["accelerator"])
        intent = ProviderEffectIntent.create(
            operation_id=operation_id,
            effect_id=effect_id,
            idempotency_key=f"kaggle-research:{research.research_id}:{run_id}",
            task_id=UUID(run_id),
            action=MutationAction.PUSH_NOTEBOOK,
            provider_ref=provider_ref,
            expected_fingerprint=None,
            arguments=arguments,
            requested_at=self.clock(),
        )
        lease_id = str(uuid4())
        holder_id = f"research-submit:{run_id}"
        run, lease, created = self.ledger.prepare_kaggle_run(
            owner_subject=principal.subject,
            client_id=principal.client_id,
            research_id=research.research_id,
            revision_id=revision.revision_id,
            run_id=run_id,
            retry_of_run_id=retry_of,
            operation_id=str(operation_id),
            effect_id=str(effect_id),
            provider_intent=intent.model_dump(mode="json"),
            provider_source_sha256=provider_source_sha,
            lease_id=lease_id,
            holder_id=holder_id,
            lease_until=self.clock() + timedelta(seconds=self.lease_seconds),
        )
        if lease is not None:
            self.ledger.release_resource_lease_exact(str(lease.lease_id), lease.holder_id, lease.epoch)
        return run, created

    def _record_submission(
        self, run_id: str, identity: KaggleKernelRunIdentity, outcome: EffectOutcome
    ) -> None:
        current = self.ledger.kaggle_run_internal(run_id)
        if current is None:
            raise KaggleResearchError("research run disappeared")
        intent_payload = self.ledger.kaggle_provider_intent(run_id)
        if intent_payload is None:
            raise KaggleResearchError("research run lost its exact provider intent")
        expected_ref = str(intent_payload["provider_ref"])
        if (
            identity.task_run_id != UUID(run_id)
            or identity.provider_ref != expected_ref
            or identity.source_version != 1
            or identity.source_sha256 != current.provider_source_sha256
        ):
            raise KaggleResearchError("provider submission identity differs from the durable run intent")
        self.ledger.transition_kaggle_run(
            run_id=run_id,
            expected_states={"SUBMITTING", "SUBMISSION_UNKNOWN"},
            new_state="QUEUED",
            provider_run_ref=identity.provider_run_ref,
            provider_kernel_id=str(identity.provider_kernel_id),
            provider_source_version=identity.source_version,
            provider_status="submitted" if outcome is EffectOutcome.APPLIED else "reconciled",
            failure_summary=None,
            next_poll_at=self.clock(),
        )

    def _schedule(self, run: KaggleRunRecord, summary: str) -> None:
        self.ledger.transition_kaggle_run(
            run_id=run.run_id,
            expected_states={run.state.value},
            new_state="SUBMISSION_UNKNOWN" if run.state is KaggleRunState.SUBMITTING else run.state.value,
            provider_status="submission_not_observed",
            failure_summary=summary[:2000],
            next_poll_at=self._next_poll(run.poll_attempts),
        )

    def _next_poll(self, attempts: int) -> datetime:
        return self.clock() + timedelta(seconds=min(300, 5 * (2 ** min(attempts, 6))))

    def _collect_outputs(self, run: KaggleRunRecord) -> None:
        identity = self._provider_run_identity(run)
        manifest_bytes = self._download_output(identity, "research-output-manifest.json", 262_144)
        manifest_sha = hashlib.sha256(manifest_bytes).hexdigest()
        try:
            manifest = json.loads(manifest_bytes)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise KaggleResearchError("research output manifest is invalid UTF-8 JSON") from exc
        revision = self._revision_internal(run)
        expected_binding = {
            "schema_version": "my-data-hub-research-output.v1",
            "research_id": run.research_id,
            "run_id": run.run_id,
            "revision_id": run.revision_id,
            "source_sha256": revision.source_sha256,
            "provider_source_sha256": run.provider_source_sha256,
            "inputs_sha256": revision.inputs_sha256,
        }
        if not isinstance(manifest, dict) or any(manifest.get(key) != value for key, value in expected_binding.items()):
            raise KaggleResearchError("research output manifest binding differs from the frozen run")
        declared = manifest.get("artifacts")
        if not isinstance(declared, list) or not 4 <= len(declared) <= 32:
            raise KaggleResearchError("research output manifest artifact count is invalid")
        artifacts: list[dict[str, Any]] = [
            self._artifact_metadata(
                run.run_id,
                path="research-output-manifest.json",
                role="manifest",
                media_type="application/json",
                content=manifest_bytes,
                storage_mode="kaggle",
            )
        ]
        paths: set[str] = set()
        total = len(manifest_bytes)
        for item in declared:
            if not isinstance(item, dict) or set(item) != {"path", "role", "media_type", "byte_size", "sha256"}:
                raise KaggleResearchError("research output manifest contains a non-closed artifact declaration")
            path = _safe_path(str(item["path"]))
            if Path(path).name != path or path in paths or path in {"research-output-manifest.json", "provenance.json"}:
                raise KaggleResearchError("research output artifact path is duplicate, nested, or reserved")
            paths.add(path)
            size = item["byte_size"]
            digest = str(item["sha256"])
            media_type = str(item["media_type"])
            role = str(item["role"])
            if (
                isinstance(size, bool)
                or not isinstance(size, int)
                or not 0 <= size <= _MAX_OUTPUT_FILE_BYTES
                or not _SHA256.fullmatch(digest)
                or not re.fullmatch(r"[A-Za-z0-9.+-]+/[A-Za-z0-9.+-]+", media_type)
                or role not in {"summary", "metrics", "diagnostics", "log", "table", "figure", "other"}
            ):
                raise KaggleResearchError("research output artifact metadata is invalid")
            standard = _STANDARD_MEDIA.get(path)
            if standard and standard != (role, media_type):
                raise KaggleResearchError("standard research output media type or role is invalid")
            content = self._download_output(identity, path, max(1, size))
            if len(content) != size or hashlib.sha256(content).hexdigest() != digest:
                raise KaggleResearchError("research output artifact size/SHA-256 differs from its manifest")
            if path.endswith(".json"):
                try:
                    json.loads(content)
                except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                    raise KaggleResearchError("declared JSON research output is invalid") from exc
            total += size
            if total > _MAX_OUTPUT_TOTAL_BYTES:
                raise KaggleResearchError("research output total exceeds the bounded contract")
            artifacts.append(
                self._artifact_metadata(
                    run.run_id,
                    path=path,
                    role=role,
                    media_type=media_type,
                    content=content,
                    storage_mode="kaggle",
                )
            )
        if not paths >= _PROVIDER_REQUIRED:
            raise KaggleResearchError("research run omitted a required standard output")
        provenance = self._provenance(run, revision, manifest_sha, artifacts)
        provenance_bytes = canonical_json_bytes(provenance)
        cache_relpath = f"{run.run_id}/provenance.json"
        self._write_cache(cache_relpath, provenance_bytes)
        artifacts.append(
            self._artifact_metadata(
                run.run_id,
                path="provenance.json",
                role="provenance",
                media_type="application/json",
                content=provenance_bytes,
                storage_mode="local_cache",
                cache_relpath=cache_relpath,
            )
        )
        self.ledger.transition_kaggle_run(
            run_id=run.run_id,
            expected_states={"COLLECTING"},
            new_state="SUCCEEDED",
            provider_status="complete",
            output_manifest_sha256=manifest_sha,
            finished=True,
            artifacts=artifacts,
        )

    def _download_output(self, run: KaggleKernelRunIdentity, path: str, max_bytes: int) -> bytes:
        with tempfile.TemporaryDirectory(prefix="my-data-hub-research-output-") as temporary:
            destination = Path(temporary) / "exact"
            self.adapter.download_exact_run_output_file(
                run, destination=destination, file_name=path, max_bytes=max_bytes
            )
            target = destination / path
            if not target.is_file() or target.is_symlink():
                raise KaggleResearchError("exact research output file is missing")
            return target.read_bytes()

    def _artifact_bytes(self, artifact: KaggleArtifactRecord, run: KaggleRunRecord) -> bytes:
        if artifact.storage_mode == "local_cache":
            if not artifact.cache_relpath:
                raise KaggleResearchError("cached artifact path is missing")
            target = self.cache_root.joinpath(*artifact.cache_relpath.split("/"))
            if self.cache_root not in target.resolve().parents or not target.is_file() or target.is_symlink():
                raise KaggleResearchError("cached artifact path is unavailable or unsafe")
            return target.read_bytes()
        return self._download_output(self._provider_run_identity(run), artifact.path, max(1, artifact.byte_size))

    def _compact_outputs(
        self, run: KaggleRunRecord, artifacts: list[KaggleArtifactRecord]
    ) -> dict[str, Any]:
        projected: dict[str, Any] = {}
        by_path = {item.path: item for item in artifacts}
        for path in ("research-output-manifest.json", "summary.md", "metrics.json", "provenance.json"):
            artifact = by_path.get(path)
            if artifact is None:
                continue
            if artifact.byte_size > _MAX_CHUNK_BYTES:
                projected[path] = {
                    "available": True,
                    "byte_size": artifact.byte_size,
                    "sha256": artifact.sha256,
                    "requires_artifacts_read": True,
                }
                continue
            content = self._artifact_bytes(artifact, run)
            if len(content) != artifact.byte_size or hashlib.sha256(content).hexdigest() != artifact.sha256:
                raise KaggleResearchError("compact artifact SHA-256 verification failed")
            if path.endswith(".json"):
                projected[path] = json.loads(content)
            else:
                projected[path] = content.decode("utf-8")
        return projected

    def _write_cache(self, relative: str, content: bytes) -> None:
        _safe_path(relative)
        target = self.cache_root.joinpath(*relative.split("/"))
        target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(target.parent, 0o700)
        temporary = target.with_suffix(target.suffix + ".part")
        with temporary.open("wb") as handle:
            os.chmod(temporary, 0o600)
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        temporary.replace(target)
        os.chmod(target, 0o600)

    @staticmethod
    def _artifact_metadata(
        run_id: str,
        *,
        path: str,
        role: str,
        media_type: str,
        content: bytes,
        storage_mode: str,
        cache_relpath: str | None = None,
    ) -> dict[str, Any]:
        return {
            "artifact_id": str(uuid4()),
            "run_id": run_id,
            "path": path,
            "role": role,
            "media_type": media_type,
            "byte_size": len(content),
            "sha256": hashlib.sha256(content).hexdigest(),
            "storage_mode": storage_mode,
            "cache_relpath": cache_relpath,
        }

    def _provenance(
        self,
        run: KaggleRunRecord,
        revision: KaggleRevisionRecord,
        manifest_sha: str,
        artifacts: list[dict[str, Any]],
    ) -> dict[str, Any]:
        return {
            "schema_version": "my-data-hub-research-provenance.v1",
            "research_id": run.research_id,
            "run_id": run.run_id,
            "revision_id": revision.revision_id,
            "revision_no": revision.revision_no,
            "source_sha256": revision.source_sha256,
            "provider_source_sha256": run.provider_source_sha256,
            "provider_source_version": run.provider_source_version,
            "inputs": revision.inputs,
            "inputs_sha256": revision.inputs_sha256,
            "files_manifest_sha256": [item["files_manifest_sha256"] for item in revision.inputs],
            "runtime_options": revision.runtime,
            "provider_run_identity": run.provider_run_ref,
            "created_at": _public_time(run.created_at),
            "started_at": _public_time(run.started_at),
            "finished_at": _public_time(self.clock()),
            "output_manifest_sha256": manifest_sha,
            "artifact_hashes": {item["path"]: item["sha256"] for item in artifacts},
        }

    def _materialize_source(
        self, research: KaggleResearchRecord, revision: KaggleRevisionRecord, run_id: str
    ) -> bytes:
        context = {
            "research_id": research.research_id,
            "run_id": run_id,
            "revision_id": revision.revision_id,
            "revision_no": revision.revision_no,
            "source_sha256": revision.source_sha256,
            "inputs_sha256": revision.inputs_sha256,
            "inputs": revision.inputs,
        }
        assignment = "MY_DATA_HUB_RESEARCH_CONTEXT = " + repr(context) + "\n"
        if revision.kernel_type == "script":
            return assignment.encode() + revision.source_utf8.encode()
        try:
            notebook = json.loads(revision.source_utf8)
        except json.JSONDecodeError as exc:
            raise KaggleResearchError("Notebook revision is invalid JSON") from exc
        cells = notebook.get("cells") if isinstance(notebook, dict) else None
        if not isinstance(cells, list):
            raise KaggleResearchError("Notebook revision lacks cells")
        cells.insert(
            0,
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [assignment],
            },
        )
        return json.dumps(notebook, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()

    def _pin(self, inspection: KaggleDatasetInspection) -> dict[str, Any]:
        return {
            "provider_ref": inspection.provider_ref,
            "provider_version": inspection.provider_version,
            "visibility": inspection.visibility,
            "license": inspection.license,
            "terms_acceptance_required": inspection.terms_acceptance_required,
            "files": [item.model_dump(mode="json") for item in inspection.files],
            "files_manifest_sha256": inspection.files_manifest_sha256,
            "attach_mode": inspection.attach_mode,
        }

    def _inspect_dataset(
        self, *, provider_ref: str, provider_version: int | None = None
    ) -> KaggleDatasetInspection:
        self._assert_not_protected(provider_ref, resource_kind="Dataset")
        try:
            inspection = self.adapter.inspect_dataset(
                provider_ref=provider_ref, provider_version=provider_version
            )
        except KagglePolicyError as exc:
            if str(exc) == "TERMS_ACCEPTANCE_REQUIRED":
                raise KaggleResearchError(
                    "TERMS_ACCEPTANCE_REQUIRED", code="TERMS_ACCEPTANCE_REQUIRED"
                ) from exc
            raise
        if inspection.terms_acceptance_required:
            raise KaggleResearchError("TERMS_ACCEPTANCE_REQUIRED")
        return inspection

    def _assert_not_protected(self, provider_ref: str, *, resource_kind: str) -> None:
        projection = self.ledger.latest_provider_resource(provider_ref)
        if projection is not None and projection["control_class"] == "orchestrator_protected":
            raise KaggleResearchError(f"orchestrator-protected {resource_kind}s are status-only")

    @staticmethod
    def _runtime(value: object) -> dict[str, Any]:
        raw = dict(value) if isinstance(value, Mapping) else {}
        result = {
            "accelerator": str(raw.get("accelerator", "none")),
            "enable_internet": bool(raw.get("enable_internet", False)),
            "timeout_seconds": int(raw.get("timeout_seconds", 1800)),
        }
        if result["accelerator"] not in {"none", "gpu"} or not 60 <= result["timeout_seconds"] <= 3600:
            raise KaggleResearchError("Notebook runtime options are invalid")
        if result["enable_internet"]:
            raise KaggleResearchError("research Notebook internet is disabled when Dataset inputs are attached")
        return result

    @staticmethod
    def _validate_source(source: str, kernel_type: str) -> None:
        encoded = source.encode()
        if not encoded or len(encoded) > _MAX_SOURCE_BYTES:
            raise KaggleResearchError("Notebook source is empty or exceeds 262144 UTF-8 bytes")
        if kernel_type not in {"script", "notebook"}:
            raise KaggleResearchError("Notebook kernel_type is invalid")
        if _SECRET_SOURCE.search(source):
            raise KaggleResearchError("Notebook source appears to contain credentials or a private key")
        executable_source_sha256(encoded, kernel_type=kernel_type)

    def _research(self, arguments: Mapping[str, Any], principal: AccessIdentity) -> KaggleResearchRecord:
        research = self.ledger.kaggle_research(
            owner_subject=principal.subject,
            research_id=str(arguments["research_id"]) if arguments.get("research_id") else None,
            alias=str(arguments["alias"]) if arguments.get("alias") else None,
            dataset_ref=str(arguments["dataset_ref"]) if arguments.get("dataset_ref") else None,
        )
        if research is None:
            raise KaggleResearchError("research was not found")
        return research

    @staticmethod
    def _has_selector(arguments: Mapping[str, Any]) -> bool:
        return sum(arguments.get(name) is not None for name in ("research_id", "alias", "dataset_ref")) == 1

    def _revision(
        self,
        arguments: Mapping[str, Any],
        principal: AccessIdentity,
        research: KaggleResearchRecord,
        *,
        current_default: bool = False,
    ) -> KaggleRevisionRecord:
        revision_id = arguments.get("revision_id")
        revision_no = arguments.get("revision_no")
        if revision_id is None and revision_no is None and current_default:
            revision_id = research.current_revision_id
        revision = self.ledger.kaggle_revision(
            owner_subject=principal.subject,
            research_id=research.research_id,
            revision_id=str(revision_id) if revision_id is not None else None,
            revision_no=int(revision_no) if revision_no is not None else None,
        )
        if revision is None:
            raise KaggleResearchError("research revision was not found")
        return revision

    def _revision_internal(self, run: KaggleRunRecord) -> KaggleRevisionRecord:
        research = self._research_internal(run)
        revision = self.ledger.kaggle_revision(
            owner_subject=research.owner_subject,
            research_id=run.research_id,
            revision_id=run.revision_id,
        )
        if revision is None:
            raise KaggleResearchError("run revision is unavailable")
        return revision

    def _research_internal(self, run: KaggleRunRecord) -> KaggleResearchRecord:
        # The run is already an internal ledger row; obtain its owner through a bounded owner scan.
        with self.ledger._reader() as connection:
            row = connection.execute(
                "SELECT owner_subject FROM kaggle_researches WHERE research_id=?", (run.research_id,)
            ).fetchone()
        if row is None:
            raise KaggleResearchError("run research is unavailable")
        research = self.ledger.kaggle_research(owner_subject=str(row["owner_subject"]), research_id=run.research_id)
        if research is None:
            raise KaggleResearchError("run research is unavailable")
        return research

    def _provider_run_identity(self, run: KaggleRunRecord) -> KaggleKernelRunIdentity:
        if not run.provider_run_ref or not run.provider_source_version or not run.provider_source_sha256:
            raise KaggleResearchError("exact provider run identity is incomplete")
        provider_ref = run.provider_run_ref.rsplit("/", 1)[0]
        return KaggleKernelRunIdentity(
            task_run_id=UUID(run.run_id),
            provider_ref=provider_ref,
            source_version=run.provider_source_version,
            source_sha256=run.provider_source_sha256,
            provider_kernel_id=int(run.provider_kernel_id or 0),
            provider_run_ref=run.provider_run_ref,
            started_at=run.started_at or run.created_at,
        )

    def _provider_notebook_ref(self, research_id: str, revision_no: int, attempt: int) -> str:
        slug = f"mdh-r-{research_id.replace('-', '')[:12]}-v{revision_no}-a{attempt}"
        return f"{self.adapter.provider_identity().username}/{slug}"

    def _research_response(
        self,
        research: KaggleResearchRecord,
        *,
        include_history: bool = False,
        revision_cursor: int = 0,
        run_cursor: int = 0,
        history_limit: int = 20,
    ) -> dict[str, Any]:
        result: dict[str, Any] = {
            "research_id": research.research_id,
            "alias": research.alias,
            "title": research.title,
            "goal": research.goal,
            "state": research.state.value,
            "primary_dataset_ref": research.primary_dataset_ref,
            "notebook_ref": research.notebook_ref,
            "current_revision_id": research.current_revision_id,
            "active_run_id": research.active_run_id,
            "last_completed_run_id": research.last_completed_run_id,
            "created_at": _public_time(research.created_at),
            "updated_at": _public_time(research.updated_at),
            "allowed_next_actions": self._research_actions(research),
        }
        if include_history:
            revisions = self.ledger.kaggle_revisions(
                owner_subject=research.owner_subject,
                research_id=research.research_id,
                cursor=revision_cursor,
                limit=history_limit + 1,
            )
            runs = self.ledger.kaggle_runs(
                owner_subject=research.owner_subject,
                research_id=research.research_id,
                cursor=run_cursor,
                limit=history_limit + 1,
            )
            more_revisions = len(revisions) > history_limit
            more_runs = len(runs) > history_limit
            result["revisions"] = [
                self._revision_public(item, include_source=False)
                for item in revisions[:history_limit]
            ]
            result["runs"] = [
                self._run_public(item)
                for item in runs[:history_limit]
            ]
            result["history_complete"] = not more_revisions and not more_runs
            result["history_continuation"] = (
                {
                    "tool": "research.get",
                    "research_id": research.research_id,
                    "revision_cursor": _encode_offset_cursor(
                        "research-revisions",
                        revision_cursor + (history_limit if more_revisions else len(revisions)),
                        research.research_id,
                    ),
                    "run_cursor": _encode_offset_cursor(
                        "research-runs",
                        run_cursor + (history_limit if more_runs else len(runs)),
                        research.research_id,
                    ),
                    "history_limit": history_limit,
                }
                if more_revisions or more_runs
                else None
            )
        return result

    def _run_response(
        self,
        run: KaggleRunRecord,
        principal: AccessIdentity,
        *,
        revision: KaggleRevisionRecord,
    ) -> dict[str, Any]:
        result = {
            **self._run_public(run),
            "revision": self._revision_public(revision, include_source=False),
            "pins": [
                {key: value for key, value in pin.items() if key != "files"}
                | {"file_count": len(pin.get("files", []))}
                for pin in revision.inputs
            ],
            "allowed_next_actions": self._run_actions(run),
        }
        if run.state is KaggleRunState.FAILED and run.provider_run_ref:
            try:
                logs = self.runs_logs({"run_id": run.run_id, "offset": 0, "max_bytes": 16_384}, principal)
                result["logs"] = logs
            except Exception:
                result["logs"] = {"available": False, "bounded": True}
        return result

    @staticmethod
    def _run_public(run: KaggleRunRecord) -> dict[str, Any]:
        return {
            "run_id": run.run_id,
            "research_id": run.research_id,
            "revision_id": run.revision_id,
            "attempt_no": run.attempt_no,
            "retry_of_run_id": run.retry_of_run_id,
            "semantic_status": run.state.value,
            "provider_status": run.last_provider_status,
            "provider_run_identity": run.provider_run_ref,
            "provider_source_version": run.provider_source_version,
            "failure_summary": run.failure_summary,
            "manifest_sha256": run.output_manifest_sha256,
            "created_at": _public_time(run.created_at),
            "started_at": _public_time(run.started_at),
            "finished_at": _public_time(run.finished_at),
        }

    @staticmethod
    def _revision_public(revision: KaggleRevisionRecord, *, include_source: bool) -> dict[str, Any]:
        return {
            "revision_id": revision.revision_id,
            "revision_no": revision.revision_no,
            "parent_revision_id": revision.parent_revision_id,
            "state": revision.state.value,
            "code_file": revision.code_file,
            "kernel_type": revision.kernel_type,
            "language": revision.language,
            **({"source_utf8": revision.source_utf8} if include_source else {}),
            "source_sha256": revision.source_sha256,
            "runtime": revision.runtime,
            "inputs": [
                {
                    key: value
                    for key, value in pin.items()
                    if key != "files"
                }
                | {"file_count": len(pin.get("files", []))}
                for pin in revision.inputs
            ],
            "inputs_sha256": revision.inputs_sha256,
            "provider_source_version": revision.provider_source_version,
            "created_at": _public_time(revision.created_at),
            "frozen_at": _public_time(revision.frozen_at),
        }

    @staticmethod
    def _artifact_public(artifact: KaggleArtifactRecord) -> dict[str, Any]:
        return {
            "artifact_id": artifact.artifact_id,
            "path": artifact.path,
            "role": artifact.role,
            "media_type": artifact.media_type,
            "byte_size": artifact.byte_size,
            "sha256": artifact.sha256,
        }

    @staticmethod
    def _research_actions(research: KaggleResearchRecord) -> list[str]:
        if research.state.value == "RUNNING":
            return ["runs.get", "runs.logs", "research.get"]
        if research.state.value in {"REVIEW_REQUIRED", "COMPLETED"}:
            return ["notebooks.save", "artifacts.list", "research.get"]
        return ["notebooks.get", "notebooks.save", "notebooks.inputs.set", "runs.start"]

    @staticmethod
    def _revision_actions(revision: KaggleRevisionRecord) -> list[str]:
        return ["notebooks.inputs.set", "runs.start"] if revision.state.value == "DRAFT" else ["runs.get"]

    @staticmethod
    def _run_actions(run: KaggleRunRecord) -> list[str]:
        if run.state is KaggleRunState.FAILED:
            return ["runs.logs", "runs.retry", "notebooks.save", "runs.get"]
        if run.state is KaggleRunState.SUCCEEDED:
            return ["artifacts.list", "notebooks.save", "runs.get"]
        return ["runs.get", "runs.logs"]
