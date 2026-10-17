"""Opt-in operational delivery of immutable voice-analysis candidates.

No transcript, canonical project data, credentials or model calls are stored in
this outbox. A candidate is not permission to perform the dictated actions.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import stat
from contextlib import suppress
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlsplit

import httpx

from my_data_hub.voice_intake.errors import VoiceIntakeError

from .markdown import render_publication
from .store import PublicationProjection, VoiceIntakeV2Store

LOGGER = logging.getLogger(__name__)
TRIGGER = re.compile(r"(?<!\w)задай\s+мне\s+вопросы(?!\w)", re.IGNORECASE)


@dataclass(frozen=True)
class AnalysisSettings:
    repository: str
    owner_id: str = ""
    enabled: bool = False
    endpoint: str = ""
    token: str = field(default="", repr=False)

    @classmethod
    def from_env(cls, repository: str) -> AnalysisSettings:
        config_path = os.getenv("MY_DATA_HUB_VOICE_ANALYSIS_CONFIG_FILE", "").strip()
        if config_path:
            try:
                descriptor = os.open(config_path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
                try:
                    metadata = os.fstat(descriptor)
                    if (not stat.S_ISREG(metadata.st_mode) or metadata.st_mode & 0o077
                            or metadata.st_size > 32768):
                        raise ValueError("private_config_required")
                    value = json.loads(os.read(descriptor, 32769))
                finally:
                    os.close(descriptor)
                if (not isinstance(value, dict) or set(value) != {"enabled", "owner_id", "endpoint", "token"}
                        or not isinstance(value["enabled"], bool)
                        or any(not isinstance(value[k], str) for k in ("owner_id", "endpoint", "token"))):
                    raise ValueError("invalid_config")
                return cls(repository=repository, **value)
            except (OSError, ValueError, TypeError):
                # Add-on configuration must never take recording/ASR down.
                LOGGER.error("voice_analysis_configuration_unavailable")
                return cls(repository=repository)
        return cls(
            repository=repository,
            owner_id=os.getenv("MY_DATA_HUB_VOICE_ANALYSIS_OWNER_ID", "").strip(),
            enabled=os.getenv("MY_DATA_HUB_VOICE_ANALYSIS_ENABLED", "false").lower() == "true",
            endpoint=os.getenv("MY_DATA_HUB_VOICE_ANALYSIS_ENDPOINT", "").strip(),
            token=os.getenv("MY_DATA_HUB_VOICE_ANALYSIS_TOKEN", "").strip(),
        )

    @property
    def delivery_ready(self) -> bool:
        try:
            url = urlsplit(self.endpoint)
        except ValueError:
            return False
        return bool(self.enabled and self.owner_id and self.token and url.scheme == "https"
                    and url.hostname and not url.username and not url.password
                    and not url.query and not url.fragment)


def build_intent(
    projection: PublicationProjection, commit_sha: str, settings: AnalysisSettings,
) -> dict[str, Any] | None:
    if not settings.enabled or not settings.owner_id:
        return None
    transcript = str(projection.transcript["transcript"]).strip()
    match = TRIGGER.search(transcript)
    if match is None:
        return None
    path = render_publication(projection).source_path
    source = {"repository": settings.repository, "path": path, "commit_sha": commit_sha,
              "transcript_sha256": hashlib.sha256(transcript.encode()).hexdigest()}
    identity = json.dumps(source, sort_keys=True, separators=(",", ":"))
    return {
        "intent_id": "voice:" + hashlib.sha256(identity.encode()).hexdigest(),
        "owner_id": settings.owner_id, "session_id": projection.session_id,
        "source": source, "input_manifest": [source],
        "trigger": {"phrase": match.group(), "start": match.start(), "end": match.end()},
    }


class AnalysisDelivery:
    """Independent bounded sender. Lost responses safely replay the same ID."""
    def __init__(self, store: VoiceIntakeV2Store, settings: AnalysisSettings, publisher: Any = None) -> None:
        self.store = store
        self.settings = settings
        self.publisher = publisher
        self.stop_event = asyncio.Event()

    async def process_once(self, client: httpx.AsyncClient) -> bool:
        if not self.settings.delivery_ready:
            return False
        row = self.store.claim_analysis_intent()
        if row is None:
            return False
        payload = json.loads(row["payload_json"])
        # Configuration changes must not deliver another owner's queued input.
        if payload["owner_id"] != self.settings.owner_id or payload["source"]["repository"] != self.settings.repository:
            self.store.finish_analysis_delivery(
                row["intent_id"], error="binding_changed", terminal=True, attempt=row["attempts"]
            )
            return True
        try:
            response = await client.post(
                self.settings.endpoint, json=payload,
                headers={"Authorization": f"Bearer {self.settings.token}"}, timeout=10,
                follow_redirects=False,
            )
            if response.status_code == 200:
                receipt = response.json()
                if (receipt.get("intent_id") == payload["intent_id"]
                        and receipt.get("source") == payload["source"]
                        and receipt.get("state") == "accepted" and receipt.get("request_id")
                        and receipt.get("event_id")):
                    self.store.finish_analysis_delivery(row["intent_id"], receipt=receipt, attempt=row["attempts"])
                    return True
                error = "receipt_mismatch"
                terminal = True
            else:
                error = f"http_{response.status_code}"
                terminal = response.status_code in {400, 401, 403, 404, 409, 422} or 300 <= response.status_code < 400
        except (httpx.HTTPError, ValueError, AttributeError):
            error, terminal = "transport_or_receipt_unknown", False
        self.store.finish_analysis_delivery(row["intent_id"], error=error, terminal=terminal, attempt=row["attempts"])
        return True

    async def project_once(self, client: httpx.AsyncClient) -> bool:
        if not self.settings.delivery_ready or not hasattr(self.publisher, "project_analysis"):
            return False
        row = self.store.claim_analysis_projection()
        if row is None:
            return False
        payload = json.loads(row["payload_json"])
        receipt = json.loads(row["receipt_json"])
        error = None
        reconciliation_required = False
        try:
            if (payload["owner_id"] != self.settings.owner_id
                    or payload["source"]["repository"] != self.settings.repository):
                raise ValueError("binding_changed")
            response = await client.get(
                self.settings.endpoint.rstrip("/") + "/" + row["intent_id"],
                headers={"Authorization": f"Bearer {self.settings.token}"}, timeout=10,
                follow_redirects=False,
            )
            response.raise_for_status()
            value = response.json()
            if value.get("intent_id") != row["intent_id"] or value.get("source") != payload["source"]:
                raise ValueError("projection_source_mismatch")
            if value.get("needs_redrive") is True:
                self.store.finish_analysis_delivery(
                    row["intent_id"], error="event_redrive", attempt=row["attempts"]
                )
                return True
            projection = value.get("projection")
            if projection and projection["revision"] != receipt.get("projection_revision"):
                markdown = projection["markdown"]
                if hashlib.sha256(markdown.encode()).hexdigest() != projection["revision"]:
                    raise ValueError("projection_revision_mismatch")
                commit = await self.publisher.project_analysis(
                    intent_id=row["intent_id"], source=payload["source"],
                    revision=projection["revision"], markdown=markdown,
                    expected_previous_revision=receipt.get("projection_revision"),
                )
                receipt.update(projection_revision=projection["revision"], projection_commit=commit)
        except Exception as exc:
            # No note text, token, remote response or exception detail in logs.
            error = exc.code if isinstance(exc, VoiceIntakeError) else "projection_" + type(exc).__name__
            reconciliation_required = error in {
                "analysis_projection_revision_conflict", "analysis_projection_manually_changed",
                "analysis_source_changed", "analysis_source_structure_changed", "analysis_source_hash_mismatch",
                "analysis_projection_block_missing", "analysis_projection_markers_conflict",
            }
        self.store.finish_analysis_projection(
            row["intent_id"], receipt=receipt, error=error, attempt=row["attempts"],
            reconciliation_required=reconciliation_required,
        )
        return True

    async def run(self) -> None:
        async with httpx.AsyncClient() as client:
            while not self.stop_event.is_set():
                try:
                    await self.process_once(client)
                    await self.project_once(client)
                except Exception as exc:
                    LOGGER.error("voice_analysis_delivery_failure type=%s", type(exc).__name__)
                with suppress(TimeoutError):
                    await asyncio.wait_for(self.stop_event.wait(), timeout=5)


def main() -> None:
    """Local operator-only recovery; not an endpoint exposed to recording devices."""
    import argparse
    from pathlib import Path
    parser = argparse.ArgumentParser(description="Redrive one blocked voice analysis transport intent")
    parser.add_argument("--spool", type=Path, required=True)
    parser.add_argument("--intent-id", required=True)
    parser.add_argument("--status", action="store_true", help="Read operational state without redriving")
    args = parser.parse_args()
    config = AnalysisSettings.from_env(os.getenv("MY_DATA_HUB_VOICE_GITHUB_REPOSITORY", ""))
    if not (args.spool / "voice-intake-v2.sqlite3").is_file():
        parser.error("existing private spool required")
    store = VoiceIntakeV2Store(args.spool)
    if args.status:
        print(json.dumps(store.analysis_status(args.intent_id)))
        return
    if not config.delivery_ready or not config.repository:
        parser.error("trusted configured delivery binding required for redrive")
    changed = store.redrive_analysis_intent(
        args.intent_id, owner_id=config.owner_id, repository=config.repository,
    )
    print(json.dumps({"redriven": changed}))


if __name__ == "__main__":
    main()
