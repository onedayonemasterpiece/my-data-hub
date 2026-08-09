from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass, field
from typing import Any
from urllib.parse import urlsplit, urlunsplit
from uuid import NAMESPACE_URL, UUID, uuid5

from my_data_hub.domain.commands import SemanticCommand
from my_data_hub.hashing import sha256_value
from my_data_hub.orchestrator.backlog import load_region_talk_backlog
from my_data_hub.orchestrator.policy import plan_region_talk


class SemanticCommandError(RuntimeError):
    pass


class HubPermissionError(PermissionError):
    pass


_SAFE_SUBJECT_TYPE = re.compile(r"^[a-z][a-z0-9_.-]{0,199}$")


@dataclass(slots=True)
class HubService:
    database_url: str
    scopes: frozenset[str] = field(default_factory=frozenset)
    write_enabled: bool = False

    def _connect(self):  # type: ignore[no-untyped-def]
        try:
            import psycopg
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError("psycopg is required") from exc
        return psycopg.connect(self.database_url)

    def _require(self, scope: str) -> None:
        if scope not in self.scopes:
            raise HubPermissionError(f"MCP scope required: {scope}")

    def _require_write(self, scope: str) -> None:
        self._require(scope)
        if not self.write_enabled:
            raise HubPermissionError("MCP writes are disabled by configuration")

    def health(self) -> dict[str, Any]:
        self._require("hub:read")
        with self._connect() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                    SELECT canonical_revision, schema_revision, updated_at
                    FROM hub.canonical_state WHERE singleton = true
                    """
            )
            row = cursor.fetchone()
        if row is None:
            raise RuntimeError("canonical state singleton is missing")
        return {
            "ok": True,
            "canonical_revision": int(row[0]),
            "schema_revision": int(row[1]),
            "updated_at": row[2].isoformat(),
            "write_enabled": self.write_enabled,
        }

    def list_projects(self, limit: int = 50) -> list[dict[str, Any]]:
        self._require("hub:read")
        limit = max(1, min(limit, 100))
        with self._connect() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                    SELECT project_id, slug, name, left(coalesce(description, ''), 1000),
                           status, revision, updated_at
                    FROM hub.project ORDER BY slug LIMIT %s
                    """,
                (limit,),
            )
            rows = cursor.fetchall()
        return [
            {
                "project_id": str(row[0]),
                "slug": str(row[1]),
                "name": str(row[2]),
                "description": str(row[3]),
                "status": str(row[4]),
                "revision": int(row[5]),
                "updated_at": row[6].isoformat(),
            }
            for row in rows
        ]

    def search_content(self, query: str, limit: int = 20) -> list[dict[str, Any]]:
        self._require("hub:read")
        query = query.strip()
        if not query:
            raise ValueError("query must not be empty")
        if len(query) > 500:
            raise ValueError("query is too long")
        limit = max(1, min(limit, 50))
        with self._connect() as connection, connection.cursor() as cursor:
            cursor.execute("SET LOCAL statement_timeout = '3000ms'")
            cursor.execute(
                """
                    SELECT content_id, content_type, left(coalesce(title, ''), 1000),
                           left(coalesce(summary, ''), 2000), canonical_url, published_at,
                           status, revision,
                           ts_rank_cd(
                               search_document,
                               websearch_to_tsquery('pg_catalog.russian', %s)
                           ) AS rank
                    FROM hub.content_item
                    WHERE search_document @@ websearch_to_tsquery('pg_catalog.russian', %s)
                    ORDER BY rank DESC, published_at DESC NULLS LAST, content_id
                    LIMIT %s
                    """,
                (query, query, limit),
            )
            rows = cursor.fetchall()
        return [
            {
                "content_id": str(row[0]),
                "content_type": str(row[1]),
                "title": str(row[2]),
                "summary": str(row[3]),
                "canonical_url": row[4],
                "published_at": row[5].isoformat() if row[5] else None,
                "status": str(row[6]),
                "revision": int(row[7]),
                "rank": float(row[8]),
            }
            for row in rows
        ]

    def get_content(self, content_id: UUID) -> dict[str, Any] | None:
        self._require("hub:read")
        with self._connect() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                    SELECT content_id, content_type, left(coalesce(title, ''), 1000),
                           left(coalesce(summary, ''), 4000),
                           left(coalesce(body_excerpt, ''), 8000), language,
                           canonical_url, normalized_url, content_hash, published_at,
                           first_observed_at, last_observed_at, status, revision, updated_at
                    FROM hub.content_item
                    WHERE content_id = %s
                    """,
                (content_id,),
            )
            row = cursor.fetchone()
            if row is None:
                return None
            cursor.execute(
                """
                    SELECT asset_id, asset_type, source_url, position, mime_type,
                           byte_size, sha256, width, height, status
                    FROM hub.content_asset
                    WHERE content_id = %s
                    ORDER BY position, asset_id
                    LIMIT 20
                    """,
                (content_id,),
            )
            assets = cursor.fetchall()
        return {
            "content_id": str(row[0]),
            "content_type": str(row[1]),
            "title": str(row[2]),
            "summary": str(row[3]),
            "body_excerpt": str(row[4]),
            "language": row[5],
            "canonical_url": row[6],
            "normalized_url": row[7],
            "content_hash": row[8],
            "published_at": row[9].isoformat() if row[9] else None,
            "first_observed_at": row[10].isoformat(),
            "last_observed_at": row[11].isoformat(),
            "status": str(row[12]),
            "revision": int(row[13]),
            "updated_at": row[14].isoformat(),
            "assets": [
                {
                    "asset_id": str(asset[0]),
                    "asset_type": str(asset[1]),
                    "source_url": asset[2],
                    "position": int(asset[3]),
                    "mime_type": asset[4],
                    "byte_size": int(asset[5]) if asset[5] is not None else None,
                    "sha256": asset[6],
                    "width": int(asset[7]) if asset[7] is not None else None,
                    "height": int(asset[8]) if asset[8] is not None else None,
                    "status": str(asset[9]),
                }
                for asset in assets
            ],
        }

    def get_trace(
        self, subject_type: str, subject_id: UUID, limit: int = 50
    ) -> list[dict[str, Any]]:
        self._require("hub:read")
        subject_type = subject_type.strip()
        if not _SAFE_SUBJECT_TYPE.fullmatch(subject_type):
            raise ValueError("invalid subject_type")
        limit = max(1, min(limit, 100))
        with self._connect() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                    SELECT provenance_event_id, event_type, actor_kind, actor_ref,
                           left(coalesce(query_text, ''), 1000), source_uri, run_id,
                           observed_at, evidence
                    FROM hub.provenance_event
                    WHERE subject_type = %s AND subject_id = %s
                    ORDER BY observed_at DESC, provenance_event_id DESC
                    LIMIT %s
                    """,
                (subject_type, subject_id, limit),
            )
            rows = cursor.fetchall()
        return [
            {
                "provenance_event_id": str(row[0]),
                "event_type": str(row[1]),
                "actor_kind": str(row[2]),
                "actor_ref": row[3],
                "query_text": str(row[4]),
                "source_uri": row[5],
                "run_id": str(row[6]) if row[6] else None,
                "observed_at": row[7].isoformat(),
                "evidence": row[8] or {},
            }
            for row in rows
        ]

    def region_talk_queue_summary(self) -> list[dict[str, Any]]:
        self._require("region-talk:read")
        with self._connect() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                    SELECT stage_key, status, item_count, oldest_created_at,
                           earliest_available_at, pipeline_status
                    FROM orchestration.queue_summary
                    WHERE workload = 'region-talk'
                    ORDER BY stage_key, status NULLS FIRST
                    """
            )
            rows = cursor.fetchall()
        return [
            {
                "stage": str(row[0]),
                "status": str(row[1]) if row[1] else "empty",
                "count": int(row[2]),
                "oldest_created_at": row[3].isoformat() if row[3] else None,
                "earliest_available_at": row[4].isoformat() if row[4] else None,
                "pipeline_status": str(row[5]),
            }
            for row in rows
        ]

    def region_talk_plan(self, max_actions: int = 8) -> dict[str, Any]:
        self._require("region-talk:read")
        max_actions = max(1, min(max_actions, 32))
        backlog = load_region_talk_backlog(self.database_url)
        actions = plan_region_talk(backlog, max_actions=max_actions)
        return {
            "policy": "region-talk-pressure-aware.v1",
            "backlog": asdict(backlog),
            "actions": [asdict(action) for action in actions],
            "executed": False,
        }

    def migration_status(self, limit: int = 20) -> list[dict[str, Any]]:
        self._require("migration:read")
        limit = max(1, min(limit, 50))
        with self._connect() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                    SELECT batch.export_batch_id, batch.status, batch.expected_row_count,
                           coalesce(acc.raw_count, 0),
                           coalesce(acc.dispositioned_count, 0),
                           coalesce(acc.undispositioned_count, 0),
                           coalesce(acc.quarantined_count, 0),
                           coalesce(acc.raw_count_matches_manifest, false),
                           coalesce(acc.fully_accounted, false),
                           coalesce(acc.cutover_ready, false),
                           batch.source_database, batch.source_tables,
                           batch.created_at, batch.completed_at
                    FROM migration.export_batch batch
                    LEFT JOIN migration.batch_accounting acc
                      ON acc.export_batch_id = batch.export_batch_id
                    ORDER BY batch.created_at DESC
                    LIMIT %s
                    """,
                (limit,),
            )
            rows = cursor.fetchall()
        return [
            {
                "export_batch_id": str(row[0]),
                "status": str(row[1]),
                "expected": int(row[2]),
                "raw": int(row[3]),
                "dispositioned": int(row[4]),
                "undispositioned": int(row[5]),
                "quarantined": int(row[6]),
                "raw_count_matches_manifest": bool(row[7]),
                "fully_accounted": bool(row[8]),
                "cutover_ready": bool(row[9]),
                "source_database": str(row[10]),
                "source_tables": list(row[11]),
                "created_at": row[12].isoformat(),
                "completed_at": row[13].isoformat() if row[13] else None,
            }
            for row in rows
        ]

    def migration_accounting(
        self, export_batch_id: UUID | None = None, limit: int = 200
    ) -> list[dict[str, Any]]:
        self._require("migration:read")
        limit = max(1, min(limit, 500))
        query = """
            SELECT export_batch_id, row_kind, expected_row_count, raw_count,
                   normalized_count, deduplicated_count,
                   intentionally_excluded_count, retained_raw_count,
                   quarantined_count, undispositioned_count,
                   raw_count_matches_manifest, fully_accounted, cutover_ready
            FROM migration.row_accounting
        """
        params: tuple[Any, ...]
        if export_batch_id is None:
            query += " ORDER BY export_batch_id DESC, row_kind LIMIT %s"
            params = (limit,)
        else:
            query += " WHERE export_batch_id = %s ORDER BY row_kind LIMIT %s"
            params = (export_batch_id, limit)
        with self._connect() as connection, connection.cursor() as cursor:
            cursor.execute(query, params)
            rows = cursor.fetchall()
        return [
            {
                "export_batch_id": str(row[0]),
                "row_kind": str(row[1]),
                "expected": int(row[2]),
                "raw": int(row[3]),
                "normalized": int(row[4]),
                "deduplicated": int(row[5]),
                "intentionally_excluded": int(row[6]),
                "retained_raw": int(row[7]),
                "quarantined": int(row[8]),
                "undispositioned": int(row[9]),
                "raw_count_matches_manifest": bool(row[10]),
                "fully_accounted": bool(row[11]),
                "cutover_ready": bool(row[12]),
            }
            for row in rows
        ]

    def connector_status(self, limit: int = 50) -> list[dict[str, Any]]:
        """Return bounded connector/product status without payloads or credentials."""

        self._require("connector:read")
        limit = max(1, min(limit, 100))
        with self._connect() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT connector.connector_id, connector.status,
                       product.data_product, product.schema_version, product.enabled,
                       connector.expected_cadence,
                       max(batch.accepted_at) AS last_accepted_at,
                       max(batch.committed_at) AS last_committed_at,
                       count(batch.batch_id) FILTER (WHERE batch.status = 'canonical_committed'),
                       (SELECT count(*) FROM integration.quarantine quarantine
                         WHERE quarantine.connector_id = connector.connector_id)
                FROM integration.connector connector
                JOIN integration.data_product product USING (connector_id)
                LEFT JOIN integration.batch batch
                  ON batch.connector_id = connector.connector_id
                 AND batch.data_product = product.data_product
                GROUP BY connector.connector_id, connector.status, product.data_product,
                         product.schema_version, product.enabled, connector.expected_cadence
                ORDER BY connector.connector_id, product.data_product
                LIMIT %s
                """,
                (limit,),
            )
            rows = cursor.fetchall()
        return [
            {
                "connector_id": str(row[0]),
                "connector_status": str(row[1]),
                "data_product": str(row[2]),
                "schema_version": str(row[3]),
                "enabled": bool(row[4]),
                "expected_cadence": str(row[5]) if row[5] else None,
                "last_accepted_at": row[6].isoformat() if row[6] else None,
                "last_committed_at": row[7].isoformat() if row[7] else None,
                "committed_batches": int(row[8]),
                "quarantined_or_conflicting_batches": int(row[9]),
            }
            for row in rows
        ]

    def provider_resource_status(self, limit: int = 100) -> list[dict[str, Any]]:
        """Return the same minimal status projection for every provider class."""

        self._require("provider:read")
        limit = max(1, min(limit, 200))
        with self._connect() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT resource_id, provider, resource_kind, control_class,
                       lifecycle_state, privacy_attestation = 'private', last_observed_at
                FROM integration.provider_resource
                ORDER BY last_observed_at DESC, resource_id
                LIMIT %s
                """,
                (limit,),
            )
            rows = cursor.fetchall()
        return [
            {
                "resource_id": str(row[0]),
                "provider": str(row[1]),
                "resource_kind": str(row[2]),
                "control_class": str(row[3]),
                "lifecycle_state": str(row[4]),
                "private": bool(row[5]) if row[5] is not None else None,
                "last_observed_at": row[6].isoformat(),
            }
            for row in rows
        ]

    def get_command(self, command_id: UUID) -> dict[str, Any] | None:
        self._require("hub:read")
        with self._connect() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                    SELECT cmd.command_id, cmd.command_type, cmd.status, cmd.created_at,
                           cmd.applied_at, receipt.accepted_revision, receipt.result,
                           receipt.output_fingerprint
                    FROM sync.command cmd
                    LEFT JOIN sync.command_receipt receipt
                      ON receipt.command_id = cmd.command_id
                    WHERE cmd.command_id = %s
                    """,
                (command_id,),
            )
            row = cursor.fetchone()
        if row is None:
            return None
        return {
            "command_id": str(row[0]),
            "command_type": str(row[1]),
            "status": str(row[2]),
            "created_at": row[3].isoformat(),
            "applied_at": row[4].isoformat() if row[4] else None,
            "accepted_revision": int(row[5]) if row[5] is not None else None,
            "result": row[6] or {},
            "output_fingerprint": row[7],
        }

    def list_conflicts(self, limit: int = 20) -> list[dict[str, Any]]:
        self._require("hub:read")
        limit = max(1, min(limit, 50))
        with self._connect() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                    SELECT conflict_id, conflict_kind, target_type, target_id,
                           status, created_at
                    FROM sync.conflict
                    WHERE status = 'open'
                    ORDER BY created_at, conflict_id
                    LIMIT %s
                    """,
                (limit,),
            )
            rows = cursor.fetchall()
        return [
            {
                "conflict_id": str(row[0]),
                "kind": str(row[1]),
                "target_type": row[2],
                "target_id": str(row[3]) if row[3] else None,
                "status": str(row[4]),
                "created_at": row[5].isoformat(),
            }
            for row in rows
        ]

    def enqueue_region_talk_work(
        self,
        *,
        stage: str,
        url: str | None = None,
        subject_id: UUID | None = None,
        subject_type: str = "content_url",
        priority: int = 100,
        dedupe_key: str | None = None,
        dry_run: bool = True,
    ) -> dict[str, Any]:
        self._require_write("region-talk:write")
        stage = stage.strip()
        subject_type = subject_type.strip()
        if not _SAFE_SUBJECT_TYPE.fullmatch(subject_type):
            raise ValueError("invalid subject_type")
        if not 1 <= priority <= 10_000:
            raise ValueError("priority must be between 1 and 10000")
        normalized_url = self._normalize_url(url) if url else None
        if subject_id is None:
            if normalized_url is None:
                raise ValueError("subject_id or url is required")
            subject_id = uuid5(
                NAMESPACE_URL,
                f"my-data-hub:region-talk:{subject_type}:{normalized_url}",
            )
        payload = {
            "stage": stage,
            "subject_type": subject_type,
            "subject_id": str(subject_id),
            "subject_url": normalized_url,
            "priority": priority,
            "dedupe_key": dedupe_key or f"mcp:{stage}:{subject_id}",
        }
        if len(str(payload["dedupe_key"])) > 300:
            raise ValueError("dedupe_key is too long")
        input_fingerprint = sha256_value(payload)
        idempotency_key = f"region-talk.work.enqueue:{input_fingerprint}"
        command_id = uuid5(NAMESPACE_URL, f"my-data-hub:{idempotency_key}")
        canonical_revision = self._canonical_revision()
        command = SemanticCommand.model_validate(
            {
                "schema_version": "my-data-hub-semantic-command.v1",
                "command_id": command_id,
                "client_id": "my-data-hub-mcp",
                "actor_id": "mcp-principal",
                "idempotency_key": idempotency_key,
                "command_type": "region_talk.work.enqueue",
                "base_revision": canonical_revision,
                "expected_revision": canonical_revision,
                "target": {"type": subject_type, "id": subject_id},
                "input_fingerprint": input_fingerprint,
                "payload": payload,
                "reason": "bounded MCP queue request",
                "dry_run": dry_run,
            }
        )
        return self._submit_validated_command(command, required_scope="region-talk:write")

    def submit_command(self, raw: dict[str, Any]) -> dict[str, Any]:
        self._require_write("hub:write")
        command = SemanticCommand.model_validate(raw)
        return self._submit_validated_command(command, required_scope="hub:write")

    def _canonical_revision(self) -> int:
        with self._connect() as connection, connection.cursor() as cursor:
            cursor.execute(
                "SELECT canonical_revision FROM hub.canonical_state WHERE singleton = true"
            )
            row = cursor.fetchone()
        if row is None:
            raise SemanticCommandError("canonical state singleton is missing")
        return int(row[0])

    @staticmethod
    def _normalize_url(raw: str) -> str:
        raw = raw.strip()
        if not raw or len(raw) > 4000:
            raise ValueError("url is empty or too long")
        parsed = urlsplit(raw)
        if parsed.scheme.lower() not in {"http", "https"} or not parsed.netloc:
            raise ValueError("url must be absolute HTTP(S)")
        host = parsed.hostname.lower() if parsed.hostname else ""
        port = parsed.port
        default_port = (parsed.scheme.lower() == "http" and port == 80) or (
            parsed.scheme.lower() == "https" and port == 443
        )
        netloc = host if port is None or default_port else f"{host}:{port}"
        path = parsed.path or "/"
        return urlunsplit((parsed.scheme.lower(), netloc, path, parsed.query, ""))

    def _submit_validated_command(
        self, command: SemanticCommand, *, required_scope: str
    ) -> dict[str, Any]:
        self._require_write(required_scope)
        if command.dry_run:
            with self._connect() as connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"
                    )
                    cursor.execute(
                        "SELECT canonical_revision FROM hub.canonical_state "
                        "WHERE singleton = true"
                    )
                    row = cursor.fetchone()
                    if row is None:
                        raise SemanticCommandError("canonical state singleton is missing")
                    canonical_revision = int(row[0])
                    self._validate_revision(command, canonical_revision)
                    result = self._apply_supported_command(
                        cursor, command, canonical_revision, preview_only=True
                    )
                connection.rollback()
            return {
                "command_id": str(command.command_id),
                "status": "dry_run",
                "accepted_revision": canonical_revision,
                "result": result,
                "output_fingerprint": sha256_value(result),
                "duplicate": False,
                "persisted": False,
            }

        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
                cursor.execute(
                    """
                    SELECT canonical_revision
                    FROM hub.canonical_state
                    WHERE singleton = true
                    FOR UPDATE
                    """
                )
                state = cursor.fetchone()
                if state is None:
                    raise SemanticCommandError("canonical state singleton is missing")
                canonical_revision = int(state[0])
                cursor.execute(
                    """
                    SELECT command_id, command_type, input_fingerprint, status
                    FROM sync.command
                    WHERE client_id = %s AND idempotency_key = %s
                    """,
                    (command.client_id, command.idempotency_key),
                )
                existing = cursor.fetchone()
                if existing:
                    if (
                        str(existing[0]) != str(command.command_id)
                        or str(existing[1]) != command.command_type
                        or str(existing[2]) != command.input_fingerprint
                    ):
                        raise SemanticCommandError(
                            "idempotency key is already bound to a different command"
                        )
                    cursor.execute(
                        """
                        SELECT accepted_revision, result, output_fingerprint
                        FROM sync.command_receipt WHERE command_id = %s
                        """,
                        (existing[0],),
                    )
                    receipt = cursor.fetchone()
                    return {
                        "command_id": str(existing[0]),
                        "status": str(existing[3]),
                        "accepted_revision": (
                            int(receipt[0]) if receipt and receipt[0] is not None else None
                        ),
                        "result": receipt[1] if receipt else {},
                        "output_fingerprint": receipt[2] if receipt else None,
                        "duplicate": True,
                        "persisted": True,
                    }
                self._validate_revision(command, canonical_revision)
                self._validate_dependencies(cursor, command)
                cursor.execute(
                    """
                    INSERT INTO sync.command (
                        command_id, session_id, client_id, actor_id, idempotency_key,
                        command_type, schema_version, base_revision, expected_revision,
                        target_type, target_id, depends_on, input_fingerprint, payload,
                        reason, status, dry_run
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s::uuid[], %s, %s::jsonb, %s, 'accepted', false
                    )
                    """,
                    (
                        command.command_id,
                        command.session_id,
                        command.client_id,
                        command.actor_id,
                        command.idempotency_key,
                        command.command_type,
                        command.schema_version,
                        command.base_revision,
                        command.expected_revision,
                        command.target.type if command.target else None,
                        command.target.id if command.target else None,
                        command.depends_on,
                        command.input_fingerprint,
                        json.dumps(command.payload, ensure_ascii=False),
                        command.reason,
                    ),
                )
                result = self._apply_supported_command(
                    cursor, command, canonical_revision, preview_only=False
                )
                accepted_revision = canonical_revision
                if bool(result.get("canonical_change")):
                    accepted_revision += 1
                    cursor.execute(
                        """
                        UPDATE hub.canonical_state
                        SET canonical_revision = %s, updated_at = now()
                        WHERE singleton = true
                        """,
                        (accepted_revision,),
                    )
                cursor.execute(
                    """
                    UPDATE sync.command
                    SET status = 'applied', applied_at = now()
                    WHERE command_id = %s
                    """,
                    (command.command_id,),
                )
                output_fingerprint = sha256_value(result)
                cursor.execute(
                    """
                    INSERT INTO sync.command_receipt (
                        command_id, accepted_revision, result, output_fingerprint
                    ) VALUES (%s, %s, %s::jsonb, %s)
                    """,
                    (
                        command.command_id,
                        accepted_revision,
                        json.dumps(result, ensure_ascii=False),
                        output_fingerprint,
                    ),
                )
                cursor.execute(
                    """
                    INSERT INTO sync.audit_event (
                        actor_id, client_id, action, outcome, subject_type,
                        subject_id, command_id, details
                    ) VALUES (%s, %s, %s, 'applied', %s, %s, %s, %s::jsonb)
                    """,
                    (
                        command.actor_id,
                        command.client_id,
                        command.command_type,
                        command.target.type if command.target else None,
                        command.target.id if command.target else None,
                        command.command_id,
                        json.dumps(
                            {
                                "base_revision": command.base_revision,
                                "accepted_revision": accepted_revision,
                                "input_fingerprint": command.input_fingerprint,
                                "output_fingerprint": output_fingerprint,
                            },
                            ensure_ascii=False,
                        ),
                    ),
                )
            connection.commit()
        return {
            "command_id": str(command.command_id),
            "status": "applied",
            "accepted_revision": accepted_revision,
            "result": result,
            "output_fingerprint": output_fingerprint,
            "duplicate": False,
            "persisted": True,
        }

    @staticmethod
    def _validate_revision(command: SemanticCommand, canonical_revision: int) -> None:
        if command.base_revision > canonical_revision:
            raise SemanticCommandError("command base revision is ahead of canonical state")
        if (
            command.expected_revision is not None
            and command.expected_revision != canonical_revision
        ):
            raise SemanticCommandError(
                f"expected revision {command.expected_revision} does not match "
                f"canonical revision {canonical_revision}"
            )

    @staticmethod
    def _validate_dependencies(cursor, command: SemanticCommand) -> None:  # type: ignore[no-untyped-def]
        if not command.depends_on:
            return
        cursor.execute(
            """
            SELECT command_id, status
            FROM sync.command
            WHERE command_id = ANY(%s::uuid[])
            """,
            (command.depends_on,),
        )
        found = {UUID(str(row[0])): str(row[1]) for row in cursor.fetchall()}
        missing = [item for item in command.depends_on if item not in found]
        blocked = [item for item in command.depends_on if found.get(item) != "applied"]
        if missing or blocked:
            raise SemanticCommandError("command dependencies are missing or not applied")

    def _apply_supported_command(
        self,
        cursor,  # type: ignore[no-untyped-def]
        command: SemanticCommand,
        revision: int,
        *,
        preview_only: bool,
    ) -> dict[str, Any]:
        if command.command_type != "region_talk.work.enqueue":
            raise SemanticCommandError(
                f"unsupported bootstrap command type: {command.command_type}"
            )
        stage_key = str(command.payload.get("stage", "")).strip()
        subject_type = str(command.payload.get("subject_type", "content_url")).strip()
        subject_id_raw = command.payload.get("subject_id")
        subject_url = str(command.payload.get("subject_url") or "").strip()
        if not stage_key:
            raise SemanticCommandError("stage is required")
        if not _SAFE_SUBJECT_TYPE.fullmatch(subject_type):
            raise SemanticCommandError("invalid subject_type")
        if subject_id_raw:
            subject_id = UUID(str(subject_id_raw))
        elif subject_url:
            subject_id = uuid5(
                NAMESPACE_URL,
                f"my-data-hub:region-talk:{subject_type}:{subject_url}",
            )
        else:
            raise SemanticCommandError("subject_id or subject_url is required")
        priority = int(command.payload.get("priority", 100))
        if not 1 <= priority <= 10_000:
            raise SemanticCommandError("priority must be between 1 and 10000")
        cursor.execute(
            """
            SELECT p.pipeline_id, ps.stage_id, hp.project_id, p.status,
                   ps.enabled, ps.compute_lane
            FROM orchestration.pipeline p
            JOIN orchestration.pipeline_stage ps ON ps.pipeline_id = p.pipeline_id
            JOIN hub.project hp ON hp.slug = 'region-talk'
            WHERE p.workload = 'region-talk' AND ps.stage_key = %s
            ORDER BY p.created_at DESC
            LIMIT 1
            """,
            (stage_key,),
        )
        row = cursor.fetchone()
        if row is None:
            raise SemanticCommandError(f"Region Talk stage not found: {stage_key}")
        pipeline_status = str(row[3])
        stage_enabled = bool(row[4])
        compute_lane = str(row[5])
        side_effect_blocked = compute_lane == "local-side-effect"
        would_enqueue = (
            pipeline_status == "active" and stage_enabled and not side_effect_blocked
        )
        dedupe_key = str(command.payload.get("dedupe_key") or command.input_fingerprint)
        if len(dedupe_key) > 300:
            raise SemanticCommandError("dedupe_key is too long")
        preview = {
            "would_enqueue": would_enqueue,
            "stage": stage_key,
            "subject_id": str(subject_id),
            "dedupe_key": dedupe_key,
            "pipeline_status": pipeline_status,
            "stage_enabled": stage_enabled,
            "compute_lane": compute_lane,
            "side_effect_stage_blocked": side_effect_blocked,
            "canonical_change": False,
        }
        if preview_only:
            return preview
        if pipeline_status != "active":
            raise SemanticCommandError("Region Talk pipeline is not active")
        if not stage_enabled:
            raise SemanticCommandError(f"Region Talk stage is disabled: {stage_key}")
        if side_effect_blocked:
            raise SemanticCommandError(
                "MCP cannot enqueue external side-effect stages in the bootstrap surface"
            )
        cursor.execute(
            """
            INSERT INTO orchestration.work_item (
                pipeline_id, stage_id, project_id, subject_type, subject_id,
                dedupe_key, input_fingerprint, priority, expected_revision, payload
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            ON CONFLICT (pipeline_id, stage_id, dedupe_key) DO NOTHING
            RETURNING work_item_id, status, queue_seq, input_fingerprint
            """,
            (
                row[0],
                row[1],
                row[2],
                subject_type,
                subject_id,
                dedupe_key,
                command.input_fingerprint,
                priority,
                command.expected_revision,
                json.dumps(command.payload, ensure_ascii=False),
            ),
        )
        work = cursor.fetchone()
        created = work is not None
        if work is None:
            cursor.execute(
                """
                SELECT work_item_id, status, queue_seq, input_fingerprint
                FROM orchestration.work_item
                WHERE pipeline_id = %s AND stage_id = %s AND dedupe_key = %s
                """,
                (row[0], row[1], dedupe_key),
            )
            work = cursor.fetchone()
            if work is None:
                raise SemanticCommandError("work-item idempotency readback failed")
            if str(work[3]) != command.input_fingerprint:
                raise SemanticCommandError(
                    "dedupe key is already bound to a different input fingerprint"
                )
        if created:
            cursor.execute(
                """
                INSERT INTO orchestration.work_item_event (
                    work_item_id, event_kind, to_status, actor_kind, actor_ref,
                    reason, evidence
                ) VALUES (%s, 'enqueued', %s, 'mcp', %s, %s, %s::jsonb)
                """,
                (
                    work[0],
                    work[1],
                    command.actor_id,
                    command.reason,
                    json.dumps(
                        {
                            "command_id": str(command.command_id),
                            "canonical_revision_before": revision,
                        },
                        ensure_ascii=False,
                    ),
                ),
            )
        return {
            "work_item_id": str(work[0]),
            "status": str(work[1]),
            "queue_seq": int(work[2]),
            "stage": stage_key,
            "created": created,
            "base_revision": revision,
            "canonical_change": created,
        }
