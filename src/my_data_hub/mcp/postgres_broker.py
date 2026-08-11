"""Epoch-bound direct PostgreSQL sessions for the remote MCP reader.

The broker never knows a static master URL.  A tunnel/session registrar writes a
short-lived credential envelope for one ACTIVE master epoch into the service
secret directory.  The MCP process resolves that exact envelope for every tool
call and connects directly through the loopback reverse tunnel.
"""

from __future__ import annotations

import asyncio
import json
import math
import os
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import parse_qs, urlsplit

from my_data_hub.embeddings.rrf import (
    RankedRetrieverResult,
    UnavailableRetriever,
    reciprocal_rank_fusion,
)
from my_data_hub.hashing import canonical_json_bytes
from my_data_hub.mcp.contracts import MasterSession, MasterSessionBroker, SessionRequest


class SessionBrokerError(RuntimeError):
    """The exact master session could not be issued or safely executed."""


_EMBEDDING_COVERAGE_SQL = """
SELECT model.provider_model_id || '@' || model.exact_revision AS model_exact_id,
       count(document.search_document_id) AS expected_documents,
       count(document.search_document_id) FILTER (
           WHERE job.status='succeeded' AND CASE model.dimensions
             WHEN 768 THEN e768.search_document_id IS NOT NULL
             WHEN 1024 THEN e1024.search_document_id IS NOT NULL END
       ) AS completed_documents
FROM search.embedding_model model
CROSS JOIN search.document document
LEFT JOIN search.embedding_job job
 ON job.search_document_id=document.search_document_id
AND job.representation_kind=document.representation_kind AND job.model_id=model.model_id
LEFT JOIN search.embedding_768 e768
 ON model.dimensions=768 AND e768.search_document_id=job.search_document_id
AND e768.model_id=job.model_id AND e768.input_hash=job.input_hash
LEFT JOIN search.embedding_1024 e1024
 ON model.dimensions=1024 AND e1024.search_document_id=job.search_document_id
AND e1024.model_id=job.model_id AND e1024.input_hash=job.input_hash
WHERE document.is_current
GROUP BY model.model_key,model.provider_model_id,model.exact_revision,model.dimensions
ORDER BY model.model_key
"""


@dataclass(frozen=True, slots=True)
class EpochDatabaseCredential:
    master_instance_id: str
    epoch: int
    role: str
    database_url: str
    expires_at: datetime

    def validate(self, request: SessionRequest, *, now: datetime) -> None:
        self.validate_binding(
            master_instance_id=request.master_instance_id,
            epoch=request.epoch,
            role=request.role,
            now=now,
        )

    def validate_binding(
        self, *, master_instance_id: str, epoch: int, role: str, now: datetime
    ) -> None:
        if self.master_instance_id != master_instance_id or self.epoch != epoch:
            raise SessionBrokerError("credential is bound to a different master epoch")
        if self.role != role:
            raise SessionBrokerError("credential role does not match the requested MCP role")
        if self.expires_at.tzinfo is None or self.expires_at.astimezone(UTC) <= now.astimezone(UTC):
            raise SessionBrokerError("master credential is expired")
        parsed = urlsplit(self.database_url)
        if parsed.scheme not in {"postgres", "postgresql"} or not parsed.username or not parsed.password:
            raise SessionBrokerError("master credential is not a password-bound PostgreSQL URL")
        if parsed.hostname not in {"127.0.0.1", "::1", "localhost", "postgres-master.internal"}:
            raise SessionBrokerError("master credential must target the loopback tunnel")
        query = parse_qs(parsed.query)
        if query.get("sslmode", [""])[0] not in {"verify-ca", "verify-full"}:
            raise SessionBrokerError("master PostgreSQL session must verify TLS")
        if query.get("sslrootcert", [""])[0] != "/state/master-tls/ca.pem":
            raise SessionBrokerError("master PostgreSQL session must use the fixed task CA path")
        if query.get("connect_timeout", [""])[0] != "5":
            raise SessionBrokerError("master PostgreSQL session connect timeout differs from policy")


class EpochCredentialSource(Protocol):
    def load(self, request: SessionRequest) -> EpochDatabaseCredential: ...


class DirectoryEpochCredentialSource:
    """Read one short-lived secret envelope from a service-owned 0700 directory."""

    def __init__(self, root: Path) -> None:
        self.root = root

    @staticmethod
    def _name(master_instance_id: str, epoch: int, role: str) -> str:
        safe_instance = master_instance_id.replace("-", "")
        if (
            not safe_instance.isalnum()
            or epoch < 1
            or not role.replace("_", "").isalnum()
        ):
            raise SessionBrokerError("session identity contains unsafe path characters")
        return f"{safe_instance}.{epoch}.{role}.json"

    def load(self, request: SessionRequest) -> EpochDatabaseCredential:
        if self.root.is_symlink() or not self.root.is_dir() or self.root.stat().st_mode & 0o077:
            raise SessionBrokerError("master session secret directory must be a private regular directory")
        path = self.root / self._name(request.master_instance_id, request.epoch, request.role)
        if path.is_symlink() or not path.is_file() or path.stat().st_mode & 0o077:
            raise SessionBrokerError("epoch credential file is absent or not mode 0600")
        if path.stat().st_size > 16 * 1024:
            raise SessionBrokerError("epoch credential envelope is oversized")
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise SessionBrokerError("epoch credential envelope is invalid") from exc
        required = {"master_instance_id", "epoch", "role", "database_url", "expires_at"}
        if not isinstance(payload, dict) or set(payload) != required:
            raise SessionBrokerError("epoch credential envelope fields differ from the contract")
        try:
            credential = EpochDatabaseCredential(
                master_instance_id=str(payload["master_instance_id"]),
                epoch=int(payload["epoch"]),
                role=str(payload["role"]),
                database_url=str(payload["database_url"]),
                expires_at=datetime.fromisoformat(str(payload["expires_at"]).replace("Z", "+00:00")),
            )
        except (TypeError, ValueError) as exc:
            raise SessionBrokerError("epoch credential envelope values are invalid") from exc
        credential.validate(request, now=datetime.now(UTC))
        return credential

    def store(self, credential: EpochDatabaseCredential) -> Path:
        """Atomic registrar seam; callers authenticate/fence before invoking it."""

        credential.validate_binding(
            master_instance_id=credential.master_instance_id,
            epoch=credential.epoch,
            role=credential.role,
            now=datetime.now(UTC),
        )
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.root.chmod(0o700)
        path = self.root / self._name(
            credential.master_instance_id, credential.epoch, credential.role
        )
        temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}")
        payload = {
            "master_instance_id": credential.master_instance_id,
            "epoch": credential.epoch,
            "role": credential.role,
            "database_url": credential.database_url,
            "expires_at": credential.expires_at.astimezone(UTC).isoformat().replace("+00:00", "Z"),
        }
        temporary.write_bytes(canonical_json_bytes(payload) + b"\n")
        temporary.chmod(0o600)
        temporary.replace(path)
        return path


_ROLE_GROUPS = {
    "reader": "mdh_mcp_reader",
    "migration_operator": "mdh_migration_operator",
    "operator": "mdh_mcp_editor",
    "provider_operator": "mdh_mcp_editor",
}


class PostgresMasterSessionBroker(MasterSessionBroker):
    def __init__(self, source: EpochCredentialSource) -> None:
        self.source = source

    def issue_session(self, request: SessionRequest) -> MasterSession:
        if request.role not in _ROLE_GROUPS:
            raise SessionBrokerError("requested MCP role is not brokerable")
        credential = self.source.load(request)
        credential.validate(request, now=datetime.now(UTC))
        return PostgresMasterSession(request, credential)


class PostgresMasterSession(MasterSession):
    def __init__(self, request: SessionRequest, credential: EpochDatabaseCredential) -> None:
        self.request = request
        self.credential = credential
        self._closed = False

    async def execute(self, arguments: Mapping[str, Any]) -> Mapping[str, Any]:
        if self._closed:
            raise SessionBrokerError("master session is closed")
        return await asyncio.wait_for(
            asyncio.to_thread(self._execute_sync, dict(arguments)),
            timeout=self.request.limits.timeout_ms / 1000 + 2,
        )

    async def close(self) -> None:
        self._closed = True

    def _execute_sync(self, arguments: dict[str, Any]) -> dict[str, Any]:
        import psycopg
        from psycopg.rows import dict_row

        with psycopg.connect(
            self.credential.database_url,
            connect_timeout=3,
            row_factory=dict_row,
        ) as connection, connection.cursor() as cursor:
            cursor.execute("SET TRANSACTION READ ONLY")
            cursor.execute("SET LOCAL statement_timeout = %s", (self.request.limits.timeout_ms,))
            cursor.execute("SET LOCAL lock_timeout = %s", (min(2_000, self.request.limits.timeout_ms),))
            cursor.execute("SET LOCAL idle_in_transaction_session_timeout = %s", (self.request.limits.timeout_ms,))
            cursor.execute("SET LOCAL ROLE " + _ROLE_GROUPS[self.request.role])
            rows = self._dispatch(cursor, arguments)
            state = cursor.execute(
                "SELECT c.canonical_revision,e.current_epoch "
                "FROM hub.canonical_state c CROSS JOIN master_control.epoch_state e "
                "WHERE c.singleton=true AND e.singleton=true"
            ).fetchone()
            connection.rollback()
        if state is None or int(state["current_epoch"] or 0) != self.request.epoch:
            raise SessionBrokerError("master epoch changed during the MCP session")
        result = {
            **rows,
            "canonical_revision": int(state["canonical_revision"]),
            "master_epoch": self.request.epoch,
        }
        encoded = canonical_json_bytes(_jsonable(result))
        if len(encoded) > self.request.limits.max_bytes:
            raise SessionBrokerError("master response exceeds the broker byte cap")
        return _jsonable(result)

    def _dispatch(self, cursor: Any, arguments: dict[str, Any]) -> dict[str, Any]:
        tool = self.request.tool
        limit = self.request.limits.max_rows
        if tool == "runtime.stale_epoch.probe":
            cursor.execute("SELECT current_epoch FROM master_control.epoch_state WHERE singleton=true")
            row = cursor.fetchone()
            return {"observed_epoch": int(row["current_epoch"]) if row else None}
        if tool == "bloggers.list":
            requested = min(int(arguments.get("limit", 50)), limit)
            cursor_value = arguments.get("cursor")
            cursor.execute(
                "SELECT * FROM region_talk.bloggers_ru_v1 "
                "WHERE (%s::uuid IS NULL OR blogger_id > %s::uuid) "
                "ORDER BY blogger_id LIMIT %s",
                (cursor_value, cursor_value, requested + 1),
            )
            rows = cursor.fetchall()
            has_more = len(rows) > requested
            page = rows[:requested]
            return {
                "items": page,
                "cursor": str(page[-1]["blogger_id"]) if has_more and page else None,
                "complete": not has_more,
            }
        if tool == "bloggers.get":
            row = cursor.execute(
                "SELECT * FROM region_talk.bloggers_ru_v1 WHERE blogger_id=%s::uuid",
                (arguments.get("blogger_id"),),
            ).fetchone()
            return {"found": row is not None, "blogger": row}
        if tool == "bloggers.provenance":
            requested = min(int(arguments.get("limit", 50)), limit)
            rows = cursor.execute(
                "SELECT provenance_event_id,event_type,actor_kind,actor_ref,source_uri,run_id,"
                "observed_at,evidence FROM hub.provenance_event "
                "WHERE subject_type='actor' AND subject_id=%s::uuid "
                "ORDER BY observed_at DESC,provenance_event_id LIMIT %s",
                (arguments.get("blogger_id"), requested),
            ).fetchall()
            return {"items": rows, "complete": len(rows) < requested}
        if tool == "bloggers.statistics":
            row = cursor.execute(
                "SELECT count(*) AS bloggers, count(*) FILTER (WHERE requires_review) AS requires_review,"
                "count(*) FILTER (WHERE jsonb_array_length(public_accounts)>0) AS with_public_accounts "
                "FROM region_talk.bloggers_ru_v1"
            ).fetchone()
            return {"statistics": row}
        if tool == "bloggers.migration.accounting":
            batch_id = str(arguments.get("export_batch_id", ""))
            try:
                from uuid import UUID

                exact_batch_id = str(UUID(batch_id))
            except ValueError as exc:
                raise SessionBrokerError("export_batch_id is not an exact UUID") from exc
            row = cursor.execute(
                "SELECT b.export_batch_id,b.expected_row_count,b.status,b.logical_sha256,"
                "b.metadata->>'record_id_set_sha256' AS record_id_set_sha256,"
                "b.metadata->>'canonical_outcome_sha256' AS canonical_outcome_sha256,"
                "(b.metadata->>'duplicate_groups_pending')::integer AS duplicate_groups_pending,"
                "(b.metadata->>'canonical_revision')::bigint AS imported_canonical_revision,"
                "a.raw_count,a.dispositioned_count,a.undispositioned_count,a.quarantined_count,"
                "(SELECT count(*) FROM region_talk.blogger_profile p WHERE p.export_batch_id=b.export_batch_id) "
                "AS actor_count,"
                "(SELECT count(*) FROM hub.external_account x JOIN region_talk.blogger_profile p "
                "ON p.actor_id=x.actor_id WHERE p.export_batch_id=b.export_batch_id) AS account_count,"
                "EXISTS (SELECT 1 FROM sync.external_outbox o WHERE o.aggregate_type='blogger_import' "
                "AND o.aggregate_id=b.export_batch_id AND o.effect_kind='verified_checkpoint_required' "
                "AND o.required_revision=(b.metadata->>'canonical_revision')::bigint) AS checkpoint_required "
                "FROM migration.export_batch b JOIN migration.batch_accounting a USING(export_batch_id) "
                "WHERE b.export_batch_id=%s::uuid",
                (exact_batch_id,),
            ).fetchone()
            if row is None:
                return {"found": False, "export_batch_id": exact_batch_id}
            return {"found": True, "accounting": row}
        if tool == "embedding.coverage":
            rows = cursor.execute(
                "SELECT model_exact_id,expected_documents,completed_documents,"
                "CASE WHEN expected_documents=0 THEN 0.0 "
                "ELSE completed_documents::double precision/expected_documents END AS coverage "
                f"FROM ({_EMBEDDING_COVERAGE_SQL}) coverage"
            ).fetchall()
            revision = cursor.execute(
                "SELECT canonical_revision FROM hub.canonical_state WHERE singleton=true"
            ).fetchone()
            return {
                "models": rows,
                "canonical_revision": int(revision["canonical_revision"]),
                "complete": bool(rows) and all(float(row["coverage"]) == 1.0 for row in rows),
            }
        if tool == "bloggers.search":
            query = str(arguments.get("query", "")).strip()
            if not 1 <= len(query) <= 500:
                raise SessionBrokerError("blogger search query is empty or oversized")
            requested = min(int(arguments.get("limit", 20)), limit)
            candidate_limit = min(max(requested * 5, 50), 500)
            exact_ids = tuple(
                str(row["blogger_id"])
                for row in cursor.execute(
                    "SELECT blogger_id FROM region_talk.bloggers_ru_v1 "
                    "WHERE display_name ILIKE '%%'||%s||'%%' "
                    "ORDER BY (lower(display_name)=lower(%s)) DESC,lower(display_name),blogger_id LIMIT %s",
                    (query, query, candidate_limit),
                ).fetchall()
            )
            fts_ids = tuple(
                str(row["actor_id"])
                for row in cursor.execute(
                    "SELECT actor_id FROM search.document WHERE is_current "
                    "AND search_vector @@ websearch_to_tsquery('pg_catalog.russian',%s) "
                    "ORDER BY ts_rank_cd(search_vector,websearch_to_tsquery('pg_catalog.russian',%s)) DESC,"
                    "actor_id LIMIT %s",
                    (query, query, candidate_limit),
                ).fetchall()
            )
            coverage = cursor.execute(
                "SELECT model_exact_id,expected_documents,completed_documents,"
                "CASE WHEN expected_documents=0 THEN 0.0 "
                "ELSE completed_documents::double precision/expected_documents END AS coverage "
                f"FROM ({_EMBEDDING_COVERAGE_SQL}) coverage"
            ).fetchall()
            rankings = [
                RankedRetrieverResult(name="exact", document_ids=exact_ids),
                RankedRetrieverResult(name="fts", document_ids=fts_ids),
            ]
            index_revision_row = cursor.execute(
                "SELECT canonical_revision FROM hub.canonical_state WHERE singleton=true"
            ).fetchone()
            index_revision = int(index_revision_row["canonical_revision"])
            unavailable: list[UnavailableRetriever] = []
            vector_specs = (
                ("e5", "e5_query_vector", 768, "embedding_768", "e5_multilingual_base_v1"),
                ("bge_m3", "bge_m3_query_vector", 1024, "embedding_1024", "bge_m3_dense_v1"),
            )
            for name, argument_name, dimensions, table, vector_space in vector_specs:
                vector = arguments.get(argument_name)
                if vector is None:
                    unavailable.append(
                        UnavailableRetriever(name=name, reason="exact_query_vector_absent", retryable=False)
                    )
                    continue
                if (
                    not isinstance(vector, list)
                    or len(vector) != dimensions
                    or any(not isinstance(item, (int, float)) or not math.isfinite(float(item)) for item in vector)
                ):
                    raise SessionBrokerError(f"{name} query vector violates its exact vector space")
                norm = math.sqrt(sum(float(item) ** 2 for item in vector))
                if not 0.999 <= norm <= 1.001:
                    raise SessionBrokerError(f"{name} query vector is not L2-normalized")
                halfvec = "[" + ",".join(format(float(item), ".9g") for item in vector) + "]"
                vector_rows = cursor.execute(
                    f"SELECT d.actor_id,m.exact_revision FROM search.{table} e "
                    "JOIN search.document d ON d.search_document_id=e.search_document_id AND d.is_current "
                    "JOIN search.embedding_model m ON m.model_id=e.model_id "
                    "JOIN search.embedding_job j ON j.search_document_id=e.search_document_id "
                    "AND j.model_id=e.model_id AND j.input_hash=e.input_hash AND j.status='succeeded' "
                    "ORDER BY e.embedding <=> %s::halfvec,d.actor_id LIMIT %s",
                    (halfvec, candidate_limit),
                ).fetchall()
                rankings.append(
                    RankedRetrieverResult(
                        name=name,
                        document_ids=tuple(str(row["actor_id"]) for row in vector_rows),
                        index_revision=index_revision,
                        vector_space=vector_space,
                    )
                )
            fused = reciprocal_rank_fusion(
                requested_retrievers=("exact", "fts", "e5", "bge_m3"),
                rankings=tuple(rankings),
                unavailable=tuple(unavailable),
            )
            selected = [hit.document_id for hit in fused.hits[:requested]]
            rows_by_id: dict[str, Any] = {}
            if selected:
                detail_rows = cursor.execute(
                    "SELECT * FROM region_talk.bloggers_ru_v1 WHERE blogger_id=ANY(%s::uuid[])",
                    (selected,),
                ).fetchall()
                rows_by_id = {str(row["blogger_id"]): row for row in detail_rows}
            rows = [
                {**rows_by_id[hit.document_id], "rrf": hit.model_dump(mode="json")}
                for hit in fused.hits[:requested]
                if hit.document_id in rows_by_id
            ]
            return {
                "items": rows,
                "cursor": None,
                "canonical_revision": index_revision,
                "retrievers": {
                    "requested": list(fused.coverage.retrievers_requested),
                    "completed": list(fused.coverage.retrievers_completed),
                    "unavailable": [item.name for item in fused.coverage.retrievers_unavailable],
                    "details": fused.coverage.model_dump(mode="json"),
                    "rrf_k": fused.rrf_k,
                },
                "embedding_coverage": coverage,
                "complete": fused.coverage.is_complete,
            }
        if tool == "data.query":
            parameters = arguments.get("parameters", [])
            if not isinstance(parameters, list):
                raise SessionBrokerError("data.query parameters must be an array")
            sql = str(arguments.get("sql", ""))
            if parameters:
                # The public contract uses native PostgreSQL $1..$N placeholders.
                # PREPARE keeps the already AST-classified statement intact while
                # EXECUTE binds JSON values through psycopg instead of interpolating.
                cursor.execute("PREPARE mdh_bounded_query AS " + sql)
                placeholders = ",".join("%s" for _ in parameters)
                cursor.execute(f"EXECUTE mdh_bounded_query ({placeholders})", parameters)
            else:
                cursor.execute(sql)
            columns = [column.name for column in cursor.description or ()]
            rows = cursor.fetchmany(limit + 1)
            truncated = len(rows) > limit
            return {"columns": columns, "rows": rows[:limit], "truncated": truncated}
        raise SessionBrokerError("tool is not implemented by the read-only PostgreSQL broker")


def _jsonable(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
    if hasattr(value, "hex") and value.__class__.__module__ == "uuid":
        return str(value)
    return value
