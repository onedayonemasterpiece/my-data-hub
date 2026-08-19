"""Epoch-bound direct PostgreSQL sessions for the remote MCP reader.

The broker never knows a static master URL.  A tunnel/session registrar writes a
short-lived credential envelope for one ACTIVE master epoch into the service
secret directory.  The MCP process resolves that exact envelope for every tool
call and connects directly through the loopback reverse tunnel.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import parse_qs, urlsplit

from my_data_hub.hashing import canonical_json_bytes
from my_data_hub.mcp.contracts import MasterSession, MasterSessionBroker, SessionRequest
from my_data_hub.mcp.sql_policy import BoundedSQLPolicy, change_request_sha256


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
        if parsed.hostname not in {
            "127.0.0.1",
            "::1",
            "localhost",
            "postgres-master.internal",
            "master-tunnel.internal",
        }:
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
        self.prune(
            now=datetime.now(UTC),
            active_master_instance_id=credential.master_instance_id,
            active_epoch=credential.epoch,
        )
        return path

    def prune(
        self,
        *,
        now: datetime,
        active_master_instance_id: str | None = None,
        active_epoch: int | None = None,
        max_files: int = 1_000,
    ) -> int:
        """Remove expired or superseded plaintext credential envelopes."""

        if not self.root.exists():
            return 0
        if self.root.is_symlink() or not self.root.is_dir() or self.root.stat().st_mode & 0o077:
            raise SessionBrokerError("master session secret directory must be a private regular directory")
        removed = 0
        for index, path in enumerate(sorted(self.root.iterdir())):
            if index >= max_files:
                raise SessionBrokerError("master session secret directory exceeds the cleanup bound")
            if path.is_symlink() or not path.is_file() or path.stat().st_mode & 0o077:
                continue
            delete = False
            try:
                if path.stat().st_size > 16 * 1024:
                    delete = True
                else:
                    payload = json.loads(path.read_text(encoding="utf-8"))
                    expires_at = datetime.fromisoformat(
                        str(payload["expires_at"]).replace("Z", "+00:00")
                    )
                    delete = expires_at.tzinfo is None or expires_at.astimezone(UTC) <= now.astimezone(UTC)
                    if (
                        not delete
                        and active_master_instance_id is not None
                        and active_epoch is not None
                        and (
                            str(payload.get("master_instance_id")) != active_master_instance_id
                            or int(payload.get("epoch", 0)) != active_epoch
                        )
                    ):
                        delete = True
            except (KeyError, TypeError, ValueError, OSError, json.JSONDecodeError):
                delete = True
            if delete:
                path.unlink(missing_ok=True)
                removed += 1
        return removed


_ROLE_GROUPS = {
    "reader": "mdh_mcp_reader",
    "migration_operator": "mdh_migration_operator",
    "operator": "mdh_mcp_editor",
    "provider_operator": "mdh_mcp_editor",
    "connector": "mdh_connector_intake",
    "canonical_committer": "mdh_canonical_committer",
}


class PostgresMasterSessionBroker(MasterSessionBroker):
    def __init__(
        self,
        source: EpochCredentialSource,
        *,
        sql_policy: BoundedSQLPolicy | None = None,
    ) -> None:
        self.source = source
        self.sql_policy = sql_policy or BoundedSQLPolicy()

    def issue_session(self, request: SessionRequest) -> MasterSession:
        if request.role not in _ROLE_GROUPS:
            raise SessionBrokerError("requested MCP role is not brokerable")
        credential = self.source.load(request)
        credential.validate(request, now=datetime.now(UTC))
        return PostgresMasterSession(request, credential, sql_policy=self.sql_policy)


class PostgresMasterSession(MasterSession):
    def __init__(
        self,
        request: SessionRequest,
        credential: EpochDatabaseCredential,
        *,
        sql_policy: BoundedSQLPolicy | None = None,
    ) -> None:
        self.request = request
        self.credential = credential
        self.sql_policy = sql_policy or BoundedSQLPolicy()
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
            is_change = self.request.tool in {
                "data.change.preview",
                "data.change.apply",
                "bloggers.import.preview",
                "bloggers.import.apply",
            }
            is_reconciliation = self.request.tool in {
                "data.change.reconcile",
                "bloggers.import.reconcile",
            }
            cursor.execute("SET TRANSACTION READ WRITE" if is_change else "SET TRANSACTION READ ONLY")
            self._set_local_timeouts(cursor)
            cursor.execute("SET LOCAL ROLE " + _ROLE_GROUPS[self.request.role])
            self._assert_restricted_login(cursor)
            if self.request.tool == "submit_discovery_batch":
                rows = self._dispatch_discovery_submission(arguments)
            elif self.request.tool.startswith("bloggers.import."):
                rows = self._dispatch_blogger_import(cursor, arguments)
            elif is_change:
                rows = self._dispatch_change(cursor, arguments)
            elif is_reconciliation:
                rows = self._dispatch_change_reconciliation(cursor, arguments)
            else:
                rows = self._dispatch(cursor, arguments)
            canonical_revision = self._canonical_revision(cursor)
            if self.request.tool in {"data.change.apply", "bloggers.import.preview", "bloggers.import.apply"}:
                connection.commit()
            else:
                connection.rollback()
        result = {
            **rows,
            "canonical_revision": canonical_revision,
            "master_epoch": self.request.epoch,
        }
        encoded = canonical_json_bytes(_jsonable(result))
        if len(encoded) > self.request.limits.max_bytes:
            raise SessionBrokerError("master response exceeds the broker byte cap")
        return _jsonable(result)

    def _canonical_revision(self, cursor: Any) -> int:
        """Return the revision without granting readers epoch-control authority.

        The credential envelope and loopback tunnel are already bound to the
        resolved ACTIVE master instance and epoch.  Canonical write functions
        perform their own database-side epoch checks.  Reading
        ``master_control.epoch_state`` here was both redundant and invalid for
        the deliberately restricted reader/connector roles.

        Connector intake does not mutate canonical state and intentionally has
        no access to ``hub.canonical_state``.  For that role the control-plane
        snapshot carried in the session request is the exact bounded response
        metadata.  All roles that may observe or mutate canonical data read the
        post-dispatch revision from the canonical singleton itself.
        """

        if self.request.role == "connector":
            if self.request.canonical_revision is None:
                raise SessionBrokerError(
                    "connector session omitted the resolved canonical revision"
                )
            return self.request.canonical_revision
        row = cursor.execute(
            "SELECT canonical_revision FROM hub.canonical_state WHERE singleton=true"
        ).fetchone()
        if row is None:
            raise SessionBrokerError("canonical revision singleton is absent")
        value = row["canonical_revision"]
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise SessionBrokerError("canonical revision singleton is invalid")
        return value

    def _set_local_timeouts(self, cursor: Any) -> None:
        """Set transaction-local timeouts without parameterizing SET syntax.

        PostgreSQL's ``SET ... = value`` production does not accept a bind
        placeholder. ``set_config`` accepts a bound text value and its final
        ``true`` argument keeps each setting transaction-local.
        """

        values = (
            ("statement_timeout", self.request.limits.timeout_ms),
            ("lock_timeout", min(2_000, self.request.limits.timeout_ms)),
            ("idle_in_transaction_session_timeout", self.request.limits.timeout_ms),
        )
        for setting, milliseconds in values:
            cursor.execute(
                f"SELECT pg_catalog.set_config('{setting}', %s, true)",
                (f"{milliseconds}ms",),
            )

    def _dispatch_discovery_submission(self, arguments: dict[str, Any]) -> dict[str, Any]:
        if self.request.role != "connector" or set(arguments) != {"payload"}:
            raise SessionBrokerError("blogger discovery intake requires one closed payload")
        raw = arguments.get("payload")
        if not isinstance(raw, Mapping):
            raise SessionBrokerError("blogger discovery payload must be an object")
        from my_data_hub.connectors.postgres import PostgresConnectorAcceptanceRepository
        from my_data_hub.connectors.service import ConnectorIntakeService
        from my_data_hub.workloads.bloggers.discovery import validate_submit_discovery_batch

        request = validate_submit_discovery_batch(raw)
        envelope = request.connector_envelope()
        decision = ConnectorIntakeService(
            PostgresConnectorAcceptanceRepository(
                self.credential.database_url, session_role="mdh_connector_intake"
            )
        ).submit(
            request.connector_envelope_bytes(),
            authenticated_connector_id=envelope.connector_id,
            authenticated_principal=f"service:{envelope.connector_id}",
            correlation_id=request.request_sha256[:32],
            artifact_record_count=request.record_count if request.artifact is not None else None,
        )
        response: dict[str, Any] = {
            "batch_id": str(request.batch_id),
            "request_sha256": request.request_sha256,
            "payload_sha256": request.semantic_payload_sha256,
            "record_count": request.record_count,
            "delivery_mode": request.delivery_mode.value,
            "disposition": decision.disposition.value,
        }
        if decision.receipt is not None:
            response["receipt"] = decision.receipt.model_dump(mode="json")
        if decision.quarantine is not None:
            response["quarantine"] = _jsonable(asdict(decision.quarantine))
        if request.artifact is not None:
            response.update(
                materialization_required=True,
                materialization_state="AWAITING_VERIFIED_PROVIDER_MATERIALIZER",
            )
        return response

    def _dispatch_blogger_import(
        self, cursor: Any, arguments: dict[str, Any]
    ) -> dict[str, Any]:
        if self.request.role != "canonical_committer":
            raise SessionBrokerError("blogger import requires canonical_committer credential")
        from my_data_hub.workloads.bloggers.discovery_postgres import (
            BloggerDiscoveryPostgres,
            BloggerImportIdentity,
        )

        required = {
            "operation_id",
            "batch_id",
            "request_sha256",
            "expected_revision",
            "principal_id",
            "client_id",
        }
        permit = arguments.get("_write_permit")
        if self.request.tool != "bloggers.import.reconcile":
            required = required | {"_write_permit"}
            if not isinstance(permit, dict) or set(permit) != {
                "permit_id", "tool", "master_epoch", "canonical_revision", "expires_at"
            }:
                raise SessionBrokerError("blogger import lacks an exact write permit")
            if (
                permit["permit_id"] != arguments.get("operation_id")
                or permit["tool"] != self.request.tool
                or permit["master_epoch"] != self.request.epoch
                or permit["canonical_revision"] != arguments.get("expected_revision")
                or int(permit["expires_at"]) <= int(datetime.now(UTC).timestamp())
            ):
                raise SessionBrokerError("blogger import permit is stale or mismatched")
        if self.request.tool in {"bloggers.import.apply", "bloggers.import.reconcile"}:
            required = required | {"plan_sha256"}
        if self.request.tool == "bloggers.import.reconcile":
            required = required | {"master_instance_id", "master_epoch"}
        if set(arguments) != required:
            raise SessionBrokerError("blogger import arguments differ from the fixed contract")
        if (
            arguments["principal_id"] != self.request.principal.subject
            or arguments["client_id"] != self.request.principal.client_id
        ):
            raise SessionBrokerError("blogger import identity differs from the session")
        from uuid import UUID

        identity = BloggerImportIdentity(
            batch_id=UUID(str(arguments["batch_id"])),
            operation_id=str(arguments["operation_id"]),
            request_sha256=str(arguments["request_sha256"]),
            expected_revision=int(arguments["expected_revision"]),
            principal_id=str(arguments["principal_id"]),
            client_id=str(arguments["client_id"]),
        )
        if self.request.tool == "bloggers.import.preview":
            receipt = BloggerDiscoveryPostgres.preview(cursor, identity)
            return {
                "operation_id": receipt.operation_id,
                "batch_id": str(receipt.batch_id),
                "request_sha256": receipt.request_sha256,
                "plan_sha256": receipt.plan_sha256,
                "expected_revision": receipt.expected_revision,
                "summary": {
                    "create_actor_count": receipt.create_actor_count,
                    "link_existing_count": receipt.link_existing_count,
                    "quarantine_count": receipt.quarantine_count,
                    "account_count": receipt.account_count,
                },
                "status": "PREVIEW_EXECUTED",
            }
        if self.request.tool == "bloggers.import.apply":
            receipt = BloggerDiscoveryPostgres.apply(
                cursor, identity, plan_sha256=str(arguments["plan_sha256"])
            )
        else:
            receipt = BloggerDiscoveryPostgres.reconcile(
                cursor,
                identity,
                plan_sha256=str(arguments["plan_sha256"]),
                master_instance_id=UUID(str(arguments["master_instance_id"])),
                master_epoch=int(arguments["master_epoch"]),
            )
            if receipt is None:
                return {"found": False, "operation_id": identity.operation_id}
        result = {
            "found": True,
            "operation_id": receipt.operation_id,
            "batch_id": str(receipt.batch_id),
            "plan_sha256": receipt.plan_sha256,
            "affected_rows": receipt.affected_rows,
            "committed_revision": receipt.revision_after,
            "duplicate": receipt.duplicate,
            "request_sha256": identity.request_sha256,
            "receipt_master_instance_id": str(arguments.get("master_instance_id", self.request.master_instance_id)),
            "receipt_master_epoch": int(arguments.get("master_epoch", self.request.epoch)),
            "expected_revision": identity.expected_revision,
            "principal_id": identity.principal_id,
            "client_id": identity.client_id,
            "status": "COMMITTED_PENDING_CHECKPOINT",
        }
        if receipt.committed_at is not None:
            result["committed_at"] = receipt.committed_at
        return result

    def _assert_restricted_login(self, cursor: Any) -> None:
        row = cursor.execute(
            "SELECT r.rolsuper,r.rolcreatedb,r.rolcreaterole,r.rolreplication,r.rolbypassrls,"
            "pg_has_role(session_user,%s,'member') AS requested_member,"
            "pg_has_role(session_user,'mdh_owner','member') AS owner_member "
            "FROM pg_roles r WHERE r.rolname=session_user",
            (_ROLE_GROUPS[self.request.role],),
        ).fetchone()
        if (
            row is None
            or any(bool(row[name]) for name in (
                "rolsuper", "rolcreatedb", "rolcreaterole", "rolreplication", "rolbypassrls",
                "owner_member",
            ))
            or not bool(row["requested_member"])
        ):
            raise SessionBrokerError("master credential is not an exact restricted role login")

    def _dispatch_change(self, cursor: Any, arguments: dict[str, Any]) -> dict[str, Any]:
        if self.request.role != "operator":
            raise SessionBrokerError("data changes require the restricted operator credential")
        permit = arguments.get("_write_permit")
        if not isinstance(permit, dict) or set(permit) != {
            "permit_id", "tool", "master_epoch", "canonical_revision", "expires_at"
        }:
            raise SessionBrokerError("data change lacks an exact write permit")
        if (
            permit["tool"] != self.request.tool
            or permit["master_epoch"] != self.request.epoch
            or permit["canonical_revision"] != arguments.get("expected_revision")
            or not isinstance(permit["expires_at"], int)
            or permit["expires_at"] <= int(datetime.now(UTC).timestamp())
        ):
            raise SessionBrokerError("data change permit is stale or mismatched")
        parameters = arguments.get("parameters", [])
        if not isinstance(parameters, list):
            raise SessionBrokerError("data change parameters must be an array")
        classified = self.sql_policy.classify_change(str(arguments.get("sql", "")), parameters)
        from my_data_hub.db_operator import compile_psycopg_parameters

        query, bound = compile_psycopg_parameters(str(arguments["sql"]), parameters)
        cursor.execute("SELECT master_control.assert_session_write_epoch()")
        state = cursor.execute(
            "SELECT canonical_revision FROM hub.canonical_state WHERE singleton=true FOR SHARE"
        ).fetchone()
        if state is None or int(state["canonical_revision"]) != int(arguments["expected_revision"]):
            raise SessionBrokerError("canonical revision changed after operator preview")
        cursor.execute(query, bound)
        affected = int(cursor.rowcount)
        maximum = int(arguments.get("max_affected_rows", 0))
        if not 0 <= affected <= maximum <= 1_000:
            raise SessionBrokerError("data change affected rows outside the permitted bound")
        if self.request.tool == "data.change.apply":
            if affected == 0:
                raise SessionBrokerError("data change apply must affect at least one row")
            parameter_sha256 = hashlib.sha256(canonical_json_bytes(parameters)).hexdigest()
            revision = cursor.execute(
                "SELECT operator_control.commit_mcp_change_v2("
                "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) AS canonical_revision",
                (
                    str(permit["permit_id"]),
                    change_request_sha256(arguments),
                    int(arguments["expected_revision"]),
                    str(classified.target),
                    classified.kind,
                    affected,
                    classified.sql_sha256,
                    parameter_sha256,
                    self.request.principal.subject,
                    self.request.principal.client_id,
                ),
            ).fetchone()
            if revision is None:
                raise SessionBrokerError("operator change did not return a canonical revision")
        return {
            "operation_id": str(permit["permit_id"]),
            "affected_rows": affected,
            "target": classified.target,
            "status": (
                "PREVIEW_EXECUTED_ROLLED_BACK"
                if self.request.tool == "data.change.preview"
                else "COMMITTED_PENDING_CHECKPOINT"
            ),
        }

    def _dispatch_change_reconciliation(
        self, cursor: Any, arguments: dict[str, Any]
    ) -> dict[str, Any]:
        if self.request.role != "operator" or set(arguments) != {
            "operation_id",
            "request_sha256",
            "master_instance_id",
            "master_epoch",
            "expected_revision",
            "principal_id",
            "client_id",
        }:
            raise SessionBrokerError("operator reconciliation requires exact bounded identity")
        if (
            arguments["master_instance_id"] != self.request.master_instance_id
            or arguments["master_epoch"] != self.request.epoch
            or arguments["principal_id"] != self.request.principal.subject
            or arguments["client_id"] != self.request.principal.client_id
        ):
            raise SessionBrokerError("operator reconciliation identity differs from the session")
        row = cursor.execute(
            "SELECT affected_rows,revision_after,committed_at "
            "FROM operator_control.reconcile_mcp_change(%s,%s,%s::uuid,%s,%s,%s,%s)",
            (
                arguments["operation_id"],
                arguments["request_sha256"],
                arguments["master_instance_id"],
                arguments["master_epoch"],
                arguments["expected_revision"],
                arguments["principal_id"],
                arguments["client_id"],
            ),
        ).fetchone()
        if row is None:
            return {"found": False, "operation_id": arguments["operation_id"]}
        return {
            "found": True,
            "operation_id": arguments["operation_id"],
            "request_sha256": arguments["request_sha256"],
            "master_instance_id": arguments["master_instance_id"],
            "master_epoch": arguments["master_epoch"],
            "expected_revision": arguments["expected_revision"],
            "principal_id": arguments["principal_id"],
            "client_id": arguments["client_id"],
            "affected_rows": int(row["affected_rows"]),
            "committed_revision": int(row["revision_after"]),
            "committed_at": row["committed_at"],
        }

    def _dispatch(self, cursor: Any, arguments: dict[str, Any]) -> dict[str, Any]:
        tool = self.request.tool
        limit = self.request.limits.max_rows
        if tool == "runtime.stale_epoch.probe":
            cursor.execute("SELECT current_epoch FROM master_control.epoch_state WHERE singleton=true")
            row = cursor.fetchone()
            return {"observed_epoch": int(row["current_epoch"]) if row else None}
        if tool == "bloggers.list":
            from my_data_hub.workloads.bloggers.discovery_reader import (
                BloggerDiscoveryReader,
                BloggerSearchRequest,
            )

            requested = min(int(arguments.get("limit", 50)), limit, 100)
            rows = BloggerDiscoveryReader.search(
                cursor,
                BloggerSearchRequest(
                    project_slug=str(arguments.get("project_slug", "")),
                    limit=requested,
                    after_name=arguments.get("after_name"),
                    after_blogger_id=arguments.get("after_blogger_id"),
                ),
            )
            return {
                "items": [asdict(row) for row in rows],
                "next_cursor": (
                    {
                        "after_name": rows[-1].display_name,
                        "after_blogger_id": str(rows[-1].blogger_id),
                    }
                    if len(rows) == requested
                    else None
                ),
                "complete": len(rows) < requested,
            }
        if tool == "bloggers.get":
            row = cursor.execute(
                "SELECT b.blogger_id,b.display_name,b.actor_kind,b.public_description,"
                "b.public_accounts,b.requires_review FROM hub.bloggers_v1 b "
                "JOIN hub.project p ON p.project_id=b.project_id "
                "WHERE p.slug=%s AND b.blogger_id=%s::uuid",
                (arguments.get("project_slug"), arguments.get("blogger_id")),
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
                "FROM hub.bloggers_v1 b JOIN hub.project p ON p.project_id=b.project_id "
                "WHERE p.slug=%s",
                (arguments.get("project_slug"),),
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
            from my_data_hub.workloads.bloggers.discovery_reader import (
                BloggerDiscoveryReader,
                BloggerSearchRequest,
            )

            requested = min(int(arguments.get("limit", 20)), limit, 100)
            request = BloggerSearchRequest.model_validate(
                {
                    "project_slug": arguments.get("project_slug"),
                    "query": arguments.get("query"),
                    "limit": requested,
                    "after_name": arguments.get("after_name"),
                    "after_blogger_id": arguments.get("after_blogger_id"),
                }
            )
            rows = BloggerDiscoveryReader.search(cursor, request)
            return {
                "items": [asdict(row) for row in rows],
                "next_cursor": (
                    {
                        "after_name": rows[-1].display_name,
                        "after_blogger_id": str(rows[-1].blogger_id),
                    }
                    if len(rows) == requested
                    else None
                ),
                "complete": len(rows) < requested,
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
