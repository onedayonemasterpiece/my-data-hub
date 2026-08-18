"""Transactional positive and adversarial PostgreSQL ACL probes.

The caller owns the administrative connection.  Every disposable object and every
successful write is rolled back before the bounded, secret-free result is returned.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any

from my_data_hub.hashing import canonical_json_bytes


@dataclass(frozen=True, slots=True)
class Probe:
    role: str
    name: str
    expected: str
    passed: bool
    sqlstate: str | None = None
    detail: str | None = None


def _set_role(cursor: Any, role: str) -> None:
    from psycopg import sql

    cursor.execute(sql.SQL("SET ROLE {}").format(sql.Identifier(role)))
    cursor.execute("SELECT current_user")
    current = str(cursor.fetchone()[0])
    if current != role:
        raise RuntimeError(f"SET ROLE identity mismatch: expected={role}, actual={current}")


def _positive(cursor: Any, role: str, name: str, statement: str) -> Probe:
    cursor.execute("RESET ROLE")
    _set_role(cursor, role)
    cursor.execute("SAVEPOINT positive_probe")
    try:
        cursor.execute(statement)
        cursor.fetchall() if cursor.description else None
        cursor.execute("RELEASE SAVEPOINT positive_probe")
        return Probe(role, name, "allow", True)
    except Exception as exc:  # pragma: no cover - exercised by live PostgreSQL job
        cursor.execute("ROLLBACK TO SAVEPOINT positive_probe")
        cursor.execute("RELEASE SAVEPOINT positive_probe")
        return Probe(role, name, "allow", False, getattr(exc, "sqlstate", None), str(exc).splitlines()[0])
    finally:
        cursor.execute("RESET ROLE")


def _negative(cursor: Any, role: str, name: str, statement: str) -> Probe:
    cursor.execute("RESET ROLE")
    _set_role(cursor, role)
    cursor.execute("SAVEPOINT negative_probe")
    try:
        cursor.execute(statement)
    except Exception as exc:  # PostgreSQL permission denial is the expected boundary
        cursor.execute("ROLLBACK TO SAVEPOINT negative_probe")
        cursor.execute("RELEASE SAVEPOINT negative_probe")
        cursor.execute("RESET ROLE")
        sqlstate = getattr(exc, "sqlstate", None)
        return Probe(
            role,
            name,
            "deny",
            sqlstate in {"42501", "P0001"},
            sqlstate,
            str(exc).splitlines()[0],
        )
    cursor.execute("ROLLBACK TO SAVEPOINT negative_probe")
    cursor.execute("RELEASE SAVEPOINT negative_probe")
    cursor.execute("RESET ROLE")
    return Probe(role, name, "deny", False, detail="statement unexpectedly succeeded")


def run_role_security_probes(connection: Any) -> dict[str, Any]:
    probes: list[Probe] = []
    with connection.cursor() as cursor:
        cursor.execute("BEGIN")
        cursor.execute("CREATE SCHEMA operator_disposable")
        cursor.execute("CREATE TABLE operator_disposable.probe (id integer PRIMARY KEY, value text NOT NULL)")
        cursor.execute("GRANT USAGE ON SCHEMA operator_disposable TO mdh_mcp_editor")
        cursor.execute("GRANT SELECT, INSERT, UPDATE, DELETE ON operator_disposable.probe TO mdh_mcp_editor")
        cursor.execute(
            """
            INSERT INTO orchestration.pipeline
                (pipeline_id, workload, name, version, status, definition)
            VALUES
                ('00000000-0000-4000-8000-000000000090', 'role-probe', 'role-probe',
                 'v1', 'active', '{}'::jsonb)
            """
        )
        cursor.execute(
            """
            INSERT INTO orchestration.run (run_id, pipeline_id, run_kind, canonical_revision)
            VALUES ('00000000-0000-4000-8000-000000000091',
                    '00000000-0000-4000-8000-000000000090', 'manual', 0)
            """
        )

        probes.extend(
            [
                _positive(
                    cursor,
                    "mdh_mcp_reader",
                    "bounded application read",
                    "SELECT schema_revision FROM hub.canonical_state",
                ),
                _positive(
                    cursor,
                    "mdh_canonical_committer",
                    "bounded canonical compare-and-swap",
                    "SELECT hub.advance_canonical_revision((SELECT canonical_revision "
                    "FROM hub.canonical_state WHERE singleton))",
                ),
                _positive(
                    cursor,
                    "mdh_authenticator",
                    "revocation lookup",
                    "SELECT count(*) FROM auth.oauth_revocation",
                ),
                _positive(
                    cursor,
                    "mdh_connector_intake",
                    "connector registry read",
                    "SELECT connector_id FROM integration.connector LIMIT 1",
                ),
                _positive(
                    cursor,
                    "mdh_monitoring",
                    "health projection read",
                    "SELECT schema_revision FROM hub.canonical_state",
                ),
                _positive(
                    cursor,
                    "mdh_application",
                    "worker intake orchestration read",
                    "SELECT stage_id FROM orchestration.pipeline_stage LIMIT 1",
                ),
                _positive(
                    cursor,
                    "mdh_application",
                    "worker result intake write",
                    "INSERT INTO orchestration.worker_result_inbox "
                    "(result_id, run_id, workload, stage_key, stage_contract_version, "
                    "input_manifest_sha256, result_sha256, byte_size, producer, "
                    "result_status, envelope) VALUES "
                    "('00000000-0000-4000-8000-000000000092', "
                    "'00000000-0000-4000-8000-000000000091', 'probe', 'probe', 'v1', "
                    "repeat('a',64), repeat('b',64), 0, '{}'::jsonb, 'succeeded', '{}'::jsonb)",
                ),
                _positive(
                    cursor,
                    "mdh_application",
                    "worker intake audit write",
                    "INSERT INTO sync.audit_event "
                    "(actor_id, client_id, action, outcome, details) VALUES "
                    "('role-probe', 'application', 'probe', 'passed', '{}'::jsonb)",
                ),
                _positive(
                    cursor,
                    "mdh_orchestrator",
                    "queue read",
                    "SELECT count(*) FROM orchestration.work_item",
                ),
                _positive(
                    cursor,
                    "mdh_orchestrator",
                    "orchestrator audit write",
                    "INSERT INTO sync.audit_event "
                    "(actor_id, client_id, action, outcome, details) VALUES "
                    "('role-probe', 'orchestrator', 'probe', 'passed', '{}'::jsonb)",
                ),
                _positive(
                    cursor,
                    "mdh_migration_operator",
                    "bounded export batch landing",
                    "INSERT INTO migration.export_batch "
                    "(export_batch_id, source_system, source_database, source_tables, "
                    "source_scope, schema_version, consistency_mode, expected_row_count, "
                    "manifest_sha256) VALUES "
                    "('00000000-0000-4000-8000-000000000093', 'ydb', 'probe', '[]'::jsonb, "
                    "'probe', 'v1', 'bounded_fixture', 0, repeat('c',64))",
                ),
                _positive(
                    cursor,
                    "mdh_backup",
                    "backup read-all-data",
                    "SELECT schema_revision FROM hub.canonical_state",
                ),
                _positive(
                    cursor,
                    "mdh_mcp_editor",
                    "disposable insert",
                    "INSERT INTO operator_disposable.probe VALUES (1, 'ok') RETURNING id",
                ),
                _positive(
                    cursor,
                    "mdh_mcp_editor",
                    "disposable update",
                    "UPDATE operator_disposable.probe SET value = 'updated' WHERE id = 1 RETURNING id",
                ),
                _positive(
                    cursor,
                    "mdh_mcp_editor",
                    "disposable delete",
                    "DELETE FROM operator_disposable.probe WHERE id = 1 RETURNING id",
                ),
            ]
        )

        adversarial = {
            "permanent DDL": "CREATE TABLE hub.mcp_forbidden(id integer)",
            "temporary DDL": "CREATE TEMP TABLE mcp_forbidden(id integer)",
            "role management": "CREATE ROLE mcp_forbidden",
            "extension management": "CREATE EXTENSION hstore WITH SCHEMA public",
            "server file": "SELECT pg_read_file('/etc/passwd', 0, 1)",
            "COPY PROGRAM": "COPY (SELECT 1) TO PROGRAM 'true'",
            "migration accounting write": "DELETE FROM hub_meta.schema_migration",
            "cutover write": "DELETE FROM migration.cutover_receipt",
            "provider class write": "UPDATE integration.provider_resource SET control_class = 'mcp_managed'",
            "audit mutation": "DELETE FROM sync.audit_event",
            "operator receipt mutation": "DELETE FROM operator_control.apply_receipt",
        }
        for role in (
            "mdh_mcp_reader",
            "mdh_mcp_editor",
            "mdh_connector_intake",
            "mdh_authenticator",
        ):
            for name, statement in adversarial.items():
                probes.append(_negative(cursor, role, name, statement))
        for role in ("mdh_mcp_reader", "mdh_mcp_editor", "mdh_connector_intake"):
            probes.append(
                _negative(
                    cursor,
                    role,
                    "revocation journal read",
                    "SELECT * FROM auth.oauth_revocation",
                )
            )
        probes.append(
            _negative(
                cursor,
                "mdh_application",
                "recovery checkpoint forgery",
                "INSERT INTO sync.checkpoint "
                "(canonical_revision, checkpoint_kind, locator, sha256, manifest_sha256, "
                "postgres_major, extension_versions, encrypted) VALUES "
                "(999999, 'portable_logical', 'forbidden', repeat('a',64), repeat('b',64), "
                "18, '{}'::jsonb, true)",
            )
        )
        probes.append(
            _negative(
                cursor,
                "mdh_application",
                "canonical business row bypass",
                "DELETE FROM hub.content_item",
            )
        )
        for role in (
            "mdh_application",
            "mdh_orchestrator",
            "mdh_migration_operator",
            "mdh_backup",
            "mdh_monitoring",
        ):
            probes.extend(
                [
                    _negative(cursor, role, "role management", "CREATE ROLE mdh_forbidden"),
                    _negative(
                        cursor,
                        role,
                        "COPY PROGRAM",
                        "COPY (SELECT 1) TO PROGRAM 'true'",
                    ),
                    _negative(
                        cursor,
                        role,
                        "canonical revision bypass",
                        "UPDATE hub.canonical_state "
                        "SET canonical_revision = canonical_revision + 1",
                    ),
                ]
            )
        for role in (
            "mdh_application",
            "mdh_mcp_reader",
            "mdh_mcp_editor",
            "mdh_connector_intake",
            "mdh_canonical_committer",
        ):
            probes.extend(
                [
                    _negative(
                        cursor,
                        role,
                        "direct canonical revision update",
                        "UPDATE hub.canonical_state SET canonical_revision = canonical_revision + 1",
                    ),
                    _negative(
                        cursor,
                        role,
                        "canonical singleton delete",
                        "DELETE FROM hub.canonical_state",
                    ),
                ]
            )

        cursor.execute("RESET ROLE")
        cursor.execute(
            """
            SELECT n.nspname, c.relname, r.rolname
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_roles r ON r.oid = c.relowner
            WHERE c.relkind IN ('r', 'p', 'S', 'v', 'm')
              AND n.nspname IN (
                'hub_meta', 'hub', 'analysis', 'orchestration', 'sync', 'region_talk',
                'migration', 'joplin', 'integration', 'recovery', 'operator_control', 'auth'
              )
              AND r.rolname <> 'mdh_owner'
            ORDER BY n.nspname, c.relname
            """
        )
        wrong_owners = cursor.fetchall()
        probes.append(
            Probe(
                "mdh_owner",
                "all canonical objects owned by non-login owner",
                "allow",
                not wrong_owners,
                detail=None if not wrong_owners else str(wrong_owners[:10]),
            )
        )

        # The entire disposable probe environment and all successful DML are removed.
        cursor.execute("RESET ROLE")
        connection.rollback()

    failures = [probe for probe in probes if not probe.passed]
    return {
        "ok": not failures,
        "probe_count": len(probes),
        "role_probe_count": sum(probe.expected == "allow" for probe in probes),
        "security_probe_count": sum(probe.expected == "deny" for probe in probes),
        "failures": [asdict(probe) for probe in failures],
        "probes": [asdict(probe) for probe in probes],
        "cleanup": "transaction_rolled_back",
    }


def build_role_security_evidence(
    result: dict[str, Any],
    *,
    source_commit: str,
    master_instance_id: str,
    epoch: int,
    schema_version: int,
    canonical_revision: int,
    observed_at: datetime | None = None,
) -> dict[str, Any]:
    """Reduce the full probe output to a bounded, authenticated control receipt."""

    if not result.get("ok") or result.get("failures"):
        raise RuntimeError("PostgreSQL role/security verification failed")
    probes = result.get("probes")
    if not isinstance(probes, list) or not probes:
        raise RuntimeError("PostgreSQL role/security verification returned no probes")
    identity = {
        "source_commit": source_commit,
        "master_instance_id": master_instance_id,
        "epoch": epoch,
        "schema_version": schema_version,
        "canonical_revision": canonical_revision,
    }
    role_probes = [probe for probe in probes if probe.get("expected") == "allow"]
    security_probes = [probe for probe in probes if probe.get("expected") == "deny"]
    if (
        len(role_probes) != result.get("role_probe_count")
        or len(security_probes) != result.get("security_probe_count")
        or not role_probes
        or not security_probes
    ):
        raise RuntimeError("PostgreSQL role/security verification accounting differs")
    import hashlib

    role_receipt = {
        "contract": "my-data-hub-postgres-role-verification.v1",
        **identity,
        "cleanup": result.get("cleanup"),
        "probes": role_probes,
    }
    security_receipt = {
        "contract": "my-data-hub-postgres-security-verification.v1",
        **identity,
        "cleanup": result.get("cleanup"),
        "probes": security_probes,
    }
    timestamp = (observed_at or datetime.now(UTC)).astimezone(UTC).isoformat().replace("+00:00", "Z")
    return {
        "contract": "my-data-hub-master-security-evidence.v1",
        **identity,
        "outcome": "PASSED",
        "role_probe_count": len(role_probes),
        "security_probe_count": len(security_probes),
        "role_verification_sha256": hashlib.sha256(canonical_json_bytes(role_receipt)).hexdigest(),
        "security_test_receipt_sha256": hashlib.sha256(canonical_json_bytes(security_receipt)).hexdigest(),
        "observed_at": timestamp,
    }
