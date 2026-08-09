#!/usr/bin/env python3
"""Exercise the bounded database operator against a disposable PostgreSQL schema."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import secrets
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

from my_data_hub.db_operator import (
    BackupState,
    DatabaseAllowlist,
    DatabaseOperator,
    Function,
    PostgresOperatorJournal,
    ReceiptSigner,
)

DISPOSABLE_SCHEMA = "operator_disposable_r1"


def _operator_connection(database_url: str) -> Any:
    import psycopg

    connection = psycopg.connect(database_url, autocommit=True)
    connection.execute("SET ROLE mdh_mcp_editor")
    connection.autocommit = False
    return connection


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--database-url",
        default=os.getenv("MY_DATA_HUB_ROLE_ADMIN_DATABASE_URL", ""),
    )
    args = parser.parse_args()
    if not args.database_url:
        raise SystemExit("MY_DATA_HUB_ROLE_ADMIN_DATABASE_URL or --database-url is required")

    import psycopg

    cleanup = "not_started"
    now = datetime.now(UTC)
    evidence_id = uuid4()
    try:
        with psycopg.connect(args.database_url, autocommit=True) as connection:
            connection.execute(f"DROP SCHEMA IF EXISTS {DISPOSABLE_SCHEMA} CASCADE")
            connection.execute(f"CREATE SCHEMA {DISPOSABLE_SCHEMA}")
            connection.execute(
                f"CREATE TABLE {DISPOSABLE_SCHEMA}.items "
                "(item_id bigint PRIMARY KEY, label text NOT NULL)"
            )
            connection.execute(
                f"INSERT INTO {DISPOSABLE_SCHEMA}.items(item_id, label) VALUES (1, 'before')"
            )
            connection.execute(f"GRANT USAGE ON SCHEMA {DISPOSABLE_SCHEMA} TO mdh_mcp_editor")
            connection.execute(
                f"GRANT SELECT, INSERT, UPDATE, DELETE ON {DISPOSABLE_SCHEMA}.items "
                "TO mdh_mcp_editor"
            )
            connection.execute("GRANT USAGE ON SCHEMA hub TO mdh_mcp_editor")
            connection.execute("GRANT SELECT ON hub.canonical_state TO mdh_mcp_editor")
            connection.execute(
                """
                INSERT INTO recovery.evidence (
                    evidence_id, run_id, commit_sha, evidence_type, status,
                    artifact_sha256, readback_sha256, encrypted, private_offhost,
                    readback_verified, restore_verified, schema_revision, manifest,
                    completed_at
                ) VALUES (%s, 'disposable-operator-canary', %s, 'isolated_restore',
                    'passed', %s, %s, true, true, true, true, 10, %s::jsonb, %s)
                """,
                (
                    evidence_id,
                    "0" * 40,
                    "0" * 64,
                    "0" * 64,
                    json.dumps({"test_only": True}),
                    now - timedelta(minutes=1),
                ),
            )

        allowlist = DatabaseAllowlist.rollout_r1(
            environment="test",
            disposable_schema=DISPOSABLE_SCHEMA,
            readable_tables=("items",),
            writable_tables={"items": ("item_id", "label")},
            readable_functions=(Function("pg_catalog", "count"),),
        )
        backup = BackupState(
            evidence_revision=str(evidence_id),
            completed_at=now - timedelta(minutes=1),
            readback_verified=True,
            offsite_available=True,
            schema_revision=10,
            restore_drill_at=now - timedelta(minutes=1),
            restore_drill_succeeded=True,
        )
        operator = DatabaseOperator(
            connection_factory=lambda: _operator_connection(args.database_url),
            allowlist=allowlist,
            revision_reader=lambda cursor: int(
                cursor.execute(
                    "SELECT canonical_revision FROM hub.canonical_state WHERE singleton = true"
                ).fetchone()[0]
            ),
            backup_state_provider=lambda: backup,
            schema_revision=10,
            signer=ReceiptSigner(secrets.token_bytes(32)),
            clock=lambda: now,
            journal=PostgresOperatorJournal(
                lambda: _operator_connection(args.database_url)
            ),
        )

        read_before = operator.read(
            f"SELECT item_id, label FROM {DISPOSABLE_SCHEMA}.items ORDER BY item_id"
        )
        preview = operator.preview(
            f"UPDATE {DISPOSABLE_SCHEMA}.items SET label = $1 WHERE item_id = $2",
            params=("after", 1),
            principal="r1-disposable-canary",
            session_id="r1-disposable-session",
            correlation_id="r1-disposable-correlation",
            expected_revision=0,
            expected_row_min=1,
            expected_row_max=1,
        )
        apply_result = operator.apply(
            f"UPDATE {DISPOSABLE_SCHEMA}.items SET label = $1 WHERE item_id = $2",
            params=("after", 1),
            principal="r1-disposable-canary",
            session_id="r1-disposable-session",
            correlation_id="r1-disposable-correlation",
            preview_receipt=preview.receipt,
            idempotency_key="r1-disposable-apply-1",
        )
        replay = operator.apply(
            f"UPDATE {DISPOSABLE_SCHEMA}.items SET label = $1 WHERE item_id = $2",
            params=("after", 1),
            principal="r1-disposable-canary",
            session_id="r1-disposable-session",
            correlation_id="r1-disposable-correlation",
            preview_receipt=preview.receipt,
            idempotency_key="r1-disposable-apply-1",
        )
        read_after = operator.read(
            f"SELECT item_id, label FROM {DISPOSABLE_SCHEMA}.items ORDER BY item_id"
        )

        ddl_denied = False
        with _operator_connection(args.database_url) as connection, connection.cursor() as cursor:
            try:
                cursor.execute("CREATE TABLE hub.operator_forbidden(id bigint)")
            except psycopg.Error:
                ddl_denied = True
                connection.rollback()

        ok = (
            read_before.rows == ((1, "before"),)
            and preview.affected_rows == 1
            and apply_result.affected_rows == 1
            and not apply_result.replayed
            and replay.replayed
            and replay.receipt == apply_result.receipt
            and read_after.rows == ((1, "after"),)
            and ddl_denied
        )
        report = {
            "ok": ok,
            "scope": "disposable_schema_only",
            "schema": DISPOSABLE_SCHEMA,
            "role": "mdh_mcp_editor",
            "read_before": [list(row) for row in read_before.rows],
            "preview_affected_rows": preview.affected_rows,
            "apply_affected_rows": apply_result.affected_rows,
            "apply_receipt_sha256": hashlib.sha256(
                apply_result.receipt.encode("utf-8")
            ).hexdigest(),
            "idempotent_replay": replay.replayed,
            "read_after": [list(row) for row in read_after.rows],
            "ddl_denied_by_postgres": ddl_denied,
            "production_gate_evidence": False,
            "note": (
                "The freshness object is synthetic and valid only for this disposable canary; "
                "it must never open a production write gate."
            ),
        }
    finally:
        try:
            with psycopg.connect(args.database_url, autocommit=True) as connection:
                connection.execute(f"DROP SCHEMA IF EXISTS {DISPOSABLE_SCHEMA} CASCADE")
                connection.execute("REVOKE ALL ON SCHEMA hub FROM mdh_mcp_editor")
                connection.execute("REVOKE ALL ON hub.canonical_state FROM mdh_mcp_editor")
            cleanup = "dropped_disposable_schema_and_revoked_canary_grants"
        except Exception:
            cleanup = "failed"

    report["cleanup"] = cleanup
    report["ok"] = bool(report["ok"] and cleanup != "failed")
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["ok"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
