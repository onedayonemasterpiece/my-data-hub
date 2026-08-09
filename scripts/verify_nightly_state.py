#!/usr/bin/env python3
"""Fail-closed read-only checks for queue, connector cadence, recovery and inventory."""

from __future__ import annotations

import argparse
import json
import os


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--database-url", default=os.getenv("MY_DATA_HUB_MONITORING_DATABASE_URL", "")
    )
    args = parser.parse_args()
    if not args.database_url:
        parser.error("MY_DATA_HUB_MONITORING_DATABASE_URL or --database-url is required")

    import psycopg

    findings: list[str] = []
    with psycopg.connect(
        args.database_url, connect_timeout=3
    ) as connection, connection.cursor() as cursor:
        cursor.execute("SET TRANSACTION READ ONLY")
        cursor.execute("SET LOCAL statement_timeout = '2000ms'")
        cursor.execute(
            "SELECT coalesce(sum(expired_lease_count), 0), "
            "coalesce(max(max_attempt_count), 0) FROM orchestration.queue_health"
        )
        expired_leases, max_attempts = (int(value or 0) for value in cursor.fetchone())
        if expired_leases:
            findings.append("queue contains expired leases")
        cursor.execute(
            """
            SELECT c.connector_id, c.expected_cadence, max(b.committed_at)
            FROM integration.connector c
            LEFT JOIN integration.batch b ON b.connector_id = c.connector_id
            WHERE c.status = 'active' AND c.expected_cadence IS NOT NULL
            GROUP BY c.connector_id, c.expected_cadence
            ORDER BY c.connector_id
            """
        )
        connector_rows = cursor.fetchall()
        stale_connectors = [
            str(connector_id)
            for connector_id, cadence, committed_at in connector_rows
            if committed_at is None
            or connection.execute(
                "SELECT %s < now() - (%s * 2)", (committed_at, cadence)
            ).fetchone()[0]
        ]
        if stale_connectors:
            findings.append("stale active connector cadence: " + ", ".join(stale_connectors))
        cursor.execute(
            """
            SELECT count(*),
                   count(*) FILTER (WHERE control_class = 'orchestrator_protected'),
                   max(last_observed_at)
            FROM integration.provider_resource
            WHERE lifecycle_state <> 'deleted'
            """
        )
        inventory_count, protected_count, inventory_at = cursor.fetchone()
        if int(inventory_count) == 0:
            findings.append("bounded provider inventory is empty")
        if int(protected_count) == 0:
            findings.append("no orchestrator_protected provider resource is registered")
        if inventory_at is None:
            findings.append("provider inventory has no observation timestamp")
        else:
            cursor.execute("SELECT %s < now() - interval '2 days'", (inventory_at,))
            if bool(cursor.fetchone()[0]):
                findings.append("provider inventory is older than two days")
        cursor.execute(
            """
            SELECT completed_at
            FROM recovery.evidence
            WHERE evidence_type = 'isolated_restore' AND status = 'passed'
              AND readback_verified AND restore_verified
            ORDER BY completed_at DESC LIMIT 1
            """
        )
        restore_row = cursor.fetchone()
        if restore_row is None:
            findings.append("no passed isolated restore evidence exists")

    report = {
        "ok": not findings,
        "findings": findings,
        "queue": {"expired_leases": expired_leases, "max_attempts": max_attempts},
        "active_connector_count": len(connector_rows),
        "stale_connectors": stale_connectors,
        "provider_inventory": {
            "resource_count": int(inventory_count),
            "protected_count": int(protected_count),
            "last_observed_at": inventory_at.isoformat() if inventory_at else None,
        },
        "latest_restore_at": restore_row[0].isoformat() if restore_row else None,
    }
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if not findings else 2


if __name__ == "__main__":
    raise SystemExit(main())
