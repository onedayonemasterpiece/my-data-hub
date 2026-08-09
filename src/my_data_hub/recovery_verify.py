"""Post-restore structural and bounded MCP-read verification."""

from __future__ import annotations

import argparse
import json
import os

from my_data_hub.db.health import verify_database
from my_data_hub.mcp.service import HubService


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--database-url", default=os.getenv("MY_DATA_HUB_RESTORE_DATABASE_URL", "")
    )
    args = parser.parse_args()
    if not args.database_url:
        parser.error("--database-url or MY_DATA_HUB_RESTORE_DATABASE_URL is required")

    findings: list[str] = []
    health = verify_database(args.database_url)
    findings.extend(health.findings)
    evidence: dict[str, object] = {
        "schema_revision": health.schema_revision,
        "canonical_revision": health.canonical_revision,
        "extensions": list(health.extensions),
    }
    import psycopg

    with psycopg.connect(args.database_url) as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT to_regclass('integration.batch'),
                   to_regclass('sync.external_outbox'),
                   to_regclass('recovery.evidence'),
                   to_regprocedure('hub.advance_canonical_revision(bigint)')
            """
        )
        objects = cursor.fetchone()
        evidence["required_objects"] = [str(value) if value else None for value in objects]
        if any(value is None for value in objects):
            findings.append("required restored relation/function is missing")
        cursor.execute(
            """
            SELECT count(*) FROM sync.external_outbox
            WHERE required_revision > (
                SELECT canonical_revision FROM hub.canonical_state WHERE singleton = true
            )
            """
        )
        invalid_outbox = int(cursor.fetchone()[0])
        evidence["outbox_ahead_of_canonical"] = invalid_outbox
        if invalid_outbox:
            findings.append("semantic outbox contains a revision ahead of canonical state")
        cursor.execute(
            "SELECT count(*) FROM orchestration.pipeline WHERE workload='region-talk' AND status='paused'"
        )
        paused = int(cursor.fetchone()[0])
        evidence["region_talk_paused_pipelines"] = paused
        if paused != 1:
            findings.append("Region Talk pipeline is not uniquely paused")

    mcp_health = HubService(
        args.database_url, scopes=frozenset({"hub:read"}), write_enabled=False
    ).health()
    evidence["bounded_mcp_health"] = mcp_health
    if not mcp_health.get("ok") or mcp_health.get("write_enabled") is not False:
        findings.append("bounded MCP health verification failed")
    report = {"ok": not findings, "findings": findings, "evidence": evidence}
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return 0 if not findings else 2


if __name__ == "__main__":
    raise SystemExit(main())
