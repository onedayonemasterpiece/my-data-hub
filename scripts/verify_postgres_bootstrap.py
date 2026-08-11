#!/usr/bin/env python3
from __future__ import annotations

import json
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PIPELINE_PATH = ROOT / "config" / "pipelines" / "region-talk.v1.json"
EXPECTED_SCHEMAS = {
    "analysis",
    "auth",
    "hub",
    "hub_meta",
    "integration",
    "joplin",
    "migration",
    "master_control",
    "operator_control",
    "orchestration",
    "recovery",
    "region_talk",
    "sync",
    "search",
}
EXPECTED_EXTENSIONS = {"citext", "pg_trgm", "pgcrypto", "vector"}


def main() -> int:
    database_url = os.environ.get("MY_DATA_HUB_DATABASE_URL", "").strip()
    if not database_url:
        raise SystemExit("MY_DATA_HUB_DATABASE_URL is required")

    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - integration script
        raise SystemExit("psycopg is required") from exc

    pipeline = json.loads(PIPELINE_PATH.read_text(encoding="utf-8"))
    expected_stage_keys = {str(item["key"]) for item in pipeline["stages"]}

    findings: list[str] = []
    evidence: dict[str, object] = {}
    with psycopg.connect(database_url) as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT schema_name FROM information_schema.schemata "
            "WHERE schema_name = ANY(%s) ORDER BY schema_name",
            (sorted(EXPECTED_SCHEMAS),),
        )
        schemas = {str(row[0]) for row in cursor.fetchall()}
        evidence["schemas"] = sorted(schemas)
        if schemas != EXPECTED_SCHEMAS:
            findings.append(
                f"schema mismatch: missing={sorted(EXPECTED_SCHEMAS - schemas)} "
                f"extra={sorted(schemas - EXPECTED_SCHEMAS)}"
            )

        cursor.execute(
            "SELECT extname FROM pg_extension WHERE extname = ANY(%s) ORDER BY extname",
            (sorted(EXPECTED_EXTENSIONS),),
        )
        extensions = {str(row[0]) for row in cursor.fetchall()}
        evidence["extensions"] = sorted(extensions)
        if extensions != EXPECTED_EXTENSIONS:
            findings.append(
                f"extension mismatch: missing={sorted(EXPECTED_EXTENSIONS - extensions)}"
            )

        cursor.execute(
            "SELECT version, filename FROM hub_meta.schema_migration ORDER BY version"
        )
        migrations = [(int(row[0]), str(row[1])) for row in cursor.fetchall()]
        evidence["migrations"] = migrations
        expected_versions = list(range(1, 13))
        if [row[0] for row in migrations] != expected_versions:
            findings.append(f"migration history mismatch: {migrations}")

        cursor.execute(
            "SELECT schema_revision, canonical_revision "
            "FROM hub.canonical_state WHERE singleton = true"
        )
        state = cursor.fetchone()
        evidence["canonical_state"] = (
            {"schema_revision": int(state[0]), "canonical_revision": int(state[1])}
            if state
            else None
        )
        if state is None or int(state[0]) != 12 or int(state[1]) != 0:
            findings.append(f"unexpected canonical state: {state}")

        cursor.execute(
            "SELECT project_id, status FROM hub.project WHERE slug = 'region-talk'"
        )
        projects = cursor.fetchall()
        evidence["region_talk_projects"] = len(projects)
        if len(projects) != 1 or str(projects[0][1]) != "paused":
            findings.append(f"unexpected Region Talk project seed: {projects}")

        cursor.execute(
            "SELECT pipeline_id, status FROM orchestration.pipeline "
            "WHERE workload = 'region-talk' AND name = 'region-talk-main' "
            "AND version = '1.0.0'"
        )
        pipelines = cursor.fetchall()
        evidence["region_talk_pipelines"] = len(pipelines)
        if len(pipelines) != 1 or str(pipelines[0][1]) != "paused":
            findings.append(f"unexpected Region Talk pipeline registration: {pipelines}")
        else:
            pipeline_id = pipelines[0][0]
            cursor.execute(
                "SELECT stage_key, enabled FROM orchestration.pipeline_stage "
                "WHERE pipeline_id = %s ORDER BY stage_key",
                (pipeline_id,),
            )
            stage_rows = [(str(row[0]), bool(row[1])) for row in cursor.fetchall()]
            actual_stage_keys = {row[0] for row in stage_rows}
            evidence["pipeline_stage_count"] = len(stage_rows)
            if actual_stage_keys != expected_stage_keys:
                findings.append(
                    "pipeline stage mismatch: "
                    f"missing={sorted(expected_stage_keys - actual_stage_keys)} "
                    f"extra={sorted(actual_stage_keys - expected_stage_keys)}"
                )
            publication = [enabled for key, enabled in stage_rows if key == "publication_dispatch"]
            if publication != [False]:
                findings.append(
                    f"publication_dispatch must be uniquely registered and disabled: {publication}"
                )

        cursor.execute(
            """
                SELECT to_regclass('migration.raw_record'),
                       to_regclass('migration.row_disposition'),
                       to_regclass('migration.region_talk_accounting'),
                       to_regclass('orchestration.queue_health'),
                       to_regclass('region_talk.funnel_current'),
                       to_regclass('integration.batch'),
                       to_regclass('integration.provider_resource'),
                       to_regclass('recovery.evidence'),
                       to_regclass('operator_control.preview_receipt'),
                       to_regclass('auth.oauth_revocation'),
                       to_regprocedure('orchestration.claim_work_items(uuid,text,integer,integer)')
                """
        )
        objects = cursor.fetchone()
        evidence["required_objects"] = [str(value) if value is not None else None for value in objects]
        if any(value is None for value in objects):
            findings.append(f"required relation/function is missing: {objects}")

        cursor.execute(
            "SELECT expected_row_count, raw_count, dispositioned_count, "
            "undispositioned_count, quarantined_count, fully_accounted, "
            "cutover_ready FROM migration.batch_accounting"
        )
        if cursor.fetchall():
            findings.append("clean bootstrap unexpectedly contains migration accounting rows")

    report = {"ok": not findings, "findings": findings, "evidence": evidence}
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True, default=str))
    return 0 if not findings else 2


if __name__ == "__main__":
    raise SystemExit(main())
