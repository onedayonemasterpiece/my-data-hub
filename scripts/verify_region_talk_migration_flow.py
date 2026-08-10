#!/usr/bin/env python3
"""Exercise the Region Talk migration contract against a live PostgreSQL instance.

This is an integration gate, not a production migration. It proves that the lossless
landing, replay guard, accounting views, quarantine blocker and reconciliation report
work together on the PostgreSQL version used by CI/devstand.
"""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from uuid import UUID, uuid4

from my_data_hub.hashing import sha256_value
from my_data_hub.workloads.region_talk.migration import (
    import_raw_export,
    reconciliation_report_from_database,
    validate_export,
)
from my_data_hub.workloads.region_talk.ydb_export import LegacyYdbRecord, export_records

SOURCE_TABLE = "region_talk_state"
ACCOUNTING_COLUMNS = (
    "expected_row_count",
    "raw_count",
    "dispositioned_count",
    "undispositioned_count",
    "quarantined_count",
    "raw_count_matches_manifest",
    "fully_accounted",
    "cutover_ready",
)


def _fixture_records() -> list[LegacyYdbRecord]:
    observed_at = datetime(2026, 8, 9, 12, 0, tzinfo=UTC)
    return [
        LegacyYdbRecord(
            source_table=SOURCE_TABLE,
            source_pk="candidate_memory_item:ci-001",
            source_updated_at=observed_at,
            payload={"url": "https://example.invalid/region-talk/1", "score": 0.91},
        ),
        LegacyYdbRecord(
            source_table=SOURCE_TABLE,
            source_pk="post_link_queue_item:ci-002",
            source_updated_at=observed_at,
            payload={"url": "https://example.invalid/region-talk/2"},
        ),
        LegacyYdbRecord(
            source_table=SOURCE_TABLE,
            source_pk="source_queue_item:ci-003",
            source_updated_at=observed_at,
            payload={"source": "ci-fixture"},
        ),
    ]


def _fetch_batch_accounting(cursor: Any, export_batch_id: UUID) -> dict[str, Any]:
    cursor.execute(
        "SELECT " + ", ".join(ACCOUNTING_COLUMNS) + " "
        "FROM migration.batch_accounting WHERE export_batch_id = %s",
        (export_batch_id,),
    )
    row = cursor.fetchone()
    if row is None:
        raise RuntimeError(f"missing batch accounting for {export_batch_id}")
    return {key: value for key, value in zip(ACCOUNTING_COLUMNS, row, strict=True)}


def _set_disposition(
    cursor: Any,
    *,
    export_batch_id: UUID,
    source_pk: str,
    disposition: str,
    reason_code: str,
) -> None:
    cursor.execute(
        """
        INSERT INTO migration.row_disposition (
            raw_record_id, mapping_version, disposition, target_refs,
            reason_code, reason_detail
        )
        SELECT raw_record_id, 'ci-fixture-v1', %s, '[]'::jsonb, %s,
               'PostgreSQL integration fixture'
        FROM migration.raw_record
        WHERE export_batch_id = %s AND source_table = %s AND source_pk = %s
        ON CONFLICT (raw_record_id) DO UPDATE SET
            mapping_version = EXCLUDED.mapping_version,
            disposition = EXCLUDED.disposition,
            target_refs = EXCLUDED.target_refs,
            reason_code = EXCLUDED.reason_code,
            reason_detail = EXCLUDED.reason_detail
        """,
        (disposition, reason_code, export_batch_id, SOURCE_TABLE, source_pk),
    )
    if cursor.rowcount != 1:
        raise RuntimeError(f"fixture raw record was not found: {source_pk}")


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [_json_safe(item) for item in value]
    if isinstance(value, (UUID, datetime)):
        return str(value)
    return value


def main() -> int:
    database_url = os.environ.get("MY_DATA_HUB_DATABASE_URL", "").strip()
    if not database_url:
        raise SystemExit("MY_DATA_HUB_DATABASE_URL is required")

    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - integration-only dependency
        raise SystemExit("psycopg is required") from exc

    export_batch_id = uuid4()
    with TemporaryDirectory(prefix="my-data-hub-region-talk-ci-") as temp_directory:
        bundle = export_records(
            _fixture_records(),
            output_root=Path(temp_directory),
            database="/ci/my-data-hub/region-talk",
            table=SOURCE_TABLE,
            scope="region-talk-ci-integration",
            source_revision="ci-fixture-source-v1",
            source_code_revision="ci-fixture-code-v1",
            export_batch_id=export_batch_id,
            metadata={"purpose": "postgres-integration-gate"},
        )
        validated = validate_export(bundle.manifest_path)
        first_inserted = import_raw_export(database_url, validated)
        replay_inserted = import_raw_export(database_url, validated)

    if first_inserted != 3 or replay_inserted != 0:
        raise RuntimeError(
            "landing replay contract failed: "
            f"first_inserted={first_inserted}, replay_inserted={replay_inserted}"
        )

    with psycopg.connect(database_url) as connection:
        with connection.cursor() as cursor:
            _set_disposition(
                cursor,
                export_batch_id=export_batch_id,
                source_pk="candidate_memory_item:ci-001",
                disposition="normalized",
                reason_code="ci_normalized",
            )
            _set_disposition(
                cursor,
                export_batch_id=export_batch_id,
                source_pk="post_link_queue_item:ci-002",
                disposition="retained_raw",
                reason_code="ci_retained_raw",
            )
            _set_disposition(
                cursor,
                export_batch_id=export_batch_id,
                source_pk="source_queue_item:ci-003",
                disposition="quarantined",
                reason_code="ci_intentional_quarantine",
            )
            blocked_accounting = _fetch_batch_accounting(cursor, export_batch_id)
        connection.commit()

    if not blocked_accounting["fully_accounted"]:
        raise RuntimeError(f"quarantined fixture must remain fully accounted: {blocked_accounting}")
    if blocked_accounting["cutover_ready"] or blocked_accounting["quarantined_count"] != 1:
        raise RuntimeError(f"quarantine did not block cutover: {blocked_accounting}")

    blocked_report = reconciliation_report_from_database(database_url, export_batch_id)
    if blocked_report["passed"]:
        raise RuntimeError("reconciliation report hid an unresolved quarantine")
    blocked_reasons = {
        reason
        for finding in blocked_report["blocking_findings"]
        for reason in finding["reasons"]
    }
    if blocked_reasons != {"quarantined_rows"}:
        raise RuntimeError(f"unexpected quarantine findings: {blocked_report['blocking_findings']}")

    with psycopg.connect(database_url) as connection:
        with connection.cursor() as cursor:
            _set_disposition(
                cursor,
                export_batch_id=export_batch_id,
                source_pk="source_queue_item:ci-003",
                disposition="intentionally_excluded",
                reason_code="ci_owner_accepted_exclusion",
            )
            cursor.execute(
                "UPDATE migration.export_batch SET status = 'reconciled' "
                "WHERE export_batch_id = %s",
                (export_batch_id,),
            )
            final_accounting = _fetch_batch_accounting(cursor, export_batch_id)
        connection.commit()

    final_report = reconciliation_report_from_database(database_url, export_batch_id)
    if not final_report["passed"] or final_report["blocking_findings"]:
        raise RuntimeError(f"resolved migration report did not pass: {final_report}")
    if not final_accounting["cutover_ready"] or final_accounting["quarantined_count"] != 0:
        raise RuntimeError(f"resolved batch is not cutover-ready: {final_accounting}")
    if not all(row["cutover_ready"] for row in final_report["accounting"]):
        raise RuntimeError("row-kind accounting is not uniformly cutover-ready")

    evidence = {
        "ok": True,
        "schema_version": "region-talk-migration-integration-evidence.v1",
        "export_batch_id": str(export_batch_id),
        "bundle": {
            "row_count": validated.row_count,
            "row_kind_counts": validated.row_kind_counts,
            "logical_sha256": validated.logical_sha256,
        },
        "landing": {
            "first_inserted": first_inserted,
            "replay_inserted": replay_inserted,
        },
        "blocked_state": {
            "accounting": blocked_accounting,
            "report_sha256": sha256_value(blocked_report),
            "blocking_findings": blocked_report["blocking_findings"],
        },
        "resolved_state": {
            "accounting": final_accounting,
            "report_sha256": sha256_value(final_report),
            "report": final_report,
        },
    }
    print(json.dumps(_json_safe(evidence), ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
