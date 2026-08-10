from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from my_data_hub.cli import _required_value, build_parser
from scripts import validate_repository

ROOT = Path(__file__).resolve().parents[1]


def test_repository_secret_scan_ignores_local_virtual_environment(
    tmp_path: Path, monkeypatch  # type: ignore[no-untyped-def]
) -> None:
    certificate = tmp_path / ".venv/lib/python3.12/site-packages/certifi/cacert.pem"
    certificate.parent.mkdir(parents=True)
    certificate.write_text("public CA fixture", encoding="utf-8")
    monkeypatch.setattr(validate_repository, "ROOT", tmp_path)
    report = validate_repository.Report()

    validate_repository.validate_secret_hygiene(report)

    assert report.errors == []


def test_cli_exposes_real_ydb_export_and_safe_dry_run() -> None:
    parser = build_parser()
    export = parser.parse_args(
        [
            "region-talk",
            "export-ydb",
            "--output-root",
            "/tmp/export",
            "--endpoint",
            "grpc://example",
            "--database",
            "/db",
            "--table",
            "state",
        ]
    )
    assert export.page_size == 1000
    importer = parser.parse_args(
        ["region-talk", "import-ydb-export", "--manifest", "/tmp/manifest.json"]
    )
    assert importer.apply is False


def test_notebook_generator_is_deterministic() -> None:
    result = subprocess.run(
        [sys.executable, "scripts/create_notebooks.py", "--check"],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_no_duplicate_notebook_directories_in_generator() -> None:
    source = (ROOT / "scripts/create_notebooks.py").read_text(encoding="utf-8")
    assert source.count('"60-region-talk-source-profile"') == 1
    assert source.count('"70-region-talk-writer"') == 1


def test_pipeline_and_generated_notebook_contracts_match() -> None:
    pipeline = json.loads((ROOT / "config/pipelines/region-talk.v1.json").read_text())
    contracts = {item["key"]: item["contract"] for item in pipeline["stages"]}
    for directory in (ROOT / "notebooks").iterdir():
        metadata_path = directory / "kernel-metadata.example.json"
        if not metadata_path.is_file():
            continue
        metadata = json.loads(metadata_path.read_text())
        for stage, contract in metadata["my_data_hub"]["contracts"].items():
            if stage in {"platform_smoke", "migration_reconciliation"}:
                continue
            assert contracts[stage] == contract


def test_final_ydb_table_environment_name_is_canonical(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.delenv("YDB_REGION_TALK_TABLE", raising=False)
    monkeypatch.setenv("MY_DATA_HUB_REGION_TALK_YDB_TABLE", "state")
    assert (
        _required_value(
            None,
            "MY_DATA_HUB_REGION_TALK_YDB_TABLE",
            option_name="table",
        )
        == "state"
    )


def test_postgresql_18_integration_profile_is_tmpfs_only() -> None:
    compose = (ROOT / "compose.yaml").read_text(encoding="utf-8")
    ci = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    image = "pgvector/pgvector:0.8.6-pg18-bookworm"
    assert image in compose
    assert image in ci
    assert "disposable-integration-test-only" in compose
    assert "/var/lib/postgresql:size=1g" in compose
    assert "postgres-data:" not in compose


def test_pipeline_registration_refresh_preserves_operational_status(
    monkeypatch,  # type: ignore[no-untyped-def]
) -> None:
    """Re-running migrations must not reset an operator-controlled pipeline status."""
    import sys
    from types import SimpleNamespace
    from uuid import UUID

    from my_data_hub.orchestrator.registry import load_pipeline_definition
    from my_data_hub.orchestrator.repository import register_pipeline

    executed: list[tuple[str, object]] = []
    committed = False

    class Cursor:
        def __enter__(self):  # type: ignore[no-untyped-def]
            return self

        def __exit__(self, *_args):  # type: ignore[no-untyped-def]
            return False

        def execute(self, sql: str, parameters: object = None) -> None:
            executed.append((sql, parameters))

        def fetchone(self) -> tuple[UUID, str]:
            return (UUID("11111111-1111-4111-8111-111111111111"), "active")

    class Connection:
        def __enter__(self):  # type: ignore[no-untyped-def]
            return self

        def __exit__(self, *_args):  # type: ignore[no-untyped-def]
            return False

        def cursor(self) -> Cursor:
            return Cursor()

        def commit(self) -> None:
            nonlocal committed
            committed = True

    monkeypatch.setitem(
        sys.modules,
        "psycopg",
        SimpleNamespace(connect=lambda _database_url: Connection()),
    )
    pipeline_path = ROOT / "config/pipelines/region-talk.v1.json"
    registration = register_pipeline(
        "postgresql://fixture",
        load_pipeline_definition(pipeline_path),
        pipeline_path,
    )

    assert registration.status == "active"
    assert registration.pipeline_id == UUID("11111111-1111-4111-8111-111111111111")
    assert committed is True
    registration_sql = next(
        sql for sql, _parameters in executed if "INSERT INTO orchestration.pipeline" in sql
    )
    assert "status = EXCLUDED.status" not in registration_sql
    assert "RETURNING pipeline_id, status" in registration_sql
