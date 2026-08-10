from __future__ import annotations

import argparse
import json
import os
from dataclasses import asdict
from pathlib import Path
from typing import Any

from my_data_hub.config import ConfigurationError, Settings
from my_data_hub.db.health import verify_database
from my_data_hub.db.migrations import applied_migrations, discover_migrations, migrate
from my_data_hub.orchestrator.backlog import load_region_talk_backlog
from my_data_hub.orchestrator.loop import run_loop
from my_data_hub.orchestrator.models import RegionTalkBacklog
from my_data_hub.orchestrator.policy import plan_region_talk
from my_data_hub.orchestrator.registry import load_pipeline_definition
from my_data_hub.orchestrator.repository import register_pipeline
from my_data_hub.workloads.region_talk.migration import import_raw_export, validate_export
from my_data_hub.workloads.region_talk.ydb_export import export_ydb_table

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
MIGRATION_DIRECTORY = REPOSITORY_ROOT / "sql" / "migrations"
REGION_TALK_PIPELINE = REPOSITORY_ROOT / "config" / "pipelines" / "region-talk.v1.json"


def _print(value: Any) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True, default=str))


def _database_settings() -> Settings:
    return Settings.from_env(require_database=True)


def command_api_serve(_args: argparse.Namespace) -> int:
    from my_data_hub.api.server import serve

    serve()
    return 0


def command_db_migrate(_args: argparse.Namespace) -> int:
    settings = _database_settings()
    database_url = os.getenv("MY_DATA_HUB_MIGRATOR_DATABASE_URL", "").strip() or settings.database_url
    executed = migrate(database_url, MIGRATION_DIRECTORY)
    definition = load_pipeline_definition(REGION_TALK_PIPELINE)
    registration = register_pipeline(
        database_url, definition, REGION_TALK_PIPELINE
    )
    _print(
        {
            "applied": [item.filename for item in executed],
            "region_talk_pipeline_id": str(registration.pipeline_id),
            "pipeline_status": registration.status,
        }
    )
    return 0


def command_db_status(_args: argparse.Namespace) -> int:
    settings = _database_settings()
    repository = discover_migrations(MIGRATION_DIRECTORY)
    applied = applied_migrations(settings.database_url)
    _print(
        {
            "repository": [item.filename for item in repository],
            "applied": [
                {"version": version, "filename": value[0], "sha256": value[1]}
                for version, value in sorted(applied.items())
            ],
        }
    )
    return 0


def command_db_verify(_args: argparse.Namespace) -> int:
    settings = _database_settings()
    health = verify_database(settings.application_database_url or settings.database_url)
    _print(asdict(health))
    return 0 if health.ok else 2


def command_orchestrator_register(_args: argparse.Namespace) -> int:
    settings = _database_settings()
    definition = load_pipeline_definition(REGION_TALK_PIPELINE)
    registration = register_pipeline(
        settings.database_url, definition, REGION_TALK_PIPELINE
    )
    _print(
        {
            "pipeline_id": str(registration.pipeline_id),
            "workload": definition.workload,
            "status": registration.status,
        }
    )
    return 0


def command_orchestrator_plan(args: argparse.Namespace) -> int:
    if args.backlog_json:
        backlog = RegionTalkBacklog(
            **json.loads(Path(args.backlog_json).read_text(encoding="utf-8"))
        )
    else:
        backlog = load_region_talk_backlog(_database_settings().database_url)
    actions = plan_region_talk(backlog, max_actions=args.max_actions)
    _print({"backlog": asdict(backlog), "actions": [asdict(item) for item in actions]})
    return 0


def command_orchestrator_loop(args: argparse.Namespace) -> int:
    settings = _database_settings()
    if settings.environment in {"prod", "production"} and not settings.orchestrator_database_url:
        raise ConfigurationError(
            "production orchestrator requires MY_DATA_HUB_ORCHESTRATOR_DATABASE_URL"
        )
    run_loop(
        settings.orchestrator_database_url or settings.database_url,
        settings.instance_id,
        interval_seconds=args.interval_seconds or settings.orchestrator_interval_seconds,
        scheduler_enabled=settings.scheduler_enabled,
        max_actions=args.max_actions,
    )
    return 0


def _required_value(
    argument: str | None, environment_name: str, *, option_name: str
) -> str:
    value = (argument or os.getenv(environment_name, "")).strip()
    if not value:
        raise SystemExit(f"provide --{option_name} or {environment_name}")
    return value


def command_region_talk_export(args: argparse.Namespace) -> int:
    bundle = export_ydb_table(
        endpoint=_required_value(args.endpoint, "YDB_ENDPOINT", option_name="endpoint"),
        database=_required_value(args.database, "YDB_DATABASE", option_name="database"),
        table=_required_value(
            args.table,
            "MY_DATA_HUB_REGION_TALK_YDB_TABLE",
            option_name="table",
        ),
        output_root=Path(args.output_root),
        page_size=args.page_size,
        scope=args.scope,
        source_revision=args.source_revision,
        source_code_revision=args.source_code_revision,
        connect_timeout_seconds=args.connect_timeout_seconds,
    )
    _print(
        {
            "status": "exported_and_validated",
            "export_batch_id": str(bundle.export_batch_id),
            "directory": str(bundle.directory),
            "manifest": str(bundle.manifest_path),
            "row_count": bundle.row_count,
            "row_kind_counts": bundle.row_kind_counts,
            "unknown_row_kinds": bundle.unknown_row_kinds,
            "logical_sha256": bundle.logical_sha256,
        }
    )
    return 0


def command_region_talk_validate(args: argparse.Namespace) -> int:
    validated = validate_export(Path(args.manifest))
    _print(
        {
            "export_batch_id": str(validated.manifest.export_batch_id),
            "row_count": validated.row_count,
            "row_kind_counts": validated.row_kind_counts,
            "logical_sha256": validated.logical_sha256,
            "files": [str(path) for path in validated.files],
        }
    )
    return 0


def command_region_talk_import(args: argparse.Namespace) -> int:
    validated = validate_export(Path(args.manifest))
    if not args.apply:
        _print(
            {
                "status": "validated_dry_run",
                "export_batch_id": str(validated.manifest.export_batch_id),
                "row_count": validated.row_count,
                "hint": "repeat with --apply after reviewing the manifest and backup state",
            }
        )
        return 0
    inserted = import_raw_export(_database_settings().database_url, validated)
    _print(
        {
            "status": "raw_landed",
            "export_batch_id": str(validated.manifest.export_batch_id),
            "inserted": inserted,
            "accounted": validated.row_count,
        }
    )
    return 0


def command_mcp_serve(args: argparse.Namespace) -> int:
    from my_data_hub.mcp.server import serve

    serve(transport=args.transport)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="my-data-hub")
    sub = parser.add_subparsers(dest="group", required=True)

    api = sub.add_parser("api")
    api_sub = api.add_subparsers(dest="api_command", required=True)
    api_sub.add_parser("serve").set_defaults(handler=command_api_serve)

    db = sub.add_parser("db")
    db_sub = db.add_subparsers(dest="db_command", required=True)
    db_sub.add_parser("migrate").set_defaults(handler=command_db_migrate)
    db_sub.add_parser("status").set_defaults(handler=command_db_status)
    db_sub.add_parser("verify").set_defaults(handler=command_db_verify)

    orchestrator = sub.add_parser("orchestrator")
    orchestrator_sub = orchestrator.add_subparsers(
        dest="orchestrator_command", required=True
    )
    orchestrator_sub.add_parser("register-pipeline").set_defaults(
        handler=command_orchestrator_register
    )
    plan = orchestrator_sub.add_parser("plan")
    plan.add_argument("--backlog-json")
    plan.add_argument("--max-actions", type=int, default=8)
    plan.set_defaults(handler=command_orchestrator_plan)
    loop = orchestrator_sub.add_parser("run-loop")
    loop.add_argument("--interval-seconds", type=int)
    loop.add_argument("--max-actions", type=int, default=8)
    loop.set_defaults(handler=command_orchestrator_loop)

    region_talk = sub.add_parser("region-talk")
    region_sub = region_talk.add_subparsers(dest="region_command", required=True)
    exporter = region_sub.add_parser("export-ydb")
    exporter.add_argument("--endpoint")
    exporter.add_argument("--database")
    exporter.add_argument("--table")
    exporter.add_argument("--output-root", required=True)
    exporter.add_argument("--scope", default="region-talk")
    exporter.add_argument("--page-size", type=int, default=1000)
    exporter.add_argument("--connect-timeout-seconds", type=int, default=20)
    exporter.add_argument("--source-revision")
    exporter.add_argument("--source-code-revision")
    exporter.set_defaults(handler=command_region_talk_export)
    validate = region_sub.add_parser("validate-ydb-export")
    validate.add_argument("--manifest", required=True)
    validate.set_defaults(handler=command_region_talk_validate)
    importer = region_sub.add_parser("import-ydb-export")
    importer.add_argument("--manifest", required=True)
    importer.add_argument("--apply", action="store_true")
    importer.set_defaults(handler=command_region_talk_import)

    mcp = sub.add_parser("mcp")
    mcp_sub = mcp.add_subparsers(dest="mcp_command", required=True)
    serve = mcp_sub.add_parser("serve")
    serve.add_argument("--transport", choices=["stdio", "streamable-http"], default="stdio")
    serve.set_defaults(handler=command_mcp_serve)
    return parser


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    raise SystemExit(args.handler(args))
