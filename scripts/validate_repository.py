#!/usr/bin/env python3
"""Fail-closed structural validation for the my-data-hub bootstrap repository."""

from __future__ import annotations

import ast
import json
import re
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import nbformat
import yaml
from jsonschema import Draft202012Validator, FormatChecker

ROOT = Path(__file__).resolve().parents[1]
SCHEMA_DIR = ROOT / "schemas"


@dataclass(slots=True)
class Report:
    checks: int = 0
    errors: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    def check(self, condition: bool, message: str) -> None:
        self.checks += 1
        if not condition:
            self.errors.append(message)

    def fail(self, message: str) -> None:
        self.checks += 1
        self.errors.append(message)


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def validate_json_and_schemas(report: Report) -> None:
    schemas: dict[str, dict[str, Any]] = {}
    for path in sorted(SCHEMA_DIR.glob("*.json")):
        try:
            raw = load_json(path)
            Draft202012Validator.check_schema(raw)
            schemas[path.name] = raw
            report.checks += 1
        except Exception as exc:
            report.fail(f"invalid JSON Schema {path.relative_to(ROOT)}: {exc}")

    mappings = {
        "semantic-command.v1.example.json": "semantic-command.v1.schema.json",
        "notebook-result.v1.example.json": "notebook-result.v1.schema.json",
        "changeset.v1.example.json": "changeset.v1.schema.json",
        "notebook-input-manifest.v1.example.json": "notebook-input-manifest.v1.schema.json",
        "migration-reconciliation-report.v1.example.json": (
            "migration-reconciliation-report.v1.schema.json"
        ),
    }
    checker = FormatChecker()
    for example_name, schema_name in mappings.items():
        path = ROOT / "examples" / "contracts" / example_name
        report.check(path.is_file(), f"missing contract example: {path.relative_to(ROOT)}")
        if not path.is_file() or schema_name not in schemas:
            continue
        raw = load_json(path)
        errors = sorted(
            Draft202012Validator(schemas[schema_name], format_checker=checker).iter_errors(raw),
            key=lambda item: list(item.path),
        )
        report.check(
            not errors,
            f"{path.relative_to(ROOT)} violates {schema_name}: "
            + "; ".join(error.message for error in errors[:5]),
        )

    # These schemas are generated from runtime models. Drift is a correctness error.
    sys.path.insert(0, str(ROOT / "src"))
    from my_data_hub.domain.commands import Changeset, SemanticCommand
    from my_data_hub.notebooks.contracts import NotebookInputManifest, NotebookResult
    from my_data_hub.workloads.region_talk.contracts import (
        MigrationReconciliationReport,
        YdbExportManifest,
        YdbExportRow,
    )

    generated = {
        "changeset.v1.schema.json": Changeset,
        "semantic-command.v1.schema.json": SemanticCommand,
        "notebook-input-manifest.v1.schema.json": NotebookInputManifest,
        "notebook-result.v1.schema.json": NotebookResult,
        "region-talk-ydb-export-manifest.v1.schema.json": YdbExportManifest,
        "region-talk-ydb-export-row.v1.schema.json": YdbExportRow,
        "migration-reconciliation-report.v1.schema.json": (
            MigrationReconciliationReport
        ),
    }

    def normalized(schema: dict[str, Any]) -> dict[str, Any]:
        value = json.loads(json.dumps(schema))
        for key in ("$schema", "$id", "title"):
            value.pop(key, None)
        return value

    for filename, model in generated.items():
        report.check(
            normalized(schemas[filename]) == normalized(model.model_json_schema(mode="validation")),
            f"runtime model / JSON Schema drift: {filename}",
        )


def validate_python(report: Report) -> None:
    for path in sorted((ROOT / "src").rglob("*.py")) + sorted((ROOT / "scripts").rglob("*.py")):
        if "__pycache__" in path.parts:
            continue
        try:
            ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            report.checks += 1
        except SyntaxError as exc:
            report.fail(f"Python syntax error in {path.relative_to(ROOT)}: {exc}")

    stale_tokens = {
        "my_data_hub.workers": "removed notebook package",
        "orchestration.worker_result_bundle": "removed result table",
        "orchestration.worker_result_acceptance": "removed acceptance table",
        "migration.region_talk_ydb_raw": "removed migration table",
    }
    source_text = "\n".join(
        path.read_text(encoding="utf-8")
        for path in sorted((ROOT / "src").rglob("*.py"))
        if "__pycache__" not in path.parts
    )
    for token, reason in stale_tokens.items():
        report.check(token not in source_text, f"stale reference {token!r}: {reason}")


def validate_sql(report: Report) -> None:
    migrations = sorted((ROOT / "sql" / "migrations").glob("*.sql"))
    versions: list[int] = []
    for path in migrations:
        match = re.fullmatch(r"(\d{4})_[a-z0-9_]+\.sql", path.name)
        report.check(match is not None, f"invalid migration filename: {path.name}")
        if match:
            versions.append(int(match.group(1)))
    report.check(versions == list(range(1, len(versions) + 1)), "SQL migration versions are not contiguous")

    try:
        from pglast import parse_sql
    except ImportError:
        parse_sql = None
        report.notes.append(
            "pglast is not installed; PostgreSQL AST parsing was skipped. "
            "CI/dev deployment must install .[dev] and run the same validator."
        )
    if parse_sql:
        for path in migrations:
            try:
                parse_sql(path.read_text(encoding="utf-8"))
                report.checks += 1
            except Exception as exc:
                report.fail(f"PostgreSQL parse error in {path.relative_to(ROOT)}: {exc}")

    sql = "\n".join(path.read_text(encoding="utf-8") for path in migrations)
    tables = set(re.findall(r"CREATE TABLE\s+([a-z_]+\.[a-z_]+)", sql, flags=re.I))
    views = set(re.findall(r"CREATE(?: OR REPLACE)? VIEW\s+([a-z_]+\.[a-z_]+)", sql, flags=re.I))
    report.check("migration.raw_record" in tables, "lossless migration.raw_record landing is missing")
    report.check("orchestration.worker_result_inbox" in tables, "worker result inbox is missing")
    report.check("sync.external_outbox" in tables, "transactional external outbox is missing")
    report.check("joplin.note_link" in tables, "Joplin bridge projection is missing")
    report.check("migration.region_talk_accounting" in views, "migration accounting view is missing")
    report.check(
        "FOREIGN KEY (export_batch_id, row_kind)" in sql
        and "REFERENCES migration.export_batch_kind(export_batch_id, row_kind)" in sql,
        "raw migration rows are not constrained to manifest-declared row kinds",
    )
    report.check(
        "UNIQUE (stage_run_id, input_manifest_sha256)" in sql,
        "worker-result stage-run uniqueness invariant is missing",
    )
    report.check(
        re.search(r"schema_revision\s*=\s*%d\b" % len(migrations), migrations[-1].read_text()),
        "hub.canonical_state.schema_revision does not match latest migration",
    )

    sys.path.insert(0, str(ROOT / "src"))
    from my_data_hub.workloads.region_talk.constants import MAPPING_TARGETS

    missing_targets = sorted(
        {target for targets in MAPPING_TARGETS.values() for target in targets if target not in tables}
    )
    report.check(not missing_targets, f"Region Talk mapping targets absent from SQL: {missing_targets}")


def validate_pipeline(report: Report) -> None:
    path = ROOT / "config" / "pipelines" / "region-talk.v1.json"
    raw = load_json(path)
    report.check(raw.get("schema_version") == "my-data-hub-pipeline.v1", "bad pipeline schema version")
    stages = raw.get("stages", [])
    keys = [item.get("key") for item in stages]
    report.check(len(keys) == len(set(keys)), "pipeline stage keys are not unique")
    for index, stage in enumerate(stages):
        prefix = f"pipeline stage #{index}"
        report.check(bool(stage.get("key")), f"{prefix} has no key")
        report.check(int(stage.get("max_attempts", 0)) >= 1, f"{prefix} has invalid max_attempts")
        report.check(int(stage.get("timeout_seconds", 0)) >= 1, f"{prefix} has invalid timeout")
        report.check(bool(stage.get("contract")), f"{prefix} has no result contract")
    publication = next((item for item in stages if item.get("key") == "publication_dispatch"), None)
    report.check(publication is not None, "publication_dispatch stage is missing")
    report.check(
        publication is not None and publication.get("enabled_by_default") is False,
        "production publication must be disabled by default",
    )
    report.check(raw.get("status") == "paused", "Region Talk pipeline must bootstrap paused")
    repository_source = (ROOT / "src/my_data_hub/orchestrator/repository.py").read_text(encoding="utf-8")
    report.check(
        "status = EXCLUDED.status" not in repository_source,
        "pipeline definition refresh can reset the operator-controlled runtime status",
    )
    report.check(
        "RETURNING pipeline_id, status" in repository_source,
        "pipeline registration does not report the actual persisted status",
    )


def validate_notebooks(report: Report) -> None:
    process = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "create_notebooks.py"), "--check"],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    report.check(process.returncode == 0, f"generated notebook drift: {process.stdout}{process.stderr}")

    forbidden_imports = ("import psycopg", "import sqlite3", "import ydb", "from ydb")
    forbidden_mutations = ("INSERT INTO", "UPDATE ", "DELETE FROM", "CREATE TABLE", "DROP TABLE")
    for path in sorted((ROOT / "notebooks").glob("*/worker.ipynb")):
        try:
            nb = nbformat.read(path, as_version=4)
            nbformat.validate(nb)
            report.checks += 1
        except Exception as exc:
            report.fail(f"invalid notebook {path.relative_to(ROOT)}: {exc}")
            continue
        metadata = nb.metadata.get("my_data_hub", {})
        report.check(metadata.get("canonical_write_allowed") is False, f"{path} allows canonical writes")
        report.check(
            metadata.get("external_side_effects_allowed") is False,
            f"{path} allows external side effects",
        )
        code = "\n".join(cell.source for cell in nb.cells if cell.cell_type == "code")
        for token in forbidden_imports + forbidden_mutations:
            report.check(token not in code, f"forbidden notebook token {token!r} in {path.relative_to(ROOT)}")


def validate_docs_and_layout(report: Report) -> None:
    required = [
        "README.md",
        "PROJECT_STATUS.md",
        "docs/00-source-of-truth.md",
        "docs/02-target-architecture.md",
        "docs/05-mcp.md",
        "docs/migrations/region-talk/README.md",
        "docs/migrations/region-talk/cutover.md",
        "docs/migrations/region-talk/rollback.md",
        "docs/12-code-agent-handoff.md",
        "docs/source-material/source-manifest.yaml",
        "docs/source-material/idea-hub/README.md",
        "docs/source-material/region-talk/README.md",
        "scripts/import_source_material.py",
        "scripts/verify_postgres_bootstrap.py",
        "scripts/verify_region_talk_migration_flow.py",
        "BOOTSTRAP_VALIDATION.md",
        "docs/13-external-references.md",
        "docs/migrations/region-talk/adaptation-manifest.json",
        "schemas/adaptation-manifest.v1.schema.json",
        "schemas/migration-reconciliation-report.v1.schema.json",
        "examples/contracts/migration-reconciliation-report.v1.example.json",
        "tests/test_mcp_sdk_v2_contract.py",
    ]
    for relative in required:
        report.check((ROOT / relative).is_file(), f"missing required document: {relative}")

    manifest = yaml.safe_load((ROOT / "docs/source-material/source-manifest.yaml").read_text())
    sources = manifest.get("sources", []) if isinstance(manifest, dict) else []
    idea_source = next(
        (item for item in sources if item.get("source_repository") == "onedayonemasterpiece/idea-hub"),
        None,
    )
    report.check(idea_source is not None, "canonical idea-hub source is absent from provenance manifest")
    if idea_source:
        report.check(idea_source.get("source_commit") == "0c3fcf7", "wrong target-vision source commit")
        report.check(
            idea_source.get("status") in {"pending_authenticated_import", "verified_import"},
            "target-vision source status is ambiguous",
        )

    adaptation_path = ROOT / "docs/migrations/region-talk/adaptation-manifest.json"
    adaptation_schema = SCHEMA_DIR / "adaptation-manifest.v1.schema.json"
    if adaptation_path.is_file() and adaptation_schema.is_file():
        adaptation = load_json(adaptation_path)
        schema = load_json(adaptation_schema)
        errors = sorted(
            Draft202012Validator(schema, format_checker=FormatChecker()).iter_errors(adaptation),
            key=lambda item: list(item.path),
        )
        report.check(
            not errors,
            "Region Talk adaptation manifest violates its schema: "
            + "; ".join(error.message for error in errors[:5]),
        )

    region_talk_source = next(
        (item for item in sources if item.get("source_repository") == "onedayonemasterpiece/region-talk"),
        None,
    )
    report.check(
        region_talk_source is not None,
        "dedicated Region Talk donor is absent from provenance manifest",
    )
    if region_talk_source:
        report.check(
            region_talk_source.get("status") in {"pending_curated_import", "verified_import"},
            "Region Talk donor source status is ambiguous",
        )

    link_pattern = re.compile(r"\[[^\]]*\]\((?!https?://|mailto:|#)([^)]+)\)")
    stale_schema_tokens = {
        "hub.object",
        "hub.object_revision",
        "hub.external_identity",
        "hub.project_membership",
        "migration.legacy_identity_alias",
        "pipeline_definition",
        "task_attempt",
        "run_event",
    }
    for path in sorted(ROOT.glob("*.md")) + sorted((ROOT / "docs").rglob("*.md")):
        text = path.read_text(encoding="utf-8")
        for token in stale_schema_tokens:
            report.check(
                token not in text,
                f"stale schema token {token!r} in {path.relative_to(ROOT)}",
            )
        for raw in link_pattern.findall(text):
            target = raw.split("#", 1)[0].strip().replace("%20", " ")
            if not target:
                continue
            resolved = (path.parent / target).resolve()
            try:
                resolved.relative_to(ROOT)
            except ValueError:
                report.fail(f"Markdown link escapes repository: {path.relative_to(ROOT)} -> {raw}")
                continue
            report.check(resolved.exists(), f"broken Markdown link: {path.relative_to(ROOT)} -> {raw}")

    agents = (ROOT / "AGENTS.md").read_text(encoding="utf-8")
    report.check("docs/00-source-of-truth.md" in agents, "AGENTS authority order references wrong document")
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    env_example = (ROOT / ".env.example").read_text(encoding="utf-8")
    report.check("make seed" not in readme, "README references removed make seed target")
    report.check(
        "MY_DATA_HUB_REGION_TALK_YDB_TABLE" in readme
        and "MY_DATA_HUB_REGION_TALK_YDB_TABLE" in env_example,
        "Region Talk YDB table variable is not documented consistently",
    )
    report.check(
        "YDB_REGION_TALK_TABLE" not in readme,
        "README still uses the superseded YDB table variable",
    )
    report.check(
        "docs/migrations/region-talk/README.md" in readme,
        "README points to the wrong Region Talk migration path",
    )


def validate_deployment(report: Report) -> None:
    compose = (ROOT / "compose.yaml").read_text(encoding="utf-8")
    report.check(
        '127.0.0.1:${POSTGRES_PORT:-5432}:5432' in compose,
        "PostgreSQL must be exposed only on loopback for host-side operations",
    )
    postgres_image = "pgvector/pgvector:0.8.6-pg18-bookworm"
    report.check(postgres_image in compose, "PostgreSQL image is not pinned to the target")
    report.check(
        "postgres-data:/var/lib/postgresql" in compose
        and "postgres-data:/var/lib/postgresql/data" not in compose,
        "PostgreSQL 18 volume must mount /var/lib/postgresql, not the legacy /data path",
    )
    ci = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    report.check(postgres_image in ci, "CI PostgreSQL image differs from the deployment target")
    report.check(
        "python scripts/verify_postgres_bootstrap.py" in ci,
        "CI does not verify the live PostgreSQL bootstrap contract",
    )
    report.check(
        "python scripts/verify_region_talk_migration_flow.py" in ci,
        "CI does not exercise the live Region Talk migration flow",
    )
    report.check(
        ci.count("my-data-hub db migrate") >= 2,
        "CI does not prove migration and pipeline-registration idempotency",
    )
    report.check(
        "my-data-hub db verify" in ci,
        "CI does not run database health verification",
    )
    report.check(
        "pgvector/pgvector:0.8.6-pg16" not in ci,
        "stale PostgreSQL 16 integration service remains in CI",
    )
    api_unit = (ROOT / "deploy/systemd/my-data-hub-api.service").read_text()
    orch_unit = (ROOT / "deploy/systemd/my-data-hub-orchestrator.service").read_text()
    report.check("my-data-hub api serve" in api_unit, "systemd API command does not match CLI")
    report.check(
        "my-data-hub orchestrator run-loop" in orch_unit,
        "systemd orchestrator command does not match CLI",
    )
    for script in sorted((ROOT / "scripts").glob("*.sh")):
        report.check(script.stat().st_mode & 0o111 != 0, f"script is not executable: {script.name}")


def validate_secret_hygiene(report: Report) -> None:
    forbidden_files = re.compile(r"(^|/)(\.env|.*\.pem|.*\.key|.*\.sqlite(?:3)?|.*\.db)$")
    for path in ROOT.rglob("*"):
        ignored_parts = {".git", "__pycache__", ".pytest_cache", ".ruff_cache", ".mypy_cache"}
        if not path.is_file() or ignored_parts.intersection(path.parts):
            continue
        relative = path.relative_to(ROOT).as_posix()
        if relative == ".env.example":
            continue
        report.check(not forbidden_files.search(relative), f"forbidden secret/data file: {relative}")


def main() -> int:
    report = Report()
    validate_json_and_schemas(report)
    validate_python(report)
    validate_sql(report)
    validate_pipeline(report)
    validate_notebooks(report)
    validate_docs_and_layout(report)
    validate_deployment(report)
    validate_secret_hygiene(report)
    payload = {
        "ok": not report.errors,
        "checks": report.checks,
        "errors": report.errors,
        "notes": report.notes,
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if not report.errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
