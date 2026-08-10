#!/usr/bin/env python3
"""Fail-closed structural validation for the my-data-hub bootstrap repository."""

from __future__ import annotations

import ast
import hashlib
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
        "data-connector-envelope.v1.example.json": "data-connector-envelope.v1.schema.json",
        "kaggle-exchange-manifest.v1.example.json": "kaggle-exchange-manifest.v1.schema.json",
        "workflow-receipt.v1.example.json": "workflow-receipt.v1.schema.json",
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
        re.search(rf"schema_revision\s*=\s*{len(migrations)}\b", migrations[-1].read_text()),
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
        "docs/15-infrastructure-first-plan.md",
        "docs/16-data-connectors.md",
        "docs/17-kaggle-control-plane.md",
        "docs/18-mcp-operator-and-database-access.md",
        "docs/19-test-first-rollout.md",
        "docs/20-remote-mcp-endpoint.md",
        "docs/21-infrastructure-addendum-delivery.md",
        "architecture/invariants.yaml",
        "docs/adr/0016-kaggle-postgresql-master-architecture-reset.md",
        "docs/incidents/2026-08-10-local-postgres-architecture-drift.md",
        "docs/architecture/work-preservation-map.md",
        "docs/roadmap-architecture-reset.md",
        "compose.control-plane.yaml",
        "deploy/control-plane/install.sh",
        "docs/operations/evidence/2026-08-10-pr-a-host.json",
        "docs/operations/first-deploy-template.md",
        "docs/adr/0009-canonical-postgres-availability.md",
        "docs/adr/0010-data-connector-ingress-contract.md",
        "docs/adr/0011-kaggle-resource-control-classes.md",
        "docs/adr/0012-mcp-database-operator-profiles.md",
        "docs/adr/0013-remote-mcp-endpoint.md",
        "docs/adr/0014-test-first-infrastructure-rollout.md",
        "schemas/data-connector-envelope.v1.schema.json",
        "schemas/kaggle-exchange-manifest.v1.schema.json",
        "examples/contracts/data-connector-envelope.v1.example.json",
        "examples/contracts/kaggle-exchange-manifest.v1.example.json",
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
        report.check(
            idea_source.get("source_commit")
            == "0c3fcf71b2ee8ba8afa49624bef4b779873802f7",
            "wrong target-vision source commit",
        )
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
            # Match a retired identifier, not a valid longer target name such as
            # ``hub.object_scope_relation`` or an MCP tool such as
            # ``hub.object.context.get``.
            stale_pattern = re.compile(
                rf"(?<![A-Za-z0-9_.]){re.escape(token)}(?![A-Za-z0-9_.])"
            )
            report.check(
                stale_pattern.search(text) is None,
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
    invariants_path = ROOT / "architecture/invariants.yaml"
    report.check(invariants_path.is_file(), "architecture invariants are missing")
    if not invariants_path.is_file():
        return
    invariants = yaml.safe_load(invariants_path.read_text(encoding="utf-8"))
    expected_authority = [
        "owner_decisions",
        "exact_imported_source_research",
        "corrective_adr",
        "machine_readable_invariants",
        "derived_docs_code_tests",
    ]
    expected_architecture = {
        "final_project_name": "my-data-hub",
        "legacy_alias": "content-platform",
        "canonical_database_engine": "postgresql",
        "canonical_database_runtime": "kaggle_notebook",
        "active_writable_primary_max": 1,
        "devstand_role": "lightweight_control_plane",
        "persistent_local_postgresql": "forbidden",
        "persistent_local_pgdata": "forbidden",
        "canonical_business_data_on_devstand": "forbidden",
        "checkpoint_store": "private_kaggle_datasets",
        "checkpoint_generations_minimum": 2,
        "direct_internal_data_plane": "required",
        "stable_external_mcp_on_devstand": "required",
    }
    expected_safety = {
        "region_talk_state": "paused",
        "production_publication": "disabled",
        "remote_mcp_writes": "disabled",
        "legacy_same_host_install": "forbidden",
        "dns_vpn_443_changes_in_pr_a": "forbidden",
    }
    report.check(invariants.get("authority_order") == expected_authority, "owner-approved authority order drifted")
    report.check(invariants.get("architecture") == expected_architecture, "owner-approved architecture invariants drifted")
    report.check(invariants.get("safety") == expected_safety, "owner-approved safety invariants drifted")

    source_relative = "docs/source-material/idea-hub/idea-20260809-content-platform-current-design.md"
    source_path = ROOT / source_relative
    source_sha = "c7efb28231223caa6fd02fcc001a38e0f16bcc3fa4c4cd53e744721b2eac0852"
    report.check(
        invariants.get("canonical_source") == {"path": source_relative, "sha256": source_sha},
        "canonical source identity drifted",
    )
    report.check(
        source_path.is_file() and hashlib.sha256(source_path.read_bytes()).hexdigest() == source_sha,
        "exact imported architecture source bytes drifted",
    )
    adr9 = (ROOT / "docs/adr/0009-canonical-postgres-availability.md").read_text(encoding="utf-8")
    report.check("SUPERSEDED_BY_ARCHITECTURE_RESET" in adr9, "ADR-0009 is not explicitly superseded")
    report.check(
        (ROOT / "docs/adr/0016-kaggle-postgresql-master-architecture-reset.md").is_file(),
        "corrective ADR-0016 is missing",
    )

    control_path = ROOT / "compose.control-plane.yaml"
    report.check(control_path.is_file(), "production control-plane Compose contract is missing")
    control = yaml.safe_load(control_path.read_text(encoding="utf-8")) if control_path.is_file() else {}
    report.check(control.get("x-my-data-hub-profile") == "production-lightweight-control-plane", "control profile marker drifted")
    report.check(set(control.get("services", {})) == {"control-plane"}, "production profile must contain only control-plane service")
    report.check(not control.get("volumes"), "production control plane must not declare volumes")
    control_serialized = json.dumps(control, sort_keys=True).lower()
    for token in ("postgres", "pgdata", "pg_dump", "database_url", "db migrate", "backup_postgres", "connector-committer"):
        report.check(token not in control_serialized, f"production control plane contains forbidden local-master token: {token}")
    environment = control.get("services", {}).get("control-plane", {}).get("environment", {})
    report.check(environment.get("MY_DATA_HUB_PRODUCTION_PUBLISH_ENABLED") == "false", "production publication gate is not false")
    report.check(environment.get("MY_DATA_HUB_MCP_WRITE_ENABLED") == "false", "remote MCP write gate is not false")

    legacy = (ROOT / "deploy/same-host/install.sh").read_text(encoding="utf-8")
    report.check("INSTALL_MY_DATA_HUB_SAME_HOST" in legacy and "exit 78" in legacy, "legacy same-host token is not hard-disabled")
    for token in ("docker compose", "db migrate", "systemctl", "postgres.env", "pg_dump"):
        report.check(token not in legacy, f"legacy installer remains executable beyond its guard: {token}")
    control_installer = (ROOT / "deploy/control-plane/install.sh").read_text(encoding="utf-8").lower()
    for token in ("database_url", "db migrate", "pg_dump", "backup-loop", "connector-committer"):
        report.check(token not in control_installer, f"control installer contains forbidden local-master operation: {token}")
    report.check(not (ROOT / "deploy/systemd").exists(), "DB-coupled legacy systemd deployment directory remains")
    report.check(not (ROOT / "compose.same-host.yaml").exists(), "legacy same-host production Compose remains")
    compose_files = {path.relative_to(ROOT).as_posix() for path in ROOT.glob("compose*.yaml")}
    report.check(
        compose_files == {"compose.yaml", "compose.control-plane.yaml"},
        f"unclassified Compose deployment profile exists: {sorted(compose_files)}",
    )
    deploy_files = {
        path.relative_to(ROOT).as_posix()
        for path in (ROOT / "deploy").rglob("*")
        if path.is_file()
    }
    expected_deploy_files = {
        "deploy/control-plane/Dockerfile",
        "deploy/control-plane/install.sh",
        "deploy/same-host/install.sh",
    }
    report.check(
        deploy_files == expected_deploy_files,
        f"unclassified production deployment file exists: {sorted(deploy_files - expected_deploy_files)}",
    )

    disposable = yaml.safe_load((ROOT / "compose.yaml").read_text(encoding="utf-8"))
    report.check(disposable.get("x-my-data-hub-profile") == "disposable-integration-test-only", "root Compose is not explicitly disposable")
    report.check(not disposable.get("volumes"), "disposable integration Compose must not declare named volumes")
    postgres = disposable.get("services", {}).get("postgres", {})
    report.check(postgres.get("restart") == "no", "disposable PostgreSQL restart policy must be disabled")
    report.check(postgres.get("tmpfs") == ["/var/lib/postgresql:size=1g,mode=0700"], "disposable PostgreSQL must use exact tmpfs PGDATA parent")
    postgres_image = "pgvector/pgvector:0.8.6-pg18-bookworm"
    report.check(postgres.get("image") == postgres_image, "disposable PostgreSQL image is not pinned")
    makefile = (ROOT / "Makefile").read_text(encoding="utf-8")
    report.check("docker compose down -v --remove-orphans" in makefile, "disposable integration cleanup does not remove volumes")

    workflow_directory = ROOT / ".github/workflows"
    workflows = {path.name for path in workflow_directory.glob("*.yml")}
    report.check(workflows == {"ci.yml"}, "deferred deployment/provider workflows remain enabled")
    ci_path = workflow_directory / "ci.yml"
    ci = ci_path.read_text(encoding="utf-8")
    ci_yaml = yaml.safe_load(ci)
    postgres_job = ci_yaml.get("jobs", {}).get("postgres-integration", {})
    postgres_service = postgres_job.get("services", {}).get("postgres", {})
    report.check(postgres_job.get("runs-on") == "ubuntu-latest", "PostgreSQL integration must remain GitHub-hosted disposable CI")
    report.check(postgres_service.get("image") == postgres_image, "CI PostgreSQL image differs from integration target")
    report.check("volumes" not in postgres_service and "docker volume create" not in ci, "CI PostgreSQL declares persistent volume state")
    for command in (
        "python scripts/verify_postgres_bootstrap.py",
        "python scripts/verify_region_talk_migration_flow.py",
        "python scripts/verify_postgres_upgrade.py",
        "python scripts/verify_postgres_roles.py",
        "python scripts/verify_db_operator.py",
        "python scripts/verify_connector_flow.py",
    ):
        report.check(command in ci, f"CI lost topology-neutral integration proof: {command}")

    # Scan every executable/deployment-shaped repository file, not merely the known
    # Compose document. Exact allowlist entries are topology-neutral tools or disposable
    # test paths and each addition requires an explicit architecture review here.
    executable_patterns = {
        "postgresql service supervision": re.compile(r"^.*postgresql\.service.*$", re.I | re.M),
        "PostgreSQL/PGDATA volume creation": re.compile(
            r"^.*(?:docker\s+volume\s+create[^\n]*(?:postgres|pgdata)|"
            r"docker\s+compose[^\n]*up[^\n]*(?:postgres|pgdata)).*$",
            re.I | re.M,
        ),
        "PostgreSQL process initialization": re.compile(r"^.*\b(?:initdb|pg_ctl)\b.*$", re.I | re.M),
        "local master dump": re.compile(r"^.*\bpg_dump\b.*$", re.I | re.M),
        "master migration from deployment": re.compile(r"^.*\bdb\s+migrate\b.*$", re.I | re.M),
        "legacy confirmation token": re.compile(r"^.*INSTALL_MY_DATA_HUB_SAME_HOST.*$", re.M),
    }
    allowed_occurrences = {
        ("PostgreSQL/PGDATA volume creation", "Makefile"): [
            "docker compose up -d postgres",
        ],
        ("master migration from deployment", "Makefile"): [
            "docker compose run --rm api db migrate",
            "docker compose run --rm api db migrate",
        ],
        ("master migration from deployment", ".github/workflows/ci.yml"): [
            "run: my-data-hub db migrate",
            "run: my-data-hub db migrate",
        ],
        ("local master dump", "scripts/backup_postgres.sh"): [
            'command -v pg_dump >/dev/null || { echo "pg_dump is required" >&2; exit 2; }',
            "# pg_dump streams directly into age. No plaintext dump is ever written to local storage.",
            'PGDATABASE="$DATABASE_URL" pg_dump --format=custom --compress=9 \\',
            'pg_dump_version="$(pg_dump --version)"',
        ],
        ("legacy confirmation token", "deploy/same-host/install.sh"): [
            'if [[ "${1:-}" == "INSTALL_MY_DATA_HUB_SAME_HOST" || "${1:-}" == "PREPARE" ]]; then',
        ],
        ("legacy confirmation token", "deploy/control-plane/install.sh"): [
            'if [[ "$action" == "INSTALL_MY_DATA_HUB_SAME_HOST" ]]; then',
        ],
    }
    executable_candidates: list[Path] = []
    for path in ROOT.rglob("*"):
        if not path.is_file() or any(part in {".git", ".venv", "__pycache__"} for part in path.parts):
            continue
        relative = path.relative_to(ROOT)
        if (
            path.suffix in {".sh", ".service", ".timer"}
            or path.name == "Makefile"
            or path.name.startswith("Dockerfile")
            or relative.parts[:2] == (".github", "workflows")
            or path.name.startswith("compose")
        ):
            executable_candidates.append(path)
    observed_occurrences: dict[tuple[str, str], list[str]] = {}
    for path in executable_candidates:
        relative = path.relative_to(ROOT).as_posix()
        text = path.read_text(encoding="utf-8")
        for label, pattern in executable_patterns.items():
            matches = [match.group(0).strip() for match in pattern.finditer(text)]
            if matches:
                observed_occurrences[(label, relative)] = matches
    all_occurrence_keys = set(observed_occurrences) | set(allowed_occurrences)
    for key in sorted(all_occurrence_keys):
        report.check(
            sorted(observed_occurrences.get(key, [])) == sorted(allowed_occurrences.get(key, [])),
            f"repository-wide forbidden execution occurrences drifted for {key}",
        )

    pipeline = load_json(ROOT / "config/pipelines/region-talk.v1.json")
    report.check(pipeline.get("status") == "paused", "Region Talk pipeline is not paused")
    publication = next((stage for stage in pipeline.get("stages", []) if stage.get("key") == "publication_dispatch"), {})
    report.check(publication.get("enabled_by_default") is False, "Region Talk publication is not disabled")

    reversal_patterns = (
        "kaggle is not master",
        "kaggle never becomes a writable",
        "kaggle is not the canonical database",
        "never hosts a writable master database",
        "canonical postgresql instance remains on the devstand",
        "normally-always-on canonical postgresql on devstand",
        "postgresql и orchestrator работают на одном initial devstand",
        "postgresql/internal services on the private devstand",
        "postgresql migration revision matches repository head",
        "latest local and off-host generation age",
        "local plus off-host backup is hash-verified",
        "devstand auto-start and health checks work",
    )
    reversal_allowlist = {
        "docs/adr/0009-canonical-postgres-availability.md": "superseded historical decision",
        "docs/incidents/2026-08-10-local-postgres-architecture-drift.md": "incident history",
    }
    for path in sorted((ROOT / "docs").rglob("*.md")):
        relative = path.relative_to(ROOT).as_posix()
        if relative.startswith("docs/source-material/") or relative in reversal_allowlist:
            continue
        text = path.read_text(encoding="utf-8").lower()
        normalized = re.sub(r"\s+", " ", text)
        for pattern in reversal_patterns:
            report.check(pattern not in normalized, f"architecture reversal phrase {pattern!r} in {relative}")
        for claim in re.finditer(
            r"(?:postgresql|postgres|pgdata|canonical database)[^.;,]{0,120}"
            r"(?:on|in|at) (?:the )?(?:private |initial )?devstand|"
            r"devstand[^.;,]{0,120}(?:hosts?|runs?|stores?|contains?|keeps?)[^.;,]{0,120}"
            r"(?:postgresql|postgres|pgdata|canonical database)",
            normalized,
        ):
            statement = claim.group(0)
            context = normalized[max(0, claim.start() - 48) : claim.end()]
            negated = any(
                marker in context
                for marker in (" no ", " not ", " never ", "without", "forbidden", "must not", "does not")
            )
            report.check(
                negated,
                f"positive local-devstand database claim in {relative}: {statement!r}",
            )

    receipt = load_json(ROOT / "docs/operations/evidence/2026-08-10-pr-a-host.json")
    report.check(receipt.get("install_confirmation") == "explicitly_rejected", "host receipt omits rejected INSTALL")
    report.check(receipt.get("my_data_hub_container_count") == 0, "host receipt reports a deployed my-data-hub container")
    report.check(receipt.get("local_postgresql_process_observed") is False, "host receipt reports local PostgreSQL")
    report.check(receipt.get("legacy_user_unit", {}).get("enabled") is False, "legacy same-host unit was enabled")
    volume = receipt.get("postgres_volume", {})
    report.check(volume.get("exists") is True and volume.get("pgdata_initialized") is False, "host residue is not disclosed accurately")
    report.check(volume.get("read_only_inventory_entry_count") == 0, "validation-residue volume was not observed empty")


def validate_secret_hygiene(report: Report) -> None:
    forbidden_files = re.compile(r"(^|/)(\.env|.*\.pem|.*\.key|.*\.sqlite(?:3)?|.*\.db)$")
    for path in ROOT.rglob("*"):
        ignored_parts = {
            ".git",
            ".venv",
            "__pycache__",
            ".pytest_cache",
            ".ruff_cache",
            ".mypy_cache",
        }
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
