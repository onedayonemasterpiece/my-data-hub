from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "docs/source-material/idea-hub/idea-20260809-content-platform-current-design.md"
EXPECTED_SOURCE_SHA = "c7efb28231223caa6fd02fcc001a38e0f16bcc3fa4c4cd53e744721b2eac0852"
EXPECTED_AUTHORITY = [
    "owner_decisions",
    "exact_imported_source_research",
    "corrective_adr",
    "machine_readable_invariants",
    "derived_docs_code_tests",
]


def load_yaml(path: str) -> dict:  # type: ignore[type-arg]
    value = yaml.safe_load((ROOT / path).read_text(encoding="utf-8"))
    assert isinstance(value, dict)
    return value


def test_exact_source_and_authority_cannot_be_silently_overridden() -> None:
    invariants = load_yaml("architecture/invariants.yaml")
    assert invariants["authority_order"] == EXPECTED_AUTHORITY
    assert invariants["canonical_source"] == {
        "path": SOURCE.relative_to(ROOT).as_posix(),
        "sha256": EXPECTED_SOURCE_SHA,
    }
    assert hashlib.sha256(SOURCE.read_bytes()).hexdigest() == EXPECTED_SOURCE_SHA
    adr9 = (ROOT / "docs/adr/0009-canonical-postgres-availability.md").read_text()
    assert "SUPERSEDED_BY_ARCHITECTURE_RESET" in adr9
    adr16 = (ROOT / invariants["corrective_adr"]).read_text()
    assert "corrective owner decision" in adr16


def test_owner_approved_architecture_values_are_constant() -> None:
    values = load_yaml("architecture/invariants.yaml")
    assert values["architecture"] == {
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
    assert values["safety"] == {
        "region_talk_state": "paused",
        "production_publication": "disabled",
        "remote_mcp_writes": "disabled",
        "legacy_same_host_install": "forbidden",
        "dns_vpn_443_changes_in_pr_a": "forbidden",
    }


def test_production_control_profile_has_no_local_database_path() -> None:
    compose = load_yaml("compose.control-plane.yaml")
    assert compose["x-my-data-hub-profile"] == "production-lightweight-control-plane"
    assert set(compose["services"]) == {"control-plane"}
    assert not compose.get("volumes")
    serialized = json.dumps(compose, sort_keys=True).lower()
    for forbidden in (
        "postgres",
        "pgdata",
        "pg_dump",
        "database_url",
        "db migrate",
        "backup_postgres",
        "connector-committer",
    ):
        assert forbidden not in serialized
    environment = compose["services"]["control-plane"]["environment"]
    assert environment["MY_DATA_HUB_PRODUCTION_PUBLISH_ENABLED"] == "false"
    assert environment["MY_DATA_HUB_MCP_WRITE_ENABLED"] == "false"
    installer = (ROOT / "deploy/control-plane/install.sh").read_text().lower()
    assert "compose.control-plane.yaml" in installer
    assert "database_url" not in installer
    assert "db migrate" not in installer
    assert "pg_dump" not in installer


def test_repository_wide_deployment_surface_is_closed() -> None:
    assert {path.relative_to(ROOT).as_posix() for path in ROOT.glob("compose*.yaml")} == {
        "compose.yaml",
        "compose.control-plane.yaml",
    }
    assert {
        path.relative_to(ROOT).as_posix()
        for path in (ROOT / "deploy").rglob("*")
        if path.is_file()
    } == {
        "deploy/control-plane/Dockerfile",
        "deploy/control-plane/install.sh",
        "deploy/same-host/install.sh",
    }
    patterns = (
        re.compile(r"postgresql\.service", re.I),
        re.compile(
            r"docker\s+volume\s+create[^\n]*(?:postgres|pgdata)|"
            r"docker\s+compose[^\n]*up[^\n]*(?:postgres|pgdata)",
            re.I,
        ),
        re.compile(r"\b(?:initdb|pg_ctl)\b", re.I),
        re.compile(r"\bpg_dump\b", re.I),
        re.compile(r"\bdb\s+migrate\b", re.I),
        re.compile(r"INSTALL_MY_DATA_HUB_SAME_HOST"),
    )
    allowlist = {
        ".github/workflows/ci.yml",
        "Makefile",
        "compose.yaml",
        "deploy/same-host/install.sh",
        "deploy/control-plane/install.sh",
        "scripts/backup_postgres.sh",
        "scripts/restore_postgres.sh",
    }
    for path in ROOT.rglob("*"):
        if not path.is_file() or any(
            part in {".git", ".venv", "__pycache__"} for part in path.parts
        ):
            continue
        relative_path = path.relative_to(ROOT)
        if not (
            path.suffix in {".sh", ".service", ".timer"}
            or path.name == "Makefile"
            or path.name.startswith("Dockerfile")
            or relative_path.parts[:2] == (".github", "workflows")
            or path.name.startswith("compose")
        ):
            continue
        text = path.read_text(encoding="utf-8")
        if any(pattern.search(text) for pattern in patterns):
            assert relative_path.as_posix() in allowlist


def test_legacy_install_token_hard_fails_before_side_effects(tmp_path: Path) -> None:
    home = tmp_path / "home"
    home.mkdir()
    result = subprocess.run(
        ["bash", str(ROOT / "deploy/same-host/install.sh"), "INSTALL_MY_DATA_HUB_SAME_HOST"],
        env={**os.environ, "HOME": str(home), "PATH": "/usr/bin:/bin"},
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 78
    assert "superseded" in result.stderr and "forbidden" in result.stderr.lower()
    assert list(home.iterdir()) == []


def test_disposable_postgres_has_no_persistent_volume() -> None:
    compose = load_yaml("compose.yaml")
    assert compose["x-my-data-hub-profile"] == "disposable-integration-test-only"
    assert not compose.get("volumes")
    postgres = compose["services"]["postgres"]
    assert postgres["restart"] == "no"
    assert postgres["tmpfs"] == ["/var/lib/postgresql:size=1g,mode=0700"]
    makefile = (ROOT / "Makefile").read_text()
    assert "docker compose down -v --remove-orphans" in makefile
    ci = load_yaml(".github/workflows/ci.yml")
    job = ci["jobs"]["postgres-integration"]
    assert job["runs-on"] == "ubuntu-latest"
    assert "volumes" not in job["services"]["postgres"]
    assert "docker volume create" not in (ROOT / ".github/workflows/ci.yml").read_text()


def test_docs_have_one_kaggle_master_topology() -> None:
    for path in (
        "README.md",
        "docs/00-source-of-truth.md",
        "docs/02-target-architecture.md",
        "docs/vision/my-data-hub-target-vision.md",
    ):
        text = (ROOT / path).read_text(encoding="utf-8")
        assert "Kaggle master" in text or "Kaggle\n  master" in text
        assert "devstand" in text.lower()
    reversal_patterns = (
        "Kaggle is not master",
        "Kaggle never becomes a writable",
        "Kaggle is not the canonical database",
        "never hosts a writable master database",
        "canonical PostgreSQL instance remains on the devstand",
        "normally-always-on canonical PostgreSQL on devstand",
        "PostgreSQL и orchestrator работают на одном initial devstand",
        "PostgreSQL/internal services on the private devstand",
        "PostgreSQL migration revision matches repository head",
        "latest local and off-host generation age",
        "local plus off-host backup is hash-verified",
        "Devstand auto-start and health checks work",
    )
    allow = {
        "docs/adr/0009-canonical-postgres-availability.md",
        "docs/incidents/2026-08-10-local-postgres-architecture-drift.md",
    }
    for path in sorted((ROOT / "docs").rglob("*.md")):
        relative = path.relative_to(ROOT).as_posix()
        if relative.startswith("docs/source-material/") or relative in allow:
            continue
        text = path.read_text(encoding="utf-8")
        normalized = re.sub(r"\s+", " ", text.lower())
        for pattern in reversal_patterns:
            assert pattern.lower() not in normalized, f"{pattern!r} in {relative}"
        for claim in re.finditer(
            r"(?:postgresql|postgres|pgdata|canonical database)[^.;,]{0,120}"
            r"(?:on|in|at) (?:the )?(?:private |initial )?devstand|"
            r"devstand[^.;,]{0,120}(?:hosts?|runs?|stores?|contains?|keeps?)[^.;,]{0,120}"
            r"(?:postgresql|postgres|pgdata|canonical database)",
            normalized,
        ):
            statement = claim.group(0)
            context = normalized[max(0, claim.start() - 48) : claim.end()]
            assert any(
                marker in context
                for marker in (" no ", " not ", " never ", "without", "forbidden", "must not", "does not")
            ), f"positive local-devstand database claim in {relative}: {statement!r}"


def test_region_talk_and_write_gates_remain_frozen() -> None:
    invariants = load_yaml("architecture/invariants.yaml")
    pipeline = json.loads((ROOT / "config/pipelines/region-talk.v1.json").read_text())
    assert invariants["safety"]["region_talk_state"] == "paused"
    assert pipeline["status"] == "paused"
    publication = next(stage for stage in pipeline["stages"] if stage["key"] == "publication_dispatch")
    assert publication["enabled_by_default"] is False
    control = load_yaml("compose.control-plane.yaml")["services"]["control-plane"]["environment"]
    assert control["MY_DATA_HUB_PRODUCTION_PUBLISH_ENABLED"] == "false"
    assert control["MY_DATA_HUB_MCP_WRITE_ENABLED"] == "false"


def test_host_evidence_is_honest_about_validation_residue() -> None:
    receipt = json.loads((ROOT / "docs/operations/evidence/2026-08-10-pr-a-host.json").read_text())
    assert receipt["install_confirmation"] == "explicitly_rejected"
    assert receipt["install_receipt_present"] is False
    assert receipt["my_data_hub_container_count"] == 0
    assert receipt["local_postgresql_process_observed"] is False
    assert receipt["legacy_user_unit"]["enabled"] is False
    assert receipt["postgres_volume"]["exists"] is True
    assert receipt["postgres_volume"]["read_only_inventory_entry_count"] == 0
    assert receipt["postgres_volume"]["pgdata_initialized"] is False
    assert receipt["production_runtime_deployed"] is False
