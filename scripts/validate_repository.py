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


def find_dangerous_python_process_calls(source: str) -> list[str]:
    """Return static process commands that could create a local PostgreSQL runtime."""

    tree = ast.parse(source)
    process_calls = {
        "os.system",
        "os.execv",
        "os.execve",
        "os.execl",
        "os.execlp",
        "os.execvp",
        "os.spawnl",
        "os.spawnlp",
        "os.spawnv",
        "os.spawnvp",
        "subprocess.run",
        "subprocess.Popen",
        "subprocess.call",
        "subprocess.check_call",
        "subprocess.check_output",
        "asyncio.create_subprocess_exec",
        "asyncio.create_subprocess_shell",
    }
    dangerous = re.compile(
        r"docker\s+(?:compose|volume).*(?:postgres|pgdata)|"
        r"\b(?:initdb|pg_ctl|pg_dump)\b|"
        r"\bdb\s+migrate\b|"
        r"systemctl.*postgres",
        re.I,
    )

    def qualified_name(node: ast.AST) -> str:
        if isinstance(node, ast.Name):
            return node.id
        if isinstance(node, ast.Attribute):
            parent = qualified_name(node.value)
            return f"{parent}.{node.attr}" if parent else node.attr
        return ""

    def literals(node: ast.AST) -> list[str]:
        values: list[str] = []
        for child in ast.walk(node):
            if isinstance(child, ast.Constant) and isinstance(child.value, str):
                values.append(child.value)
        return values

    findings: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or qualified_name(node.func) not in process_calls:
            continue
        command = " ".join(value for argument in node.args for value in literals(argument))
        if dangerous.search(command):
            findings.append(f"line {node.lineno}: {command}")
    return findings


@dataclass(frozen=True, slots=True)
class KaggleTransportFinding:
    kind: str
    line: int
    detail: str


def find_direct_kaggle_transports(source: str) -> list[KaggleTransportFinding]:
    """Find direct Kaggle SDK, HTTP, or CLI transport surfaces in Python.

    Imports of the repository's central adapter are intentionally not findings:
    they are reviewed call sites, not another transport implementation.
    """

    tree = ast.parse(source)
    findings: list[KaggleTransportFinding] = []

    def qualified_name(node: ast.AST) -> str:
        if isinstance(node, ast.Name):
            return node.id
        if isinstance(node, ast.Attribute):
            parent = qualified_name(node.value)
            return f"{parent}.{node.attr}" if parent else node.attr
        return ""

    def static_text(node: ast.AST) -> str | None:
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return node.value
        if isinstance(node, ast.JoinedStr):
            parts: list[str] = []
            for value in node.values:
                if isinstance(value, ast.Constant) and isinstance(value.value, str):
                    parts.append(value.value)
                else:
                    parts.append("{}")
            return "".join(parts)
        if isinstance(node, (ast.List, ast.Tuple)):
            values = [static_text(value) for value in node.elts]
            return " ".join(value for value in values if value is not None)
        return None

    assignments: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, (ast.Assign, ast.AnnAssign)):
            value = static_text(node.value) if node.value is not None else None
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            if value is not None:
                for target in targets:
                    if isinstance(target, ast.Name):
                        assignments[target.id] = value

    def resolved_text(node: ast.AST) -> str:
        if isinstance(node, ast.Name):
            return assignments.get(node.id, "")
        return static_text(node) or ""

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                root = alias.name.split(".", 1)[0]
                if root in {"kaggle", "kagglesdk"}:
                    findings.append(KaggleTransportFinding("sdk_import", node.lineno, alias.name))
        elif isinstance(node, ast.ImportFrom) and node.module:
            root = node.module.split(".", 1)[0]
            if root in {"kaggle", "kagglesdk"}:
                findings.append(KaggleTransportFinding("sdk_import", node.lineno, node.module))
        elif isinstance(node, ast.Call):
            function = qualified_name(node.func)
            arguments = [*node.args, *(keyword.value for keyword in node.keywords)]
            command = " ".join(resolved_text(argument) for argument in arguments)
            is_http_call = function.rsplit(".", 1)[-1] in {
                "Request",
                "urlopen",
                "request",
                "get",
                "post",
                "put",
                "patch",
                "delete",
            }
            if is_http_call and re.search(r"https?://(?:www\.)?(?:api\.)?kaggle\.com/api/", command, re.I):
                findings.append(KaggleTransportFinding("http", node.lineno, command))
            process_calls = {
                "os.system",
                "subprocess.run",
                "subprocess.Popen",
                "subprocess.call",
                "subprocess.check_call",
                "subprocess.check_output",
                "asyncio.create_subprocess_exec",
                "asyncio.create_subprocess_shell",
            }
            if function in process_calls and re.search(
                r"(?:^|[;&|\s])(?:python(?:3)?\s+-m\s+)?kaggle\s+"
                r"(?:auth|config|datasets?|kernels?|competitions?|models?)(?:\s|$)",
                command,
                re.I,
            ):
                findings.append(KaggleTransportFinding("cli", node.lineno, command))
    return sorted(findings, key=lambda item: (item.line, item.kind, item.detail))


def find_direct_kaggle_text_transports(source: str) -> list[KaggleTransportFinding]:
    """Find direct Kaggle transports in shell, workflow, and Notebook text."""

    patterns = {
        "sdk_import": re.compile(r"\b(?:from|import)\s+(?:kaggle(?:\.|\s)|kagglesdk(?:\.|\s))", re.I),
        "http": re.compile(
            r"\b(?:curl|wget)\b[^\n]*https?://(?:www\.)?(?:api\.)?kaggle\.com/api/",
            re.I,
        ),
        "cli": re.compile(
            r"(?:^|[;&|\s])(?:python(?:3)?\s+-m\s+)?kaggle\s+"
            r"(?:auth|config|datasets?|kernels?|competitions?|models?)(?:\s|$)",
            re.I,
        ),
    }
    findings: list[KaggleTransportFinding] = []
    for line_number, line in enumerate(source.splitlines(), start=1):
        for kind, pattern in patterns.items():
            if pattern.search(line):
                findings.append(KaggleTransportFinding(kind, line_number, line.strip()))
    return findings


def validate_kaggle_transport(report: Report) -> None:
    """Enforce one production Kaggle transport implementation repository-wide."""

    central = "src/my_data_hub/providers/kaggle/adapter.py"
    ignored_roots = {"tests", "docs", ".git", ".venv", ".codex", "artifacts"}
    implementation_files: set[str] = set()
    central_findings: list[KaggleTransportFinding] = []
    for path in sorted(ROOT.rglob("*.py")):
        relative = path.relative_to(ROOT).as_posix()
        if ignored_roots.intersection(path.relative_to(ROOT).parts):
            continue
        try:
            findings = find_direct_kaggle_transports(path.read_text(encoding="utf-8"))
        except SyntaxError as exc:
            report.fail(f"cannot parse Kaggle transport surface {relative}: {exc}")
            continue
        if not findings:
            continue
        implementation_files.add(relative)
        if relative == central:
            central_findings = findings
            continue
        report.fail(
            f"second direct Kaggle transport implementation in {relative}: "
            + ", ".join(f"{finding.kind}@{finding.line}" for finding in findings)
        )
    text_candidates = [
        path
        for path in ROOT.rglob("*")
        if path.is_file()
        and not ignored_roots.intersection(path.relative_to(ROOT).parts)
        and (
            path.suffix.lower() in {".sh", ".yml", ".yaml", ".ipynb"}
            or path.name == "Makefile"
            or path.name.startswith("Dockerfile")
        )
    ]
    for path in sorted(text_candidates):
        relative = path.relative_to(ROOT).as_posix()
        findings = find_direct_kaggle_text_transports(path.read_text(encoding="utf-8"))
        if not findings:
            continue
        implementation_files.add(relative)
        report.fail(
            f"second direct Kaggle transport implementation in {relative}: "
            + ", ".join(f"{finding.kind}@{finding.line}" for finding in findings)
        )
    report.check((ROOT / central).is_file(), f"central Kaggle transport implementation is missing: {central}")
    report.check(
        any(
            finding.kind == "sdk_import" and finding.detail == "kaggle.api.kaggle_api_extended"
            for finding in central_findings
        ),
        "central Kaggle adapter lost its pinned official SDK transport",
    )
    report.check(
        implementation_files == {central},
        f"direct Kaggle transport implementation inventory drifted: {sorted(implementation_files)}",
    )


OPERATIONAL_MVP_COMPLETE = "MY_DATA_HUB_OPERATIONAL_MVP_COMPLETE"
OPERATIONAL_MVP_BLOCKED = "MY_DATA_HUB_OPERATIONAL_MVP_BLOCKED"
OPERATIONAL_MVP_GATE_IDS = tuple("ABCDEFGHIJKLMN")
OPERATIONAL_MVP_REQUIRED_EVIDENCE_KINDS = {
    "implementation_review": "IMPLEMENTATION_REVIEW",
    "deployment": "DEPLOYMENT",
    "post_deploy": "POST_DEPLOY",
    "security_audit": "SECURITY_AUDIT",
    "data_integrity_audit": "DATA_INTEGRITY_AUDIT",
    "operational_matrix": "REAL_KAGGLE_MATRIX",
}
OPERATIONAL_MVP_EVIDENCE_SCHEMAS = {
    "IMPLEMENTATION_REVIEW": "schemas/operational-mvp-evidence.v1.schema.json",
    "DEPLOYMENT": "schemas/operational-mvp-evidence.v1.schema.json",
    "POST_DEPLOY": "schemas/operational-mvp-evidence.v1.schema.json",
    "SECURITY_AUDIT": "schemas/operational-mvp-evidence.v1.schema.json",
    "DATA_INTEGRITY_AUDIT": "schemas/operational-mvp-evidence.v1.schema.json",
    "GATE_EVIDENCE": "schemas/operational-mvp-evidence.v1.schema.json",
    "REAL_KAGGLE_MATRIX": "schemas/provider/operational-kaggle-matrix-receipt.v1.schema.json",
    "CONNECTOR_DURABILITY": "schemas/connector-durability-receipt.v1.schema.json",
}
OPERATIONAL_MVP_GATE_EVIDENCE_KINDS = {
    "A": frozenset({"REAL_KAGGLE_MATRIX", "GATE_EVIDENCE"}),
    "B": frozenset({"REAL_KAGGLE_MATRIX", "GATE_EVIDENCE"}),
    "C": frozenset({"REAL_KAGGLE_MATRIX", "GATE_EVIDENCE"}),
    "D": frozenset({"REAL_KAGGLE_MATRIX", "GATE_EVIDENCE"}),
    "E": frozenset({"REAL_KAGGLE_MATRIX", "GATE_EVIDENCE"}),
    "F": frozenset({"REAL_KAGGLE_MATRIX", "GATE_EVIDENCE"}),
    "G": frozenset({"REAL_KAGGLE_MATRIX", "GATE_EVIDENCE"}),
    "H": frozenset({"REAL_KAGGLE_MATRIX", "GATE_EVIDENCE"}),
    "I": frozenset({"SECURITY_AUDIT"}),
    "J": frozenset({"DATA_INTEGRITY_AUDIT"}),
    "K": frozenset({"DATA_INTEGRITY_AUDIT"}),
    "L": frozenset({"CONNECTOR_DURABILITY"}),
    "M": frozenset({"DEPLOYMENT", "POST_DEPLOY"}),
    "N": frozenset({"IMPLEMENTATION_REVIEW", "POST_DEPLOY"}),
}
OPERATIONAL_MVP_REQUIRED_HOSTED_CHECKS = frozenset({"contracts", "postgres-integration"})
PROVIDER_REAL_RUNNER = ["self-hosted", "linux", "my-data-hub-devstand"]


def repository_head_commit(root: Path) -> str | None:
    """Return the exact checkout commit without accepting a symbolic label."""

    result = subprocess.run(
        ["git", "rev-parse", "--verify", "HEAD^{commit}"],
        cwd=root,
        text=True,
        capture_output=True,
        check=False,
    )
    commit = result.stdout.strip()
    if result.returncode != 0 or re.fullmatch(r"[0-9a-f]{40}", commit) is None:
        return None
    return commit


def repository_commit_relationship(root: Path, ancestor: str, descendant: str) -> str | None:
    """Return the exact git relationship without trusting receipt labels."""

    if ancestor == descendant:
        return None
    parents = subprocess.run(
        ["git", "show", "-s", "--format=%P", descendant],
        cwd=root,
        text=True,
        capture_output=True,
        check=False,
    )
    if parents.returncode != 0:
        return None
    exact_parents = parents.stdout.strip().split()
    if len(exact_parents) < 2:
        return None
    if ancestor in exact_parents:
        return "PARENT"
    relationship = subprocess.run(
        ["git", "merge-base", "--is-ancestor", ancestor, descendant],
        cwd=root,
        capture_output=True,
        check=False,
    )
    return "ANCESTOR" if relationship.returncode == 0 else None


def validate_operational_mvp_evidence_content(
    item: dict[str, Any],
    content: dict[str, Any],
    *,
    root: Path,
) -> list[str]:
    """Bind an evidence index entry to schema-valid, semantically typed JSON."""

    errors: list[str] = []
    evidence_id = str(item.get("evidence_id", "<unknown>"))
    artifact_kind = item.get("artifact_kind")
    expected_schema = OPERATIONAL_MVP_EVIDENCE_SCHEMAS.get(str(artifact_kind))
    schema_path = item.get("schema_path")
    if expected_schema is None or schema_path != expected_schema:
        errors.append(f"evidence {evidence_id} schema {schema_path!r} is not authoritative for {artifact_kind!r}")
        return errors
    relative_schema = Path(str(schema_path))
    if relative_schema.is_absolute() or ".." in relative_schema.parts:
        errors.append(f"evidence {evidence_id} schema path is unsafe")
        return errors
    schema_file = root / relative_schema
    if not schema_file.is_file():
        errors.append(f"evidence {evidence_id} schema is absent: {schema_path}")
        return errors
    try:
        schema = load_json(schema_file)
        schema_errors = sorted(
            Draft202012Validator(schema, format_checker=FormatChecker()).iter_errors(content),
            key=lambda error: list(error.path),
        )
    except (OSError, json.JSONDecodeError) as exc:
        errors.append(f"evidence {evidence_id} schema cannot be read: {exc}")
        return errors
    if schema_errors:
        errors.append(
            f"evidence {evidence_id} violates its semantic schema: "
            + "; ".join(error.message for error in schema_errors[:5])
        )
        return errors

    source_commit = item.get("source_commit")
    gate_ids = item.get("gate_ids")
    requirement_ids = item.get("requirement_ids")
    if artifact_kind in {
        "IMPLEMENTATION_REVIEW",
        "DEPLOYMENT",
        "POST_DEPLOY",
        "SECURITY_AUDIT",
        "DATA_INTEGRITY_AUDIT",
        "GATE_EVIDENCE",
    }:
        if content.get("evidence_class") != artifact_kind:
            errors.append(f"evidence {evidence_id} content class differs from its index")
        if content.get("source_commit") != source_commit:
            errors.append(f"evidence {evidence_id} content source commit differs from its index")
        if content.get("gate_ids") != gate_ids:
            errors.append(f"evidence {evidence_id} content gate scope differs from its index")
        if content.get("requirement_ids") != requirement_ids:
            errors.append(f"evidence {evidence_id} content requirement scope differs from its index")
        assertions = content.get("assertions", [])
        assertion_gates = {assertion.get("gate_id") for assertion in assertions if isinstance(assertion, dict)}
        assertion_requirements = {
            requirement
            for assertion in assertions
            if isinstance(assertion, dict)
            for requirement in assertion.get("requirement_ids", [])
        }
        if assertion_gates != set(gate_ids or []):
            errors.append(f"evidence {evidence_id} assertions do not cover its exact gate scope")
        if assertion_requirements != set(requirement_ids or []):
            errors.append(f"evidence {evidence_id} assertions do not cover its exact requirement scope")
        for check in content.get("hosted_checks", []):
            if not isinstance(check, dict):
                continue
            check_name = check.get("name")
            expected_runner: str | list[str] = (
                PROVIDER_REAL_RUNNER if check_name == "provider-real" else "ubuntu-latest"
            )
            if check.get("runner") != expected_runner:
                errors.append(f"evidence {evidence_id} hosted check {check_name!r} uses an untrusted runner")
    elif artifact_kind == "REAL_KAGGLE_MATRIX":
        if content.get("commit_sha") != source_commit or content.get("outcome") != "PASS":
            errors.append(f"evidence {evidence_id} matrix content is not a source-bound PASS")
        expected_requirements = {f"FM{ordinal:02d}" for ordinal in range(1, 25)}
        if set(requirement_ids or []) != expected_requirements:
            errors.append(f"evidence {evidence_id} matrix does not declare exact FM01-FM24 coverage")
    elif artifact_kind == "CONNECTOR_DURABILITY":
        if content.get("state") != "DURABLE_COMPLETE":
            errors.append(f"evidence {evidence_id} connector receipt is not DURABLE_COMPLETE")
        if gate_ids != ["L"]:
            errors.append(f"evidence {evidence_id} connector receipt must be scoped only to Gate L")
    return errors


def validate_operational_mvp_receipt_semantics(
    receipt: dict[str, Any],
    *,
    root: Path,
    expected_source_commit: str | None,
    allow_complete: bool,
) -> list[str]:
    """Validate cross-field and referenced-evidence acceptance invariants.

    JSON Schema enforces the shape and conditional thresholds. This function
    binds identifiers to exact artifacts and to the checkout being evaluated.
    """

    errors: list[str] = []
    verdict = receipt.get("verdict")
    complete = verdict == OPERATIONAL_MVP_COMPLETE
    if complete and not allow_complete:
        errors.append("synthetic/example receipts may not use the COMPLETE verdict")

    evidence_items = receipt.get("evidence", [])
    evidence_by_id: dict[str, dict[str, Any]] = {}
    evidence_content_by_id: dict[str, dict[str, Any]] = {}
    for item in evidence_items if isinstance(evidence_items, list) else []:
        if not isinstance(item, dict):
            continue
        evidence_id = item.get("evidence_id")
        if not isinstance(evidence_id, str):
            continue
        if evidence_id in evidence_by_id:
            errors.append(f"duplicate evidence_id: {evidence_id}")
            continue
        evidence_by_id[evidence_id] = item

        locator = item.get("locator")
        if item.get("storage") == "REPOSITORY_FILE" and isinstance(locator, str):
            relative = Path(locator)
            if relative.is_absolute() or ".." in relative.parts:
                errors.append(f"unsafe repository evidence locator for {evidence_id}: {locator}")
                continue
            evidence_path = root / relative
            if not evidence_path.is_file():
                errors.append(f"repository evidence does not exist for {evidence_id}: {locator}")
                continue
            observed_hash = hashlib.sha256(evidence_path.read_bytes()).hexdigest()
            if observed_hash != item.get("sha256"):
                errors.append(
                    f"repository evidence hash mismatch for {evidence_id}: "
                    f"declared {item.get('sha256')}, observed {observed_hash}"
                )
                continue
            try:
                content = load_json(evidence_path)
            except (OSError, json.JSONDecodeError) as exc:
                errors.append(f"repository evidence is not readable JSON for {evidence_id}: {exc}")
                continue
            if not isinstance(content, dict):
                errors.append(f"repository evidence must be a JSON object for {evidence_id}")
                continue
            evidence_content_by_id[evidence_id] = content
            errors.extend(validate_operational_mvp_evidence_content(item, content, root=root))

    referenced_ids: set[str] = set()
    for gate in receipt.get("gate_results", []):
        if isinstance(gate, dict):
            referenced_ids.update(gate.get("evidence_refs", []))
    for blocker in receipt.get("blockers", []):
        if isinstance(blocker, dict):
            referenced_ids.update(blocker.get("evidence_refs", []))
    required_evidence = receipt.get("required_evidence", {})
    if isinstance(required_evidence, dict):
        for refs in required_evidence.values():
            if isinstance(refs, list):
                referenced_ids.update(refs)
    operational_matrix = receipt.get("operational_matrix", {})
    if isinstance(operational_matrix, dict):
        matrix_evidence_id = operational_matrix.get("receipt_evidence_id")
        if isinstance(matrix_evidence_id, str):
            referenced_ids.add(matrix_evidence_id)
    for evidence_id in sorted(referenced_ids - set(evidence_by_id)):
        errors.append(f"receipt references unknown evidence_id: {evidence_id}")

    if isinstance(required_evidence, dict):
        for section, required_kind in OPERATIONAL_MVP_REQUIRED_EVIDENCE_KINDS.items():
            refs = required_evidence.get(section, [])
            if not isinstance(refs, list):
                continue
            for evidence_id in refs:
                evidence = evidence_by_id.get(evidence_id)
                if evidence is not None and evidence.get("artifact_kind") != required_kind:
                    errors.append(
                        f"required_evidence.{section} references {evidence_id} with kind "
                        f"{evidence.get('artifact_kind')!r}, expected {required_kind!r}"
                    )

    counts = receipt.get("counts", {})
    matrix = operational_matrix if isinstance(operational_matrix, dict) else {}
    run_refs = matrix.get("distinct_provider_run_refs", [])
    kernel_ids = matrix.get("distinct_provider_kernel_ids", [])
    if isinstance(counts, dict):
        if isinstance(run_refs, list) and counts.get("real_kaggle_run_ids") != len(run_refs):
            errors.append("counts.real_kaggle_run_ids does not match distinct provider run refs")
        if isinstance(kernel_ids, list) and counts.get("real_kaggle_kernel_ids") != len(kernel_ids):
            errors.append("counts.real_kaggle_kernel_ids does not match distinct provider kernel IDs")
        if counts.get("operational_scenarios_passed") != matrix.get("passed_scenarios"):
            errors.append("counts.operational_scenarios_passed does not match matrix PASS count")
    scenario_total = sum(
        value
        for value in (
            matrix.get("passed_scenarios"),
            matrix.get("failed_scenarios"),
            matrix.get("blocked_scenarios"),
        )
        if isinstance(value, int) and not isinstance(value, bool)
    )
    if scenario_total != matrix.get("planned_scenarios"):
        errors.append("operational matrix PASS/FAIL/BLOCKED counts do not total 24")

    if complete:
        evaluated_commit = receipt.get("evaluated_source_commit")
        if expected_source_commit is None:
            errors.append("COMPLETE receipt cannot bind the evaluated checkout commit")
        elif evaluated_commit != expected_source_commit:
            errors.append(
                "COMPLETE receipt evaluated_source_commit is stale: "
                f"expected {expected_source_commit}, received {evaluated_commit}"
            )

        identity = receipt.get("implementation_identity", {})
        if isinstance(identity, dict):
            for field in ("merge_commit", "deployed_commit", "post_deploy_verified_commit"):
                if identity.get(field) != evaluated_commit:
                    errors.append(f"COMPLETE receipt {field} must equal evaluated_source_commit")

            reviewed_head = identity.get("reviewed_head_commit")
            merge_commit = identity.get("merge_commit")
            if not isinstance(reviewed_head, str) or not isinstance(merge_commit, str):
                errors.append("COMPLETE receipt lacks exact reviewed-head/merge provenance")
            else:
                relationship = repository_commit_relationship(root, reviewed_head, merge_commit)
                if relationship is None:
                    errors.append(
                        "COMPLETE reviewed_head_commit is neither a parent nor ancestor of an exact merge commit"
                    )

            review_refs = (
                required_evidence.get("implementation_review", []) if isinstance(required_evidence, dict) else []
            )
            review_contents = [
                evidence_content_by_id[evidence_id]
                for evidence_id in review_refs
                if evidence_id in evidence_content_by_id
            ]
            if not review_contents:
                errors.append("COMPLETE receipt lacks readable semantic implementation review evidence")
            for content in review_contents:
                if content.get("reviewed_head_commit") != reviewed_head:
                    errors.append("implementation review evidence has another reviewed head commit")
                if content.get("merge_commit") != merge_commit:
                    errors.append("implementation review evidence has another merge commit")
                if isinstance(reviewed_head, str) and isinstance(merge_commit, str):
                    exact_relationship = repository_commit_relationship(root, reviewed_head, merge_commit)
                    if content.get("review_relationship") != exact_relationship:
                        errors.append("implementation review evidence misstates the reviewed-head relationship")
                checks = content.get("hosted_checks", [])
                check_names = {check.get("name") for check in checks if isinstance(check, dict)}
                if len(check_names) != len(checks):
                    errors.append("implementation review evidence repeats a hosted check name")
                missing_checks = OPERATIONAL_MVP_REQUIRED_HOSTED_CHECKS - check_names
                if missing_checks:
                    errors.append(
                        "implementation review evidence lacks required hosted checks: "
                        + ", ".join(sorted(missing_checks))
                    )
                for check in checks:
                    if isinstance(check, dict) and check.get("head_commit") != reviewed_head:
                        errors.append(f"hosted check {check.get('name')} is not bound to reviewed_head_commit")

            for section, commit_fields in {
                "deployment": ("deployed_commit",),
                "post_deploy": ("deployed_commit", "post_deploy_verified_commit"),
            }.items():
                refs = required_evidence.get(section, []) if isinstance(required_evidence, dict) else []
                contents = [
                    evidence_content_by_id[evidence_id] for evidence_id in refs if evidence_id in evidence_content_by_id
                ]
                if not contents:
                    errors.append(f"COMPLETE receipt lacks readable semantic {section} evidence")
                for content in contents:
                    for field in commit_fields:
                        if content.get(field) != identity.get(field):
                            errors.append(f"{section} evidence {field} differs from implementation identity")
                    if content.get("deployment_tree_state") != "CLEAN":
                        errors.append(f"{section} evidence did not observe a clean deployed tree")
                    if section == "post_deploy":
                        for check in content.get("hosted_checks", []):
                            if isinstance(check, dict) and check.get("head_commit") != identity.get("deployed_commit"):
                                errors.append("post-deploy hosted check is not bound to deployed_commit")

        gate_results = receipt.get("gate_results", [])
        gate_ids = [
            gate.get("gate_id")
            for gate in gate_results
            if isinstance(gate, dict) and isinstance(gate.get("gate_id"), str)
        ]
        if sorted(gate_ids) != list(OPERATIONAL_MVP_GATE_IDS):
            errors.append("COMPLETE receipt must contain each Gate A-N exactly once")

        for gate in gate_results if isinstance(gate_results, list) else []:
            if not isinstance(gate, dict) or not isinstance(gate.get("gate_id"), str):
                continue
            gate_id = str(gate["gate_id"])
            refs = gate.get("evidence_refs", [])
            scoped_items = [evidence_by_id[evidence_id] for evidence_id in refs if evidence_id in evidence_by_id]
            for item in scoped_items:
                if item.get("evidence_id") not in evidence_content_by_id:
                    errors.append(
                        f"Gate {gate_id} evidence is not locally content-verifiable: {item.get('evidence_id')}"
                    )
                if gate_id not in item.get("gate_ids", []):
                    errors.append(
                        f"Gate {gate_id} references evidence outside its declared gate scope: {item.get('evidence_id')}"
                    )
                if not item.get("requirement_ids"):
                    errors.append(
                        f"Gate {gate_id} references evidence without requirement-specific scope: "
                        f"{item.get('evidence_id')}"
                    )
            required_kinds = OPERATIONAL_MVP_GATE_EVIDENCE_KINDS.get(gate_id, frozenset())
            if not any(
                item.get("artifact_kind") in required_kinds and item.get("evidence_id") in evidence_content_by_id
                for item in scoped_items
            ):
                errors.append(
                    f"Gate {gate_id} lacks requirement-specific evidence kind; expected one of {sorted(required_kinds)}"
                )

        for evidence_id, item in evidence_by_id.items():
            if item.get("live_evidence") is not True:
                errors.append(f"COMPLETE evidence is not live: {evidence_id}")
            if item.get("source_commit") != evaluated_commit:
                errors.append(f"COMPLETE evidence source commit differs from evaluated commit: {evidence_id}")
            locator = str(item.get("locator", "")).lower()
            if "examples/" in locator or "synthetic" in locator:
                errors.append(f"COMPLETE receipt cites synthetic/example evidence: {evidence_id}")

        required_ids = (
            {evidence_id for refs in required_evidence.values() if isinstance(refs, list) for evidence_id in refs}
            if isinstance(required_evidence, dict)
            else set()
        )
        for evidence_id in sorted(required_ids):
            item = evidence_by_id.get(evidence_id)
            if item is not None and item.get("storage") != "REPOSITORY_FILE":
                errors.append(f"required COMPLETE evidence must be locally hash-verifiable: {evidence_id}")

        if isinstance(counts, dict) and counts.get("bloggers_imported") != counts.get("ydb_bloggers_observed"):
            errors.append("COMPLETE receipt blogger import count differs from YDB inventory")

        matrix_evidence_id = matrix.get("receipt_evidence_id")
        matrix_evidence = evidence_by_id.get(matrix_evidence_id)
        if matrix_evidence is None:
            errors.append("COMPLETE receipt has no resolvable operational matrix evidence")
        elif matrix_evidence.get("artifact_kind") != "REAL_KAGGLE_MATRIX":
            errors.append("operational matrix receipt evidence has the wrong artifact kind")
        elif matrix_evidence.get("storage") == "REPOSITORY_FILE":
            matrix_path = root / str(matrix_evidence.get("locator"))
            if matrix_path.is_file():
                try:
                    matrix_receipt = load_json(matrix_path)
                except (OSError, json.JSONDecodeError) as exc:
                    errors.append(f"cannot read operational matrix receipt: {exc}")
                else:
                    if not isinstance(matrix_receipt, dict):
                        errors.append("operational matrix evidence must be a JSON object")
                        matrix_receipt = {}
                    matrix_schema_path = (
                        root / "schemas" / "provider" / "operational-kaggle-matrix-receipt.v1.schema.json"
                    )
                    scenario_schema_path = (
                        root / "schemas" / "provider" / "operational-kaggle-scenario-receipt.v1.schema.json"
                    )
                    matrix_schema_errors: list[Any] = []
                    if not matrix_schema_path.is_file() or not scenario_schema_path.is_file():
                        errors.append("operational matrix evidence schemas are absent")
                    else:
                        matrix_schema = load_json(matrix_schema_path)
                        matrix_schema_errors = sorted(
                            Draft202012Validator(
                                matrix_schema,
                                format_checker=FormatChecker(),
                            ).iter_errors(matrix_receipt),
                            key=lambda item: list(item.path),
                        )
                        if matrix_schema_errors:
                            errors.append(
                                "operational matrix evidence violates its schema: "
                                + "; ".join(error.message for error in matrix_schema_errors[:5])
                            )
                    comparisons = {
                        "commit_sha": evaluated_commit,
                        "outcome": "PASS",
                        "live_evidence": True,
                        "planned_scenarios": 24,
                        "passed_scenarios": matrix.get("passed_scenarios"),
                        "failed_scenarios": matrix.get("failed_scenarios"),
                        "blocked_scenarios": matrix.get("blocked_scenarios"),
                        "distinct_provider_run_refs": run_refs,
                        "distinct_provider_kernel_ids": kernel_ids,
                    }
                    for field, expected in comparisons.items():
                        if matrix_receipt.get(field) != expected:
                            errors.append(f"operational matrix evidence {field} does not match final receipt")
                    matrix_lifecycle = matrix_receipt.get("lifecycle_gates", {})
                    final_lifecycle = matrix.get("lifecycle_gates", {})
                    for field in (
                        "master_boots",
                        "clean_rotations",
                        "abrupt_master_terminations",
                        "control_plane_restarts",
                        "host_reboots",
                        "soak_runs",
                        "soak_duration_seconds",
                    ):
                        if matrix_lifecycle.get(field) != final_lifecycle.get(field):
                            errors.append(f"operational matrix lifecycle field {field} does not match")
                    if scenario_schema_path.is_file() and not matrix_schema_errors:
                        scenario_schema = load_json(scenario_schema_path)
                        scenario_run_refs: set[str] = set()
                        scenario_kernel_ids: set[int] = set()
                        for summary in matrix_receipt.get("scenario_receipts", []):
                            if not isinstance(summary, dict):
                                continue
                            receipt_name = summary.get("receipt")
                            if not isinstance(receipt_name, str):
                                continue
                            scenario_path = matrix_path.parent / receipt_name
                            if not scenario_path.is_file():
                                errors.append(f"operational scenario evidence is absent: {receipt_name}")
                                continue
                            try:
                                scenario_receipt = load_json(scenario_path)
                            except (OSError, json.JSONDecodeError) as exc:
                                errors.append(f"cannot read operational scenario evidence {receipt_name}: {exc}")
                                continue
                            scenario_schema_errors = sorted(
                                Draft202012Validator(
                                    scenario_schema,
                                    format_checker=FormatChecker(),
                                ).iter_errors(scenario_receipt),
                                key=lambda item: list(item.path),
                            )
                            if scenario_schema_errors:
                                errors.append(
                                    f"operational scenario evidence {receipt_name} violates its schema: "
                                    + "; ".join(error.message for error in scenario_schema_errors[:3])
                                )
                                continue
                            expected_scenario = {
                                "ordinal": summary.get("ordinal"),
                                "requirement_id": summary.get("requirement_id"),
                                "scenario": summary.get("scenario"),
                                "outcome": "PASS",
                                "live_evidence": True,
                                "commit_sha": evaluated_commit,
                            }
                            for field, expected in expected_scenario.items():
                                if scenario_receipt.get(field) != expected:
                                    errors.append(
                                        f"operational scenario evidence {receipt_name} has inconsistent {field}"
                                    )
                            real_identity = scenario_receipt.get("real_run_identity")
                            if isinstance(real_identity, dict):
                                provider_run_ref = real_identity.get("provider_run_ref")
                                provider_kernel_id = real_identity.get("provider_kernel_id")
                                if isinstance(provider_run_ref, str):
                                    scenario_run_refs.add(provider_run_ref)
                                if isinstance(provider_kernel_id, int):
                                    scenario_kernel_ids.add(provider_kernel_id)
                        if scenario_run_refs != set(run_refs):
                            errors.append("operational scenario run identities do not match matrix summary")
                        if scenario_kernel_ids != set(kernel_ids):
                            errors.append("operational scenario kernel IDs do not match matrix summary")

    elif verdict == OPERATIONAL_MVP_BLOCKED:
        gate_results = receipt.get("gate_results", [])
        complete_gate_ids = {
            gate.get("gate_id") for gate in gate_results if isinstance(gate, dict) and gate.get("outcome") == "PASS"
        }
        all_required_refs_present = isinstance(required_evidence, dict) and all(
            bool(required_evidence.get(section)) for section in OPERATIONAL_MVP_REQUIRED_EVIDENCE_KINDS
        )
        matrix_qualifies = (
            matrix.get("passed_scenarios") == 24
            and matrix.get("failed_scenarios") == 0
            and matrix.get("blocked_scenarios") == 0
            and isinstance(run_refs, list)
            and len(run_refs) >= 15
            and isinstance(kernel_ids, list)
            and len(kernel_ids) >= 15
        )
        if complete_gate_ids == set(OPERATIONAL_MVP_GATE_IDS) and all_required_refs_present and matrix_qualifies:
            errors.append(
                "BLOCKED receipt presents all completion gates/evidence as complete; "
                "a blocker must correspond to an actually incomplete criterion"
            )

    return errors


def validate_connector_intake_compose_service(report: Report, service: Any) -> None:
    """Validate the sole reviewed, default-off connector control service."""

    report.check(isinstance(service, dict), "connector intake service must be a Compose mapping")
    if not isinstance(service, dict):
        return
    report.check(
        service.get("profiles") == ["connectors"],
        "connector intake must remain an explicit default-off connectors profile",
    )
    report.check(service.get("read_only") is True, "connector intake filesystem must be read-only")
    report.check(
        service.get("ports") == ["127.0.0.1:${MY_DATA_HUB_CONNECTOR_PORT:-8081}:8081"],
        "connector intake must publish only the reviewed loopback port",
    )
    report.check(not service.get("expose"), "connector intake must not expose an unbounded Compose port")
    connector_environment = service.get("environment", {})
    expected_environment = {
        "MY_DATA_HUB_ENVIRONMENT",
        "MY_DATA_HUB_API_HOST",
        "MY_DATA_HUB_API_PORT",
        "MY_DATA_HUB_CONTROL_LEDGER_PATH",
        "MY_DATA_HUB_MASTER_SESSION_DIRECTORY",
        "MY_DATA_HUB_SCHEDULER_ENABLED",
        "MY_DATA_HUB_PRODUCTION_PUBLISH_ENABLED",
    }
    report.check(
        isinstance(connector_environment, dict) and set(connector_environment) == expected_environment,
        "connector intake environment crossed its reviewed lightweight control boundary",
    )
    connector_serialized = json.dumps(service, sort_keys=True).casefold()
    report.check(
        not any(
            marker in connector_serialized
            for marker in (
                "postgresql://",
                "postgres://",
                "pgdata",
                "database_url",
                "data_plane",
                "25432",
            )
        ),
        "connector intake embeds a database/PGDATA/data-plane endpoint",
    )


def validate_provider_real_workflow_auth_boundary(
    report: Report,
    workflow: Any,
    source: str,
    preflight_source: str,
) -> None:
    """Require provider-real to use the private rotating OAuth runner boundary."""

    jobs = workflow.get("jobs", {}) if isinstance(workflow, dict) else {}
    job = jobs.get("private-notebook-canary", {}) if isinstance(jobs, dict) else {}
    report.check(
        isinstance(job, dict) and job.get("runs-on") == PROVIDER_REAL_RUNNER,
        "provider-real must use exact owner-controlled self-hosted runner labels",
    )
    environment_names: set[str] = set()

    def collect_environment_names(value: Any) -> None:
        if isinstance(value, dict):
            environment = value.get("env")
            if isinstance(environment, dict):
                environment_names.update(str(name) for name in environment)
            for child in value.values():
                collect_environment_names(child)
        elif isinstance(value, list):
            for child in value:
                collect_environment_names(child)

    collect_environment_names(job)
    static_mcp_names = {
        "MY_DATA_HUB_MCP_CANARY_TOKEN",
        "MY_DATA_HUB_MCP_ACCEPTANCE_OPERATOR_TOKEN",
        "MY_DATA_HUB_MCP_MIGRATION_OPERATOR_TOKEN",
        "MY_DATA_HUB_MCP_PROVIDER_OPERATOR_TOKEN",
        "MY_DATA_HUB_DATA_MCP_READER_TOKEN",
        "MY_DATA_HUB_DATA_MCP_OPERATOR_TOKEN",
    }
    forbidden_names = {name for name in environment_names if name in static_mcp_names or "KAGGLE" in name.upper()}
    report.check(
        not forbidden_names,
        f"provider-real declares static MCP/Kaggle credential variables: {sorted(forbidden_names)}",
    )
    report.check(
        "MY_DATA_HUB_MCP_OAUTH_CREDENTIAL_FILE" not in environment_names,
        "provider-real must not materialize the devstand-local OAuth credential path",
    )
    report.check(
        re.search(
            r"secrets\.[A-Z0-9_]*(?:KAGGLE|MCP)[A-Z0-9_]*(?:TOKEN|BEARER|KEY)",
            source,
            re.I,
        )
        is None,
        "provider-real references a static MCP/Kaggle bearer secret",
    )
    report.check(
        "devstand_acceptance_controller.py preflight" in source,
        "provider-real does not run the devstand-local rotating OAuth preflight",
    )
    report.check(
        "validate_oauth_credential_file" in preflight_source
        and "MY_DATA_HUB_MCP_OAUTH_CREDENTIAL_FILE" in preflight_source
        and "RUNNER_ENVIRONMENT" in preflight_source
        and all(
            f'"{profile}"' in preflight_source or f"'{profile}'" in preflight_source
            for profile in ("reader", "operator", "provider")
        ),
        "devstand preflight lacks exact private refresh-file validation for required OAuth profiles",
    )


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
        "migration-reconciliation-report.v1.example.json": ("migration-reconciliation-report.v1.schema.json"),
        "data-connector-envelope.v1.example.json": "data-connector-envelope.v1.schema.json",
        "kaggle-exchange-manifest.v1.example.json": "kaggle-exchange-manifest.v1.schema.json",
        "kaggle-real-canary-receipt.v1.example.json": ("kaggle-real-canary-receipt.v1.schema.json"),
        "kaggle-real-canary-receipt.v2.example.json": ("kaggle-real-canary-receipt.v2.schema.json"),
        "workflow-receipt.v1.example.json": "workflow-receipt.v1.schema.json",
        "deployment-evidence.v1.example.json": "deployment-evidence.v1.schema.json",
        "deployment-evidence.v2.example.json": "deployment-evidence.v2.schema.json",
        "deployment-evidence-state.v1.example.json": ("deployment-evidence-state.v1.schema.json"),
        "post-deploy-verification.v1.example.json": ("post-deploy-verification.v1.schema.json"),
        "post-deploy-verification.v2.example.json": ("post-deploy-verification.v2.schema.json"),
        "operational-mvp-evidence.v1.example.json": ("operational-mvp-evidence.v1.schema.json"),
        "operational-mvp-acceptance-receipt.v1.example.json": ("operational-mvp-acceptance-receipt.v1.schema.json"),
        "master-asset-bundle.v1.example.json": "master-asset-bundle.v1.schema.json",
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
            f"{path.relative_to(ROOT)} violates {schema_name}: " + "; ".join(error.message for error in errors[:5]),
        )
        if example_name == "operational-mvp-acceptance-receipt.v1.example.json" and not errors:
            semantic_errors = validate_operational_mvp_receipt_semantics(
                raw,
                root=ROOT,
                expected_source_commit=repository_head_commit(ROOT),
                allow_complete=False,
            )
            report.check(
                not semantic_errors,
                "synthetic operational MVP receipt is semantically invalid: " + "; ".join(semantic_errors[:5]),
            )

    blocked_receipt = (
        ROOT
        / "docs"
        / "operations"
        / "evidence"
        / "2026-08-11-operational-mvp"
        / "operational-mvp-acceptance-blocked.json"
    )
    blocked_schema = schemas.get("operational-mvp-acceptance-receipt.v1.schema.json")
    report.check(blocked_receipt.is_file(), "final blocked operational MVP receipt is absent")
    if blocked_receipt.is_file() and blocked_schema is not None:
        raw = load_json(blocked_receipt)
        errors = sorted(
            Draft202012Validator(blocked_schema, format_checker=checker).iter_errors(raw),
            key=lambda item: list(item.path),
        )
        report.check(
            not errors,
            "final blocked operational MVP receipt violates its schema: "
            + "; ".join(error.message for error in errors[:5]),
        )
        if not errors:
            semantic_errors = validate_operational_mvp_receipt_semantics(
                raw,
                root=ROOT,
                expected_source_commit=repository_head_commit(ROOT),
                allow_complete=True,
            )
            report.check(
                not semantic_errors,
                "final operational MVP receipt is semantically invalid: " + "; ".join(semantic_errors[:5]),
            )

    evidence_path = (
        ROOT
        / "docs"
        / "operations"
        / "evidence"
        / "2026-08-11-operational-mvp"
        / "kaggle-private-dataset-canary-2.json"
    )
    report.check(evidence_path.is_file(), "missing second real Kaggle private Dataset canary receipt")
    if evidence_path.is_file():
        evidence_errors = sorted(
            Draft202012Validator(
                schemas["kaggle-real-canary-receipt.v1.schema.json"],
                format_checker=checker,
            ).iter_errors(load_json(evidence_path)),
            key=lambda item: list(item.path),
        )
        report.check(
            not evidence_errors,
            f"{evidence_path.relative_to(ROOT)} violates real Kaggle canary schema: "
            + "; ".join(error.message for error in evidence_errors[:5]),
        )

    exact_evidence_path = (
        ROOT
        / "docs"
        / "operations"
        / "evidence"
        / "2026-08-11-operational-mvp"
        / "kaggle-private-dataset-canary-3.json"
    )
    report.check(
        exact_evidence_path.is_file(),
        "missing exact-commit real Kaggle private Dataset canary receipt",
    )
    if exact_evidence_path.is_file():
        exact_evidence_errors = sorted(
            Draft202012Validator(
                schemas["kaggle-real-canary-receipt.v2.schema.json"],
                format_checker=checker,
            ).iter_errors(load_json(exact_evidence_path)),
            key=lambda item: list(item.path),
        )
        report.check(
            not exact_evidence_errors,
            f"{exact_evidence_path.relative_to(ROOT)} violates exact-commit Kaggle canary schema: "
            + "; ".join(error.message for error in exact_evidence_errors[:5]),
        )

    # These schemas are generated from runtime models. Drift is a correctness error.
    sys.path.insert(0, str(ROOT / "src"))
    from my_data_hub.domain.commands import Changeset, SemanticCommand
    from my_data_hub.notebooks.contracts import NotebookInputManifest, NotebookResult
    from my_data_hub.workloads.bloggers.ydb_reader import BloggerYdbSourceReadReceipt
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
        "migration-reconciliation-report.v1.schema.json": (MigrationReconciliationReport),
        "region-talk-ydb-source-read-receipt.v1.schema.json": BloggerYdbSourceReadReceipt,
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
    operational = {
        "01-platform-runtime-smoke": ("runtime_smoke/runtime.py", False, True, frozenset()),
        "02-postgres-master": ("postgres_master/runtime.py", True, True, frozenset()),
        "03-checkpoint-verifier-restore-smoke": (
            "checkpoint_verifier/runtime.py",
            False,
            True,
            frozenset({"import psycopg"}),
        ),
        "04-region-talk-ydb-bloggers-importer": (
            "blogger_importer/runtime.py",
            True,
            True,
            frozenset({"import psycopg", "import ydb"}),
        ),
        "05-e5-blogger-embedding-worker": (
            "embedding_workers/e5_runtime.py", False, True, frozenset({"import psycopg"})
        ),
        "06-bge-m3-blogger-embedding-worker": (
            "embedding_workers/bge_m3_runtime.py", False, True, frozenset({"import psycopg"})
        ),
    }
    for path in sorted((ROOT / "notebooks").glob("*/worker.ipynb")):
        try:
            nb = nbformat.read(path, as_version=4)
            nbformat.validate(nb)
            report.checks += 1
        except Exception as exc:
            report.fail(f"invalid notebook {path.relative_to(ROOT)}: {exc}")
            continue
        metadata = nb.metadata.get("my_data_hub", {})
        code = "\n".join(cell.source for cell in nb.cells if cell.cell_type == "code")
        spec = operational.get(path.parent.name)
        if spec is not None:
            template, canonical_write, external_effects, allowed_imports = spec
            source_path = ROOT / "notebooks" / "templates" / template
            source = source_path.read_text(encoding="utf-8") if source_path.is_file() else ""
            source_sha = hashlib.sha256(source.encode()).hexdigest()
            report.check(source_path.is_file(), f"operational notebook template is absent: {template}")
            report.check(
                metadata.get("primary_source") == f"notebooks/templates/{template}",
                f"operational notebook source path differs: {path.relative_to(ROOT)}",
            )
            report.check(
                metadata.get("primary_source_sha256") == source_sha,
                f"operational notebook source hash differs: {path.relative_to(ROOT)}",
            )
            report.check(
                metadata.get("resource_class") == "orchestrator_protected"
                and metadata.get("privacy") == "private"
                and metadata.get("embedded_secrets") is False,
                f"operational notebook safety metadata differs: {path.relative_to(ROOT)}",
            )
            report.check(
                metadata.get("canonical_write_allowed") is canonical_write
                and metadata.get("external_side_effects_allowed") is external_effects,
                f"operational notebook capability contract differs: {path.relative_to(ROOT)}",
            )
            report.check(
                repr(source) in code,
                f"operational notebook does not embed exact primary source: {template}",
            )
            for token in forbidden_imports:
                report.check(
                    token not in source or token in allowed_imports,
                    f"unapproved operational import {token!r} in {template}",
                )
            report.check(
                not any(marker in source.casefold() for marker in ("kaggle_key =", "password =", "token =")),
                f"operational notebook template may embed a credential: {template}",
            )
            continue
        report.check(metadata.get("canonical_write_allowed") is False, f"{path} allows canonical writes")
        report.check(
            metadata.get("external_side_effects_allowed") is False,
            f"{path} allows external side effects",
        )
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
            idea_source.get("source_commit") == "0c3fcf71b2ee8ba8afa49624bef4b779873802f7",
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
            "Region Talk adaptation manifest violates its schema: " + "; ".join(error.message for error in errors[:5]),
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
            stale_pattern = re.compile(rf"(?<![A-Za-z0-9_.]){re.escape(token)}(?![A-Za-z0-9_.])")
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
        "MY_DATA_HUB_REGION_TALK_YDB_TABLE" in readme and "MY_DATA_HUB_REGION_TALK_YDB_TABLE" in env_example,
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
    expected_remote_mcp = {
        "default_profile": "read_only",
        "owner_operator_profile": "enabled_after_all_write_checkpoint_security_gates",
    }
    expected_region_talk = {
        "production_pipeline": "paused",
        "production_publication": "disabled",
        "bounded_bloggers_import": "completed_or_exact_blocker",
    }
    report.check(invariants.get("authority_order") == expected_authority, "owner-approved authority order drifted")
    report.check(
        invariants.get("architecture") == expected_architecture, "owner-approved architecture invariants drifted"
    )
    report.check(invariants.get("safety") == expected_safety, "owner-approved safety invariants drifted")
    report.check(
        invariants.get("remote_mcp") == expected_remote_mcp,
        "owner-approved remote MCP profile invariants drifted",
    )
    report.check(
        invariants.get("region_talk") == expected_region_talk,
        "owner-approved Region Talk workload invariants drifted",
    )
    report.check(
        invariants.get("operational_state_adr") == "docs/adr/0017-operational-mvp-gated-profiles.md",
        "operational owner-decision ADR binding drifted",
    )

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
    report.check(
        control.get("x-my-data-hub-profile") == "production-lightweight-control-plane", "control profile marker drifted"
    )
    service_names = set(control.get("services", {}))
    report.check(
        service_names == {"control-plane", "connector-intake", "remote-mcp", "oauth-server"},
        "production profile must contain only control API, connector intake, OAuth and opt-in remote MCP services",
    )
    report.check(not control.get("volumes"), "production control plane must not declare volumes")
    control_serialized = json.dumps(control, sort_keys=True).lower()
    for token in (
        "postgres",
        "pgdata",
        "pg_dump",
        "database_url",
        "db migrate",
        "backup_postgres",
        "connector-committer",
    ):
        report.check(
            token not in control_serialized, f"production control plane contains forbidden local-master token: {token}"
        )
    environment = control.get("services", {}).get("control-plane", {}).get("environment", {})
    report.check(
        environment.get("MY_DATA_HUB_PRODUCTION_PUBLISH_ENABLED") == "false", "production publication gate is not false"
    )
    report.check(environment.get("MY_DATA_HUB_MCP_WRITE_ENABLED") == "false", "remote MCP write gate is not false")
    remote_mcp = control.get("services", {}).get("remote-mcp", {})
    report.check(remote_mcp.get("profiles") == ["remote-mcp"], "remote MCP must remain an explicit opt-in profile")
    oauth_server = control.get("services", {}).get("oauth-server", {})
    report.check(
        oauth_server.get("profiles") == ["remote-mcp"],
        "OAuth authorization server must remain coupled to the opt-in remote MCP profile",
    )
    report.check(
        oauth_server.get("entrypoint") == ["python", "-m", "my_data_hub.oauth_server.runtime"],
        "OAuth authorization service does not use the fail-closed production runtime",
    )
    cimd = (ROOT / "src/my_data_hub/oauth_server/client_metadata.py").read_text(
        encoding="utf-8"
    )
    for required in (
        'parsed.hostname != "chatgpt.com"',
        "or port is not none",
        "max_metadata_bytes = 32 * 1_024",
        "fetch_timeout_seconds = 3.0",
        'response.final_url != client_id',
        '"client_secret"',
        '"none" not in methods',
        'prefix = "/connector/oauth/"',
    ):
        report.check(
            required in cimd.lower(),
            f"ChatGPT CIMD bounded public-client invariant is missing: {required}",
        )
    oauth_service = (ROOT / "src/my_data_hub/oauth_server/service.py").read_text(
        encoding="utf-8"
    )
    report.check(
        'result["client_id_metadata_document_supported"] = true'
        in oauth_service.lower(),
        "OAuth discovery does not advertise enabled CIMD support",
    )
    report.check(
        '"registration_endpoint"' not in oauth_service,
        "OAuth service unexpectedly advertises dynamic client registration",
    )
    report.check(
        remote_mcp.get("environment", {}).get("MY_DATA_HUB_MCP_WRITE_ENABLED") == "false",
        "remote MCP owner/operator writes are not fail-closed",
    )
    report.check(
        remote_mcp.get("ports") == ["127.0.0.1:${MY_DATA_HUB_MCP_PORT:-8765}:8765"],
        "remote MCP upstream must bind loopback only",
    )
    connector_intake = control.get("services", {}).get("connector-intake")
    if connector_intake is not None:
        validate_connector_intake_compose_service(report, connector_intake)

    legacy = (ROOT / "deploy/same-host/install.sh").read_text(encoding="utf-8")
    report.check(
        "INSTALL_MY_DATA_HUB_SAME_HOST" in legacy and "exit 78" in legacy, "legacy same-host token is not hard-disabled"
    )
    for token in ("docker compose", "db migrate", "systemctl", "postgres.env", "pg_dump"):
        report.check(token not in legacy, f"legacy installer remains executable beyond its guard: {token}")
    control_installer = (ROOT / "deploy/control-plane/install.sh").read_text(encoding="utf-8").lower()
    for token in ("database_url", "db migrate", "pg_dump", "backup-loop", "connector-committer"):
        report.check(
            token not in control_installer, f"control installer contains forbidden local-master operation: {token}"
        )
    report.check(
        "install_my_data_hub_provider_mcp" in control_installer,
        "provider-only MCP install action is not explicit",
    )
    report.check(
        "my_data_hub_mcp_scopes: platform:read,provider:read,provider:write"
        in control_installer,
        "provider-only MCP profile does not pin the exact provider OAuth scopes",
    )
    provider_override_start = control_installer.find('cat > "$provider_only_override"')
    provider_override_end = control_installer.find(
        'chmod 600 "$provider_only_override"', provider_override_start
    )
    report.check(
        provider_override_start >= 0 and provider_override_end > provider_override_start,
        "provider-only runtime Compose override is missing",
    )
    if provider_override_start >= 0 and provider_override_end > provider_override_start:
        provider_override = control_installer[provider_override_start:provider_override_end]
        report.check(
            "!override" in provider_override,
            "provider-only Compose does not replace inherited master mounts",
        )
        for token in (
            "my_data_hub_master_asset_dir",
            "my_data_hub_tunnel_broker_socket_dir",
            "my_data_hub_checkpoint_upload_broker_key_file",
            "kaggle_api_token",
            "kaggle_username",
            "kaggle_key",
            "acceptance:operate",
        ):
            report.check(
                token not in provider_override,
                f"provider-only Compose crosses a forbidden authority boundary: {token}",
            )
    report.check(
        "my_data_hub_oauth_chatgpt_cimd_enabled" in control_installer
        and "chatgpt_oauth_client_mode=cimd-public" in control_installer,
        "provider-only install does not enable and disclose bounded ChatGPT CIMD",
    )
    report.check(
        "provider-only readiness did not prove the central adapter gateway" in control_installer,
        "provider-only install lacks its central adapter readiness receipt gate",
    )
    report.check(not (ROOT / "deploy/systemd").exists(), "DB-coupled legacy systemd deployment directory remains")
    report.check(not (ROOT / "compose.same-host.yaml").exists(), "legacy same-host production Compose remains")

    def is_compose_filename(path: Path) -> bool:
        name = path.name.lower()
        return path.suffix.lower() in {".yml", ".yaml"} and (
            name.startswith("compose.") or name.startswith("docker-compose.")
        )

    repository_files = [
        path
        for path in ROOT.rglob("*")
        if path.is_file() and not any(part in {".git", ".venv", "__pycache__"} for part in path.parts)
    ]
    compose_files = {path.relative_to(ROOT).as_posix() for path in repository_files if is_compose_filename(path)}
    report.check(
        compose_files == {"compose.yaml", "compose.control-plane.yaml"},
        f"unclassified Compose deployment profile exists: {sorted(compose_files)}",
    )
    deploy_files = {path.relative_to(ROOT).as_posix() for path in (ROOT / "deploy").rglob("*") if path.is_file()}
    expected_deploy_files = {
        "deploy/control-plane/Dockerfile",
        "deploy/control-plane/collect_deployment_evidence.py",
        "deploy/control-plane/install.sh",
        "deploy/control-plane/install_master_tunnel_broker.sh",
        "deploy/same-host/install.sh",
        "deploy/yandex-edge/autossh.service",
        "deploy/yandex-edge/cloud-init.yaml.tpl",
        "deploy/yandex-edge/create_tunnel_identity.sh",
        "deploy/yandex-edge/edge-nginx.conf",
        "deploy/yandex-edge/fetch-lockbox-key.py",
        "deploy/yandex-edge/provision.sh",
        "deploy/yandex-edge/proxy.conf",
        "deploy/yandex-edge/render_cloud_init.py",
    }
    report.check(
        deploy_files == expected_deploy_files,
        f"unclassified production deployment file exists: {sorted(deploy_files - expected_deploy_files)}",
    )

    disposable = yaml.safe_load((ROOT / "compose.yaml").read_text(encoding="utf-8"))
    report.check(
        disposable.get("x-my-data-hub-profile") == "disposable-integration-test-only",
        "root Compose is not explicitly disposable",
    )
    report.check(not disposable.get("volumes"), "disposable integration Compose must not declare named volumes")
    disposable_services = disposable.get("services", {})
    report.check(
        set(disposable_services) == {"postgres", "api", "orchestrator", "mcp"},
        "disposable Compose service inventory drifted",
    )
    for service_name, service in disposable_services.items():
        report.check("volumes" not in service, f"disposable service {service_name} declares a persistent mount")
        report.check(
            service.get("restart") == "no", f"disposable service {service_name} restart policy is not disabled"
        )
        postgres_markers = json.dumps(
            {
                "name": service_name,
                "image": service.get("image"),
                "build": service.get("build"),
                "command": service.get("command"),
                "entrypoint": service.get("entrypoint"),
            },
            sort_keys=True,
        ).lower()
        if any(marker in postgres_markers for marker in ("postgres", "pgvector", "/var/lib/postgresql")):
            report.check(service_name == "postgres", f"unclassified PostgreSQL-like Compose service: {service_name}")
    postgres = disposable_services.get("postgres", {})
    report.check(postgres.get("restart") == "no", "disposable PostgreSQL restart policy must be disabled")
    report.check("volumes" not in postgres, "disposable PostgreSQL must not declare bind/anonymous volumes")
    report.check(
        postgres.get("tmpfs") == ["/var/lib/postgresql:size=1g,mode=0700,uid=999,gid=999"],
        "disposable PostgreSQL must use exact postgres-owned tmpfs PGDATA parent",
    )
    postgres_image = "pgvector/pgvector:0.8.6-pg18-bookworm"
    report.check(postgres.get("image") == postgres_image, "disposable PostgreSQL image is not pinned")
    makefile = (ROOT / "Makefile").read_text(encoding="utf-8")
    report.check(
        "docker compose down -v --remove-orphans" in makefile, "disposable integration cleanup does not remove volumes"
    )

    workflow_directory = ROOT / ".github/workflows"
    workflows = {
        path.name
        for path in workflow_directory.iterdir()
        if path.is_file() and path.suffix.lower() in {".yml", ".yaml"}
    }
    report.check(
        workflows == {"ci.yml", "nightly.yml", "post-deploy.yml", "provider-real.yml"},
        "workflow inventory drifted or gained an unclassified execution path",
    )
    expected_scheduled_jobs = {
        "nightly.yml": {"deterministic-control-plane"},
        "post-deploy.yml": {"remote-read-only-contract"},
        "provider-real.yml": {"private-notebook-canary"},
    }
    for workflow_name, expected_jobs in expected_scheduled_jobs.items():
        workflow_path = workflow_directory / workflow_name
        workflow_source = workflow_path.read_text(encoding="utf-8")
        workflow = yaml.safe_load(workflow_source)
        report.check(
            set(workflow.get("jobs", {})) == expected_jobs,
            f"{workflow_name} job inventory drifted",
        )
        for job_name, job in workflow.get("jobs", {}).items():
            expected_runner: str | list[str] = (
                PROVIDER_REAL_RUNNER if workflow_name == "provider-real.yml" else "ubuntu-latest"
            )
            report.check(
                job.get("runs-on") == expected_runner,
                f"{workflow_name}:{job_name} does not use its exact bounded runner",
            )
            report.check(
                not job.get("services") and "container" not in job,
                f"{workflow_name}:{job_name} declares an unclassified persistent runtime",
            )
        if workflow_name == "provider-real.yml":
            preflight_path = ROOT / "scripts/provider/devstand_acceptance_controller.py"
            report.check(preflight_path.is_file(), "provider-real devstand OAuth preflight is missing")
            preflight_source = preflight_path.read_text(encoding="utf-8") if preflight_path.is_file() else ""
            validate_provider_real_workflow_auth_boundary(
                report,
                workflow,
                workflow_source,
                preflight_source,
            )
    ci_path = workflow_directory / "ci.yml"
    ci = ci_path.read_text(encoding="utf-8")
    ci_yaml = yaml.safe_load(ci)
    ci_jobs = ci_yaml.get("jobs", {})
    report.check(
        set(ci_jobs) == {"contracts", "postgres-integration"},
        "CI job inventory drifted or gained an unclassified execution job",
    )
    for job_name, job in ci_jobs.items():
        report.check(job.get("runs-on") == "ubuntu-latest", f"CI job {job_name} is not GitHub-hosted disposable")
        report.check("container" not in job, f"CI job {job_name} declares an unclassified persistent container")
        job_services = job.get("services", {})
        if job_name == "postgres-integration":
            report.check(set(job_services) == {"postgres"}, "PostgreSQL CI service inventory drifted")
        else:
            report.check(not job_services, f"non-integration CI job {job_name} declares services")
    postgres_job = ci_jobs.get("postgres-integration", {})
    postgres_service = postgres_job.get("services", {}).get("postgres", {})
    report.check(
        postgres_job.get("runs-on") == "ubuntu-latest", "PostgreSQL integration must remain GitHub-hosted disposable CI"
    )
    report.check(postgres_service.get("image") == postgres_image, "CI PostgreSQL image differs from integration target")
    report.check(
        "volumes" not in postgres_service and "docker volume create" not in ci,
        "CI PostgreSQL declares persistent volume state",
    )
    for path in repository_files:
        if path.suffix.lower() not in {".yml", ".yaml"}:
            continue
        try:
            document = yaml.safe_load(path.read_text(encoding="utf-8"))
        except yaml.YAMLError:
            continue
        if not isinstance(document, dict) or not isinstance(document.get("services"), dict):
            continue
        relative = path.relative_to(ROOT).as_posix()
        report.check(
            relative in {"compose.yaml", "compose.control-plane.yaml"},
            f"unclassified YAML service/deployment document exists: {relative}",
        )
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
    for path in repository_files:
        relative = path.relative_to(ROOT)
        if (
            path.suffix in {".sh", ".service", ".timer"}
            or path.name == "Makefile"
            or path.name.startswith("Dockerfile")
            or relative.parts[:2] == (".github", "workflows")
            or is_compose_filename(path)
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
    for path in repository_files:
        if path.suffix != ".py" or not bool(path.stat().st_mode & 0o111):
            continue
        relative = path.relative_to(ROOT).as_posix()
        try:
            findings = find_dangerous_python_process_calls(path.read_text(encoding="utf-8"))
        except SyntaxError as exc:
            report.fail(f"cannot parse executable Python deployment surface {relative}: {exc}")
            continue
        report.check(
            not findings,
            f"executable Python local-master process call in {relative}: {findings}",
        )

    pipeline = load_json(ROOT / "config/pipelines/region-talk.v1.json")
    report.check(pipeline.get("status") == "paused", "Region Talk pipeline is not paused")
    publication = next(
        (stage for stage in pipeline.get("stages", []) if stage.get("key") == "publication_dispatch"), {}
    )
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
    report.check(
        receipt.get("my_data_hub_container_count") == 0, "host receipt reports a deployed my-data-hub container"
    )
    report.check(receipt.get("local_postgresql_process_observed") is False, "host receipt reports local PostgreSQL")
    report.check(receipt.get("legacy_user_unit", {}).get("enabled") is False, "legacy same-host unit was enabled")
    volume = receipt.get("postgres_volume", {})
    report.check(
        volume.get("exists") is True and volume.get("pgdata_initialized") is False,
        "host residue is not disclosed accurately",
    )
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
    validate_kaggle_transport(report)
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
