#!/usr/bin/env python3
"""Generate deterministic, fail-closed notebook worker entrypoints."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import nbformat

ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK_ROOT = ROOT / "notebooks"
TEMPLATE_ROOT = NOTEBOOK_ROOT / "templates"

NOTEBOOK_README = """# Notebook workers

Every directory is one isolated worker/lane. E5, BGE-M3, source-profile, writer and image
diagnostics are deliberately separate so model memory, dependency and failure domains do
not collapse into one kernel.

## Runtime contract

A worker receives `MY_DATA_HUB_NOTEBOOK_INPUT_MANIFEST` and writes
`MY_DATA_HUB_NOTEBOOK_RESULT_PATH`. It may read immutable artifacts and perform computation,
but it may not connect to canonical PostgreSQL, mutate YDB, publish to Telegram/VK or
advance a queue cursor. The local reconciler validates and commits results.

`01` through `06` are the operational MVP notebooks generated from reviewed Python templates.
They remain marked `production_ready=false` until an exact real-provider receipt proves the
source, input versions, privacy and terminal output. `00-platform-smoke` and
`80-region-talk-migration-reconciliation` have implemented legacy typed-worker adapters.
Other Region Talk notebooks contain complete contract, accounting, error and atomic-output
plumbing; their `process_item()` adapters intentionally fail with
`PROCESSOR_ADAPTER_NOT_PORTED` until code is adapted from an exact donor revision and covered
by golden fixtures. A placeholder notebook therefore cannot be mistaken for a working
production stage.

## Activation gate

For each Region Talk worker, replace only `process_item()` and pin model/code revisions.
Record source and destination hashes in an adaptation manifest, then prove behavioural
equivalence on fixtures and shadow data before enabling that stage.
"""


@dataclass(frozen=True, slots=True)
class NotebookSpec:
    directory: str
    title: str
    purpose: str
    contracts: dict[str, str]
    model: dict[str, str]
    adapter: Literal["smoke", "migration_reconciliation", "pending"] = "pending"


@dataclass(frozen=True, slots=True)
class OperationalNotebookSpec:
    """One deterministic, private operational notebook built from Python source.

    The Python module is the primary source.  The generated ipynb only installs
    an exact task wheel from a private Kaggle input and invokes ``main()``.  This
    keeps reviewable code out of notebook JSON and makes drift mechanically
    detectable.
    """

    directory: str
    title: str
    purpose: str
    template: str
    runtime_contract: str
    resource_class: Literal["orchestrator_protected"] = "orchestrator_protected"
    enable_internet: bool = False
    model_id: str | None = None
    model_revision: str | None = None
    timeout_seconds: int = 1_800
    canonical_write_allowed: bool = False
    external_side_effects_allowed: bool = True


OPERATIONAL_SPECS: tuple[OperationalNotebookSpec, ...] = (
    OperationalNotebookSpec(
        "01-platform-runtime-smoke",
        "01 Platform runtime smoke",
        "Exercise callback retry, heartbeat, local JSONL replay and a bound terminal receipt.",
        "runtime_smoke/runtime.py",
        "my-data-hub-platform-runtime-smoke.v1",
        timeout_seconds=600,
    ),
    OperationalNotebookSpec(
        "02-postgres-master",
        "02 PostgreSQL 18 master",
        "Run the single epoch-fenced PostgreSQL primary inside /kaggle/working.",
        "postgres_master/runtime.py",
        "my-data-hub-postgres-master.v1",
        # Leave the provider hard-cutoff reserve declared in runtime_sdk.lifetime.
        timeout_seconds=42_300,
        canonical_write_allowed=True,
    ),
    OperationalNotebookSpec(
        "03-checkpoint-verifier-restore-smoke",
        "03 Checkpoint verifier restore smoke",
        "Independently verify and restore an exact private checkpoint candidate.",
        "checkpoint_verifier/runtime.py",
        "my-data-hub-checkpoint-restore-smoke.v1",
        timeout_seconds=3_600,
    ),
    OperationalNotebookSpec(
        "04-region-talk-ydb-bloggers-importer",
        "04 Region Talk YDB bloggers importer",
        "Stream the exact read-only YDB snapshot directly into the ACTIVE master.",
        "blogger_importer/runtime.py",
        "region-talk-ydb-bloggers-import.v1",
        timeout_seconds=3_600,
        canonical_write_allowed=True,
    ),
    OperationalNotebookSpec(
        "05-e5-blogger-embedding-worker",
        "05 E5 blogger embedding worker",
        "Encode one exact job artifact in the isolated 768-dimensional E5 space.",
        "embedding_workers/e5_runtime.py",
        "my-data-hub-blogger-embedding-artifact.v1",
        enable_internet=True,
        model_id="intfloat/multilingual-e5-base",
        model_revision="d128750597153bb5987e10b1c3493a34e5a4502a",
        timeout_seconds=7_200,
    ),
    OperationalNotebookSpec(
        "06-bge-m3-blogger-embedding-worker",
        "06 BGE-M3 blogger embedding worker",
        "Encode one exact job artifact in the isolated 1024-dimensional BGE-M3 space.",
        "embedding_workers/bge_m3_runtime.py",
        "my-data-hub-blogger-embedding-artifact.v1",
        enable_internet=True,
        model_id="BAAI/bge-m3",
        model_revision="5617a9f61b028005a4858fdac845db406aefb181",
        timeout_seconds=10_800,
    ),
)


SPECS: tuple[NotebookSpec, ...] = (
    NotebookSpec(
        "00-platform-smoke",
        "00 Platform smoke",
        "Validate package import, input contract, per-item accounting and immutable output.",
        {"platform_smoke": "my-data-hub.platform-smoke.v1"},
        {
            "provider": "none",
            "name": "contract-smoke",
            "version": "v1",
            "task": "validation",
        },
        "smoke",
    ),
    NotebookSpec(
        "10-region-talk-candidate-report",
        "10 Region Talk candidate report",
        "Adapter shell for source and post discovery from the CandidateReport donor.",
        {
            "source_discovery": "region-talk.source-discovery.v1",
            "post_discovery": "region-talk.post-discovery.v1",
        },
        {
            "provider": "region-talk",
            "name": "candidate-report",
            "version": "adapter-pending",
            "task": "discovery",
        },
    ),
    NotebookSpec(
        "20-region-talk-e5-enrichment",
        "20 Region Talk E5 enrichment",
        "Adapter shell for 768-dimensional multilingual E5 evidence.",
        {"e5_embedding": "e5_semantic_bank_scores_v1"},
        {
            "provider": "intfloat",
            "name": "multilingual-e5-base",
            "version": "pin-at-deployment",
            "task": "embedding",
        },
    ),
    NotebookSpec(
        "30-region-talk-bge-m3-enrichment",
        "30 Region Talk BGE-M3 enrichment",
        (
            "Adapter shell for isolated BGE-M3 evidence; it never shares a production "
            "kernel with E5."
        ),
        {"bge_m3_embedding": "bge_m3_flagembedding_dense_v1"},
        {
            "provider": "BAAI",
            "name": "bge-m3",
            "version": "pin-at-deployment",
            "task": "embedding",
        },
    ),
    NotebookSpec(
        "40-region-talk-image-diagnostic",
        "40 Region Talk image diagnostic",
        "Adapter shell for ordered-media diagnostics and explicit terminal evidence.",
        {"image_scoring": "region-talk.image-diagnostic.v1"},
        {
            "provider": "region-talk",
            "name": "image-diagnostic",
            "version": "adapter-pending",
            "task": "image-analysis",
        },
    ),
    NotebookSpec(
        "50-region-talk-final-verifier",
        "50 Region Talk final verifier",
        "Adapter shell for the single versioned final eligibility verifier.",
        {"final_verifier": "region-talk.final-verifier.v1"},
        {
            "provider": "region-talk",
            "name": "final-verifier",
            "version": "adapter-pending",
            "task": "verification",
        },
    ),
    NotebookSpec(
        "60-region-talk-source-profile",
        "60 Region Talk source profile",
        "Adapter shell for versioned source-profile capture and evidence classification.",
        {"source_profile": "region-talk.source-profile.v1"},
        {
            "provider": "region-talk",
            "name": "source-profile",
            "version": "adapter-pending",
            "task": "source-profile",
        },
    ),
    NotebookSpec(
        "70-region-talk-writer",
        "70 Region Talk writer",
        "Adapter shell for versioned review copy; it never approves or publishes.",
        {"writer": "region-talk.writer.v1"},
        {
            "provider": "region-talk",
            "name": "writer",
            "version": "adapter-pending",
            "task": "editorial-draft",
        },
    ),
    NotebookSpec(
        "80-region-talk-migration-reconciliation",
        "80 Region Talk migration reconciliation",
        "Pure contract worker for row-kind accounting; it never reads YDB or writes PostgreSQL.",
        {"migration_reconciliation": "region-talk.migration-reconciliation.v1"},
        {
            "provider": "none",
            "name": "migration-reconciliation",
            "version": "v1",
            "task": "migration-audit",
        },
        "migration_reconciliation",
    ),
)


def _cell(cell_type: str, source: str, cell_id: str):  # type: ignore[no-untyped-def]
    cell = (
        nbformat.v4.new_markdown_cell(source)
        if cell_type == "markdown"
        else nbformat.v4.new_code_cell(source)
    )
    cell["id"] = cell_id
    return cell


def _adapter_source(spec: NotebookSpec) -> str:
    if spec.adapter == "smoke":
        return '''def process_item(work_item: dict) -> dict:
    payload = work_item.get("payload", {})
    return {
        "contract_smoke": True,
        "subject_type": work_item["subject_type"],
        "subject_id": work_item["subject_id"],
        "payload_keys": sorted(payload),
    }'''
    if spec.adapter == "migration_reconciliation":
        return '''from my_data_hub.workloads.region_talk.migration import (
    build_reconciliation_accounting,
)


def process_item(work_item: dict) -> dict:
    payload = work_item.get("payload", {})
    expected = payload.get("expected_by_kind")
    actual = payload.get("actual_rows")
    if not isinstance(expected, dict) or not isinstance(actual, list):
        raise ValueError("payload requires expected_by_kind object and actual_rows list")
    pairs = []
    for item in actual:
        if not isinstance(item, dict) or "row_kind" not in item:
            raise ValueError("each actual_rows item requires row_kind")
        pairs.append((str(item["row_kind"]), item.get("disposition")))
    return {
        "accounting": build_reconciliation_accounting(
            expected_by_kind=expected,
            actual_rows=pairs,
        )
    }'''
    return '''def process_item(_work_item: dict) -> dict:
    raise NotImplementedError(
        "stage adapter has not been ported and shadow-validated from the Region Talk donor"
    )'''


def build_notebook(spec: NotebookSpec):  # type: ignore[no-untyped-def]
    nb = nbformat.v4.new_notebook()
    nb.nbformat = 4
    nb.nbformat_minor = 5
    nb.metadata = {
        "kernelspec": {
            "display_name": "Python 3",
            "language": "python",
            "name": "python3",
        },
        "language_info": {"name": "python", "version": "3.12"},
        "my_data_hub": {
            "contracts": spec.contracts,
            "canonical_write_allowed": False,
            "external_side_effects_allowed": False,
            "output_contract": "my-data-hub-notebook-result.v1",
            "adapter_status": "implemented" if spec.adapter != "pending" else "pending",
        },
    }
    nb.cells = [
        _cell(
            "markdown",
            f"# {spec.title}\n\n{spec.purpose}\n\n"
            "This notebook is a **typed producer**, not a database client. It reads one exact "
            "input manifest and writes one immutable result envelope. It must never mutate "
            "canonical PostgreSQL, YDB, a shared SQLite file or a publication target.\n\n"
            "Adapter-pending stages fail closed per item until donor code, model revisions and "
            "shadow equivalence have been recorded.",
            "intro",
        ),
        _cell(
            "code",
            "from __future__ import annotations\n\n"
            "import json\n"
            "import os\n"
            "import tempfile\n"
            "from pathlib import Path\n\n"
            "from my_data_hub.notebooks.runtime import (\n"
            "    NotebookResultBuilder,\n"
            "    manifest_path_from_env,\n"
            ")\n\n"
            f"STAGE_CONTRACTS = {spec.contracts!r}\n"
            f"MODEL = {spec.model!r}",
            "imports",
        ),
        _cell(
            "code",
            "builder = NotebookResultBuilder(\n"
            "    manifest_path=manifest_path_from_env(),\n"
            "    code_revision=os.environ.get(\"MY_DATA_HUB_CODE_REVISION\", \"UNPINNED\"),\n"
            "    runtime_name=os.environ.get(\"KAGGLE_KERNEL_RUN_TYPE\", \"local-notebook\"),\n"
            ")\n"
            "manifest = builder.manifest\n"
            "expected_contract = STAGE_CONTRACTS.get(manifest.stage)\n"
            "if expected_contract is None:\n"
            "    raise RuntimeError(f\"unsupported stage for this notebook: {manifest.stage}\")\n"
            "if manifest.stage_contract_version != expected_contract:\n"
            "    raise RuntimeError(\n"
            "        f\"stage contract mismatch: {manifest.stage_contract_version} != {expected_contract}\"\n"
            "    )\n"
            "print({\"run_id\": str(manifest.run_id), \"stage\": manifest.stage, "
            "\"items\": len(manifest.work_items)})",
            "load-manifest",
        ),
        _cell(
            "markdown",
            "## Stage adapter\n\nReplace only the adapter implementation after focused tests. Keep "
            "manifest validation, per-item accounting, fingerprints and result emission unchanged.",
            "adapter-doc",
        ),
        _cell("code", _adapter_source(spec), "adapter"),
        _cell(
            "code",
            "for item in manifest.work_items:\n"
            "    work_item = item.model_dump(mode=\"json\")\n"
            "    try:\n"
            "        result = process_item(work_item)\n"
            "    except NotImplementedError as exc:\n"
            "        builder.add_failure(\n"
            "            work_item_id=item.work_item_id,\n"
            "            code=\"PROCESSOR_ADAPTER_NOT_PORTED\",\n"
            "            message=str(exc),\n"
            "            retryable=False,\n"
            "        )\n"
            "    except Exception as exc:\n"
            "        builder.add_failure(\n"
            "            work_item_id=item.work_item_id,\n"
            "            code=\"PROCESSOR_FAILURE\",\n"
            "            message=str(exc),\n"
            "            retryable=True,\n"
            "            details={\"exception_type\": type(exc).__name__},\n"
            "        )\n"
            "    else:\n"
            "        builder.add_success(\n"
            "            work_item_id=item.work_item_id,\n"
            "            input_fingerprint=item.input_fingerprint,\n"
            "            result=result,\n"
            "        )",
            "process-items",
        ),
        _cell(
            "code",
            "result = builder.build(MODEL)\n"
            "output_path = Path(\n"
            "    os.environ.get(\n"
            "        \"MY_DATA_HUB_NOTEBOOK_RESULT_PATH\",\n"
            "        \"/kaggle/working/result.json\",\n"
            "    )\n"
            ")\n"
            "output_path.parent.mkdir(parents=True, exist_ok=True)\n"
            "payload = json.dumps(\n"
            "    result, ensure_ascii=False, sort_keys=True, separators=(\",\", \":\")\n"
            ").encode(\"utf-8\")\n"
            "descriptor, temporary_name = tempfile.mkstemp(\n"
            "    prefix=\".result.\", suffix=\".tmp\", dir=output_path.parent\n"
            ")\n"
            "try:\n"
            "    with os.fdopen(descriptor, \"wb\") as handle:\n"
            "        handle.write(payload)\n"
            "        handle.flush()\n"
            "        os.fsync(handle.fileno())\n"
            "    os.replace(temporary_name, output_path)\n"
            "finally:\n"
            "    if os.path.exists(temporary_name):\n"
            "        os.unlink(temporary_name)\n"
            "print({\"status\": result[\"status\"], \"result\": str(output_path), "
            "\"failures\": len(result[\"failures\"])})",
            "write-result",
        ),
    ]
    nbformat.validate(nb)
    return nb


def serialize_notebook(notebook) -> str:  # type: ignore[no-untyped-def]
    return json.dumps(notebook, ensure_ascii=False, indent=1, sort_keys=True) + "\n"


def kernel_metadata(spec: NotebookSpec) -> str:
    payload = {
        "id": f"OWNER/{spec.directory}",
        "title": f"my-data-hub {spec.title}",
        "code_file": "worker.ipynb",
        "language": "python",
        "kernel_type": "notebook",
        "is_private": True,
        "enable_gpu": False,
        "enable_internet": False,
        "dataset_sources": [],
        "competition_sources": [],
        "kernel_sources": [],
        "my_data_hub": {
            "contracts": spec.contracts,
            "adapter_status": "implemented" if spec.adapter != "pending" else "pending",
            "production_ready": False,
        },
    }
    return json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"


def _operational_source(spec: OperationalNotebookSpec) -> str:
    path = TEMPLATE_ROOT / spec.template
    source = path.read_text(encoding="utf-8")
    if "def main(" not in source:
        raise ValueError(f"operational template has no main(): {spec.template}")
    return source


def build_operational_notebook(spec: OperationalNotebookSpec):  # type: ignore[no-untyped-def]
    source = _operational_source(spec)
    source_sha256 = __import__("hashlib").sha256(source.encode()).hexdigest()
    nb = nbformat.v4.new_notebook()
    nb.nbformat = 4
    nb.nbformat_minor = 5
    nb.metadata = {
        "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
        "language_info": {"name": "python", "version": "3.12"},
        "my_data_hub": {
            "contracts": {},
            "runtime_contract": spec.runtime_contract,
            "primary_source": f"notebooks/templates/{spec.template}",
            "primary_source_sha256": source_sha256,
            "privacy": "private",
            "resource_class": spec.resource_class,
            "canonical_database_location": "kaggle-notebook-only",
            "embedded_secrets": False,
            "canonical_write_allowed": spec.canonical_write_allowed,
            "external_side_effects_allowed": spec.external_side_effects_allowed,
            "timeout_seconds": spec.timeout_seconds,
            "model_id": spec.model_id,
            "model_revision": spec.model_revision,
        },
    }
    nb.cells = [
        _cell(
            "markdown",
            f"# {spec.title}\n\n{spec.purpose}\n\n"
            "This private `orchestrator_protected` notebook is generated from a reviewed Python "
            "template. It receives exact input versions and secrets through Kaggle runtime inputs; "
            "no credential is embedded in this notebook or written to its output.",
            "intro",
        ),
        _cell(
            "code",
            "from __future__ import annotations\n\n"
            "import hashlib\n"
            "import os\n"
            "import subprocess\n"
            "import sys\n"
            "from pathlib import Path\n\n"
            f"EXPECTED_SOURCE_SHA256 = {source_sha256!r}\n"
            f"RUNTIME_CONTRACT = {spec.runtime_contract!r}\n"
            "wheel = Path(os.environ.get('MY_DATA_HUB_WHEEL_PATH', ''))\n"
            "if not wheel.is_file() or wheel.suffix != '.whl':\n"
            "    raise RuntimeError('exact private my-data-hub wheel input is required')\n"
            "expected_wheel_sha = os.environ.get('MY_DATA_HUB_WHEEL_SHA256', '')\n"
            "if (len(expected_wheel_sha) != 64 or \n"
            "        hashlib.sha256(wheel.read_bytes()).hexdigest() != expected_wheel_sha):\n"
            "    raise RuntimeError('my-data-hub wheel hash mismatch')\n"
            "subprocess.run(\n"
            "    [sys.executable, '-m', 'pip', 'install', '--no-deps', "
            "'--disable-pip-version-check', str(wheel)],\n"
            "    check=True,\n"
            ")",
            "install-exact-wheel",
        ),
        _cell(
            "code",
            f"PRIMARY_SOURCE = {source!r}\n"
            "if hashlib.sha256(PRIMARY_SOURCE.encode()).hexdigest() != EXPECTED_SOURCE_SHA256:\n"
            "    raise RuntimeError('embedded primary source hash mismatch')\n"
            "exec(compile(PRIMARY_SOURCE, '<my-data-hub-primary-source>', 'exec'), globals())",
            "primary-source",
        ),
        _cell("code", "raise SystemExit(globals()['main']())", "run"),
    ]
    nbformat.validate(nb)
    return nb


def operational_kernel_metadata(spec: OperationalNotebookSpec) -> str:
    source = _operational_source(spec)
    payload = {
        "id": f"OWNER/{spec.directory}",
        # The real provider adapter replaces OWNER and requires title == slug,
        # which prevents Kaggle from silently rewriting the exact resource ref.
        "title": spec.directory,
        "code_file": "worker.ipynb",
        "language": "python",
        "kernel_type": "notebook",
        "is_private": True,
        "enable_gpu": False,
        "enable_tpu": False,
        "enable_internet": spec.enable_internet,
        "dataset_sources": [],
        "competition_sources": [],
        "kernel_sources": [],
        "model_sources": [],
        "my_data_hub": {
            "contracts": {},
            "runtime_contract": spec.runtime_contract,
            "primary_source": spec.template,
            "primary_source_sha256": __import__("hashlib").sha256(source.encode()).hexdigest(),
            "resource_class": spec.resource_class,
            "privacy": "private",
            "timeout_seconds": spec.timeout_seconds,
            "model_id": spec.model_id,
            "model_revision": spec.model_revision,
            "production_ready": False,
            "activation_requires_real_receipt": True,
            "canonical_write_allowed": spec.canonical_write_allowed,
            "external_side_effects_allowed": spec.external_side_effects_allowed,
        },
    }
    return json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"


def expected_files() -> dict[Path, str]:
    files = {NOTEBOOK_ROOT / "README.md": NOTEBOOK_README}
    for spec in OPERATIONAL_SPECS:
        directory = NOTEBOOK_ROOT / spec.directory
        files[directory / "worker.ipynb"] = serialize_notebook(build_operational_notebook(spec))
        files[directory / "kernel-metadata.example.json"] = operational_kernel_metadata(spec)
    for spec in SPECS:
        directory = NOTEBOOK_ROOT / spec.directory
        files[directory / "worker.ipynb"] = serialize_notebook(build_notebook(spec))
        files[directory / "kernel-metadata.example.json"] = kernel_metadata(spec)
    return files


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true", help="fail if generated files drift")
    args = parser.parse_args()

    drift: list[str] = []
    written: list[str] = []
    for path, expected in expected_files().items():
        relative = str(path.relative_to(ROOT))
        if args.check:
            if not path.is_file() or path.read_text(encoding="utf-8") != expected:
                drift.append(relative)
            continue
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(expected, encoding="utf-8")
        written.append(relative)

    report = {"mode": "check" if args.check else "write", "drift": drift, "written": written}
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    return 1 if drift else 0


if __name__ == "__main__":
    sys.exit(main())
