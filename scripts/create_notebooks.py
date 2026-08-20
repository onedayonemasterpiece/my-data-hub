#!/usr/bin/env python3
"""Generate deterministic, fail-closed notebook worker entrypoints."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import nbformat

ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK_ROOT = ROOT / "notebooks"
TEMPLATE_ROOT = NOTEBOOK_ROOT / "templates"
EMBEDDING_ASSET_ROOT = ROOT / "src" / "my_data_hub" / "embeddings" / "assets"

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
The queue-formation E5, BGE-M3, image, final-verifier and writer notebooks validate the exact
typed work-request contract and return `HEAVY_RUNTIME_NOT_ATTACHED` until their verified model
runtime is present. Discovery and source-profile adapters remain explicitly unported. Neither
state can be mistaken for successful heavyweight evidence.

Every operational notebook also fails before installing or executing code unless a hashed
`my-data-hub-notebook-execution-pins/v1` manifest binds the exact CPython patch version,
immutable Kaggle image digest, numeric private Dataset versions, task wheel and embedded source
hashes, output contract, model revision (when applicable), resource class, and cleanup/retention
policy. Launch-time values are used because Dataset versions and the Kaggle image are provider
observations, not values this repository may invent. The checked-in metadata declares the full
contract and remains private and `production_ready=false` until the control plane supplies and
attests those exact values.

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
    adapter: Literal[
        "smoke", "migration_reconciliation", "stage_contract", "pending"
    ] = "pending"


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


OPERATIONAL_CLEANUP_RETENTION_POLICY = {
    "cleanup_receipt_required": True,
    "notebook_resource": "orchestrator_protected_until_owner_supersedes",
    "run_outputs": "retain_until_terminal_receipt_then_control_policy",
    "task_owned_inputs": "claim_bound_delete_after_terminal_or_expiry",
}
EXECUTION_PINS_SCHEMA = "my-data-hub-notebook-execution-pins/v1"
SUPPORTED_PYTHON_SERIES = "3.12"


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
            "version": "d128750597153bb5987e10b1c3493a34e5a4502a",
            "task": "embedding",
        },
        "stage_contract",
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
            "version": "5617a9f61b028005a4858fdac845db406aefb181",
            "task": "embedding",
        },
        "stage_contract",
    ),
    NotebookSpec(
        "35-region-talk-vector-fusion",
        "35 Region Talk vector fusion",
        "Execute deterministic fusion of exact-current E5 and BGE-M3 evidence.",
        {"vector_fusion": "region-talk.vector-fusion.v1"},
        {
            "provider": "my-data-hub",
            "name": "region-talk-vector-fusion",
            "version": "region-talk.vector-fusion.v1",
            "task": "vector-fusion",
        },
        "stage_contract",
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
        "stage_contract",
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
        "stage_contract",
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
        "stage_contract",
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
    if spec.adapter == "stage_contract":
        return '''from my_data_hub.workloads.region_talk.notebook_stages import (
    attached_stage_runtime_from_env,
    process_region_talk_stage_item,
)


attached_runtime = attached_stage_runtime_from_env(manifest.stage)


def process_item(work_item: dict) -> dict:
    return process_region_talk_stage_item(
        work_item,
        stage=manifest.stage,
        contract_version=manifest.stage_contract_version,
        runtime=attached_runtime,
    )'''
    return '''def process_item(_work_item: dict) -> dict:
    raise NotImplementedError(
        "stage adapter has not been ported and shadow-validated from the Region Talk donor"
    )'''


def build_notebook(spec: NotebookSpec):  # type: ignore[no-untyped-def]
    stage_runtime_note = (
        "Heavy stages validate exact work requests and fail closed until their model runtime "
        "and shadow equivalence have been recorded."
        if spec.adapter == "stage_contract"
        else "Adapter-pending stages fail closed per item until donor code, model revisions and "
        "shadow equivalence have been recorded."
    )
    unavailable_handler = (
        ""
        if spec.adapter == "stage_contract"
        else "    except NotImplementedError as exc:\n"
        "        builder.add_failure(\n"
        "            work_item_id=item.work_item_id,\n"
        "            code=\"PROCESSOR_ADAPTER_NOT_PORTED\",\n"
        "            message=str(exc),\n"
        "            retryable=False,\n"
        "        )\n"
    )
    processor_error_code = (
        'getattr(exc, "code", "PROCESSOR_FAILURE")'
        if spec.adapter == "stage_contract"
        else '"PROCESSOR_FAILURE"'
    )
    processor_retryable = (
        'getattr(exc, "retryable", True)'
        if spec.adapter == "stage_contract"
        else "True"
    )
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
            "adapter_status": (
                "contract_ready"
                if spec.adapter == "stage_contract"
                else "pending"
                if spec.adapter == "pending"
                else "implemented"
            ),
        },
    }
    nb.cells = [
        _cell(
            "markdown",
            f"# {spec.title}\n\n{spec.purpose}\n\n"
            "This notebook is a **typed producer**, not a database client. It reads one exact "
            "input manifest and writes one immutable result envelope. It must never mutate "
            "canonical PostgreSQL, YDB, a shared SQLite file or a publication target.\n\n"
            f"{stage_runtime_note}",
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
            f"{unavailable_handler}"
            "    except Exception as exc:\n"
            "        builder.add_failure(\n"
            "            work_item_id=item.work_item_id,\n"
            f"            code={processor_error_code},\n"
            "            message=str(exc),\n"
            f"            retryable={processor_retryable},\n"
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
            "adapter_status": (
                "contract_ready"
                if spec.adapter == "stage_contract"
                else "pending"
                if spec.adapter == "pending"
                else "implemented"
            ),
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
    source_sha256 = hashlib.sha256(source.encode()).hexdigest()
    embedding_dependency_assets = (
        ["embedding_dependency_manifest_sha256", "embedding_dependency_smoke_receipt_sha256"]
        if spec.model_id is not None
        else []
    )
    pin_contract = {
        "schema": EXECUTION_PINS_SCHEMA,
        "notebook": spec.directory,
        "supported_python_series": SUPPORTED_PYTHON_SERIES,
        "kaggle_runtime_image_identity": "required-immutable-sha256-at-launch",
        "input_dataset_versions": "required-exact-numeric-private-refs-at-launch",
        "immutable_assets": [
            "my_data_hub_wheel_sha256", "primary_source_sha256", *embedding_dependency_assets
        ],
        "output_contract": spec.runtime_contract,
        "model": (
            {"id": spec.model_id, "revision": spec.model_revision}
            if spec.model_id is not None
            else None
        ),
        "privacy": "private",
        "resource_class": spec.resource_class,
        "cleanup_retention_policy": OPERATIONAL_CLEANUP_RETENTION_POLICY,
    }
    dependency_hash_bootstrap = ""
    dependency_bootstrap = ""
    dependency_import = ""
    project_install_index_option = ""
    if spec.model_id is not None:
        project_install_index_option = "'--no-index', "
        dependency_import = "import importlib.metadata\n"
        dependency_hash_bootstrap = (
            "expected_dependency_sha = os.environ.get(\n"
            "    'MY_DATA_HUB_EMBEDDING_DEPENDENCY_MANIFEST_SHA256', ''\n"
            ")\n"
            "expected_smoke_sha = os.environ.get(\n"
            "    'MY_DATA_HUB_EMBEDDING_DEPENDENCY_SMOKE_RECEIPT_SHA256', ''\n"
            ")\n"
            "if (not re.fullmatch(r'[a-f0-9]{64}', expected_dependency_sha) or\n"
            "        not re.fullmatch(r'[a-f0-9]{64}', expected_smoke_sha)):\n"
            "    raise RuntimeError('embedding dependency hashes are required')\n"
            "expected_assets.update({\n"
            "    'embedding_dependency_manifest_sha256': expected_dependency_sha,\n"
            "    'embedding_dependency_smoke_receipt_sha256': expected_smoke_sha,\n"
            "})\n"
        )
        dependency_bootstrap = (
            "dependency_manifest_path = Path(os.environ.get(\n"
            "    'MY_DATA_HUB_EMBEDDING_DEPENDENCY_MANIFEST_PATH',\n"
            "    str(wheel.parent / 'embedding-worker-dependencies.json'),\n"
            "))\n"
            "wheelhouse_path = Path(os.environ.get(\n"
            "    'MY_DATA_HUB_EMBEDDING_WHEELHOUSE_PATH',\n"
            "    str(wheel.parent / 'embedding-worker-wheelhouse'),\n"
            "))\n"
            "smoke_receipt_path = Path(os.environ.get(\n"
            "    'MY_DATA_HUB_EMBEDDING_DEPENDENCY_SMOKE_RECEIPT_PATH', ''\n"
            "))\n"
            "expected_dependency_sha = pins['immutable_asset_sha256s'].get(\n"
            "    'embedding_dependency_manifest_sha256', ''\n"
            ")\n"
            "expected_smoke_sha = pins['immutable_asset_sha256s'].get(\n"
            "    'embedding_dependency_smoke_receipt_sha256', ''\n"
            ")\n"
            "if (not dependency_manifest_path.is_file() or dependency_manifest_path.is_symlink() or\n"
            "        not wheelhouse_path.is_dir() or wheelhouse_path.is_symlink() or\n"
            "        not smoke_receipt_path.is_file() or smoke_receipt_path.is_symlink() or\n"
            "        not re.fullmatch(r'[a-f0-9]{64}', expected_dependency_sha) or\n"
            "        not re.fullmatch(r'[a-f0-9]{64}', expected_smoke_sha)):\n"
            "    raise RuntimeError('verified offline embedding dependency inputs are required')\n"
            "dependency_body = dependency_manifest_path.read_bytes()\n"
            "smoke_body = smoke_receipt_path.read_bytes()\n"
            "if hashlib.sha256(dependency_body).hexdigest() != expected_dependency_sha:\n"
            "    raise RuntimeError('embedding dependency manifest hash mismatch')\n"
            "if hashlib.sha256(smoke_body).hexdigest() != expected_smoke_sha:\n"
            "    raise RuntimeError('embedding dependency smoke receipt hash mismatch')\n"
            "dependencies = json.loads(dependency_body)\n"
            "smoke = json.loads(smoke_body)\n"
            "if dependency_body != json.dumps(\n"
            "        dependencies, sort_keys=True, separators=(',', ':'), ensure_ascii=False\n"
            "    ).encode():\n"
            "    raise RuntimeError('embedding dependency manifest is not canonical JSON')\n"
            "if smoke_body != json.dumps(\n"
            "        smoke, sort_keys=True, separators=(',', ':'), ensure_ascii=False\n"
            "    ).encode():\n"
            "    raise RuntimeError('embedding dependency smoke receipt is not canonical JSON')\n"
            "dependency_keys = {\n"
            "    'schema_version', 'source_lock_sha256', 'index_url', 'runtime',\n"
            "    'install_order', 'required_image_distributions', 'wheels',\n"
            "    'smoke_requirement',\n"
            "}\n"
            "if (not isinstance(dependencies, dict) or set(dependencies) != dependency_keys or\n"
            "        dependencies['schema_version'] !=\n"
            "        'my-data-hub-embedding-worker-dependencies.v1' or\n"
            "        dependencies['runtime'].get('image_identity') != image_identity or\n"
            "        dependencies['runtime'].get('source_commit') != source_commit or\n"
            "        dependencies['runtime'].get('python_abi') != 'cp312' or\n"
            "        dependencies['runtime'].get('platform') != 'manylinux2014_x86_64'):\n"
            "    raise RuntimeError('embedding dependency manifest runtime differs')\n"
            "wheels = dependencies['wheels']\n"
            "required_image_distributions = dependencies['required_image_distributions']\n"
            "smoke_requirement = dependencies['smoke_requirement']\n"
            "if (not isinstance(wheels, list) or not wheels or\n"
            "        not isinstance(required_image_distributions, list) or\n"
            "        not required_image_distributions or\n"
            "        len(required_image_distributions) != len(set(required_image_distributions)) or\n"
            "        not isinstance(smoke_requirement, dict) or\n"
            "        smoke_requirement.get('schema_version') !=\n"
            "        'my-data-hub-embedding-dependency-smoke-receipt.v1' or\n"
            "        smoke_requirement.get('observation_schema_version') !=\n"
            "        'my-data-hub-embedding-dependency-smoke-observation.v1' or\n"
            "        smoke_requirement.get('receipt_source') !=\n"
            "        'central-provider-exact-private-kaggle-run' or\n"
            "        smoke_requirement.get('worker_admission') !=\n"
            "        'deny-without-verified-receipt' or\n"
            "        smoke_requirement.get('required') is not True or\n"
            "        dependencies['install_order'] != [item.get('filename') for item in wheels]):\n"
            "    raise RuntimeError('embedding dependency install order is invalid')\n"
            "expected_wheel_hashes = {item['filename']: item['sha256'] for item in wheels}\n"
            "if ({path.name for path in wheelhouse_path.iterdir()} != set(expected_wheel_hashes) or\n"
            "        any(path.is_symlink() or not path.is_file() for path in wheelhouse_path.iterdir())):\n"
            "    raise RuntimeError('embedding wheelhouse inventory differs from manifest')\n"
            "smoke_keys = {\n"
            "    'schema_version', 'status', 'observed_at', 'provider_run_ref',\n"
            "    'observation_sha256', 'image_identity',\n"
            "    'image_source_commit', 'python_version', 'dependency_manifest_sha256',\n"
            "    'project_wheel_sha256', 'wheel_sha256s', 'imports',\n"
            "    'psycopg_implementation', 'distributions', 'notebook_private',\n"
            "    'internet_enabled', 'verified_by_central_adapter',\n"
            "}\n"
            "if (not isinstance(smoke, dict) or set(smoke) != smoke_keys or\n"
            "        smoke['schema_version'] !=\n"
            "        'my-data-hub-embedding-dependency-smoke-receipt.v1' or\n"
            "        smoke['status'] != 'pass' or smoke['image_identity'] != image_identity or\n"
            "        smoke['image_source_commit'] != source_commit or\n"
            "        not str(smoke['python_version']).startswith(pins['python_series'] + '.') or\n"
            "        smoke['dependency_manifest_sha256'] != expected_dependency_sha or\n"
            "        smoke['project_wheel_sha256'] != expected_wheel_sha or\n"
            "        smoke['wheel_sha256s'] != expected_wheel_hashes or\n"
            "        smoke['imports'] != dependencies['smoke_requirement']['imports'] or\n"
            "        smoke['psycopg_implementation'] != 'binary' or\n"
            "        not isinstance(smoke['distributions'], dict) or\n"
            "        smoke['notebook_private'] is not True or smoke['internet_enabled'] is not False or\n"
            "        smoke['verified_by_central_adapter'] is not True or\n"
            "        not re.fullmatch(r'[a-f0-9]{64}', str(smoke['observation_sha256'])) or\n"
            "        not re.fullmatch(r'[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/[1-9][0-9]*',\n"
            "                         str(smoke['provider_run_ref']))):\n"
            "    raise RuntimeError('embedding dependency smoke receipt is not verified')\n"
            "for item in wheels:\n"
            "    dependency_wheel = wheelhouse_path / item['filename']\n"
            "    if hashlib.sha256(dependency_wheel.read_bytes()).hexdigest() != item['sha256']:\n"
            "        raise RuntimeError('embedding dependency wheel hash mismatch')\n"
            "    subprocess.run(\n"
            "        [sys.executable, '-m', 'pip', 'install', '--no-index', '--no-deps',\n"
            "         '--disable-pip-version-check', str(dependency_wheel)], check=True\n"
            "    )\n"
            "for distribution in [\n"
            "        *required_image_distributions,\n"
            "        *(item['distribution'] for item in wheels),\n"
            "    ]:\n"
            "    if smoke['distributions'].get(distribution) != importlib.metadata.version(distribution):\n"
            "        raise RuntimeError('embedding dependency smoke version differs from runtime')\n"
        )
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
            "execution_pin_contract": pin_contract,
            "activation_prerequisites_satisfied": False,
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
            f"{dependency_import}"
            "import json\n"
            "import os\n"
            "import platform\n"
            "import re\n"
            "import subprocess\n"
            "import sys\n"
            "from pathlib import Path\n\n"
            f"EXPECTED_SOURCE_SHA256 = {source_sha256!r}\n"
            f"RUNTIME_CONTRACT = {spec.runtime_contract!r}\n"
            f"PIN_CONTRACT = {pin_contract!r}\n"
            "pin_path = Path(os.environ.get('MY_DATA_HUB_EXECUTION_PINS_PATH', ''))\n"
            "expected_pin_sha = os.environ.get('MY_DATA_HUB_EXECUTION_PINS_SHA256', '')\n"
            "if not pin_path.is_file() or not re.fullmatch(r'[a-f0-9]{64}', expected_pin_sha):\n"
            "    raise RuntimeError('hashed execution pins manifest is required')\n"
            "pin_bytes = pin_path.read_bytes()\n"
            "if hashlib.sha256(pin_bytes).hexdigest() != expected_pin_sha:\n"
            "    raise RuntimeError('execution pins manifest hash mismatch')\n"
            "pins = json.loads(pin_bytes)\n"
            "required_pin_keys = {\n"
            "    'schema', 'notebook', 'python_series', 'image_source_commit',\n"
            "    'kaggle_runtime_image_identity',\n"
            "    'input_dataset_versions', 'immutable_asset_sha256s', 'output_contract',\n"
            "    'model', 'privacy', 'resource_class', 'cleanup_retention_policy',\n"
            "}\n"
            "if not isinstance(pins, dict) or set(pins) != required_pin_keys:\n"
            "    raise RuntimeError('execution pins manifest keys differ from the exact contract')\n"
            "if pins['schema'] != PIN_CONTRACT['schema'] or pins['notebook'] != PIN_CONTRACT['notebook']:\n"
            "    raise RuntimeError('execution pins manifest targets a different notebook contract')\n"
            "python_version = platform.python_version()\n"
            "if (pins['python_series'] != PIN_CONTRACT['supported_python_series'] or\n"
            "        not python_version.startswith(pins['python_series'] + '.')):\n"
            "    raise RuntimeError('CPython series differs from execution pins')\n"
            "source_commit = Path('/etc/git_commit').read_text().strip()\n"
            "if (pins['image_source_commit'] != source_commit or\n"
            "        os.environ.get('MY_DATA_HUB_KAGGLE_RUNTIME_SOURCE_COMMIT') != source_commit or\n"
            "        not re.fullmatch(r'[a-f0-9]{40}', source_commit)):\n"
            "    raise RuntimeError('Kaggle runtime source commit differs from execution pins')\n"
            "image_identity = os.environ.get('MY_DATA_HUB_KAGGLE_RUNTIME_IMAGE_IDENTITY', '')\n"
            "if (pins['kaggle_runtime_image_identity'] != image_identity or\n"
            "        not re.fullmatch(r'[^@\\s]+@sha256:[a-f0-9]{64}', image_identity)):\n"
            "    raise RuntimeError('immutable Kaggle runtime image identity is required')\n"
            "dataset_versions = pins['input_dataset_versions']\n"
            "if (not isinstance(dataset_versions, list) or not dataset_versions or\n"
            "        any(not isinstance(ref, str) or not re.fullmatch(\n"
            "            r'[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/[1-9][0-9]*', ref\n"
            "        ) for ref in dataset_versions) or\n"
            "        len(dataset_versions) != len(set(dataset_versions))):\n"
            "    raise RuntimeError('exact numeric input Dataset versions are required')\n"
            "try:\n"
            "    observed_dataset_versions = json.loads(\n"
            "        os.environ.get('MY_DATA_HUB_INPUT_DATASET_VERSIONS_JSON', '')\n"
            "    )\n"
            "except json.JSONDecodeError as exc:\n"
            "    raise RuntimeError('observed input Dataset versions are required') from exc\n"
            "if observed_dataset_versions != dataset_versions:\n"
            "    raise RuntimeError('attached input Dataset versions differ from execution pins')\n"
            "if os.environ.get('MY_DATA_HUB_NOTEBOOK_IS_PRIVATE') != 'true':\n"
            "    raise RuntimeError('operational notebook must be provider-confirmed private')\n"
            "for key in ('output_contract', 'model', 'privacy', 'resource_class', 'cleanup_retention_policy'):\n"
            "    if pins[key] != PIN_CONTRACT[key]:\n"
            "        raise RuntimeError(f'execution pins {key} differs from the generated contract')\n"
            "wheel = Path(os.environ.get('MY_DATA_HUB_WHEEL_PATH', ''))\n"
            "if not wheel.is_file() or wheel.suffix != '.whl':\n"
            "    raise RuntimeError('exact private my-data-hub wheel input is required')\n"
            "expected_wheel_sha = os.environ.get('MY_DATA_HUB_WHEEL_SHA256', '')\n"
            "if (len(expected_wheel_sha) != 64 or \n"
            "        hashlib.sha256(wheel.read_bytes()).hexdigest() != expected_wheel_sha):\n"
            "    raise RuntimeError('my-data-hub wheel hash mismatch')\n"
            "expected_assets = {\n"
            "    'my_data_hub_wheel_sha256': expected_wheel_sha,\n"
            "    'primary_source_sha256': EXPECTED_SOURCE_SHA256,\n"
            "}\n"
            f"{dependency_hash_bootstrap}"
            "if pins['immutable_asset_sha256s'] != expected_assets:\n"
            "    raise RuntimeError('immutable dependency/source asset hashes differ from execution pins')\n"
            f"{dependency_bootstrap}"
            "subprocess.run(\n"
            f"    [sys.executable, '-m', 'pip', 'install', {project_install_index_option}'--no-deps', "
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
    source_sha256 = hashlib.sha256(source.encode()).hexdigest()
    pin_contract = {
        "schema": EXECUTION_PINS_SCHEMA,
        "notebook": spec.directory,
        "supported_python_series": SUPPORTED_PYTHON_SERIES,
        "kaggle_runtime_image_identity": "required-immutable-sha256-at-launch",
        "input_dataset_versions": "required-exact-numeric-private-refs-at-launch",
        "immutable_assets": [
            "my_data_hub_wheel_sha256",
            "primary_source_sha256",
            *(
                ["embedding_dependency_manifest_sha256", "embedding_dependency_smoke_receipt_sha256"]
                if spec.model_id is not None
                else []
            ),
        ],
        "output_contract": spec.runtime_contract,
        "model": (
            {"id": spec.model_id, "revision": spec.model_revision}
            if spec.model_id is not None
            else None
        ),
        "privacy": "private",
        "resource_class": spec.resource_class,
        "cleanup_retention_policy": OPERATIONAL_CLEANUP_RETENTION_POLICY,
    }
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
            "primary_source_sha256": source_sha256,
            "resource_class": spec.resource_class,
            "privacy": "private",
            "timeout_seconds": spec.timeout_seconds,
            "model_id": spec.model_id,
            "model_revision": spec.model_revision,
            "production_ready": False,
            "activation_requires_real_receipt": True,
            "canonical_write_allowed": spec.canonical_write_allowed,
            "external_side_effects_allowed": spec.external_side_effects_allowed,
            "execution_pin_contract": pin_contract,
            "activation_prerequisites_satisfied": False,
        },
    }
    return json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"


def expected_files() -> dict[Path, str]:
    files = {NOTEBOOK_ROOT / "README.md": NOTEBOOK_README}
    for spec in OPERATIONAL_SPECS:
        directory = NOTEBOOK_ROOT / spec.directory
        serialized = serialize_notebook(build_operational_notebook(spec))
        files[directory / "worker.ipynb"] = serialized
        if spec.directory == "05-e5-blogger-embedding-worker":
            files[EMBEDDING_ASSET_ROOT / "e5-worker.json"] = serialized
        elif spec.directory == "06-bge-m3-blogger-embedding-worker":
            files[EMBEDDING_ASSET_ROOT / "bge-worker.json"] = serialized
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
