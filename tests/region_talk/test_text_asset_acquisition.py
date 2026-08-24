from __future__ import annotations

import hashlib
import json
from pathlib import Path

from my_data_hub.hashing import canonical_json_bytes
from my_data_hub.workloads.region_talk.text_asset_acquisition import (
    CandidateSmokeObservation,
    FixedOutputObservation,
    ObservedModelFile,
    TextAssetSmokeObservation,
    compare_smoke_to_official,
    read_official_model_trees,
)
from my_data_hub.workloads.region_talk.text_runtimes import SemanticBankDocument
from my_data_hub.workloads.region_talk.transforms.evidence import SEMANTIC_BANK_HASH

ASSETS = Path("src/my_data_hub/workloads/region_talk/assets")


def _candidate(stage: str, *, tamper: bool = False) -> CandidateSmokeObservation:
    official = next(item for item in read_official_model_trees().entries if item.stage == stage)
    files = [
        ObservedModelFile(
            path=item.path,
            byte_size=item.byte_size + (1 if tamper and index == 0 else 0),
            sha256=item.lfs_sha256 or "0" * 64,
            git_blob_oid=item.git_oid,
        )
        for index, item in enumerate(official.files)
    ]
    if tamper:
        files.append(
            ObservedModelFile(
                path="unexpected.bin",
                byte_size=1,
                sha256="1" * 64,
                git_blob_oid="2" * 40,
            )
        )
    files.sort(key=lambda item: item.path)
    dumped = [item.model_dump(mode="json") for item in files]
    source = (
        "tanviranjumapurbo/multilingual-e5-base/Transformers/default/1"
        if stage == "e5_embedding"
        else "yethukmutt/bge-m3/Transformers/m3/1"
    )
    return CandidateSmokeObservation(
        stage=stage,
        model_source=source,
        model_root_name="fixture",
        files=tuple(files),
        inventory_sha256=hashlib.sha256(canonical_json_bytes(dumped)).hexdigest(),
        huggingface_provenance=None,
        fixed_output=FixedOutputObservation(
            tokenizer_output_sha256="3" * 64,
            dense_output_sha256="4" * 64,
            dimensions=768 if stage == "e5_embedding" else 1024,
            norms=(1.0, 1.0),
        ),
    )


def test_official_tree_metadata_is_canonical_exact_revision_metadata() -> None:
    path = ASSETS / "text-model-official-trees.v1.json"
    raw = path.read_bytes()
    assert raw == canonical_json_bytes(json.loads(raw)) + b"\n"
    trees = read_official_model_trees(path)
    by_stage = {item.stage: item for item in trees.entries}
    assert by_stage["e5_embedding"].revision == "d128750597153bb5987e10b1c3493a34e5a4502a"
    assert by_stage["bge_m3_embedding"].revision == "5617a9f61b028005a4858fdac845db406aefb181"
    assert len(by_stage["e5_embedding"].files) == 23
    assert len(by_stage["bge_m3_embedding"].files) == 30
    e5_weights = next(
        item for item in by_stage["e5_embedding"].files if item.path == "model.safetensors"
    )
    assert e5_weights.lfs_sha256 == "a18a44fad1d0b46ded15928144138cff1135d5cc8233bdd90be5f18822de09a7"


def test_semantic_bank_is_exact_donor_blob_reconstruction() -> None:
    path = ASSETS / "semantic-bank.v1.json"
    raw = path.read_bytes()
    assert raw == canonical_json_bytes(json.loads(raw)) + b"\n"
    bank = SemanticBankDocument.model_validate_json(raw)
    logical = {entry.label: list(entry.examples) for entry in bank.entries}
    assert len(bank.entries) == 11
    assert sum(len(entry.examples) for entry in bank.entries) == 29
    assert hashlib.sha256(canonical_json_bytes(logical)).hexdigest() == SEMANTIC_BANK_HASH
    assert bank.semantic_bank_sha256 == SEMANTIC_BANK_HASH
    assert bank.donor_commit == "d727cc4e256f8018e86c571a531f7ff20b2056fc"
    assert bank.donor_blob_git_oid == "50a68cf33a3e587a3bdcd1668f0d56ccd8b556b6"


def test_official_comparator_accepts_exact_tree_and_reports_precise_mismatch() -> None:
    observation = TextAssetSmokeObservation(
        schema_version="region-talk-text-asset-smoke-observation.v1",
        task_run_id="fixture",
        status="observed",
        python_version="3.12.0",
        internet_enabled=False,
        notebook_kaggle_credentials=False,
        distributions={
            "safetensors": "0.7.0",
            "tokenizers": "0.22.2",
            "torch": "2.10.0+cpu",
            "transformers": "5.0.0",
        },
        candidates=(_candidate("e5_embedding"), _candidate("bge_m3_embedding")),
    )
    results = compare_smoke_to_official(observation, read_official_model_trees())
    assert [item.status for item in results] == ["byte_equivalent", "byte_equivalent"]

    changed = observation.model_copy(
        update={
            "candidates": (
                _candidate("e5_embedding", tamper=True),
                _candidate("bge_m3_embedding"),
            )
        }
    )
    results = compare_smoke_to_official(changed, read_official_model_trees())
    assert results[0].status == "mismatch"
    assert {(item.path, item.reason) for item in results[0].mismatches} == {
        (".eval_results/ArguAna.yaml", "byte_size"),
        ("unexpected.bin", "unexpected"),
    }
    assert results[1].status == "byte_equivalent"


def test_kaggle_smoke_is_offline_no_secret_and_hashes_all_files() -> None:
    source = Path("scripts/provider/assets/region_talk_text_asset_smoke.py").read_text()
    assert "urllib" not in source
    assert "requests" not in source
    assert "snapshot_download" not in source
    assert "from FlagEmbedding" not in source
    assert "hidden[:, 0]" in source
    assert "KAGGLE_USERNAME" in source and "credential env is forbidden" in source
    assert "Path(\"/kaggle/input\").rglob" in source
    assert "yethukmutt/bge-m3/Transformers/m3/1" in source
    assert "tanviranjumapurbo/multilingual-e5-base/Transformers/default/1" in source
