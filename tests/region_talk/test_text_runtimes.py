from __future__ import annotations

import hashlib
import json
import math
from importlib.resources import files
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest

from my_data_hub.embeddings.models import BGE_M3, E5_MULTILINGUAL_BASE
from my_data_hub.hashing import canonical_json_bytes, sha256_file
from my_data_hub.workloads.region_talk.notebook_stages import process_region_talk_stage_item
from my_data_hub.workloads.region_talk.stage_dispatch import StageExecutionPayload
from my_data_hub.workloads.region_talk.stage_execution import stage_run_id, work_item_id
from my_data_hub.workloads.region_talk.text_runtimes import (
    SemanticBankDocument,
    TextRuntimeAssetError,
    TextRuntimeAssetManifest,
    VerifiedTextStageRuntime,
    build_text_runtime_asset_manifest,
    discover_attached_text_runtime,
    read_e5_frozen_producer_authority,
    read_text_runtime_registry,
    verified_text_runtime_kernel_source,
    verified_text_runtime_model_source,
    verify_text_runtime_asset_bundle,
)
from my_data_hub.workloads.region_talk.transforms.evidence import ALL_LABELS

TASK = UUID("11111111-1111-4111-8111-111111111111")
BATCH = UUID("22222222-2222-4222-8222-222222222222")
SUBJECT = UUID("33333333-3333-4333-8333-333333333333")
CONTENT = UUID("44444444-4444-4444-8444-444444444444")
INPUT = hashlib.sha256(b"typed-text-input").hexdigest()
MASTER = UUID("55555555-5555-4555-8555-555555555555")
EPOCH = 7
IMAGE_IDENTITY = "gcr.io/kaggle-images/python@sha256:" + "8" * 64
IMAGE_COMMIT = "9" * 40


def _semantic_bank(path: Path) -> str:
    entries = []
    for label in sorted(ALL_LABELS):
        entries.append(
            {
                "label": label,
                "examples": [
                    f"fixture semantic definition one for {label}",
                    f"fixture semantic definition two for {label}",
                ],
            }
        )
    logical = {entry["label"]: entry["examples"] for entry in entries}
    unsigned = {
        "schema_version": "region-talk-semantic-bank.v1",
        "semantic_bank_version": "semantic_bank_v1",
        "semantic_bank_sha256": hashlib.sha256(canonical_json_bytes(logical)).hexdigest(),
        "donor_repository": "https://example.invalid/fixture.git",
        "donor_commit": "1" * 40,
        "donor_path": "fixture.py",
        "donor_blob_git_oid": "2" * 40,
        "entries": entries,
    }
    value = {
        **unsigned,
        "receipt_sha256": hashlib.sha256(canonical_json_bytes(unsigned)).hexdigest(),
    }
    SemanticBankDocument.model_validate(value)
    path.write_bytes(canonical_json_bytes(value) + b"\n")
    return unsigned["semantic_bank_sha256"]


def _bundle(tmp_path: Path, stage: str) -> tuple[Path, Path, str, dict[str, str]]:
    root = tmp_path / stage
    model = root / "model"
    model.mkdir(parents=True)
    (model / "config.json").write_text('{"fixture":true}\n')
    (model / "tokenizer.json").write_text('{"fixture":"tokenizer"}\n')
    weight_name = "model.safetensors" if stage == "e5_embedding" else "pytorch_model.bin"
    (model / weight_name).write_bytes(b"deterministic-fixture-weights")
    bank_path = root / "semantic-bank.v1.json"
    bank_sha = _semantic_bank(bank_path)
    dependencies = (
        {"torch": "2.7.0", "transformers": "4.52.0"}
        if stage == "e5_embedding"
        else {"FlagEmbedding": "1.3.5", "torch": "2.7.0"}
    )
    manifest_body = build_text_runtime_asset_manifest(
        stage=stage,
        bundle_root=root,
        model_directory="model",
        semantic_bank_relative_path="semantic-bank.v1.json",
        required_distributions=dependencies,
        expected_semantic_bank_sha256=bank_sha,
    )
    filename = (
        "region-talk-e5-assets.v1.json"
        if stage == "e5_embedding"
        else "region-talk-bge-m3-assets.v1.json"
    )
    manifest = root / filename
    manifest.write_bytes(manifest_body)
    return root, manifest, bank_sha, dependencies


class _FixtureEncoder:
    def __init__(self, dimensions: int) -> None:
        self.dimensions = dimensions
        self.calls: list[tuple[tuple[str, ...], dict[str, Any]]] = []

    def encode(self, texts, **kwargs):  # type: ignore[no-untyped-def]
        self.calls.append((tuple(texts), dict(kwargs)))
        vectors = []
        for index in range(len(texts)):
            if index == 0:
                vector = [1.0, *([0.0] * (self.dimensions - 1))]
            else:
                cosine = round(0.05 + index * 0.04, 6)
                vector = [cosine, math.sqrt(1.0 - cosine * cosine), *([0.0] * (self.dimensions - 2))]
            vectors.append(vector)
        return vectors


def _verified_runtime(tmp_path: Path, stage: str) -> tuple[VerifiedTextStageRuntime, _FixtureEncoder]:
    root, manifest, bank_sha, dependencies = _bundle(tmp_path, stage)
    manifest_sha = sha256_file(manifest)
    assets = verify_text_runtime_asset_bundle(
        bundle_root=root,
        manifest_path=manifest,
        expected_manifest_sha256=manifest_sha,
        expected_stage=stage,
        expected_semantic_bank_sha256=bank_sha,
        version_resolver=dependencies.__getitem__,
    )
    model = E5_MULTILINGUAL_BASE if stage == "e5_embedding" else BGE_M3
    encoder = _FixtureEncoder(model.dimensions)
    runtime = VerifiedTextStageRuntime(
        assets=assets,
        encoder=encoder,
    )
    runtime.bind_worker_capability(master_instance_id=MASTER, epoch=EPOCH)
    return runtime, encoder


def _runtime_pin(runtime: VerifiedTextStageRuntime) -> dict[str, Any]:
    registration = runtime.registration_metadata
    pin_base = {
        "stage": registration.stage,
        "contract_version": registration.contract_version,
        "effective_canonical_revision": 1,
        "pin_generation": 1,
        "master_instance_id": str(MASTER),
        "epoch": EPOCH,
        "model_id": registration.model_id,
        "model_revision": registration.model_revision,
        "encoder_contract": registration.encoder_contract,
        "semantic_bank_version": registration.semantic_bank_version,
        "semantic_bank_sha256": registration.semantic_bank_sha256,
        "runtime_source_sha256": registration.runtime_source_sha256,
        "asset_manifest_sha256": registration.asset_manifest_sha256,
        "provider_image_identity": IMAGE_IDENTITY,
        "provider_image_source_commit": IMAGE_COMMIT,
        "producer_exact_id": "filled-by-signing-helper",
    }
    return _resign_runtime_pin(pin_base)


def _resign_runtime_pin(pin_base: dict[str, Any]) -> dict[str, Any]:
    pin_base = dict(pin_base)
    pin_base["producer_exact_id"] = (
        f"{pin_base['model_id']}@{pin_base['model_revision']}"
        f"+assets:{pin_base['asset_manifest_sha256']}"
        f"+source:{pin_base['runtime_source_sha256']}"
        f"+image:{pin_base['provider_image_identity']}"
        f"+commit:{pin_base['provider_image_source_commit']}"
    )
    unsigned = {
        "schema_version": "region-talk-stage-runtime-pin-receipt.v1",
        "registered": True,
        **pin_base,
        "prior_pin_receipt_sha256": None,
        "pin_sha256": hashlib.sha256(canonical_json_bytes(pin_base)).hexdigest(),
        "publication_dispatch": False,
        "notification_dispatch": False,
    }
    return {
        **unsigned,
        "receipt_sha256": hashlib.sha256(canonical_json_bytes(unsigned)).hexdigest(),
    }


def _payload(stage: str, runtime: VerifiedTextStageRuntime) -> StageExecutionPayload:
    text = "Калининградский музей и маршрут путешествия"
    run = stage_run_id(TASK, BATCH)
    return StageExecutionPayload(
        schema_version="region-talk-stage-work-execution.v1",
        stage_run_id=run,
        candidate_id=SUBJECT,
        candidate_revision=1,
        revision_fingerprint="a" * 64,
        content_id=CONTENT,
        content_type="article",
        canonical_url="https://example.org/article",
        canonical_source_key="web:example.org",
        input_fingerprint=INPUT,
        upstream_results=(),
        input_data={
            "schema_version": "region-talk-stage-text-input.v1",
            "text": text,
            "text_sha256": hashlib.sha256(text.encode()).hexdigest(),
            "topics": ["museum"],
            "runtime_pin": _runtime_pin(runtime),
        },
    )


@pytest.mark.parametrize(
    ("stage", "contract", "model"),
    (
        ("e5_embedding", "e5_semantic_bank_scores_v1", E5_MULTILINGUAL_BASE),
        ("bge_m3_embedding", "bge_m3_flagembedding_dense_v1", BGE_M3),
    ),
)
def test_verified_text_runtime_executes_typed_stage_deterministically(
    tmp_path: Path,
    stage: str,
    contract: str,
    model: Any,
) -> None:
    runtime, encoder = _verified_runtime(tmp_path, stage)
    payload = _payload(stage, runtime)
    item = {
        "work_item_id": str(
            work_item_id(
                run_id=payload.stage_run_id,
                candidate_id=SUBJECT,
                revision=1,
                stage=stage,
                input_fingerprint=INPUT,
            )
        ),
        "subject_type": "region_talk.candidate",
        "subject_id": str(SUBJECT),
        "input_fingerprint": INPUT,
        "payload": payload.model_dump(mode="json"),
    }
    first = process_region_talk_stage_item(
        item, stage=stage, contract_version=contract, runtime=runtime
    )
    second = process_region_talk_stage_item(
        item, stage=stage, contract_version=contract, runtime=runtime
    )
    assert first == second
    assert first["producer_exact_id"] == payload.input_data["runtime_pin"]["producer_exact_id"]
    assert hashlib.sha256(canonical_json_bytes(first)).hexdigest() == hashlib.sha256(
        canonical_json_bytes(second)
    ).hexdigest()
    metrics = first["metrics"]
    assert metrics["model_id"] == model.model_key
    assert metrics["model_revision"] == model.revision
    assert metrics["encoder_contract"] == model.encoder_contract_version
    assert metrics["provider_image_identity"] == IMAGE_IDENTITY
    assert metrics["provider_image_source_commit"] == IMAGE_COMMIT
    assert metrics["pin_sha256"] == payload.input_data["runtime_pin"]["pin_sha256"]
    assert set(metrics["scores"]) == ALL_LABELS
    assert len(metrics["evidence_fingerprint"]) == 64
    texts, call = encoder.calls[0]
    assert texts[0].startswith(model.query_prefix)
    assert all(text.startswith(model.document_prefix) for text in texts[1:])
    assert call == {
        "model": model,
        "max_tokens": model.max_tokens,
        "pooling": model.pooling,
        "normalize": True,
        "dense_only": True,
    }
    registration = runtime.registration_metadata
    assert registration.stage == stage
    assert registration.contract_version == contract
    assert registration.model_id == model.model_key
    assert registration.model_revision == model.revision
    assert registration.encoder_contract == model.encoder_contract_version
    assert registration.asset_manifest_sha256 == metrics["asset_manifest_sha256"]
    assert not hasattr(registration, "image_identity")


def test_runtime_pin_tamper_and_epoch_mismatch_are_denied_before_encoding(tmp_path: Path) -> None:
    runtime, encoder = _verified_runtime(tmp_path, "e5_embedding")
    payload = _payload("e5_embedding", runtime)
    tampered = payload.model_copy(deep=True)
    tampered.input_data["runtime_pin"]["asset_manifest_sha256"] = "0" * 64
    with pytest.raises(ValueError, match="runtime pin is invalid"):
        runtime.execute(
            stage="e5_embedding",
            contract_version="e5_semantic_bank_scores_v1",
            subject_id=SUBJECT,
            input_fingerprint=INPUT,
            payload=tampered,
        )
    assert encoder.calls == []

    validly_rehashed = payload.model_copy(deep=True)
    pin_base = {
        key: value
        for key, value in validly_rehashed.input_data["runtime_pin"].items()
        if key
        not in {
            "schema_version",
            "registered",
            "prior_pin_receipt_sha256",
            "pin_sha256",
            "publication_dispatch",
            "notification_dispatch",
            "receipt_sha256",
        }
    }
    pin_base["asset_manifest_sha256"] = "0" * 64
    validly_rehashed.input_data["runtime_pin"] = _resign_runtime_pin(pin_base)
    with pytest.raises(ValueError, match="verified runtime/capability"):
        runtime.execute(
            stage="e5_embedding",
            contract_version="e5_semantic_bank_scores_v1",
            subject_id=SUBJECT,
            input_fingerprint=INPUT,
            payload=validly_rehashed,
        )
    assert encoder.calls == []

    fixture_runtime, other_encoder = _verified_runtime(tmp_path / "other", "e5_embedding")
    other_runtime = VerifiedTextStageRuntime(
        assets=fixture_runtime.assets,
        encoder=other_encoder,
    )
    other_runtime.bind_worker_capability(master_instance_id=MASTER, epoch=EPOCH + 1)
    with pytest.raises(ValueError, match="verified runtime/capability"):
        other_runtime.execute(
            stage="e5_embedding",
            contract_version="e5_semantic_bank_scores_v1",
            subject_id=SUBJECT,
            input_fingerprint=INPUT,
            payload=_payload("e5_embedding", other_runtime).model_copy(
                update={
                    "input_data": {
                        **_payload("e5_embedding", runtime).input_data,
                    }
                }
            ),
        )
    assert other_encoder.calls == []


def test_asset_manifest_is_complete_canonical_and_tamper_fails(tmp_path: Path) -> None:
    root, manifest, bank_sha, dependencies = _bundle(tmp_path, "e5_embedding")
    manifest_sha = sha256_file(manifest)
    parsed = TextRuntimeAssetManifest.model_validate(json.loads(manifest.read_bytes()))
    assert parsed.model.revision == E5_MULTILINGUAL_BASE.revision
    assert parsed.runtime_source_sha256
    assert manifest.read_bytes() == canonical_json_bytes(parsed.model_dump(mode="json")) + b"\n"
    verify_text_runtime_asset_bundle(
        bundle_root=root,
        manifest_path=manifest,
        expected_manifest_sha256=manifest_sha,
        expected_stage="e5_embedding",
        expected_semantic_bank_sha256=bank_sha,
        version_resolver=dependencies.__getitem__,
    )
    (root / "model" / "config.json").write_text('{"fixture":false}\n')
    with pytest.raises(TextRuntimeAssetError, match="MODEL_FILE_MISMATCH"):
        verify_text_runtime_asset_bundle(
            bundle_root=root,
            manifest_path=manifest,
            expected_manifest_sha256=manifest_sha,
            expected_stage="e5_embedding",
            expected_semantic_bank_sha256=bank_sha,
            version_resolver=dependencies.__getitem__,
        )


def test_source_and_dependency_attestation_fail_closed(tmp_path: Path) -> None:
    root, manifest, bank_sha, dependencies = _bundle(tmp_path, "bge_m3_embedding")
    manifest_sha = sha256_file(manifest)
    with pytest.raises(TextRuntimeAssetError, match="DEPENDENCY_VERSION_MISMATCH"):
        verify_text_runtime_asset_bundle(
            bundle_root=root,
            manifest_path=manifest,
            expected_manifest_sha256=manifest_sha,
            expected_stage="bge_m3_embedding",
            expected_semantic_bank_sha256=bank_sha,
            version_resolver=lambda _name: "0.0.0",
        )
    value = json.loads(manifest.read_bytes())
    value["runtime_source_sha256"] = "0" * 64
    unsigned = {key: item for key, item in value.items() if key != "receipt_sha256"}
    value["receipt_sha256"] = hashlib.sha256(canonical_json_bytes(unsigned)).hexdigest()
    manifest.write_bytes(canonical_json_bytes(value) + b"\n")
    with pytest.raises(TextRuntimeAssetError, match="MANIFEST_BINDING_MISMATCH"):
        verify_text_runtime_asset_bundle(
            bundle_root=root,
            manifest_path=manifest,
            expected_manifest_sha256=sha256_file(manifest),
            expected_stage="bge_m3_embedding",
            expected_semantic_bank_sha256=bank_sha,
            version_resolver=dependencies.__getitem__,
        )


def test_unmanifested_symlink_is_denied(tmp_path: Path) -> None:
    root, manifest, bank_sha, dependencies = _bundle(tmp_path, "e5_embedding")
    (root / "model" / "unreviewed-link").symlink_to(root / "model" / "config.json")
    with pytest.raises(TextRuntimeAssetError, match="MODEL_DIRECTORY_UNSAFE"):
        verify_text_runtime_asset_bundle(
            bundle_root=root,
            manifest_path=manifest,
            expected_manifest_sha256=sha256_file(manifest),
            expected_stage="e5_embedding",
            expected_semantic_bank_sha256=bank_sha,
            version_resolver=dependencies.__getitem__,
        )


def test_committed_registry_approves_exact_provider_carrier_for_each_text_runtime(
    tmp_path: Path,
) -> None:
    registry = read_text_runtime_registry()
    assert [item.availability for item in registry.entries] == [
        "verified",
        "verified",
    ]
    assert verified_text_runtime_model_source("bge_m3_embedding") == (
        "yethukmutt/bge-m3/Transformers/m3/1"
    )
    assert verified_text_runtime_model_source("e5_embedding") is None
    assert verified_text_runtime_kernel_source("e5_embedding") == (
        "zigomaro/mdh-region-talk-e5-assets-v1"
    )
    assert verified_text_runtime_kernel_source("bge_m3_embedding") is None
    bge_manifest = TextRuntimeAssetManifest.model_validate_json(
        Path("src/my_data_hub/workloads/region_talk/assets/region-talk-bge-m3-assets.v1.json").read_bytes()
    )
    assert bge_manifest.excluded_nonruntime_paths == ("imgs/.DS_Store",)
    assert len(bge_manifest.model_files) == 29
    assert bge_manifest.runtime_source_sha256 == sha256_file(
        Path("src/my_data_hub/workloads/region_talk/text_runtimes.py")
    )
    wrong = bge_manifest.model_dump(mode="json")
    wrong["excluded_nonruntime_paths"] = ["README.md"]
    unsigned = {key: item for key, item in wrong.items() if key != "receipt_sha256"}
    wrong["receipt_sha256"] = hashlib.sha256(canonical_json_bytes(unsigned)).hexdigest()
    with pytest.raises(ValueError, match="acquired-model contract differs"):
        TextRuntimeAssetManifest.model_validate(wrong)
    e5_manifest = TextRuntimeAssetManifest.model_validate_json(
        Path("src/my_data_hub/workloads/region_talk/assets/region-talk-e5-assets.v1.json").read_bytes()
    )
    authority = read_e5_frozen_producer_authority()
    assert e5_manifest.provider_kernel_source == authority["provider_ref"]
    assert e5_manifest.producer_authority_sha256 == authority["authority_sha256"]
    assert e5_manifest.official_tree_receipt_sha256 == (
        "a9bf9a773342bb1593801f34bdd8d230b44c4a934842deea0b444ad5371aae70"
    )
    assert len(e5_manifest.model_files) == 23
    with pytest.raises(TextRuntimeAssetError, match="MODEL_DIRECTORY_ABSENT_OR_AMBIGUOUS"):
        discover_attached_text_runtime("e5_embedding", input_root=tmp_path)
    source = Path(
        "src/my_data_hub/workloads/region_talk/text_runtimes.py"
    ).read_text()
    assert "snapshot_download" not in source
    assert "local_files_only=True" in source
    assert 'BGEM3FlagModel(str(model_root)' in source


def test_verified_model_source_discovery_checks_split_model_and_packaged_bank(
    tmp_path: Path,
) -> None:
    root, manifest, bank_sha, dependencies = _bundle(tmp_path, "bge_m3_embedding")
    value = json.loads(manifest.read_bytes())
    unsigned = {key: item for key, item in value.items() if key != "receipt_sha256"}
    production_bank = Path(
        "src/my_data_hub/workloads/region_talk/assets/semantic-bank.v1.json"
    ).read_bytes()
    (root / "semantic-bank.v1.json").write_bytes(production_bank)
    production_bank_value = json.loads(production_bank)
    unsigned.update(
        provider_model_source="yethukmutt/bge-m3/Transformers/m3/1",
        official_tree_receipt_sha256=(
            "526c363c7abfa3c60eed26ab559885a29cb23384abd06f4ead6beef636d3c418"
        ),
        excluded_nonruntime_paths=["imgs/.DS_Store"],
        semantic_bank_file={
            "relative_path": "semantic-bank.v1.json",
            "sha256": hashlib.sha256(production_bank).hexdigest(),
            "byte_size": len(production_bank),
        },
        semantic_bank_sha256=production_bank_value["semantic_bank_sha256"],
    )
    value = {
        **unsigned,
        "receipt_sha256": hashlib.sha256(canonical_json_bytes(unsigned)).hexdigest(),
    }
    manifest.write_bytes(canonical_json_bytes(value) + b"\n")
    manifest_sha = sha256_file(manifest)
    entry = {
        "stage": "bge_m3_embedding",
        "availability": "verified",
        "manifest_filename": manifest.name,
        "manifest_sha256": manifest_sha,
        "model_source": "yethukmutt/bge-m3/Transformers/m3/1",
        "model": value["model"],
    }
    e5 = read_text_runtime_registry().entries[1].model_dump(mode="json")
    registry_unsigned = {
        "schema_version": "region-talk-text-runtime-registry.v1",
        "entries": [entry, e5],
    }
    registry = {
        **registry_unsigned,
        "receipt_sha256": hashlib.sha256(canonical_json_bytes(registry_unsigned)).hexdigest(),
    }
    registry_path = root / "text-runtime-assets.v1.json"
    registry_path.write_bytes(canonical_json_bytes(registry) + b"\n")
    runtime = discover_attached_text_runtime(
        "bge_m3_embedding",
        input_root=root,
        registry_path=registry_path,
        encoder_factory=lambda _stage, _root: _FixtureEncoder(BGE_M3.dimensions),
        version_resolver=dependencies.__getitem__,
    )
    assert runtime is not None
    assert runtime.assets.model_root == (root / "model").resolve()
    assert bank_sha != runtime.assets.semantic_bank.semantic_bank_sha256
    assert runtime.assets.semantic_bank.semantic_bank_sha256 == production_bank_value[
        "semantic_bank_sha256"
    ]


def test_registry_is_packaged_as_canonical_runtime_data() -> None:
    packaged = files("my_data_hub.workloads.region_talk.assets").joinpath(
        "text-runtime-assets.v1.json"
    )
    body = packaged.read_bytes()
    value = json.loads(body)
    assert body == canonical_json_bytes(value) + b"\n"
    assert read_text_runtime_registry(Path(str(packaged))).receipt_sha256 == value["receipt_sha256"]
    packaged_bge = files("my_data_hub.workloads.region_talk.assets").joinpath(
        "region-talk-bge-m3-assets.v1.json"
    )
    assert TextRuntimeAssetManifest.model_validate_json(packaged_bge.read_bytes()).stage == (
        "bge_m3_embedding"
    )


def test_e5_frozen_producer_authority_rejects_cross_binding_even_if_outer_hash_is_resigned(
    tmp_path: Path,
) -> None:
    source = Path(
        "src/my_data_hub/workloads/region_talk/assets/"
        "region-talk-e5-frozen-producer-authority.v1.json"
    )
    value = json.loads(source.read_bytes())
    value["source_commit"] = "0" * 40
    unsigned = {key: item for key, item in value.items() if key != "authority_sha256"}
    value["authority_sha256"] = hashlib.sha256(canonical_json_bytes(unsigned)).hexdigest()
    tampered = tmp_path / source.name
    tampered.write_bytes(canonical_json_bytes(value) + b"\n")
    with pytest.raises(TextRuntimeAssetError, match="AUTHORITY_INVALID"):
        read_e5_frozen_producer_authority(tampered)
