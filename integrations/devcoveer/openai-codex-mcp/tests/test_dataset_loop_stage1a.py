from __future__ import annotations

import hashlib
import json
import stat
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parents[1]))
from dataset_loop.catalog import classify_catalog, has_real_tool_receipt, profile_models, rank_nvidia_candidates
from dataset_loop.datasets import DatasetResolver, ResolutionError
from dataset_loop.models import DatasetSelector
from dataset_loop.prompt import render_canonical_prompt
from dataset_loop.service import DatasetLoopService


class MemoryRemote:
    def __init__(self, files: dict[str, str], sha: str = "a" * 40) -> None:
        self.files = files
        self.sha = sha

    def default_branch_sha(self) -> str:
        return self.sha

    def list_paths(self, sha: str) -> list[str]:
        assert sha == self.sha
        return sorted(self.files)

    def read_text(self, sha: str, path: str) -> str:
        assert sha == self.sha
        if path not in self.files:
            raise ResolutionError("missing")
        return self.files[path]


def fixture_files() -> dict[str, str]:
    return {
        "datasets/alpha/dataset.yaml": (
            "dataset_id: alpha\ntitle: Alpha  Dataset\nrecord_schema: schemas/alpha.json\n"
            "pipeline: pipelines/alpha.yaml\ncurrent_state: states/alpha.json\n"
        ),
        "datasets/beta/dataset.yaml": (
            "id: beta\ntitle: Beta\nrecord_schema: schemas/beta.json\n"
            "pipeline: pipelines/beta.yaml\ncurrent_state: states/beta.json\n"
        ),
        "schemas/alpha.json": '{"type":"object"}',
        "pipelines/alpha.yaml": "version: 1\n",
        "states/alpha.json": "{}\n",
        "schemas/beta.json": '{"type":"object","beta":true}',
        "pipelines/beta.yaml": "version: 2\n",
        "states/beta.json": '{"current":true}\n',
    }


def test_resolver_freezes_remote_sha_paths_and_hashes() -> None:
    selectors = [DatasetSelector(dataset_id="alpha"), DatasetSelector(title=" Beta ")]
    result = DatasetResolver(MemoryRemote(fixture_files())).resolve(selectors)
    assert result["source_sha"] == "a" * 40
    assert [item["id"] for item in result["datasets"]] == ["alpha", "beta"]
    alpha = result["datasets"][0]
    alpha_yaml = fixture_files()["datasets/alpha/dataset.yaml"]
    assert alpha["dataset_yaml_sha256"] == hashlib.sha256(alpha_yaml.encode()).hexdigest()
    assert alpha["references"]["record_schema"]["sha256"] == hashlib.sha256(b'{"type":"object"}').hexdigest()
    assert len(result["manifest_sha256"]) == 64
    assert len(alpha["contract_sha256"]) == 64


def test_baseline_manifest_hashes_match_copied_safe_files() -> None:
    root = Path(__file__).parents[1]
    manifest = json.loads((root / "BASELINE_MANIFEST.json").read_text())
    for name, expected in manifest["files"].items():
        assert hashlib.sha256((root / "baseline" / name).read_bytes()).hexdigest() == expected


def test_resolver_missing_ambiguous_path_and_duplicate_fail_closed() -> None:
    resolver = DatasetResolver(MemoryRemote(fixture_files()))
    with pytest.raises(ResolutionError, match="missing"):
        resolver.resolve([DatasetSelector(dataset_id="none")])
    ambiguous = fixture_files()
    ambiguous["datasets/other/dataset.yaml"] = ambiguous["datasets/alpha/dataset.yaml"].replace(
        "dataset_id: alpha", "dataset_id: alpha2"
    )
    with pytest.raises(ResolutionError, match="ambiguous"):
        DatasetResolver(MemoryRemote(ambiguous)).resolve([DatasetSelector(title="Alpha Dataset")])
    with pytest.raises(ResolutionError, match="duplicate"):
        resolver.resolve([DatasetSelector(dataset_id="alpha"), DatasetSelector(path="datasets/alpha/dataset.yaml")])


def test_prompt_is_byte_invariant_for_iteration_and_model_context() -> None:
    run_id = "run_" + "1" * 32
    first = render_canonical_prompt(
        run_id=run_id,
        frozen_dataset_ids=["beta", "alpha"],
        logical_iteration=1,
        model="zen-a",
    )
    second = render_canonical_prompt(
        run_id=run_id,
        frozen_dataset_ids=["alpha", "beta"],
        logical_iteration=99,
        model="zen-z",
    )
    assert first.encode() == second.encode()


def catalog_items() -> list[dict[str, object]]:
    return [
        {"selection": "zen/a", "provider": "open", "active": True, "free": True, "metadata": {"profile": "zen"}},
        {"selection": "zen/b", "provider": "open", "active": True, "free": True, "tags": ["zen"]},
        {"selection": "zen/off", "provider": "open", "active": False, "free": True, "metadata": {"profile": "zen"}},
        {"selection": "nvidia/deepseek-v4-2026-08-30", "provider": "nvidia", "active": True, "free": False},
        {"selection": "nvidia/moonshot-kimi-k3", "provider": "nvidia", "active": True, "free": False},
        {"selection": "nvidia/nemotron-3", "provider": "nvidia", "active": True, "free": False},
        {"selection": "nvidia/deepseek-old", "provider": "nvidia", "active": True, "free": False},
    ]


def receipt(valid: bool = True) -> dict[str, object]:
    return {
        "terminal": valid,
        "messages": [
            {
                "parts": [
                    {"type": "tool", "tool": "websearch", "state": "completed"},
                    {"type": "tool-invocation", "toolName": "webfetch", "state": "success"},
                ]
            }
        ],
    }


def test_profiles_receipts_and_nvidia_ranking() -> None:
    catalog = classify_catalog(catalog_items())
    assert set(profile_models("zen", catalog)) == {"zen", "nvidia"}
    assert not profile_models("zen", catalog)["nvidia"]
    assert len(profile_models("zen_nvidia_audit", catalog)["zen"]) == 2
    assert len(profile_models("nvidia_assisted", catalog)["nvidia"]) == 4
    names = [model.selection for model in rank_nvidia_candidates(catalog)]
    assert names[:3] == ["nvidia/deepseek-v4-2026-08-30", "nvidia/moonshot-kimi-k3", "nvidia/nemotron-3"]
    assert has_real_tool_receipt(receipt())
    prose = {"terminal": True, "messages": [{"parts": [{"type": "text", "text": "websearch webfetch"}]}]}
    assert not has_real_tool_receipt(prose)
    assert not has_real_tool_receipt({"terminal": True, "websearch": True, "webfetch": True})
    assert not has_real_tool_receipt(receipt(False))


def test_blocked_persistence_artifacts_and_controls(tmp_path: Path) -> None:
    resolver = DatasetResolver(MemoryRemote(fixture_files()))
    service = DatasetLoopService(tmp_path / "state", resolver=resolver)
    blocked = service.start(
        selectors=[DatasetSelector(dataset_id="alpha")],
        catalog_items=catalog_items(),
        probe_receipts={"zen/a": receipt()},
    )
    assert blocked["status"] == "blocked"
    assert blocked["mutations"] == 0
    assert "fewer than two" in blocked["blocker"]
    artifacts = tmp_path / "state" / "artifacts" / blocked["run_id"]
    expected = {"ledger.jsonl", "metrics.json", "scorecard.json", "schema-gaps.json", "audit.jsonl"}
    assert {path.name for path in artifacts.iterdir()} == expected
    assert stat.S_IMODE((tmp_path / "state").stat().st_mode) == 0o700
    assert all(stat.S_IMODE(path.stat().st_mode) == 0o600 for path in artifacts.iterdir())
    reconstructed = DatasetLoopService(tmp_path / "state", resolver=resolver)
    assert reconstructed.status(blocked["run_id"]) == "blocked"
    assert reconstructed.control(blocked["run_id"], "stop")["status"] == "stopped"
    assert reconstructed.control(blocked["run_id"], "stop")["status"] == "stopped"
    assert reconstructed.list()[0]["run_id"] == blocked["run_id"]


def test_ready_requires_two_distinct_zen_receipts_and_pause_resume(tmp_path: Path) -> None:
    resolver = DatasetResolver(MemoryRemote(fixture_files()))
    service = DatasetLoopService(tmp_path / "state", resolver=resolver)
    ready = service.start(
        selectors=[DatasetSelector(dataset_id="alpha")],
        catalog_items=catalog_items(),
        probe_receipts={"zen/a": receipt(), "zen/b": receipt()},
    )
    assert ready["status"] == "ready"
    assert ready["mutations"] == 0
    assert service.control(ready["run_id"], "pause")["status"] == "paused"
    fresh = DatasetLoopService(tmp_path / "state", resolver=resolver)
    assert fresh.control(ready["run_id"], "pause")["status"] == "paused"
    assert fresh.control(ready["run_id"], "resume")["status"] == "ready"
    assert fresh.control(ready["run_id"], "resume")["status"] == "ready"
