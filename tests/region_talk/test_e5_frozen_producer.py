from __future__ import annotations

import ast
import hashlib
from pathlib import Path
from uuid import UUID

from scripts.provider.run_region_talk_e5_asset_builder import _source


def test_frozen_e5_builder_embeds_exact_official_tree_and_bank() -> None:
    root = Path(__file__).parents[2]
    task = UUID("11111111-1111-4111-8111-111111111111")
    source = _source(root, task, "a" * 40)
    ast.parse(source)
    text = source.decode()
    assert str(task) in text
    assert "d128750597153bb5987e10b1c3493a34e5a4502a" in text
    assert "4ec81e6ede79f3dae1bb366a06366e7197d960e1c04e124f77b3db12f2f1981f" in text
    assert "allow_patterns=expected_paths" in text
    assert "token=False" in text
    assert "KAGGLE_API_TOKEN" in text and "HF_TOKEN" in text
    assert "snapshot_download" in text
    assert "publication_dispatch" in text and "notification_dispatch" in text
    assert hashlib.sha256(source).hexdigest()


def test_frozen_e5_builder_never_places_receipt_under_large_output_tree() -> None:
    source = Path("scripts/provider/assets/region_talk_e5_asset_builder.py").read_text()
    assert 'Path("/kaggle/working/region-talk-e5-frozen-producer-receipt.v1.json")' in source
    runner = Path("scripts/provider/run_region_talk_e5_asset_builder.py").read_text()
    assert 'RECEIPT = "region-talk-e5-frozen-producer-receipt.v1.json"' in runner
    assert "delete_task_created_resource" not in runner
