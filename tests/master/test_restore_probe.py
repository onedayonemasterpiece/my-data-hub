from __future__ import annotations

import pytest

from my_data_hub.checkpoints.restore_probe import logical_probe_hash


def test_logical_probe_hash_is_order_independent_and_revision_bound() -> None:
    first = logical_probe_hash(
        schema_version=12,
        canonical_revision=7,
        row_counts={"hub.actor": 266, "region_talk.blogger_profile": 266},
    )
    second = logical_probe_hash(
        schema_version=12,
        canonical_revision=7,
        row_counts={"region_talk.blogger_profile": 266, "hub.actor": 266},
    )
    assert first == second
    assert first != logical_probe_hash(
        schema_version=12,
        canonical_revision=8,
        row_counts={"hub.actor": 266, "region_talk.blogger_profile": 266},
    )


@pytest.mark.parametrize("name", ["hub.actor;DROP TABLE x", "public..x", "UPPER.x", "x"])
def test_logical_probe_rejects_unsafe_relation_names(name: str) -> None:
    with pytest.raises(ValueError, match="row-count"):
        logical_probe_hash(schema_version=1, canonical_revision=0, row_counts={name: 1})
