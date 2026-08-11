from __future__ import annotations

from my_data_hub.workloads.bloggers.schema import SOURCE_QUERY
from my_data_hub.workloads.bloggers.ydb_reader import ZERO_ROW_WRITE_DENIAL_PROBE


def test_reader_contract_is_one_ordered_snapshot_and_zero_row_probe() -> None:
    assert "ORDER BY `record_id`" in SOURCE_QUERY
    assert "LIMIT" not in SOURCE_QUERY
    assert "UPDATE `region_talk_external_blogger_evidence`" in ZERO_ROW_WRITE_DENIAL_PROBE
    assert "__my_data_hub_permission_probe_never_matches__" in ZERO_ROW_WRITE_DENIAL_PROBE
