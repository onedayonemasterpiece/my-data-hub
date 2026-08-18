from __future__ import annotations

from datetime import UTC, datetime

import pytest

from my_data_hub.master_runtime.role_security_probe import build_role_security_evidence


def test_security_evidence_binds_full_probe_hashes_without_probe_bodies() -> None:
    result = {
        "ok": True,
        "probe_count": 2,
        "role_probe_count": 1,
        "security_probe_count": 1,
        "failures": [],
        "probes": [
            {"role": "mdh_mcp_reader", "name": "read", "expected": "allow", "passed": True},
            {
                "role": "mdh_mcp_reader",
                "name": "ddl",
                "expected": "deny",
                "passed": True,
                "sqlstate": "42501",
            },
        ],
        "cleanup": "transaction_rolled_back",
    }
    evidence = build_role_security_evidence(
        result,
        source_commit="a" * 40,
        master_instance_id="33333333-3333-4333-8333-333333333333",
        epoch=7,
        schema_version=21,
        canonical_revision=13,
        observed_at=datetime(2026, 8, 18, tzinfo=UTC),
    )
    assert evidence["outcome"] == "PASSED"
    assert evidence["role_probe_count"] == evidence["security_probe_count"] == 1
    assert len(evidence["role_verification_sha256"]) == 64
    assert len(evidence["security_test_receipt_sha256"]) == 64
    assert "probes" not in evidence and "failures" not in evidence


def test_security_evidence_rejects_any_failed_probe() -> None:
    with pytest.raises(RuntimeError, match="verification failed") as captured:
        build_role_security_evidence(
            {
                "ok": False,
                "failures": [
                    {
                        "role": "mdh_mcp_reader",
                        "name": "ddl\nnot-a-log-line",
                        "sqlstate": "42501",
                        "detail": "must not be copied into the runtime log",
                    }
                ],
                "probes": [],
            },
            source_commit="a" * 40,
            master_instance_id="33333333-3333-4333-8333-333333333333",
            epoch=1,
            schema_version=1,
            canonical_revision=0,
        )
    message = str(captured.value)
    assert "mdh_mcp_reader:ddl_not-a-log-line:42501" in message
    assert "must not be copied" not in message
    assert "\n" not in message
