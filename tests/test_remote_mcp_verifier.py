from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from my_data_hub.control_plane.adapters import LedgerControlReader
from my_data_hub.control_plane.ledger import ControlLedger
from my_data_hub.mcp.oauth import AccessIdentity
from scripts.verify_remote_mcp import _structured_status


def test_remote_status_prefers_structured_result() -> None:
    result = SimpleNamespace(
        structuredContent={"deployed_commit": "a" * 40},
        content=[SimpleNamespace(text='{"deployed_commit":"wrong"}')],
    )
    assert _structured_status(result) == {"deployed_commit": "a" * 40}


def test_remote_status_accepts_json_text_fallback_and_rejects_missing() -> None:
    result = SimpleNamespace(
        structuredContent=None,
        content=[SimpleNamespace(text='{"deployed_commit":"' + "b" * 40 + '"}')],
    )
    assert _structured_status(result)["deployed_commit"] == "b" * 40
    with pytest.raises(RuntimeError, match="structured JSON"):
        _structured_status(SimpleNamespace(structuredContent=None, content=[]))


def test_control_status_binds_exact_deployed_commit(tmp_path: Path) -> None:
    reader = LedgerControlReader(
        ControlLedger(tmp_path / "control" / "ledger.sqlite3"),
        deployed_commit="c" * 40,
    )
    identity = AccessIdentity(
        issuer="https://issuer.example",
        subject="reader",
        audience="https://mcp.example/mcp",
        resource="https://mcp.example/mcp",
        client_id="reader-client",
        scopes=frozenset({"platform:read"}),
        token_id="token-1",
        expires_at=2_000_000_000,
        issued_at=1_900_000_000,
    )
    assert reader.invoke_control("platform.status", {}, identity)["deployed_commit"] == "c" * 40
    with pytest.raises(ValueError, match="exact lowercase Git SHA"):
        LedgerControlReader(reader.ledger, deployed_commit="not-a-sha")
