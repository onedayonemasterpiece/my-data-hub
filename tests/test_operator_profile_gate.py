from __future__ import annotations

import json
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/operator_profile_gate.py"
COMMIT = "a" * 40


def _issue(tmp_path: Path) -> tuple[Path, Path]:
    key = tmp_path / "write-gate.key"
    key.write_bytes(b"operator-gate-test-key-material-32-bytes")
    key.chmod(0o600)
    receipt = tmp_path / "receipt.json"
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "issue",
            "--commit",
            COMMIT,
            "--checkpoint-id",
            str(uuid4()),
            "--checkpoint-revision",
            "41",
            "--role-verification-sha256",
            "b" * 64,
            "--security-test-receipt-sha256",
            "c" * 64,
            "--expires-at",
            (datetime.now(UTC) + timedelta(hours=1)).isoformat(),
            "--signing-key-file",
            str(key),
            "--output",
            str(receipt),
        ],
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    return key, receipt


def test_operator_gate_binds_exact_commit_security_receipts_and_signature(tmp_path: Path) -> None:
    key, receipt = _issue(tmp_path)
    assert receipt.stat().st_mode & 0o077 == 0
    verified = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "verify",
            "--commit",
            COMMIT,
            "--receipt",
            str(receipt),
            "--signing-key-file",
            str(key),
        ],
        text=True,
        capture_output=True,
        check=False,
    )
    assert verified.returncode == 0, verified.stderr
    assert "verified_operator_gate_commit=" + COMMIT in verified.stdout

    body = json.loads(receipt.read_text())
    body["checkpoint_revision"] = 42
    receipt.write_text(json.dumps(body))
    receipt.chmod(0o600)
    rejected = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "verify",
            "--commit",
            COMMIT,
            "--receipt",
            str(receipt),
            "--signing-key-file",
            str(key),
        ],
        text=True,
        capture_output=True,
        check=False,
    )
    assert rejected.returncode != 0
    assert "signature is invalid" in rejected.stderr


def test_operator_install_is_explicit_gated_override_while_base_remains_reader_only() -> None:
    compose = (ROOT / "compose.control-plane.yaml").read_text()
    installer = (ROOT / "deploy/control-plane/install.sh").read_text()
    assert compose.count('MY_DATA_HUB_MCP_WRITE_ENABLED: "false"') == 2
    assert 'MY_DATA_HUB_MCP_OPERATOR_CREDENTIALS_ENABLED: "false"' in compose
    assert "INSTALL_MY_DATA_HUB_CONTROL_PLANE_OPERATOR" in installer
    assert "I_ACKNOWLEDGE_REMOTE_CANONICAL_WRITES" in installer
    assert 'MY_DATA_HUB_MCP_WRITE_ENABLED: "true"' in installer
    assert 'MY_DATA_HUB_MCP_OPERATOR_PROFILE_ENABLED: "true"' in installer
    assert 'MY_DATA_HUB_MCP_OPERATOR_CREDENTIALS_ENABLED: "true"' in installer
    assert "operator_profile_gate.py\" verify" in installer
    assert "only one modern KAGGLE_API_TOKEN" in installer
    assert "mcp-write-gate.key:ro" in installer
    assert "MY_DATA_HUB_DATABASE_URL" not in installer
