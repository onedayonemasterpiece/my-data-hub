from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_old_same_host_path_is_only_a_deprecation_guard() -> None:
    installer = (ROOT / "deploy/same-host/install.sh").read_text(encoding="utf-8")
    assert "INSTALL_MY_DATA_HUB_SAME_HOST" in installer
    assert "exit 78" in installer
    for forbidden in ("docker compose", "db migrate", "systemctl", "postgres.env", "pg_dump"):
        assert forbidden not in installer


def test_control_plane_installer_targets_only_database_free_compose() -> None:
    installer = (ROOT / "deploy/control-plane/install.sh").read_text(encoding="utf-8")
    assert "compose.control-plane.yaml" in installer
    assert "my-data-hub-control-plane.service" in installer
    assert "master_state=ABSENT" in installer
    for forbidden in ("db migrate", "postgres.env", "backup-loop", "connector-committer"):
        assert forbidden not in installer
