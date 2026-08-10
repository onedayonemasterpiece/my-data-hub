from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_same_host_compose_uses_stable_private_resources_and_split_environments() -> None:
    compose = (ROOT / "compose.same-host.yaml").read_text(encoding="utf-8")
    assert "name: my-data-hub-postgres-data" in compose
    assert '"127.0.0.1:${POSTGRES_PORT:-5432}:5432"' in compose
    assert '"127.0.0.1:${MY_DATA_HUB_API_PORT:-8080}:8080"' in compose
    assert '"127.0.0.1:${MY_DATA_HUB_MCP_PORT:-8765}:8765"' in compose
    for name in ("api", "orchestrator", "committer", "backup", "mcp"):
        assert f"/{name}.env" in compose
    assert "env_file: .env" not in compose
    assert "profiles: [remote-mcp]" in compose


def test_same_host_installer_preserves_fail_closed_gates_and_autostart() -> None:
    installer = (ROOT / "deploy/same-host/install.sh").read_text(encoding="utf-8")
    assert "MY_DATA_HUB_SCHEDULER_ENABLED=false" in installer
    assert "MY_DATA_HUB_PRODUCTION_PUBLISH_ENABLED=false" in installer
    assert "MY_DATA_HUB_MCP_WRITE_ENABLED=false" in installer
    assert "MY_DATA_HUB_MCP_REMOTE_ENABLED=false" in installer
    assert "systemctl --user enable my-data-hub-compose.service" in installer
    assert "candidate_deployment_env" in installer
    assert "failed INSTALL attempts cannot advance boot state" in installer
    assert "systemctl --user restart my-data-hub-compose.service" in installer
    assert "INSTALL_MY_DATA_HUB_SAME_HOST" in installer


def test_backup_failure_is_visible_and_freshness_is_health_checked() -> None:
    loop = (ROOT / "deploy/same-host/backup-loop.sh").read_text(encoding="utf-8")
    compose = (ROOT / "compose.same-host.yaml").read_text(encoding="utf-8")
    assert "backup_postgres.sh || true" not in loop
    assert "last-success" in loop
    assert "healthcheck:" in compose
    assert "last-success" in compose


def test_same_host_edge_routes_only_exact_mcp_and_metadata_paths() -> None:
    edge = (ROOT / "deploy/same-host/nginx-mcp-edge.conf.example").read_text(encoding="utf-8")
    assert "mcp-datahub.kenigevents.ru mcp_https" in edge
    assert "location = /mcp" in edge
    assert "location = /.well-known/oauth-protected-resource/mcp" in edge
    assert "location / { return 404; }" in edge
    assert 'proxy_set_header X-Forwarded-For ""' in edge
