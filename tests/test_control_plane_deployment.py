from __future__ import annotations

import subprocess
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
INSTALLER = ROOT / "deploy/control-plane/install.sh"
COMPOSE = ROOT / "compose.control-plane.yaml"


def installer_source() -> str:
    return INSTALLER.read_text(encoding="utf-8")


def test_deployment_tokens_are_explicit_and_legacy_token_remains_forbidden() -> None:
    invalid = subprocess.run(
        ["bash", str(INSTALLER), "NOT_AN_INSTALL_GATE"],
        text=True,
        capture_output=True,
        check=False,
    )
    assert invalid.returncode == 2
    assert "PREPARE_CONTROL_PLANE|INSTALL_MY_DATA_HUB_CONTROL_PLANE" in invalid.stderr

    legacy = subprocess.run(
        ["bash", str(INSTALLER), "INSTALL_MY_DATA_HUB_SAME_HOST"],
        text=True,
        capture_output=True,
        check=False,
    )
    assert legacy.returncode == 78
    assert "no local database" in legacy.stderr


def test_prepare_gate_cannot_start_or_enable_runtime() -> None:
    source = installer_source()
    prepare_exit = source.index('if [[ "$action" == "PREPARE_CONTROL_PLANE" ]]')
    prefix = source[:prepare_exit]
    assert '"$docker_path" build' in prefix
    for forbidden in (
        "systemctl --user",
        "docker compose",
        "curl --fail",
        'mv -Tf "$next_link" "$current"',
    ):
        assert forbidden not in prefix
    prepare_block = source[prepare_exit : source.index("for command_name in curl", prepare_exit)]
    assert "runtime=unchanged" in prepare_block
    assert "autostart=unchanged" in prepare_block
    assert "exit 0" in prepare_block


def test_install_unit_reconciles_all_opted_in_processes_across_failure_and_reboot() -> None:
    source = installer_source()
    assert "MY_DATA_HUB_APPROVED_CONTROL_COMMIT" in source
    assert "--profile remote-mcp" in source
    exact_services = "control-plane remote-mcp oauth-server"
    assert source.count(exact_services) >= 2
    assert "Type=simple" in source
    assert "Restart=on-failure" in source
    assert "WantedBy=default.target" in source
    assert "loginctl show-user" in source and "Linger" in source
    assert "systemctl --user enable my-data-hub-control-plane.service" in source
    assert "systemctl --user restart my-data-hub-control-plane.service" in source
    exec_start = next(line for line in source.splitlines() if line.startswith("ExecStart="))
    assert " up --remove-orphans " in exec_start
    assert " -d " not in exec_start
    for port in (8080, 8765, 8780):
        assert f"http://127.0.0.1:{port}" in source


def test_compose_has_exact_opt_in_profile_split_secret_boundaries_and_loopback_ports() -> None:
    compose = yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))
    assert compose["x-my-data-hub-opt-in-profile"] == "remote-mcp"
    services = compose["services"]
    assert set(services) == {"control-plane", "remote-mcp", "oauth-server"}
    assert services["remote-mcp"]["profiles"] == ["remote-mcp"]
    assert services["oauth-server"]["profiles"] == ["remote-mcp"]
    assert services["remote-mcp"]["depends_on"] == {
        "control-plane": {"condition": "service_healthy"},
        "oauth-server": {"condition": "service_healthy"},
    }
    assert services["oauth-server"]["depends_on"] == {
        "control-plane": {"condition": "service_healthy"}
    }

    env_files = {
        name: service["env_file"][0]["path"] for name, service in services.items()
    }
    assert len(set(env_files.values())) == 3
    assert all(service["env_file"][0]["required"] is True for service in services.values())
    assert "provider.env" in env_files["control-plane"]
    assert "mcp-reader.env" in env_files["remote-mcp"]
    assert "oauth.env" in env_files["oauth-server"]

    for service in services.values():
        assert service["restart"] == "unless-stopped"
        assert all(binding.startswith("127.0.0.1:") for binding in service["ports"])
    serialized = COMPOSE.read_text(encoding="utf-8").casefold()
    for forbidden in ("pgdata", "pg_dump", "db migrate", "connector-committer"):
        assert forbidden not in serialized


def test_install_requires_private_split_inputs_without_static_master_credentials() -> None:
    source = installer_source()
    for variable in (
        "MY_DATA_HUB_CONTROL_PROVIDER_ENV_FILE",
        "MY_DATA_HUB_MCP_ENV_FILE",
        "MY_DATA_HUB_OAUTH_ENV_FILE",
        "MY_DATA_HUB_OAUTH_SIGNING_KEY_FILE",
        "MY_DATA_HUB_OAUTH_OVERLAP_JWKS_FILE",
        "MY_DATA_HUB_MASTER_TLS_CA_FILE",
    ):
        assert variable in source
    assert "require_private_file \"$provider_env\"" in source
    assert "require_private_file \"$mcp_env\"" in source
    assert "require_private_file \"$oauth_env\"" in source
    assert "require_private_file \"$oauth_key\"" in source
    assert "require_regular_file \"$oauth_overlap_jwks\"" in source
    assert "reject_data_plane_environment" in source
    assert "external DNS" not in source
    assert "yc " not in source.casefold()
    assert "vpn" not in source.casefold()
