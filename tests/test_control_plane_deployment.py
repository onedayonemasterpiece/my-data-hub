from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
INSTALLER = ROOT / "deploy/control-plane/install.sh"
COMPOSE = ROOT / "compose.control-plane.yaml"
DOCKERFILE = ROOT / "deploy/control-plane/Dockerfile"


def installer_source() -> str:
    return INSTALLER.read_text(encoding="utf-8")


def test_control_plane_image_includes_all_wheel_force_include_roots() -> None:
    source = DOCKERFILE.read_text(encoding="utf-8")
    assert "COPY sql ./sql" in source
    assert "COPY schemas ./schemas" in source
    assert "install -d -o mdh -g mdh -m 0711 /state" in source


def test_voice_v2_media_tools_and_private_spool_are_bounded_to_control_plane() -> None:
    dockerfile = DOCKERFILE.read_text(encoding="utf-8")
    assert "apt-get install --yes --no-install-recommends ffmpeg" in dockerfile
    assert "command -v ffmpeg >/dev/null" in dockerfile
    assert "command -v ffprobe >/dev/null" in dockerfile

    compose = yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))
    services = compose["services"]
    control = services["control-plane"]
    spool_binding = "${MY_DATA_HUB_VOICE_V2_SPOOL_DIR:-./.state/voice-intake-v2}:/voice-intake-v2"
    assert control["read_only"] is True
    assert control["environment"]["MY_DATA_HUB_VOICE_INTAKE_V2_SPOOL"] == "/voice-intake-v2"
    assert spool_binding in control["volumes"]
    for name, service in services.items():
        if name != "control-plane":
            assert not any("voice-intake-v2" in volume for volume in service.get("volumes", []))

    installer = installer_source()
    assert 'voice_v2_spool_dir="${MY_DATA_HUB_VOICE_V2_SPOOL_DIR:-$runtime_root/voice-intake-v2}"' in installer
    assert 'mkdir -p "$env_root" "$secret_root" "$ledger_dir" "$voice_v2_spool_dir"' in installer
    assert 'private_dirs=("$env_root" "$secret_root" "$ledger_dir" "$voice_v2_spool_dir")' in installer
    assert '[[ ! -L "$private_dir" ]]' in installer
    assert '"$(stat -c \'%u\' "$private_dir")" != "$runtime_uid"' in installer
    assert '"$(stat -c \'%a\' "$private_dir")" != "700"' in installer
    assert "MY_DATA_HUB_VOICE_V2_SPOOL_DIR=$voice_v2_spool_dir" in installer


def provider_oauth_client_probe_source() -> str:
    source = installer_source()
    marker = 'provider_oauth_client_id="$(python3 - "$oauth_env" <<\'PY\'\n'
    start = source.index(marker) + len(marker)
    return source[start : source.index("\nPY\n)", start)]


def run_provider_oauth_client_probe(tmp_path: Path, clients: list[object]) -> subprocess.CompletedProcess[str]:
    oauth_env = tmp_path / "oauth.env"
    oauth_env.write_text(
        f"MY_DATA_HUB_OAUTH_CLIENTS_JSON='{json.dumps(clients)}'\n",
        encoding="utf-8",
    )
    return subprocess.run(
        ["python3", "-", str(oauth_env)],
        input=provider_oauth_client_probe_source(),
        text=True,
        capture_output=True,
        check=False,
    )


def unified_oauth_client_probe_source() -> str:
    source = installer_source()
    marker = 'unified_oauth_client_id="$(python3 - "$oauth_env" <<\'PY\'\n'
    start = source.index(marker) + len(marker)
    return source[start : source.index("\nPY\n)", start)]


def run_unified_oauth_client_probe(tmp_path: Path, clients: list[object]) -> subprocess.CompletedProcess[str]:
    oauth_env = tmp_path / "oauth.env"
    oauth_env.write_text(
        f"MY_DATA_HUB_OAUTH_CLIENTS_JSON='{json.dumps(clients)}'\n",
        encoding="utf-8",
    )
    return subprocess.run(
        ["python3", "-", str(oauth_env)],
        input=unified_oauth_client_probe_source(),
        text=True,
        capture_output=True,
        check=False,
    )


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
    assert "INSTALL_MY_DATA_HUB_PROVIDER_MCP" in installer_source()


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


def test_connector_runtime_install_is_explicit_default_off_and_restart_managed() -> None:
    source = installer_source()
    compose = yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))
    assert compose["services"]["control-plane"]["environment"]["MY_DATA_HUB_CONNECTOR_RUNTIME_ENABLED"] == "false"
    assert "I_ACKNOWLEDGE_CONNECTOR_CANONICAL_WRITES" in source
    assert 'require_private_file "$connector_env" "connector environment"' in source
    assert 'reject_data_plane_environment "$connector_env"' in source
    assert 'MY_DATA_HUB_CONNECTOR_RUNTIME_ENABLED: "true"' in source
    assert 'connector_profile_arg=" --profile connectors"' in source
    assert 'connector_service=" connector-intake"' in source
    assert "wait_http connector-intake http://127.0.0.1:8081/health/ready" in source


def test_compose_has_exact_opt_in_profile_split_secret_boundaries_and_loopback_ports() -> None:
    compose = yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))
    assert compose["x-my-data-hub-opt-in-profile"] == "remote-mcp"
    services = compose["services"]
    assert set(services) == {
        "connector-intake",
        "control-plane",
        "remote-mcp",
        "oauth-server",
    }
    connector = services["connector-intake"]
    assert connector["profiles"] == ["connectors"]
    assert connector["entrypoint"] == [
        "python",
        "-m",
        "my_data_hub.api.connector_runtime",
    ]
    assert connector["depends_on"] == {"control-plane": {"condition": "service_healthy"}}
    assert services["remote-mcp"]["profiles"] == ["remote-mcp"]
    assert services["oauth-server"]["profiles"] == ["remote-mcp"]
    assert services["remote-mcp"]["depends_on"] == {
        "control-plane": {"condition": "service_healthy"},
        "oauth-server": {"condition": "service_healthy"},
    }
    assert services["remote-mcp"]["network_mode"] == "host"
    assert services["remote-mcp"]["extra_hosts"] == [
        "master-tunnel.internal:127.0.0.1"
    ]
    assert services["remote-mcp"]["environment"]["MY_DATA_HUB_MCP_HOST"] == "127.0.0.1"
    assert "ports" not in services["remote-mcp"]
    assert services["oauth-server"]["depends_on"] == {"control-plane": {"condition": "service_healthy"}}

    env_files = {name: service["env_file"][0]["path"] for name, service in services.items()}
    assert len(set(env_files.values())) == 4
    assert all(service["env_file"][0]["required"] is True for service in services.values())
    assert "provider.env" in env_files["control-plane"]
    assert "mcp-reader.env" in env_files["remote-mcp"]
    assert "oauth.env" in env_files["oauth-server"]
    assert "connectors.env" in env_files["connector-intake"]
    assert set(connector["volumes"]) == {
        "${MY_DATA_HUB_CONTROL_LEDGER_DIR:-./.state/control-ledger}:/ledger",
        "${MY_DATA_HUB_MASTER_SESSION_DIR:-./.state/master-sessions}:/sessions:ro",
        "${MY_DATA_HUB_MASTER_TLS_DIR:-./.state/master-tls}:/state/master-tls:ro",
    }
    assert connector["extra_hosts"] == ["master-tunnel.internal:host-gateway"]
    assert not any("DATABASE_URL" in name or name.startswith("PG") for name in connector["environment"])
    oauth = services["oauth-server"]
    assert oauth["environment"]["MY_DATA_HUB_OWNER_AUTH_MODE"] == "local_token"
    assert oauth["environment"]["MY_DATA_HUB_OWNER_OPERATOR_TOKEN_FILE"] == (
        "/run/secrets/owner-operator.token"
    )
    assert oauth["environment"]["MY_DATA_HUB_OWNER_PORTAL_STATE_KEY_FILE"] == ("/run/secrets/owner-portal-state.key")
    assert all(":ro" in binding for binding in oauth["volumes"] if "/run/secrets/" in binding)

    for name, service in services.items():
        assert service["restart"] == "unless-stopped"
        assert service["logging"] == {
            "driver": "json-file",
            "options": {"max-size": "10m", "max-file": "5"},
        }
        if name != "remote-mcp":
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
        "MY_DATA_HUB_OWNER_OPERATOR_TOKEN_FILE",
        "MY_DATA_HUB_OWNER_PORTAL_STATE_KEY_FILE",
        "MY_DATA_HUB_MASTER_TLS_DIR",
    ):
        assert variable in source
    assert 'require_private_file "$provider_env"' in source
    assert 'require_private_file "$mcp_env"' in source
    assert 'require_private_file "$oauth_env"' in source
    assert 'require_private_file "$oauth_key"' in source
    assert 'require_private_file "$owner_operator_token"' in source
    assert "secrets.token_urlsafe(48)" in source
    assert "owner operator token must contain 32..1024 bytes" in source
    assert 'require_private_file "$owner_portal_state_key"' in source
    assert "owner portal state key must be exactly 32 bytes" in source
    assert 'require_regular_file "$oauth_overlap_jwks"' in source
    assert 'verify_master_assets.py"' in source
    assert '--bundle "$asset_dir" --expected-commit "$commit"' in source
    assert "reject_data_plane_environment" in source
    assert "external DNS" not in source
    assert "yc " not in source.casefold()
    assert "vpn" not in source.casefold()


def test_operator_profile_keeps_kaggle_authority_only_in_control_process() -> None:
    source = installer_source()
    start = source.index('cat > "$operator_override"')
    end = source.index('chmod 600 "$operator_override"', start)
    operator_override = source[start:end]
    assert "MY_DATA_HUB_MCP_OPERATOR_PROVIDER_ENV_FILE" not in source
    assert 'MY_DATA_HUB_MCP_PROVIDER_GATEWAY_ENABLED: "true"' in source
    assert "MY_DATA_HUB_MCP_CONTROL_GATEWAY_URL" in source
    assert operator_override.count("mcp-control-gateway.token:ro") == 2
    assert "kaggle_token_count=" in source
    assert "kaggle_username_count=" in source
    assert "kaggle_key_count=" in source
    assert "access token OR one legacy username/key pair" in source

    compose = yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))
    assert compose["services"]["control-plane"]["environment"]["MY_DATA_HUB_MCP_PROVIDER_GATEWAY_ENABLED"] == "false"
    assert "KAGGLE_API_TOKEN" not in json.dumps(compose["services"]["remote-mcp"])


def test_operator_profile_keeps_chatgpt_cimd_with_exact_operator_scopes() -> None:
    source = installer_source()
    start = source.index('cat > "$operator_override"')
    end = source.index('chmod 600 "$operator_override"', start)
    operator_override = source[start:end]
    exact_scopes = (
        "openid,offline_access,platform:read,master:read,operation:read,checkpoint:read,"
        "embedding:read,provider:read,bloggers:read,region-talk:read,data:read,master:ensure,master:rotate,"
        "recovery:request,acceptance:probe,data:write,"
        "bloggers:write,region-talk:operate,provider:write"
    )
    assert 'MY_DATA_HUB_OAUTH_CHATGPT_CIMD_ENABLED: "true"' in operator_override
    assert f"MY_DATA_HUB_OAUTH_CHATGPT_CIMD_SCOPES: {exact_scopes}" in operator_override
    assert "acceptance:operate" not in operator_override
    assert "migration:operate" not in operator_override


def test_google_youtube_operator_profile_is_separate_explicit_and_rollbackable() -> None:
    source = installer_source()
    assert "MY_DATA_HUB_ENABLE_GOOGLE_YOUTUBE_ANALYSIS" in source
    assert "I_ACKNOWLEDGE_SHARED_GOOGLE_AI_QUOTA" in source
    assert 'google_youtube_override="$runtime_root/google-youtube.$commit.yaml"' in source
    operator_start = source.index('cat > "$operator_override"')
    operator_end = source.index('chmod 600 "$operator_override"', operator_start)
    operator_override = source[operator_start:operator_end]
    assert 'MY_DATA_HUB_GOOGLE_YOUTUBE_ENABLED: "false"' in operator_override
    start = source.index('cat > "$google_youtube_override"')
    end = source.index('chmod 600 "$google_youtube_override"', start)
    youtube_override = source[start:end]
    assert 'MY_DATA_HUB_GOOGLE_YOUTUBE_ENABLED: "true"' in youtube_override
    assert "MY_DATA_HUB_MCP_SCOPES:" in youtube_override
    assert "youtube:analyze" in source
    assert "MY_DATA_HUB_OAUTH_CHATGPT_CIMD_SCOPES:" in youtube_override
    assert 'google_youtube_compose_arg=" -f $google_youtube_override"' in source
    assert source.count("$google_youtube_compose_arg") >= 3
    assert (
        "Google YouTube analysis and protected acceptance scenarios require separate operator installs"
        in source
    )


def test_google_youtube_analysis_can_use_bounded_unified_profile_without_operator_gate() -> None:
    source = installer_source()
    assert '"$operator_profile" != true && "$unified_bootstrap" != true' in source
    start = source.index('if [[ "$google_youtube_analysis" == true ]]; then')
    end = source.index('acceptance_scenarios_override=""', start)
    youtube_block = source[start:end]
    assert 'if [[ "$unified_bootstrap" == true ]]; then' in youtube_block
    bounded_scopes = (
        "platform:read,master:read,operation:read,checkpoint:read,embedding:read,provider:read,"
        "bloggers:read,region-talk:read,provider:write,youtube:analyze"
    )
    assert f'youtube_mcp_scopes="{bounded_scopes}"' in youtube_block
    assert 'youtube_oauth_scopes="openid,offline_access,$youtube_mcp_scopes"' in youtube_block
    assert "MY_DATA_HUB_MCP_SCOPES: $youtube_mcp_scopes" in youtube_block
    assert "MY_DATA_HUB_OAUTH_CHATGPT_CIMD_SCOPES: $youtube_oauth_scopes" in youtube_block

    compose_start = source.index('compose_files=(-f "$release/compose.control-plane.yaml")')
    compose_end = source.index('compose=("$docker_path" compose', compose_start)
    compose_block = source[compose_start:compose_end]
    assert compose_block.index('if [[ -n "$unified_bootstrap_override" ]]') < compose_block.index(
        'if [[ -n "$google_youtube_override" ]]'
    )

    exec_start = next(line for line in source.splitlines() if line.startswith("ExecStart="))
    assert exec_start.index("$unified_bootstrap_compose_arg") < exec_start.index(
        "$google_youtube_compose_arg"
    )


def test_remote_mcp_host_network_uses_only_loopback_control_gateway() -> None:
    source = installer_source()
    compose = yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))
    remote = compose["services"]["remote-mcp"]
    assert remote["network_mode"] == "host"
    assert remote["environment"]["MY_DATA_HUB_MCP_HOST"] == "127.0.0.1"
    assert "ports" not in remote
    assert "http://control-plane:8080/internal/mcp-provider/invoke" not in source
    assert source.count(
        "MY_DATA_HUB_MCP_CONTROL_GATEWAY_URL: "
        "http://127.0.0.1:8080/internal/mcp-provider/invoke"
    ) == 3


def test_provider_only_mcp_action_is_explicit_and_skips_master_only_prerequisites() -> None:
    source = installer_source()
    assert "provider_only=true" in source
    assert "provider-only.$commit.yaml" in source
    assert 'MY_DATA_HUB_PROVIDER_ONLY_MODE: "true"' in source
    assert 'MY_DATA_HUB_MCP_PROVIDER_PROFILE_ENABLED: "true"' in source
    assert "MY_DATA_HUB_MCP_SCOPES: platform:read,provider:read,provider:write" in source
    assert 'provider_only_mode="provider-only-mcp"' in source
    assert 'provider gateway token"' in source
    assert 'operator write-gate signing key"' in source
    assert source.count("MY_DATA_HUB_MCP_WRITE_GATE_SECRET_FILE: /run/secrets/mcp-write-gate.key") >= 2
    assert "provider-only control plane requires one central Kaggle credential mode" in source
    assert "provider-only readiness did not prove the central adapter gateway" in source
    assert 'MY_DATA_HUB_OAUTH_CHATGPT_CIMD_ENABLED: "true"' in source
    assert "chatgpt_oauth_client_mode=cimd-public" in source
    assert (
        "MY_DATA_HUB_OAUTH_CHATGPT_CIMD_SCOPES: "
        "openid,offline_access,platform:read,provider:read,provider:write"
    ) in source
    assert "provider_oauth_client_id=%s" in source
    assert '"openid", "offline_access", "platform:read", "provider:read", "provider:write"' in source
    assert "!override" in source

    # Compose deliberately runs as the owning host UID rather than the image's
    # baked-in uid.  The official Kaggle SDK needs a writable HOME even when
    # legacy credentials arrive only through the environment; keep that home
    # on the existing ephemeral /tmp tmpfs instead of the read-only image.
    compose = yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))
    assert compose["services"]["control-plane"]["environment"]["HOME"] == "/tmp/my-data-hub-control"

    provider_branch = source[source.index('if [[ "$provider_only" == true ]]') :]
    assert "verify_master_assets.py" not in provider_branch.split("else", 1)[0]
    assert "root-installed epoch tunnel broker socket is required" not in provider_branch.split("else", 1)[0]
    assert "operator_profile_gate.py" not in provider_branch.split("else", 1)[0]


def test_control_plane_tmpfs_can_hold_master_asset_staging_and_exact_readback() -> None:
    """The central adapter holds upload staging plus zip+tree readback concurrently."""

    compose = yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))
    assert compose["services"]["control-plane"]["tmpfs"] == ["/tmp:size=256m,mode=1777"]


def test_provider_only_probe_recognizes_exact_opencode_public_loopback_client(tmp_path: Path) -> None:
    result = run_provider_oauth_client_probe(
        tmp_path,
        [
            {
                "client_id": "bootstrap-reader",
                "redirect_uris": ["https://identity.example/bootstrap/callback"],
                "allowed_scopes": ["openid", "offline_access", "provider:read"],
            },
            {
                "client_id": "opencode-my-data-hub",
                "redirect_uris": ["http://127.0.0.1:19876/mcp/oauth/callback"],
                "allowed_scopes": [
                    "openid",
                    "offline_access",
                    "platform:read",
                    "provider:read",
                    "provider:write",
                ],
            },
        ],
    )
    assert result.returncode == 0
    assert result.stdout == "opencode-my-data-hub\n"
    assert result.stderr == ""


@pytest.mark.parametrize(
    "redirect_uri",
    [
        "http://localhost:19876/mcp/oauth/callback",
        "http://0.0.0.0:19876/mcp/oauth/callback",
        "http://127.0.0.1/mcp/oauth/callback",
        "http://127.0.0.1:019876/mcp/oauth/callback",
        "http://127.0.0.1:19876/mcp/oauth/callback#fragment",
    ],
)
def test_provider_only_probe_rejects_nonexact_http_redirect(
    tmp_path: Path, redirect_uri: str
) -> None:
    result = run_provider_oauth_client_probe(
        tmp_path,
        [
            {
                "client_id": "opencode-my-data-hub",
                "redirect_uris": [redirect_uri],
                "allowed_scopes": [
                    "openid",
                    "offline_access",
                    "platform:read",
                    "provider:read",
                    "provider:write",
                ],
            }
        ],
    )
    assert result.returncode == 0
    assert result.stdout == "\n"


def test_provider_only_override_removes_master_mounts_and_static_bearers() -> None:
    source = installer_source()
    start = source.index('cat > "$provider_only_override"')
    end = source.index('chmod 600 "$provider_only_override"', start)
    override = source[start:end]
    assert "MY_DATA_HUB_MASTER_ASSET_DIR" not in override
    assert "MY_DATA_HUB_TUNNEL_BROKER_SOCKET_DIR" not in override
    assert "MY_DATA_HUB_CHECKPOINT_UPLOAD_BROKER_KEY_FILE" not in override
    assert "connector-intake" not in override
    assert "acceptance:operate" not in override
    for forbidden in (
        "MY_DATA_HUB_MCP_CANARY_TOKEN",
        "MY_DATA_HUB_MCP_ACCEPTANCE_OPERATOR_TOKEN",
        "MY_DATA_HUB_MCP_PROVIDER_OPERATOR_TOKEN",
        "KAGGLE_API_TOKEN",
        "KAGGLE_USERNAME",
        "KAGGLE_KEY",
    ):
        assert forbidden not in override
    control, remote = override.split("  remote-mcp:", 1)
    assert 'MY_DATA_HUB_PROVIDER_UPLOAD_ROOT: /uploads' in control
    assert 'MY_DATA_HUB_PROVIDER_UPLOAD_DIR:?provider upload directory is required}:/uploads' in control
    assert "/uploads" not in remote


def test_unified_bootstrap_action_combines_master_runtime_provider_and_bounded_reads() -> None:
    source = installer_source()
    assert "INSTALL_MY_DATA_HUB_UNIFIED_BOOTSTRAP" in source
    start = source.index('cat > "$unified_bootstrap_override"')
    end = source.index('chmod 600 "$unified_bootstrap_override"', start)
    override = source[start:end]
    exact_scopes = (
        "platform:read,master:read,operation:read,checkpoint:read,embedding:read,"
        "provider:read,bloggers:read,region-talk:read,provider:write"
    )
    assert 'MY_DATA_HUB_UNIFIED_BOOTSTRAP_MODE: "true"' in override
    assert 'MY_DATA_HUB_MCP_OPERATOR_CREDENTIALS_ENABLED: "false"' in override
    assert 'MY_DATA_HUB_MCP_UNIFIED_BOOTSTRAP_PROFILE_ENABLED: "true"' in override
    assert f"MY_DATA_HUB_MCP_SCOPES: {exact_scopes}" in override
    assert "data:write" not in override
    assert "master:ensure" not in override
    assert "acceptance:operate" not in override
    assert "migration:operate" not in override
    assert (
        "MY_DATA_HUB_OAUTH_CHATGPT_CIMD_SCOPES: openid,offline_access," + exact_scopes
    ) in override
    assert 'unified_bootstrap_compose_arg=" -f $unified_bootstrap_override"' in source


def test_unified_bootstrap_opencode_client_requires_exact_scopes_and_loopback(tmp_path: Path) -> None:
    exact = [
        "openid",
        "offline_access",
        "platform:read",
        "master:read",
        "operation:read",
        "checkpoint:read",
        "embedding:read",
        "provider:read",
        "provider:write",
        "bloggers:read",
        "region-talk:read",
    ]
    valid = run_unified_oauth_client_probe(
        tmp_path,
        [{
            "client_id": "opencode-my-data-hub-unified",
            "redirect_uris": ["http://127.0.0.1:19876/mcp/oauth/callback"],
            "allowed_scopes": exact,
        }],
    )
    assert valid.returncode == 0
    assert valid.stdout == "opencode-my-data-hub-unified\n"

    extra = run_unified_oauth_client_probe(
        tmp_path,
        [{
            "client_id": "unsafe-overbroad",
            "redirect_uris": ["http://127.0.0.1:19876/mcp/oauth/callback"],
            "allowed_scopes": [*exact, "data:write"],
        }],
    )
    assert extra.returncode == 0
    assert extra.stdout == "\n"


def test_unified_bootstrap_readiness_precedes_release_commit_and_is_rollback_guarded() -> None:
    source = installer_source()
    readiness = source.index("unified bootstrap readiness did not prove both master runtime and provider gateway")
    release_commit = source.index('mv -Tf "$next_link" "$current"')
    trap_removed = source.index("trap - ERR", release_commit)
    rollback = source.index("rollback()")
    trap_installed = source.index("trap rollback ERR")
    assert rollback < trap_installed < readiness < release_commit < trap_removed
    for required in (
        'receipt.get("master_runtime_ready") is True',
        'receipt.get("master_provider_status") == "available"',
        'receipt.get("provider_gateway_ready") is True',
    ):
        assert required in source


def test_chunked_upload_staging_is_private_and_mounted_only_into_central_control() -> None:
    source = installer_source()
    assert 'provider_upload_dir="${MY_DATA_HUB_PROVIDER_UPLOAD_DIR:-$runtime_root/provider-uploads}"' in source
    assert 'private_dirs+=("$provider_upload_dir")' in source
    assert "MY_DATA_HUB_PROVIDER_UPLOAD_DIR=$provider_upload_dir" in source
    start = source.index('cat > "$operator_override"')
    end = source.index('chmod 600 "$operator_override"', start)
    operator_override = source[start:end]
    control, remote = operator_override.split("  remote-mcp:", 1)
    assert 'MY_DATA_HUB_PROVIDER_UPLOAD_ROOT: /uploads' in control
    assert 'MY_DATA_HUB_PROVIDER_UPLOAD_DIR:?provider upload directory is required}:/uploads' in control
    assert "/uploads" not in remote


def test_fm08_host_supervisor_is_explicit_private_and_has_one_restart_target() -> None:
    source = installer_source()
    assert "I_ACKNOWLEDGE_TASK_BOUND_CONTROL_RESTART" in source
    assert "acceptance_supervisor=false" in source
    assert "acceptance supervisor requires operator install" in source
    assert "my-data-hub-acceptance-supervisor.service" in source
    assert "acceptance_supervisor.py --socket" in source
    assert "--allowed-uid $(id -u)" in source
    assert 'chmod 700 "$acceptance_socket_dir"' in source
    assert 'require_private_file "$acceptance_key"' in source
    assert "control.sock" in source
    assert "/run/mdh-acceptance:ro" in source
    assert "restart --no-deps control-plane" not in source  # fixed inside the host implementation
    assert "docker.sock" not in source
    assert "MY_DATA_HUB_ACCEPTANCE_SUPERVISOR_SOCKET" not in COMPOSE.read_text()

    supervisor = (ROOT / "src/my_data_hub/control_plane/acceptance_supervisor.py").read_text(encoding="utf-8")
    assert 'result.extend(("restart", "--no-deps", "control-plane"))' in supervisor
    for forbidden in ('"restart", "remote-mcp"', '"restart", "oauth-server"', "shell=True"):
        assert forbidden not in supervisor


def test_acceptance_scenarios_are_owner_opt_in_and_use_provider_status_input() -> None:
    source = installer_source()
    assert "I_ACKNOWLEDGE_PROTECTED_ACCEPTANCE_EFFECTS" in source
    assert "acceptance scenarios require operator install" in source
    assert 'MY_DATA_HUB_MCP_ACCEPTANCE_SCENARIOS_ENABLED: "true"' in source
    assert "/run/mdh-checkpoint-acceptance/deployment.json:ro" in source
    assert "acceptance:operate" in source
    assert '"runtime_root_secret_name" in value' in source
    assert "callback roots may not be provisioned as Kaggle User Secrets" in source
    assert '"kaggle_secret_bindings" in value' in source
    assert "checkpoint acceptance Kaggle credentials are forbidden in the Notebook" in source
    assert 'value.get("brokered_checkpoint_upload") is not True' in source
    start = source.index('cat > "$acceptance_scenarios_override"')
    end = source.index('chmod 600 "$acceptance_scenarios_override"', start)
    acceptance_override = source[start:end]
    assert 'MY_DATA_HUB_MCP_ACCEPTANCE_SCENARIOS_ENABLED: "false"' not in acceptance_override
    assert "MY_DATA_HUB_MCP_SCOPES:" in acceptance_override
    assert "MY_DATA_HUB_OAUTH_CHATGPT_CIMD_SCOPES:" in acceptance_override
    assert "acceptance:operate" in acceptance_override
    assert "MY_DATA_HUB_MCP_ACCEPTANCE_SCENARIOS_ENABLED" not in COMPOSE.read_text()


def test_prepare_checks_bounded_disk_headroom_before_building_release_image() -> None:
    source = installer_source()
    disk_check = source.index('minimum_free_kib="${MY_DATA_HUB_CONTROL_MIN_FREE_KIB:-4194304}"')
    image_build = source.index('"$docker_path" build')
    assert disk_check < image_build
    assert 'df -Pk "$disk_path"' in source
    assert "available_kib < minimum_free_kib" in source
