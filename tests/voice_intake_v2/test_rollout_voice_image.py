import json
import runpy
from pathlib import Path

import pytest

# run_path executes this standalone helper without generating deployment-tree bytecode.
mod = runpy.run_path(str(Path(__file__).resolve().parents[2] / "deploy/control-plane/rollout_voice_image.py"))
rollout = mod["rollout"]
Error = mod["RolloutError"]
START = (
    "/usr/bin/docker compose --env-file /state/existing.env --profile remote-mcp "
    "--project-directory /release/base -f /release/base/compose.yaml -f /state/provider.yaml -f "
    "/state/voice-recovery.yaml up --remove-orphans control-plane remote-mcp oauth-server "
    "showcase-runtime"
)
OLD = "sha256:" + "1" * 64
NEW = "sha256:" + "2" * 64
TAG = "my-data-hub-control-plane:" + "a" * 40


class Host:
    def __init__(self, dropin, fail_health=False):
        self.dropin = dropin
        self.calls = []
        self.image = OLD
        self.fail_health = fail_health

    def __call__(self, args):
        self.calls.append(args)
        if args[:3] == ["systemctl", "--user", "show"]:
            if "--property=FragmentPath" in args:
                return "/home/dev/.config/systemd/user/my-data-hub-control-plane.service"
            if "--property=DropInPaths" in args:
                return "/home/dev/.config/systemd/user/my-data-hub-control-plane.service.d/90-recovery.conf"
            start = START
            if self.dropin.exists():
                start = [
                    x[10:] for x in self.dropin.read_text().splitlines() if x.startswith("ExecStart=") and len(x) > 10
                ][-1]
            if "--property=ExecReload" in args:
                start = start.replace("up --remove-orphans", "up -d --wait --remove-orphans")
            return (
                "{ path=/usr/bin/docker ; argv[]="
                + start
                + " ; ignore_errors=no ; start_time=[n/a] ; stop_time=[n/a] ; pid=0 ; code=(null) ; status=0/0 }"
            )
        if args[:3] == ["systemctl", "--user", "is-active"]:
            return "active"
        if args[:3] == ["systemctl", "--user", "daemon-reload"]:
            return ""
        if args[:3] == ["/usr/bin/docker", "image", "inspect"]:
            return json.dumps(NEW)
        if args[:2] == ["/usr/bin/docker", "inspect"]:
            assert "{{json .}}" not in args[3] and ".Config.Env" not in args[3]
            cp = args[-1] == "cp"
            return json.dumps(
                dict(
                    Id="cp" if cp else "other",
                    Image=self.image if cp else OLD,
                    Service="control-plane" if cp else "remote-mcp",
                    Revision="a" * 40 if self.image == NEW and cp else "old",
                    State=dict(
                        Running=True,
                        StartedAt="fixed",
                        Health=dict(Status="unhealthy" if self.fail_health and self.image == NEW and cp else "healthy"),
                    ),
                )
            )
        if "ps" in args:
            return "cp" if args[-1] == "control-plane" else "cp\nother"
        if "up" in args:
            assert args[-4:] == ["up", "-d", "--no-deps", "control-plane"]
            self.image = NEW if str(self.dropin.parent / "overlay.json") in args else OLD
            return ""
        raise AssertionError(args)


def kwargs(tmp_path, host):
    return dict(
        expected_start=START,
        expected_image=OLD,
        target_image=TAG,
        dropin=host.dropin,
        override=tmp_path / "overlay.json",
        run=host,
        timeout=0,
    )


def test_dry_run_no_writes_or_mutations(tmp_path):
    host = Host(tmp_path / "unit.conf")
    result = rollout(**kwargs(tmp_path, host))
    assert result["state"] == "dry_run" and not list(tmp_path.iterdir())
    assert all("up" not in x and "daemon-reload" not in x for x in host.calls)
    assert "/state/voice-recovery.yaml" in result["command"]


def test_only_control_plane_and_persistent_overlay(tmp_path):
    host = Host(tmp_path / "unit.conf")
    result = rollout(**kwargs(tmp_path, host), apply=True)
    assert result["state"] == "healthy" and host.image == NEW
    content = host.dropin.read_text()
    assert "ExecStop" not in content and "ExecReload=" in content
    overlay = json.loads((tmp_path / "overlay.json").read_text())
    assert list(overlay["services"]) == ["control-plane"]
    assert "environment" not in overlay["services"]["control-plane"]
    assert all("restart" not in x and "down" not in x for x in host.calls)


def test_health_failure_restores_old_chain_and_image(tmp_path):
    host = Host(tmp_path / "unit.conf", fail_health=True)
    with pytest.raises(Error, match="prior startup and image restored"):
        rollout(**kwargs(tmp_path, host), apply=True)
    assert host.image == OLD and "overlay.json.rollback.json" in host.dropin.read_text()
    assert all("restart" not in x and "down" not in x for x in host.calls)


@pytest.mark.parametrize(
    "field,value", [("expected_start", START + " extra"), ("expected_image", "sha256:" + "3" * 64)]
)
def test_guard_failure_no_writes(tmp_path, field, value):
    host = Host(tmp_path / "unit.conf")
    args = kwargs(tmp_path, host)
    args[field] = value
    with pytest.raises(Error):
        rollout(**args, apply=True)
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize(
    "value", ["/bin/sh -c true", "/usr/bin/docker compose -f relative up control-plane", START + " $SECRET"]
)
def test_reject_unsupported_command(value):
    with pytest.raises(Error):
        mod["compose_parts"](value.split())


def test_optional_approved_config_mount_is_private_and_read_only(tmp_path):
    host = Host(tmp_path / "unit.conf")
    config = tmp_path / "approved.json"
    config.write_text('{"token":"fixture-only"}')
    config.chmod(0o600)
    value = rollout(**kwargs(tmp_path, host), apply=True, voice_config_file=config)
    overlay = json.loads((tmp_path / "overlay.json").read_text())["services"]["control-plane"]
    assert overlay["environment"] == {"MY_DATA_HUB_VOICE_ANALYSIS_CONFIG_FILE": "/run/secrets/voice-analysis.json"}
    assert overlay["volumes"] == [str(config) + ":/run/secrets/voice-analysis.json:ro"]
    assert "fixture-only" not in json.dumps(value) and "fixture-only" not in json.dumps(overlay)


def test_config_with_wide_permissions_rejected_before_writes(tmp_path):
    host = Host(tmp_path / "unit.conf")
    config = tmp_path / "approved.json"
    config.write_text("{}")
    config.chmod(0o644)
    with pytest.raises(Error, match="permissions"):
        rollout(**kwargs(tmp_path, host), voice_config_file=config, apply=True)
    assert not host.dropin.exists()


def test_broad_installer_guard_precedes_mutation():
    path = Path(__file__).resolve().parents[2] / "deploy/control-plane/install.sh"
    text = path.read_text()
    assert text.index("managed voice image override exists") < text.index("mkdir -p")
    assert '"$action" != "PREPARE_CONTROL_PLANE" && ( -e "$voice_image_dropin" || -e "$voice_analysis_dropin" )' in text
    assert "60-voice-image.conf" in text and "z-voice-analysis-image.conf" in text


def test_rollback_pins_original_id_even_when_original_tag_moves(tmp_path):
    class MovedTagHost(Host):
        def __call__(self, args):
            if "up" in args:
                self.calls.append(args)
                assert args[-4:] == ["up", "-d", "--no-deps", "control-plane"]
                files = [Path(args[i + 1]) for i, x in enumerate(args) if x == "-f"]
                last = files[-1]
                if last.exists():
                    self.image = json.loads(last.read_text())["services"]["control-plane"]["image"]
                else:
                    self.image = "sha256:" + "9" * 64  # old tag no longer resolves to OLD
                return ""
            return super().__call__(args)

    host = MovedTagHost(tmp_path / "unit.conf", fail_health=True)
    # Existing unrelated unit settings must survive rollback.
    host.dropin.write_text("[Service]\nTimeoutStartSec=77\nExecStart=\nExecStart=" + START + "\n")
    with pytest.raises(Error, match="prior startup and image restored"):
        rollout(**kwargs(tmp_path, host), apply=True)
    assert host.image == OLD
    rollback = json.loads((tmp_path / "overlay.json.rollback.json").read_text())
    assert rollback == {"services": {"control-plane": {"image": OLD}}}
    assert "TimeoutStartSec=77" in host.dropin.read_text()
    assert "/state/voice-recovery.yaml" in host.dropin.read_text()
    # A subsequent service start still selects the captured ID, not the moved tag.
    start = [x[10:] for x in host.dropin.read_text().splitlines() if x.startswith("ExecStart=") and len(x) > 10][-1]
    prefix, _ = mod["compose_parts"](start.split())
    host([*prefix, "up", "-d", "--no-deps", "control-plane"])
    assert host.image == OLD


def test_inspect_only_returns_nonsecret_start_and_container_identity(tmp_path):
    host = Host(tmp_path / "unit.conf")
    result = mod["inspect_current"](host)
    assert result["state"] == "inspection" and result["exec_start"] == START
    assert result["containers"]["control-plane"]["image"] == OLD
    assert result["containers"]["control-plane"]["id"] == "cp"
    assert result["service_state"] == "active"
    assert not list(tmp_path.iterdir())
    assert all("up" not in x and "daemon-reload" not in x for x in host.calls)
    assert all(".Config.Env" not in str(x) for x in host.calls)


def test_inspect_cli_needs_no_deployment_args(tmp_path, monkeypatch, capsys):
    import sys

    host = Host(tmp_path / "unit.conf")
    main = mod["main"]
    monkeypatch.setitem(main.__globals__, "inspect_current", lambda: mod["inspect_current"](host))
    monkeypatch.setattr(sys, "argv", ["rollout_voice_image.py", "--inspect"])
    assert main() == 0
    result = json.loads(capsys.readouterr().out)
    assert result["exec_start"] == START
    assert not list(tmp_path.iterdir())


def test_command_sets_current_user_bus_without_inherited_environment(monkeypatch):
    from types import SimpleNamespace

    command = mod["command"]
    monkeypatch.delenv("XDG_RUNTIME_DIR", raising=False)
    monkeypatch.delenv("DBUS_SESSION_BUS_ADDRESS", raising=False)
    monkeypatch.setattr(mod["os"], "getuid", lambda: 1234)
    monkeypatch.setattr(Path, "exists", lambda p: str(p) == "/run/user/1234/bus")
    calls = []

    def run(argv, **kwargs):
        calls.append((argv, kwargs))
        return SimpleNamespace(returncode=0, stdout="active\n", stderr="")

    monkeypatch.setattr(mod["subprocess"], "run", run)
    assert command(["systemctl", "--user", "is-active", "unit"]) == "active"
    env = calls[0][1]["env"]
    assert env["XDG_RUNTIME_DIR"] == "/run/user/1234"
    assert env["DBUS_SESSION_BUS_ADDRESS"] == "unix:path=/run/user/1234/bus"
    assert env["HOME"] == str(Path.home())


def test_missing_bus_fails_before_subprocess(monkeypatch):
    monkeypatch.setattr(Path, "exists", lambda p: False)
    monkeypatch.setattr(mod["subprocess"], "run", lambda *a, **k: pytest.fail("must not execute"))
    with pytest.raises(Error, match="user-systemd bus is unavailable"):
        mod["command"](["systemctl", "--user", "show", "unit"])


def test_command_error_diagnostics_do_not_echo_stderr(monkeypatch):
    from types import SimpleNamespace

    monkeypatch.setattr(Path, "exists", lambda p: True)
    monkeypatch.setattr(
        mod["subprocess"],
        "run",
        lambda *a, **k: SimpleNamespace(returncode=1, stdout="", stderr="Failed to connect to bus: secret-value"),
    )
    with pytest.raises(Error) as error:
        mod["command"](["systemctl", "--user", "show", "unit"])
    assert "exit=1; category=user_bus_connection_failed" in str(error.value)
    assert "secret-value" not in str(error.value)


def test_inspect_digest_roundtrip_and_mismatch(tmp_path):
    import hashlib

    host = Host(tmp_path / "unit.conf")
    inspected = mod["inspect_current"](host)
    digest = hashlib.sha256(inspected["exec_start"].encode("utf-8")).hexdigest()
    args = kwargs(tmp_path, host)
    args.pop("expected_start")
    result = rollout(**args, expected_start_sha256=digest)
    assert result["state"] == "dry_run" and not list(tmp_path.iterdir())
    with pytest.raises(Error, match="ExecStart changed"):
        rollout(**args, expected_start_sha256="0" * 64, apply=True)
    assert not list(tmp_path.iterdir())


def test_exec_start_guards_are_exclusive(tmp_path):
    host = Host(tmp_path / "unit.conf")
    with pytest.raises(Error, match="exactly one"):
        rollout(**kwargs(tmp_path, host), expected_start_sha256="0" * 64)


def test_ineffective_dropin_rejected_before_container_change(tmp_path):
    class ShadowedHost(Host):
        def __call__(self, args):
            if args[:3] == ["systemctl", "--user", "show"] and any(
                x in args for x in ("--property=ExecStart", "--property=ExecReload")
            ):
                self.calls.append(args)
                value = (
                    START.replace("up --remove-orphans", "up -d --wait --remove-orphans")
                    if "--property=ExecReload" in args
                    else START
                )
                return "{ path=/usr/bin/docker ; argv[]=" + value + " ; ignore_errors=no ; status=0/0 }"
            return super().__call__(args)

    host = ShadowedHost(tmp_path / "unit.conf")
    original = "[Service]\n# existing managed metadata\n"
    host.dropin.write_text(original)
    with pytest.raises(Error, match="before container switch"):
        rollout(**kwargs(tmp_path, host), apply=True)
    assert host.dropin.read_text() == original
    assert host.image == OLD
    assert not any("up" in call for call in host.calls)


def test_inspect_reports_only_unit_path_metadata(tmp_path):
    result = mod["inspect_current"](Host(tmp_path / "unit.conf"))
    assert result["fragment_path"].endswith("/my-data-hub-control-plane.service")
    assert result["dropin_paths"][0].endswith("/90-recovery.conf")


def test_default_dropin_follows_verified_recovery_dropin():
    source = (Path(__file__).resolve().parents[2] / "deploy/control-plane/rollout_voice_image.py").read_text()
    assert 'f"{SERVICE}.d/z-voice-analysis-image.conf"' in source
    assert "z-voice-analysis-image.conf" > "voice-recovery.conf"
