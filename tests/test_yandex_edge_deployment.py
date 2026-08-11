from __future__ import annotations

import base64
import subprocess
from pathlib import Path
from types import ModuleType

import pytest
import yaml

ROOT = Path(__file__).parents[1]
EDGE = ROOT / "deploy" / "yandex-edge"


def load_renderer() -> ModuleType:
    path = EDGE / "render_cloud_init.py"
    module = ModuleType("my_data_hub_yandex_edge_renderer")
    module.__file__ = str(path)
    exec(compile(path.read_text(encoding="utf-8"), str(path), "exec"), module.__dict__)
    return module


def test_edge_shell_scripts_parse_and_old_token_does_not_provision() -> None:
    for name in ("create_tunnel_identity.sh", "provision.sh"):
        subprocess.run(["bash", "-n", str(EDGE / name)], check=True)
        rejected = subprocess.run(
            ["bash", str(EDGE / name), "WRONG_TOKEN"],
            check=False,
            capture_output=True,
            text=True,
        )
        assert rejected.returncode == 2
        assert "usage:" in rejected.stderr


def test_cloud_init_contains_only_lockbox_reference_and_fixed_tunnels(tmp_path: Path) -> None:
    renderer = load_renderer()
    known_host = "188.227.84.107 ssh-ed25519 " + "A" * 68
    rendered = renderer.render(root=EDGE, secret_id="e6q12345678901234567", known_host=known_host)
    parsed = yaml.safe_load(rendered)
    assert parsed["ssh_pwauth"] is False
    forbidden_private_key_header = "\n-----BEGIN " + "OPENSSH PRIVATE KEY-----"
    assert forbidden_private_key_header not in rendered
    assert "e6q12345678901234567" in rendered
    decoded = {
        item["path"]: base64.b64decode(item["content"]).decode()
        for item in parsed["write_files"]
        if item.get("encoding") == "b64"
    }
    known_hosts_entry = next(
        item for item in parsed["write_files"] if item["path"] == "/etc/my-data-hub/known_hosts"
    )
    assert known_hosts_entry["owner"] == "root:root"
    assert known_hosts_entry["permissions"] == "0644"
    service = decoded["/etc/systemd/system/my-data-hub-edge-tunnel.service"]
    assert "-L 127.0.0.1:18080:127.0.0.1:8080" in service
    assert "-L 127.0.0.1:18765:127.0.0.1:8765" in service
    assert "-L 127.0.0.1:18780:127.0.0.1:8780" in service
    assert "dev@188.227.84.107" in service
    assert decoded["/etc/nginx/conf.d/my-data-hub-edge.conf"] == (EDGE / "edge-nginx.conf").read_text()
    assert "public-ip" not in rendered.lower()
    assert len(rendered.encode()) < 256 * 1024


@pytest.mark.parametrize(
    ("secret_id", "known_host"),
    [
        ("bad", "188.227.84.107 ssh-ed25519 " + "A" * 68),
        ("e6q12345678901234567", "evil.example ssh-ed25519 " + "A" * 68),
        ("e6q12345678901234567", "188.227.84.107 ssh-rsa " + "A" * 68),
    ],
)
def test_cloud_init_rejects_unbounded_or_untrusted_identity(secret_id: str, known_host: str) -> None:
    renderer = load_renderer()
    with pytest.raises(ValueError):
        renderer.render(root=EDGE, secret_id=secret_id, known_host=known_host)


def test_edge_admission_and_network_contract_are_exact() -> None:
    nginx = (EDGE / "edge-nginx.conf").read_text()
    proxy = (EDGE / "proxy.conf").read_text()
    provision = (EDGE / "provision.sh").read_text()
    identity = (EDGE / "create_tunnel_identity.sh").read_text()

    assert "server_name mcp-datahub.kenigevents.ru;" in nginx
    assert "server_name identity.kenigevents.ru;" in nginx
    assert "server_name _;" in nginx and "return 421;" in nginx
    assert "location ^~ /internal/" in nginx
    assert "proxy_set_header Forwarded \"\";" in proxy
    assert "proxy_set_header X-Forwarded-For \"\";" in proxy
    assert "--public-ip" not in provision
    assert "aws-v1-http-endpoint=enabled,aws-v1-http-token=disabled" in provision
    assert "ipv4-address=10.210.0.10" in provision
    assert "public-TLS,direction=ingress,port=443" in provision
    assert "restricted-SSH-tunnel,direction=egress,port=22" in provision
    assert "v4-cidrs=188.227.84.107/32" in provision
    assert "port=5432" not in provision
    assert "permitopen=\"127.0.0.1:8080\"" in identity
    assert "permitopen=\"127.0.0.1:8765\"" in identity
    assert "permitopen=\"127.0.0.1:8780\"" in identity
    assert 'command="/bin/false"' in identity
