from __future__ import annotations

import subprocess
from pathlib import Path

ROOT = Path(__file__).parents[1]
LOCAL = ROOT / "deploy" / "local-edge" / "README.md"
RETIRED = ROOT / "deploy" / "yandex-edge" / "provision.sh"


def test_cloud_edge_provisioner_is_an_unconditional_tombstone() -> None:
    subprocess.run(["bash", "-n", str(RETIRED)], check=True)
    for token in ("PROVISION_MY_DATA_HUB_YANDEX_EDGE", "WRONG_TOKEN"):
        result = subprocess.run(["bash", str(RETIRED), token], capture_output=True, text=True)
        assert result.returncode == 78
        assert "retired" in result.stderr.lower()
    script = RETIRED.read_text(encoding="utf-8")
    for mutation in ("yc ", "create ", "update ", "delete ", "add-records"):
        assert mutation not in script


def test_local_edge_contract_preserves_identity_and_loopback_boundary() -> None:
    contract = LOCAL.read_text(encoding="utf-8")
    for value in (
        "mcp-datahub.kenigevents.ru",
        "identity.kenigevents.ru",
        "127.0.0.1:8080",
        "127.0.0.1:8765",
        "127.0.0.1:8780",
        "loopback-only",
        "Forwarded",
        "X-Forwarded-*",
        "OAuth query strings",
        "real VPN-client regression",
    ):
        assert value in contract


def test_retirement_contract_protects_shared_site_and_mail_assets() -> None:
    contract = " ".join(
        (ROOT / "deploy" / "yandex-edge" / "README.md").read_text(encoding="utf-8").split()
    )
    for protected in (
        "shared DNS zone",
        "Object Storage",
        "CDN",
        "static-site certificates",
        "Postbox/mail infrastructure",
        "Identity Hub",
        "YDB",
        "unlabelled resources",
    ):
        assert protected in contract
