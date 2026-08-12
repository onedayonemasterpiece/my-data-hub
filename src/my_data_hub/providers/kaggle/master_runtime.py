"""Concrete master-lifecycle bridge over the repository's single Kaggle adapter.

The bridge translates the provider-neutral, persist-before-effect coordinator port
to :class:`KaggleProviderAdapter`.  It does not create an API client or transport.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import re
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from types import MappingProxyType
from typing import Any, Protocol
from uuid import NAMESPACE_URL, UUID, uuid5

from my_data_hub.hashing import canonical_json_bytes
from my_data_hub.orchestrator.master.evidence import MasterTerminalOutput, PlatformStatus
from my_data_hub.orchestrator.master.provider import (
    EffectReconciliation,
    MasterRuntimeProvider,
    MasterTerminalEvidence,
    MasterTerminalQuery,
    PlannedProviderEffect,
    ProviderEffectReceipt,
    ReconciliationStatus,
)
from my_data_hub.providers.models import ControlClass
from my_data_hub.runtime_sdk import CANONICAL_RUNTIME_CALLBACK_URL, KAGGLE_PROVIDER_TIMEOUT_SECONDS
from my_data_hub.workloads.bloggers.master_stage import BloggerImportStageReceipt

from .adapter import KaggleProviderAdapter
from .contracts import (
    DatasetMutationResult,
    KaggleKernelRunIdentity,
    KernelState,
    MutationAction,
    NotebookMutationResult,
    ProviderEffectIntent,
    TaskResourceClaim,
)
from .contracts import (
    ProviderEffectReceipt as KaggleProviderEffectReceipt,
)
from .source_attestation import executable_source_sha256

MASTER_TERMINAL_OUTPUT_NAME = "my-data-hub-master-terminal.json"
MAX_MASTER_TERMINAL_OUTPUT_BYTES = 256 * 1024
MAX_MASTER_STATUS_BYTES = 16 * 1024
EXECUTION_PINS_NAME = "execution-pins.json"
MASTER_CONFIG_NAME = "master-config.json"
POSTGRES_RUNTIME_ARCHIVE_NAME = "postgresql-18-runtime.bundle"
POSTGRES_RUNTIME_MANIFEST_NAME = "postgresql-18-runtime.json"
TUNNEL_KNOWN_HOSTS_NAME = "tunnel-known-hosts"
POSTGRES_TLS_CERT_NAME = "postgres-server.crt"
POSTGRES_TLS_KEY_NAME = "postgres-server.key"
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
POSTGRES_RUNTIME_RECIPE_SHA256 = "3fbcf52450dd44e3eb0eb7b826ebdb84a4293fbc54b713408083f10b44964d61"
POSTGRESQL_SOURCE_URL = "https://ftp.postgresql.org/pub/source/v18.4/postgresql-18.4.tar.bz2"
POSTGRESQL_SOURCE_SHA256 = "81a81ec695fb0c7901407defaa1d2f7973617154cf27ba74e3a7ab8e64436094"
PGVECTOR_SOURCE_URL = "https://github.com/pgvector/pgvector/archive/refs/tags/v0.8.6.tar.gz"
PGVECTOR_SOURCE_SHA256 = "10bf9938906e5d643bbc4a7eea104b6f57ba4898e5b76b20e60484ea1d5a7f8f"

MASTER_STATUS_HELPER = (
    b'"""Fixed master status bootstrap; token values are never logged."""\n'
    b"import json, os, pathlib\n\n"
    b"def load_run_config(path, *, run_id, attempt_id, notebook):\n"
    b"    raw = pathlib.Path(path).read_bytes()\n"
    b"    if not 1 <= len(raw) <= 16384:\n"
    b'        raise RuntimeError("status input size invalid")\n'
    b"    value = json.loads(raw)\n"
    b'    expected = {"schema_version","run_id","attempt_id","kind","notebook",'
    b'"callback_url","token","resource_leases","tls_certificate_sha256","tls_key_material_sha256"}\n'
    b"    if set(value) != expected or value['schema_version'] != 'my-data-hub-kaggle-run.v1':\n"
    b'        raise RuntimeError("status input shape invalid")\n'
    b"    if value['kind'] != 'postgres-master' or value['run_id'] != run_id:\n"
    b'        raise RuntimeError("status input run binding invalid")\n'
    b"    if value['attempt_id'] != attempt_id or value['notebook'] != notebook:\n"
    b'        raise RuntimeError("status input attempt binding invalid")\n'
    b"    token = value.pop('token')\n"
    b"    if not isinstance(token, str) or len(token) != 64:\n"
    b'        raise RuntimeError("status input token invalid")\n'
    b"    os.environ['MY_DATA_HUB_RUN_SECRET'] = token\n"
    b"    os.environ['MY_DATA_HUB_STATUS_RESOURCE_LEASES_JSON'] = json.dumps("
    b"value['resource_leases'], sort_keys=True, separators=(',', ':'))\n"
    b"    return value\n"
)


class MasterLaunchContractError(ValueError):
    """Raised before a provider call when launch identity/assets are incomplete."""


class NotebookRunReconciler(Protocol):
    def reconcile_private_notebook_run(
        self, *, task_run_id: UUID, provider_ref: str, expected_source_sha256: str
    ) -> KaggleKernelRunIdentity | None: ...


@dataclass(frozen=True, slots=True)
class KaggleMasterLaunchAssets:
    """Exact, secret-free assets and provider identities for one master deployment."""

    source_identity: str
    source_version: str
    checkpoint_ref: str
    dataset_ref: str
    notebook_ref: str
    dataset_files: Mapping[str, bytes]
    notebook_source: bytes
    callback_url: str
    checkpoint_verifier_ref: str
    checkpoint_verifier_source_file: str
    checkpoint_probe_relations: tuple[str, ...]
    tunnel_gateway_host: str
    tunnel_gateway_port: int
    tunnel_gateway_user: str
    tunnel_remote_port: int
    runtime_image_identity: str = (
        "gcr.io/kaggle-images/python@sha256:"
        "c1fa4de30bc268e601e6dcddb6ceb2519b9adde3527dbbfb05e6bdfbbbdcd1a2"
    )
    runtime_image_source_commit: str = "fc61d5cda7da39530055bae9bd0e92865f995cd9"
    runtime_python_series: str = "3.12"
    runtime_secret_bindings: Mapping[str, str] = field(default_factory=dict)
    notebook_code_file: str = "worker.ipynb"
    notebook_kernel_type: str = "notebook"
    notebook_language: str = "python"
    notebook_timeout_seconds: int = KAGGLE_PROVIDER_TIMEOUT_SECONDS
    enable_internet: bool = True

    def __post_init__(self) -> None:
        object.__setattr__(self, "dataset_files", MappingProxyType(dict(self.dataset_files)))
        object.__setattr__(self, "runtime_secret_bindings", MappingProxyType(dict(self.runtime_secret_bindings)))
        for field_name in ("dataset_ref", "notebook_ref", "checkpoint_ref", "checkpoint_verifier_ref"):
            value = getattr(self, field_name)
            parts = value.split("/")
            if len(parts) != 2 or not all(parts):
                raise MasterLaunchContractError(f"{field_name} must be exact owner/slug")
        if not self.source_identity or not self.source_version or not self.checkpoint_ref:
            raise MasterLaunchContractError("source/checkpoint identity must be exact")
        if (
            not re.fullmatch(r"[^@\s]+@sha256:[a-f0-9]{64}", self.runtime_image_identity)
            or not re.fullmatch(r"[a-f0-9]{40}", self.runtime_image_source_commit)
            or not re.fullmatch(r"[0-9]+\.[0-9]+", self.runtime_python_series)
        ):
            raise MasterLaunchContractError("master runtime image provenance is incomplete")
        if not self.dataset_files or not self.notebook_source:
            raise MasterLaunchContractError("master launch assets must be non-empty")
        if self.callback_url != CANONICAL_RUNTIME_CALLBACK_URL:
            raise MasterLaunchContractError("callback URL must be the owner-pinned HTTPS runtime endpoint")
        if (
            self.checkpoint_verifier_source_file not in self.dataset_files
            or not self.checkpoint_verifier_source_file.endswith(".ipynb")
            or not self.checkpoint_probe_relations
            or len(set(self.checkpoint_probe_relations)) != len(self.checkpoint_probe_relations)
            or any(not value or len(value) > 200 for value in self.checkpoint_probe_relations)
        ):
            raise MasterLaunchContractError("checkpoint verifier/probe assets are incomplete")
        if self.notebook_kernel_type not in {"notebook", "script"}:
            raise MasterLaunchContractError("master notebook kernel type is invalid")
        if (
            not self.tunnel_gateway_host
            or not re.fullmatch(r"[A-Za-z0-9.-]{1,253}", self.tunnel_gateway_host)
            or not self.tunnel_gateway_user
            or not re.fullmatch(r"[a-z_][a-z0-9_-]{0,31}", self.tunnel_gateway_user)
            or not 1 <= self.tunnel_gateway_port <= 65535
            or not 1 <= self.tunnel_remote_port <= 65535
        ):
            raise MasterLaunchContractError("master tunnel binding is invalid")
        self._validate_runtime_assets()
        if not 1_800 <= self.notebook_timeout_seconds <= KAGGLE_PROVIDER_TIMEOUT_SECONDS:
            raise MasterLaunchContractError(
                "master provider timeout must leave the declared reserve below Kaggle's 12-hour cap"
            )
        allowed_secret_bindings = {
            "YDB_ACCESS_TOKEN_CREDENTIALS",
        }
        for environment_name, secret_name in self.runtime_secret_bindings.items():
            if environment_name not in allowed_secret_bindings or not secret_name:
                raise MasterLaunchContractError("runtime secret binding is invalid")

    def _validate_runtime_assets(self) -> None:
        required = {
            POSTGRES_RUNTIME_ARCHIVE_NAME,
            POSTGRES_RUNTIME_MANIFEST_NAME,
            TUNNEL_KNOWN_HOSTS_NAME,
        }
        if not required.issubset(self.dataset_files):
            raise MasterLaunchContractError("pinned PostgreSQL runtime or tunnel host key is absent")
        manifest_body = self.dataset_files[POSTGRES_RUNTIME_MANIFEST_NAME]
        try:
            manifest = json.loads(manifest_body)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise MasterLaunchContractError("PostgreSQL runtime manifest is invalid JSON") from exc
        expected = {
            "schema_version",
            "postgresql_version",
            "pgvector_version",
            "platform",
            "archive_sha256",
            "postgresql_source_url",
            "postgresql_source_sha256",
            "pgvector_source_url",
            "pgvector_source_sha256",
            "builder_image",
            "build_recipe_sha256",
        }
        archive_sha = hashlib.sha256(self.dataset_files[POSTGRES_RUNTIME_ARCHIVE_NAME]).hexdigest()
        if (
            not isinstance(manifest, dict)
            or set(manifest) != expected
            or manifest.get("schema_version") != "my-data-hub-postgresql-runtime.v1"
            or manifest.get("postgresql_version") != "18.4"
            or manifest.get("pgvector_version") != "0.8.6"
            or manifest.get("platform") != "linux-x86_64"
            or manifest.get("archive_sha256") != archive_sha
            or archive_sha == "9be7324987fa81656e6b54888b9ec707851481254cdf839517a6a0f9732671f6"
            or manifest.get("postgresql_source_url") != POSTGRESQL_SOURCE_URL
            or manifest.get("postgresql_source_sha256") != POSTGRESQL_SOURCE_SHA256
            or manifest.get("pgvector_source_url") != PGVECTOR_SOURCE_URL
            or manifest.get("pgvector_source_sha256") != PGVECTOR_SOURCE_SHA256
            or manifest.get("builder_image")
            != "ubuntu:22.04@sha256:3b06811b2afd352be909dd088a004166d665dc76d38b13eada33522a9d915c6f"
            or manifest.get("build_recipe_sha256") != POSTGRES_RUNTIME_RECIPE_SHA256
        ):
            raise MasterLaunchContractError("PostgreSQL runtime provenance is incomplete or mismatched")
        known_hosts = self.dataset_files[TUNNEL_KNOWN_HOSTS_NAME]
        try:
            known_hosts_text = known_hosts.decode("ascii")
        except UnicodeDecodeError as exc:
            raise MasterLaunchContractError("tunnel known_hosts must be bounded ASCII") from exc
        lines = tuple(line for line in known_hosts_text.splitlines() if line)
        if (
            not 1 <= len(known_hosts) <= 64 * 1024
            or not lines
            or any(not line.startswith("|") or " ssh-ed25519 " not in line or len(line) > 4096 for line in lines)
        ):
            raise MasterLaunchContractError("tunnel known_hosts is not a reviewed hashed host-key asset")

    def render_values(self, identity: Mapping[str, Any]) -> dict[str, str]:
        values = {
            "MY_DATA_HUB_CALLBACK_URL": self.callback_url,
            "MY_DATA_HUB_OPERATION_ID": str(identity["operation_id"]),
            "MY_DATA_HUB_RUN_ID": str(identity["run_id"]),
            "MY_DATA_HUB_ATTEMPT_ID": str(identity["attempt_id"]),
            "MY_DATA_HUB_SERVICE_INSTANCE_ID": str(identity["service_instance_id"]),
            "MY_DATA_HUB_MASTER_INSTANCE_ID": str(identity["master_instance_id"]),
            "MY_DATA_HUB_EPOCH": str(identity["epoch"]),
            "MY_DATA_HUB_SOURCE_IDENTITY": self.source_identity,
            "MY_DATA_HUB_SOURCE_VERSION": self.source_version,
            "MY_DATA_HUB_CHECKPOINT_REF": self.checkpoint_ref,
        }
        input_root = f"/kaggle/input/{self.dataset_ref.split('/', 1)[1]}"
        values.update(
            {
                "MY_DATA_HUB_POSTGRES_RUNTIME_ARCHIVE": f"{input_root}/{POSTGRES_RUNTIME_ARCHIVE_NAME}",
                "MY_DATA_HUB_POSTGRES_RUNTIME_ARCHIVE_SHA256": hashlib.sha256(
                    self.dataset_files[POSTGRES_RUNTIME_ARCHIVE_NAME]
                ).hexdigest(),
                "MY_DATA_HUB_POSTGRES_RUNTIME_MANIFEST": f"{input_root}/{POSTGRES_RUNTIME_MANIFEST_NAME}",
                "MY_DATA_HUB_POSTGRES_RUNTIME_MANIFEST_SHA256": hashlib.sha256(
                    self.dataset_files[POSTGRES_RUNTIME_MANIFEST_NAME]
                ).hexdigest(),
                "MY_DATA_HUB_TUNNEL_KNOWN_HOSTS": f"{input_root}/{TUNNEL_KNOWN_HOSTS_NAME}",
                "MY_DATA_HUB_TUNNEL_KNOWN_HOSTS_SHA256": hashlib.sha256(
                    self.dataset_files[TUNNEL_KNOWN_HOSTS_NAME]
                ).hexdigest(),
            }
        )
        verifier_source = self.dataset_files[self.checkpoint_verifier_source_file]
        values.update(
            {
                "MY_DATA_HUB_CONTROL_PLANE_URL": self.callback_url.removesuffix("/internal/runtime/events"),
                "MY_DATA_HUB_CHECKPOINT_DATASET_REF": self.checkpoint_ref,
                "MY_DATA_HUB_CHECKPOINT_VERIFIER_REF": self.checkpoint_verifier_ref,
                "MY_DATA_HUB_CHECKPOINT_VERIFIER_SOURCE_PATH": (f"{input_root}/{self.checkpoint_verifier_source_file}"),
                "MY_DATA_HUB_CHECKPOINT_VERIFIER_SOURCE_SHA256": hashlib.sha256(verifier_source).hexdigest(),
                "MY_DATA_HUB_CHECKPOINT_PROBE_RELATIONS_JSON": json.dumps(
                    self.checkpoint_probe_relations, separators=(",", ":")
                ),
            }
        )
        wheels = sorted(path for path in self.dataset_files if path.endswith(".whl"))
        if len(wheels) == 1:
            values["MY_DATA_HUB_WHEEL_PATH"] = f"{input_root}/{wheels[0]}"
            values["MY_DATA_HUB_WHEEL_SHA256"] = hashlib.sha256(self.dataset_files[wheels[0]]).hexdigest()
        return values


def _replace_nonsecret_markers(content: bytes, values: Mapping[str, str]) -> bytes:
    rendered = content
    for key, value in values.items():
        rendered = rendered.replace(f"{{{{{key}}}}}".encode(), value.encode())
    if b"{{MY_DATA_HUB_RUN_SECRET}}" in rendered:
        raise MasterLaunchContractError("launch assets may not contain a runtime secret placeholder")
    return rendered


def _runtime_bootstrap(
    values: Mapping[str, str],
    *,
    status_dataset_ref: str,
    status_config_sha256: str,
    status_helper_sha256: str,
    master_config_sha256: str,
    secret_bindings: Mapping[str, str],
    execution_pins: bytes | None = None,
) -> str:
    # The callback token is loaded only from the exact private status Dataset.
    encoded = json.dumps(dict(values), sort_keys=True)
    bindings = json.dumps(dict(secret_bindings), sort_keys=True)
    status_mount = f"/kaggle/input/{status_dataset_ref.split('/', 1)[1]}"
    bootstrap = (
        "import hashlib as _mdh_hashlib, importlib.util as _mdh_importlib, os as _mdh_os, "
        "pathlib as _mdh_pathlib\n"
        f"_mdh_values = {encoded}\n"
        "_mdh_os.environ.update(_mdh_values)\n"
        f"_mdh_status_root = _mdh_pathlib.Path({status_mount!r})\n"
        "_mdh_config = _mdh_status_root / 'kaggle_run.json'\n"
        "_mdh_helper = _mdh_status_root / 'kaggle_status_client.py'\n"
        "_mdh_master_config = _mdh_status_root / 'master-config.json'\n"
        f"assert _mdh_hashlib.sha256(_mdh_config.read_bytes()).hexdigest() == {status_config_sha256!r}\n"
        f"assert _mdh_hashlib.sha256(_mdh_helper.read_bytes()).hexdigest() == {status_helper_sha256!r}\n"
        f"assert _mdh_hashlib.sha256(_mdh_master_config.read_bytes()).hexdigest() == {master_config_sha256!r}\n"
        "_mdh_os.environ['MY_DATA_HUB_MASTER_CONFIG'] = str(_mdh_master_config)\n"
        "_mdh_spec = _mdh_importlib.spec_from_file_location('mdh_status_bootstrap', _mdh_helper)\n"
        "_mdh_module = _mdh_importlib.module_from_spec(_mdh_spec)\n"
        "_mdh_spec.loader.exec_module(_mdh_module)\n"
        "_mdh_status = _mdh_module.load_run_config(_mdh_config, "
        "run_id=_mdh_values['MY_DATA_HUB_RUN_ID'], attempt_id=_mdh_values['MY_DATA_HUB_ATTEMPT_ID'], "
        "notebook=_mdh_values['MY_DATA_HUB_SOURCE_IDENTITY'])\n"
    )
    if execution_pins is not None:
        bootstrap += (
            f"_mdh_pins_body = {execution_pins!r}\n"
            "_mdh_pins_path = _mdh_pathlib.Path('/kaggle/working/execution-pins.json')\n"
            "_mdh_fd = _mdh_os.open(_mdh_pins_path, _mdh_os.O_WRONLY|_mdh_os.O_CREAT|_mdh_os.O_EXCL, 0o600)\n"
            "with _mdh_os.fdopen(_mdh_fd, 'wb') as _mdh_pins_stream: _mdh_pins_stream.write(_mdh_pins_body)\n"
            "_mdh_os.environ['MY_DATA_HUB_EXECUTION_PINS_PATH'] = str(_mdh_pins_path)\n"
            "_mdh_os.environ['MY_DATA_HUB_EXECUTION_PINS_SHA256'] = _mdh_hashlib.sha256(_mdh_pins_body).hexdigest()\n"
        )
    if secret_bindings:
        bootstrap += (
            "from kaggle_secrets import UserSecretsClient as _MdhSecrets\n"
            "_mdh_secrets = _MdhSecrets()\n"
            f"for _mdh_env, _mdh_name in {bindings}.items():\n"
            "    _mdh_value = _mdh_secrets.get_secret(_mdh_name)\n"
            "    _mdh_os.environ[_mdh_env] = _mdh_value\n"
        )
    bootstrap += (
        "import json as _mdh_json, tarfile as _mdh_tarfile\n"
        "_mdh_archive = _mdh_pathlib.Path(_mdh_values['MY_DATA_HUB_POSTGRES_RUNTIME_ARCHIVE'])\n"
        "_mdh_manifest_path = _mdh_pathlib.Path(_mdh_values['MY_DATA_HUB_POSTGRES_RUNTIME_MANIFEST'])\n"
        "if _mdh_hashlib.sha256(_mdh_archive.read_bytes()).hexdigest() != "
        "_mdh_values['MY_DATA_HUB_POSTGRES_RUNTIME_ARCHIVE_SHA256']:\n"
        "    raise RuntimeError('PostgreSQL runtime archive hash differs')\n"
        "if _mdh_hashlib.sha256(_mdh_manifest_path.read_bytes()).hexdigest() != "
        "_mdh_values['MY_DATA_HUB_POSTGRES_RUNTIME_MANIFEST_SHA256']:\n"
        "    raise RuntimeError('PostgreSQL runtime manifest hash differs')\n"
        "_mdh_pg_manifest = _mdh_json.loads(_mdh_manifest_path.read_bytes())\n"
        "if _mdh_pg_manifest.get('schema_version') != 'my-data-hub-postgresql-runtime.v1' or "
        "_mdh_pg_manifest.get('archive_sha256') != "
        "_mdh_values['MY_DATA_HUB_POSTGRES_RUNTIME_ARCHIVE_SHA256']:\n"
        "    raise RuntimeError('PostgreSQL runtime provenance differs')\n"
        "_mdh_pg_root = _mdh_pathlib.Path('/kaggle/working/mdh-postgresql-runtime')\n"
        "_mdh_pg_root.mkdir(mode=0o700, parents=True, exist_ok=False)\n"
        "with _mdh_tarfile.open(_mdh_archive, 'r:gz') as _mdh_tar:\n"
        "    _mdh_members = _mdh_tar.getmembers()\n"
        "    if not 1 <= len(_mdh_members) <= 4000 or sum(max(0, m.size) for m in _mdh_members) > 536870912:\n"
        "        raise RuntimeError('PostgreSQL runtime archive exceeds fixed bounds')\n"
        "    if any(m.islnk() or (m.issym() and ('/' in m.linkname or "
        "'..' in _mdh_pathlib.PurePosixPath(m.linkname).parts)) or "
        "not m.name.startswith('pgsql/') or "
        "'..' in _mdh_pathlib.PurePosixPath(m.name).parts for m in _mdh_members):\n"
        "        raise RuntimeError('PostgreSQL runtime archive contains an unsafe member')\n"
        "    _mdh_tar.extractall(_mdh_pg_root, members=_mdh_members, filter='data')\n"
        "_mdh_os.environ['LD_LIBRARY_PATH'] = "
        "'/kaggle/working/mdh-postgresql-runtime/pgsql/lib:"
        "/kaggle/working/mdh-postgresql-runtime/pgsql/lib/runtime-deps'\n"
        "_mdh_tls_root = _mdh_pathlib.Path('/kaggle/working/mdh-tls')\n"
        "_mdh_tls_root.mkdir(mode=0o700, parents=True, exist_ok=False)\n"
        "for _mdh_tls_name, _mdh_tls_hash, _mdh_tls_env in "
        "(('postgres-server.crt','tls_certificate_sha256','MY_DATA_HUB_POSTGRES_TLS_CERT'),"
        "('postgres-server.key','tls_key_material_sha256','MY_DATA_HUB_POSTGRES_TLS_KEY')):\n"
        "    _mdh_tls_input = _mdh_status_root / _mdh_tls_name\n"
        "    _mdh_tls_body = _mdh_tls_input.read_bytes()\n"
        "    if _mdh_hashlib.sha256(_mdh_tls_body).hexdigest() != _mdh_status[_mdh_tls_hash]:\n"
        "        raise RuntimeError('PostgreSQL TLS status asset hash differs')\n"
        "    _mdh_tls_output = _mdh_tls_root / _mdh_tls_name\n"
        "    _mdh_fd = _mdh_os.open(_mdh_tls_output, _mdh_os.O_WRONLY|_mdh_os.O_CREAT|_mdh_os.O_EXCL, 0o600)\n"
        "    with _mdh_os.fdopen(_mdh_fd, 'wb') as _mdh_stream:\n"
        "        _mdh_stream.write(_mdh_tls_body)\n"
        "    _mdh_os.environ[_mdh_tls_env] = str(_mdh_tls_output)\n"
        "_mdh_known_hosts = _mdh_pathlib.Path(_mdh_values['MY_DATA_HUB_TUNNEL_KNOWN_HOSTS'])\n"
        "if _mdh_hashlib.sha256(_mdh_known_hosts.read_bytes()).hexdigest() != "
        "_mdh_values['MY_DATA_HUB_TUNNEL_KNOWN_HOSTS_SHA256']:\n"
        "    raise RuntimeError('tunnel known_hosts hash differs')\n"
    )
    return bootstrap


def render_notebook_source(
    source: bytes,
    *,
    kernel_type: str,
    values: Mapping[str, str],
    status_dataset_ref: str,
    status_config_sha256: str,
    status_helper_sha256: str,
    master_config_sha256: str,
    secret_bindings: Mapping[str, str] | None = None,
    execution_pins: bytes | None = None,
) -> bytes:
    source = _replace_nonsecret_markers(source, values)
    bootstrap = _runtime_bootstrap(
        values,
        status_dataset_ref=status_dataset_ref,
        status_config_sha256=status_config_sha256,
        status_helper_sha256=status_helper_sha256,
        master_config_sha256=master_config_sha256,
        secret_bindings=secret_bindings or {},
        execution_pins=execution_pins,
    )
    if kernel_type == "script":
        return (bootstrap + "\n").encode() + source
    try:
        body = json.loads(source)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MasterLaunchContractError("master notebook asset is not valid UTF-8 JSON") from exc
    if not isinstance(body, dict) or not isinstance(body.get("cells"), list):
        raise MasterLaunchContractError("master notebook asset lacks a cells array")
    body["cells"].insert(
        0,
        {
            "cell_type": "code",
            "execution_count": None,
            "id": "my-data-hub-runtime-identity",
            "metadata": {},
            "outputs": [],
            "source": bootstrap,
        },
    )
    return json.dumps(body).encode()


class KaggleMasterRuntimeProvider(MasterRuntimeProvider):
    """MasterRuntimeProvider implemented by the one official Kaggle adapter."""

    def __init__(
        self,
        adapter: KaggleProviderAdapter,
        assets: KaggleMasterLaunchAssets,
        *,
        status_authority: object | None = None,
    ) -> None:
        self.adapter = adapter
        self.assets = assets
        self.status_authority = status_authority

    def status_dataset_ref(self, identity: Mapping[str, Any]) -> str:
        owner = self.assets.notebook_ref.split("/", 1)[0]
        return f"{owner}/mdh-master-status-{UUID(str(identity['run_id'])).hex}"

    def status_files(
        self,
        identity: Mapping[str, Any],
        token: str,
        *,
        tls_certificate: bytes,
        tls_private_key: bytes,
    ) -> dict[str, bytes]:
        if (
            not tls_certificate.startswith(b"-----BEGIN CERTIFICATE-----")
            or not 1 <= len(tls_certificate) <= 64 * 1024
            or not tls_private_key.startswith(b"-----BEGIN " + b"PRIVATE KEY-----")
            or not 1 <= len(tls_private_key) <= 64 * 1024
        ):
            raise MasterLaunchContractError("master TLS status assets are invalid")
        value = {
            "schema_version": "my-data-hub-kaggle-run.v1",
            "run_id": str(identity["run_id"]),
            "attempt_id": str(identity["attempt_id"]),
            "kind": "postgres-master",
            "notebook": self.assets.source_identity,
            "callback_url": self.assets.callback_url,
            "token": token,
            "resource_leases": [identity["status_resource_lease"]],
            "tls_certificate_sha256": hashlib.sha256(tls_certificate).hexdigest(),
            "tls_key_material_sha256": hashlib.sha256(tls_private_key).hexdigest(),
        }
        encoded = canonical_json_bytes(value)
        if len(encoded) > MAX_MASTER_STATUS_BYTES:
            raise MasterLaunchContractError("master status config exceeds 16 KiB")
        master_config = self._master_config(identity)
        return {
            "kaggle_run.json": encoded,
            "kaggle_status_client.py": MASTER_STATUS_HELPER,
            MASTER_CONFIG_NAME: canonical_json_bytes(master_config),
            POSTGRES_TLS_CERT_NAME: tls_certificate,
            POSTGRES_TLS_KEY_NAME: tls_private_key,
        }

    def _execution_pins(self, identity: Mapping[str, Any]) -> dict[str, object] | None:
        try:
            notebook = json.loads(self.assets.notebook_source)
        except (TypeError, json.JSONDecodeError):
            return None
        try:
            contract = notebook.get("metadata", {}).get("my_data_hub", {}).get("execution_pin_contract")
            if contract is None:
                return None
            asset_version = int(identity["asset_dataset"]["provider_version"])
        except (KeyError, TypeError, ValueError) as exc:
            raise MasterLaunchContractError("generated master execution pin contract is absent") from exc
        status = self._status_authority_row(identity).get("status_dataset", {})
        status_ref = str(status.get("exact_version_ref", ""))
        refs = [f"{self.assets.dataset_ref}/{asset_version}", status_ref]
        checkpoint = identity.get("boot_checkpoint")
        if isinstance(checkpoint, Mapping) and checkpoint.get("kind") == "VERIFIED":
            refs.append(str(checkpoint["exact_version_ref"]))
        wheel_names = sorted(name for name in self.assets.dataset_files if name.endswith(".whl"))
        if len(wheel_names) != 1:
            raise MasterLaunchContractError("master execution pins require one exact wheel")
        return {
            "schema": contract["schema"],
            "notebook": contract["notebook"],
            "python_series": self.assets.runtime_python_series,
            "image_source_commit": self.assets.runtime_image_source_commit,
            "kaggle_runtime_image_identity": self.assets.runtime_image_identity,
            "input_dataset_versions": refs,
            "immutable_asset_sha256s": {
                "my_data_hub_wheel_sha256": hashlib.sha256(
                    self.assets.dataset_files[wheel_names[0]]
                ).hexdigest(),
                "primary_source_sha256": notebook["metadata"]["my_data_hub"]["primary_source_sha256"],
            },
            "output_contract": contract["output_contract"],
            "model": contract["model"],
            "privacy": contract["privacy"],
            "resource_class": contract["resource_class"],
            "cleanup_retention_policy": contract["cleanup_retention_policy"],
        }

    def _master_config(self, identity: Mapping[str, Any]) -> dict[str, object]:
        checkpoint = identity.get("boot_checkpoint")
        if not isinstance(checkpoint, Mapping) or checkpoint.get("kind") not in {"EMPTY", "VERIFIED"}:
            raise MasterLaunchContractError("master boot checkpoint snapshot is absent")
        verified = checkpoint.get("kind") == "VERIFIED"
        checkpoint_directory: str | None = None
        if verified:
            exact_ref = str(checkpoint.get("exact_version_ref", ""))
            parts = exact_ref.split("/")
            if len(parts) != 3 or not parts[2].isdigit() or int(parts[2]) < 1:
                raise MasterLaunchContractError("master boot checkpoint ref is not exact numeric")
            checkpoint_directory = f"/kaggle/input/{parts[1]}"
        return {
            "master_instance_id": str(identity["master_instance_id"]),
            "run_id": str(identity["run_id"]),
            "attempt_id": str(identity["attempt_id"]),
            "service_instance_id": str(identity["service_instance_id"]),
            "epoch": int(identity["epoch"]),
            "boot_source": "verified_checkpoint" if verified else "empty_baseline",
            "checkpoint_directory": checkpoint_directory,
            "checkpoint_id": str(checkpoint["checkpoint_id"]) if verified else None,
            "checkpoint_exact_version_ref": str(checkpoint["exact_version_ref"]) if verified else None,
            "checkpoint_manifest_sha256": str(checkpoint["manifest_sha256"]) if verified else None,
            "checkpoint_head_generation": int(checkpoint["generation"]),
            "lease_seconds": 120,
            "postgres_bin": "/kaggle/working/mdh-postgresql-runtime/pgsql/bin",
            "postgres_port": 15432,
            "tunnel_gateway_host": self.assets.tunnel_gateway_host,
            "tunnel_gateway_port": self.assets.tunnel_gateway_port,
            "tunnel_gateway_user": self.assets.tunnel_gateway_user,
            "tunnel_remote_port": self.assets.tunnel_remote_port,
            "maximum_runtime_seconds": 21600,
            "checkpoint_reserve_seconds": 10800,
            "source_identity": self.assets.source_identity,
            "source_version": self.assets.source_version,
        }

    def create_status_dataset(self, identity: Mapping[str, Any], files: Mapping[str, bytes]) -> DatasetMutationResult:
        provider_ref = self.status_dataset_ref(identity)
        intent = ProviderEffectIntent.create(
            operation_id=UUID(str(identity["operation_id"])),
            effect_id=uuid5(NAMESPACE_URL, f"master-status-create:{identity['operation_id']}"),
            idempotency_key=f"master-status-create:{identity['operation_id']}",
            task_id=UUID(str(identity["run_id"])),
            action=MutationAction.CREATE_DATASET,
            provider_ref=provider_ref,
            arguments={
                "content_tree_sha256": self._mapping_sha(files),
                "control_class": ControlClass.ORCHESTRATOR_PROTECTED.value,
                "disposable": True,
            },
            requested_at=datetime.fromisoformat(str(identity["status_requested_at"]).replace("Z", "+00:00")),
        )
        return self.adapter.create_private_dataset(
            intent=intent,
            files=files,
            title=provider_ref.split("/", 1)[1],
            control_class=ControlClass.ORCHESTRATOR_PROTECTED,
            disposable=True,
        )

    def delete_status_dataset(self, identity: Mapping[str, Any], claim: TaskResourceClaim) -> object:
        return self.adapter.delete_task_created_resource(
            intent=ProviderEffectIntent.create(
                operation_id=UUID(str(identity["operation_id"])),
                effect_id=uuid5(NAMESPACE_URL, f"master-status-delete:{identity['operation_id']}"),
                idempotency_key=f"master-status-delete:{identity['operation_id']}",
                task_id=UUID(str(identity["run_id"])),
                action=MutationAction.DELETE_DATASET,
                provider_ref=claim.provider_ref,
                expected_fingerprint=claim.fingerprint,
                arguments={
                    "claim_sha256": claim.claim_sha256,
                    "provider_version": claim.provider_version,
                },
                requested_at=datetime.fromisoformat(str(identity["status_requested_at"]).replace("Z", "+00:00")),
            ),
            claim=claim,
        )

    def terminate_run_for_fm08(
        self,
        *,
        task_id: UUID,
        operation_id: UUID,
        run: KaggleKernelRunIdentity,
        requested_at: datetime,
    ) -> KaggleProviderEffectReceipt:
        """Execute the fixed task-owned abrupt termination for one old run."""

        if run.provider_ref != self.assets.notebook_ref:
            raise MasterLaunchContractError("FM08 old run differs from the configured master ref")
        intent = ProviderEffectIntent.create(
            operation_id=operation_id,
            effect_id=uuid5(NAMESPACE_URL, f"fm08-abrupt-terminate:{task_id}"),
            idempotency_key=f"fm08-abrupt-terminate:{task_id}",
            task_id=task_id,
            action=MutationAction.DELETE_NOTEBOOK,
            provider_ref=run.provider_ref,
            arguments={
                "task_run_id": str(run.task_run_id),
                "source_version": run.source_version,
                "source_sha256": run.source_sha256,
                "provider_kernel_id": run.provider_kernel_id,
                "provider_run_ref": run.provider_run_ref,
                "termination_kind": "fm08_abrupt_master",
            },
            requested_at=requested_at,
        )
        return self.adapter.terminate_attested_master_run(intent=intent, run=run)

    def execute(self, effect: PlannedProviderEffect) -> ProviderEffectReceipt:
        self._validate_effect(effect)
        if effect.effect_kind == "ensure_dataset":
            files = {
                path: _replace_nonsecret_markers(content, self.assets.render_values(effect.exact_identity))
                for path, content in self.assets.dataset_files.items()
            }
            intent = self._intent(
                effect,
                MutationAction.CREATE_DATASET,
                self.assets.dataset_ref,
                {
                    "content_tree_sha256": self._mapping_sha(files),
                    "control_class": ControlClass.ORCHESTRATOR_PROTECTED.value,
                    "disposable": False,
                },
            )
            result = self.adapter.create_private_dataset(
                intent=intent,
                files=files,
                title=self.assets.dataset_ref.split("/", 1)[1],
                control_class=ControlClass.ORCHESTRATOR_PROTECTED,
                disposable=False,
            )
            return self._receipt(
                effect,
                self.assets.dataset_ref,
                {
                    "provider_ref": result.identity.provider_ref,
                    "provider_version": result.identity.version,
                    "package_sha256": result.identity.package_sha256,
                },
            )
        if effect.effect_kind == "push_notebook":
            source = self._source(effect.exact_identity)
            status = self._status_authority_row(effect.exact_identity)
            self._assert_boot_checkpoint_current(status)
            asset = effect.exact_identity.get("asset_dataset")
            if not isinstance(asset, Mapping) or not isinstance(asset.get("provider_version"), int):
                raise MasterLaunchContractError("master push lacks exact asset Dataset version")
            asset_ref = f"{self.assets.dataset_ref}/{asset['provider_version']}"
            status_ref = str(status["status_dataset"]["exact_version_ref"])
            boot = status["status_dataset"].get("boot_checkpoint")
            checkpoint_sources = (
                (str(boot["exact_version_ref"]),)
                if isinstance(boot, Mapping) and boot.get("kind") == "VERIFIED"
                else ()
            )
            dataset_sources = (asset_ref, status_ref, *checkpoint_sources)
            intent = self._intent(
                effect,
                MutationAction.PUSH_NOTEBOOK,
                self.assets.notebook_ref,
                {
                    "task_run_id": str(effect.exact_identity["run_id"]),
                    "source_sha256": executable_source_sha256(source, kernel_type=self.assets.notebook_kernel_type),
                    "dataset_sources": dataset_sources,
                    "control_class": ControlClass.ORCHESTRATOR_PROTECTED.value,
                    "disposable": False,
                    "docker_image": self.assets.runtime_image_identity,
                    "docker_image_pinning_type": "original",
                },
            )
            result = self.adapter.push_private_master_notebook_pending_attestation(
                intent=intent,
                task_run_id=UUID(str(effect.exact_identity["run_id"])),
                source=source,
                title=self.assets.notebook_ref.split("/", 1)[1],
                code_file=self.assets.notebook_code_file,
                kernel_type=self.assets.notebook_kernel_type,
                language=self.assets.notebook_language,
                control_class=ControlClass.ORCHESTRATOR_PROTECTED,
                disposable=False,
                dataset_sources=dataset_sources,
                enable_internet=self.assets.enable_internet,
                timeout_seconds=self.assets.notebook_timeout_seconds,
                docker_image=self.assets.runtime_image_identity,
                docker_image_pinning_type="original",
            )
            return self._notebook_receipt(effect, result)
        if effect.effect_kind == "trigger_run":
            launch = effect.exact_identity.get("notebook_launch")
            run = self._run_from_identity(launch)
            read_status = getattr(self.adapter, "read_attested_master_run_status", None)
            if not callable(read_status):
                read_status = self.adapter.read_run_status
            read_status(run)
            return self._receipt(effect, run.provider_run_ref, run.model_dump(mode="json"))
        raise MasterLaunchContractError(f"unsupported master provider effect: {effect.effect_kind}")

    def reconcile(self, effect: PlannedProviderEffect) -> EffectReconciliation:
        self._validate_effect(effect)
        if effect.effect_kind == "ensure_dataset":
            # Replaying the exact deterministic create intent is safe: the adapter
            # performs exact package readback on provider conflict and returns
            # ALREADY_APPLIED rather than creating a second dataset.
            try:
                return EffectReconciliation(ReconciliationStatus.FOUND, self.execute(effect))
            except Exception:
                return EffectReconciliation(ReconciliationStatus.AMBIGUOUS)
        if effect.effect_kind == "push_notebook":
            source = self._source(effect.exact_identity)
            reconciler = getattr(self.adapter, "reconcile_private_notebook_run", None)
            if not callable(reconciler):
                return EffectReconciliation(ReconciliationStatus.AMBIGUOUS)
            run = reconciler(
                task_run_id=UUID(str(effect.exact_identity["run_id"])),
                provider_ref=self.assets.notebook_ref,
                expected_source_sha256=executable_source_sha256(source, kernel_type=self.assets.notebook_kernel_type),
            )
            if run is None:
                return EffectReconciliation(ReconciliationStatus.ABSENT)
            result = self._receipt(effect, run.provider_run_ref, run.model_dump(mode="json"))
            return EffectReconciliation(ReconciliationStatus.FOUND, result)
        if effect.effect_kind == "trigger_run":
            try:
                return EffectReconciliation(ReconciliationStatus.FOUND, self.execute(effect))
            except Exception:
                return EffectReconciliation(ReconciliationStatus.AMBIGUOUS)
        return EffectReconciliation(ReconciliationStatus.AMBIGUOUS)

    def observe_terminal(self, query: MasterTerminalQuery) -> MasterTerminalEvidence:
        """Read bounded terminal evidence from this exact launched Notebook run."""

        self._validate_terminal_query(query)
        run = self._run_from_identity(query.provider_run_identity)
        if run.task_run_id != UUID(query.run_id) or run.provider_ref != self.assets.notebook_ref:
            raise MasterLaunchContractError("terminal query differs from the exact launched run")
        read_status = getattr(self.adapter, "read_attested_master_run_status", None)
        if not callable(read_status):
            read_status = self.adapter.read_run_status
        observed = read_status(run)
        platform_status = {
            KernelState.QUEUED: PlatformStatus.QUEUED,
            KernelState.RUNNING: PlatformStatus.RUNNING,
            KernelState.COMPLETE: PlatformStatus.COMPLETE,
            KernelState.FAILED: PlatformStatus.ERROR,
            KernelState.UNKNOWN: PlatformStatus.UNKNOWN,
        }.get(KernelState(observed.state), PlatformStatus.UNKNOWN)
        if platform_status != PlatformStatus.COMPLETE:
            return MasterTerminalEvidence(platform_status)
        with tempfile.TemporaryDirectory(prefix="my-data-hub-terminal-") as folder:
            destination = Path(folder)
            download_output = getattr(
                self.adapter,
                "download_attested_master_output_file",
                self.adapter.download_exact_run_output_file,
            )
            output_tree = download_output(
                run,
                destination=destination,
                file_name=MASTER_TERMINAL_OUTPUT_NAME,
                max_bytes=MAX_MASTER_TERMINAL_OUTPUT_BYTES,
            )
            output = self._read_terminal_output(
                destination / MASTER_TERMINAL_OUTPUT_NAME,
                output_tree_sha256=output_tree.output_tree_sha256,
            )
        expected = (
            query.run_id,
            query.attempt_id,
            query.service_instance_id,
            query.master_instance_id,
            query.source_identity,
            query.source_version,
            query.epoch,
        )
        actual = (
            output.run_id,
            output.attempt_id,
            output.service_instance_id,
            output.master_instance_id,
            output.source_identity,
            output.source_version,
            output.epoch,
        )
        if actual != expected:
            raise MasterLaunchContractError("terminal output differs from the exact master attempt")
        if not hmac.compare_digest(output.executed_source_sha256, run.source_sha256):
            raise MasterLaunchContractError("terminal output source differs from the exact provider push")
        return MasterTerminalEvidence(platform_status, output)

    def _validate_terminal_query(self, query: MasterTerminalQuery) -> None:
        if (
            query.source_identity != self.assets.source_identity
            or query.source_version != self.assets.source_version
            or query.checkpoint_ref != self.assets.checkpoint_ref
            or query.epoch < 1
        ):
            raise MasterLaunchContractError("terminal query differs from configured launch assets")

    @staticmethod
    def _read_terminal_output(path: Path, *, output_tree_sha256: str) -> MasterTerminalOutput:
        try:
            if path.is_symlink() or not path.is_file():
                raise MasterLaunchContractError("exact run output lacks the master terminal contract")
            if path.stat().st_size > MAX_MASTER_TERMINAL_OUTPUT_BYTES:
                raise MasterLaunchContractError("master terminal output exceeds 256 KiB")
            encoded = path.read_bytes()
            raw = json.loads(encoded)
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise MasterLaunchContractError("master terminal output is not bounded canonical JSON") from exc
        required_keys = {
            "schema_version",
            "run_id",
            "attempt_id",
            "service_instance_id",
            "master_instance_id",
            "source_identity",
            "source_version",
            "executed_source_sha256",
            "epoch",
            "status",
            "checkpoint",
            "events",
        }
        allowed_keys = required_keys | {"blogger_import_receipt"}
        if not isinstance(raw, dict) or not required_keys.issubset(raw) or not set(raw).issubset(allowed_keys):
            raise MasterLaunchContractError("master terminal output has an invalid top-level contract")
        try:
            canonical = canonical_json_bytes(raw)
        except ValueError as exc:
            raise MasterLaunchContractError("master terminal output contains non-finite JSON values") from exc
        if encoded != canonical:
            raise MasterLaunchContractError("master terminal output is not canonical JSON")
        checkpoint = raw["checkpoint"]
        if not isinstance(checkpoint, dict) or set(checkpoint) != {
            "checkpoint_id",
            "manifest_sha256",
            "current_checkpoint_id",
        }:
            raise MasterLaunchContractError("master terminal checkpoint contract is invalid")
        events = raw["events"]
        if not isinstance(events, list) or any(not isinstance(event, dict) for event in events):
            raise MasterLaunchContractError("master terminal events contract is invalid")
        if raw["schema_version"] != "my-data-hub-master-terminal.v1":
            raise MasterLaunchContractError("master terminal schema version is unsupported")
        string_fields = (
            "run_id",
            "attempt_id",
            "service_instance_id",
            "master_instance_id",
            "source_identity",
            "source_version",
            "executed_source_sha256",
            "status",
        )
        if (
            any(not isinstance(raw[field], str) for field in string_fields)
            or not isinstance(raw["epoch"], int)
            or isinstance(raw["epoch"], bool)
            or any(not isinstance(checkpoint[field], str) for field in checkpoint)
        ):
            raise MasterLaunchContractError("master terminal output field types are invalid")
        encoded_events = tuple(
            json.dumps(event, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode() for event in events
        )
        blogger_import_receipt: dict[str, object] | None = None
        try:
            if raw.get("blogger_import_receipt") is not None:
                parsed_blogger_receipt = BloggerImportStageReceipt.model_validate(raw["blogger_import_receipt"])
                if (
                    parsed_blogger_receipt.run_id != raw["run_id"]
                    or parsed_blogger_receipt.epoch != raw["epoch"]
                    or str(parsed_blogger_receipt.master_instance_id) != raw["master_instance_id"]
                ):
                    raise ValueError("blogger receipt differs from terminal runtime identity")
                blogger_import_receipt = parsed_blogger_receipt.model_dump(mode="json")
            return MasterTerminalOutput(
                run_id=raw["run_id"],
                attempt_id=raw["attempt_id"],
                service_instance_id=raw["service_instance_id"],
                master_instance_id=raw["master_instance_id"],
                source_identity=raw["source_identity"],
                source_version=raw["source_version"],
                executed_source_sha256=raw["executed_source_sha256"],
                epoch=raw["epoch"],
                status=raw["status"],
                checkpoint_id=checkpoint["checkpoint_id"],
                manifest_sha256=checkpoint["manifest_sha256"],
                current_checkpoint_id=checkpoint["current_checkpoint_id"],
                recovered_events=encoded_events,
                output_tree_sha256=output_tree_sha256,
                output_receipt_sha256=hashlib.sha256(encoded).hexdigest(),
                blogger_import_receipt=blogger_import_receipt,
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise MasterLaunchContractError("master terminal output values are invalid") from exc

    def _validate_effect(self, effect: PlannedProviderEffect) -> None:
        expected = {
            "ensure_dataset": self.assets.dataset_ref,
            "push_notebook": self.assets.notebook_ref,
        }.get(effect.effect_kind)
        if expected is not None and effect.exact_identity.get("exact_ref") != expected:
            raise MasterLaunchContractError("provider effect ref differs from configured exact launch identity")
        if effect.exact_identity.get("source_identity") != self.assets.source_identity:
            raise MasterLaunchContractError("provider effect source identity differs from launch assets")
        if effect.exact_identity.get("source_version") != self.assets.source_version:
            raise MasterLaunchContractError("provider effect source version differs from launch assets")

    def _source(self, identity: Mapping[str, Any]) -> bytes:
        status = self._status_authority_row(identity)
        status_dataset = status.get("status_dataset")
        if not isinstance(status_dataset, Mapping):
            raise MasterLaunchContractError("master status Dataset claim is absent")
        pin_payload = self._execution_pins(identity)
        values = self.assets.render_values(identity)
        if pin_payload is not None:
            values.update({
                "MY_DATA_HUB_INPUT_DATASET_VERSIONS_JSON": json.dumps(
                    pin_payload["input_dataset_versions"], separators=(",", ":")
                ),
                "MY_DATA_HUB_NOTEBOOK_IS_PRIVATE": "true",
                "MY_DATA_HUB_KAGGLE_RUNTIME_IMAGE_IDENTITY": self.assets.runtime_image_identity,
                "MY_DATA_HUB_KAGGLE_RUNTIME_SOURCE_COMMIT": self.assets.runtime_image_source_commit,
            })
        return render_notebook_source(
            self.assets.notebook_source,
            kernel_type=self.assets.notebook_kernel_type,
            values=values,
            status_dataset_ref=str(status_dataset["provider_ref"]),
            status_config_sha256=str(status_dataset["status_config_sha256"]),
            status_helper_sha256=str(status_dataset["status_helper_sha256"]),
            master_config_sha256=str(status_dataset["master_config_sha256"]),
            secret_bindings=self.assets.runtime_secret_bindings,
            execution_pins=(canonical_json_bytes(pin_payload) if pin_payload is not None else None),
        )

    def _assert_boot_checkpoint_current(self, status: Mapping[str, Any]) -> None:
        status_dataset = status.get("status_dataset")
        boot = status_dataset.get("boot_checkpoint") if isinstance(status_dataset, Mapping) else None
        head_lookup = getattr(self.status_authority, "checkpoint_head", None)
        candidate_lookup = getattr(self.status_authority, "checkpoint_candidate", None)
        if not isinstance(boot, Mapping) or not callable(head_lookup) or not callable(candidate_lookup):
            raise MasterLaunchContractError("master boot checkpoint CAS authority is unavailable")
        head = head_lookup("postgres-master")
        if boot.get("kind") == "EMPTY":
            if head is not None and head.current_checkpoint_id is not None:
                raise MasterLaunchContractError("checkpoint HEAD advanced after EMPTY boot admission")
            return
        if boot.get("kind") != "VERIFIED" or head is None:
            raise MasterLaunchContractError("verified master boot checkpoint is unavailable")
        candidate = candidate_lookup(str(boot.get("checkpoint_id")))
        if (
            head.generation != int(boot.get("generation", -1))
            or head.current_checkpoint_id != boot.get("checkpoint_id")
            or candidate is None
            or candidate.get("status") != "VERIFIED"
            or candidate.get("dataset_ref") != self.assets.checkpoint_ref
            or candidate.get("version_ref") != boot.get("exact_version_ref")
            or candidate.get("manifest_sha256") != boot.get("manifest_sha256")
        ):
            raise MasterLaunchContractError("checkpoint HEAD changed after master boot admission")

    def _status_authority_row(self, identity: Mapping[str, Any]) -> Mapping[str, Any]:
        lookup = getattr(self.status_authority, "master_status_dataset_authority", None)
        if not callable(lookup):
            raise MasterLaunchContractError("master status Dataset authority is unavailable")
        row = lookup(str(identity["operation_id"]))
        if (
            not isinstance(row, Mapping)
            or row.get("run_id") != str(identity["run_id"])
            or row.get("attempt_id") != str(identity["attempt_id"])
            or row.get("state") not in {"READY", "CLEANED"}
        ):
            raise MasterLaunchContractError("master status Dataset authority differs from attempt")
        return row

    def _canonical_source(self, source: bytes) -> bytes:
        # Retained for callers that need canonical bytes; source identity is
        # the digest of this executable-only representation.
        from .source_attestation import canonical_executable_source

        return canonical_executable_source(source, kernel_type=self.assets.notebook_kernel_type)

    @staticmethod
    def _mapping_sha(files: Mapping[str, bytes]) -> str:
        from .adapter import mapping_sha256

        return mapping_sha256(files)

    @staticmethod
    def _intent(
        effect: PlannedProviderEffect,
        action: MutationAction,
        provider_ref: str,
        arguments: Mapping[str, Any],
    ) -> ProviderEffectIntent:
        return ProviderEffectIntent.create(
            operation_id=UUID(str(effect.exact_identity.get("operation_id", effect.idempotency_key.split(":", 1)[0]))),
            effect_id=UUID(effect.effect_id),
            idempotency_key=effect.idempotency_key,
            task_id=UUID(str(effect.exact_identity["run_id"])),
            action=action,
            provider_ref=provider_ref,
            arguments=arguments,
            requested_at=datetime.fromisoformat(
                str(effect.exact_identity["operation_requested_at"]).replace("Z", "+00:00")
            ),
        )

    def _receipt(
        self, effect: PlannedProviderEffect, exact_ref: str, exact_identity: dict[str, Any]
    ) -> ProviderEffectReceipt:
        return ProviderEffectReceipt(
            provider="kaggle",
            effect_kind=effect.effect_kind,
            exact_ref=exact_ref,
            source_identity=self.assets.source_identity,
            source_version=self.assets.source_version,
            exact_identity=exact_identity,
        )

    def _notebook_receipt(self, effect: PlannedProviderEffect, result: NotebookMutationResult) -> ProviderEffectReceipt:
        return self._receipt(effect, result.run.provider_run_ref, result.run.model_dump(mode="json"))

    @staticmethod
    def _run_from_identity(value: object) -> KaggleKernelRunIdentity:
        if not isinstance(value, dict):
            raise MasterLaunchContractError("trigger_run lacks exact notebook launch identity")
        return KaggleKernelRunIdentity.model_validate(value)
