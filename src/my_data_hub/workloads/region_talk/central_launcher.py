"""Central-launch contracts and bootstrap source for the private supervisor.

Provider mutations stay behind :class:`RegionTalkNotebookLaunchPort` in
``pipeline_runtime``.  This module owns the separate Region Talk capability
envelope and generated Notebook bootstrap without depending on the embedding
credential poller or its shared-account revocation rules.
"""

from __future__ import annotations

import hashlib
from datetime import datetime
from typing import Literal, Protocol
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, SecretStr, model_validator

from my_data_hub.hashing import canonical_json_bytes

from .pipeline_contracts import (
    RegionTalkDirectMasterAccess,
    RegionTalkLaunchMetadata,
    TaskWorkerCredentialBatch,
    TaskWorkerCredentialCommand,
    TaskWorkerCredentialRegistration,
    TaskWorkerCredentialRegistrationResponse,
    TaskWorkerCredentialRevocation,
)


class TaskBoundDirectMasterAccessPort(Protocol):
    """Adapter to the generic master-polled task credential command channel."""

    def commands(self, master_run_id: str, master_attempt_id: str) -> TaskWorkerCredentialBatch: ...

    def request(self, command: TaskWorkerCredentialCommand) -> None: ...

    def register(
        self, registration: TaskWorkerCredentialRegistration
    ) -> TaskWorkerCredentialRegistrationResponse: ...

    def request_revocation(self, revocation: TaskWorkerCredentialRevocation) -> None: ...

    def direct_access(
        self, command: TaskWorkerCredentialCommand, *, task_token: SecretStr
    ) -> RegionTalkDirectMasterAccess | None: ...


class RegionTalkSupervisorCapability(BaseModel):
    """Task-private Dataset payload. It is categorically not journal data."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["region-talk-supervisor-capability.v1"] = (
        "region-talk-supervisor-capability.v1"
    )
    launch: RegionTalkLaunchMetadata
    direct_access: RegionTalkDirectMasterAccess
    callback_base_url: str = Field(min_length=12, max_length=500)
    task_token: SecretStr
    task_token_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")

    @model_validator(mode="after")
    def exact_binding(self) -> RegionTalkSupervisorCapability:
        parsed = urlsplit(self.callback_base_url)
        if (
            parsed.scheme != "https"
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError("callback_base_url must be credential-free HTTPS")
        access = self.direct_access
        if (
            access.task_run_id != self.launch.task_run_id
            or access.master_instance_id != self.launch.master.master_instance_id
            or access.epoch != self.launch.master.epoch
        ):
            raise ValueError("direct access differs from the launch task/master binding")
        token = self.task_token.get_secret_value()
        if hashlib.sha256(token.encode()).hexdigest() != self.task_token_sha256:
            raise ValueError("task_token_sha256 differs from the private token")
        if access.task_token_sha256 != self.task_token_sha256:
            raise ValueError("direct access differs from the task token binding")
        return self

    def private_dataset_bytes(self) -> bytes:
        """Serialize only for the disposable private status Dataset."""

        value = self.model_dump(mode="json")
        value["task_token"] = self.task_token.get_secret_value()
        direct = self.direct_access
        value["direct_access"].update(
            {
                "database_url": direct.database_url.get_secret_value(),
                "tls_ca_pem": direct.tls_ca_pem.get_secret_value(),
                "ssh_private_key": direct.ssh_private_key.get_secret_value(),
                "ssh_certificate": direct.ssh_certificate.get_secret_value(),
                "ssh_known_hosts": direct.ssh_known_hosts.get_secret_value(),
            }
        )
        return canonical_json_bytes(value)


class RegionTalkBootstrapConfig(BaseModel):
    """Immutable code-generation inputs for a bounded supervisor Notebook."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    capability_filename: str = Field(default="region-talk-supervisor.json", pattern=r"^[A-Za-z0-9_.-]+$")
    cycle_executor_factory: str = Field(
        default="my_data_hub.workloads.region_talk.direct_pipeline:build_cycle_executor",
        pattern=r"^[A-Za-z_][A-Za-z0-9_.]*:[A-Za-z_][A-Za-z0-9_]*$",
    )
    attestation_path: str = Field(default="/internal/region-talk-pipeline/attest", pattern=r"^/[A-Za-z0-9_./-]+$")
    running_path: str = Field(default="/internal/region-talk-pipeline/running", pattern=r"^/[A-Za-z0-9_./-]+$")
    terminal_path: str = Field(default="/internal/region-talk-pipeline/terminal", pattern=r"^/[A-Za-z0-9_./-]+$")


def render_region_talk_supervisor_source(
    metadata: RegionTalkLaunchMetadata,
    *,
    config: RegionTalkBootstrapConfig | None = None,
) -> bytes:
    """Render a source/image/epoch-attested finite Kaggle supervisor.

    The generated code posts the attestation and requires a 2xx response before
    exporting ``database_url`` or constructing the cycle executor.  It also
    rejects ambiguous status/wheel inputs and hard-codes publication dispatch
    to false.
    """

    config = config or RegionTalkBootstrapConfig()
    expected_launch = metadata.model_dump(mode="json")
    lines = [
        "import hashlib, importlib, json, os, pathlib, subprocess, time, urllib.request",
        f"EXPECTED_LAUNCH={expected_launch!r}",
        f"CAPABILITY_FILENAME={config.capability_filename!r}",
        f"EXECUTOR_FACTORY={config.cycle_executor_factory!r}",
        f"ATTESTATION_PATH={config.attestation_path!r}",
        f"RUNNING_PATH={config.running_path!r}",
        f"TERMINAL_PATH={config.terminal_path!r}",
        'INPUT_ROOT=pathlib.Path("/kaggle/input")',
        'WORK_ROOT=pathlib.Path("/kaggle/working")',
        'if not INPUT_ROOT.is_dir() or INPUT_ROOT.is_symlink(): raise RuntimeError("unsafe Kaggle input root")',
        "def safe_file(path, limit):",
        "    rel=path.relative_to(INPUT_ROOT)",
        (
            "    return path.is_file() and not path.is_symlink() and path.stat().st_size<=limit "
            "and not any(INPUT_ROOT.joinpath(*rel.parts[:i]).is_symlink() "
            "for i in range(1,len(rel.parts)))"
        ),
        "matches=[]",
        "for candidate in INPUT_ROOT.rglob(CAPABILITY_FILENAME):",
        "    if not safe_file(candidate,1048576): continue",
        "    try: value=json.loads(candidate.read_bytes())",
        "    except (OSError,ValueError): continue",
        (
            '    if value.get("launch",{}).get("task_run_id")==EXPECTED_LAUNCH["task_run_id"]: '
            "matches.append((candidate,value))"
        ),
        'if len(matches)!=1: raise RuntimeError("exact Region Talk capability input is absent or ambiguous")',
        "capability_file,capability=matches[0]",
        (
            'if capability.get("schema_version")!="region-talk-supervisor-capability.v1": '
            'raise RuntimeError("capability schema differs")'
        ),
        'if capability.get("launch")!=EXPECTED_LAUNCH: raise RuntimeError("capability launch binding differs")',
        'launch=capability["launch"]; access=capability["direct_access"]',
        (
            'if launch["publication_dispatch"] is not False: '
            'raise RuntimeError("publication dispatch must remain disabled")'
        ),
        (
            'if access["task_run_id"]!=launch["task_run_id"] or '
            'access["master_instance_id"]!=launch["master"]["master_instance_id"] or '
            'int(access["epoch"])!=int(launch["master"]["epoch"]): '
            'raise RuntimeError("direct access binding differs")'
        ),
        'wheel_name=pathlib.Path(launch["wheel_relative_path"]).name',
        "wheel_matches=[]",
        "for candidate in INPUT_ROOT.rglob(wheel_name):",
        (
            "    if safe_file(candidate,134217728) and "
            'hashlib.sha256(candidate.read_bytes()).hexdigest()==launch["wheel_sha256"]: '
            "wheel_matches.append(candidate)"
        ),
        'if len(wheel_matches)!=1: raise RuntimeError("exact Region Talk runtime wheel is absent or ambiguous")',
        "wheel=wheel_matches[0]",
        'observed_commit=pathlib.Path("/etc/git_commit").read_text().strip()',
        (
            'if observed_commit!=launch["runtime_image_source_commit"]: '
            'raise RuntimeError("runtime image source commit differs")'
        ),
        'source_sha256=hashlib.sha256(pathlib.Path(__file__).read_bytes()).hexdigest()',
        "def post_metadata(path, payload):",
        '    body=json.dumps(payload,separators=(",",":"),sort_keys=True).encode()',
        (
            '    request=urllib.request.Request(capability["callback_base_url"].rstrip("/")+path,'
            'data=body,headers={"Authorization":"Bearer "+capability["task_token"],'
            '"Content-Type":"application/json"},method="POST")'
        ),
        "    with urllib.request.urlopen(request,timeout=30) as response:",
        '        if not 200<=int(response.status)<300: raise RuntimeError("metadata callback rejected")',
        "        return json.loads(response.read(262144) or b'{}')",
        "attested_at=__import__('datetime').datetime.now(__import__('datetime').UTC).isoformat()",
        "post_metadata(ATTESTATION_PATH,{",
        '    "schema_version":"region-talk-runtime-attestation.v1",',
        '    "request_id":launch["request_id"],"task_run_id":launch["task_run_id"],',
        '    "master_instance_id":launch["master"]["master_instance_id"],"epoch":launch["master"]["epoch"],',
        '    "source_sha256":source_sha256,"image_identity":launch["runtime_image_identity"],',
        '    "image_source_commit":observed_commit,"attested_at":attested_at,',
        "})",
        "# No PostgreSQL capability is materialized before the successful attestation above.",
        'ca=WORK_ROOT/"region-talk-master-ca.pem"; ca.write_text(access["tls_ca_pem"]); ca.chmod(0o600)',
        'key=WORK_ROOT/"region-talk-task-key"; key.write_text(access["ssh_private_key"]); key.chmod(0o600)',
        (
            'certificate=WORK_ROOT/"region-talk-task-key-cert.pub"; '
            'certificate.write_text(access["ssh_certificate"]+"\\n"); certificate.chmod(0o600)'
        ),
        'known=WORK_ROOT/"region-talk-known-hosts"; known.write_text(access["ssh_known_hosts"]); known.chmod(0o600)',
        "local_port=25434",
        'destination=f"{access[\'ssh_account\']}@{access[\'ssh_gateway_host\']}"',
        'ssh=subprocess.Popen(["ssh","-N","-L",f"127.0.0.1:{local_port}:127.0.0.1:25432","-i",str(key),"-o",f"CertificateFile={certificate}","-o",f"UserKnownHostsFile={known}","-o","StrictHostKeyChecking=yes","-p",str(access["ssh_gateway_port"]),destination])',
        'time.sleep(2);\nif ssh.poll() is not None: raise RuntimeError("task-bound SSH tunnel failed")',
        'database_url=access["database_url"].replace(access["tunnel_endpoint"],f"127.0.0.1:{local_port}")',
        'subprocess.run([__import__("sys").executable,"-m","pip","install","--no-deps",str(wheel)],check=True)',
        'module_name,factory_name=EXECUTOR_FACTORY.split(":",1)',
        "factory=getattr(importlib.import_module(module_name),factory_name)",
        "post_metadata(RUNNING_PATH,{",
        '    "task_run_id":launch["task_run_id"],"master_instance_id":launch["master"]["master_instance_id"],',
        '    "epoch":launch["master"]["epoch"],"publication_dispatch":False,',
        "})",
        "executor=factory(database_url=database_url,tls_ca_path=str(ca),task_run_id=launch['task_run_id'],master_instance_id=launch['master']['master_instance_id'],epoch=int(launch['master']['epoch']),source_revision=launch.get('source_revision'),publication_dispatch=False)",
        'runtime=importlib.import_module("my_data_hub.workloads.region_talk.pipeline_runtime")',
        "result=runtime.run_bounded_supervisor(executor=executor,task_run_id=__import__('uuid').UUID(launch['task_run_id']),master_instance_id=__import__('uuid').UUID(launch['master']['master_instance_id']),epoch=int(launch['master']['epoch']),max_cycles=int(launch['max_cycles']),max_runtime_seconds=int(launch['max_runtime_seconds']))",
        "completed_at=__import__('datetime').datetime.now(__import__('datetime').UTC).isoformat()",
        "post_metadata(TERMINAL_PATH,{",
        '    "schema_version":"region-talk-terminal-receipt.v1","request_id":launch["request_id"],',
        '    "task_run_id":launch["task_run_id"],"master_instance_id":launch["master"]["master_instance_id"],',
        '    "epoch":launch["master"]["epoch"],"status":"SUCCEEDED" if result.completed else "FAILED",',
        '    "cycles_completed":result.cycles_completed,"rows_observed":result.rows_observed,',
        '    "rows_changed":result.rows_changed,"queue_revision":None,',
        '    "aggregate_receipt_sha256":result.aggregate_receipt_sha256,"completed_at":completed_at,',
        '    "publication_dispatch":False,',
        "})",
    ]
    return ("\n".join(lines) + "\n").encode()


def source_sha256(source: bytes) -> str:
    return hashlib.sha256(source).hexdigest()


def capability_expired(value: RegionTalkDirectMasterAccess, *, now: datetime) -> bool:
    return value.expires_at <= now.astimezone(value.expires_at.tzinfo)
