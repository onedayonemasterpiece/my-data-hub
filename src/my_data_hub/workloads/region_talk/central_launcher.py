# ruff: noqa: E501
"""Central-launch contracts and bootstrap source for the private supervisor.

Provider mutations stay behind :class:`RegionTalkNotebookLaunchPort` in
``pipeline_runtime``.  This module owns the separate Region Talk capability
envelope and generated Notebook bootstrap without depending on the embedding
credential poller or its shared-account revocation rules.
"""

from __future__ import annotations

import hashlib
from datetime import datetime
from typing import Any, Literal, Protocol
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
from .stage_dispatch import (
    StageWorkerBindingReceipt,
    StageWorkerLaunch,
    StageWorkerRotateReceipt,
    StageWorkMetadataClaimReceipt,
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


class RegionTalkStageWorkerCapability(BaseModel):
    """Private child Dataset payload; never valid as callback or journal data."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["region-talk-stage-worker-capability.v1"] = (
        "region-talk-stage-worker-capability.v1"
    )
    launch: StageWorkerLaunch
    direct_access: RegionTalkDirectMasterAccess
    callback_base_url: str = Field(min_length=12, max_length=500)
    task_token: SecretStr
    task_token_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    runtime_dataset_exact_ref: str = Field(
        pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/[1-9][0-9]*$"
    )
    runtime_image_identity: str = Field(pattern=r"^[^@\s]+@sha256:[a-f0-9]{64}$")
    runtime_image_source_commit: str = Field(pattern=r"^[a-f0-9]{40}$")
    wheel_relative_path: str = Field(min_length=1, max_length=240)
    wheel_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    dependency_manifest_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    model_inputs: dict[str, Any]
    model_inputs_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    publication_dispatch: Literal[False] = False
    notification_dispatch: Literal[False] = False

    @model_validator(mode="after")
    def exact_binding(self) -> RegionTalkStageWorkerCapability:
        access = self.direct_access
        parsed = urlsplit(self.callback_base_url)
        if (
            parsed.scheme != "https"
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
            or access.task_run_id != self.launch.worker_task_run_id
            or access.master_instance_id != self.launch.master_instance_id
            or access.epoch != self.launch.epoch
            or access.generation < 1
            or access.task_token_sha256 != self.task_token_sha256
            or hashlib.sha256(
                canonical_json_bytes(self.model_inputs)
            ).hexdigest()
            != self.model_inputs_sha256
        ):
            raise ValueError("stage worker capability differs from exact launch")
        return self

    def private_dataset_bytes(self) -> bytes:
        value = self.model_dump(mode="json")
        value["task_token"] = self.task_token.get_secret_value()
        access = self.direct_access
        for field_name in (
            "database_url",
            "tls_ca_pem",
            "ssh_private_key",
            "ssh_certificate",
            "ssh_known_hosts",
        ):
            value["direct_access"][field_name] = getattr(
                access, field_name
            ).get_secret_value()
        return canonical_json_bytes(value)


class RegionTalkStageDispatchCallback(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    claim: StageWorkMetadataClaimReceipt
    binding: StageWorkerBindingReceipt


class RegionTalkStageWorkerAttestation(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["region-talk-stage-worker-attestation.v1"] = (
        "region-talk-stage-worker-attestation.v1"
    )
    worker_task_run_id: str
    dispatch_id: str
    effect_id: str
    master_instance_id: str
    epoch: int = Field(ge=1)
    source_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    image_identity: str = Field(pattern=r"^[^@\s]+@sha256:[a-f0-9]{64}$")
    image_source_commit: str = Field(pattern=r"^[a-f0-9]{40}$")
    wheel_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    dependency_manifest_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    model_inputs_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    attested_at: datetime
    publication_dispatch: Literal[False] = False
    notification_dispatch: Literal[False] = False


class RegionTalkStageWorkerRotationCheckpoint(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["region-talk-stage-worker-rotation-checkpoint.v1"] = (
        "region-talk-stage-worker-rotation-checkpoint.v1"
    )
    supervisor_task_run_id: str
    export_batch_id: str
    worker_task_run_id: str
    dispatch_id: str
    effect_id: str
    work_item_id: str
    prior_worker_generation: int = Field(ge=1)
    prior_worker_binding_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    requested_at: datetime
    publication_dispatch: Literal[False] = False
    notification_dispatch: Literal[False] = False


class RegionTalkStageSupervisorRotationPoll(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["region-talk-stage-supervisor-rotation-poll.v1"] = (
        "region-talk-stage-supervisor-rotation-poll.v1"
    )
    supervisor_task_run_id: str
    export_batch_id: str
    master_instance_id: str
    epoch: int = Field(ge=1)
    requested_at: datetime
    publication_dispatch: Literal[False] = False
    notification_dispatch: Literal[False] = False


class RegionTalkStageWorkerRotationActivation(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["region-talk-stage-worker-rotation-activation.v1"] = (
        "region-talk-stage-worker-rotation-activation.v1"
    )
    receipt: StageWorkerRotateReceipt
    activated_at: datetime
    publication_dispatch: Literal[False] = False
    notification_dispatch: Literal[False] = False


class RegionTalkStageWorkerTerminal(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["region-talk-stage-worker-terminal.v1"] = (
        "region-talk-stage-worker-terminal.v1"
    )
    worker_task_run_id: str
    dispatch_id: str
    effect_id: str
    master_instance_id: str
    epoch: int = Field(ge=1)
    result_status: Literal["SUCCEEDED", "FAILED_RETRYABLE", "FAILED_TERMINAL"]
    result_receipt_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    completed_at: datetime
    publication_dispatch: Literal[False] = False
    notification_dispatch: Literal[False] = False


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
    refresh_path: str = Field(
        default="/internal/region-talk-pipeline/access/refresh",
        pattern=r"^/[A-Za-z0-9_./-]+$",
    )
    activate_path: str = Field(
        default="/internal/region-talk-pipeline/access/activate",
        pattern=r"^/[A-Za-z0-9_./-]+$",
    )
    stage_prepare_path: str = Field(
        default="/internal/region-talk-pipeline/stage/prepare",
        pattern=r"^/[A-Za-z0-9_./-]+$",
    )
    stage_dispatch_path: str = Field(
        default="/internal/region-talk-pipeline/stage/dispatch",
        pattern=r"^/[A-Za-z0-9_./-]+$",
    )
    stage_rotation_poll_path: str = Field(
        default="/internal/region-talk-pipeline/stage/rotation/poll",
        pattern=r"^/[A-Za-z0-9_./-]+$",
    )
    stage_rotation_activate_path: str = Field(
        default="/internal/region-talk-pipeline/stage/rotation/activate",
        pattern=r"^/[A-Za-z0-9_./-]+$",
    )


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
        f"REFRESH_PATH={config.refresh_path!r}",
        f"ACTIVATE_PATH={config.activate_path!r}",
        f"STAGE_PREPARE_PATH={config.stage_prepare_path!r}",
        f"STAGE_DISPATCH_PATH={config.stage_dispatch_path!r}",
        f"STAGE_ROTATION_POLL_PATH={config.stage_rotation_poll_path!r}",
        f"STAGE_ROTATION_ACTIVATE_PATH={config.stage_rotation_activate_path!r}",
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
        "def dataset_root(path): return INPUT_ROOT/path.relative_to(INPUT_ROOT).parts[0]",
        "runtime_dataset_root=dataset_root(wheel)",
        "ydb_manifest_matches=[]",
        "for candidate in INPUT_ROOT.rglob('master-ydb-dependency.json'):",
        "    if (safe_file(candidate,65536) and hashlib.sha256(candidate.read_bytes()).hexdigest()==",
        "        launch['ydb_dependency_manifest_sha256']): ydb_manifest_matches.append(candidate)",
        'if len(ydb_manifest_matches)!=1: raise RuntimeError("exact YDB dependency manifest is absent or ambiguous")',
        "ydb_manifest_path=ydb_manifest_matches[0]",
        "if dataset_root(ydb_manifest_path)!=runtime_dataset_root:",
        '    raise RuntimeError("YDB dependency manifest Dataset differs")',
        "ydb_manifest_body=ydb_manifest_path.read_bytes()",
        "try: ydb_manifest=json.loads(ydb_manifest_body)",
        'except ValueError as exc: raise RuntimeError("YDB dependency manifest is invalid JSON") from exc',
        "if ydb_manifest_body!=json.dumps(ydb_manifest,sort_keys=True,separators=(',',':')).encode():",
        '    raise RuntimeError("YDB dependency manifest is not canonical JSON")',
        "expected_ydb_distributions={'aiohappyeyeballs','aiohttp','aiosignal','attrs','frozenlist',",
        "    'grpcio','idna','multidict','packaging','propcache','protobuf','typing-extensions','yarl','ydb'}",
        "ydb_wheels=ydb_manifest.get('wheels') if isinstance(ydb_manifest,dict) else None",
        "if (set(ydb_manifest)!={'schema_version','index_url','runtime','root_requirement',",
        "    'install_order','wheels'} or",
        "    ydb_manifest.get('schema_version')!='my-data-hub-master-ydb-wheel-lock.v2' or",
        "    ydb_manifest.get('index_url')!='https://pypi.org/simple' or",
        "    ydb_manifest.get('root_requirement')!='ydb==3.31.2' or",
        "    ydb_manifest.get('runtime')!={'python_abi':'cp312','platform':'manylinux2014_x86_64',",
        "      'source_commit':launch['runtime_image_source_commit']} or not isinstance(ydb_wheels,list) or",
        "    len(ydb_wheels)!=14 or ydb_manifest.get('install_order')!=",
        "      [item.get('filename') for item in ydb_wheels if isinstance(item,dict)] or",
        "    {item.get('distribution') for item in ydb_wheels if isinstance(item,dict)}!=expected_ydb_distributions):",
        '    raise RuntimeError("YDB dependency closure differs")',
        "ydb_dependency_paths=[]",
        "for item in ydb_wheels:",
        "    if (not isinstance(item,dict) or set(item)!={'distribution','version','filename',",
        "        'sha256','source_url'} or",
        "        pathlib.Path(str(item.get('filename',''))).name!=item.get('filename') or",
        "        not isinstance(item.get('sha256'),str) or len(item['sha256'])!=64):",
        '        raise RuntimeError("YDB dependency wheel identity differs")',
        "    matches=[]",
        "    for candidate in INPUT_ROOT.rglob(item['filename']):",
        "        if (safe_file(candidate,16777216) and dataset_root(candidate)==runtime_dataset_root and",
        "            hashlib.sha256(candidate.read_bytes()).hexdigest()==item['sha256']): matches.append(candidate)",
        '    if len(matches)!=1: raise RuntimeError("exact YDB dependency wheel is absent or ambiguous")',
        "    ydb_dependency_paths.append((item,matches[0]))",
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
        "def post_metadata_with_replay(path,payload):",
        "    error=None",
        "    for _attempt in range(3):",
        "        try: return post_metadata(path,payload)",
        "        except Exception as exc: error=exc; time.sleep(2)",
        "    raise error",
        "def access_binding(value):",
        "    keys=('credential_id','generation','command_sha256','task_token_sha256',",
        "          'expires_at','ssh_certificate_serial')",
        "    return {key:value[key] for key in keys}",
        "def refresh_access(previous):",
        "    requested_at=__import__('datetime').datetime.now(__import__('datetime').UTC).isoformat()",
        "    return post_metadata(REFRESH_PATH,{",
        '        "schema_version":"region-talk-credential-refresh.v1","request_id":launch["request_id"],',
        '        "task_run_id":launch["task_run_id"],"master_instance_id":launch["master"]["master_instance_id"],',
        '        "epoch":launch["master"]["epoch"],"source_sha256":source_sha256,',
        '        "image_identity":launch["runtime_image_identity"],"image_source_commit":observed_commit,',
        '        "previous":access_binding(previous),"requested_at":requested_at,"publication_dispatch":False,',
        "    })",
        "def activate_access(previous,replacement):",
        "    asserted_at=__import__('datetime').datetime.now(__import__('datetime').UTC).isoformat()",
        "    return post_metadata(ACTIVATE_PATH,{",
        '        "schema_version":"region-talk-credential-activation.v1","request_id":launch["request_id"],',
        '        "task_run_id":launch["task_run_id"],"master_instance_id":launch["master"]["master_instance_id"],',
        '        "epoch":launch["master"]["epoch"],"source_sha256":source_sha256,',
        '        "image_identity":launch["runtime_image_identity"],"image_source_commit":observed_commit,',
        '        "previous":access_binding(previous),"replacement":access_binding(replacement),',
        '        "asserted_at":asserted_at,"publication_dispatch":False,',
        "    })",
        "attested_at=__import__('datetime').datetime.now(__import__('datetime').UTC).isoformat()",
        "post_metadata(ATTESTATION_PATH,{",
        '    "schema_version":"region-talk-runtime-attestation.v1",',
        '    "request_id":launch["request_id"],"task_run_id":launch["task_run_id"],',
        '    "master_instance_id":launch["master"]["master_instance_id"],"epoch":launch["master"]["epoch"],',
        '    "source_sha256":source_sha256,"image_identity":launch["runtime_image_identity"],',
        '    "image_source_commit":observed_commit,"attested_at":attested_at,',
        "})",
        "# No PostgreSQL capability is materialized before the successful attestation above.",
        "metadata=__import__('importlib.metadata',fromlist=['version'])",
        "for item,path in ydb_dependency_paths:",
        "    subprocess.run([__import__('sys').executable,'-m','pip','install','--no-index','--no-deps',",
        "                    '--force-reinstall','--disable-pip-version-check',str(path)],check=True)",
        "    if metadata.version(item['distribution'])!=item['version']:",
        '        raise RuntimeError("YDB dependency version differs after offline install")',
        'if metadata.version("ydb")!="3.31.2": raise RuntimeError("exact YDB SDK is unavailable")',
        'subprocess.run([__import__("sys").executable,"-m","pip","install","--no-index","--no-deps",str(wheel)],check=True)',
        'module_name,factory_name=EXECUTOR_FACTORY.split(":",1)',
        "factory=getattr(importlib.import_module(module_name),factory_name)",
        "# The reviewed Kaggle User Secret is attached to this private Notebook in the UI.",
        "# Only its label is in launch metadata; the value never enters Dataset/status/log output.",
        "secrets_client=__import__('kaggle_secrets',fromlist=['UserSecretsClient']).UserSecretsClient()",
        "ydb_viewer_json=secrets_client.get_secret(launch['ydb_viewer_secret_label'])",
        "if not isinstance(ydb_viewer_json,str) or not 64<=len(ydb_viewer_json)<=65536:",
        '    raise RuntimeError("reviewed YDB viewer secret is absent or invalid")',
        "try: ydb_viewer_value=json.loads(ydb_viewer_json)",
        'except ValueError as exc: raise RuntimeError("reviewed YDB viewer secret is not JSON") from exc',
        'if not isinstance(ydb_viewer_value,dict): raise RuntimeError("reviewed YDB viewer secret is invalid")',
        'ydb_key=WORK_ROOT/("region-talk-ydb-viewer-"+launch["task_run_id"]+".json")',
        "descriptor=os.open(ydb_key,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)",
        "with os.fdopen(descriptor,'w',encoding='utf-8') as stream:",
        "    stream.write(ydb_viewer_json); stream.flush(); os.fsync(stream.fileno())",
        "del ydb_viewer_json,ydb_viewer_value,secrets_client",
        "os.environ['MY_DATA_HUB_YDB_ENDPOINT']=launch['ydb_endpoint']",
        "os.environ['MY_DATA_HUB_YDB_DATABASE']=launch['ydb_database']",
        "os.environ['YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS']=str(ydb_key)",
        "def materialize(value):",
        "    generation=int(value['generation']); suffix=str(generation)",
        '    ca=WORK_ROOT/("region-talk-master-ca-"+suffix+".pem")',
        '    ca.write_text(value["tls_ca_pem"]); ca.chmod(0o600)',
        '    key=WORK_ROOT/("region-talk-task-key-"+suffix)',
        '    key.write_text(value["ssh_private_key"]); key.chmod(0o600)',
        '    certificate=WORK_ROOT/("region-talk-task-key-"+suffix+"-cert.pub")',
        '    certificate.write_text(value["ssh_certificate"]+"\\n")',
        "    certificate.chmod(0o600)",
        '    known=WORK_ROOT/("region-talk-known-hosts-"+suffix)',
        '    known.write_text(value["ssh_known_hosts"]); known.chmod(0o600)',
        "    local_port=25434+(generation%96)",
        '    destination=f"{value[\'ssh_account\']}@{value[\'ssh_gateway_host\']}"',
        '    command=["ssh","-N","-L",f"127.0.0.1:{local_port}:127.0.0.1:25432",',
        '             "-i",str(key),"-o",f"CertificateFile={certificate}",',
        '             "-o",f"UserKnownHostsFile={known}","-o","StrictHostKeyChecking=yes",',
        '             "-p",str(value["ssh_gateway_port"]),destination]',
        "    tunnel=subprocess.Popen(command)",
        '    time.sleep(2)\n    if tunnel.poll() is not None: raise RuntimeError("task-bound SSH tunnel failed")',
        '    database_url=value["database_url"].replace(value["tunnel_endpoint"],f"127.0.0.1:{local_port}")',
        "    urlmod=__import__('urllib.parse',fromlist=['parse_qsl'])",
        "    parsed=urlmod.urlsplit(database_url); query=dict(urlmod.parse_qsl(parsed.query,keep_blank_values=True))",
        "    query['sslrootcert']=str(ca)",
        "    database_url=urlmod.urlunsplit((parsed.scheme,parsed.netloc,parsed.path,",
        "                                         urlmod.urlencode(query),parsed.fragment))",
        "    try:",
        "        built=factory(database_url=database_url,tls_ca_path=str(ca),",
        "                      task_run_id=launch['task_run_id'],",
        "                      master_instance_id=launch['master']['master_instance_id'],",
        "                      epoch=int(launch['master']['epoch']),",
        "                      source_revision=launch.get('source_revision'),",
        "                      publication_dispatch=False)",
        "    except BaseException:",
        "        tunnel.terminate(); tunnel.wait(timeout=10); raise",
        "    return built,tunnel",
        "def needs_refresh(value):",
        "    expiry=__import__('datetime').datetime.fromisoformat(value['expires_at'].replace('Z','+00:00'))",
        "    now=__import__('datetime').datetime.now(__import__('datetime').UTC)",
        "    return expiry<=now+__import__('datetime').timedelta(seconds=90)",
        "def refresh_with_replay(previous):",
        "    error=None",
        "    for _attempt in range(3):",
        "        try: return refresh_access(previous)",
        "        except Exception as exc: error=exc; time.sleep(2)",
        "    raise error",
        "def activate_with_replay(previous,replacement):",
        "    error=None",
        "    for _attempt in range(3):",
        "        try: return activate_access(previous,replacement)",
        "        except Exception as exc: error=exc; time.sleep(2)",
        "    raise error",
        "previous_access=None",
        "if needs_refresh(access): previous_access=access; access=refresh_with_replay(previous_access)",
        "executor,tunnel=materialize(access)",
        "if previous_access is not None: activate_with_replay(previous_access,access)",
        "class RotatingExecutor:",
        "    def __init__(self,current,current_access,current_tunnel):",
        "        self.current=current",
        "        self.access=current_access",
        "        self.tunnel=current_tunnel",
        "        self.current.set_transport_refresher(self.refresh_transport)",
        "    def refresh_transport(self,connection,**_position):",
        "        if not needs_refresh(self.access): return connection",
        "        previous=self.access; replacement=refresh_with_replay(previous)",
        "        replacement_executor,replacement_tunnel=materialize(replacement)",
        "        try: activate_with_replay(previous,replacement)",
        "        except BaseException:",
        "            replacement_executor.connection.close()",
        "            replacement_executor._ydb_driver.stop()",
        "            replacement_tunnel.terminate(); replacement_tunnel.wait(timeout=10)",
        "            raise",
        "        old_tunnel=self.tunnel",
        "        self.access,self.tunnel=replacement,replacement_tunnel",
        "        self.current.connection=replacement_executor.connection",
        "        replacement_executor._ydb_driver.stop()",
        "        connection.close(); old_tunnel.terminate(); old_tunnel.wait(timeout=10)",
        "        return self.current.connection",
        "    def execute_cycle(self,request):",
        "        self.refresh_transport(self.current.connection,phase='cycle')",
        "        return self.current.execute_cycle(request)",
        "executor=RotatingExecutor(executor,access,tunnel)",
        "stage_module=importlib.import_module('my_data_hub.workloads.region_talk.stage_dispatch')",
        "stage_functions=stage_module.PostgresStageSupervisorFunctions(executor.current.connection)",
        "class CentralStageBridge:",
        "    def prepare_worker(self,claim):",
        "        return stage_module.StageWorkerCredentialStatus.model_validate(post_metadata_with_replay(STAGE_PREPARE_PATH,claim.model_dump(mode='json')))",
        "    def dispatch_bound(self,claim,binding):",
        "        body={'claim':claim.model_dump(mode='json'),'binding':binding.model_dump(mode='json')}",
        "        return stage_module.StageProviderObservation.model_validate(post_metadata_with_replay(STAGE_DISPATCH_PATH,body))",
        "stage_bridge=CentralStageBridge()",
        "stage_coordinator=stage_module.PrivateSupervisorStageCoordinator(functions=stage_functions,bridge=stage_bridge,supervisor_task_run_id=__import__('uuid').UUID(launch['task_run_id']),export_batch_id=__import__('uuid').UUID(int=0),lease_owner='private-supervisor:'+launch['task_run_id'])",
        "class StageReconciler:",
        "    def reconcile_next(self):",
        "        stage_functions.connection=executor.current.connection",
        "        if executor.current._export_batch_id is None: return None",
        "        stage_coordinator.export_batch_id=executor.current._export_batch_id",
        "        poll={'schema_version':'region-talk-stage-supervisor-rotation-poll.v1','supervisor_task_run_id':launch['task_run_id'],'export_batch_id':str(executor.current._export_batch_id),'master_instance_id':launch['master']['master_instance_id'],'epoch':launch['master']['epoch'],'requested_at':__import__('datetime').datetime.now(__import__('datetime').UTC).isoformat(),'publication_dispatch':False,'notification_dispatch':False}",
        "        rotations=post_metadata_with_replay(STAGE_ROTATION_POLL_PATH,poll).get('requests',[])",
        "        for raw in rotations:",
        "            request=stage_module.StageWorkerRotateRequest.model_validate(raw)",
        "            receipt=stage_functions.rotate_worker(supervisor_task_run_id=__import__('uuid').UUID(launch['task_run_id']),export_batch_id=executor.current._export_batch_id,request=request)",
        "            post_metadata_with_replay(STAGE_ROTATION_ACTIVATE_PATH,{'schema_version':'region-talk-stage-worker-rotation-activation.v1','receipt':receipt.model_dump(mode='json'),'activated_at':__import__('datetime').datetime.now(__import__('datetime').UTC).isoformat(),'publication_dispatch':False,'notification_dispatch':False})",
        "        return stage_coordinator.reconcile_next()",
        "executor.current.set_stage_work_reconciler(StageReconciler())",
        "post_metadata(RUNNING_PATH,{",
        '    "task_run_id":launch["task_run_id"],"master_instance_id":launch["master"]["master_instance_id"],',
        '    "epoch":launch["master"]["epoch"],"publication_dispatch":False,',
        "})",
        'runtime=importlib.import_module("my_data_hub.workloads.region_talk.pipeline_runtime")',
        "result=runtime.run_bounded_supervisor(executor=executor,task_run_id=__import__('uuid').UUID(launch['task_run_id']),master_instance_id=__import__('uuid').UUID(launch['master']['master_instance_id']),epoch=int(launch['master']['epoch']),max_cycles=int(launch['max_cycles']),max_runtime_seconds=int(launch['max_runtime_seconds']))",
        "completed_at=__import__('datetime').datetime.now(__import__('datetime').UTC).isoformat()",
        "post_metadata_with_replay(TERMINAL_PATH,{",
        '    "schema_version":"region-talk-terminal-receipt.v1","request_id":launch["request_id"],',
        '    "task_run_id":launch["task_run_id"],"master_instance_id":launch["master"]["master_instance_id"],',
        '    "epoch":launch["master"]["epoch"],"status":"SUCCEEDED" if result.completed else "FAILED",',
        '    "cycles_completed":result.cycles_completed,"rows_observed":result.rows_observed,',
        '    "rows_changed":result.rows_changed,"queue_revision":result.queue_revision,',
        '    "aggregate_receipt_sha256":result.aggregate_receipt_sha256,"completed_at":completed_at,',
        '    "publication_dispatch":False,',
        "})",
    ]
    return ("\n".join(lines) + "\n").encode()


def source_sha256(source: bytes) -> str:
    return hashlib.sha256(source).hexdigest()


def render_region_talk_stage_worker_source(
    claim: StageWorkMetadataClaimReceipt,
    *,
    runtime_image_identity: str,
    runtime_image_source_commit: str,
    wheel_relative_path: str,
    wheel_sha256: str,
    dependency_manifest_sha256: str,
    model_inputs: dict[str, Any],
) -> bytes:
    """Render one direct PostgreSQL child with no central business payload path."""

    expected = {
        "supervisor_task_run_id": str(claim.supervisor_task_run_id),
        "export_batch_id": str(claim.export_batch_id),
        "master_instance_id": str(claim.master_instance_id),
        "epoch": claim.epoch,
        "stage_run_id": str(claim.stage_run_id),
        "work_item_id": str(claim.work_item_id),
        "effect_id": str(claim.effect_id),
        "dispatch_id": str(claim.dispatch_id),
        "worker_task_run_id": str(claim.worker_task_run_id),
        "attempt": claim.attempt,
        "stage": claim.stage,
        "contract_version": claim.contract_version,
        "input_fingerprint": claim.input_fingerprint,
        "timeout_seconds": claim.timeout_seconds,
        "claim_receipt_sha256": claim.claim_receipt_sha256,
    }
    model_sha = hashlib.sha256(canonical_json_bytes(model_inputs)).hexdigest()
    lines = [
        "import hashlib, json, os, pathlib, subprocess, time, urllib.request",
        f"EXPECTED={expected!r}",
        f"EXPECTED_IMAGE={runtime_image_identity!r}",
        f"EXPECTED_COMMIT={runtime_image_source_commit!r}",
        f"EXPECTED_WHEEL_NAME={wheel_relative_path.rsplit('/', 1)[-1]!r}",
        f"EXPECTED_WHEEL_SHA={wheel_sha256!r}",
        f"EXPECTED_DEPENDENCY_SHA={dependency_manifest_sha256!r}",
        f"EXPECTED_MODEL_INPUTS={model_inputs!r}",
        f"EXPECTED_MODEL_SHA={model_sha!r}",
        'INPUT_ROOT=pathlib.Path("/kaggle/input"); WORK_ROOT=pathlib.Path("/kaggle/working")',
        'if not INPUT_ROOT.is_dir() or INPUT_ROOT.is_symlink(): raise RuntimeError("unsafe Kaggle input root")',
        "def safe_file(path,limit):",
        "    rel=path.relative_to(INPUT_ROOT)",
        "    return path.is_file() and not path.is_symlink() and 0<path.stat().st_size<=limit and not any(INPUT_ROOT.joinpath(*rel.parts[:i]).is_symlink() for i in range(1,len(rel.parts)))",
        "matches=[]",
        "for candidate in INPUT_ROOT.rglob('region-talk-stage-worker.json'):",
        "    if not safe_file(candidate,1048576): continue",
        "    try: value=json.loads(candidate.read_bytes())",
        "    except (OSError,ValueError): continue",
        "    if value.get('launch',{}).get('worker_task_run_id')==EXPECTED['worker_task_run_id']: matches.append((candidate,value))",
        'if len(matches)!=1: raise RuntimeError("exact stage capability is absent or ambiguous")',
        "capability_file,capability=matches[0]",
        "launch=capability.get('launch'); access=capability.get('direct_access')",
        "fixed={key:launch.get(key) for key in EXPECTED}",
        'if capability.get("schema_version")!="region-talk-stage-worker-capability.v1" or fixed!=EXPECTED:',
        '    raise RuntimeError("stage capability launch differs")',
        "if capability.get('publication_dispatch') is not False or capability.get('notification_dispatch') is not False:",
        '    raise RuntimeError("stage dispatch effects must remain disabled")',
        "if capability.get('runtime_image_identity')!=EXPECTED_IMAGE or capability.get('runtime_image_source_commit')!=EXPECTED_COMMIT:",
        '    raise RuntimeError("stage runtime image pins differ")',
        "if capability.get('wheel_sha256')!=EXPECTED_WHEEL_SHA or capability.get('dependency_manifest_sha256')!=EXPECTED_DEPENDENCY_SHA:",
        '    raise RuntimeError("stage dependency pins differ")',
        "if capability.get('model_inputs')!=EXPECTED_MODEL_INPUTS or capability.get('model_inputs_sha256')!=EXPECTED_MODEL_SHA:",
        '    raise RuntimeError("stage model input pins differ")',
        "observed_commit=pathlib.Path('/etc/git_commit').read_text().strip()",
        'if observed_commit!=EXPECTED_COMMIT: raise RuntimeError("stage image source commit differs")',
        "source_sha256=hashlib.sha256(pathlib.Path(__file__).read_bytes()).hexdigest()",
        "def exact_file(name,digest,limit):",
        "    found=[path for path in INPUT_ROOT.rglob(name) if safe_file(path,limit) and hashlib.sha256(path.read_bytes()).hexdigest()==digest]",
        '    if len(found)!=1: raise RuntimeError("exact stage asset is absent or ambiguous")',
        "    return found[0]",
        "wheel=exact_file(EXPECTED_WHEEL_NAME,EXPECTED_WHEEL_SHA,134217728)",
        "dependency_path=exact_file('embedding-worker-dependencies.json',EXPECTED_DEPENDENCY_SHA,1048576)",
        "dependency_body=dependency_path.read_bytes(); dependencies=json.loads(dependency_body)",
        "if dependency_body!=json.dumps(dependencies,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode():",
        '    raise RuntimeError("stage dependency manifest is not canonical")',
        "wheels=dependencies.get('wheels') if isinstance(dependencies,dict) else None",
        "if dependencies.get('schema_version')!='my-data-hub-embedding-worker-dependencies.v1' or not isinstance(wheels,list) or not wheels:",
        '    raise RuntimeError("stage dependency manifest differs")',
        "if dependencies.get('install_order')!=[item.get('filename') for item in wheels if isinstance(item,dict)]:",
        '    raise RuntimeError("stage dependency install order differs")',
        "runtime_pins=dependencies.get('runtime',{})",
        "if runtime_pins.get('image_identity')!=EXPECTED_IMAGE or runtime_pins.get('source_commit')!=EXPECTED_COMMIT:",
        '    raise RuntimeError("stage dependency runtime pins differ")',
        "dependency_files=[]",
        "for item in wheels:",
        "    if not isinstance(item,dict) or set(item)!={'distribution','version','filename','sha256','byte_size'}:",
        '        raise RuntimeError("stage dependency entry differs")',
        "    path=exact_file(item['filename'],item['sha256'],67108864)",
        "    if not isinstance(item['byte_size'],int) or item['byte_size']!=path.stat().st_size:",
        '        raise RuntimeError("stage dependency byte size differs")',
        "    dependency_files.append((item,path))",
        "for item,path in dependency_files:",
        "    subprocess.run([__import__('sys').executable,'-m','pip','install','--no-index','--no-deps','--disable-pip-version-check',str(path)],check=True)",
        "subprocess.run([__import__('sys').executable,'-m','pip','install','--no-index','--no-deps','--disable-pip-version-check',str(wheel)],check=True)",
        "def post(path,payload):",
        "    body=json.dumps(payload,sort_keys=True,separators=(',',':')).encode()",
        "    request=urllib.request.Request(capability['callback_base_url'].rstrip('/')+path,data=body,headers={'Authorization':'Bearer '+capability['task_token'],'Content-Type':'application/json'},method='POST')",
        "    with urllib.request.urlopen(request,timeout=30) as response:",
        '        if not 200<=int(response.status)<300: raise RuntimeError("stage callback rejected")',
        "        return json.loads(response.read(262144) or b'{}')",
        "attestation={'schema_version':'region-talk-stage-worker-attestation.v1','worker_task_run_id':EXPECTED['worker_task_run_id'],'dispatch_id':EXPECTED['dispatch_id'],'effect_id':EXPECTED['effect_id'],'master_instance_id':EXPECTED['master_instance_id'],'epoch':EXPECTED['epoch'],'source_sha256':source_sha256,'image_identity':EXPECTED_IMAGE,'image_source_commit':observed_commit,'wheel_sha256':EXPECTED_WHEEL_SHA,'dependency_manifest_sha256':EXPECTED_DEPENDENCY_SHA,'model_inputs_sha256':EXPECTED_MODEL_SHA,'attested_at':__import__('datetime').datetime.now(__import__('datetime').UTC).isoformat(),'publication_dispatch':False,'notification_dispatch':False}",
        "post('/internal/region-talk-pipeline/stage/attestation',attestation)",
        "def materialize(value):",
        "    suffix=str(value['generation']); ca=WORK_ROOT/('stage-ca-'+suffix+'.pem'); ca.write_text(value['tls_ca_pem']); ca.chmod(0o600)",
        "    key=WORK_ROOT/('stage-key-'+suffix); key.write_text(value['ssh_private_key']); key.chmod(0o600)",
        "    cert=WORK_ROOT/('stage-key-'+suffix+'-cert.pub'); cert.write_text(value['ssh_certificate']+'\\n'); cert.chmod(0o600)",
        "    known=WORK_ROOT/('stage-known-'+suffix); known.write_text(value['ssh_known_hosts']); known.chmod(0o600)",
        "    port=25600+(int(value['generation'])%96); destination=f\"{value['ssh_account']}@{value['ssh_gateway_host']}\"",
        "    tunnel=subprocess.Popen(['ssh','-N','-L',f'127.0.0.1:{port}:127.0.0.1:25432','-i',str(key),'-o',f'CertificateFile={cert}','-o',f'UserKnownHostsFile={known}','-o','StrictHostKeyChecking=yes','-p',str(value['ssh_gateway_port']),destination])",
        "    time.sleep(2)",
        '    if tunnel.poll() is not None: raise RuntimeError("stage tunnel failed")',
        "    database_url=value['database_url'].replace(value['tunnel_endpoint'],f'127.0.0.1:{port}')",
        "    urlmod=__import__('urllib.parse',fromlist=['parse_qsl']); parsed=urlmod.urlsplit(database_url); query=dict(urlmod.parse_qsl(parsed.query,keep_blank_values=True)); query['sslrootcert']=str(ca)",
        "    database_url=urlmod.urlunsplit((parsed.scheme,parsed.netloc,parsed.path,urlmod.urlencode(query),parsed.fragment))",
        "    psycopg=__import__('psycopg'); connection=psycopg.connect(database_url)",
        "    functions=__import__('my_data_hub.workloads.region_talk.stage_dispatch',fromlist=['PostgresStageWorkerFunctions']).PostgresStageWorkerFunctions(connection)",
        "    return functions,tunnel,connection",
        "functions,tunnel,connection=materialize(access)",
        "request_module=__import__('my_data_hub.workloads.region_talk.stage_dispatch',fromlist=['StageWorkerPayloadFetchRequest'])",
        "request=request_module.StageWorkerPayloadFetchRequest(worker_task_run_id=EXPECTED['worker_task_run_id'],dispatch_id=EXPECTED['dispatch_id'],effect_id=EXPECTED['effect_id'],worker_binding_sha256=launch['worker_binding_sha256'],requested_at=__import__('datetime').datetime.now(__import__('datetime').UTC))",
        "def checkpoint(current_functions,current_request,*,phase):",
        "    global access,functions,tunnel,connection",
        "    expiry=__import__('datetime').datetime.fromisoformat(access['expires_at'].replace('Z','+00:00')); now=__import__('datetime').datetime.now(__import__('datetime').UTC)",
        "    if expiry>now+__import__('datetime').timedelta(seconds=90): return current_functions,current_request",
        "    checkpoint_body={'schema_version':'region-talk-stage-worker-rotation-checkpoint.v1','supervisor_task_run_id':EXPECTED['supervisor_task_run_id'],'export_batch_id':EXPECTED['export_batch_id'],'worker_task_run_id':EXPECTED['worker_task_run_id'],'dispatch_id':EXPECTED['dispatch_id'],'effect_id':EXPECTED['effect_id'],'work_item_id':EXPECTED['work_item_id'],'prior_worker_generation':access['generation'],'prior_worker_binding_sha256':current_request.worker_binding_sha256,'requested_at':now.isoformat(),'publication_dispatch':False,'notification_dispatch':False}",
        "    for _attempt in range(120):",
        "        status=post('/internal/region-talk-pipeline/stage/rotation/checkpoint',checkpoint_body)",
        "        if status.get('status')=='ACTIVATED': break",
        "        time.sleep(2)",
        "    else: raise RuntimeError('stage rotation did not activate')",
        "    replacement=post('/internal/region-talk-pipeline/stage/rotation/access',checkpoint_body)",
        "    replacement_functions,replacement_tunnel,replacement_connection=materialize(replacement)",
        "    connection.close(); tunnel.terminate(); tunnel.wait(timeout=10)",
        "    access,functions,tunnel,connection=replacement,replacement_functions,replacement_tunnel,replacement_connection",
        "    return functions,current_request.model_copy(update={'worker_binding_sha256':status['worker_binding_sha256'],'requested_at':__import__('datetime').datetime.now(__import__('datetime').UTC)})",
        "worker=__import__('my_data_hub.workloads.region_talk.notebook_stages',fromlist=['execute_direct_region_talk_stage_worker','attached_stage_runtime_from_env'])",
        "runtime=worker.attached_stage_runtime_from_env(EXPECTED['stage'])",
        "result=worker.execute_direct_region_talk_stage_worker(functions,request,runtime=runtime,credential_checkpoint=checkpoint)",
        "result_body=result.model_dump(mode='json'); result_sha=hashlib.sha256(json.dumps(result_body,sort_keys=True,separators=(',',':')).encode()).hexdigest()",
        "post('/internal/region-talk-pipeline/stage/terminal',{'schema_version':'region-talk-stage-worker-terminal.v1','worker_task_run_id':EXPECTED['worker_task_run_id'],'dispatch_id':EXPECTED['dispatch_id'],'effect_id':EXPECTED['effect_id'],'master_instance_id':EXPECTED['master_instance_id'],'epoch':EXPECTED['epoch'],'result_status':result.result_status.value,'result_receipt_sha256':result_sha,'completed_at':__import__('datetime').datetime.now(__import__('datetime').UTC).isoformat(),'publication_dispatch':False,'notification_dispatch':False})",
        "connection.close(); tunnel.terminate(); tunnel.wait(timeout=10)",
    ]
    return ("\n".join(lines) + "\n").encode()


def capability_expired(value: RegionTalkDirectMasterAccess, *, now: datetime) -> bool:
    return value.expires_at <= now.astimezone(value.expires_at.tzinfo)
