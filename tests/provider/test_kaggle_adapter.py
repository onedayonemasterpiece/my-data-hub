from __future__ import annotations

import hashlib
import json
import re
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest

from my_data_hub.providers import BoundedInventory, ControlClass, ProviderKind, ProviderRegistry
from my_data_hub.providers.kaggle import (
    RUN_RECEIPT_NAME,
    EffectOutcome,
    KaggleContractError,
    KaggleIdentityError,
    KaggleKernelRunIdentity,
    KagglePolicyError,
    KaggleProviderAdapter,
    KaggleProviderIdentity,
    KernelState,
    MutationAction,
    ProviderEffectIntent,
    TaskResourceClaim,
)
from my_data_hub.providers.kaggle.adapter import _canonical_notebook_source

NOW = datetime(2026, 8, 10, 12, tzinfo=UTC)


class FakeJournal:
    def __init__(self) -> None:
        self.intents: list[ProviderEffectIntent] = []
        self.receipts: list[object] = []
        self.claims: dict[str, TaskResourceClaim] = {}
        self.fail_receipts = 0
        self.fail_claims = 0
        self.commit_then_fail_claims = 0
        self.claim_attempts: list[TaskResourceClaim] = []

    def persist_intent(self, intent: ProviderEffectIntent) -> None:
        self.intents.append(intent)

    def persist_receipt(self, receipt: object) -> None:
        if self.fail_receipts:
            self.fail_receipts -= 1
            raise RuntimeError("simulated lost receipt response")
        self.receipts.append(receipt)

    def persist_resource_claim(self, claim: TaskResourceClaim) -> None:
        self.claim_attempts.append(claim)
        if self.fail_claims:
            self.fail_claims -= 1
            raise RuntimeError("simulated lost claim response")
        for existing in self.claims.values():
            if existing.effect_id == claim.effect_id or (
                existing.provider_ref == claim.provider_ref
                and existing.kind == claim.kind
                and existing.provider_version == claim.provider_version
            ):
                if existing != claim:
                    raise KagglePolicyError("claim effect/version already has different authority")
                break
        self.claims[claim.claim_sha256] = claim
        if self.commit_then_fail_claims:
            self.commit_then_fail_claims -= 1
            raise RuntimeError("simulated lost claim response after commit")

    def assert_resource_claim(self, claim: TaskResourceClaim) -> None:
        if self.claims.get(claim.claim_sha256) != claim:
            raise KagglePolicyError("claim is not in the caller-owned ledger")


class FakeKaggleApi:
    CONFIG_NAME_USER = "username"

    def __init__(self, journal: FakeJournal) -> None:
        self.journal = journal
        self.datasets: dict[str, dict[int, dict[str, bytes]]] = {}
        self.kernels: dict[str, dict[int, bytes]] = {}
        self.outputs: dict[str, dict[str, bytes]] = {}
        self.statuses: dict[str, str] = {}
        self.calls: list[tuple[object, ...]] = []
        self.output_file_patterns: list[str | None] = []
        self.ignore_output_file_pattern = False
        self.output_logs: dict[str, bytes] = {}
        self.status_requests: list[str] = []
        self.status_after_output: str | None = None

    def authenticate(self) -> None:
        self.calls.append(("authenticate",))

    def get_config_value(self, name: str) -> str | None:
        return "owner" if name == self.CONFIG_NAME_USER else None

    def dataset_list_with_response(
        self,
        *,
        mine: bool,
        page_size: int,
        page_token: str | None,
        search: str | None = None,
        sort_by: str | None = None,
    ):
        assert mine is True
        rows = [
            SimpleNamespace(
                ref=ref,
                is_private=True,
                current_version_number=max(versions),
                status="ready",
            )
            for ref, versions in sorted(self.datasets.items())
        ]
        if search:
            rows = [row for row in rows if search in row.ref]
        return SimpleNamespace(datasets=rows[:page_size], next_page_token=None)

    def kernels_list_with_response(
        self, *, mine: bool, page_size: int, page_token: str | None, search: str | None = None
    ):
        assert mine is True
        rows = [
            SimpleNamespace(
                ref=ref,
                id=1000,
                is_private=True,
                version_number=max(versions),
                status=self.statuses.get(ref, "complete"),
            )
            for ref, versions in sorted(self.kernels.items())
        ]
        if search:
            rows = [row for row in rows if search in row.ref]
        return SimpleNamespace(kernels=rows[:page_size], next_page_token=None)

    def dataset_status(self, dataset: str, format: str | None = None) -> str:
        ref = "/".join(dataset.split("/")[:2])
        if ref not in self.datasets:
            raise HttpFailure(404)
        version = max(self.datasets[ref])
        if format:
            return json.dumps({"status": "ready", "current_version_number": version})
        return "ready"

    def dataset_create_new(
        self,
        folder: str,
        public: bool = False,
        quiet: bool = False,
        convert_to_csv: bool = True,
        dir_mode: str = "skip",
        ignore_patterns: list[str] | None = None,
    ):
        assert self.journal.intents, "intent must be durable before provider mutation"
        assert public is False
        assert convert_to_csv is False
        metadata = json.loads((Path(folder) / "dataset-metadata.json").read_bytes())
        ref = metadata["id"]
        assert ref not in self.datasets
        self.datasets[ref] = {1: read_tree(Path(folder), excluded={"dataset-metadata.json"})}
        self.calls.append(("dataset_create_new", ref, public, dir_mode))
        return SimpleNamespace(status="ok", error="")

    def dataset_create_version(
        self,
        folder: str,
        version_notes: str,
        quiet: bool = False,
        convert_to_csv: bool = True,
        delete_old_versions: bool = False,
        dir_mode: str = "skip",
        ignore_patterns: list[str] | None = None,
    ):
        assert self.journal.intents
        assert delete_old_versions is False
        metadata = json.loads((Path(folder) / "dataset-metadata.json").read_bytes())
        ref = metadata["id"]
        version = max(self.datasets[ref]) + 1
        self.datasets[ref][version] = read_tree(Path(folder), excluded={"dataset-metadata.json"})
        self.calls.append(("dataset_create_version", ref, version, version_notes))
        return SimpleNamespace(status="ok", error="")

    def dataset_download_files(
        self,
        dataset: str,
        path: str | None = None,
        force: bool = False,
        quiet: bool = True,
        unzip: bool = False,
        licenses: list[str] | None = None,
    ) -> None:
        owner, slug, version = dataset.split("/")
        self.calls.append(("dataset_download_files", dataset, path))
        write_tree(Path(path or "."), self.datasets[f"{owner}/{slug}"][int(version)])

    def dataset_delete(self, owner_slug: str | None, dataset_slug: str, no_confirm: bool = False) -> bool:
        assert self.journal.intents
        ref = f"{owner_slug}/{dataset_slug}"
        self.calls.append(("dataset_delete", ref, no_confirm))
        del self.datasets[ref]
        return True

    def kernels_push(self, folder: str, timeout: str | None = None, acc: str | None = None):
        assert self.journal.intents
        metadata = json.loads((Path(folder) / "kernel-metadata.json").read_bytes())
        assert metadata["is_private"] is True
        ref = metadata["id"]
        source = (Path(folder) / metadata["code_file"]).read_bytes()
        canonical = _canonical_notebook_source(source, kernel_type=metadata["kernel_type"])
        version = max(self.kernels.get(ref, {0: b""})) + 1
        self.kernels.setdefault(ref, {})[version] = canonical
        self.statuses[ref] = "complete"
        self.calls.append(("kernels_push", ref, version, metadata, timeout, acc))
        return SimpleNamespace(ref=ref, kernelId=1000, versionNumber=version, error="")

    def get_kernel_latest_response(self, ref: str):
        version = max(self.kernels[ref])
        return SimpleNamespace(
            metadata=SimpleNamespace(
                ref=ref,
                id=1000,
                current_version_number=version,
                is_private=True,
            ),
            blob=SimpleNamespace(source=self.kernels[ref][version].decode("utf-8")),
        )

    def kernels_pull(self, kernel: str, path: str, metadata: bool = False, quiet: bool = True) -> str:
        parts = kernel.split("/")
        owner, slug = parts[:2]
        version = int(parts[2]) if len(parts) == 3 else max(self.kernels[f"{owner}/{slug}"])
        target = Path(path) / "source.py"
        target.write_bytes(self.kernels[f"{owner}/{slug}"][version])
        if metadata:
            (Path(path) / "kernel-metadata.json").write_text(
                json.dumps(
                    {
                        "id": f"{owner}/{slug}",
                        "id_no": 1000,
                        "title": slug,
                        "code_file": "source.py",
                        "language": "python",
                        "kernel_type": "script",
                        "is_private": True,
                    }
                )
            )
            return str(path)
        return str(target)

    def kernels_status(self, kernel: str):
        if kernel not in self.kernels:
            raise HttpFailure(404)
        self.status_requests.append(kernel)
        return SimpleNamespace(status=self.statuses[kernel], failure_message=None)

    def kernels_output(
        self,
        kernel: str,
        path: str,
        file_pattern: str | None = None,
        force: bool = False,
        quiet: bool = True,
        page_token: str | None = None,
        page_size: int = 20,
    ) -> tuple[list[str], str]:
        exact_run_ref = kernel
        kernel = "/".join(kernel.split("/")[:2])
        self.calls.append(("kernels_output", exact_run_ref))
        self.output_file_patterns.append(file_pattern)
        outputs = self.outputs[kernel]
        if file_pattern is not None and not self.ignore_output_file_pattern:
            outputs = {name: body for name, body in outputs.items() if re.fullmatch(file_pattern, name)}
        write_tree(Path(path), outputs)
        # kaggle==2.2.4 applies file_pattern only to response.files. A supplied
        # response.log is written independently of that anchored filter.
        provider_log = self.output_logs.get(kernel)
        if provider_log:
            (Path(path) / f"{kernel.split('/', 1)[1]}.log").write_bytes(provider_log)
        if self.status_after_output is not None:
            self.statuses[kernel] = self.status_after_output
        return [str(Path(path) / name) for name in outputs], ""

    def kernels_delete(self, kernel: str, no_confirm: bool = False) -> None:
        assert self.journal.intents
        self.calls.append(("kernels_delete", kernel, no_confirm))
        del self.kernels[kernel]
        self.statuses.pop(kernel, None)


class HttpFailure(RuntimeError):
    def __init__(self, status: int) -> None:
        self.response = SimpleNamespace(status_code=status, headers={})
        super().__init__(f"http {status}")


def read_tree(root: Path, *, excluded: set[str] | None = None) -> dict[str, bytes]:
    excluded = excluded or set()
    return {
        path.relative_to(root).as_posix(): path.read_bytes()
        for path in root.rglob("*")
        if path.is_file() and path.relative_to(root).as_posix() not in excluded
    }


def write_tree(root: Path, files: dict[str, bytes]) -> None:
    for relative, content in files.items():
        target = root.joinpath(*relative.split("/"))
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)


def effect(
    action: MutationAction,
    ref: str,
    *,
    task_id: UUID,
    arguments: dict[str, object],
    expected=None,
) -> ProviderEffectIntent:
    return ProviderEffectIntent.create(
        operation_id=uuid4(),
        effect_id=uuid4(),
        idempotency_key="provider-test-key",
        task_id=task_id,
        action=action,
        provider_ref=ref,
        expected_fingerprint=expected,
        arguments=arguments,
        requested_at=NOW,
    )


def adapter() -> tuple[KaggleProviderAdapter, FakeKaggleApi, FakeJournal]:
    journal = FakeJournal()
    api = FakeKaggleApi(journal)
    return (
        KaggleProviderAdapter(
            api,
            identity=KaggleProviderIdentity(username="owner"),
            journal=journal,
            sleep=lambda _: None,
            monotonic=lambda: 0.0,
            clock=lambda: NOW,
        ),
        api,
        journal,
    )


def test_official_224_calls_are_private_and_exact(tmp_path: Path) -> None:
    client, api, journal = adapter()
    task_id = uuid4()
    files = {"payload/value.txt": b"exact bytes"}
    from my_data_hub.providers.kaggle import mapping_sha256

    create = effect(
        MutationAction.CREATE_DATASET,
        "owner/private-canary",
        task_id=task_id,
        arguments={
            "content_tree_sha256": mapping_sha256(files),
            "control_class": "mcp_managed",
            "disposable": True,
        },
    )
    created = client.create_private_dataset(
        intent=create,
        files=files,
        title="Private canary",
        control_class=ControlClass.MCP_MANAGED,
        disposable=True,
    )
    assert created.identity.version == 1
    assert created.identity.package_sha256
    assert created.effect.outcome == EffectOutcome.APPLIED
    assert journal.claims[created.claim.claim_sha256] == created.claim
    assert ("dataset_create_new", "owner/private-canary", False, "zip") in api.calls
    destination = tmp_path / "exact-v1"
    downloaded = client.download_private_dataset_exact(
        provider_ref="owner/private-canary",
        version=1,
        destination=destination,
    )
    assert downloaded.version == 1
    assert (destination / "payload/value.txt").read_bytes() == b"exact bytes"
    assert any(call[:2] == ("dataset_download_files", "owner/private-canary/1") for call in api.calls)

    replacement = {"payload/value.txt": b"version two"}
    version_intent = effect(
        MutationAction.VERSION_DATASET,
        created.claim.provider_ref,
        task_id=task_id,
        expected=created.claim.fingerprint,
        arguments={
            "content_tree_sha256": mapping_sha256(replacement),
            "previous_version": 1,
            "version_notes_sha256": hashlib.sha256(b"exact version").hexdigest(),
        },
    )
    versioned = client.create_private_dataset_version(
        intent=version_intent,
        claim=created.claim,
        files=replacement,
        version_notes="exact version",
    )
    assert versioned.identity.version == 2
    assert versioned.identity.package_sha256 != created.identity.package_sha256
    assert api.datasets[created.claim.provider_ref][1] != api.datasets[created.claim.provider_ref][2]


def test_checkpoint_directory_upload_streams_without_bytes_mapping(tmp_path: Path) -> None:
    client, api, _journal = adapter()
    task_id = uuid4()
    source = tmp_path / "checkpoint"
    (source / "physical").mkdir(parents=True)
    (source / "physical/base.tar.gz").write_bytes(b"checkpoint bytes")
    (source / "checkpoint-manifest.json").write_bytes(b'{"manifest":"metadata"}')
    from my_data_hub.providers.kaggle import directory_sha256

    intent = effect(
        MutationAction.CREATE_DATASET,
        "owner/private-checkpoints",
        task_id=task_id,
        arguments={
            "content_tree_sha256": directory_sha256(source),
            "control_class": "orchestrator_protected",
            "disposable": False,
        },
    )
    created = client.create_private_dataset_from_directory(
        intent=intent,
        source_directory=source,
        title="Private checkpoints",
        control_class=ControlClass.ORCHESTRATOR_PROTECTED,
        disposable=False,
    )
    assert created.identity.version == 1
    assert api.datasets[created.identity.provider_ref][1]["physical/base.tar.gz"] == b"checkpoint bytes"
    assert (source / "physical/base.tar.gz").is_file()


def test_dataset_directory_reconciliation_repairs_lost_journal_without_second_version(
    tmp_path: Path,
) -> None:
    client, api, journal = adapter()
    task_id = uuid4()
    first_source = tmp_path / "checkpoint-v1"
    first_source.mkdir()
    (first_source / "checkpoint-manifest.json").write_bytes(b'{"version":1}')
    from my_data_hub.providers.kaggle import directory_sha256

    created = client.create_private_dataset_from_directory(
        intent=effect(
            MutationAction.CREATE_DATASET,
            "owner/reconciled-checkpoints",
            task_id=task_id,
            arguments={
                "content_tree_sha256": directory_sha256(first_source),
                "control_class": "orchestrator_protected",
                "disposable": False,
            },
        ),
        source_directory=first_source,
        title="Reconciled checkpoints",
        control_class=ControlClass.ORCHESTRATOR_PROTECTED,
        disposable=False,
    )
    second_source = tmp_path / "checkpoint-v2"
    second_source.mkdir()
    (second_source / "checkpoint-manifest.json").write_bytes(b'{"version":2}')
    notes = "exact checkpoint version two"
    arguments = {
        "content_tree_sha256": directory_sha256(second_source),
        "previous_version": 1,
        "version_notes_sha256": hashlib.sha256(notes.encode()).hexdigest(),
    }
    version_intent = effect(
        MutationAction.VERSION_DATASET,
        created.identity.provider_ref,
        task_id=task_id,
        expected=created.claim.fingerprint,
        arguments=arguments,
    )
    ticks = 0

    def advancing_clock() -> datetime:
        nonlocal ticks
        ticks += 1
        return NOW + timedelta(seconds=ticks)

    client.clock = advancing_clock
    journal.commit_then_fail_claims = 1
    with pytest.raises(RuntimeError, match="after commit"):
        client.create_private_dataset_version_from_directory(
            intent=version_intent,
            claim=created.claim,
            source_directory=second_source,
            version_notes=notes,
        )

    reconciled = client.reconcile_private_dataset_directory_mutation(
        intent=version_intent,
        source_directory=second_source,
        expected_version=2,
        arguments=arguments,
        control_class=ControlClass.ORCHESTRATOR_PROTECTED,
        disposable=False,
    )

    assert reconciled.identity.version == 2
    assert reconciled.effect.outcome == EffectOutcome.ALREADY_APPLIED
    assert sum(call[0] == "dataset_create_version" for call in api.calls) == 1
    assert journal.claims[reconciled.claim.claim_sha256] == reconciled.claim
    attempts = [item for item in journal.claim_attempts if item.effect_id == version_intent.effect_id]
    assert len(attempts) == 2
    assert attempts[0] == attempts[1] == reconciled.claim
    assert reconciled.claim.registered_at == version_intent.requested_at


def test_notebook_source_run_and_output_are_bound_to_exact_version() -> None:
    client, api, _journal = adapter()
    task_id = uuid4()
    run_id = uuid4()
    source = f'RUN_ID = "{run_id}"\nprint(RUN_ID)\n'.encode()
    source_sha = hashlib.sha256(source).hexdigest()
    push = effect(
        MutationAction.PUSH_NOTEBOOK,
        "owner/private-kernel",
        task_id=task_id,
        arguments={
            "task_run_id": str(run_id),
            "source_sha256": source_sha,
            "dataset_sources": (),
            "control_class": "mcp_managed",
            "disposable": True,
        },
    )
    result = client.push_private_notebook(
        intent=push,
        task_run_id=run_id,
        source=source,
        title="private-kernel",
        code_file="run.py",
        kernel_type="script",
        language="python",
        control_class=ControlClass.MCP_MANAGED,
        disposable=True,
    )
    assert result.run.provider_run_ref == "owner/private-kernel/1"
    assert result.run.provider_kernel_id == 1000
    assert result.source.source_sha256 == source_sha
    assert client.read_run_status(result.run).state == KernelState.COMPLETE

    runtime_receipt = {
        "task_run_id": str(run_id),
        "provider_ref": result.run.provider_ref,
        "source_version": result.run.source_version,
        "source_sha256": result.run.source_sha256,
        "terminal_state": "complete",
    }
    api.outputs[result.run.provider_ref] = {
        RUN_RECEIPT_NAME: json.dumps(runtime_receipt).encode(),
        "result.txt": b"exact output",
    }
    output = client.read_exact_run_output(result.run)
    assert output.file_count == 2
    assert output.output_tree_sha256
    assert ("kernels_output", "owner/private-kernel/1") in api.calls


def test_master_legacy_push_persists_numeric_response_pending_runtime_attestation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    client, api, journal = adapter()
    run_id = uuid4()
    source = f'RUN_ID = "{run_id}"\nprint(RUN_ID)\n'.encode()
    intent = effect(
        MutationAction.PUSH_NOTEBOOK,
        "owner/postgres-master",
        task_id=run_id,
        arguments={
            "task_run_id": str(run_id),
            "source_sha256": hashlib.sha256(source).hexdigest(),
            "dataset_sources": (),
            "control_class": "orchestrator_protected",
            "disposable": False,
        },
    )
    monkeypatch.setattr(
        api,
        "kernels_pull",
        lambda *args, **kwargs: (_ for _ in ()).throw(PermissionError("GetKernel 403")),
    )
    result = client.push_private_master_notebook_pending_attestation(
        intent=intent,
        task_run_id=run_id,
        source=source,
        title="postgres-master",
        code_file="run.py",
        kernel_type="script",
        language="python",
        control_class=ControlClass.ORCHESTRATOR_PROTECTED,
        disposable=False,
    )
    assert result.run.provider_run_ref == "owner/postgres-master/1"
    assert result.run.provider_kernel_id == 1000
    assert result.run.source_sha256 == hashlib.sha256(source).hexdigest()
    assert journal.intents == [intent]
    assert len(journal.receipts) == 1 and len(journal.claims) == 1
    assert client.read_attested_master_run_status(result.run).state is KernelState.COMPLETE
    api.outputs[result.run.provider_ref] = {"master-terminal.json": b"{}"}
    output = client.download_attested_master_output_file(
        result.run,
        destination=tmp_path / "master-output",
        file_name="master-terminal.json",
        max_bytes=1024,
    )
    assert output.file_count == 1
    assert not any(call[0] == "kernels_pull" for call in api.calls)


def test_master_pending_attestation_reconciles_lost_push_response_without_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, api, journal = adapter()
    run_id = uuid4()
    source = f'RUN_ID = "{run_id}"\n'.encode()
    intent = effect(
        MutationAction.PUSH_NOTEBOOK,
        "owner/ambiguous-master",
        task_id=run_id,
        arguments={
            "task_run_id": str(run_id),
            "source_sha256": hashlib.sha256(source).hexdigest(),
            "dataset_sources": (),
            "control_class": "orchestrator_protected",
            "disposable": False,
        },
    )
    original = api.kernels_push
    calls = 0

    def lost_response(*args, **kwargs):  # type: ignore[no-untyped-def]
        nonlocal calls
        calls += 1
        original(*args, **kwargs)
        raise ConnectionError("response lost after provider commit")

    monkeypatch.setattr(api, "kernels_push", lost_response)
    result = client.push_private_master_notebook_pending_attestation(
        intent=intent,
        task_run_id=run_id,
        source=source,
        title="ambiguous-master",
        code_file="run.py",
        kernel_type="script",
        language="python",
        control_class=ControlClass.ORCHESTRATOR_PROTECTED,
        disposable=False,
    )
    assert calls == 1
    assert result.effect.outcome is EffectOutcome.ALREADY_APPLIED
    assert result.run.provider_run_ref == "owner/ambiguous-master/1"
    assert result.run.provider_kernel_id == 1000
    assert journal.intents == [intent]
    assert len(journal.receipts) == 1
    assert len(journal.claims) == 1


def test_master_legacy_empty_push_response_uses_exact_latest_get_kernel() -> None:
    client, api, journal = adapter()
    run_id = uuid4()
    source = f'RUN_ID = "{run_id}"\n'.encode()
    intent = effect(
        MutationAction.PUSH_NOTEBOOK,
        "owner/legacy-master",
        task_id=run_id,
        arguments={
            "task_run_id": str(run_id),
            "source_sha256": hashlib.sha256(source).hexdigest(),
            "dataset_sources": (),
            "control_class": "orchestrator_protected",
            "disposable": False,
        },
    )
    original = api.kernels_push

    def empty_response(*args, **kwargs):  # type: ignore[no-untyped-def]
        original(*args, **kwargs)
        return SimpleNamespace(ref="", kernelId=0, versionNumber=0, error="")

    api.kernels_push = empty_response  # type: ignore[method-assign]
    result = client.push_private_master_notebook_pending_attestation(
        intent=intent,
        task_run_id=run_id,
        source=source,
        title="legacy-master",
        code_file="run.py",
        kernel_type="script",
        language="python",
        control_class=ControlClass.ORCHESTRATOR_PROTECTED,
        disposable=False,
    )
    assert result.effect.outcome is EffectOutcome.APPLIED
    assert result.run.provider_run_ref == "owner/legacy-master/1"
    assert result.run.provider_kernel_id == 1000
    assert len(journal.receipts) == 1 and len(journal.claims) == 1


def test_fm08_termination_reconciles_lost_delete_response_without_second_delete(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, api, journal = adapter()
    task_run_id = uuid4()
    provider_ref = "owner/fm08-old-master"
    api.kernels[provider_ref] = {1: b"source"}
    source_sha256 = hashlib.sha256(b"source").hexdigest()
    run = KaggleKernelRunIdentity(
        provider_ref=provider_ref,
        provider_run_ref=f"{provider_ref}/1",
        source_version=1,
        provider_kernel_id=1000,
        source_sha256=source_sha256,
        task_run_id=task_run_id,
        started_at=NOW,
    )
    arguments = {
        "task_run_id": str(task_run_id),
        "source_version": 1,
        "source_sha256": source_sha256,
        "provider_kernel_id": 1000,
        "provider_run_ref": f"{provider_ref}/1",
        "termination_kind": "fm08_abrupt_master",
    }
    intent = effect(
        MutationAction.DELETE_NOTEBOOK,
        provider_ref,
        task_id=task_run_id,
        arguments=arguments,
    )
    original = api.kernels_delete
    calls = 0

    def lost_response(*args, **kwargs):  # type: ignore[no-untyped-def]
        nonlocal calls
        calls += 1
        original(*args, **kwargs)
        raise ConnectionError("response lost after exact delete")

    monkeypatch.setattr(api, "kernels_delete", lost_response)
    first = client.terminate_attested_master_run(intent=intent, run=run)
    second = client.terminate_attested_master_run(intent=intent, run=run)
    assert first.outcome is EffectOutcome.APPLIED
    assert second.outcome is EffectOutcome.ALREADY_APPLIED
    assert calls == 1
    assert journal.intents == [intent, intent]
    assert len(journal.receipts) == 2


def test_delete_absence_reconciliation_waits_without_a_second_mutation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, _api, _journal = adapter()
    observations = iter((False, False, True))
    sleeps: list[float] = []
    monkeypatch.setattr(client, "_is_absent", lambda *_args: next(observations))
    client.sleep = sleeps.append
    assert client._wait_for_absence("owner/eventual-delete", ProviderKind.NOTEBOOK) is True
    assert sleeps == [5.0, 5.0]


def test_output_rejects_stale_run_receipt() -> None:
    client, api, _journal = adapter()
    task_id = uuid4()
    run_id = uuid4()
    source = f'RUN_ID = "{run_id}"\n'.encode()
    pushed = client.push_private_notebook(
        intent=effect(
            MutationAction.PUSH_NOTEBOOK,
            "owner/stale-kernel",
            task_id=task_id,
            arguments={
                "task_run_id": str(run_id),
                "source_sha256": hashlib.sha256(source).hexdigest(),
                "dataset_sources": (),
                "control_class": "mcp_managed",
                "disposable": True,
            },
        ),
        task_run_id=run_id,
        source=source,
        title="stale-kernel",
        code_file="run.py",
        kernel_type="script",
        language="python",
        control_class=ControlClass.MCP_MANAGED,
        disposable=True,
    )
    api.outputs[pushed.run.provider_ref] = {
        RUN_RECEIPT_NAME: json.dumps(
            {
                "task_run_id": str(uuid4()),
                "provider_ref": pushed.run.provider_ref,
                "source_version": pushed.run.source_version,
                "source_sha256": pushed.run.source_sha256,
                "terminal_state": "complete",
            }
        ).encode()
    }
    with pytest.raises(KaggleIdentityError, match="stale"):
        client.read_exact_run_output(pushed.run)


def test_exact_single_output_file_uses_anchored_pattern_and_never_downloads_broad_tree(tmp_path: Path) -> None:
    client, api, _journal = adapter()
    task_id = uuid4()
    run_id = uuid4()
    source = f'RUN_ID = "{run_id}"\n'.encode()
    pushed = client.push_private_notebook(
        intent=effect(
            MutationAction.PUSH_NOTEBOOK,
            "owner/single-output-kernel",
            task_id=task_id,
            arguments={
                "task_run_id": str(run_id),
                "source_sha256": hashlib.sha256(source).hexdigest(),
                "dataset_sources": (),
                "control_class": "mcp_managed",
                "disposable": True,
            },
        ),
        task_run_id=run_id,
        source=source,
        title="single-output-kernel",
        code_file="run.py",
        kernel_type="script",
        language="python",
        control_class=ControlClass.MCP_MANAGED,
        disposable=True,
    )
    terminal_name = "my-data-hub-master-terminal.json"
    api.outputs[pushed.run.provider_ref] = {
        terminal_name: b'{"status":"succeeded"}',
        "private-business-bytes.bin": b"must never reach the devstand",
    }
    api.output_logs[pushed.run.provider_ref] = b"normal nonempty provider log"

    receipt = client.download_exact_run_output_file(
        pushed.run,
        destination=tmp_path / "single-output",
        file_name=terminal_name,
        max_bytes=256 * 1024,
    )

    assert receipt.file_count == 1
    assert api.output_file_patterns[-1] == r"^my\-data\-hub\-master\-terminal\.json$"
    assert [path.name for path in (tmp_path / "single-output").iterdir()] == [terminal_name]
    assert b"business" not in (tmp_path / "single-output" / terminal_name).read_bytes()


def test_exact_single_output_file_fails_closed_when_missing_or_api_ignores_pattern(tmp_path: Path) -> None:
    client, api, _journal = adapter()
    run_id = uuid4()
    source = f'RUN_ID = "{run_id}"\n'.encode()
    pushed = client.push_private_notebook(
        intent=effect(
            MutationAction.PUSH_NOTEBOOK,
            "owner/single-output-denial",
            task_id=run_id,
            arguments={
                "task_run_id": str(run_id),
                "source_sha256": hashlib.sha256(source).hexdigest(),
                "dataset_sources": (),
                "control_class": "mcp_managed",
                "disposable": True,
            },
        ),
        task_run_id=run_id,
        source=source,
        title="single-output-denial",
        code_file="run.py",
        kernel_type="script",
        language="python",
        control_class=ControlClass.MCP_MANAGED,
        disposable=True,
    )
    api.outputs[pushed.run.provider_ref] = {"private-business-bytes.bin": b"never copy broadly"}
    destination = tmp_path / "missing-output"
    with pytest.raises(KaggleIdentityError, match="missing or extra"):
        client.download_exact_run_output_file(
            pushed.run,
            destination=destination,
            file_name="my-data-hub-master-terminal.json",
            max_bytes=256 * 1024,
        )
    assert not destination.exists()

    api.outputs[pushed.run.provider_ref]["my-data-hub-master-terminal.json"] = b"{}"
    api.ignore_output_file_pattern = False
    api.output_logs[pushed.run.provider_ref] = b"x" * (1024 * 1024 + 1)
    with pytest.raises(KaggleContractError, match="provider output log exceeds"):
        client.download_exact_run_output_file(
            pushed.run,
            destination=destination,
            file_name="my-data-hub-master-terminal.json",
            max_bytes=256 * 1024,
        )
    assert not destination.exists()

    api.output_logs[pushed.run.provider_ref] = b""
    api.ignore_output_file_pattern = True
    with pytest.raises(KaggleIdentityError, match="missing or extra"):
        client.download_exact_run_output_file(
            pushed.run,
            destination=destination,
            file_name="my-data-hub-master-terminal.json",
            max_bytes=256 * 1024,
        )
    assert not destination.exists()


@pytest.mark.parametrize("provider_status", ["failed", "error"])
def test_exact_failed_output_is_status_fenced_typed_and_selective(tmp_path: Path, provider_status: str) -> None:
    client, api, _journal = adapter()
    run_id = uuid4()
    source = f'RUN_ID = "{run_id}"\n'.encode()
    pushed = client.push_private_notebook(
        intent=effect(
            MutationAction.PUSH_NOTEBOOK,
            "owner/fm15-failed-output",
            task_id=run_id,
            arguments={
                "task_run_id": str(run_id),
                "source_sha256": hashlib.sha256(source).hexdigest(),
                "dataset_sources": (),
                "control_class": "mcp_managed",
                "disposable": True,
            },
        ),
        task_run_id=run_id,
        source=source,
        title="fm15-failed-output",
        code_file="run.py",
        kernel_type="script",
        language="python",
        control_class=ControlClass.MCP_MANAGED,
        disposable=True,
    )
    receipt_name = "checkpoint-acceptance-fm15-failure.json"
    receipt_bytes = b'{"detail_code":"FORCED_DISPOSABLE_RESTORE_FAILURE"}'
    api.statuses[pushed.run.provider_ref] = provider_status
    api.outputs[pushed.run.provider_ref] = {
        receipt_name: receipt_bytes,
        "private-business-export.json": b"must not be downloaded",
    }
    api.output_logs[pushed.run.provider_ref] = b"bounded provider failure log"

    result = client.download_exact_failed_run_output_file(
        pushed.run,
        destination=tmp_path / "failed-output",
        file_name=receipt_name,
        max_bytes=64 * 1024,
    )

    assert result.terminal_state is KernelState.FAILED
    assert result.provider_status == provider_status
    assert result.receipt_sha256 == hashlib.sha256(receipt_bytes).hexdigest()
    assert api.output_file_patterns[-1] == r"^checkpoint\-acceptance\-fm15\-failure\.json$"
    assert api.status_requests[-2:] == [pushed.run.provider_ref, pushed.run.provider_ref]
    assert [path.name for path in (tmp_path / "failed-output").iterdir()] == [receipt_name]

    api.output_logs[pushed.run.provider_ref] = b"x" * (1024 * 1024 + 1)
    oversized_log = tmp_path / "failed-output-log-overrun"
    with pytest.raises(KaggleContractError, match="failed provider output log exceeds"):
        client.download_exact_failed_run_output_file(
            pushed.run,
            destination=oversized_log,
            file_name=receipt_name,
            max_bytes=64 * 1024,
        )
    assert not oversized_log.exists()

    api.output_logs[pushed.run.provider_ref] = b""
    api.ignore_output_file_pattern = True
    ignored_pattern = tmp_path / "failed-output-pattern-ignored"
    with pytest.raises(KaggleIdentityError, match="missing or extra"):
        client.download_exact_failed_run_output_file(
            pushed.run,
            destination=ignored_pattern,
            file_name=receipt_name,
            max_bytes=64 * 1024,
        )
    assert not ignored_pattern.exists()

    api.ignore_output_file_pattern = False
    api.outputs[pushed.run.provider_ref] = {receipt_name: b"x" * (64 * 1024 + 1)}
    oversized_receipt = tmp_path / "failed-output-receipt-overrun"
    with pytest.raises(KaggleContractError, match="receipt exceeds"):
        client.download_exact_failed_run_output_file(
            pushed.run,
            destination=oversized_receipt,
            file_name=receipt_name,
            max_bytes=64 * 1024,
        )
    assert not oversized_receipt.exists()


@pytest.mark.parametrize("provider_status", ["complete", "failure", "cancelled", "canceled"])
def test_exact_failed_output_rejects_non_failed_error_status(tmp_path: Path, provider_status: str) -> None:
    client, api, _journal = adapter()
    run_id = uuid4()
    source = f'RUN_ID = "{run_id}"\n'.encode()
    pushed = client.push_private_notebook(
        intent=effect(
            MutationAction.PUSH_NOTEBOOK,
            "owner/fm15-status-denial",
            task_id=run_id,
            arguments={
                "task_run_id": str(run_id),
                "source_sha256": hashlib.sha256(source).hexdigest(),
                "dataset_sources": (),
                "control_class": "mcp_managed",
                "disposable": True,
            },
        ),
        task_run_id=run_id,
        source=source,
        title="fm15-status-denial",
        code_file="run.py",
        kernel_type="script",
        language="python",
        control_class=ControlClass.MCP_MANAGED,
        disposable=True,
    )
    api.statuses[pushed.run.provider_ref] = provider_status
    with pytest.raises(KaggleContractError, match="FAILED or ERROR"):
        client.download_exact_failed_run_output_file(
            pushed.run,
            destination=tmp_path / provider_status,
            file_name="checkpoint-acceptance-fm15-failure.json",
            max_bytes=64 * 1024,
        )


def test_exact_failed_output_rejects_status_change_and_cleans_destination(tmp_path: Path) -> None:
    client, api, _journal = adapter()
    run_id = uuid4()
    source = f'RUN_ID = "{run_id}"\n'.encode()
    pushed = client.push_private_notebook(
        intent=effect(
            MutationAction.PUSH_NOTEBOOK,
            "owner/fm15-status-race",
            task_id=run_id,
            arguments={
                "task_run_id": str(run_id),
                "source_sha256": hashlib.sha256(source).hexdigest(),
                "dataset_sources": (),
                "control_class": "mcp_managed",
                "disposable": True,
            },
        ),
        task_run_id=run_id,
        source=source,
        title="fm15-status-race",
        code_file="run.py",
        kernel_type="script",
        language="python",
        control_class=ControlClass.MCP_MANAGED,
        disposable=True,
    )
    api.statuses[pushed.run.provider_ref] = "failed"
    api.status_after_output = "complete"
    api.outputs[pushed.run.provider_ref] = {"checkpoint-acceptance-fm15-failure.json": b"{}"}
    destination = tmp_path / "status-race"
    with pytest.raises(KaggleIdentityError, match="status changed"):
        client.download_exact_failed_run_output_file(
            pushed.run,
            destination=destination,
            file_name="checkpoint-acceptance-fm15-failure.json",
            max_bytes=64 * 1024,
        )
    assert not destination.exists()


def test_unknown_names_are_external_read_only_and_never_cleanup_authority() -> None:
    client, api, journal = adapter()
    api.datasets["owner/mcp-managed-looking"] = {1: {"value.txt": b"foreign"}}
    inventory = BoundedInventory(client, ProviderRegistry()).collect(ProviderKind.DATASET)
    assert inventory[0].control_class == ControlClass.EXTERNAL_READ_ONLY

    fabricated = TaskResourceClaim.create(
        task_id=uuid4(),
        effect_id=uuid4(),
        provider_ref="owner/mcp-managed-looking",
        kind=ProviderKind.DATASET,
        control_class=ControlClass.MCP_MANAGED,
        disposable=True,
        fingerprint=inventory[0].fingerprint,
        provider_version=1,
        registered_at=NOW,
    )
    delete = effect(
        MutationAction.DELETE_DATASET,
        fabricated.provider_ref,
        task_id=fabricated.task_id,
        expected=fabricated.fingerprint,
        arguments={"claim_sha256": fabricated.claim_sha256, "provider_version": 1},
    )
    with pytest.raises(KagglePolicyError, match="ledger"):
        client.delete_task_created_resource(intent=delete, claim=fabricated)
    assert "owner/mcp-managed-looking" in api.datasets
    assert journal.claims == {}


def test_inventory_ignores_only_exact_non_addressable_private_notebook_tombstone() -> None:
    client, api, _journal = adapter()
    api.kernels["owner/visible-private-notebook"] = {1: b"print('visible')\n"}
    original = api.kernels_list_with_response

    def with_redacted_tombstone(**kwargs):  # type: ignore[no-untyped-def]
        response = original(**kwargs)
        response.kernels.append(
            SimpleNamespace(
                ref="",
                slug="",
                author="",
                title="[Private Notebook]",
                id=0,
                current_version_number=0,
                is_private=False,
            )
        )
        return response

    api.kernels_list_with_response = with_redacted_tombstone
    inventory = BoundedInventory(client, ProviderRegistry()).collect(ProviderKind.NOTEBOOK)
    assert [item.provider_ref for item in inventory] == ["owner/visible-private-notebook"]

    def with_malformed_identifiable_row(**kwargs):  # type: ignore[no-untyped-def]
        response = original(**kwargs)
        response.kernels.append(
            SimpleNamespace(
                ref="",
                slug="",
                author="",
                title="not-the-provider-tombstone",
                id=0,
                current_version_number=0,
            )
        )
        return response

    api.kernels_list_with_response = with_malformed_identifiable_row
    with pytest.raises(KaggleIdentityError, match="exact owner/slug"):
        BoundedInventory(client, ProviderRegistry()).collect(ProviderKind.NOTEBOOK)


def test_exact_task_claim_allows_only_disposable_resource_cleanup() -> None:
    client, api, _journal = adapter()
    task_id = uuid4()
    files = {"value.txt": b"delete me"}
    from my_data_hub.providers.kaggle import mapping_sha256

    created = client.create_private_dataset(
        intent=effect(
            MutationAction.CREATE_DATASET,
            "owner/disposable-canary",
            task_id=task_id,
            arguments={
                "content_tree_sha256": mapping_sha256(files),
                "control_class": "mcp_managed",
                "disposable": True,
            },
        ),
        files=files,
        title="Disposable canary",
        control_class=ControlClass.MCP_MANAGED,
        disposable=True,
    )
    delete = effect(
        MutationAction.DELETE_DATASET,
        created.claim.provider_ref,
        task_id=task_id,
        expected=created.claim.fingerprint,
        arguments={"claim_sha256": created.claim.claim_sha256, "provider_version": 1},
    )
    receipt = client.delete_task_created_resource(intent=delete, claim=created.claim)
    assert receipt.outcome == EffectOutcome.APPLIED
    assert created.claim.provider_ref not in api.datasets
    assert api.calls[-1] == ("dataset_delete", created.claim.provider_ref, True)
    reconciled = client.delete_task_created_resource(intent=delete, claim=created.claim)
    assert reconciled.outcome == EffectOutcome.ALREADY_APPLIED
    assert reconciled.effect_id == receipt.effect_id
    assert sum(call[0] == "dataset_delete" for call in api.calls) == 1


class DeniedProbe:
    def __init__(self, status: int) -> None:
        self.status = status

    def read_dataset(self, provider_ref: str, version: int) -> None:
        raise HttpFailure(self.status)


def test_private_dataset_proof_includes_unauthenticated_denial() -> None:
    client, api, _journal = adapter()
    api.datasets["owner/private-proof"] = {1: {"value.txt": b"private"}}
    proof = client.prove_private_dataset_access(
        provider_ref="owner/private-proof", version=1, unauthenticated_probe=DeniedProbe(403)
    )
    assert proof.unauthenticated_http_status == 403
    assert proof.denial_class == "authorization"
    with pytest.raises(KagglePolicyError, match="not proven"):
        client.prove_private_dataset_access(
            provider_ref="owner/private-proof", version=1, unauthenticated_probe=DeniedProbe(500)
        )
