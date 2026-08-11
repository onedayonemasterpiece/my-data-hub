from __future__ import annotations

import hashlib
import importlib.metadata
import json
import re
import shutil
import tempfile
import time
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any
from uuid import UUID

from my_data_hub.hashing import canonical_json_bytes, sha256_file, sha256_value
from my_data_hub.providers.inventory import InventoryPage
from my_data_hub.providers.models import (
    ControlClass,
    ObservedProviderResource,
    ProviderFingerprint,
    ProviderKind,
)

from .contracts import (
    CONTROL_MANIFEST_NAME,
    KAGGLE_API_PACKAGE,
    KAGGLE_API_VERSION,
    RUN_RECEIPT_NAME,
    DatasetMutationResult,
    EffectOutcome,
    KaggleAmbiguousMutation,
    KaggleApiProtocol,
    KaggleContractError,
    KaggleDatasetIdentity,
    KaggleDependencyError,
    KaggleIdentityError,
    KaggleKernelFailureOutputIdentity,
    KaggleKernelOutputIdentity,
    KaggleKernelOutputTreeIdentity,
    KaggleKernelRunIdentity,
    KaggleKernelSourceIdentity,
    KaggleKernelStatus,
    KaggleNotFound,
    KagglePolicyError,
    KagglePollingTimeout,
    KaggleProviderIdentity,
    KaggleTerminalFailure,
    KernelState,
    MutationAction,
    NotebookMutationResult,
    PollPolicy,
    PrivateAccessProof,
    ProviderEffectIntent,
    ProviderEffectJournal,
    ProviderEffectReceipt,
    RetryClass,
    TaskResourceClaim,
    UnauthenticatedDatasetProbe,
)
from .retry import BoundedRetry, RetryPolicy, classify_failure

MAX_EXACT_OUTPUT_PROVIDER_LOG_BYTES = 1024 * 1024

_CONTROLLED_CLASSES = {
    ControlClass.ORCHESTRATOR_PROTECTED,
    ControlClass.MCP_MANAGED,
    ControlClass.MCP_EXCHANGE,
}
_QUEUED = {"queued", "pending", "initializing"}
_RUNNING = {"running"}
_COMPLETE = {"complete", "completed", "success", "succeeded"}
_FAILED = {"error", "failed", "failure", "cancelled", "canceled"}


def _field(value: object, *names: str) -> Any:
    if isinstance(value, Mapping):
        for name in names:
            if name in value:
                return value[name]
    to_dict = getattr(value, "to_dict", None)
    if callable(to_dict):
        converted = to_dict()
        if isinstance(converted, Mapping):
            for name in names:
                if name in converted:
                    return converted[name]
    for name in names:
        if hasattr(value, name):
            return getattr(value, name)
    return None


def _normalized_ref(value: object) -> str:
    ref = str(value or "").strip()
    if ref.startswith("/code/"):
        ref = ref.removeprefix("/code/")
    ref = ref.removeprefix("/")
    parts = ref.split("/")
    if len(parts) != 2 or not all(parts):
        raise KaggleIdentityError("Kaggle resource ref must be exact owner/slug")
    return ref


def _normalized_dataset_source(value: object) -> str:
    source = str(value or "").strip().removeprefix("/")
    parts = source.split("/")
    if len(parts) not in {2, 3} or not parts[0] or not parts[1]:
        raise KaggleIdentityError("Kaggle dataset source must be exact owner/slug[/version]")
    _normalized_ref("/".join(parts[:2]))
    if len(parts) == 3 and (not parts[2].isdigit() or int(parts[2]) < 1):
        raise KaggleIdentityError("Kaggle dataset source version must be an exact positive integer")
    return source


def _version(value: object) -> int | None:
    if value in (None, ""):
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 1 else None


def _privacy(value: object) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().casefold()
        if lowered in {"true", "private"}:
            return True
        if lowered in {"false", "public"}:
            return False
    return None


def _validate_relative_path(path: str) -> None:
    if not path or len(path) > 1000 or "\\" in path or path.startswith("/") or "\x00" in path:
        raise KaggleContractError("artifact paths must be bounded relative POSIX paths")
    parts = path.split("/")
    if any(part in {"", ".", ".."} for part in parts) or PurePosixPath(path).as_posix() != path:
        raise KaggleContractError("artifact paths must be normalized and traversal-free")


def _tree_entries(root: Path, *, excluded: frozenset[str] = frozenset()) -> list[dict[str, object]]:
    entries: list[dict[str, object]] = []
    for path in sorted(root.rglob("*")):
        if path.is_symlink():
            raise KaggleContractError("artifact trees may not contain symbolic links")
        if not path.is_file():
            continue
        relative = path.relative_to(root).as_posix()
        _validate_relative_path(relative)
        if relative in excluded:
            continue
        entries.append(
            {
                "path": relative,
                "byte_size": path.stat().st_size,
                "sha256": sha256_file(path),
            }
        )
    return entries


def tree_sha256(root: Path, *, excluded: frozenset[str] = frozenset()) -> str:
    return sha256_value({"files": _tree_entries(root, excluded=excluded)})


def mapping_sha256(files: Mapping[str, bytes]) -> str:
    entries: list[dict[str, object]] = []
    for path, content in sorted(files.items()):
        _validate_relative_path(path)
        if not isinstance(content, bytes):
            raise KaggleContractError("artifact content must be bytes")
        entries.append({"path": path, "byte_size": len(content), "sha256": hashlib.sha256(content).hexdigest()})
    if not entries:
        raise KaggleContractError("artifact file set must not be empty")
    if CONTROL_MANIFEST_NAME in files or "dataset-metadata.json" in files:
        raise KaggleContractError("provider-owned metadata paths cannot be supplied by callers")
    return sha256_value({"files": entries})


def directory_sha256(root: Path) -> str:
    """Hash a caller-owned tree without reading the whole checkpoint into memory."""

    if not root.is_dir() or root.is_symlink():
        raise KaggleContractError("artifact source must be a real directory")
    entries = _tree_entries(
        root,
        excluded=frozenset({CONTROL_MANIFEST_NAME, "dataset-metadata.json"}),
    )
    if not entries:
        raise KaggleContractError("artifact source directory must not be empty")
    reserved = {CONTROL_MANIFEST_NAME, "dataset-metadata.json"}
    if any((root / name).exists() for name in reserved):
        raise KaggleContractError("provider-owned metadata paths cannot be supplied by callers")
    return sha256_value({"files": entries})


def _write_files(root: Path, files: Mapping[str, bytes]) -> None:
    for relative, content in files.items():
        _validate_relative_path(relative)
        path = root.joinpath(*relative.split("/"))
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)


def _copy_files(source: Path, destination: Path) -> None:
    # directory_sha256 performs the complete safety walk first.  Copy each
    # regular file independently so a multi-gigabyte checkpoint is never
    # materialized as a bytes mapping in the Kaggle runtime.
    directory_sha256(source)
    for path in sorted(source.rglob("*")):
        if not path.is_file():
            continue
        relative = path.relative_to(source).as_posix()
        target = destination.joinpath(*relative.split("/"))
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(path, target)


def _prepare_destination(destination: Path) -> None:
    if destination.is_symlink():
        raise KaggleContractError("artifact destination must not be a symbolic link")
    if destination.exists():
        if not destination.is_dir() or any(destination.iterdir()):
            raise KaggleContractError("artifact destination must be an empty real directory")
        return
    parent = destination.parent
    if not parent.is_dir() or parent.is_symlink():
        raise KaggleContractError("artifact destination parent must be a real directory")
    destination.mkdir(mode=0o700)


def _canonical_notebook_source(source: bytes, *, kernel_type: str) -> bytes:
    if kernel_type != "notebook":
        return source
    try:
        body = json.loads(source)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise KaggleContractError("notebook source must be valid UTF-8 JSON") from exc
    if not isinstance(body, dict):
        raise KaggleContractError("notebook source must be a JSON object")
    cells = body.get("cells")
    if isinstance(cells, list):
        for cell in cells:
            if not isinstance(cell, dict):
                continue
            if cell.get("cell_type") == "code" and "outputs" in cell:
                cell["outputs"] = []
            if isinstance(cell.get("source"), list):
                cell["source"] = "".join(str(item) for item in cell["source"])
    # Matches the source transformation in kaggle==2.2.4 KaggleApi.kernels_push.
    return json.dumps(body).encode("utf-8")


class KaggleProviderAdapter:
    """The repository's only concrete Kaggle transport adapter.

    Every mutation requires a caller-owned persist-intent journal. There is no
    requests/curl/CLI/direct-client fallback: all provider calls go through the
    injected official ``kaggle==2.2.4`` ``KaggleApi`` surface.
    """

    def __init__(
        self,
        api: KaggleApiProtocol,
        *,
        identity: KaggleProviderIdentity,
        journal: ProviderEffectJournal,
        retry_policy: RetryPolicy | None = None,
        sleep: Any = time.sleep,
        monotonic: Any = time.monotonic,
        clock: Any = lambda: datetime.now(UTC),
        random_source: Any = None,
    ) -> None:
        self.api = api
        self.identity = identity
        self.journal = journal
        self.clock = clock
        self.sleep = sleep
        self.monotonic = monotonic
        self.retry = BoundedRetry(
            retry_policy,
            sleep=sleep,
            monotonic=monotonic,
            wall_clock=clock,
            random_source=random_source,
        )

    @classmethod
    def from_environment(
        cls,
        *,
        journal: ProviderEffectJournal,
        retry_policy: RetryPolicy | None = None,
        sleep: Any = time.sleep,
        monotonic: Any = time.monotonic,
        clock: Any = lambda: datetime.now(UTC),
        random_source: Any = None,
    ) -> KaggleProviderAdapter:
        try:
            installed = importlib.metadata.version(KAGGLE_API_PACKAGE)
        except importlib.metadata.PackageNotFoundError as exc:
            raise KaggleDependencyError("the official kaggle==2.2.4 package is required") from exc
        if installed != KAGGLE_API_VERSION:
            raise KaggleDependencyError(
                f"unsupported Kaggle API package version {installed!r}; exact {KAGGLE_API_VERSION!r} is required"
            )
        try:
            from kaggle.api.kaggle_api_extended import KaggleApi

            api = KaggleApi()
            api.authenticate()
        except SystemExit as exc:
            raise KaggleIdentityError("Kaggle authentication failed closed") from exc
        except Exception as exc:
            raise KaggleIdentityError("Kaggle authentication failed closed") from exc
        username = str(api.get_config_value(api.CONFIG_NAME_USER) or "").strip()
        if not username:
            raise KaggleIdentityError("authenticated Kaggle username is unavailable")
        identity = KaggleProviderIdentity(username=username)
        adapter = cls(
            api,
            identity=identity,
            journal=journal,
            retry_policy=retry_policy,
            sleep=sleep,
            monotonic=monotonic,
            clock=clock,
            random_source=random_source,
        )

        # KaggleApi 2.2.4 routes create/version upload calls through this
        # extension point. Replace its default uncapped Retry-After sleep with
        # this adapter's bounded policy rather than creating another transport.
        def bounded_official_retry(fn: Any, *_args: object, **_kwargs: object) -> Any:
            return adapter.retry.decorator("kaggle_sdk_request")(fn)

        api.with_retry = bounded_official_retry
        return adapter

    def provider_identity(self) -> KaggleProviderIdentity:
        return self.identity

    def list_resources(self, *, kind: ProviderKind, cursor: str | None, limit: int) -> InventoryPage:
        if not 1 <= limit <= 100:
            raise KaggleContractError("Kaggle inventory page limit must be between 1 and 100")
        if kind == ProviderKind.DATASET:
            response, _ = self.retry.call(
                "dataset_list",
                lambda: self.api.dataset_list_with_response(mine=True, page_size=limit, page_token=cursor),
            )
            rows = _field(response, "datasets") or []
        elif kind == ProviderKind.NOTEBOOK:
            response, _ = self.retry.call(
                "kernels_list",
                lambda: self.api.kernels_list_with_response(mine=True, page_size=limit, page_token=cursor),
            )
            rows = _field(response, "kernels") or []
        else:  # pragma: no cover - ProviderKind is closed, retained as fail-closed guard
            raise KaggleContractError(f"unsupported Kaggle resource kind: {kind}")
        if len(rows) > limit:
            raise KaggleContractError("Kaggle returned more inventory rows than requested")
        observed = tuple(self._observed_resource(row, kind) for row in rows)
        next_cursor = str(_field(response, "next_page_token", "nextPageToken") or "").strip() or None
        return InventoryPage(resources=observed, next_cursor=next_cursor)

    def _observed_resource(self, row: object, kind: ProviderKind) -> ObservedProviderResource:
        ref = _normalized_ref(_field(row, "ref", "id"))
        owner = ref.split("/", 1)[0]
        private = _privacy(_field(row, "is_private", "isPrivate", "private"))
        version = _version(
            _field(
                row,
                "current_version_number",
                "currentVersionNumber",
                "version_number",
                "versionNumber",
                "script_version_number",
                "scriptVersionNumber",
            )
        )
        state = str(_field(row, "status", "state") or "unknown").strip().casefold() or "unknown"
        fingerprint = ProviderFingerprint(
            value=sha256_value(
                {
                    "provider": "kaggle",
                    "provider_ref": ref,
                    "kind": kind.value,
                    "private": private,
                    "version": version,
                    "state": state,
                }
            )
        )
        return ObservedProviderResource(
            provider="kaggle",
            provider_ref=ref,
            kind=kind,
            owner=owner,
            private=private,
            fingerprint=fingerprint,
            state=state,
            observed_at=self.clock(),
        )

    def create_private_dataset(
        self,
        *,
        intent: ProviderEffectIntent,
        files: Mapping[str, bytes],
        title: str,
        control_class: ControlClass,
        disposable: bool,
    ) -> DatasetMutationResult:
        if intent.action != MutationAction.CREATE_DATASET:
            raise KaggleContractError("effect intent action does not authorize dataset creation")
        self._validate_control_class(control_class, kind=ProviderKind.DATASET)
        if not 6 <= len(title) <= 50:
            raise KaggleContractError("Kaggle dataset title must contain 6 to 50 characters")
        if not 6 <= len(intent.provider_ref.split("/", 1)[1]) <= 50:
            raise KaggleContractError("Kaggle dataset slug must contain 6 to 50 characters")
        content_sha = mapping_sha256(files)
        self._validate_intent(
            intent,
            arguments={
                "content_tree_sha256": content_sha,
                "control_class": control_class.value,
                "disposable": disposable,
            },
        )
        with tempfile.TemporaryDirectory(prefix="my-data-hub-kaggle-dataset-") as temporary:
            folder = Path(temporary)
            _write_files(folder, files)
            self._write_control_manifest(folder, intent, ProviderKind.DATASET, control_class, disposable)
            expected_package_sha = tree_sha256(folder)
            self._write_dataset_metadata(folder, intent.provider_ref, title)
            self.journal.persist_intent(intent)
            try:
                _response, attempts = self.retry.call(
                    "dataset_create_new",
                    lambda: self.api.dataset_create_new(
                        str(folder),
                        public=False,
                        quiet=True,
                        convert_to_csv=False,
                        dir_mode="zip",
                        ignore_patterns=None,
                    ),
                )
                identity = self._wait_for_dataset(intent.provider_ref, expected_package_sha, expected_version=1)
                outcome = EffectOutcome.APPLIED
            except Exception as exc:
                recovered = self._recover_dataset(intent.provider_ref, expected_package_sha, expected_version=1)
                if recovered is None:
                    self._persist_uncertain(intent, detail="dataset_create_ambiguous")
                    raise KaggleAmbiguousMutation("dataset creation outcome is not exactly reconcilable") from exc
                identity = recovered
                attempts = 0
                outcome = EffectOutcome.ALREADY_APPLIED
        return self._dataset_result(intent, identity, control_class, disposable, attempts, outcome)

    def create_private_dataset_from_directory(
        self,
        *,
        intent: ProviderEffectIntent,
        source_directory: Path,
        title: str,
        control_class: ControlClass,
        disposable: bool,
    ) -> DatasetMutationResult:
        """Create a private dataset by streaming a provider-side directory.

        This is the checkpoint path: it deliberately never converts archive
        files to a ``Mapping[str, bytes]`` and therefore cannot accidentally
        relay checkpoint bytes through the devstand control plane.
        """

        if intent.action != MutationAction.CREATE_DATASET:
            raise KaggleContractError("effect intent action does not authorize dataset creation")
        self._validate_control_class(control_class, kind=ProviderKind.DATASET)
        if not 6 <= len(title) <= 50 or not 6 <= len(intent.provider_ref.split("/", 1)[1]) <= 50:
            raise KaggleContractError("Kaggle dataset title and slug must contain 6 to 50 characters")
        content_sha = directory_sha256(source_directory)
        self._validate_intent(
            intent,
            arguments={
                "content_tree_sha256": content_sha,
                "control_class": control_class.value,
                "disposable": disposable,
            },
        )
        with tempfile.TemporaryDirectory(prefix="my-data-hub-kaggle-dataset-") as temporary:
            folder = Path(temporary)
            _copy_files(source_directory, folder)
            self._write_control_manifest(folder, intent, ProviderKind.DATASET, control_class, disposable)
            expected_package_sha = tree_sha256(folder)
            self._write_dataset_metadata(folder, intent.provider_ref, title)
            self.journal.persist_intent(intent)
            try:
                _response, attempts = self.retry.call(
                    "dataset_create_new",
                    lambda: self.api.dataset_create_new(
                        str(folder),
                        public=False,
                        quiet=True,
                        convert_to_csv=False,
                        dir_mode="zip",
                        ignore_patterns=None,
                    ),
                )
                identity = self._wait_for_dataset(intent.provider_ref, expected_package_sha, expected_version=1)
                outcome = EffectOutcome.APPLIED
            except Exception as exc:
                recovered = self._recover_dataset(intent.provider_ref, expected_package_sha, expected_version=1)
                if recovered is None:
                    self._persist_uncertain(intent, detail="dataset_create_ambiguous")
                    raise KaggleAmbiguousMutation("dataset creation outcome is not exactly reconcilable") from exc
                identity = recovered
                attempts = 0
                outcome = EffectOutcome.ALREADY_APPLIED
        return self._dataset_result(intent, identity, control_class, disposable, attempts, outcome)

    def create_private_dataset_version(
        self,
        *,
        intent: ProviderEffectIntent,
        claim: TaskResourceClaim,
        files: Mapping[str, bytes],
        version_notes: str,
    ) -> DatasetMutationResult:
        self._validate_claim(intent, claim, ProviderKind.DATASET, require_disposable=False)
        if intent.action != MutationAction.VERSION_DATASET:
            raise KaggleContractError("effect intent action does not authorize dataset versioning")
        if not version_notes.strip() or len(version_notes) > 1000:
            raise KaggleContractError("dataset version notes must be bounded and non-empty")
        current = self.read_private_dataset(provider_ref=claim.provider_ref, version=claim.provider_version)
        if current.fingerprint != claim.fingerprint or intent.expected_fingerprint != claim.fingerprint:
            raise KagglePolicyError("dataset changed after its exact task claim")
        content_sha = mapping_sha256(files)
        self._validate_intent(
            intent,
            arguments={
                "content_tree_sha256": content_sha,
                "previous_version": claim.provider_version,
                "version_notes_sha256": hashlib.sha256(version_notes.encode()).hexdigest(),
            },
        )
        expected_version = claim.provider_version + 1
        with tempfile.TemporaryDirectory(prefix="my-data-hub-kaggle-version-") as temporary:
            folder = Path(temporary)
            _write_files(folder, files)
            self._write_control_manifest(folder, intent, ProviderKind.DATASET, claim.control_class, claim.disposable)
            expected_package_sha = tree_sha256(folder)
            self._write_dataset_metadata(folder, intent.provider_ref, intent.provider_ref.split("/", 1)[1])
            self.journal.persist_intent(intent)
            try:
                _response, attempts = self.retry.call(
                    "dataset_create_version",
                    lambda: self.api.dataset_create_version(
                        str(folder),
                        version_notes,
                        quiet=True,
                        convert_to_csv=False,
                        delete_old_versions=False,
                        dir_mode="zip",
                        ignore_patterns=None,
                    ),
                )
                identity = self._wait_for_dataset(
                    intent.provider_ref, expected_package_sha, expected_version=expected_version
                )
                outcome = EffectOutcome.APPLIED
            except Exception as exc:
                recovered = self._recover_dataset(intent.provider_ref, expected_package_sha, expected_version)
                if recovered is None:
                    self._persist_uncertain(intent, detail="dataset_version_ambiguous")
                    raise KaggleAmbiguousMutation("dataset version outcome is not exactly reconcilable") from exc
                identity = recovered
                attempts = 0
                outcome = EffectOutcome.ALREADY_APPLIED
        return self._dataset_result(intent, identity, claim.control_class, claim.disposable, attempts, outcome)

    def create_private_dataset_version_from_directory(
        self,
        *,
        intent: ProviderEffectIntent,
        claim: TaskResourceClaim,
        source_directory: Path,
        version_notes: str,
    ) -> DatasetMutationResult:
        """Create the next exact dataset version from a provider-side tree."""

        self._validate_claim(intent, claim, ProviderKind.DATASET, require_disposable=False)
        if intent.action != MutationAction.VERSION_DATASET:
            raise KaggleContractError("effect intent action does not authorize dataset versioning")
        if not version_notes.strip() or len(version_notes) > 1000:
            raise KaggleContractError("dataset version notes must be bounded and non-empty")
        current = self.read_private_dataset(provider_ref=claim.provider_ref, version=claim.provider_version)
        if current.fingerprint != claim.fingerprint or intent.expected_fingerprint != claim.fingerprint:
            raise KagglePolicyError("dataset changed after its exact task claim")
        content_sha = directory_sha256(source_directory)
        self._validate_intent(
            intent,
            arguments={
                "content_tree_sha256": content_sha,
                "previous_version": claim.provider_version,
                "version_notes_sha256": hashlib.sha256(version_notes.encode()).hexdigest(),
            },
        )
        expected_version = claim.provider_version + 1
        with tempfile.TemporaryDirectory(prefix="my-data-hub-kaggle-version-") as temporary:
            folder = Path(temporary)
            _copy_files(source_directory, folder)
            self._write_control_manifest(
                folder,
                intent,
                ProviderKind.DATASET,
                claim.control_class,
                claim.disposable,
            )
            expected_package_sha = tree_sha256(folder)
            self._write_dataset_metadata(folder, intent.provider_ref, intent.provider_ref.split("/", 1)[1])
            self.journal.persist_intent(intent)
            try:
                _response, attempts = self.retry.call(
                    "dataset_create_version",
                    lambda: self.api.dataset_create_version(
                        str(folder),
                        version_notes,
                        quiet=True,
                        convert_to_csv=False,
                        delete_old_versions=False,
                        dir_mode="zip",
                        ignore_patterns=None,
                    ),
                )
                identity = self._wait_for_dataset(
                    intent.provider_ref,
                    expected_package_sha,
                    expected_version=expected_version,
                )
                outcome = EffectOutcome.APPLIED
            except Exception as exc:
                recovered = self._recover_dataset(intent.provider_ref, expected_package_sha, expected_version)
                if recovered is None:
                    self._persist_uncertain(intent, detail="dataset_version_ambiguous")
                    raise KaggleAmbiguousMutation("dataset version outcome is not exactly reconcilable") from exc
                identity = recovered
                attempts = 0
                outcome = EffectOutcome.ALREADY_APPLIED
        return self._dataset_result(intent, identity, claim.control_class, claim.disposable, attempts, outcome)

    def current_private_dataset_version(self, *, provider_ref: str) -> int | None:
        """Return the exact owned/private current version without downloading bytes."""

        try:
            observed, version, _provider_id = self._find_resource(provider_ref, ProviderKind.DATASET)
        except KaggleNotFound:
            return None
        if observed.private is not True:
            raise KagglePolicyError("dataset privacy was not explicitly proven private")
        if version is None:
            raise KaggleIdentityError("current private dataset version is unavailable")
        return version

    def reconcile_private_dataset_directory_mutation(
        self,
        *,
        intent: ProviderEffectIntent,
        source_directory: Path,
        expected_version: int,
        arguments: Mapping[str, Any],
        control_class: ControlClass,
        disposable: bool,
    ) -> DatasetMutationResult:
        """Repair journal/claim state after an exact Kaggle dataset side effect.

        Reconciliation never mutates Kaggle.  The provider current version must
        equal the one authorized by the intent, and its complete package tree
        must match either the staged source or an exact provider readback.
        """

        if expected_version < 1:
            raise KaggleContractError("reconciled dataset version must be positive")
        self._validate_control_class(control_class, kind=ProviderKind.DATASET)
        self._validate_intent(intent, arguments=arguments)
        current_version = self.current_private_dataset_version(provider_ref=intent.provider_ref)
        if current_version != expected_version:
            raise KaggleAmbiguousMutation("provider current dataset version differs from the exact reconciled version")
        expected_package_sha = self._expected_directory_package_sha256(
            source_directory,
            intent=intent,
            control_class=control_class,
            disposable=disposable,
        )
        identity = self.read_private_dataset(
            provider_ref=intent.provider_ref,
            version=expected_version,
        )
        if identity.package_sha256 != expected_package_sha:
            raise KaggleAmbiguousMutation("provider dataset version differs from the intended exact package")
        self.journal.persist_intent(intent)
        return self._dataset_result(
            intent,
            identity,
            control_class,
            disposable,
            attempts=0,
            outcome=EffectOutcome.ALREADY_APPLIED,
        )

    def read_private_dataset(self, *, provider_ref: str, version: int) -> KaggleDatasetIdentity:
        with tempfile.TemporaryDirectory(prefix="my-data-hub-kaggle-readback-") as temporary:
            return self.download_private_dataset_exact(
                provider_ref=provider_ref,
                version=version,
                destination=Path(temporary),
            )

    def download_private_dataset_exact(
        self,
        *,
        provider_ref: str,
        version: int,
        destination: Path,
    ) -> KaggleDatasetIdentity:
        """Download one exact numeric private dataset version to ``destination``."""

        ref = _normalized_ref(provider_ref)
        if version < 1:
            raise KaggleContractError("dataset version must be positive")
        observed, observed_version, _provider_id = self._find_resource(ref, ProviderKind.DATASET)
        if observed.private is not True:
            raise KagglePolicyError("dataset privacy was not explicitly proven private")
        if observed_version is not None and version > observed_version:
            raise KaggleNotFound("requested dataset version is newer than the provider current version")
        _prepare_destination(destination)
        try:
            self.retry.call(
                "dataset_download_files",
                lambda: self.api.dataset_download_files(
                    f"{ref}/{version}",
                    path=str(destination),
                    force=True,
                    quiet=True,
                    unzip=True,
                    licenses=[],
                ),
            )
            package_sha = tree_sha256(destination)
        except Exception:
            shutil.rmtree(destination, ignore_errors=True)
            raise
        fingerprint = ProviderFingerprint(
            value=sha256_value(
                {"provider_ref": ref, "version": version, "privacy": "private", "package_sha256": package_sha}
            )
        )
        return KaggleDatasetIdentity(
            provider_ref=ref,
            version=version,
            privacy="private",
            package_sha256=package_sha,
            fingerprint=fingerprint,
            observed_at=self.clock(),
        )

    def push_private_notebook(
        self,
        *,
        intent: ProviderEffectIntent,
        task_run_id: UUID,
        source: bytes,
        title: str,
        code_file: str,
        kernel_type: str,
        language: str,
        control_class: ControlClass,
        disposable: bool,
        dataset_sources: Sequence[str] = (),
        enable_internet: bool = False,
        timeout_seconds: int | None = None,
    ) -> NotebookMutationResult:
        if intent.action != MutationAction.PUSH_NOTEBOOK:
            raise KaggleContractError("effect intent action does not authorize notebook push/run")
        self._validate_control_class(control_class, kind=ProviderKind.NOTEBOOK)
        _validate_relative_path(code_file)
        if kernel_type not in {"script", "notebook"}:
            raise KaggleContractError("kernel_type must be script or notebook")
        if language not in {"python", "r", "rmarkdown", "sqlite", "julia"}:
            raise KaggleContractError("unsupported Kaggle kernel language")
        if not 5 <= len(title) <= 80:
            raise KaggleContractError("Kaggle notebook title must contain 5 to 80 characters")
        if title != intent.provider_ref.split("/", 1)[1]:
            raise KaggleContractError(
                "Kaggle notebook title must equal the exact requested slug to prevent provider-side identity rewrite"
            )
        canonical_source = _canonical_notebook_source(source, kernel_type=kernel_type)
        if str(task_run_id).encode("ascii") not in canonical_source:
            raise KaggleContractError("notebook source must embed the exact task_run_id")
        source_sha = hashlib.sha256(canonical_source).hexdigest()
        normalized_sources = tuple(_normalized_dataset_source(item) for item in dataset_sources)
        self._validate_intent(
            intent,
            arguments={
                "task_run_id": str(task_run_id),
                "source_sha256": source_sha,
                "dataset_sources": normalized_sources,
                "control_class": control_class.value,
                "disposable": disposable,
            },
        )
        with tempfile.TemporaryDirectory(prefix="my-data-hub-kaggle-kernel-") as temporary:
            folder = Path(temporary)
            code_path = folder.joinpath(*code_file.split("/"))
            code_path.parent.mkdir(parents=True, exist_ok=True)
            code_path.write_bytes(source)
            metadata = {
                "id": intent.provider_ref,
                "title": title,
                "code_file": code_file,
                "language": language,
                "kernel_type": kernel_type,
                "is_private": True,
                "enable_gpu": False,
                "enable_tpu": False,
                "enable_internet": bool(enable_internet),
                "dataset_sources": list(normalized_sources),
                "kernel_sources": [],
                "competition_sources": [],
                "model_sources": [],
            }
            (folder / "kernel-metadata.json").write_bytes(canonical_json_bytes(metadata))
            self.journal.persist_intent(intent)
            try:
                response, attempts = self.retry.call(
                    "kernels_push",
                    lambda: self.api.kernels_push(
                        str(folder), timeout=str(timeout_seconds) if timeout_seconds is not None else None, acc=None
                    ),
                )
                ref = _normalized_ref(_field(response, "ref") or intent.provider_ref)
                version = _version(_field(response, "version_number", "versionNumber"))
                provider_kernel_id = _version(_field(response, "kernel_id", "kernelId"))
                error = str(_field(response, "error") or "").strip()
                if error:
                    raise KaggleTerminalFailure(f"Kaggle rejected notebook push: {error[:500]}")
                if ref != intent.provider_ref or version is None or provider_kernel_id is None:
                    raise KaggleIdentityError("Kaggle push response lacks the exact requested ref/kernel-id/version")
                source_identity = self.read_private_notebook_source(
                    provider_ref=ref, source_version=version, expected_source_sha256=source_sha
                )
                outcome = EffectOutcome.APPLIED
            except Exception as exc:
                recovered = self._recover_notebook(intent.provider_ref, source_sha)
                if recovered is None:
                    self._persist_uncertain(intent, detail="notebook_push_ambiguous")
                    raise KaggleAmbiguousMutation("notebook push/run outcome is not exactly reconcilable") from exc
                source_identity = recovered
                _observed, _version_number, provider_kernel_id = self._find_resource(
                    intent.provider_ref, ProviderKind.NOTEBOOK
                )
                if provider_kernel_id is None:
                    raise KaggleIdentityError("recovered Kaggle run lacks an exact provider kernel id") from exc
                attempts = 0
                outcome = EffectOutcome.ALREADY_APPLIED
        run = KaggleKernelRunIdentity(
            task_run_id=task_run_id,
            provider_ref=source_identity.provider_ref,
            source_version=source_identity.source_version,
            source_sha256=source_identity.source_sha256,
            provider_kernel_id=provider_kernel_id,
            provider_run_ref=f"{source_identity.provider_ref}/{source_identity.source_version}",
            started_at=self.clock(),
        )
        receipt = ProviderEffectReceipt(
            operation_id=intent.operation_id,
            effect_id=intent.effect_id,
            action=intent.action,
            provider_ref=intent.provider_ref,
            outcome=outcome,
            attempts=attempts,
            observed_fingerprint=source_identity.fingerprint,
            provider_version=source_identity.source_version,
            observed_at=self.clock(),
            detail_code="private_notebook_pushed_and_run",
        )
        claim = TaskResourceClaim.create(
            task_id=intent.task_id,
            effect_id=intent.effect_id,
            provider_ref=intent.provider_ref,
            kind=ProviderKind.NOTEBOOK,
            control_class=control_class,
            disposable=disposable,
            fingerprint=source_identity.fingerprint,
            provider_version=source_identity.source_version,
            registered_at=intent.requested_at,
        )
        self.journal.persist_receipt(receipt)
        self.journal.persist_resource_claim(claim)
        return NotebookMutationResult(source=source_identity, run=run, claim=claim, effect=receipt)

    def read_private_notebook_source(
        self,
        *,
        provider_ref: str,
        source_version: int,
        expected_source_sha256: str | None = None,
    ) -> KaggleKernelSourceIdentity:
        ref = _normalized_ref(provider_ref)
        source_sha, _provider_id = self._pull_private_notebook_source(ref, source_version)
        if expected_source_sha256 is not None and source_sha != expected_source_sha256:
            raise KaggleIdentityError("Kaggle source readback differs from the exact pushed bytes")
        fingerprint = ProviderFingerprint(
            value=sha256_value(
                {
                    "provider_ref": ref,
                    "source_version": source_version,
                    "privacy": "private",
                    "source_sha256": source_sha,
                }
            )
        )
        return KaggleKernelSourceIdentity(
            provider_ref=ref,
            source_version=source_version,
            privacy="private",
            source_sha256=source_sha,
            fingerprint=fingerprint,
            observed_at=self.clock(),
        )

    def _pull_private_notebook_source(self, ref: str, source_version: int | None) -> tuple[str, int]:
        with tempfile.TemporaryDirectory(prefix="my-data-hub-kaggle-source-") as temporary:
            folder = Path(temporary)
            pulled, _ = self.retry.call(
                "kernels_pull",
                lambda: self.api.kernels_pull(
                    f"{ref}/{source_version}" if source_version is not None else ref,
                    path=str(folder),
                    metadata=True,
                    quiet=True,
                ),
            )
            pulled_root = Path(str(pulled))
            if pulled_root.resolve() != folder.resolve():
                raise KaggleContractError("Kaggle source pull escaped the bounded target directory")
            metadata_path = folder / "kernel-metadata.json"
            if not metadata_path.is_file():
                raise KaggleIdentityError("Kaggle source readback omitted exact kernel metadata")
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
            if metadata.get("id") != ref or metadata.get("is_private") is not True:
                raise KagglePolicyError("notebook exact identity/privacy was not proven")
            provider_id = _version(metadata.get("id_no"))
            if provider_id is None:
                raise KaggleIdentityError("Kaggle source metadata omitted the numeric kernel identity")
            code_file = str(metadata.get("code_file") or "")
            _validate_relative_path(code_file)
            source_path = folder.joinpath(*code_file.split("/"))
            if not source_path.is_file() or folder not in source_path.parents:
                raise KaggleContractError("Kaggle source pull did not return a bounded local file")
            source_sha = sha256_file(source_path)
        return source_sha, provider_id

    def read_run_status(self, run: KaggleKernelRunIdentity) -> KaggleKernelStatus:
        self._assert_current_run(run)
        response, _ = self.retry.call("kernels_status", lambda: self.api.kernels_status(run.provider_ref))
        raw = _field(response, "status")
        raw_name = str(getattr(raw, "name", raw) or "unknown").strip().casefold()
        if raw_name in _QUEUED:
            state = KernelState.QUEUED
        elif raw_name in _RUNNING:
            state = KernelState.RUNNING
        elif raw_name in _COMPLETE:
            state = KernelState.COMPLETE
        elif raw_name in _FAILED:
            state = KernelState.FAILED
        else:
            state = KernelState.UNKNOWN
        failure = str(_field(response, "failure_message", "failureMessage") or "").strip() or None
        return KaggleKernelStatus(
            run=run,
            state=state,
            provider_status=raw_name or "unknown",
            failure_message=failure,
            observed_at=self.clock(),
        )

    def reconcile_private_notebook_run(
        self,
        *,
        task_run_id: UUID,
        provider_ref: str,
        expected_source_sha256: str,
    ) -> KaggleKernelRunIdentity | None:
        """Find an exact pushed source/run without launching a second version.

        Kaggle status is latest-by-slug, so reconciliation first binds the
        current private source hash and numeric kernel identity.  A different
        current source is deliberately ambiguous to the lifecycle bridge.
        """

        recovered = self._recover_notebook(provider_ref, expected_source_sha256)
        if recovered is None:
            return None
        try:
            _observed, _version_number, provider_kernel_id = self._find_resource(
                recovered.provider_ref, ProviderKind.NOTEBOOK
            )
        except Exception:
            return None
        if provider_kernel_id is None:
            return None
        return KaggleKernelRunIdentity(
            task_run_id=task_run_id,
            provider_ref=recovered.provider_ref,
            source_version=recovered.source_version,
            source_sha256=recovered.source_sha256,
            provider_kernel_id=provider_kernel_id,
            provider_run_ref=f"{recovered.provider_ref}/{recovered.source_version}",
            started_at=self.clock(),
        )

    def reconcile_private_notebook_mutation(
        self,
        *,
        intent: ProviderEffectIntent,
        task_run_id: UUID,
        expected_source_sha256: str,
        dataset_sources: Sequence[str],
        control_class: ControlClass,
        disposable: bool,
    ) -> NotebookMutationResult | None:
        """Repair a pushed exact notebook's remote journal without another push."""

        normalized_sources = tuple(_normalized_dataset_source(item) for item in dataset_sources)
        arguments = {
            "task_run_id": str(task_run_id),
            "source_sha256": expected_source_sha256,
            "dataset_sources": normalized_sources,
            "control_class": control_class.value,
            "disposable": disposable,
        }
        self._validate_control_class(control_class, kind=ProviderKind.NOTEBOOK)
        self._validate_intent(intent, arguments=arguments)
        run = self.reconcile_private_notebook_run(
            task_run_id=task_run_id,
            provider_ref=intent.provider_ref,
            expected_source_sha256=expected_source_sha256,
        )
        if run is None:
            return None
        source = self.read_private_notebook_source(
            provider_ref=run.provider_ref,
            source_version=run.source_version,
            expected_source_sha256=expected_source_sha256,
        )
        receipt = ProviderEffectReceipt(
            operation_id=intent.operation_id,
            effect_id=intent.effect_id,
            action=intent.action,
            provider_ref=intent.provider_ref,
            outcome=EffectOutcome.ALREADY_APPLIED,
            attempts=0,
            observed_fingerprint=source.fingerprint,
            provider_version=source.source_version,
            observed_at=self.clock(),
            detail_code="private_notebook_exact_reconciliation",
        )
        claim = TaskResourceClaim.create(
            task_id=intent.task_id,
            effect_id=intent.effect_id,
            provider_ref=intent.provider_ref,
            kind=ProviderKind.NOTEBOOK,
            control_class=control_class,
            disposable=disposable,
            fingerprint=source.fingerprint,
            provider_version=source.source_version,
            registered_at=intent.requested_at,
        )
        self.journal.persist_intent(intent)
        self.journal.persist_receipt(receipt)
        self.journal.persist_resource_claim(claim)
        return NotebookMutationResult(source=source, run=run, claim=claim, effect=receipt)

    def poll_run(self, run: KaggleKernelRunIdentity, policy: PollPolicy | None = None) -> KaggleKernelStatus:
        policy = policy or PollPolicy()
        started = self.monotonic()
        for poll in range(policy.max_polls):
            status = self.read_run_status(run)
            if status.state == KernelState.COMPLETE:
                return status
            if status.state == KernelState.FAILED:
                raise KaggleTerminalFailure(
                    f"Kaggle run {run.provider_run_ref} failed: {status.failure_message or status.provider_status}"
                )
            elapsed = max(0.0, self.monotonic() - started)
            if poll + 1 >= policy.max_polls or elapsed + policy.interval_seconds > policy.timeout_seconds:
                break
            self.sleep(policy.interval_seconds)
        raise KagglePollingTimeout(f"Kaggle run {run.provider_run_ref} exceeded its bounded polling policy")

    def read_exact_run_output(self, run: KaggleKernelRunIdentity) -> KaggleKernelOutputIdentity:
        with tempfile.TemporaryDirectory(prefix="my-data-hub-kaggle-output-") as temporary:
            folder = Path(temporary)
            downloaded = self.download_exact_run_output_tree(run, destination=folder)
            receipt_path = folder / RUN_RECEIPT_NAME
            if not receipt_path.is_file():
                raise KaggleIdentityError("Kaggle output lacks the exact runtime identity receipt")
            receipt_bytes = receipt_path.read_bytes()
            if len(receipt_bytes) > 64 * 1024:
                raise KaggleContractError("Kaggle runtime identity receipt exceeds 64 KiB")
            try:
                runtime_receipt = json.loads(receipt_bytes)
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise KaggleContractError("Kaggle runtime receipt is not valid UTF-8 JSON") from exc
            expected = {
                "task_run_id": str(run.task_run_id),
                "provider_ref": run.provider_ref,
                "source_version": run.source_version,
                "source_sha256": run.source_sha256,
                "terminal_state": "complete",
            }
            if not isinstance(runtime_receipt, dict) or any(runtime_receipt.get(k) != v for k, v in expected.items()):
                raise KaggleIdentityError("Kaggle output receipt belongs to a stale or different run")
            receipt_sha = hashlib.sha256(receipt_bytes).hexdigest()
        return KaggleKernelOutputIdentity(
            run=run,
            terminal_state=KernelState.COMPLETE,
            output_tree_sha256=downloaded.output_tree_sha256,
            receipt_sha256=receipt_sha,
            file_count=downloaded.file_count,
            observed_at=self.clock(),
        )

    def download_exact_run_output_tree(
        self,
        run: KaggleKernelRunIdentity,
        *,
        destination: Path,
    ) -> KaggleKernelOutputTreeIdentity:
        """Copy current output while fencing it to the exact numeric run identity.

        Kaggle's 2.2.4 output endpoint internally resolves the current session
        even when a numeric version is supplied.  The adapter therefore asserts
        the numeric source/run before *and* after the destination-preserving
        download and calls the official API with ``provider_run_ref``.  A
        concurrent source advance fails closed rather than mislabelling bytes.
        """

        status = self.read_run_status(run)
        if status.state != KernelState.COMPLETE:
            raise KaggleContractError("run output is unavailable before exact terminal completion")
        _prepare_destination(destination)
        try:
            self.retry.call(
                "kernels_output",
                lambda: self.api.kernels_output(
                    run.provider_run_ref,
                    path=str(destination),
                    file_pattern=None,
                    force=True,
                    quiet=True,
                    page_token=None,
                    page_size=100,
                ),
            )
            entries = _tree_entries(destination)
            output_sha = sha256_value({"files": entries})
        except Exception:
            shutil.rmtree(destination, ignore_errors=True)
            raise
        self._assert_current_run(run)
        return KaggleKernelOutputTreeIdentity(
            run=run,
            terminal_state=KernelState.COMPLETE,
            output_tree_sha256=output_sha,
            file_count=len(entries),
            observed_at=self.clock(),
        )

    def download_exact_run_output_file(
        self,
        run: KaggleKernelRunIdentity,
        *,
        destination: Path,
        file_name: str,
        max_bytes: int,
    ) -> KaggleKernelOutputTreeIdentity:
        """Download one exact top-level output without copying the run's broad tree.

        Kaggle resolves output against the current session even when given the
        numeric run ref.  As with broad checkpoint output, fence the source/run
        before and after the official ``kernels_output`` call.  Unlike that
        path, this method supplies an anchored file pattern and rejects any API
        implementation that writes a missing, nested, symlinked, or extra file.
        """

        if not file_name or Path(file_name).name != file_name or "/" in file_name or "\\" in file_name:
            raise KaggleContractError("exact output file name must be one top-level basename")
        _validate_relative_path(file_name)
        if not 1 <= max_bytes <= 64 * 1024 * 1024:
            raise KaggleContractError("exact output file size bound is invalid")
        status = self.read_run_status(run)
        if status.state != KernelState.COMPLETE:
            raise KaggleContractError("run output is unavailable before exact terminal completion")
        _prepare_destination(destination)
        pattern = rf"^{re.escape(file_name)}$"
        try:
            self.retry.call(
                "kernels_output",
                lambda: self.api.kernels_output(
                    run.provider_run_ref,
                    path=str(destination),
                    file_pattern=pattern,
                    force=True,
                    quiet=True,
                    page_token=None,
                    page_size=100,
                ),
            )
            entries = _tree_entries(destination)
            kernel_slug = run.provider_ref.split("/", 1)[1]
            provider_log_name = f"{kernel_slug}.log"
            paths = {str(entry["path"]) for entry in entries}
            if file_name not in paths or paths - {file_name, provider_log_name}:
                raise KaggleIdentityError("Kaggle exact output file selection returned a missing or extra path")
            receipt_entry = next(entry for entry in entries if entry["path"] == file_name)
            if int(receipt_entry["byte_size"]) > max_bytes:
                raise KaggleContractError("Kaggle exact output file exceeds its bounded size")
            if provider_log_name in paths:
                log_entry = next(entry for entry in entries if entry["path"] == provider_log_name)
                if int(log_entry["byte_size"]) > MAX_EXACT_OUTPUT_PROVIDER_LOG_BYTES:
                    raise KaggleContractError("Kaggle provider output log exceeds its bounded size")
                (destination / provider_log_name).unlink()
            remaining = _tree_entries(destination)
            if len(remaining) != 1 or remaining[0]["path"] != file_name:
                raise KaggleIdentityError("Kaggle exact output destination retained an unexpected path")
            output_sha = sha256_value({"files": remaining})
        except Exception:
            shutil.rmtree(destination, ignore_errors=True)
            raise
        self._assert_current_run(run)
        return KaggleKernelOutputTreeIdentity(
            run=run,
            terminal_state=KernelState.COMPLETE,
            output_tree_sha256=output_sha,
            file_count=1,
            observed_at=self.clock(),
        )

    def download_exact_failed_run_output_file(
        self,
        run: KaggleKernelRunIdentity,
        *,
        destination: Path,
        file_name: str,
        max_bytes: int,
    ) -> KaggleKernelFailureOutputIdentity:
        """Read one bounded receipt from an exact terminal failed run.

        ``kaggle==2.2.4`` applies ``file_pattern`` to ordinary outputs but
        writes a supplied response log to ``<kernel-slug>.log`` independently
        of that pattern. The official SDK call therefore has a bounded
        post-download residual: at most the requested 64-KiB receipt plus a
        1-MiB provider log. The log is size-checked and deleted before return;
        any other file or pattern violation fails closed and removes the
        destination.
        """

        if not file_name or Path(file_name).name != file_name or "/" in file_name or "\\" in file_name:
            raise KaggleContractError("failed output receipt name must be one top-level basename")
        _validate_relative_path(file_name)
        provider_log_name = f"{run.provider_ref.split('/', 1)[1]}.log"
        if file_name == provider_log_name:
            raise KaggleContractError("failed output receipt cannot alias the SDK provider log")
        if not 1 <= max_bytes <= 64 * 1024:
            raise KaggleContractError("failed output receipt bound must be between 1 and 64 KiB")
        before = self.read_run_status(run)
        if before.state != KernelState.FAILED or before.provider_status not in {"failed", "error"}:
            raise KaggleContractError("failed output requires exact provider status FAILED or ERROR")
        _prepare_destination(destination)
        pattern = rf"^{re.escape(file_name)}$"
        try:
            self.retry.call(
                "kernels_output_failed_receipt",
                lambda: self.api.kernels_output(
                    run.provider_run_ref,
                    path=str(destination),
                    file_pattern=pattern,
                    force=True,
                    quiet=True,
                    page_token=None,
                    page_size=100,
                ),
            )
            entries = _tree_entries(destination)
            paths = {str(entry["path"]) for entry in entries}
            if file_name not in paths or paths - {file_name, provider_log_name}:
                raise KaggleIdentityError("failed exact output returned a missing or extra file")
            receipt_entry = next(entry for entry in entries if entry["path"] == file_name)
            if int(receipt_entry["byte_size"]) > max_bytes:
                raise KaggleContractError("failed output receipt exceeds its bounded size")
            receipt_sha = str(receipt_entry["sha256"])
            if provider_log_name in paths:
                log_entry = next(entry for entry in entries if entry["path"] == provider_log_name)
                if int(log_entry["byte_size"]) > MAX_EXACT_OUTPUT_PROVIDER_LOG_BYTES:
                    raise KaggleContractError("failed provider output log exceeds its bounded size")
                (destination / provider_log_name).unlink()
            remaining = _tree_entries(destination)
            if len(remaining) != 1 or remaining[0]["path"] != file_name:
                raise KaggleIdentityError("failed output destination retained an unexpected file")
            output_sha = sha256_value({"files": remaining})
            after = self.read_run_status(run)
            if (
                after.state != KernelState.FAILED
                or after.provider_status not in {"failed", "error"}
                or after.provider_status != before.provider_status
            ):
                raise KaggleIdentityError("failed run status changed across exact output read")
        except Exception:
            shutil.rmtree(destination, ignore_errors=True)
            raise
        return KaggleKernelFailureOutputIdentity(
            run=run,
            terminal_state=KernelState.FAILED,
            provider_status=after.provider_status,
            output_tree_sha256=output_sha,
            receipt_sha256=receipt_sha,
            file_count=1,
            observed_at=self.clock(),
        )

    def prove_private_dataset_access(
        self,
        *,
        provider_ref: str,
        version: int,
        unauthenticated_probe: UnauthenticatedDatasetProbe,
    ) -> PrivateAccessProof:
        authenticated = self.read_private_dataset(provider_ref=provider_ref, version=version)
        try:
            unauthenticated_probe.read_dataset(authenticated.provider_ref, authenticated.version)
        except Exception as exc:
            failure = classify_failure(exc, now=self.clock())
            if failure.http_status not in {401, 403, 404}:
                raise KagglePolicyError("unauthenticated private-dataset denial was not proven") from exc
        else:
            raise KagglePolicyError("unauthenticated client unexpectedly read the private dataset")
        denial = {
            RetryClass.AUTHENTICATION: "authentication",
            RetryClass.AUTHORIZATION: "authorization",
            RetryClass.NOT_FOUND: "not_found",
        }[failure.retry_class]
        return PrivateAccessProof(
            provider_ref=authenticated.provider_ref,
            provider_version=authenticated.version,
            authenticated_readback_sha256=authenticated.package_sha256,
            unauthenticated_http_status=failure.http_status,
            denial_class=denial,
            observed_at=self.clock(),
        )

    def delete_task_created_resource(
        self,
        *,
        intent: ProviderEffectIntent,
        claim: TaskResourceClaim,
    ) -> ProviderEffectReceipt:
        self._validate_claim(intent, claim, claim.kind, require_disposable=True)
        expected_action = (
            MutationAction.DELETE_DATASET if claim.kind == ProviderKind.DATASET else MutationAction.DELETE_NOTEBOOK
        )
        if intent.action != expected_action:
            raise KaggleContractError("effect intent action does not authorize this exact delete")
        self._validate_intent(
            intent,
            arguments={"claim_sha256": claim.claim_sha256, "provider_version": claim.provider_version},
        )
        self.journal.persist_intent(intent)
        if self._is_absent(claim.provider_ref, claim.kind):
            receipt = ProviderEffectReceipt(
                operation_id=intent.operation_id,
                effect_id=intent.effect_id,
                action=intent.action,
                provider_ref=intent.provider_ref,
                outcome=EffectOutcome.ALREADY_APPLIED,
                attempts=0,
                observed_at=self.clock(),
                detail_code="task_created_resource_already_absent",
            )
            self.journal.persist_receipt(receipt)
            return receipt
        if claim.kind == ProviderKind.DATASET:
            current = self.read_private_dataset(provider_ref=claim.provider_ref, version=claim.provider_version)
            current_fingerprint = current.fingerprint
        else:
            current_source = self.read_private_notebook_source(
                provider_ref=claim.provider_ref,
                source_version=claim.provider_version,
                expected_source_sha256=None,
            )
            current_fingerprint = current_source.fingerprint
        if current_fingerprint != claim.fingerprint or intent.expected_fingerprint != claim.fingerprint:
            raise KagglePolicyError("cleanup target differs from the exact task-created fingerprint")
        try:
            if claim.kind == ProviderKind.DATASET:
                owner, slug = claim.provider_ref.split("/", 1)
                _result, attempts = self.retry.call(
                    "dataset_delete", lambda: self.api.dataset_delete(owner, slug, no_confirm=True)
                )
            else:
                _result, attempts = self.retry.call(
                    "kernels_delete", lambda: self.api.kernels_delete(claim.provider_ref, no_confirm=True)
                )
        except Exception as exc:
            if not self._is_absent(claim.provider_ref, claim.kind):
                self._persist_uncertain(intent, detail="delete_ambiguous")
                raise KaggleAmbiguousMutation("cleanup outcome is not exactly reconcilable") from exc
            attempts = 0
        if not self._is_absent(claim.provider_ref, claim.kind):
            self._persist_uncertain(intent, detail="delete_still_present")
            raise KaggleAmbiguousMutation("provider resource remains after delete")
        receipt = ProviderEffectReceipt(
            operation_id=intent.operation_id,
            effect_id=intent.effect_id,
            action=intent.action,
            provider_ref=intent.provider_ref,
            outcome=EffectOutcome.APPLIED,
            attempts=attempts,
            observed_at=self.clock(),
            detail_code="task_created_resource_absent",
        )
        self.journal.persist_receipt(receipt)
        return receipt

    def _wait_for_dataset(
        self, provider_ref: str, expected_package_sha: str, *, expected_version: int
    ) -> KaggleDatasetIdentity:
        last_status = "unknown"
        for poll in range(24):
            try:
                status_raw, _ = self.retry.call(
                    "dataset_status",
                    lambda: self.api.dataset_status(provider_ref, format="json(status,current_version_number)"),
                )
                parsed = json.loads(status_raw)
                last_status = str(parsed.get("status") or "unknown").casefold()
                current_version = _version(parsed.get("current_version_number"))
                if last_status == "ready" and current_version == expected_version:
                    identity = self.read_private_dataset(provider_ref=provider_ref, version=expected_version)
                    if identity.package_sha256 != expected_package_sha:
                        raise KaggleIdentityError("dataset exact readback hash differs from staged package")
                    return identity
                if current_version is not None and current_version > expected_version:
                    raise KaggleIdentityError("dataset version advanced beyond the intended exact version")
            except KaggleIdentityError:
                raise
            except Exception:
                pass
            if poll < 23:
                self.sleep(5.0)
        raise KagglePollingTimeout(
            f"private dataset {provider_ref}/{expected_version} did not become exactly ready: {last_status}"
        )

    def _recover_dataset(
        self, provider_ref: str, expected_package_sha: str, expected_version: int
    ) -> KaggleDatasetIdentity | None:
        try:
            identity = self.read_private_dataset(provider_ref=provider_ref, version=expected_version)
        except Exception:
            return None
        return identity if identity.package_sha256 == expected_package_sha else None

    def _recover_notebook(self, provider_ref: str, source_sha: str) -> KaggleKernelSourceIdentity | None:
        try:
            _observed, version, _provider_id = self._find_resource(provider_ref, ProviderKind.NOTEBOOK)
            if version is None:
                return None
            return self.read_private_notebook_source(
                provider_ref=provider_ref,
                source_version=version,
                expected_source_sha256=source_sha,
            )
        except Exception:
            return None

    def _find_resource(
        self, provider_ref: str, kind: ProviderKind
    ) -> tuple[ObservedProviderResource, int | None, int | None]:
        ref = _normalized_ref(provider_ref)
        slug = ref.split("/", 1)[1]
        cursor: str | None = None
        seen: set[str] = set()
        for _ in range(20):
            if kind == ProviderKind.DATASET:
                response, _attempts = self.retry.call(
                    "dataset_inventory_lookup",
                    lambda cursor=cursor: self.api.dataset_list_with_response(
                        mine=True,
                        search=slug,
                        sort_by="updated",
                        page_size=100,
                        page_token=cursor,
                    ),
                )
                rows = _field(response, "datasets") or []
            else:
                response, _attempts = self.retry.call(
                    "kernel_inventory_lookup",
                    lambda cursor=cursor: self.api.kernels_list_with_response(
                        mine=True,
                        search=slug,
                        page_size=100,
                        page_token=cursor,
                    ),
                )
                rows = _field(response, "kernels") or []
            for row in rows:
                observed = self._observed_resource(row, kind)
                if observed.provider_ref == ref:
                    version = _version(
                        _field(
                            row,
                            "current_version_number",
                            "currentVersionNumber",
                            "version_number",
                            "versionNumber",
                            "script_version_number",
                            "scriptVersionNumber",
                        )
                    )
                    provider_id = _version(_field(row, "id", "kernel_id", "kernelId"))
                    return observed, version, provider_id
            next_cursor = str(_field(response, "next_page_token", "nextPageToken") or "").strip() or None
            if next_cursor is None:
                break
            if next_cursor in seen:
                raise KaggleContractError("Kaggle inventory repeated a cursor")
            seen.add(next_cursor)
            cursor = next_cursor
        raise KaggleNotFound(f"Kaggle {kind.value} {ref} was not found in owned inventory")

    def _assert_current_run(self, run: KaggleKernelRunIdentity) -> None:
        current_sha, provider_kernel_id = self._pull_private_notebook_source(run.provider_ref, None)
        if current_sha != run.source_sha256:
            raise KaggleIdentityError("Kaggle status/output is latest-by-slug and source has advanced")
        if provider_kernel_id != run.provider_kernel_id:
            raise KaggleIdentityError("Kaggle status/output belongs to a different provider kernel id")
        self.read_private_notebook_source(
            provider_ref=run.provider_ref,
            source_version=run.source_version,
            expected_source_sha256=run.source_sha256,
        )

    def _is_absent(self, provider_ref: str, kind: ProviderKind) -> bool:
        try:
            self._find_resource(provider_ref, kind)
        except KaggleNotFound:
            return True
        except Exception:
            return False
        return False

    def _write_dataset_metadata(self, folder: Path, provider_ref: str, title: str) -> None:
        metadata = {"title": title, "id": provider_ref, "licenses": [{"name": "CC0-1.0"}]}
        (folder / "dataset-metadata.json").write_bytes(canonical_json_bytes(metadata))

    def _expected_directory_package_sha256(
        self,
        source: Path,
        *,
        intent: ProviderEffectIntent,
        control_class: ControlClass,
        disposable: bool,
    ) -> str:
        directory_sha256(source)
        with tempfile.TemporaryDirectory(prefix="my-data-hub-kaggle-reconcile-") as temporary:
            staged = Path(temporary)
            _copy_files(source, staged)
            self._write_control_manifest(
                staged,
                intent,
                ProviderKind.DATASET,
                control_class,
                disposable,
            )
            return tree_sha256(staged)

    def _write_control_manifest(
        self,
        folder: Path,
        intent: ProviderEffectIntent,
        kind: ProviderKind,
        control_class: ControlClass,
        disposable: bool,
    ) -> None:
        manifest = {
            "contract_version": "my-data-hub-kaggle-resource.v1",
            "task_id": str(intent.task_id),
            "effect_id": str(intent.effect_id),
            "provider_ref": intent.provider_ref,
            "kind": kind.value,
            "control_class": control_class.value,
            "disposable": disposable,
            "request_sha256": intent.request_sha256,
            "private": True,
        }
        (folder / CONTROL_MANIFEST_NAME).write_bytes(canonical_json_bytes(manifest))

    def _validate_control_class(self, control_class: ControlClass, *, kind: ProviderKind) -> None:
        if control_class not in _CONTROLLED_CLASSES:
            raise KagglePolicyError("new resources require an explicitly controlled class")
        if control_class == ControlClass.MCP_EXCHANGE and kind != ProviderKind.DATASET:
            raise KagglePolicyError("mcp_exchange resources must be private datasets")

    def _validate_intent(self, intent: ProviderEffectIntent, *, arguments: Mapping[str, Any]) -> None:
        if intent.provider_ref.split("/", 1)[0] != self.identity.username:
            raise KagglePolicyError("provider effect target is not owned by the authenticated exact identity")
        expected = ProviderEffectIntent.create(
            operation_id=intent.operation_id,
            effect_id=intent.effect_id,
            idempotency_key=intent.idempotency_key,
            task_id=intent.task_id,
            action=intent.action,
            provider_ref=intent.provider_ref,
            expected_fingerprint=intent.expected_fingerprint,
            arguments=arguments,
            requested_at=intent.requested_at,
        )
        if expected.request_sha256 != intent.request_sha256:
            raise KaggleContractError("effect request hash does not bind the exact provider arguments")

    def _validate_claim(
        self,
        intent: ProviderEffectIntent,
        claim: TaskResourceClaim,
        kind: ProviderKind,
        *,
        require_disposable: bool,
    ) -> None:
        self.journal.assert_resource_claim(claim)
        if claim.provider_ref != intent.provider_ref or claim.kind != kind:
            raise KagglePolicyError("effect target differs from the exact task-created claim")
        if claim.task_id != intent.task_id:
            raise KagglePolicyError("effect task differs from the creating task claim")
        if require_disposable and not claim.disposable:
            raise KagglePolicyError("permanent task-created resources cannot be cleaned up")
        if claim.control_class not in _CONTROLLED_CLASSES:
            raise KagglePolicyError("external_read_only resources cannot be mutated")

    def _dataset_result(
        self,
        intent: ProviderEffectIntent,
        identity: KaggleDatasetIdentity,
        control_class: ControlClass,
        disposable: bool,
        attempts: int,
        outcome: EffectOutcome,
    ) -> DatasetMutationResult:
        receipt = ProviderEffectReceipt(
            operation_id=intent.operation_id,
            effect_id=intent.effect_id,
            action=intent.action,
            provider_ref=intent.provider_ref,
            outcome=outcome,
            attempts=attempts,
            observed_fingerprint=identity.fingerprint,
            provider_version=identity.version,
            observed_at=self.clock(),
            detail_code="private_dataset_exact_readback",
        )
        claim = TaskResourceClaim.create(
            task_id=intent.task_id,
            effect_id=intent.effect_id,
            provider_ref=intent.provider_ref,
            kind=ProviderKind.DATASET,
            control_class=control_class,
            disposable=disposable,
            fingerprint=identity.fingerprint,
            provider_version=identity.version,
            registered_at=intent.requested_at,
        )
        self.journal.persist_receipt(receipt)
        self.journal.persist_resource_claim(claim)
        return DatasetMutationResult(identity=identity, claim=claim, effect=receipt)

    def _persist_uncertain(self, intent: ProviderEffectIntent, *, detail: str) -> None:
        self.journal.persist_receipt(
            ProviderEffectReceipt(
                operation_id=intent.operation_id,
                effect_id=intent.effect_id,
                action=intent.action,
                provider_ref=intent.provider_ref,
                outcome=EffectOutcome.UNCERTAIN,
                attempts=0,
                observed_at=self.clock(),
                detail_code=detail,
            )
        )
