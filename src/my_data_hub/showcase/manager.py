from __future__ import annotations

import inspect
import os
import secrets
import threading
from contextlib import nullcontext, suppress
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

import yaml

from .builder import AstroShowcaseBuilder
from .models import RegistryState, ShowcaseBundle, ShowcaseItem, ShowcaseView, SurfaceState
from .publisher import CommandPublisher, LocalDirectoryPublisher, ShowcasePublisher
from .requests import ShowcaseMode, ShowcaseRequestError, resolve_mode
from .source import (
    FilesystemShowcaseSource,
    GitHubShowcaseSource,
    GitHubShowcaseWriter,
    GitSshShowcaseSource,
    GitSshShowcaseWriter,
    ShowcaseSource,
    ShowcaseSourceError,
    ShowcaseSourceNotFoundError,
)
from .state import ShowcaseStateStore


class ShowcaseNotFoundError(KeyError):
    """Raised when a requested showcase surface has not been registered."""


class ShowcaseManager:
    def __init__(
        self,
        *,
        source: ShowcaseSource,
        state: ShowcaseStateStore,
        builder: AstroShowcaseBuilder,
        publisher: ShowcasePublisher,
        origin: str,
        writer: GitHubShowcaseWriter | GitSshShowcaseWriter | None = None,
    ) -> None:
        self.source = source
        self.state = state
        self.builder = builder
        self.publisher = publisher
        self.origin = origin.rstrip("/")
        self.writer = writer
        self._lock = threading.RLock()

    @classmethod
    def from_env(cls) -> ShowcaseManager:
        artifact_root = Path(os.getenv("MY_DATA_HUB_ARTIFACT_ROOT", "./artifacts")).expanduser().resolve()
        origin = os.getenv("MY_DATA_HUB_SHOWCASE_ORIGIN", "https://ideas.kenigevents.ru").rstrip("/")
        source_root = os.getenv("MY_DATA_HUB_SHOWCASE_SOURCE_ROOT", "").strip()
        if source_root:
            source: ShowcaseSource = FilesystemShowcaseSource(Path(source_root))
        else:
            repository = os.getenv(
                "MY_DATA_HUB_SHOWCASE_GITHUB_REPOSITORY",
                "onedayonemasterpiece/idea-hub",
            )
            ref = os.getenv("MY_DATA_HUB_SHOWCASE_GITHUB_REF", "main")
            root = os.getenv("MY_DATA_HUB_SHOWCASE_GITHUB_ROOT", "showcase")
            ssh_key_file = os.getenv("MY_DATA_HUB_SHOWCASE_GITHUB_SSH_KEY_FILE", "").strip()
            if ssh_key_file:
                known_hosts_file = os.getenv(
                    "MY_DATA_HUB_SHOWCASE_GITHUB_KNOWN_HOSTS_FILE",
                    "/etc/ssh/ssh_known_hosts",
                ).strip()
                source = GitSshShowcaseSource(
                    key_file=Path(ssh_key_file),
                    known_hosts_file=Path(known_hosts_file),
                    repository=repository,
                    ref=ref,
                    root=root,
                )
            else:
                source = GitHubShowcaseSource(
                    token=os.getenv("MY_DATA_HUB_SHOWCASE_GITHUB_TOKEN", "").strip(),
                    repository=repository,
                    ref=ref,
                    root=root,
                )
        repository_root = Path(__file__).resolve().parents[3]
        source_site_root = repository_root / "showcase-site"
        packaged_site_root = Path(__file__).resolve().parents[1] / "showcase_site"
        default_site_root = source_site_root if source_site_root.is_dir() else packaged_site_root
        site_root = Path(os.getenv("MY_DATA_HUB_SHOWCASE_SITE_ROOT", str(default_site_root)))
        builder = AstroShowcaseBuilder(
            site_root=site_root,
            origin=origin,
            npm_command=os.getenv("MY_DATA_HUB_SHOWCASE_NPM_COMMAND", "npm"),
            timeout_seconds=int(os.getenv("MY_DATA_HUB_SHOWCASE_BUILD_TIMEOUT_SECONDS", "180")),
        )
        if os.getenv("MY_DATA_HUB_SHOWCASE_PUBLISH_COMMAND_JSON"):
            publisher: ShowcasePublisher = CommandPublisher.from_json_env(origin=origin)
        else:
            publisher = LocalDirectoryPublisher(
                root=Path(
                    os.getenv(
                        "MY_DATA_HUB_SHOWCASE_LOCAL_PUBLISH_ROOT",
                        str(artifact_root / "showcase" / "published"),
                    )
                ),
                origin=origin,
            )
        writer = None
        write_token = os.getenv("MY_DATA_HUB_SHOWCASE_GITHUB_WRITE_TOKEN", "").strip()
        write_ssh_key_file = os.getenv("MY_DATA_HUB_SHOWCASE_GITHUB_WRITE_SSH_KEY_FILE", "").strip()
        if write_token and not source_root:
            writer = GitHubShowcaseWriter(token=write_token, repository=repository, ref=ref, root=root)
        elif write_ssh_key_file and not source_root:
            writer = GitSshShowcaseWriter(
                key_file=Path(write_ssh_key_file),
                known_hosts_file=Path(known_hosts_file),
                repository=repository,
                ref=ref,
                root=root,
            )
        return cls(
            source=source,
            writer=writer,
            state=ShowcaseStateStore(
                Path(
                    os.getenv(
                        "MY_DATA_HUB_SHOWCASE_STATE_PATH",
                        str(artifact_root / "showcase" / "state.json"),
                    )
                )
            ),
            builder=builder,
            publisher=publisher,
            origin=origin,
        )

    @staticmethod
    def _new_slug() -> str:
        return secrets.token_urlsafe(24)

    def _url(self, slug: str) -> str:
        return f"{self.origin}/v/{slug}/"

    @staticmethod
    def _surface_payload(surface: SurfaceState) -> dict[str, Any]:
        return {
            "view_id": surface.view_id,
            "active": surface.active,
            "url": None if not surface.active else surface.last_build.url if surface.last_build else None,
            "created_at": surface.created_at.isoformat(),
            "updated_at": surface.updated_at.isoformat(),
            "last_build": surface.last_build.model_dump(mode="json") if surface.last_build else None,
        }

    def get_source(self, view_id: str) -> dict[str, Any]:
        bundle = self.source.get_source(view_id)
        return {
            "schema_version": 1,
            "source_revision": bundle.source_revision,
            "view": bundle.view.model_dump(mode="json"),
            "items": [item.model_dump(mode="json") for item in bundle.items],
        }

    def apply(
        self,
        view_id: str,
        *,
        expected_source_revision: str,
        view: ShowcaseView | None = None,
        items: list[ShowcaseItem] | None = None,
        dry_run: bool | None = None,
        publish: bool | None = None,
        mode: ShowcaseMode | None = None,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        """CAS-update or legacy CAS-create. New clients create via create_view."""
        selected = resolve_mode(mode, dry_run, publish)
        if not expected_source_revision:
            raise ShowcaseRequestError("REVISION_REQUIRED", "expected_source_revision")
        creating = expected_source_revision == "absent"
        snapshot_method = getattr(self.source, "snapshot", None)
        context = snapshot_method() if callable(snapshot_method) else nullcontext(None)
        with self._lock, context as snapshot:
            if snapshot is not None and creating and snapshot.view_exists(view_id):
                raise ShowcaseRequestError("VIEW_EXISTS", "view_id")
            try:
                current = snapshot.get_source(view_id) if snapshot is not None else self.source.get_source(view_id)
            except ShowcaseSourceNotFoundError:
                current = None
            if creating and current is not None:
                raise ShowcaseRequestError("VIEW_EXISTS", "view_id")
            if not creating and current is None:
                raise ShowcaseRequestError("VIEW_NOT_FOUND", "view_id")
            if current is not None and current.source_revision != expected_source_revision:
                raise ShowcaseRequestError("REVISION_CONFLICT", "expected_source_revision")
            if creating and view is None:
                raise ShowcaseRequestError("VIEW_REQUIRED", "view")
            if view is not None:
                raw_view = view.model_dump(mode="json") if hasattr(view, "model_dump") else dict(view)
                if raw_view.get("id") not in {None, view_id}:
                    raise ShowcaseRequestError("VIEW_ID_MISMATCH", "view.id")
                raw_view["id"] = view_id
                proposed_view = ShowcaseView.model_validate(raw_view)
            else:
                proposed_view = current.view
            definitions = [
                ShowcaseItem.model_validate(item.model_dump() if hasattr(item, "model_dump") else item)
                for item in (items or [])
            ]
            if len({item.id for item in definitions}) != len(definitions):
                raise ShowcaseRequestError("DUPLICATE_ITEM", "items")
            by_id = {} if creating else {item.id: item for item in current.items}
            changed: dict[str, str] = {}
            for index, item in enumerate(definitions):
                if item.id not in proposed_view.item_ids:
                    raise ShowcaseRequestError("UNREFERENCED_ITEM", f"items[{index}].id")
                old = by_id.get(item.id)
                if snapshot is not None:
                    try:
                        old = snapshot.get_item(item.id)
                    except ShowcaseSourceNotFoundError:
                        old = None
                if old != item:
                    if old is not None and creating:
                        raise ShowcaseRequestError("ITEM_ID_CONFLICT", f"items[{index}].id")
                    if old is not None and snapshot is not None and any(v != view_id for v in snapshot.users(item.id)):
                        raise ShowcaseRequestError("SHARED_ITEM", f"items[{index}].id")
                    if item.capability_type is None:
                        raise ShowcaseRequestError("CAPABILITY_TYPE_REQUIRED", f"items[{index}].capability_type")
                    changed[f"items/{item.id}.yaml"] = yaml.safe_dump(
                        item.model_dump(mode="json", exclude_none=True),
                        allow_unicode=True,
                        sort_keys=False,
                    )
                by_id[item.id] = item
            for index, item_id in enumerate(proposed_view.item_ids):
                if item_id not in by_id and snapshot is not None:
                    with suppress(ShowcaseSourceNotFoundError):
                        by_id[item_id] = snapshot.get_item(item_id)
                if item_id not in by_id:
                    raise ShowcaseRequestError("ITEM_NOT_FOUND", f"view.item_ids[{index}]")
            ordered = [by_id[item_id] for item_id in proposed_view.item_ids]
            bundle = ShowcaseBundle.model_validate(
                {
                    "source_revision": "create-preview" if creating else current.source_revision,
                    "view": proposed_view,
                    "items": ordered,
                },
                context={"allow_drafts": True},
            )
            publication_errors = []
            for index, item in enumerate(ordered):
                if item.publish_state != "ready":
                    publication_errors.append(ShowcaseRequestError("ITEM_NOT_READY", f"view.item_ids[{index}]"))
                if proposed_view.visibility_ceiling == "public" and item.visibility == "partner":
                    publication_errors.append(ShowcaseRequestError("VISIBILITY_EXCEEDED", f"view.item_ids[{index}]"))
            if selected == "publish" and publication_errors:
                raise publication_errors[0]
            if creating or proposed_view != current.view:
                changed[f"views/{view_id}.yaml"] = yaml.safe_dump(
                    proposed_view.model_dump(mode="json", exclude_none=True),
                    allow_unicode=True,
                    sort_keys=False,
                )
            result: dict[str, Any] = {
                "schema_version": 1,
                "view_id": view_id,
                "mode": selected,
                "status": "dry_run" if selected == "preview" else "applied",
                "previous_source_revision": "absent" if creating else current.source_revision,
                "new_source_revision": "absent" if creating else current.source_revision,
                "changed_paths": sorted(changed),
                "view_count": 1,
                "item_count": len(bundle.items),
                "validation": {
                    "valid": True,
                    "publication_ready": not publication_errors,
                    "buildable": None,
                    "build_checked": False,
                    "errors": [error.payload() for error in publication_errors],
                },
                "warnings": (
                    ["Reused legacy cards without capability_type; no global cards were changed."]
                    if any(item.capability_type is None for item in ordered)
                    else []
                ),
            }
            if selected == "preview":
                return result
            new_revision = current.source_revision if current is not None else None
            if changed:
                if self.writer is None:
                    raise ShowcaseRequestError("WRITE_UNAVAILABLE")
                if creating:
                    create = getattr(self.writer, "create", None)
                    if not callable(create):
                        raise ShowcaseRequestError("WRITE_UNAVAILABLE")
                    kwargs = {"view_id": view_id, "files": changed, "message": f"showcase: create {view_id}"}
                    if snapshot is not None and "expected_revision" in inspect.signature(create).parameters:
                        kwargs["expected_revision"] = snapshot.revision
                    new_revision = create(**kwargs)
                else:
                    new_revision = self.writer.apply(
                        expected_revision=current.source_revision,
                        files=changed,
                        message=f"showcase: update {view_id}",
                    )
            result["new_source_revision"] = new_revision
        # The immutable source snapshot is no longer needed while building.
        try:
            kwargs = (
                {"revision": new_revision} if "revision" in inspect.signature(self.source.get_source).parameters else {}
            )
            readback = self.source.get_source(view_id, **kwargs)
            if readback.source_revision != new_revision or readback.view != proposed_view or readback.items != ordered:
                raise ShowcaseSourceError("exact source readback mismatch")
        except Exception:
            result.update(status="applied_not_verified", error=ShowcaseRequestError("READBACK_FAILED").payload())
            return result
        if selected == "publish":
            try:
                published = self.rebuild(
                    view_id, idempotency_key=idempotency_key, expected_source_revision=new_revision
                )
            except Exception:
                result.update(
                    status="applied_not_published",
                    error=ShowcaseRequestError("PUBLICATION_FAILED").payload(),
                    publish_failure="publication failed; retry showcase.rebuild",
                )
                return result
            result.update(status="published", publish=published, url=published["url"])
            result["validation"].update(buildable=True, build_checked=True)
        return result

    def list_surfaces(self) -> dict[str, Any]:
        state = self.state.load()
        surfaces = []
        for surface in sorted(state.surfaces.values(), key=lambda item: item.view_id):
            payload = self._surface_payload(surface)
            if surface.active and payload["url"] is None:
                payload["url"] = self._url(surface.slug)
                payload["published"] = False
            else:
                payload["published"] = surface.last_build is not None
            surfaces.append(payload)
        return {"schema_version": 1, "surfaces": surfaces}

    def get_link(self, view_id: str) -> dict[str, Any]:
        state = self.state.load()
        surface = state.surfaces.get(view_id)
        if surface is None:
            raise ShowcaseNotFoundError(view_id)
        return {
            "schema_version": 1,
            "view_id": view_id,
            "active": surface.active,
            "url": self._url(surface.slug) if surface.active else None,
            "last_build": surface.last_build.model_dump(mode="json") if surface.last_build else None,
        }

    def _ensure_surface(self, state: RegistryState, view_id: str) -> SurfaceState:
        surface = state.surfaces.get(view_id)
        if surface is None:
            surface = SurfaceState(view_id=view_id, slug=self._new_slug())
            state.surfaces[view_id] = surface
        elif not surface.active:
            raise RuntimeError(f"showcase surface {view_id} is revoked; rotate or create it explicitly")
        return surface

    def rebuild(
        self, view_id: str, *, idempotency_key: str | None = None, expected_source_revision: str | None = None
    ) -> dict[str, Any]:
        with self._lock:
            kwargs = (
                {"revision": expected_source_revision}
                if expected_source_revision is not None
                and "revision" in inspect.signature(self.source.load_bundle).parameters
                else {}
            )
            bundle = self.source.load_bundle(view_id, **kwargs)
            if expected_source_revision is not None and bundle.source_revision != expected_source_revision:
                raise ShowcaseRequestError("REVISION_CONFLICT")
            with self.state.transaction() as state:
                surface = self._ensure_surface(state, view_id)
                slug = surface.slug
            with TemporaryDirectory(prefix=f"showcase-build-{view_id}-") as temp:
                output = Path(temp) / "dist"
                receipt = self.builder.build(bundle, slug=slug, output_dir=output)
                published_url = self.publisher.publish(output, receipt)
            receipt = receipt.model_copy(update={"url": published_url})
            with self.state.transaction() as state:
                surface = self._ensure_surface(state, view_id)
                if surface.slug != slug:
                    raise RuntimeError("showcase slug changed during build")
                now = datetime.now(UTC)
                state.surfaces[view_id] = surface.model_copy(update={"updated_at": now, "last_build": receipt})
            return {
                "schema_version": 1,
                "status": "published",
                "view_id": view_id,
                "url": published_url,
                "receipt": receipt.model_dump(mode="json"),
            }

    def create_view(
        self,
        view_id: str,
        *,
        publish: bool | None = None,
        idempotency_key: str | None = None,
        view: ShowcaseView | None = None,
        items: list[ShowcaseItem] | None = None,
        mode: ShowcaseMode | None = None,
        dry_run: bool | None = None,
    ) -> dict[str, Any]:
        if view is not None:
            selected = resolve_mode(mode, dry_run, publish)
            return self.apply(
                view_id,
                expected_source_revision="absent",
                view=view,
                items=items or [],
                mode=selected,
                idempotency_key=idempotency_key,
            )
        if mode is not None or dry_run is not None or items:
            raise ShowcaseRequestError("VIEW_REQUIRED", "view")
        publish = True if publish is None else publish
        with self._lock:
            self.source.load_bundle(view_id)
            with self.state.transaction() as state:
                existing = state.surfaces.get(view_id)
                if existing and existing.active:
                    created = False
                else:
                    state.surfaces[view_id] = SurfaceState(view_id=view_id, slug=self._new_slug())
                    created = True
            if publish:
                result = self.rebuild(view_id, idempotency_key=idempotency_key)
                result["created"] = created
                return result
            link = self.get_link(view_id)
            return {"schema_version": 1, "status": "created", "created": created, **link}

    def rotate_link(
        self,
        view_id: str,
        *,
        slug: str | None = None,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        with self._lock:
            bundle = self.source.load_bundle(view_id)
            previous_state = self.state.load()
            previous = previous_state.surfaces.get(view_id)
            if previous is None:
                raise ShowcaseNotFoundError(view_id)
            old_slug = previous.slug
            new_slug = slug or self._new_slug()
            with TemporaryDirectory(prefix=f"showcase-rotate-{view_id}-") as temp:
                output = Path(temp) / "dist"
                receipt = self.builder.build(bundle, slug=new_slug, output_dir=output)
                published_url = self.publisher.publish(output, receipt)
            receipt = receipt.model_copy(update={"url": published_url})
            now = datetime.now(UTC)
            current = self.state.load()
            current_surface = current.surfaces.get(view_id)
            if current_surface is None or current_surface.slug != old_slug:
                raise RuntimeError("showcase state changed during link rotation")
            current.surfaces[view_id] = SurfaceState(
                view_id=view_id,
                slug=new_slug,
                active=True,
                created_at=current_surface.created_at,
                updated_at=now,
                last_build=receipt,
            )
            self.state.save(current)
            self.publisher.revoke(view_id=view_id, slug=old_slug)
            return {
                "schema_version": 1,
                "status": "rotated",
                "view_id": view_id,
                "url": published_url,
                "old_url_revoked": self._url(old_slug),
                "receipt": receipt.model_dump(mode="json"),
            }

    def revoke_link(self, view_id: str, *, idempotency_key: str | None = None) -> dict[str, Any]:
        with self._lock:
            with self.state.transaction() as state:
                surface = state.surfaces.get(view_id)
                if surface is None:
                    raise ShowcaseNotFoundError(view_id)
                slug = surface.slug
                self.publisher.revoke(view_id=view_id, slug=slug)
                state.surfaces[view_id] = surface.model_copy(update={"active": False, "updated_at": datetime.now(UTC)})
            return {
                "schema_version": 1,
                "status": "revoked",
                "view_id": view_id,
                "revoked_url": self._url(slug),
            }
