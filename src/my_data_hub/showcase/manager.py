from __future__ import annotations

import os
import secrets
import threading
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from .builder import AstroShowcaseBuilder
from .models import RegistryState, SurfaceState, ShowcaseItem, ShowcaseView
from .publisher import CommandPublisher, LocalDirectoryPublisher, ShowcasePublisher
from .source import FilesystemShowcaseSource, GitHubShowcaseSource, GitHubShowcaseWriter, GitSshShowcaseSource, ShowcaseSource, ShowcaseSourceError, ShowcaseSourceNotFoundError
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
        writer: GitHubShowcaseWriter | None = None,
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
        if write_token and not source_root:
            writer = GitHubShowcaseWriter(token=write_token, repository=repository, ref=ref, root=root)
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
        view: ShowcaseView | None,
        items: list[ShowcaseItem],
        dry_run: bool = True,
        publish: bool = False,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        """Validate, CAS-write, and optionally publish a bounded Showcase bundle."""
        if not expected_source_revision:
            raise ShowcaseSourceError("expected_source_revision is required")
        if isinstance(view, dict):
            view = ShowcaseView.model_validate(view)
        items = [ShowcaseItem.model_validate(item) if isinstance(item, dict) else item for item in items]
        creating = expected_source_revision == "absent"
        try:
            current = self.source.get_source(view_id)
        except ShowcaseSourceNotFoundError:
            if not creating:
                raise
            current = None
        if creating and current is not None:
            raise ShowcaseSourceError("showcase view already exists")
        if not creating and current is None:
            raise ShowcaseSourceError("showcase source is absent")
        if not creating and current.source_revision != expected_source_revision:
            raise ShowcaseSourceError("source revision conflict")
        if view is not None and view.id != view_id:
            raise ShowcaseSourceError("view id mismatch")
        if creating and view is None:
            raise ShowcaseSourceError("CAS-create requires a complete view")
        proposed_view = view if view is not None else current.view
        by_id = {} if creating else {item.id: item for item in current.items}
        for item in items:
            by_id[item.id] = item
        missing = [item_id for item_id in proposed_view.item_ids if item_id not in by_id]
        if missing:
            raise ShowcaseSourceError("view references items not included in the proposed bundle")
        from .models import ShowcaseBundle
        bundle = ShowcaseBundle(
            source_revision=("create-preview" if creating else current.source_revision),
            view=proposed_view,
            items=[by_id[item_id] for item_id in proposed_view.item_ids],
        )
        if creating and any(item.capability_type is None for item in bundle.items):
            raise ShowcaseSourceError("new partner view items require capability_type")
        changed: dict[str, str] = {}
        if view is not None:
            changed[f"views/{view_id}.yaml"] = __import__("yaml").safe_dump(view.model_dump(mode="json", exclude_none=True), allow_unicode=True, sort_keys=False)
        for item in items:
            changed[f"items/{item.id}.yaml"] = __import__("yaml").safe_dump(item.model_dump(mode="json", exclude_none=True), allow_unicode=True, sort_keys=False)
        result: dict[str, Any] = {
            "schema_version": 1,
            "status": "dry_run" if dry_run else "applied",
            "previous_source_revision": "absent" if creating else current.source_revision,
            "new_source_revision": "absent" if dry_run and creating else (current.source_revision if dry_run else None),
            "changed_paths": sorted(changed),
            "view_count": 1,
            "item_count": len(bundle.items),
            "validation": {"valid": True, "buildable": (getattr(self.builder, "site_root", Path("/nonexistent")) / "package.json").is_file()},
        }
        if dry_run:
            return result
        if self.writer is None:
            raise ShowcaseSourceError("Showcase source write credential is unavailable")
        if creating:
            create = getattr(self.writer, "create", None)
            if not callable(create):
                raise ShowcaseSourceError("Showcase source writer does not support CAS-create")
            new_revision = create(view_id=view_id, files=changed, message=f"showcase: create {view_id}")
        else:
            new_revision = self.writer.apply(expected_revision=current.source_revision, files=changed, message=f"showcase: update {view_id}")
        readback = self.source.get_source(view_id)
        if readback.source_revision != new_revision:
            raise ShowcaseSourceError("source readback did not resolve the committed revision")
        result["new_source_revision"] = new_revision
        if publish:
            try:
                published = self.rebuild(view_id, idempotency_key=idempotency_key)
            except Exception:
                result.update({"status": "applied_not_published", "publish_failure": "publication failed; retry showcase.rebuild"})
                return result
            result.update({"status": "published", "publish": published})
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

    def rebuild(self, view_id: str, *, idempotency_key: str | None = None) -> dict[str, Any]:
        with self._lock:
            bundle = self.source.load_bundle(view_id)
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
        publish: bool = True,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
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
