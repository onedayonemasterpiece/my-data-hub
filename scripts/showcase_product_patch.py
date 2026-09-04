"""One-shot source editor for this branch; removed by the materialization job."""
from pathlib import Path
import ast
import textwrap

ROOT = Path('src/my_data_hub/showcase')

def replace(path, old, new, count=1):
    text = path.read_text()
    assert text.count(old) == count, (path, old[:100], text.count(old))
    path.write_text(text.replace(old, new))

def method(path, owner, name, new):
    text = path.read_text()
    tree = ast.parse(text)
    parents = [n for n in ast.walk(tree) if isinstance(n, ast.ClassDef) and n.name == owner] if owner else [tree]
    assert len(parents) == 1, (path, owner)
    nodes = [n for n in parents[0].body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name == name]
    assert len(nodes) == 1, (path, owner, name)
    node = nodes[0]
    lines = text.splitlines(keepends=True)
    start = min([node.lineno] + [d.lineno for d in node.decorator_list]) - 1
    lines[start:node.end_lineno] = [textwrap.indent(textwrap.dedent(new).strip() + '\n', ' ' * node.col_offset)]
    path.write_text(''.join(lines))

source = ROOT / 'source.py'
replace(source, 'from typing import Protocol', 'from typing import Protocol\nfrom collections.abc import Callable, Iterator\nfrom contextlib import contextmanager\nimport re')
replace(source, 'from .models import ShowcaseBundle, ShowcaseItem, ShowcaseView', 'from .models import ShowcaseBundle, ShowcaseItem, ShowcaseView, _ID_PATTERN\nfrom .requests import ShowcaseSourceError, ShowcaseRequestError')
replace(source, 'class ShowcaseSourceError(RuntimeError):\n    """Raised when a showcase source cannot produce one consistent snapshot."""\n\n\n', '')
replace(source, 'class FilesystemShowcaseSource:', '''class ShowcaseSnapshot:
    """One immutable revision; lazily read only requested curated records."""

    def __init__(self, revision: str, read: Callable[[str], str], views: Callable[[], list[str]]) -> None:
        self.revision = revision
        self._read_impl = read
        self._views = views
        self._cache: dict[str, str] = {}
        self._users: dict[str, list[str]] | None = None

    def read(self, path: str) -> str:
        if path not in self._cache:
            self._cache[path] = self._read_impl(path)
        return self._cache[path]

    def view_exists(self, view_id: str) -> bool:
        self._id(view_id)
        try:
            self.read(f"views/{view_id}.yaml")
        except ShowcaseSourceNotFoundError:
            return False
        return True

    @staticmethod
    def _id(value: str) -> None:
        if not re.fullmatch(_ID_PATTERN, value):
            raise ShowcaseRequestError("INVALID_FIELD", "view_id")

    def get_item(self, item_id: str) -> ShowcaseItem:
        self._id(item_id)
        path = f"items/{item_id}.yaml"
        item = ShowcaseItem.model_validate(_parse_yaml(self.read(path), label=path))
        if item.id != item_id:
            raise ShowcaseRequestError("INVALID_FIELD", "items.id")
        return item

    def get_source(self, view_id: str, *, allow_drafts: bool = True) -> ShowcaseBundle:
        self._id(view_id)
        path = f"views/{view_id}.yaml"
        view = ShowcaseView.model_validate(_parse_yaml(self.read(path), label=path))
        if view.id != view_id:
            raise ShowcaseRequestError("VIEW_ID_MISMATCH", "view.id")
        items = []
        for index, item_id in enumerate(view.item_ids):
            try:
                items.append(self.get_item(item_id))
            except ShowcaseSourceNotFoundError as exc:
                raise ShowcaseRequestError("ITEM_NOT_FOUND", f"view.item_ids[{index}]") from exc
        return ShowcaseBundle.model_validate(
            {"source_revision": self.revision, "view": view, "items": items},
            context={"allow_drafts": allow_drafts},
        )

    def users(self, item_id: str) -> list[str]:
        if self._users is None:
            self._users = {}
            for path in self._views():
                doc = _parse_yaml(self.read(path), label=path)
                for ref in doc.get("item_ids", []):
                    self._users.setdefault(ref, []).append(Path(path).stem)
        return self._users.get(item_id, [])


class FilesystemShowcaseSource:''')
for owner in ['FilesystemShowcaseSource', 'GitHubShowcaseSource', 'GitSshShowcaseSource']:
    method(source, owner, 'get_source', '''
    def get_source(self, view_id: str, *, revision: str | None = None) -> ShowcaseBundle:
        with self.snapshot(revision) as snapshot:
            return snapshot.get_source(view_id, allow_drafts=True)
    ''')
# Preserve the existing SSH checkout implementation while making its lifetime explicit.
text = source.read_text()
cls = next(n for n in ast.walk(ast.parse(text)) if isinstance(n, ast.ClassDef) and n.name == 'GitSshShowcaseSource')
node = next(n for n in cls.body if isinstance(n, ast.FunctionDef) and n.name == 'load_bundle')
old = textwrap.dedent(''.join(text.splitlines(keepends=True)[node.lineno-1:node.end_lineno]))
new = old.replace('def load_bundle(self, view_id: str) -> ShowcaseBundle:', '@contextmanager\ndef snapshot(self, at_revision: str | None = None) -> Iterator[ShowcaseSnapshot]:')
new = new.replace('"origin", self.ref]', '"origin", at_revision or self.ref]')
new = new.replace('    bundle = FilesystemShowcaseSource(checkout / self.root).load_bundle(view_id)\n    return bundle.model_copy(update={"source_revision": revision})', '    with FilesystemShowcaseSource(checkout / self.root).snapshot() as snapshot:\n        snapshot.revision = revision\n        yield snapshot')
assert new != old and 'yield snapshot' in new
method(source, 'GitSshShowcaseSource', 'load_bundle', new)
# Add the snapshot protocol to each concrete reader; no new source or database.
for owner, snapshot_code in {
    'FilesystemShowcaseSource': '''
    @contextmanager
    def snapshot(self, at_revision: str | None = None) -> Iterator[ShowcaseSnapshot]:
        files: dict[str, str] = {}
        for directory in ("views", "items"):
            for path in sorted((self.root / directory).glob("*.yaml")):
                if path.is_symlink():
                    raise ShowcaseSourceError("unsafe Showcase symlink")
                relative = f"{directory}/{path.name}"
                files[relative] = self._read(relative)
        digest = hashlib.sha256()
        for relative, raw in sorted(files.items()):
            digest.update(relative.encode() + b"\\0" + raw.encode() + b"\\n")
        revision = digest.hexdigest()
        if at_revision is not None and revision != at_revision:
            raise ShowcaseRequestError("REVISION_CONFLICT")
        def read(relative: str) -> str:
            try:
                return files[relative]
            except KeyError as exc:
                raise ShowcaseSourceNotFoundError("showcase source is absent") from exc
        yield ShowcaseSnapshot(revision, read, lambda: [p for p in files if p.startswith("views/")])
    ''',
    'GitHubShowcaseSource': '''
    @contextmanager
    def snapshot(self, at_revision: str | None = None) -> Iterator[ShowcaseSnapshot]:
        revision = at_revision or self._revision()
        if not re.fullmatch(r"[0-9a-f]{40}", revision):
            raise ShowcaseRequestError("REVISION_CONFLICT")
        def views() -> list[str]:
            commit = self._request_json(f"https://api.github.com/repos/{self.repository}/git/commits/{revision}")
            tree_sha = commit["tree"]["sha"]
            tree = self._request_json(f"https://api.github.com/repos/{self.repository}/git/trees/{tree_sha}?recursive=1")
            if tree.get("truncated"):
                raise ShowcaseSourceError("cannot verify shared cards from a truncated tree")
            prefix = f"{self.root}/views/"
            return [entry["path"][len(self.root) + 1:] for entry in tree["tree"]
                    if entry.get("type") == "blob" and entry["path"].startswith(prefix)
                    and entry["path"].endswith(".yaml")]
        yield ShowcaseSnapshot(revision, lambda path: self._read_at_revision(path, revision), views)
    ''',
}.items():
    text = source.read_text()
    node = next(n for n in ast.walk(ast.parse(text)) if isinstance(n, ast.ClassDef) and n.name == owner)
    lines = text.splitlines(keepends=True)
    lines[node.end_lineno:node.end_lineno] = ['\n' + textwrap.indent(textwrap.dedent(snapshot_code).strip() + '\n', '    ')]
    source.write_text(''.join(lines))
for owner in ['FilesystemShowcaseSource', 'GitHubShowcaseSource']:
    method(source, owner, 'load_bundle', '''
    def load_bundle(self, view_id: str, *, revision: str | None = None) -> ShowcaseBundle:
        with self.snapshot(revision) as snapshot:
            return snapshot.get_source(view_id, allow_drafts=False)
    ''')
text = source.read_text()
node = next(n for n in ast.walk(ast.parse(text)) if isinstance(n, ast.ClassDef) and n.name == 'GitSshShowcaseSource')
lines = text.splitlines(keepends=True)
lines[node.end_lineno:node.end_lineno] = ['\n    def load_bundle(self, view_id: str, *, revision: str | None = None) -> ShowcaseBundle:\n        with self.snapshot(revision) as snapshot:\n            return snapshot.get_source(view_id, allow_drafts=False)\n']
source.write_text(''.join(lines))
replace(source, 'def create(self, *, view_id: str, files: dict[str, str], message: str) -> str:', 'def create(self, *, view_id: str, files: dict[str, str], message: str, expected_revision: str | None = None) -> str:', 2)
replace(source, '        commit = self._request(f"{base}/commits/{current}")', '        if expected_revision is not None and current != expected_revision:\n            raise ShowcaseRequestError("REVISION_CONFLICT")\n        commit = self._request(f"{base}/commits/{current}")')
replace(source, '        if target in paths:\n            raise ShowcaseSourceError("showcase view already exists")', '        if listing.get("truncated"):\n            raise ShowcaseSourceError("cannot verify create paths from a truncated tree")\n        if target in paths:\n            raise ShowcaseRequestError("VIEW_EXISTS")\n        if any(f"{self.root}/{path}" in paths for path in files if path.startswith("items/")):\n            raise ShowcaseRequestError("ITEM_ID_CONFLICT", "items")')
replace(source, '                if target.exists() or target.is_symlink():\n                    raise ShowcaseSourceError("showcase view already exists")', '                if target.exists() or target.is_symlink():\n                    raise ShowcaseRequestError("VIEW_EXISTS")\n                if any((root / p).exists() or (root / p).is_symlink() for p in safe if p.startswith("items/")):\n                    raise ShowcaseRequestError("ITEM_ID_CONFLICT", "items")')
replace(source, '        return self._commit(expected_revision=head, files=files, message=message, create_view_id=view_id)', '        if expected_revision is not None and head != expected_revision:\n            raise ShowcaseRequestError("REVISION_CONFLICT")\n        return self._commit(expected_revision=head, files=files, message=message, create_view_id=view_id)')
replace(source, 'raise ShowcaseSourceError("source revision conflict")', 'raise ShowcaseRequestError("REVISION_CONFLICT")', 3)

manager = ROOT / 'manager.py'
replace(manager, 'import os\n', 'import os\nimport inspect\nfrom contextlib import nullcontext\n\nimport yaml\n')
replace(manager, 'from .state import ShowcaseStateStore', 'from .state import ShowcaseStateStore\nfrom .models import ShowcaseBundle\nfrom .requests import ShowcaseMode, ShowcaseRequestError, resolve_mode')
method(manager, 'ShowcaseManager', 'apply', '''
def apply(
    self, view_id: str, *, expected_source_revision: str, view: ShowcaseView | None = None,
    items: list[ShowcaseItem] | None = None, dry_run: bool | None = None,
    publish: bool | None = None, mode: ShowcaseMode | None = None, idempotency_key: str | None = None,
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
        definitions = [ShowcaseItem.model_validate(item) if isinstance(item, dict) else item for item in (items or [])]
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
                    item.model_dump(mode="json", exclude_none=True), allow_unicode=True, sort_keys=False,
                )
            by_id[item.id] = item
        for index, item_id in enumerate(proposed_view.item_ids):
            if item_id not in by_id and snapshot is not None:
                try:
                    by_id[item_id] = snapshot.get_item(item_id)
                except ShowcaseSourceNotFoundError:
                    pass
            if item_id not in by_id:
                raise ShowcaseRequestError("ITEM_NOT_FOUND", f"view.item_ids[{index}]")
        ordered = [by_id[item_id] for item_id in proposed_view.item_ids]
        bundle = ShowcaseBundle.model_validate(
            {"source_revision": "create-preview" if creating else current.source_revision,
             "view": proposed_view, "items": ordered}, context={"allow_drafts": True},
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
                proposed_view.model_dump(mode="json", exclude_none=True), allow_unicode=True, sort_keys=False,
            )
        result: dict[str, Any] = {
            "schema_version": 1, "view_id": view_id, "mode": selected,
            "status": "dry_run" if selected == "preview" else "applied",
            "previous_source_revision": "absent" if creating else current.source_revision,
            "new_source_revision": "absent" if creating else current.source_revision,
            "changed_paths": sorted(changed), "view_count": 1, "item_count": len(bundle.items),
            "validation": {"valid": True, "publication_ready": not publication_errors,
                           "buildable": None, "build_checked": False,
                           "errors": [error.payload() for error in publication_errors]},
            "warnings": (["Reused legacy cards without capability_type; no global cards were changed."]
                         if any(item.capability_type is None for item in ordered) else []),
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
                    expected_revision=current.source_revision, files=changed, message=f"showcase: update {view_id}",
                )
        result["new_source_revision"] = new_revision
    # The immutable source snapshot is no longer needed while building.
    try:
        kwargs = {"revision": new_revision} if "revision" in inspect.signature(self.source.get_source).parameters else {}
        readback = self.source.get_source(view_id, **kwargs)
        if readback.source_revision != new_revision or readback.view != proposed_view or readback.items != ordered:
            raise ShowcaseSourceError("exact source readback mismatch")
    except Exception:
        result.update(status="applied_not_verified", error=ShowcaseRequestError("READBACK_FAILED").payload())
        return result
    if selected == "publish":
        try:
            published = self.rebuild(view_id, idempotency_key=idempotency_key, expected_source_revision=new_revision)
        except Exception:
            result.update(status="applied_not_published", error=ShowcaseRequestError("PUBLICATION_FAILED").payload(),
                          publish_failure="publication failed; retry showcase.rebuild")
            return result
        result.update(status="published", publish=published, url=published["url"])
        result["validation"].update(buildable=True, build_checked=True)
    return result
''')
# The existing builder/publisher remains the only publication implementation.
replace(manager, 'def rebuild(self, view_id: str, *, idempotency_key: str | None = None) -> dict[str, Any]:', 'def rebuild(self, view_id: str, *, idempotency_key: str | None = None, expected_source_revision: str | None = None) -> dict[str, Any]:')
replace(manager, '            bundle = self.source.load_bundle(view_id)\n            with self.state.transaction()', '            kwargs = ({"revision": expected_source_revision} if expected_source_revision is not None\n                      and "revision" in inspect.signature(self.source.load_bundle).parameters else {})\n            bundle = self.source.load_bundle(view_id, **kwargs)\n            if expected_source_revision is not None and bundle.source_revision != expected_source_revision:\n                raise ShowcaseRequestError("REVISION_CONFLICT")\n            with self.state.transaction()')
# Retain the legacy registration path only for calls without a manifest.
replace(manager, '        publish: bool = True,\n        idempotency_key: str | None = None,\n    ) -> dict[str, Any]:\n        with self._lock:\n            self.source.load_bundle(view_id)', '''        publish: bool | None = None,
        idempotency_key: str | None = None,
        view: ShowcaseView | None = None,
        items: list[ShowcaseItem] | None = None,
        mode: ShowcaseMode | None = None,
        dry_run: bool | None = None,
    ) -> dict[str, Any]:
        if view is not None:
            selected = resolve_mode(mode, dry_run, publish)
            return self.apply(view_id, expected_source_revision="absent", view=view,
                              items=items or [], mode=selected, idempotency_key=idempotency_key)
        if mode is not None or dry_run is not None or items:
            raise ShowcaseRequestError("VIEW_REQUIRED", "view")
        publish = True if publish is None else publish
        with self._lock:
            self.source.load_bundle(view_id)''')

for path in [source, manager]:
    ast.parse(path.read_text())
print('Materialized source snapshots, constructor and exact-revision publication.')
