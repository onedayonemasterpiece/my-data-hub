"""One-shot transport edit; removed after committing the resulting source."""
from pathlib import Path
import ast
import textwrap

ROOT = Path('src/my_data_hub/showcase')

def replace(path, old, new, count=1):
    text = path.read_text()
    assert text.count(old) == count, (path, old[:100], text.count(old))
    path.write_text(text.replace(old, new))

def function(path, name, code, owner=None):
    text = path.read_text()
    tree = ast.parse(text)
    scope = tree
    if owner:
        scope = next(n for n in ast.walk(tree) if isinstance(n, ast.ClassDef) and n.name == owner)
    matches = [n for n in ast.walk(scope) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name == name]
    assert len(matches) == 1, (path, name)
    node = matches[0]
    lines = text.splitlines(keepends=True)
    lines[node.lineno-1:node.end_lineno] = [textwrap.indent(textwrap.dedent(code).strip() + '\n', ' '*node.col_offset)]
    path.write_text(''.join(lines))

request = ROOT / 'requests.py'
replace(request, 'import re\n', 'import re\nimport json\n')
replace(request, 'super().__init__(PROBLEMS[code][0])', 'super().__init__(json.dumps(self.payload(), ensure_ascii=False))')

runtime = ROOT / 'runtime.py'
replace(runtime, 'from pydantic import BaseModel, ConfigDict, Field', 'from pydantic import BaseModel, ConfigDict, Field, ValidationError')
replace(runtime, 'from my_data_hub.showcase.manager import ShowcaseManager', '''from my_data_hub.showcase.manager import ShowcaseManager
from my_data_hub.showcase.requests import (
    MAX_ARGUMENT_BYTES, MAX_REQUEST_BYTES, ShowcaseRequestError, ShowcaseSourceError, resolve_mode,
)
from my_data_hub.showcase.source import ShowcaseSourceNotFoundError''')
replace(runtime, 'max_request_bytes: int = 65_536', 'max_request_bytes: int = MAX_REQUEST_BYTES', 2)
replace(runtime, '"MY_DATA_HUB_SHOWCASE_MAX_REQUEST_BYTES", "65536"', '"MY_DATA_HUB_SHOWCASE_MAX_REQUEST_BYTES", str(MAX_REQUEST_BYTES)')
replace(runtime, 'if len(bounded) > 32_768:\n            raise ShowcaseRuntimeRequestError("showcase arguments exceed the semantic limit")', 'if len(bounded) > MAX_ARGUMENT_BYTES:\n            raise ShowcaseRequestError("REQUEST_TOO_LARGE")')
replace(runtime, 'raise ShowcaseRuntimeRequestError("idempotency_key is invalid")', 'raise ShowcaseRequestError("IDEMPOTENCY_REQUIRED", "idempotency_key")')
replace(runtime, 'raise ShowcaseRuntimeRequestError("view_id is invalid")', 'raise ShowcaseRequestError("INVALID_FIELD", "view_id")')
replace(runtime, '            idempotency_key = _validate_idempotency_key(arguments.get("idempotency_key"))', '''            mutation_arguments = {}
            if tool in {"showcase.apply", "showcase.create_view"}:
                legacy_registration = (tool == "showcase.create_view" and arguments.get("view") is None
                                       and arguments.get("mode") is None and arguments.get("dry_run") is None)
                if legacy_registration:
                    selected = "save" if arguments.get("publish") is False else "publish"
                    mutation_arguments = {"publish": arguments.get("publish")}
                else:
                    selected = resolve_mode(arguments.get("mode"), arguments.get("dry_run"), arguments.get("publish"))
                    mutation_arguments = {"view": arguments.get("view"), "items": arguments.get("items") or [],
                                          "mode": selected}
                    if tool == "showcase.apply":
                        mutation_arguments["expected_source_revision"] = arguments.get("expected_source_revision")
                if selected == "preview":
                    # Pure preview never reads/writes the idempotency journal or consumes a key.
                    return _call_method(self.manager, "apply" if tool == "showcase.apply" else "create_view",
                                        view_id, **mutation_arguments)
            idempotency_key = _validate_idempotency_key(arguments.get("idempotency_key"))''')
replace(runtime, 'raise ShowcaseRuntimeConflictError("idempotency key was previously used with different payload")', 'raise ShowcaseRequestError("IDEMPOTENCY_CONFLICT", "idempotency_key")')
replace(runtime, '''                if tool == "showcase.apply":
                    result = _call_method(
                        self.manager,
                        "apply",
                        view_id,
                        expected_source_revision=arguments.get("expected_source_revision"),
                        view=arguments.get("view"),
                        items=arguments.get("items", []),
                        dry_run=arguments.get("dry_run", True),
                        publish=arguments.get("publish", False),
                        idempotency_key=idempotency_key,
                    )''', '''                if tool in {"showcase.apply", "showcase.create_view"}:
                    result = _call_method(
                        self.manager, "apply" if tool == "showcase.apply" else "create_view",
                        view_id, idempotency_key=idempotency_key, **mutation_arguments,
                    )''')
# Adapt older test/development managers while production receives the full new contract.
replace(runtime, '    accepted = {key: value for key, value in kwargs.items() if key in parameters}', '''    if "mode" in kwargs and "mode" not in parameters:
        selected = kwargs.pop("mode")
        kwargs.update(dry_run=selected == "preview", publish=selected == "publish")
    accepted = {key: value for key, value in kwargs.items() if key in parameters}''')
replace(runtime, '        except ShowcaseRuntimeAuthenticationError as exc:', '''        except ShowcaseRequestError as exc:
            return JSONResponse(status_code=exc.http_status,
                                content={"ok": False, "code": exc.code, "error": exc.payload()})
        except ShowcaseSourceNotFoundError:
            error = ShowcaseRequestError("VIEW_NOT_FOUND", "view_id")
            return JSONResponse(status_code=404, content={"ok": False, "code": error.code, "error": error.payload()})
        except ValidationError as exc:
            first = exc.errors(include_url=False, include_input=False)[0]
            field = ".".join(str(part) for part in first.get("loc", []))[:160]
            error = ShowcaseRequestError("INVALID_FIELD", field or None)
            return JSONResponse(status_code=400, content={"ok": False, "code": error.code, "error": error.payload()})
        except ShowcaseSourceError:
            error = ShowcaseRequestError("SOURCE_UNAVAILABLE")
            return JSONResponse(status_code=503, content={"ok": False, "code": error.code, "error": error.payload()})
        except ShowcaseRuntimeAuthenticationError as exc:''')

# Preserve actionable, allowlisted diagnostics across the real HTTP edge.
gateway = ROOT / 'gateway.py'
replace(gateway, 'import os\n', 'import os\nimport json\n')
replace(gateway, 'from my_data_hub.auth.context import current_identity', 'from .requests import ShowcaseMode, safe_problem\nfrom my_data_hub.auth.context import current_identity')
replace(gateway, '            code = document.get("code") if isinstance(document, dict) else None', '''            problem = safe_problem(document.get("error")) if isinstance(document, dict) else None
            if problem is not None:
                raise ShowcaseGatewayError(json.dumps(problem, ensure_ascii=False))
            code = document.get("code") if isinstance(document, dict) else None''')
function(gateway, 'apply', '''
def apply(
    self, view_id: str, *, expected_source_revision: str, view: dict[str, Any] | None = None,
    items: list[dict[str, Any]] | None = None, dry_run: bool | None = None,
    publish: bool | None = None, mode: ShowcaseMode | None = None, idempotency_key: str | None = None,
) -> dict[str, Any] | list[Any]:
    return self._invoke("showcase.apply", {
        "view_id": view_id, "expected_source_revision": expected_source_revision,
        "view": view, "items": items or [], "dry_run": dry_run, "publish": publish,
        "mode": mode, "idempotency_key": idempotency_key,
    })
''', owner='ShowcaseGatewayClient')
function(gateway, 'create_view', '''
def create_view(
    self, view_id: str, *, view: dict[str, Any] | None = None, items: list[dict[str, Any]] | None = None,
    mode: ShowcaseMode | None = None, dry_run: bool | None = None, publish: bool | None = None,
    idempotency_key: str | None = None,
) -> dict[str, Any] | list[Any]:
    return self._invoke("showcase.create_view", {
        "view_id": view_id, "view": view, "items": items or [], "mode": mode,
        "dry_run": dry_run, "publish": publish, "idempotency_key": idempotency_key,
    })
''', owner='ShowcaseGatewayClient')
# Include source/readback/build time in the synchronous request budget; retries remain idempotent.
replace(gateway, 'timeout_seconds: float = 45.0', 'timeout_seconds: float = 240.0')
replace(gateway, '"MY_DATA_HUB_SHOWCASE_GATEWAY_TIMEOUT_SECONDS", "45"', '"MY_DATA_HUB_SHOWCASE_GATEWAY_TIMEOUT_SECONDS", "240"')
replace(gateway, 'if not 1 <= timeout <= 180:', 'if not 1 <= timeout <= 300:')
replace(gateway, 'timeout must be 1..180 seconds', 'timeout must be 1..300 seconds')

for path, manager_expr in [(Path('src/my_data_hub/mcp/server.py'), 'showcase_manager()'),
                           (ROOT / 'mcp_server.py', 'control')]:
    if path.name == 'server.py':
        replace(path, 'from my_data_hub.showcase.models import ShowcaseItem, ShowcaseView',
                'from my_data_hub.showcase.models import ShowcaseWriteItem, ShowcaseViewInput\nfrom my_data_hub.showcase.requests import ShowcaseMode')
        replace(path, 'if tool.name.startswith("region_talk."):', 'if tool.name.startswith(("region_talk.", "showcase.")):')
        replace(path, '"Writes require an identity-bound preview and checkpoint lifecycle permit."',
                '"Master writes require an identity-bound preview and checkpoint permit. "\n            "Showcase is independent of the master: create_view creates curated showcases, "\n            "apply updates them; mode=preview/save/publish. Existing cards are referenced by ID."')
        replace(path, 'request_timeout_seconds=30,', 'request_timeout_seconds=300,')
    else:
        replace(path, 'from .models import ShowcaseItem, ShowcaseView',
                'from .models import ShowcaseWriteItem, ShowcaseViewInput\nfrom .requests import ShowcaseMode')
    apply_code = '''
async def showcase_apply(
    view_id: str, expected_source_revision: str, view: ShowcaseViewInput | None = None,
    idempotency_key: str | None = None, items: list[ShowcaseWriteItem] | None = None,
    mode: ShowcaseMode | None = None, dry_run: bool | None = None, publish: bool | None = None,
) -> dict[str, Any] | list[Any]:
    """Update a showcase; keep its URL. Read get_source first and copy source_revision.

    Prefer mode=preview (no writes), save (draft) or publish (save/build/publish).
    Omit unchanged cards. Pass full definitions only for new/changed cards.
    Changing a card shared with another view is rejected: give the adaptation a new ID.
    Preview needs no key; save/publish require a unique idempotency_key, reused on identical retry.
    Example: view_id='pharma', expected_source_revision='<from get_source>',
    view={title:'Updated title', subtitle:'Updated description', item_ids:['existing-card']}, mode='preview'.
    Legacy dry_run/publish and expected_source_revision='absent' remain compatible; do not mix flags with mode.
    """
    return await asyncio.to_thread(MANAGER.apply, view_id,
        expected_source_revision=expected_source_revision, view=view, items=items or [],
        mode=mode, dry_run=dry_run, publish=publish, idempotency_key=idempotency_key)
'''.replace('MANAGER', manager_expr)
    create_code = '''
async def showcase_create_view(
    view_id: str, view: ShowcaseViewInput | None = None, items: list[ShowcaseWriteItem] | None = None,
    mode: ShowcaseMode | None = None, idempotency_key: str | None = None,
    dry_run: bool | None = None, publish: bool | None = None,
) -> dict[str, Any] | list[Any]:
    """Create a NEW showcase from a manifest and return its stable link on publication.

    No source revision or 'absent' value is needed. view.id can be omitted.
    Existing cards: view={title:'For pharma', subtitle:'Four working tasks', item_ids:['existing-card']}.
    New card: add its ID to item_ids and pass its complete definition in items,
    including capability_type (technical/product/business) and publish_state='ready' after review.
    mode=preview validates without writes/key; mode=save saves a draft; mode=publish saves/builds/publishes.
    save/publish require idempotency_key; identical retries reuse it. Default with a manifest is preview.
    Optional contacts override the default Telegram contact; filters=[] hides extra filters.
    Legacy calls without view only register an already existing source; never use that form to create content.
    """
    return await asyncio.to_thread(MANAGER.create_view, view_id, view=view, items=items or [],
        mode=mode, dry_run=dry_run, publish=publish, idempotency_key=idempotency_key)
'''.replace('MANAGER', manager_expr)
    function(path, 'showcase_apply', apply_code)
    function(path, 'showcase_create_view', create_code)

for path in [request, runtime, gateway, ROOT / 'mcp_server.py', Path('src/my_data_hub/mcp/server.py')]:
    ast.parse(path.read_text())
print('Materialized public MCP schemas, preview/idempotency routing and safe diagnostics.')
