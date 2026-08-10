"""PostgreSQL AST classification for bounded operator SQL."""

from __future__ import annotations

import hashlib
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Literal

from .errors import SqlRejected
from .policy import DatabaseAllowlist, Function, Relation

StatementClass = Literal["select", "explain", "insert", "update", "delete"]

# These functions are never eligible for a reader/editor allowlist. The list is
# intentionally redundant with database grants: it prevents common session-control,
# file, lock, sequence, notification, and backend-control side effects at the AST gate.
_ALWAYS_UNSAFE_FUNCTIONS = frozenset(
    {
        "current_setting",
        "dblink_connect",
        "dblink_exec",
        "lo_export",
        "lo_import",
        "nextval",
        "pg_advisory_lock",
        "pg_advisory_lock_shared",
        "pg_advisory_unlock",
        "pg_advisory_unlock_all",
        "pg_advisory_unlock_shared",
        "pg_backup_start",
        "pg_backup_stop",
        "pg_cancel_backend",
        "pg_create_restore_point",
        "pg_log_backend_memory_contexts",
        "pg_notify",
        "pg_promote",
        "pg_read_binary_file",
        "pg_read_file",
        "pg_reload_conf",
        "pg_rotate_logfile",
        "pg_sleep",
        "pg_switch_wal",
        "pg_terminate_backend",
        "set_config",
        "setval",
    }
)


@dataclass(frozen=True, slots=True)
class SqlAnalysis:
    statement_class: StatementClass
    normalized_sql: str
    sql_fingerprint: str
    relations: tuple[Relation, ...]
    functions: tuple[Function, ...]
    parameter_numbers: tuple[int, ...]
    target: Relation | None = None
    target_columns: tuple[str, ...] = ()


def _pglast() -> tuple[Any, Any, Any, Any]:
    try:
        from pglast import ast, parse_sql, scan
        from pglast.stream import RawStream
    except ImportError as exc:  # pragma: no cover - operator packaging guard
        raise SqlRejected("pglast is required for database-operator SQL parsing") from exc
    return ast, parse_sql, scan, RawStream


def _string_value(node: Any) -> str:
    value = getattr(node, "sval", None)
    if not isinstance(value, str):
        raise SqlRejected("malformed identifier in SQL AST")
    return value


def _function_name(node: Any) -> Function:
    parts = tuple(_string_value(part) for part in (node.funcname or ()))
    if len(parts) == 1:
        # The engine fixes search_path to pg_catalog. Unqualified allowed functions
        # therefore cannot be shadowed by an application schema.
        return Function("pg_catalog", parts[0])
    if len(parts) == 2:
        return Function(parts[0], parts[1])
    raise SqlRejected("function names must be unqualified or schema-qualified")


def _explain_executes(stmt: Any) -> bool:
    for option in stmt.options or ():
        if str(option.defname).lower() != "analyze":
            continue
        arg = option.arg
        if arg is None:
            return True
        value = getattr(arg, "sval", None)
        if value is not None:
            return str(value).lower() not in {"false", "off", "no", "0"}
        bool_value = getattr(arg, "boolval", None)
        if bool_value is not None:
            return bool(bool_value)
        int_value = getattr(arg, "ival", None)
        if int_value is not None:
            return bool(int_value)
        return True
    return False


def _validate_explain_options(stmt: Any) -> None:
    # SETTINGS can disclose session configuration, BUFFERS/WAL require ANALYZE in
    # useful forms, and future options should not become implicitly enabled.
    allowed = {"analyze", "costs", "verbose", "format", "generic_plan"}
    for option in stmt.options or ():
        if str(option.defname).lower() not in allowed:
            raise SqlRejected(f"EXPLAIN option is forbidden: {option.defname}")


class _AstValidator:
    def __init__(self, ast: Any, allowlist: DatabaseAllowlist, *, editor_root: Any | None) -> None:
        self.ast = ast
        self.allowlist = allowlist
        self.editor_root = editor_root
        self.relations: set[Relation] = set()
        self.functions: set[Function] = set()
        self.parameters: set[int] = set()

    def walk(self, node: Any, visible_ctes: frozenset[str] = frozenset()) -> None:
        if node is None:
            return
        if isinstance(node, tuple):
            for item in node:
                self.walk(item, visible_ctes)
            return
        if not isinstance(node, self.ast.Node):
            return

        if isinstance(node, self.ast.CommonTableExpr):
            # CommonTableExpr nodes are handled by their owning WITH clause so
            # lexical visibility is preserved.
            self.walk(node.ctequery, visible_ctes)
            return

        with_clause = getattr(node, "withClause", None)
        scoped_ctes = visible_ctes
        if with_clause is not None:
            ctes = tuple(with_clause.ctes or ())
            names = frozenset(str(cte.ctename) for cte in ctes)
            if with_clause.recursive:
                for cte in ctes:
                    self.walk(cte.ctequery, visible_ctes | names)
            else:
                prior = set(visible_ctes)
                for cte in ctes:
                    self.walk(cte.ctequery, frozenset(prior))
                    prior.add(str(cte.ctename))
            scoped_ctes = visible_ctes | names

        if isinstance(node, self.ast.RangeVar):
            if node.catalogname:
                raise SqlRejected("cross-database/catalog relation names are forbidden")
            if node.schemaname is None:
                if str(node.relname) not in visible_ctes:
                    raise SqlRejected("physical relations must be schema-qualified")
            else:
                relation = Relation(str(node.schemaname), str(node.relname))
                if not self.allowlist.can_read(relation):
                    raise SqlRejected(f"relation is not allowlisted: {relation.qualified_name}")
                self.relations.add(relation)

        if isinstance(node, self.ast.FuncCall):
            function = _function_name(node)
            if function.name.lower() in _ALWAYS_UNSAFE_FUNCTIONS:
                raise SqlRejected(f"function is always unsafe: {function.schema}.{function.name}")
            if function not in self.allowlist.readable_functions:
                raise SqlRejected(
                    f"function is not explicitly classified safe: {function.schema}.{function.name}"
                )
            self.functions.add(function)

        if isinstance(node, self.ast.ParamRef):
            if node.number <= 0:
                raise SqlRejected("parameter numbers must be positive")
            self.parameters.add(int(node.number))

        if isinstance(node, self.ast.SelectStmt):
            if node.intoClause is not None:
                raise SqlRejected("SELECT INTO is forbidden")
            if node.lockingClause:
                raise SqlRejected("row-locking SELECT is forbidden")

        statement_types = tuple(
            statement_type
            for name in dir(self.ast)
            if name.endswith("Stmt")
            and isinstance((statement_type := getattr(self.ast, name)), type)
            and issubclass(statement_type, self.ast.Node)
        )
        if isinstance(node, statement_types):
            allowed_nested = isinstance(node, self.ast.SelectStmt)
            if node is self.editor_root:
                allowed_nested = isinstance(
                    node, (self.ast.InsertStmt, self.ast.UpdateStmt, self.ast.DeleteStmt)
                )
            if not allowed_nested:
                raise SqlRejected(f"nested or unsupported statement is forbidden: {type(node).__name__}")

        for member in node:
            if member == "withClause":
                continue
            value = getattr(node, member)
            if isinstance(value, (tuple, self.ast.Node)):
                self.walk(value, scoped_ctes)


def _parse_one(sql: str) -> tuple[Any, Any, Any, str]:
    ast, parse_sql, scan, raw_stream = _pglast()
    if not isinstance(sql, str) or not sql.strip():
        raise SqlRejected("SQL must not be empty")
    try:
        parsed = parse_sql(sql)
    except Exception as exc:
        raise SqlRejected("SQL could not be parsed") from exc
    if len(parsed) != 1:
        raise SqlRejected("exactly one SQL statement is required")
    stmt = parsed[0].stmt
    normalized = raw_stream()(stmt).strip()
    if not normalized:
        raise SqlRejected("SQL normalization produced an empty statement")
    return ast, scan, stmt, normalized


def _validate_parameter_shape(numbers: set[int], params: Sequence[object]) -> tuple[int, ...]:
    expected = set(range(1, len(params) + 1))
    if numbers != expected:
        raise SqlRejected(
            "SQL parameters must use contiguous $1..$N placeholders matching the supplied values"
        )
    return tuple(sorted(numbers))


def analyze_reader_sql(
    sql: str,
    *,
    allowlist: DatabaseAllowlist,
    params: Sequence[object] = (),
) -> SqlAnalysis:
    ast, _scan, stmt, normalized = _parse_one(sql)
    if isinstance(stmt, ast.SelectStmt):
        statement_class: StatementClass = "select"
        root = stmt
    elif isinstance(stmt, ast.ExplainStmt):
        _validate_explain_options(stmt)
        if _explain_executes(stmt):
            raise SqlRejected("EXPLAIN ANALYZE is forbidden")
        if not isinstance(stmt.query, ast.SelectStmt):
            raise SqlRejected("EXPLAIN is limited to SELECT")
        statement_class = "explain"
        root = stmt.query
    else:
        raise SqlRejected("reader accepts only SELECT or non-executing EXPLAIN SELECT")
    validator = _AstValidator(ast, allowlist, editor_root=None)
    validator.walk(root)
    parameter_numbers = _validate_parameter_shape(validator.parameters, params)
    return SqlAnalysis(
        statement_class=statement_class,
        normalized_sql=normalized,
        sql_fingerprint=hashlib.sha256(normalized.encode("utf-8")).hexdigest(),
        relations=tuple(sorted(validator.relations)),
        functions=tuple(sorted(validator.functions)),
        parameter_numbers=parameter_numbers,
    )


def _editor_target(ast: Any, stmt: Any) -> tuple[Relation, tuple[str, ...], StatementClass]:
    if isinstance(stmt, ast.InsertStmt):
        statement_class: StatementClass = "insert"
        if stmt.onConflictClause is not None:
            raise SqlRejected("INSERT ON CONFLICT is not supported by the bounded editor")
        columns = tuple(str(item.name) for item in (stmt.cols or ()) if item.name)
        if not columns or len(columns) != len(stmt.cols or ()):
            raise SqlRejected("INSERT must name every target column explicitly")
    elif isinstance(stmt, ast.UpdateStmt):
        statement_class = "update"
        columns = tuple(str(item.name) for item in (stmt.targetList or ()) if item.name)
        if not columns or len(columns) != len(stmt.targetList or ()):
            raise SqlRejected("UPDATE must use simple, explicitly named target columns")
    elif isinstance(stmt, ast.DeleteStmt):
        statement_class = "delete"
        columns = ()
    else:
        raise SqlRejected("editor accepts only INSERT, UPDATE, or DELETE")
    relation_node = stmt.relation
    if relation_node.catalogname or not relation_node.schemaname:
        raise SqlRejected("write target must be schema-qualified")
    return (
        Relation(str(relation_node.schemaname), str(relation_node.relname)),
        columns,
        statement_class,
    )


def analyze_editor_sql(
    sql: str,
    *,
    allowlist: DatabaseAllowlist,
    params: Sequence[object],
) -> SqlAnalysis:
    ast, _scan, stmt, normalized = _parse_one(sql)
    target, columns, statement_class = _editor_target(ast, stmt)
    allowed_columns = allowlist.writable_for(target)
    if allowed_columns is None:
        raise SqlRejected(f"write target is not allowlisted: {target.qualified_name}")
    denied_columns = sorted(set(columns) - allowed_columns)
    if denied_columns:
        raise SqlRejected("write columns are not allowlisted: " + ", ".join(denied_columns))
    validator = _AstValidator(ast, allowlist, editor_root=stmt)
    validator.walk(stmt)
    parameter_numbers = _validate_parameter_shape(validator.parameters, params)
    if not parameter_numbers:
        raise SqlRejected("editor SQL must use bound parameters")
    return SqlAnalysis(
        statement_class=statement_class,
        normalized_sql=normalized,
        sql_fingerprint=hashlib.sha256(normalized.encode("utf-8")).hexdigest(),
        relations=tuple(sorted(validator.relations)),
        functions=tuple(sorted(validator.functions)),
        parameter_numbers=parameter_numbers,
        target=target,
        target_columns=columns,
    )


def compile_psycopg_parameters(
    normalized_sql: str, params: Sequence[object]
) -> tuple[str, tuple[object, ...]]:
    """Translate PostgreSQL ``$N`` AST parameters to psycopg binding safely.

    Scanner token offsets are byte offsets, so replacement works on UTF-8 bytes and
    never mistakes placeholder-looking text inside a string or comment for a bind.
    Repeated and reordered parameters are expanded for psycopg's positional ``%s``.
    """

    _ast, _parse, scan, _stream = _pglast()
    source = normalized_sql.encode("utf-8")
    output = bytearray()
    bound: list[object] = []
    position = 0
    try:
        tokens = scan(normalized_sql)
    except Exception as exc:  # pragma: no cover - normalized SQL has already parsed
        raise SqlRejected("normalized SQL could not be scanned") from exc
    for token in tokens:
        if token.name != "PARAM":
            continue
        output.extend(source[position : token.start])
        raw = source[token.start : token.end + 1]
        try:
            number = int(raw[1:])
            bound.append(params[number - 1])
        except (ValueError, IndexError) as exc:
            raise SqlRejected("parameter placeholder does not match supplied values") from exc
        output.extend(b"%s")
        position = token.end + 1
    output.extend(source[position:])
    return output.decode("utf-8"), tuple(bound)
