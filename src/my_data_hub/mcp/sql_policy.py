from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any


class SQLPolicyError(ValueError):
    """A deliberately non-diagnostic rejection at the MCP SQL boundary."""


@dataclass(frozen=True, slots=True)
class ClassifiedSQL:
    kind: str
    target: str | None
    parameter_count: int
    sql_sha256: str
    target_columns: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class BoundedSQLPolicy:
    read_relations: frozenset[str] = frozenset(
        {
            "region_talk.bloggers_ru_v1",
        }
    )
    change_targets: frozenset[str] = frozenset({"hub.project", "hub.content_item"})
    # The database grants repeat this exact surface.  Identity, generated/search,
    # revision and timestamp columns are deliberately not remotely mutable.
    change_columns: tuple[tuple[str, frozenset[str]], ...] = (
        (
            "hub.project",
            frozenset({"project_id", "slug", "name", "description", "status", "metadata"}),
        ),
        (
            "hub.content_item",
            frozenset(
                {
                    "content_id",
                    "content_type",
                    "title",
                    "summary",
                    "body_excerpt",
                    "language",
                    "canonical_url",
                    "normalized_url",
                    "content_hash",
                    "published_at",
                    "first_observed_at",
                    "last_observed_at",
                    "status",
                    "metadata",
                }
            ),
        ),
    )
    safe_functions: frozenset[str] = frozenset(
        {
            "avg",
            "coalesce",
            "count",
            "greatest",
            "least",
            "lower",
            "max",
            "min",
            "sum",
            "upper",
        }
    )
    max_sql_bytes: int = 32_768
    max_parameters: int = 256

    def _parse(self, sql: str) -> tuple[Any, tuple[Any, ...], str]:
        if not isinstance(sql, str) or not sql.strip() or len(sql.encode("utf-8")) > self.max_sql_bytes:
            raise SQLPolicyError("SQL is empty or exceeds the bounded size")
        try:
            from pglast import parse_sql
            from pglast.stream import RawStream
            from pglast.visitors import Visitor

            roots = parse_sql(sql)
        except Exception as exc:
            raise SQLPolicyError("SQL is not a valid PostgreSQL statement") from exc
        if len(roots) != 1:
            raise SQLPolicyError("exactly one SQL statement is required")

        nodes: list[Any] = []

        class Collector(Visitor):
            def visit(self, _ancestors, node):  # type: ignore[no-untyped-def]
                nodes.append(node)

        Collector()(roots)
        statement = roots[0].stmt
        normalized = RawStream()(statement).strip()
        return statement, tuple(nodes), normalized

    def classify_read(self, sql: str, parameters: Sequence[Any]) -> ClassifiedSQL:
        from pglast import ast

        statement, nodes, normalized = self._parse(sql)
        if not isinstance(statement, ast.SelectStmt):
            raise SQLPolicyError("data.query accepts SELECT or read-only CTE only")
        if statement.intoClause is not None or statement.lockingClause:
            raise SQLPolicyError("SELECT INTO and row locking are forbidden")
        forbidden = (
            ast.InsertStmt,
            ast.UpdateStmt,
            ast.DeleteStmt,
            ast.CopyStmt,
            ast.CallStmt,
            ast.DoStmt,
            ast.VariableSetStmt,
            ast.CreateStmt,
            ast.AlterTableStmt,
            ast.DropStmt,
            ast.TransactionStmt,
        )
        if any(isinstance(node, forbidden) for node in nodes):
            raise SQLPolicyError("mutating or administrative SQL is forbidden")

        cte_names = {
            str(node.ctename).casefold()
            for node in nodes
            if isinstance(node, ast.CommonTableExpr)
        }
        for relation in (node for node in nodes if isinstance(node, ast.RangeVar)):
            if relation.schemaname is None and str(relation.relname).casefold() in cte_names:
                continue
            name = self._relation_name(relation)
            if name not in self.read_relations:
                raise SQLPolicyError("query relation is not allowlisted")
        for function in (node for node in nodes if isinstance(node, ast.FuncCall)):
            name = ".".join(str(part.sval) for part in function.funcname).casefold()
            if name not in self.safe_functions:
                raise SQLPolicyError("query function is not allowlisted")
        count = self._validate_parameters(nodes, parameters, require_all=False)
        return ClassifiedSQL(
            kind="select",
            target=None,
            parameter_count=count,
            sql_sha256=hashlib.sha256(normalized.encode("utf-8")).hexdigest(),
        )

    def classify_change(self, sql: str, parameters: Sequence[Any]) -> ClassifiedSQL:
        from pglast import ast

        statement, nodes, normalized = self._parse(sql)
        allowed = (ast.InsertStmt, ast.UpdateStmt, ast.DeleteStmt)
        if not isinstance(statement, allowed):
            raise SQLPolicyError("change accepts parameterized INSERT, UPDATE or DELETE only")
        if isinstance(statement, ast.InsertStmt):
            relation = statement.relation
            kind = "insert"
        elif isinstance(statement, ast.UpdateStmt):
            relation = statement.relation
            kind = "update"
            if statement.whereClause is None:
                raise SQLPolicyError("bounded UPDATE requires a WHERE clause")
        else:
            relation = statement.relation
            kind = "delete"
            if statement.whereClause is None:
                raise SQLPolicyError("bounded DELETE requires a WHERE clause")
        target = self._relation_name(relation)
        if target not in self.change_targets:
            raise SQLPolicyError("change target is not allowlisted")
        if isinstance(statement, ast.InsertStmt):
            columns = tuple(str(column.name).casefold() for column in (statement.cols or ()))
            if not columns or len(columns) != len(statement.cols or ()):
                raise SQLPolicyError("bounded INSERT must name every target column")
        elif isinstance(statement, ast.UpdateStmt):
            columns = tuple(str(column.name).casefold() for column in (statement.targetList or ()))
            if not columns or len(columns) != len(statement.targetList or ()):
                raise SQLPolicyError("bounded UPDATE must name simple target columns")
        else:
            columns = ()
        allowed_columns = dict(self.change_columns).get(target, frozenset())
        if set(columns) - allowed_columns:
            raise SQLPolicyError("change column is not allowlisted")
        relations = [node for node in nodes if isinstance(node, ast.RangeVar)]
        if len(relations) != 1 or self._relation_name(relations[0]) != target:
            raise SQLPolicyError("changes may not read or join any secondary relation")
        nested_statements = sum(isinstance(node, allowed) for node in nodes)
        if nested_statements != 1:
            raise SQLPolicyError("data-modifying CTEs are forbidden")
        selects = [node for node in nodes if isinstance(node, ast.SelectStmt)]
        if isinstance(statement, ast.InsertStmt):
            if len(selects) != 1 or not selects[0].valuesLists or selects[0].fromClause:
                raise SQLPolicyError("INSERT must use one parameterized VALUES clause")
        elif selects:
            raise SQLPolicyError("subqueries are forbidden in remote changes")
        if any(isinstance(node, (ast.FuncCall, ast.A_Const)) for node in nodes):
            raise SQLPolicyError("change values must use parameters, not functions or literals")
        forbidden = (
            ast.CopyStmt,
            ast.CallStmt,
            ast.DoStmt,
            ast.VariableSetStmt,
            ast.CreateStmt,
            ast.AlterTableStmt,
            ast.DropStmt,
            ast.TransactionStmt,
        )
        if any(isinstance(node, forbidden) for node in nodes):
            raise SQLPolicyError("administrative SQL is forbidden")
        count = self._validate_parameters(nodes, parameters, require_all=True)
        if count == 0:
            raise SQLPolicyError("remote changes must be parameterized")
        return ClassifiedSQL(
            kind=kind,
            target=target,
            parameter_count=count,
            sql_sha256=hashlib.sha256(normalized.encode("utf-8")).hexdigest(),
            target_columns=columns,
        )

    @staticmethod
    def _relation_name(relation: Any) -> str:
        if not relation.schemaname or not relation.relname or relation.catalogname:
            raise SQLPolicyError("relations must use an exact allowlisted schema")
        return f"{relation.schemaname}.{relation.relname}".casefold()

    def _validate_parameters(
        self, nodes: Sequence[Any], parameters: Sequence[Any], *, require_all: bool
    ) -> int:
        from pglast import ast

        if isinstance(parameters, (str, bytes, bytearray)) or not isinstance(parameters, Sequence):
            raise SQLPolicyError("parameters must be an ordered JSON array")
        if len(parameters) > self.max_parameters:
            raise SQLPolicyError("too many SQL parameters")
        refs = [int(node.number) for node in nodes if isinstance(node, ast.ParamRef)]
        count = max(refs, default=0)
        if refs and set(refs) != set(range(1, count + 1)):
            raise SQLPolicyError("SQL parameters must be contiguous from $1")
        if count != len(parameters):
            raise SQLPolicyError("SQL parameter count does not match")
        # Literals used for syntax (for example LIMIT) are tolerated, but all
        # externally supplied values must still travel through ParamRef.
        if require_all and any(
            isinstance(value, Mapping) and len(value) > 128 for value in parameters
        ):
            raise SQLPolicyError("SQL parameter value is too complex")
        return count
