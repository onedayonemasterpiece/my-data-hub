from __future__ import annotations

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


@dataclass(frozen=True, slots=True)
class BoundedSQLPolicy:
    read_relations: frozenset[str] = frozenset(
        {
            "bloggers.bloggers_ru_v1",
            "hub.project_public_v1",
            "hub.content_public_v1",
        }
    )
    change_targets: frozenset[str] = frozenset({"hub.project", "hub.content_item"})
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

    def _parse(self, sql: str) -> tuple[Any, tuple[Any, ...]]:
        if not isinstance(sql, str) or not sql.strip() or len(sql.encode("utf-8")) > self.max_sql_bytes:
            raise SQLPolicyError("SQL is empty or exceeds the bounded size")
        try:
            from pglast import parse_sql
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
        return roots[0].stmt, tuple(nodes)

    def classify_read(self, sql: str, parameters: Sequence[Any]) -> ClassifiedSQL:
        from pglast import ast

        statement, nodes = self._parse(sql)
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
        return ClassifiedSQL(kind="select", target=None, parameter_count=count)

    def classify_change(self, sql: str, parameters: Sequence[Any]) -> ClassifiedSQL:
        from pglast import ast

        statement, nodes = self._parse(sql)
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
        return ClassifiedSQL(kind=kind, target=target, parameter_count=count)

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
