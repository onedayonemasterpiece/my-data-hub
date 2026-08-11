from __future__ import annotations

import pytest

from my_data_hub.mcp.sql_policy import BoundedSQLPolicy, SQLPolicyError


@pytest.fixture
def policy() -> BoundedSQLPolicy:
    return BoundedSQLPolicy()


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT 1; SELECT 2",
        "INSERT INTO hub.project(project_id) VALUES ($1)",
        "UPDATE hub.project SET name=$1 WHERE project_id=$2",
        "DELETE FROM hub.project WHERE project_id=$1",
        "COPY hub.project TO STDOUT",
        "CALL hub.run_job()",
        "DO $$ BEGIN NULL; END $$",
        "SELECT * FROM hub.project",
        "SELECT * FROM pg_catalog.pg_authid",
        "SELECT pg_read_file($1)",
        "SELECT * FROM hub.project_public_v1 FOR UPDATE",
        "SELECT * INTO TEMP leaked FROM hub.project_public_v1",
        "SET ROLE owner",
    ],
)
def test_data_query_rejects_nonselect_system_and_unsafe_sql(
    policy: BoundedSQLPolicy, sql: str
) -> None:
    with pytest.raises(SQLPolicyError):
        policy.classify_read(sql, ["x"] if "$1" in sql else [])


def test_data_query_accepts_allowlisted_cte_join_and_parameters(policy: BoundedSQLPolicy) -> None:
    result = policy.classify_read(
        "WITH selected AS (SELECT * FROM region_talk.bloggers_ru_v1 WHERE blogger_id=$1) "
        "SELECT count(*) FROM selected",
        ["b1"],
    )
    assert result.kind == "select"
    assert result.parameter_count == 1


@pytest.mark.parametrize(
    ("sql", "parameters"),
    [
        ("DELETE FROM hub.project", []),
        ("UPDATE hub.project SET name=$1", ["x"]),
        ("UPDATE auth.oauth_revocation SET subject=$1 WHERE id=$2", ["x", "y"]),
        ("UPDATE hub.project SET name='literal' WHERE project_id='literal'", []),
        ("UPDATE hub.project SET name=$1 FROM auth.oauth_revocation r WHERE project_id=$2", ["x", "y"]),
        ("INSERT INTO hub.project(project_id) SELECT subject FROM auth.oauth_revocation", []),
        ("UPDATE hub.project SET name=pg_read_file($1) WHERE project_id=$2", ["x", "y"]),
        ("UPDATE hub.project SET name=$2 WHERE project_id=$3", ["x", "y", "z"]),
        ("UPDATE hub.project SET name=$1 WHERE project_id=$2", ["x"]),
        ("COPY hub.project FROM STDIN", []),
        ("SELECT 1", []),
    ],
)
def test_change_rejects_unbounded_unparameterized_or_forbidden_sql(
    policy: BoundedSQLPolicy, sql: str, parameters: list[str]
) -> None:
    with pytest.raises(SQLPolicyError):
        policy.classify_change(sql, parameters)


@pytest.mark.parametrize(
    ("sql", "parameters", "kind"),
    [
        ("INSERT INTO hub.project(project_id, name) VALUES ($1, $2)", ["p1", "n"], "insert"),
        ("UPDATE hub.project SET name=$1 WHERE project_id=$2", ["n", "p1"], "update"),
        ("DELETE FROM hub.project WHERE project_id=$1", ["p1"], "delete"),
    ],
)
def test_change_accepts_one_parameterized_allowlisted_statement(
    policy: BoundedSQLPolicy, sql: str, parameters: list[str], kind: str
) -> None:
    assert policy.classify_change(sql, parameters).kind == kind
