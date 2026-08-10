from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from my_data_hub.db_operator import (
    BackupFreshnessPolicy,
    BackupState,
    DatabaseAllowlist,
    DatabaseOperator,
    EffectBoundsError,
    Function,
    GateClosed,
    IdempotencyConflict,
    InMemoryOperatorJournal,
    OperatorLimits,
    ReceiptError,
    ReceiptSigner,
    Relation,
    RevisionConflict,
    SqlRejected,
    analyze_editor_sql,
    analyze_reader_sql,
    compile_psycopg_parameters,
)

NOW = datetime(2026, 8, 9, 20, 0, tzinfo=UTC)


def allowlist() -> DatabaseAllowlist:
    return DatabaseAllowlist.rollout_r1(
        environment="test",
        disposable_schema="operator_disposable",
        readable_tables=("items", "source"),
        writable_tables={"items": ("item_id", "label")},
        readable_functions=(Function("pg_catalog", "count"),),
    )


def backup_state() -> BackupState:
    return BackupState(
        evidence_revision="backup-12",
        completed_at=NOW - timedelta(hours=1),
        readback_verified=True,
        offsite_available=True,
        schema_revision=9,
        restore_drill_at=NOW - timedelta(days=1),
        restore_drill_succeeded=True,
    )


class FakeDescription:
    def __init__(self, name: str) -> None:
        self.name = name


class FakeCursor:
    def __init__(
        self,
        *,
        rows: list[tuple[object, ...]] | None = None,
        rowcount: int = 0,
        revisions: list[int] | None = None,
    ) -> None:
        self._rows = list(rows or [])
        self._configured_rowcount = rowcount
        self.revisions = list(revisions or [])
        self.rowcount = -1
        self.description: tuple[FakeDescription, ...] | None = None
        self.executed: list[tuple[str, tuple[object, ...]]] = []
        self.closed = False

    def execute(self, query: str, params: Any = None) -> None:
        bound = tuple(params or ())
        self.executed.append((query, bound))
        if query.startswith(("SELECT", "EXPLAIN")) and not query.startswith("SELECT set_config"):
            self.description = (FakeDescription("value"),)
        if query.startswith(("INSERT", "UPDATE", "DELETE")):
            self.rowcount = self._configured_rowcount

    def fetchone(self) -> tuple[object, ...] | None:
        return self._rows.pop(0) if self._rows else None

    def close(self) -> None:
        self.closed = True


class FakeConnection:
    def __init__(self, cursor: FakeCursor) -> None:
        self._cursor = cursor
        self.commits = 0
        self.rollbacks = 0
        self.closed = False

    def cursor(self) -> FakeCursor:
        return self._cursor

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1

    def close(self) -> None:
        self.closed = True


class FakeFactory:
    def __init__(self, *connections: FakeConnection) -> None:
        self.connections = list(connections)

    def __call__(self) -> FakeConnection:
        return self.connections.pop(0)


def revision_reader(cursor: FakeCursor) -> int:
    return cursor.revisions.pop(0)


def make_operator(
    factory: FakeFactory,
    *,
    backup_provider: Any = backup_state,
    limits: OperatorLimits | None = None,
    journal: Any | None = None,
) -> DatabaseOperator:
    return DatabaseOperator(
        connection_factory=factory,
        allowlist=allowlist(),
        revision_reader=revision_reader,
        backup_state_provider=backup_provider,
        schema_revision=9,
        signer=ReceiptSigner(b"s" * 32),
        limits=limits,
        clock=lambda: NOW,
        journal=journal or InMemoryOperatorJournal(),
    )


def test_r1_allowlist_is_disposable_only_and_always_empty_in_production() -> None:
    test_policy = allowlist()
    assert test_policy.readable_relations == {
        Relation("operator_disposable", "items"),
        Relation("operator_disposable", "source"),
    }
    production = DatabaseAllowlist.rollout_r1(
        environment="production",
        disposable_schema="operator_disposable",
        readable_tables=("items",),
        writable_tables={"items": ("label",)},
        readable_functions=(Function("pg_catalog", "count"),),
    )
    assert not production.readable_relations
    assert not production.writable_columns
    assert not production.readable_functions


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT 1; SELECT 2",
        "WITH changed AS (DELETE FROM operator_disposable.items RETURNING *) SELECT * FROM changed",
        "SELECT * INTO operator_disposable.copy FROM operator_disposable.items",
        "SELECT * FROM operator_disposable.items FOR UPDATE",
        "SELECT pg_catalog.pg_sleep(10)",
        "SELECT pg_catalog.set_config('statement_timeout', '0', true)",
        "SELECT * FROM public.items",
        "SELECT * FROM items",
        "EXPLAIN ANALYZE SELECT * FROM operator_disposable.items",
        "EXPLAIN (SETTINGS true) SELECT * FROM operator_disposable.items",
    ],
)
def test_reader_ast_rejects_mutation_locks_unsafe_functions_and_bypasses(sql: str) -> None:
    with pytest.raises(SqlRejected):
        analyze_reader_sql(sql, allowlist=allowlist())


def test_reader_accepts_safe_nested_cte_and_explicit_function() -> None:
    analysis = analyze_reader_sql(
        "WITH selected AS (SELECT * FROM operator_disposable.items) "
        "SELECT count(*) FROM selected",
        allowlist=allowlist(),
    )
    assert analysis.statement_class == "select"
    assert analysis.relations == (Relation("operator_disposable", "items"),)
    assert analysis.functions == (Function("pg_catalog", "count"),)


def test_parameter_compilation_uses_scanner_not_placeholder_text() -> None:
    analysis = analyze_reader_sql(
        "SELECT '$1' FROM operator_disposable.items WHERE item_id = $1 OR item_id = $1",
        allowlist=allowlist(),
        params=(42,),
    )
    query, bound = compile_psycopg_parameters(analysis.normalized_sql, (42,))
    assert "'$1'" in query
    assert query.count("%s") == 2
    assert bound == (42, 42)


@pytest.mark.parametrize(
    "sql, params",
    [
        ("TRUNCATE operator_disposable.items", (1,)),
        ("UPDATE operator_disposable.items SET label = 'literal'", ()),
        ("UPDATE operator_disposable.items SET missing = $1", ("x",)),
        ("INSERT INTO operator_disposable.items VALUES ($1, $2)", (1, "x")),
        (
            "INSERT INTO operator_disposable.items (item_id) VALUES ($1) "
            "ON CONFLICT (item_id) DO UPDATE SET label = $2",
            (1, "x"),
        ),
        (
            "WITH removed AS (DELETE FROM operator_disposable.source WHERE item_id = $1) "
            "UPDATE operator_disposable.items SET label = $2",
            (1, "x"),
        ),
    ],
)
def test_editor_accepts_only_parameterized_allowlisted_simple_dml(
    sql: str, params: tuple[object, ...]
) -> None:
    with pytest.raises(SqlRejected):
        analyze_editor_sql(sql, allowlist=allowlist(), params=params)

    accepted = analyze_editor_sql(
        "UPDATE operator_disposable.items SET label = $1 WHERE item_id = $2",
        allowlist=allowlist(),
        params=("fixed", 4),
    )
    assert accepted.statement_class == "update"
    assert accepted.target == Relation("operator_disposable", "items")
    assert accepted.target_columns == ("label",)


def test_backup_gate_fails_closed_for_each_required_evidence() -> None:
    policy = BackupFreshnessPolicy()
    policy.require_open(backup_state(), now=NOW, expected_schema_revision=9)
    failures = (
        replace(backup_state(), completed_at=NOW - timedelta(days=2)),
        replace(backup_state(), readback_verified=False),
        replace(backup_state(), offsite_available=False),
        replace(backup_state(), restore_drill_succeeded=False),
        replace(backup_state(), restore_drill_at=NOW - timedelta(days=8)),
        replace(backup_state(), schema_revision=8),
        replace(backup_state(), unprotected_high_impact_change=True),
    )
    for failed in failures:
        with pytest.raises(GateClosed):
            policy.require_open(failed, now=NOW, expected_schema_revision=9)


def test_reader_enforces_session_controls_and_reports_row_truncation() -> None:
    cursor = FakeCursor(rows=[(1,), (2,), (3,)])
    connection = FakeConnection(cursor)
    operator = make_operator(
        FakeFactory(connection),
        limits=OperatorLimits(max_rows=2, max_bytes=100, max_write_rows=10),
    )
    result = operator.read("SELECT item_id FROM operator_disposable.items")
    assert result.rows == ((1,), (2,))
    assert result.truncated is True
    assert result.truncation_reasons == ("row_limit",)
    controls = "\n".join(query for query, _params in cursor.executed)
    assert "BEGIN TRANSACTION READ ONLY" in controls
    assert "SET LOCAL search_path = pg_catalog" in controls
    assert "SET LOCAL statement_timeout = '10000ms'" in controls
    assert "SET LOCAL transaction_timeout = '15000ms'" in controls
    assert "SET LOCAL lock_timeout = '2000ms'" in controls
    assert "SET LOCAL idle_in_transaction_session_timeout = '15000ms'" in controls
    assert connection.rollbacks == 1
    assert connection.closed and cursor.closed


def test_reader_reports_byte_truncation_without_exceeding_cap() -> None:
    cursor = FakeCursor(rows=[("a" * 20,)])
    operator = make_operator(
        FakeFactory(FakeConnection(cursor)),
        limits=OperatorLimits(max_rows=10, max_bytes=10, max_write_rows=10),
    )
    result = operator.read("SELECT label FROM operator_disposable.items")
    assert result.rows == ()
    assert result.serialized_bytes <= 10
    assert result.truncation_reasons == ("byte_limit",)


def _preview(operator: DatabaseOperator) -> str:
    result = operator.preview(
        "UPDATE operator_disposable.items SET label = $1 WHERE item_id = $2",
        params=("fixed", 4),
        principal="owner",
        session_id="session-1",
        correlation_id="correlation-1",
        expected_revision=7,
        expected_row_min=1,
        expected_row_max=1,
    )
    return result.receipt


def test_preview_apply_binds_inputs_commits_once_and_replays_idempotently() -> None:
    preview_connection = FakeConnection(FakeCursor(rowcount=1, revisions=[7]))
    apply_connection = FakeConnection(FakeCursor(rowcount=1, revisions=[7, 8]))
    replay_connection = FakeConnection(FakeCursor())
    operator = make_operator(
        FakeFactory(preview_connection, apply_connection, replay_connection)
    )
    receipt = _preview(operator)
    assert preview_connection.rollbacks == 1
    assert preview_connection.commits == 0

    applied = operator.apply(
        "UPDATE operator_disposable.items SET label = $1 WHERE item_id = $2",
        params=("fixed", 4),
        principal="owner",
        session_id="session-1",
        correlation_id="correlation-1",
        preview_receipt=receipt,
        idempotency_key="repair-item-4",
    )
    assert applied.affected_rows == 1
    assert (applied.revision_before, applied.revision_after) == (7, 8)
    assert apply_connection.commits == 1
    assert apply_connection.rollbacks == 0

    replayed = operator.apply(
        "UPDATE operator_disposable.items SET label = $1 WHERE item_id = $2",
        params=("fixed", 4),
        principal="owner",
        session_id="session-1",
        correlation_id="correlation-1",
        preview_receipt=receipt,
        idempotency_key="repair-item-4",
    )
    assert replayed.replayed is True
    assert replayed.receipt == applied.receipt


def test_apply_rejects_forgery_other_principal_and_idempotency_collision() -> None:
    connections = [
        FakeConnection(FakeCursor(rowcount=1, revisions=[7])),
        FakeConnection(FakeCursor(rowcount=1, revisions=[7, 8])),
        FakeConnection(FakeCursor(rowcount=1, revisions=[7])),
        FakeConnection(FakeCursor()),
    ]
    operator = make_operator(FakeFactory(*connections))
    receipt = _preview(operator)
    with pytest.raises(ReceiptError):
        operator.apply(
            "UPDATE operator_disposable.items SET label = $1 WHERE item_id = $2",
            params=("fixed", 4),
            principal="intruder",
            session_id="session-1",
            correlation_id="correlation-1",
            preview_receipt=receipt,
            idempotency_key="repair-item-4",
        )
    with pytest.raises(ReceiptError):
        operator.apply(
            "UPDATE operator_disposable.items SET label = $1 WHERE item_id = $2",
            params=("fixed", 4),
            principal="owner",
            session_id="session-1",
            correlation_id="correlation-1",
            preview_receipt=receipt[:-1] + ("A" if receipt[-1] != "A" else "B"),
            idempotency_key="repair-item-4",
        )
    operator.apply(
        "UPDATE operator_disposable.items SET label = $1 WHERE item_id = $2",
        params=("fixed", 4),
        principal="owner",
        session_id="session-1",
        correlation_id="correlation-1",
        preview_receipt=receipt,
        idempotency_key="repair-item-4",
    )
    second_receipt = _preview(operator)
    with pytest.raises(IdempotencyConflict):
        operator.apply(
            "UPDATE operator_disposable.items SET label = $1 WHERE item_id = $2",
            params=("fixed", 4),
            principal="owner",
            session_id="session-1",
            correlation_id="correlation-1",
            preview_receipt=second_receipt,
            idempotency_key="repair-item-4",
        )


def test_apply_rolls_back_on_revision_or_effect_mismatch() -> None:
    preview_connection = FakeConnection(FakeCursor(rowcount=1, revisions=[7]))
    revision_connection = FakeConnection(FakeCursor(rowcount=1, revisions=[9]))
    operator = make_operator(FakeFactory(preview_connection, revision_connection))
    receipt = _preview(operator)
    with pytest.raises(RevisionConflict):
        operator.apply(
            "UPDATE operator_disposable.items SET label = $1 WHERE item_id = $2",
            params=("fixed", 4),
            principal="owner",
            session_id="session-1",
            correlation_id="correlation-1",
            preview_receipt=receipt,
            idempotency_key="revision-mismatch",
        )
    assert revision_connection.rollbacks == 1

    preview_connection = FakeConnection(FakeCursor(rowcount=1, revisions=[7]))
    effect_connection = FakeConnection(FakeCursor(rowcount=2, revisions=[7]))
    operator = make_operator(FakeFactory(preview_connection, effect_connection))
    receipt = _preview(operator)
    with pytest.raises(EffectBoundsError):
        operator.apply(
            "UPDATE operator_disposable.items SET label = $1 WHERE item_id = $2",
            params=("fixed", 4),
            principal="owner",
            session_id="session-1",
            correlation_id="correlation-1",
            preview_receipt=receipt,
            idempotency_key="effect-mismatch",
        )
    assert effect_connection.rollbacks == 1


def test_apply_rechecks_backup_freshness_and_exact_bound_state() -> None:
    state = [backup_state()]
    preview_connection = FakeConnection(FakeCursor(rowcount=1, revisions=[7]))
    operator = make_operator(
        FakeFactory(preview_connection),
        backup_provider=lambda: state[0],
    )
    receipt = _preview(operator)
    state[0] = replace(state[0], completed_at=NOW - timedelta(days=2))
    with pytest.raises(GateClosed):
        operator.apply(
            "UPDATE operator_disposable.items SET label = $1 WHERE item_id = $2",
            params=("fixed", 4),
            principal="owner",
            session_id="session-1",
            correlation_id="correlation-1",
            preview_receipt=receipt,
            idempotency_key="stale-backup",
        )


def test_high_impact_preview_requires_a_checkpoint() -> None:
    operator = make_operator(FakeFactory())
    with pytest.raises(GateClosed, match="checkpoint"):
        operator.preview(
            "UPDATE operator_disposable.items SET label = $1 WHERE item_id = $2",
            params=("fixed", 4),
            principal="owner",
            session_id="session-1",
            correlation_id="correlation-1",
            expected_revision=7,
            expected_row_min=1,
            expected_row_max=1,
            impact_tier="high",
        )

    policy = BackupFreshnessPolicy()
    with pytest.raises(GateClosed, match="expected canonical revision"):
        policy.require_open(
            replace(backup_state(), checkpoint_revision=6),
            now=NOW,
            expected_schema_revision=9,
            require_checkpoint=True,
            expected_canonical_revision=7,
        )


def test_journal_failure_rolls_back_dml_before_commit() -> None:
    class FailingJournal(InMemoryOperatorJournal):
        def record_apply(self, cursor: Any, **values: Any) -> None:
            raise RuntimeError("durable journal unavailable")

    preview_connection = FakeConnection(FakeCursor(rowcount=1, revisions=[7]))
    apply_connection = FakeConnection(FakeCursor(rowcount=1, revisions=[7, 8]))
    operator = make_operator(
        FakeFactory(preview_connection, apply_connection), journal=FailingJournal()
    )
    receipt = _preview(operator)
    with pytest.raises(RuntimeError, match="journal unavailable"):
        operator.apply(
            "UPDATE operator_disposable.items SET label = $1 WHERE item_id = $2",
            params=("fixed", 4),
            principal="owner",
            session_id="session-1",
            correlation_id="correlation-1",
            preview_receipt=receipt,
            idempotency_key="journal-failure",
        )
    assert apply_connection.commits == 0
    assert apply_connection.rollbacks == 1


def test_expired_preview_is_rejected() -> None:
    signer = ReceiptSigner(b"x" * 32, preview_ttl=timedelta(seconds=1))
    token = signer.issue_preview(
        now=NOW,
        principal="owner",
        session_id="session",
        correlation_id="correlation",
        sql_fingerprint="a" * 64,
        params_fingerprint="b" * 64,
        target="operator_disposable.items",
        expected_revision=1,
        expected_row_min=0,
        expected_row_max=1,
        preview_affected_rows=1,
        backup_evidence_revision="backup",
        backup_fingerprint="c" * 64,
        impact_tier="low",
    )
    with pytest.raises(ReceiptError, match="expired"):
        signer.verify_preview(token, now=NOW + timedelta(seconds=1))
