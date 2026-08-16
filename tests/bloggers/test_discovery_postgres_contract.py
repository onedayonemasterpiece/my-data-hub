from __future__ import annotations

from uuid import UUID

import pytest

from my_data_hub.workloads.bloggers.discovery_postgres import (
    BloggerDiscoveryPostgres,
    BloggerImportIdentity,
)


class Result:
    def __init__(self, row):
        self.row = row

    def fetchone(self):
        return self.row


class Cursor:
    def __init__(self, rows):
        self.rows = iter(rows)
        self.calls = []

    def execute(self, sql, parameters):
        self.calls.append((sql, parameters))
        return Result(next(self.rows))


def identity() -> BloggerImportIdentity:
    return BloggerImportIdentity(
        batch_id=UUID("11111111-1111-4111-8111-111111111111"),
        operation_id="a" * 64,
        request_sha256="b" * 64,
        expected_revision=4,
        principal_id="datahub-owner",
        client_id="chatgpt-owner",
    )


def test_fixed_preview_apply_and_reconcile_never_accept_sql_text() -> None:
    preview_row = {
        "batch_id": str(identity().batch_id),
        "operation_id": "a" * 64,
        "request_sha256": "b" * 64,
        "plan_sha256": "c" * 64,
        "expected_revision": 4,
        "create_actor_count": 1,
        "link_existing_count": 0,
        "quarantine_count": 0,
        "account_count": 1,
    }
    apply_row = {
        "operation_id": "a" * 64,
        "batch_id": str(identity().batch_id),
        "plan_sha256": "c" * 64,
        "affected_rows": 3,
        "revision_after": 5,
        "duplicate": False,
    }
    cursor = Cursor([preview_row, apply_row, {**apply_row, "duplicate": True}])
    preview = BloggerDiscoveryPostgres.preview(cursor, identity())
    applied = BloggerDiscoveryPostgres.apply(cursor, identity(), plan_sha256=preview.plan_sha256)
    reconciled = BloggerDiscoveryPostgres.reconcile(
        cursor,
        identity(),
        plan_sha256=preview.plan_sha256,
        master_instance_id=UUID("22222222-2222-4222-8222-222222222222"),
        master_epoch=7,
    )
    assert preview.create_actor_count == 1
    assert applied.revision_after == 5
    assert reconciled is not None and reconciled.duplicate
    assert all("integration." in sql and ";" not in sql for sql, _ in cursor.calls)
    assert all("sql" not in repr(parameters).lower() for _, parameters in cursor.calls)


def test_facade_rejects_unbounded_or_unhashed_identity_before_query() -> None:
    with pytest.raises(ValueError, match="operation_id"):
        BloggerImportIdentity(
            batch_id=UUID(int=0), operation_id="not-hash", request_sha256="b" * 64,
            expected_revision=0, principal_id="owner", client_id="client"
        )
