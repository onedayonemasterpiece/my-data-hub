from __future__ import annotations

from pathlib import Path

from pglast import parse_sql

ROOT = Path(__file__).resolve().parents[2]


def test_epoch_migration_parses_and_has_commit_time_write_guard() -> None:
    source = (ROOT / "sql/migrations/0011_master_epoch_fencing.sql").read_text()
    parse_sql(source)
    assert "clock_timestamp()" in source
    assert "DEFERRABLE INITIALLY DEFERRED" in source
    assert "session_user" in source
    assert "requested_epoch <> state.highest_epoch + 1" in source
    assert "state.lease_until <= observed_at" in source
    assert "state.gate_state <> 'open'" in source
    assert "REVOKE ALL ON ALL FUNCTIONS IN SCHEMA master_control FROM PUBLIC" in source
    assert "SET schema_revision = 11" in source


def test_remote_roles_have_no_owner_superuser_or_server_file_power() -> None:
    bootstrap = (ROOT / "sql/admin/bootstrap_roles.sql").read_text()
    contract = (ROOT / "sql/admin/role_contract.sql").read_text()
    assert "NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS" in bootstrap
    assert "mdh_master_controller" in bootstrap
    assert "mdh_checkpoint" in bootstrap
    assert "GRANT mdh_owner TO mdh_migrator" in contract
    assert "GRANT mdh_owner TO mdh_master_controller" not in contract
    assert "GRANT mdh_owner TO mdh_checkpoint" not in contract
    assert "default_transaction_read_only = on" in contract


def test_control_authoritative_epoch_can_reconcile_failed_attempt_gaps() -> None:
    source = (ROOT / "sql/migrations/0013_control_authoritative_epoch_reconciliation.sql").read_text()
    parse_sql(source)
    assert "requested_epoch <= state.highest_epoch" in source
    assert "requested_epoch <> state.highest_epoch + 1" not in source
