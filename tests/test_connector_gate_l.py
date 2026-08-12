from __future__ import annotations

import json
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker

from my_data_hub.connectors.contracts import ConnectorDurabilityReceipt

ROOT = Path(__file__).resolve().parents[1]


def test_gate_l_migration_uses_one_mode_vocabulary_and_pauses_region_talk() -> None:
    sql = (ROOT / "sql/migrations/0018_connector_durable_delivery.sql").read_text()
    vocabulary = "'push', 'pull', 'artifact_handoff', 'trusted_database_landing'"
    assert sql.count(vocabulary) == 2
    assert "'region-talk-ydb-bloggers-v1'" in sql
    assert "'pull',\n    'paused'" in sql
    assert '"no_live_import": true' in sql
    assert "'region-talk.ydb-bloggers.v1'" in sql
    assert "'internal',\n    false" in sql
    assert "schema_revision = 18" in sql


def test_durability_schema_and_example_match_runtime_contract() -> None:
    schema = json.loads(
        (ROOT / "schemas/connector-durability-receipt.v1.schema.json").read_text()
    )
    example = json.loads(
        (ROOT / "examples/contracts/connector-durability-receipt.v1.example.json").read_text()
    )
    Draft202012Validator(schema, format_checker=FormatChecker()).validate(example)
    parsed = ConnectorDurabilityReceipt.model_validate(example)
    assert parsed.state.value == "DURABLE_COMPLETE"


def test_connector_durability_grants_do_not_expand_mcp_reader() -> None:
    roles = (ROOT / "sql/admin/role_contract.sql").read_text()
    assert "integration.receipt, integration.connector_durability TO mdh_connector_intake" in roles
    assert "integration.quarantine, integration.receipt, integration.connector_durability" in roles
    reader_grant = roles.split("GRANT SELECT ON integration.connector", 2)[-1].split(
        "TO mdh_mcp_reader", 1
    )[0]
    assert "connector_durability" not in reader_grant


def test_live_connector_verifier_requires_durable_complete_not_acceptance() -> None:
    source = (ROOT / "scripts/verify_connector_flow.py").read_text()
    assert "ConnectorDurabilityState.DURABLE_COMPLETE" in source
    assert 'eventual_durability["state"] == "DURABLE_COMPLETE"' in source
    assert '"durable_complete_count"' in source
    assert 'recovery_summary.deferred == 1' in source


def test_ci_checkpoint_fixture_is_explicit_and_never_the_live_default() -> None:
    source = (ROOT / "scripts/verify_connector_flow.py").read_text()
    workflow = (ROOT / ".github/workflows/ci.yml").read_text()
    assert '"--bootstrap-disposable-checkpoint"' in source
    assert "if args.bootstrap_disposable_checkpoint:" in source
    assert "synthetic_disposable_ci" in source
    assert "live_external_gateway_required" in source
    assert "--bootstrap-disposable-checkpoint" in workflow
