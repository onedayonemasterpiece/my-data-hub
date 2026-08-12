from __future__ import annotations

import json
import random
import sqlite3
from dataclasses import dataclass
from pathlib import Path

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from my_data_hub.control_plane.ledger import ControlLedger, EffectState
from my_data_hub.orchestrator.master import (
    FakeKaggleRuntime,
    MasterCoordinator,
    MasterIntent,
    MasterSignal,
    MasterState,
    PlannedProviderEffect,
    ReconciliationStatus,
    SimulatedProcessCrash,
    transition_master,
)
from scripts import validate_repository


def _intent(key: str) -> MasterIntent:
    return MasterIntent(
        idempotency_key=key,
        source_identity="my-data-hub/postgres-master",
        source_version="git:0123456789abcdef",
        checkpoint_ref="EMPTY_BASELINE",
        dataset_ref="private/checkpoint-dataset",
        notebook_ref="private/postgres-master",
    )


def _seed_operation(ledger: ControlLedger, request: MasterIntent, *, epoch: int) -> str:
    identity = {**MasterCoordinator.identity_for(request.idempotency_key), "epoch": epoch}
    operation, _ = ledger.ensure_operation(
        operation_id=identity["operation_id"],
        idempotency_key=request.idempotency_key,
        operation_kind="ensure_master",
        intent=request.as_dict(),
        initial_state=MasterState.REQUESTED.value,
        identity=identity,
    )
    ledger.record_attempt(
        attempt_id=identity["attempt_id"],
        run_id=identity["run_id"],
        operation_id=operation.operation_id,
        source_identity=request.source_identity,
        source_version=request.source_version,
        service_instance_id=identity["service_instance_id"],
        master_instance_id=identity["master_instance_id"],
        epoch=epoch,
        state=MasterState.REQUESTED.value,
    )
    return operation.operation_id


def test_reconcile_all_isolates_poison_operation_and_records_failure(tmp_path: Path) -> None:
    ledger = ControlLedger(tmp_path / "ledger.sqlite3")
    poison = _intent("poison-operation")
    healthy = _intent("healthy-operation")
    poison_id = _seed_operation(ledger, poison, epoch=1)
    healthy_id = _seed_operation(ledger, healthy, epoch=2)
    provider = FakeKaggleRuntime({"ensure_dataset": [RuntimeError("poison transport detail")]})
    coordinator = MasterCoordinator(ledger, provider)

    handles = coordinator.reconcile_all(
        {poison.idempotency_key: poison, healthy.idempotency_key: healthy}
    )

    assert [handle.operation_id for handle in handles] == [poison_id, healthy_id]
    poison_operation = ledger.get_operation(poison_id)
    healthy_operation = ledger.get_operation(healthy_id)
    poison_effect = ledger.get_effect_by_idempotency_key(f"{poison_id}:ensure_dataset")
    assert poison_operation is not None and poison_operation.state == MasterState.REQUESTED.value
    assert healthy_operation is not None and healthy_operation.state == MasterState.REGISTERING.value
    assert poison_effect is not None and poison_effect.state == EffectState.IN_PROGRESS
    assert provider.physical_effect_counts == {
        "ensure_dataset": 1,
        "push_notebook": 1,
        "trigger_run": 1,
    }
    with sqlite3.connect(ledger.path) as connection:
        row = connection.execute(
            "SELECT from_state,to_state,metadata_json FROM operation_log "
            "WHERE operation_id=? ORDER BY rowid DESC LIMIT 1",
            (poison_id,),
        ).fetchone()
    assert row is not None and row[:2] == (MasterState.REQUESTED.value, MasterState.REQUESTED.value)
    evidence = json.loads(row[2])
    assert evidence == {
        "code": "MASTER_RECONCILIATION_EXCEPTION",
        "exception_type": "RuntimeError",
        "recovery": "EXACT_EFFECT_RECONCILIATION_REQUIRED",
        "schema_version": "my-data-hub-master-reconciliation-failure.v1",
    }
    assert "poison transport detail" not in row[2]

    # A later pass reconciles exact absence before retrying the mutation. The
    # first pass did not blindly execute the poison effect a second time.
    recovered = coordinator.reconcile_all({poison.idempotency_key: poison})
    assert len(recovered) == 1 and recovered[0].state == MasterState.REGISTERING
    assert provider.physical_effect_counts == {
        "ensure_dataset": 2,
        "push_notebook": 2,
        "trigger_run": 2,
    }


def _write_transport_fixture(root: Path, relative: str, source: str) -> None:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(source, encoding="utf-8")


def test_repository_kaggle_transport_gate_allows_only_central_adapter_and_call_sites(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_transport_fixture(
        tmp_path,
        "src/my_data_hub/providers/kaggle/adapter.py",
        "from kaggle.api.kaggle_api_extended import KaggleApi\n"
        "from kagglesdk.kernels.types.kernels_api_service import ApiGetKernelRequest\n",
    )
    _write_transport_fixture(
        tmp_path,
        "scripts/call_site.py",
        "from my_data_hub.providers.kaggle import KaggleProviderAdapter\n",
    )
    _write_transport_fixture(
        tmp_path,
        "tests/test_sdk_contract.py",
        "from kaggle.api.kaggle_api_extended import KaggleApi\n",
    )
    _write_transport_fixture(
        tmp_path,
        "docs/transport.md",
        "Example only: curl https://www.kaggle.com/api/v1/datasets/list\n",
    )
    monkeypatch.setattr(validate_repository, "ROOT", tmp_path)
    report = validate_repository.Report()

    validate_repository.validate_kaggle_transport(report)

    assert report.errors == []


@pytest.mark.parametrize(
    ("relative", "source"),
    (
        ("scripts/rogue_transport.py", "from kaggle.api.kaggle_api_extended import KaggleApi\n"),
        ("scripts/rogue_transport.py", "import kagglesdk.kernels\n"),
        (
            "scripts/rogue_transport.py",
            "import urllib.request\n"
            "urllib.request.urlopen('https://www.kaggle.com/api/v1/datasets/list')\n",
        ),
        (
            "scripts/rogue_transport.py",
            "import subprocess\nsubprocess.run(['kaggle', 'datasets', 'list'])\n",
        ),
        (
            "scripts/rogue_transport.sh",
            "curl https://www.kaggle.com/api/v1/datasets/list\n",
        ),
        (
            ".github/workflows/rogue.yml",
            "jobs:\n  rogue:\n    steps:\n      - run: kaggle kernels list\n",
        ),
    ),
)
def test_repository_kaggle_transport_gate_rejects_a_second_transport(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    relative: str,
    source: str,
) -> None:
    _write_transport_fixture(
        tmp_path,
        "src/my_data_hub/providers/kaggle/adapter.py",
        "from kaggle.api.kaggle_api_extended import KaggleApi\n",
    )
    _write_transport_fixture(tmp_path, relative, source)
    monkeypatch.setattr(validate_repository, "ROOT", tmp_path)
    report = validate_repository.Report()

    validate_repository.validate_kaggle_transport(report)

    assert report.errors
    assert any(relative in error for error in report.errors)


_EFFECT_SEQUENCE = ("ensure_dataset", "push_notebook", "trigger_run")
_STATE_SIGNAL = {
    MasterState.REQUESTED: MasterSignal.DATASET_READY,
    MasterState.STARTING: MasterSignal.SOURCE_PUSHED,
    MasterState.RESTORING: MasterSignal.RUN_TRIGGERED,
}


@dataclass
class _ModelEffect:
    plan: PlannedProviderEffect
    state: EffectState = EffectState.PLANNED
    receipt: object | None = None


def _planned_effect(kind: str, history: int) -> PlannedProviderEffect:
    return PlannedProviderEffect(
        effect_id=f"{history}:{kind}",
        idempotency_key=f"history-{history}:{kind}",
        effect_kind=kind,
        exact_identity={
            "exact_ref": f"private/{kind}-{history}",
            "source_identity": "my-data-hub/postgres-master",
            "source_version": "git:0123456789abcdef",
        },
    )


@settings(
    max_examples=10_000,
    derandomize=True,
    deadline=None,
    suppress_health_check=(HealthCheck.too_slow,),
)
@given(history=st.integers(min_value=0, max_value=2**32 - 1))
def test_fake_kaggle_property_state_machine_preserves_exactly_once_under_histories(
    history: int,
) -> None:
    """Exercise 10,000 deterministic crash/retry/duplicate/reorder histories."""

    rng = random.Random(history)
    crash_kind = _EFFECT_SEQUENCE[history % len(_EFFECT_SEQUENCE)]
    crash = (
        SimulatedProcessCrash("lost after provider mutation")
        if history & 1
        else RuntimeError("failed before provider mutation")
    )
    provider = FakeKaggleRuntime({crash_kind: [crash]})
    effects = {kind: _ModelEffect(_planned_effect(kind, history)) for kind in _EFFECT_SEQUENCE}
    state = MasterState.REQUESTED
    crash_observed = False
    retry_observed = False
    duplicate_observed = False
    reorder_observed = False

    def expected_kind() -> str | None:
        index = {
            MasterState.REQUESTED: 0,
            MasterState.STARTING: 1,
            MasterState.RESTORING: 2,
        }.get(state)
        return _EFFECT_SEQUENCE[index] if index is not None else None

    def attempt(kind: str) -> None:
        nonlocal state, crash_observed, retry_observed, reorder_observed
        expected = expected_kind()
        if kind != expected:
            reorder_observed = True
            return
        modeled = effects[kind]
        receipt = None
        if modeled.state == EffectState.PLANNED:
            # The state is durable before the provider side effect.
            modeled.state = EffectState.IN_PROGRESS
            try:
                receipt = provider.execute(modeled.plan)
            except RuntimeError:
                crash_observed = True
                return
        elif modeled.state == EffectState.IN_PROGRESS:
            retry_observed = True
            reconciliation = provider.reconcile(modeled.plan)
            if reconciliation.status == ReconciliationStatus.FOUND:
                receipt = reconciliation.receipt
            elif reconciliation.status == ReconciliationStatus.ABSENT:
                receipt = provider.execute(modeled.plan)
            else:  # pragma: no cover - FakeKaggle has no ambiguous script
                return
        else:
            return
        assert receipt is not None
        modeled.receipt = receipt
        modeled.state = EffectState.APPLIED
        state = transition_master(state, _STATE_SIGNAL[state]).current

    # Every history includes a future/reordered delivery before its dependency.
    attempt(_EFFECT_SEQUENCE[1])
    while state != MasterState.REGISTERING:
        current = expected_kind()
        assert current is not None
        # Deterministically interleave more reordered deliveries.
        if rng.randrange(3) == 0:
            attempt(rng.choice(_EFFECT_SEQUENCE))
        attempt(current)

    # Duplicate delivery/execution must return the exact receipt without a
    # second physical mutation, regardless of its reordered arrival time.
    for kind in rng.sample(list(_EFFECT_SEQUENCE), k=len(_EFFECT_SEQUENCE)):
        modeled = effects[kind]
        before = provider.physical_effect_counts[kind]
        replay = provider.execute(modeled.plan)
        assert replay == modeled.receipt
        assert provider.physical_effect_counts[kind] == before
        duplicate_observed = True

    assert crash_observed and retry_observed and duplicate_observed and reorder_observed
    assert state == MasterState.REGISTERING
    assert provider.physical_effect_counts == {
        "ensure_dataset": 1,
        "push_notebook": 1,
        "trigger_run": 1,
    }
