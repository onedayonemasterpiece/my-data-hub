from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest

from my_data_hub.master_runtime.contracts import GateState, MasterIdentity
from my_data_hub.master_runtime.fencing import EpochFence, FencingError, LeaseWatchdog

NOW = datetime(2026, 8, 10, 12, 0, tzinfo=UTC)
A = MasterIdentity(UUID("11111111-1111-4111-8111-111111111111"), "run-a", 1)
B = MasterIdentity(UUID("22222222-2222-4222-8222-222222222222"), "run-b", 2)


def test_expired_epoch_is_fenced_and_old_credential_never_writes_again() -> None:
    fence = EpochFence()
    fence.acquire(A, lease_until=NOW + timedelta(seconds=30), now=NOW)
    fence.open(A, now=NOW)
    fence.bind("mdh_e1_writer_deadbeef", A, expires_at=NOW + timedelta(seconds=20), now=NOW)
    fence.assert_write("mdh_e1_writer_deadbeef", now=NOW + timedelta(seconds=10))

    assert fence.expire(now=NOW + timedelta(seconds=31)) is True
    assert fence.lease is not None and fence.lease.gate is GateState.FENCED
    with pytest.raises(FencingError, match="write gate"):
        fence.assert_write("mdh_e1_writer_deadbeef", now=NOW + timedelta(seconds=31))

    # Exercise forced rotation while A's credential would otherwise still be valid.
    fence = EpochFence()
    fence.acquire(A, lease_until=NOW + timedelta(minutes=1), now=NOW)
    fence.open(A, now=NOW)
    fence.bind("mdh_e1_writer_deadbeef", A, expires_at=NOW + timedelta(seconds=50), now=NOW)
    fence.fence(A, reason="forced_rotation")
    fence.acquire(B, lease_until=NOW + timedelta(minutes=2), now=NOW + timedelta(seconds=10))
    fence.open(B, now=NOW + timedelta(seconds=10))
    fence.bind(
        "mdh_e2_writer_cafebabe",
        B,
        expires_at=NOW + timedelta(minutes=1),
        now=NOW + timedelta(seconds=10),
    )
    fence.assert_write("mdh_e2_writer_cafebabe", now=NOW + timedelta(seconds=15))
    with pytest.raises(FencingError, match="fenced epoch"):
        fence.assert_write("mdh_e1_writer_deadbeef", now=NOW + timedelta(seconds=15))
    with pytest.raises(FencingError, match="stale master"):
        fence.renew(A, lease_until=NOW + timedelta(minutes=3), now=NOW + timedelta(seconds=15))


def test_unexpired_master_and_stale_epoch_are_rejected_but_control_gap_reconciles() -> None:
    fence = EpochFence()
    fence.acquire(A, lease_until=NOW + timedelta(minutes=1), now=NOW)
    with pytest.raises(FencingError, match="unexpired"):
        fence.acquire(B, lease_until=NOW + timedelta(minutes=2), now=NOW)
    fence.fence(A, reason="rotation")
    wrong = MasterIdentity(B.master_instance_id, B.run_id, 3)
    assert fence.acquire(wrong, lease_until=NOW + timedelta(minutes=2), now=NOW).identity.epoch == 3
    fence.fence(wrong, reason="failed_control_attempt_gap")
    stale = MasterIdentity(UUID("33333333-3333-4333-8333-333333333333"), "stale", 2)
    with pytest.raises(FencingError, match="newer than restored local epoch 3"):
        fence.acquire(stale, lease_until=NOW + timedelta(minutes=3), now=NOW)


def test_drain_closes_gate_before_checkpoint() -> None:
    fence = EpochFence()
    fence.acquire(A, lease_until=NOW + timedelta(minutes=1), now=NOW)
    fence.open(A, now=NOW)
    fence.bind("mdh_e1_writer_deadbeef", A, expires_at=NOW + timedelta(seconds=30), now=NOW)
    lease = fence.drain(A, now=NOW + timedelta(seconds=1))
    assert lease.gate is GateState.DRAINING
    with pytest.raises(FencingError, match="draining"):
        fence.assert_write("mdh_e1_writer_deadbeef", now=NOW + timedelta(seconds=2))


def test_watchdog_closes_on_control_loss_or_before_lease_expiry() -> None:
    reasons: list[str] = []
    watchdog = LeaseWatchdog(
        close_gate=reasons.append,
        safety_margin=timedelta(seconds=10),
        control_timeout=timedelta(seconds=15),
    )
    watchdog.observe_lease(NOW + timedelta(minutes=1))
    watchdog.observe_control(NOW)
    assert watchdog.poll(NOW + timedelta(seconds=14)) is None
    assert watchdog.poll(NOW + timedelta(seconds=15)) == "control_heartbeat_lost"
    assert reasons == ["control_heartbeat_lost"]

    lease_reasons: list[str] = []
    lease_watchdog = LeaseWatchdog(
        close_gate=lease_reasons.append,
        safety_margin=timedelta(seconds=10),
        control_timeout=timedelta(minutes=5),
    )
    lease_watchdog.observe_lease(NOW + timedelta(seconds=20))
    lease_watchdog.observe_control(NOW)
    assert lease_watchdog.poll(NOW + timedelta(seconds=10)) == "lease_safety_margin"
    assert lease_reasons == ["lease_safety_margin"]
