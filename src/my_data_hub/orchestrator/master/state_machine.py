from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class MasterState(StrEnum):
    ABSENT = "ABSENT"
    REQUESTED = "REQUESTED"
    STARTING = "STARTING"
    RESTORING = "RESTORING"
    REGISTERING = "REGISTERING"
    ACTIVE = "ACTIVE"
    DRAINING = "DRAINING"
    CHECKPOINTING = "CHECKPOINTING"
    STOPPED = "STOPPED"
    FAILED = "FAILED"
    FENCED = "FENCED"
    CHECKPOINT_FAILED = "CHECKPOINT_FAILED"
    ORPHANED = "ORPHANED"


class MasterSignal(StrEnum):
    REQUEST = "request"
    DATASET_READY = "dataset_ready"
    SOURCE_PUSHED = "source_pushed"
    RUN_TRIGGERED = "run_triggered"
    SERVICE_READY = "service_ready"
    DRAIN = "drain"
    DRAINED = "drained"
    CHECKPOINT_VERIFIED = "checkpoint_verified"
    CHECKPOINT_FAILED = "checkpoint_failed"
    STOP = "stop"
    FAIL = "fail"
    FENCE = "fence"
    ORPHAN = "orphan"
    RETRY_CHECKPOINT = "retry_checkpoint"


class MasterEffect(StrEnum):
    NONE = "none"
    ENSURE_DATASET = "ensure_dataset"
    PUSH_NOTEBOOK = "push_notebook"
    TRIGGER_RUN = "trigger_run"
    BEGIN_DRAIN = "begin_drain"
    PUBLISH_CHECKPOINT = "publish_checkpoint"
    STOP_RUNTIME = "stop_runtime"


@dataclass(frozen=True, slots=True)
class Transition:
    previous: MasterState
    signal: MasterSignal
    current: MasterState
    next_effect: MasterEffect


class InvalidMasterTransition(ValueError):
    pass


_TRANSITIONS: dict[tuple[MasterState, MasterSignal], tuple[MasterState, MasterEffect]] = {
    (MasterState.ABSENT, MasterSignal.REQUEST): (MasterState.REQUESTED, MasterEffect.ENSURE_DATASET),
    (MasterState.REQUESTED, MasterSignal.DATASET_READY): (MasterState.STARTING, MasterEffect.PUSH_NOTEBOOK),
    (MasterState.STARTING, MasterSignal.SOURCE_PUSHED): (MasterState.RESTORING, MasterEffect.TRIGGER_RUN),
    (MasterState.RESTORING, MasterSignal.RUN_TRIGGERED): (MasterState.REGISTERING, MasterEffect.NONE),
    (MasterState.REGISTERING, MasterSignal.SERVICE_READY): (MasterState.ACTIVE, MasterEffect.NONE),
    (MasterState.ACTIVE, MasterSignal.DRAIN): (MasterState.DRAINING, MasterEffect.BEGIN_DRAIN),
    (MasterState.DRAINING, MasterSignal.DRAINED): (MasterState.CHECKPOINTING, MasterEffect.PUBLISH_CHECKPOINT),
    (MasterState.CHECKPOINTING, MasterSignal.CHECKPOINT_VERIFIED): (MasterState.STOPPED, MasterEffect.STOP_RUNTIME),
    (MasterState.CHECKPOINTING, MasterSignal.CHECKPOINT_FAILED): (
        MasterState.CHECKPOINT_FAILED,
        MasterEffect.NONE,
    ),
    (MasterState.CHECKPOINT_FAILED, MasterSignal.RETRY_CHECKPOINT): (
        MasterState.CHECKPOINTING,
        MasterEffect.PUBLISH_CHECKPOINT,
    ),
}


def transition_master(state: MasterState, signal: MasterSignal) -> Transition:
    if signal == MasterSignal.FENCE and state not in {MasterState.STOPPED, MasterState.FENCED}:
        return Transition(state, signal, MasterState.FENCED, MasterEffect.STOP_RUNTIME)
    if signal == MasterSignal.FAIL and state not in {MasterState.STOPPED, MasterState.FENCED}:
        return Transition(state, signal, MasterState.FAILED, MasterEffect.NONE)
    if signal == MasterSignal.ORPHAN and state not in {MasterState.STOPPED, MasterState.FENCED}:
        return Transition(state, signal, MasterState.ORPHANED, MasterEffect.NONE)
    if signal == MasterSignal.STOP and state in {
        MasterState.REQUESTED,
        MasterState.STARTING,
        MasterState.RESTORING,
        MasterState.REGISTERING,
        MasterState.FAILED,
        MasterState.FENCED,
        MasterState.ORPHANED,
    }:
        return Transition(state, signal, MasterState.STOPPED, MasterEffect.STOP_RUNTIME)
    try:
        current, effect = _TRANSITIONS[(state, signal)]
    except KeyError as exc:
        raise InvalidMasterTransition(f"invalid master transition: {state} + {signal}") from exc
    return Transition(state, signal, current, effect)
