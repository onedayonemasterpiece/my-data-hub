"""Publication plan formation; external publication effects are always disabled."""

from __future__ import annotations

from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from ._canonical import sha256_json
from .models import (
    ApprovedCandidate,
    PublicationPlanRequest,
    PublicationPlanResult,
    PublicationPlanSlot,
)


def _clock(values: tuple[str, ...], lane: str) -> tuple[time | None, str]:
    unique = sorted({value.strip() for value in values if value.strip()})
    if len(unique) != 1:
        return None, f"{lane}_time_policy_ambiguous:" + ",".join(unique)
    try:
        parsed = time.fromisoformat(unique[0])
    except ValueError:
        return None, f"{lane}_time_policy_invalid:{unique[0]}"
    if parsed.second or parsed.microsecond:
        return None, f"{lane}_time_policy_requires_minute_precision"
    return parsed, ""


def _eligible(value: ApprovedCandidate) -> bool:
    return bool(
        value.operator_decision == "approved"
        and value.operator_review_fingerprint
        == value.candidate.current_revision_fingerprint
        and value.candidate.final_verifier_status == "accept"
        and value.candidate.writer_status == "completed"
    )


def build_publication_plan(value: PublicationPlanRequest) -> PublicationPlanResult:
    policy_payload = value.policy.model_dump(mode="json")
    policy_fingerprint = sha256_json(policy_payload)
    article_time, article_error = _clock(value.policy.article_times, "article")
    social_time, social_error = _clock(value.policy.social_times, "social")
    errors = tuple(item for item in (article_error, social_error) if item)
    try:
        zone = ZoneInfo(value.policy.timezone)
    except ZoneInfoNotFoundError:
        errors += ("timezone_unknown:" + value.policy.timezone,)
        zone = None
    if errors or article_time is None or social_time is None or zone is None:
        return PublicationPlanResult(
            contract_version="region-talk.publication-plan.v1",
            status="blocked_policy_ambiguity",
            effects_enabled=False,
            policy_fingerprint=policy_fingerprint,
            reasons=errors or ("publication_policy_incomplete",),
        )

    pools: dict[str, list[ApprovedCandidate]] = {"article": [], "social": []}
    for item in value.candidates:
        if _eligible(item):
            pools[item.candidate.content_lane].append(item)
    for lane in pools:
        pools[lane].sort(
            key=lambda item: (
                item.candidate.queue_rank,
                -item.candidate.rank_score,
                item.candidate.canonical_url,
            )
        )

    slots: list[PublicationPlanSlot] = []
    clocks = {"article": article_time, "social": social_time}
    for offset in range(value.days):
        day = value.start_date + timedelta(days=offset)
        for lane in ("article", "social"):
            if not pools[lane]:
                slots.append(
                    PublicationPlanSlot(
                        plan_date=day,
                        content_lane=lane,
                        status="vacant",
                        scheduled_for=None,
                        dispatch_allowed=False,
                        reason=f"no_current_operator_approved_{lane}_candidate",
                    )
                )
                continue
            chosen = pools[lane].pop(0)
            slots.append(
                PublicationPlanSlot(
                    plan_date=day,
                    content_lane=lane,
                    status="planned",
                    scheduled_for=datetime.combine(day, clocks[lane], zone),
                    candidate_id=chosen.candidate.candidate_id,
                    revision_fingerprint=chosen.candidate.current_revision_fingerprint,
                    dispatch_allowed=False,
                    reason="publication_effect_disabled_pending_separate_owner_approval",
                )
            )
    return PublicationPlanResult(
        contract_version="region-talk.publication-plan.v1",
        status="planned",
        effects_enabled=False,
        policy_fingerprint=policy_fingerprint,
        slots=tuple(slots),
    )
