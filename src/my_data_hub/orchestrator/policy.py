from __future__ import annotations

from my_data_hub.orchestrator.models import PlannedAction, RegionTalkBacklog


def _bounded(value: int, maximum: int) -> int:
    return max(0, min(value, maximum))


def plan_region_talk(backlog: RegionTalkBacklog, *, max_actions: int = 8) -> list[PlannedAction]:
    """Preserve Region Talk priorities while reacting to downstream pressure."""
    actions: list[PlannedAction] = []

    def add(stage: str, count: int, reason: str, priority: int) -> None:
        if count > 0 and len(actions) < max_actions:
            actions.append(PlannedAction(stage, count, reason, priority))

    add(
        "reconcile_worker_results",
        _bounded(backlog.completed_worker_results, 500),
        "completed immutable worker outputs must be accepted or quarantined first",
        10,
    )
    add(
        "review_sync",
        _bounded(backlog.review_sync_pending, 100),
        "operator decisions can unblock or close exact candidate revisions",
        12,
    )
    add(
        "publication_plan",
        _bounded(backlog.publication_plan_ready, 50),
        "approved exact revisions should be planned before generating more supply",
        14,
    )
    # Publication dispatch is intentionally omitted from automatic planning. It is a
    # separately enabled side-effect lane after private-canary approval.
    add(
        "review_dispatch",
        _bounded(backlog.review_dispatch_ready, 50),
        "ready candidates should reach the operator before broad discovery",
        16,
    )
    add(
        "writer",
        _bounded(backlog.writer_ready, 50),
        "verified candidates need versioned review copy before delivery",
        18,
    )
    add(
        "exact_url_intake",
        _bounded(backlog.exact_url_pending, 100),
        "manual/exact URLs are the highest-probability bounded intake lane",
        20,
    )
    add(
        "final_verifier",
        _bounded(backlog.final_verifier_ready, 50),
        "media-ready candidates require the single final verifier",
        25,
    )
    if "image" not in backlog.provider_blocked:
        add(
            "image_scoring",
            _bounded(backlog.image_ready, 100),
            "text-eligible candidates should complete media evidence",
            30,
        )
    if "source_profile" not in backlog.provider_blocked:
        add(
            "source_profile",
            _bounded(backlog.source_profile_ready, 100),
            "candidate safety requires a versioned source profile and evidence",
            28,
        )
    add(
        "text_eligibility",
        _bounded(backlog.text_gate_ready, 200),
        "one versioned eligibility contract owns the text decision",
        35,
    )
    add(
        "vector_fusion",
        _bounded(backlog.fusion_ready, 300),
        "paired vector evidence is ready for deterministic fusion",
        40,
    )
    if "bge_m3" not in backlog.provider_blocked:
        add(
            "bge_m3_embedding",
            _bounded(backlog.bge_missing_for_e5, 200),
            "E5 rows waiting for BGE-M3 block downstream eligibility",
            50,
        )
    if "e5" not in backlog.provider_blocked:
        add(
            "e5_embedding",
            _bounded(backlog.e5_due, 200),
            "new exact/discovered posts require first-pass semantic evidence",
            60,
        )

    if backlog.actionable_backlog_growth_cycles < 2:
        add(
            "post_discovery",
            _bounded(backlog.post_discovery_due, 100),
            "downstream backlog is controlled; continue bounded post discovery",
            100,
        )
        add(
            "source_discovery",
            _bounded(backlog.source_discovery_due, 50),
            "remaining capacity may expand the source frontier",
            110,
        )
    return sorted(actions, key=lambda action: (action.priority, action.stage))[:max_actions]
