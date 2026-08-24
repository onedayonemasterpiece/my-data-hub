"""Deterministic MMR ranking for the operator review queue."""

from __future__ import annotations

import math

from .models import RankedReviewCandidate, ReviewCandidate

QUEUE_POLICY_VERSION = "region_talk_mmr_adjacency_v1"


def _cosine(left: ReviewCandidate, right: ReviewCandidate) -> float | None:
    a, b = left.diversity_vector, right.diversity_vector
    if a is None or b is None:
        return None
    if (a.model_id, a.encoder_contract, len(a.values)) != (
        b.model_id,
        b.encoder_contract,
        len(b.values),
    ):
        return None
    an = math.sqrt(sum(value * value for value in a.values))
    bn = math.sqrt(sum(value * value for value in b.values))
    if not math.isfinite(an) or not math.isfinite(bn) or an <= 0 or bn <= 0:
        return None
    return max(
        -1.0,
        min(
            1.0,
            sum(x * y for x, y in zip(a.values, b.values, strict=True)) / (an * bn),
        ),
    )


def _heuristic(left: ReviewCandidate, right: ReviewCandidate) -> float:
    similarity = 0.0
    if left.canonical_source_key == right.canonical_source_key:
        similarity = max(similarity, 0.82)
    if set(left.topics) & set(right.topics):
        similarity = max(similarity, 0.68)
    if left.content_type and left.content_type == right.content_type:
        similarity = max(similarity, 0.35)
    return similarity


def _max_similarity(
    candidate: ReviewCandidate, references: list[ReviewCandidate]
) -> tuple[float, str]:
    if not references:
        return 0.0, "not_applicable"
    best = 0.0
    best_mode = "heuristic_fallback"
    compared = False
    for reference in references:
        if candidate.canonical_url.rstrip("/") == reference.canonical_url.rstrip("/"):
            continue
        compared = True
        value = _cosine(candidate, reference)
        mode = "vector"
        if value is None:
            value = _heuristic(candidate, reference)
            mode = "heuristic_fallback"
        if value > best:
            best, best_mode = value, mode
    return (round(best, 6), best_mode) if compared else (0.0, "not_applicable")


def rank_review_queue(
    candidates: list[ReviewCandidate],
    *,
    history: list[ReviewCandidate] | None = None,
    limit: int = 20,
    diversity_weight: float = 0.28,
    adjacency_threshold: float = 0.86,
) -> list[RankedReviewCandidate]:
    diversity_weight = max(0.0, min(1.0, float(diversity_weight)))
    adjacency_threshold = max(-1.0, min(1.0, float(adjacency_threshold)))
    unique = {row.canonical_url.rstrip("/"): row for row in candidates}
    remaining = [unique[key] for key in sorted(unique)]
    selected: list[RankedReviewCandidate] = []
    history_rows: list[ReviewCandidate] = list(history or [])
    while remaining and len(selected) < max(0, min(limit, len(unique))):
        evaluated: list[
            tuple[bool, float, float, str, ReviewCandidate, float, str]
        ] = []
        for row in remaining:
            history_similarity, history_mode = _max_similarity(row, history_rows)
            selected_similarity, selected_mode = _max_similarity(row, list(selected))
            if history_similarity >= selected_similarity:
                maximum, mode = history_similarity, history_mode
            else:
                maximum, mode = selected_similarity, selected_mode
            previous_similarity = 0.0
            if selected:
                vector = _cosine(row, selected[-1])
                previous_similarity = (
                    _heuristic(row, selected[-1]) if vector is None else vector
                )
            score = row.quality_score - diversity_weight * maximum
            violates = bool(selected and previous_similarity >= adjacency_threshold)
            evaluated.append(
                (
                    violates,
                    -score,
                    -row.quality_score,
                    row.canonical_url,
                    row,
                    maximum,
                    mode,
                )
            )
        safe = [item for item in evaluated if not item[0]]
        pool = safe or evaluated
        chosen = min(pool, key=lambda item: item[:4])
        row = chosen[4]
        selected.append(
            RankedReviewCandidate(
                **row.model_dump(),
                queue_rank=len(selected) + 1,
                rank_score=round(-chosen[1], 6),
                max_similarity=chosen[5],
                diversity_mode=chosen[6],
                adjacency_relaxed=bool(chosen[0] and not safe),
            )
        )
        remaining = [
            item for item in remaining if item.canonical_url != row.canonical_url
        ]
    return selected
