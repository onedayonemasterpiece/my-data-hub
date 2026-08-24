"""Monotonic source and publisher profile merges."""

from __future__ import annotations

import re

from ._canonical import sha256_json
from .models import ProfileMergeResult, PublisherProfile, SourceProfile


def _domain(value: str) -> str:
    raw = value.strip().strip(".").casefold()
    if raw.startswith("www."):
        raw = raw[4:]
    try:
        result = raw.encode("idna").decode("ascii")
    except UnicodeError as exc:
        raise ValueError("publisher_domain_invalid_idna") from exc
    if (
        "." not in result
        or ".." in result
        or not re.fullmatch(r"[a-z0-9.-]+", result)
        or any(
            not part
            or len(part) > 63
            or part.startswith("-")
            or part.endswith("-")
            for part in result.split(".")
        )
    ):
        raise ValueError("publisher_domain_not_canonical")
    return result


def _evidence_union(*groups: tuple[dict, ...]) -> tuple[dict, ...]:
    values: dict[str, dict] = {}
    for group in groups:
        for row in group:
            values.setdefault(sha256_json(row), row)
    return tuple(values[key] for key in sorted(values))


def merge_publisher_profiles(
    existing: PublisherProfile, incoming: PublisherProfile
) -> ProfileMergeResult:
    try:
        identity_ok = bool(
            existing.canonical_source_key == incoming.canonical_source_key
            and existing.publisher_profile_id == incoming.publisher_profile_id
            and _domain(existing.source_domain) == _domain(incoming.source_domain)
        )
    except ValueError as exc:
        return ProfileMergeResult(status="conflict", reason=str(exc))
    if not identity_ok:
        return ProfileMergeResult(status="conflict", reason="publisher_identity_conflict")
    old_scope, new_scope = existing.scope, incoming.scope
    if old_scope == new_scope:
        scope = old_scope
    elif old_scope == "unknown":
        scope = new_scope
    elif new_scope == "unknown":
        scope = old_scope
    else:
        return ProfileMergeResult(
            status="conflict",
            reason=f"publisher_scope_conflict:{old_scope}:{new_scope}",
        )
    # A sidecar dossier is authoritative over a research seed. Within one
    # origin, prefer more populated dimensions, then more evidence.
    def score(row: PublisherProfile) -> tuple[int, int, int]:
        return (
            2 if row.profile_origin == "publisher_profile_sidecar" else 1,
            sum(bool(value) for value in row.profile_dimensions.values()),
            len(row.evidence),
        )

    richer = incoming if score(incoming) > score(existing) else existing
    merged_evidence = _evidence_union(existing.evidence, incoming.evidence)
    merged = richer.model_copy(
        update={
            "scope": scope,
            "evidence": merged_evidence,
            "profile_hash": sha256_json(
                {
                    "identity": richer.canonical_source_key,
                    "scope": scope,
                    "profile_origin": richer.profile_origin,
                    "profile_status": richer.profile_status,
                    "profile_dimensions": richer.profile_dimensions,
                    "evidence": merged_evidence,
                    "copy_projection": richer.copy_projection,
                    "public_copy_eligibility": richer.public_copy_eligibility,
                }
            ),
        }
    )
    return ProfileMergeResult(status="merged", publisher=merged)


def merge_source_profiles(
    existing: SourceProfile, incoming: SourceProfile
) -> ProfileMergeResult:
    if existing.source.canonical_source_key != incoming.source.canonical_source_key:
        return ProfileMergeResult(status="conflict", reason="source_identity_conflict")
    terminal = {"rejected_local", "rejected_spam"}
    if existing.status != incoming.status and (
        (existing.status in terminal and incoming.status == "confirmed_external")
        or (incoming.status in terminal and existing.status == "confirmed_external")
        or (existing.status in terminal and incoming.status in terminal)
    ):
        return ProfileMergeResult(
            status="conflict",
            reason=f"source_locality_conflict:{existing.status}:{incoming.status}",
        )
    rank = {
        "unknown": 0,
        "candidate": 1,
        "confirmed_external": 2,
        "rejected_local": 3,
        "rejected_spam": 3,
    }
    status = max((existing.status, incoming.status), key=rank.__getitem__)
    chosen_source = (
        incoming.source
        if rank[incoming.status] > rank[existing.status]
        else existing.source
    )
    merged = SourceProfile(
        source=chosen_source,
        status=status,
        posts_scanned=max(existing.posts_scanned, incoming.posts_scanned),
        ko_posts_found=max(existing.ko_posts_found, incoming.ko_posts_found),
        candidate_posts_found=max(
            existing.candidate_posts_found, incoming.candidate_posts_found
        ),
        evidence=_evidence_union(existing.evidence, incoming.evidence),
    )
    return ProfileMergeResult(status="merged", source=merged)
