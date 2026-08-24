"""Conservative normalization for Region Talk articles, sources and posts."""

from __future__ import annotations

import hashlib
import re
from urllib.parse import urlsplit

from ._canonical import (
    TransformationContractError,
    canonical_url_identity,
    canonicalize_http_url,
    normalize_doi,
    normalize_exact_text,
    stable_id,
)
from .models import (
    ArticleNormalizationResult,
    ExternalArticleInput,
    NormalizedExternalArticle,
    NormalizedPost,
    NormalizedSource,
    PostInput,
    SourceInput,
)

IMPORT_VERSION = "region_talk_external_publication_import.v1"


def _article_policy_errors(value: ExternalArticleInput) -> list[str]:
    if value.research_decision != "candidate":
        return []
    errors: list[str] = []
    if value.source_scope != "external":
        errors.append("candidate_requires_external_source")
    if value.centrality not in {"central", "substantial"}:
        errors.append("candidate_requires_central_or_substantial_relevance")
    if not (
        value.research_match
        and value.product_policy_match
        and value.language_policy_match
    ):
        errors.append("candidate_requires_all_policy_matches")
    if value.hard_exclusion_codes:
        errors.append("candidate_has_hard_exclusion")
    if value.newsiness != "non_news":
        errors.append("candidate_requires_non_news")
    if value.commerciality not in {
        "independent",
        "institutional_noncommercial",
    }:
        errors.append("candidate_requires_noncommercial_classification")
    if value.date_basis in {"search_snippet", "unknown"}:
        errors.append("candidate_requires_verified_date_basis")
    if value.access_status != "full_text":
        errors.append("candidate_requires_full_text")
    if not value.source_externality_evidence_refs:
        errors.append("candidate_requires_externality_evidence")
    if not value.run_window_start <= value.published_at <= value.run_window_end:
        errors.append("candidate_outside_run_window")
    if value.downstream_readiness != "candidate_report":
        errors.append("candidate_requires_candidate_report_readiness")
    if value.quality_tier not in {"strong", "credible"}:
        errors.append("candidate_requires_strong_or_credible_quality")
    for field in ("kaliningrad_centrality", "public_interest", "accessibility"):
        if getattr(value.quality_scores, field) < 2:
            errors.append(f"candidate_requires_{field}_score_gte_2")
    if value.track == "scholarly":
        if value.peer_reviewed is not True:
            errors.append("scholarly_candidate_requires_peer_review")
        if value.correction_status != "none_found":
            errors.append("scholarly_candidate_requires_clear_correction_status")
    elif value.original_reporting_or_analysis is not True:
        errors.append("editorial_candidate_requires_original_reporting_or_analysis")
    return errors


def normalize_external_article(
    value: ExternalArticleInput,
) -> ArticleNormalizationResult:
    errors = _article_policy_errors(value)
    evidence_ids = {item.evidence_id for item in value.evidence}
    refs = set(value.source_externality_evidence_refs) | set(
        value.copy_support_evidence_refs
    )
    missing = sorted(refs - evidence_ids)
    if missing:
        errors.append("unresolved_evidence_refs:" + ",".join(missing))
    if not value.copy_support_evidence_refs:
        errors.append("grounded_copy_requires_copy_support_evidence")
    if value.media_reuse_allowed and value.rights_policy != "reuse_verified":
        errors.append("media_reuse_requires_verified_rights")
    try:
        url = canonicalize_http_url(value.canonical_url)
        url_identity = canonical_url_identity(url)
        doi = normalize_doi(value.doi)
        evidence = tuple(
            item.model_copy(update={"url": canonicalize_http_url(item.url)})
            for item in value.evidence
        )
        media_urls = tuple(
            sorted({canonicalize_http_url(item) for item in value.media_candidate_urls})
        )
    except TransformationContractError as exc:
        errors.append(str(exc))
        url = url_identity = doi = ""
        evidence = ()
        media_urls = ()
    if errors:
        return ArticleNormalizationResult(
            status="rejected", errors=tuple(sorted(set(errors)))
        )

    normalized_title = normalize_exact_text(value.title)
    normalized_authors = tuple(
        item
        for author in value.authors
        if (item := normalize_exact_text(author))
    )
    identity_keys = {"url:" + url_identity}
    if doi:
        identity_keys.add("doi:" + doi)
    if normalized_title and normalized_authors:
        identity_keys.add(
            "title_authors:"
            + normalized_title
            + "\0"
            + "\0".join(normalized_authors)
        )
    identity = "doi:" + doi if doi else "url:" + url_identity
    scores = value.quality_scores.model_dump()
    quality = round(sum(scores.values()) / (4 * len(scores)), 3)
    import_status = (
        "ready_for_region_talk_scoring"
        if value.research_decision == "candidate"
        and value.downstream_readiness == "candidate_report"
        else "manual_review_required"
        if value.research_decision == "needs_review"
        or value.downstream_readiness == "manual_review_required"
        else "research_only_blocked"
    )
    return ArticleNormalizationResult(
        status="normalized",
        article=NormalizedExternalArticle(
            contract_version=IMPORT_VERSION,
            external_publication_id=stable_id("extpub_", identity),
            canonical_url=url,
            canonical_url_identity=url_identity,
            doi=doi or None,
            identity_keys=tuple(sorted(identity_keys)),
            title=value.title.strip(),
            normalized_title=normalized_title,
            authors=tuple(author.strip() for author in value.authors if author.strip()),
            normalized_authors=normalized_authors,
            source_name=value.source_name.strip(),
            published_at=value.published_at,
            content_origin_type=(
                "academic_publication"
                if value.track == "scholarly"
                else "editorial_publication"
            ),
            normalized_quality_score=quality,
            import_status=import_status,
            evidence=evidence,
            canonical_evidence_urls=tuple(sorted({item.url for item in evidence})),
            rights_policy=value.rights_policy,
            media_reuse_allowed=value.media_reuse_allowed,
            media_use_policy=(
                "reuse_verified"
                if value.media_reuse_allowed
                else "score_only_no_reuse"
            ),
            media_candidate_urls=media_urls,
            source_scope=value.source_scope,
        ),
    )


def normalize_source(value: SourceInput) -> NormalizedSource:
    handle = value.handle.strip().lstrip("@").casefold()
    url = canonicalize_http_url(value.canonical_url) if value.canonical_url else ""
    if value.platform == "telegram":
        if not handle and url:
            parsed = urlsplit(url)
            if parsed.hostname in {"t.me", "telegram.me"}:
                handle = parsed.path.strip("/").split("/", 1)[0].casefold()
        if not re.fullmatch(r"[a-z0-9_]{4,}", handle):
            raise TransformationContractError("telegram source requires canonical handle")
        url = f"https://t.me/{handle}"
        key = "telegram:" + handle
    elif value.platform == "vk":
        if not handle and url:
            handle = urlsplit(url).path.strip("/").split("/", 1)[0].casefold()
        if not handle or not re.fullmatch(r"[a-z0-9_.-]+", handle):
            raise TransformationContractError("vk source requires canonical handle")
        url = f"https://vk.com/{handle}"
        key = "vk:" + handle
    else:
        if not url:
            raise TransformationContractError("web source requires URL")
        host = (urlsplit(url).hostname or "").casefold()
        if host.startswith("www."):
            host = host[4:]
        handle = host
        url = "https://" + host
        key = "web:" + host
    return NormalizedSource(
        contract_version="region-talk.source-normalization.v1",
        canonical_source_key=key,
        platform=value.platform,
        handle=handle,
        canonical_url=url,
        title=" ".join(value.title.split()),
        scope=value.scope,
    )


def normalize_post(value: PostInput) -> NormalizedPost:
    source = normalize_source(value.source)
    canonical_url = canonicalize_http_url(value.canonical_url)
    text = "\n".join(line.rstrip() for line in value.text.strip().splitlines())
    text_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()
    platform_key = (
        f"{source.platform}:{source.handle}:{value.platform_post_id.strip().casefold()}"
    )
    return NormalizedPost(
        contract_version="region-talk.post-normalization.v1",
        post_id=stable_id("rtpost_", platform_key),
        platform_post_key=platform_key,
        canonical_url=canonical_url,
        text=text,
        text_hash=text_hash,
        published_at=value.published_at,
        source=source,
        content_origin_type=value.content_origin_type,
    )
