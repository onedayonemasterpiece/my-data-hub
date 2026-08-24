"""Pure Region Talk transformation contracts.

This package intentionally performs no network, PostgreSQL, YDB, provider or
publication operation. The master Notebook applies its typed results, while
heavy workers produce separately fenced evidence.
"""

from .candidates import form_candidate_revision
from .eligibility import evaluate_publication_eligibility, image_worker_input_fingerprint
from .evidence import fuse_vector_evidence, vector_evidence_fingerprint
from .merge import merge_publisher_profiles, merge_source_profiles
from .normalization import normalize_external_article, normalize_post, normalize_source
from .planning import build_publication_plan
from .ranking import rank_review_queue

__all__ = [
    "build_publication_plan",
    "evaluate_publication_eligibility",
    "form_candidate_revision",
    "fuse_vector_evidence",
    "image_worker_input_fingerprint",
    "merge_publisher_profiles",
    "merge_source_profiles",
    "normalize_external_article",
    "normalize_post",
    "normalize_source",
    "rank_review_queue",
    "vector_evidence_fingerprint",
]
