from __future__ import annotations

import math
from datetime import datetime, timezone

from services.common.models import ArticleCandidate
from services.common.settings import Settings

def _recency_score(published_at: datetime, half_life_hours: int) -> float:
    now = datetime.now(timezone.utc)
    age_hours = max(0.0, (now - published_at).total_seconds() / 3600.0)
    if half_life_hours <= 0:
        return 0.0
    return math.exp(-age_hours / half_life_hours)


def score_candidate(candidate: ArticleCandidate, settings: Settings) -> float:
    recency = _recency_score(candidate.published_at, settings.ingest_recency_half_life_hours)
    keyword_boost = settings.ingest_keyword_boost * len(candidate.keyword_hits)
    penalty = settings.ingest_opinion_penalty if candidate.is_opinion else 0.0

    score = (
        candidate.source_base_score
        + settings.ingest_recency_weight * recency
        + keyword_boost
        - penalty
    )
    candidate.rank_score = round(score, 6)
    return candidate.rank_score
