from __future__ import annotations

import logging
from typing import List

from elasticsearch import Elasticsearch

from services.common.es import ensure_indices
from services.common.models import ClubConfig, SourceConfig
from services.common.settings import Settings
from services.ingestion.dedupe import dedupe_candidates
from services.ingestion.indexer import index_articles_and_chunks
from services.ingestion.ranker import score_candidate
from services.ingestion.rss import RateLimiter, build_candidates, enrich_candidates
from services.ingestion.selector import select_daily_picks

logger = logging.getLogger("ingestion.pipeline")


def ingest_club(
    es: Elasticsearch,
    settings: Settings,
    club: ClubConfig,
    sources: List[SourceConfig],
    rate_limiter: RateLimiter,
) -> int:
    candidates = build_candidates(club, sources, settings, rate_limiter)
    if not candidates:
        return 0

    candidates, _ = dedupe_candidates(candidates, settings.ingest_similarity_threshold)
    if not candidates:
        return 0

    enrich_candidates(candidates, settings, rate_limiter, [club])
    for candidate in candidates:
        score_candidate(candidate, settings)

    candidates.sort(key=lambda c: c.rank_score, reverse=True)
    index_articles_and_chunks(es, settings, candidates)
    logger.info("club_ingested", extra={"club": club.id, "count": len(candidates)})
    return len(candidates)


def run_ingestion(
    es: Elasticsearch,
    settings: Settings,
    clubs: List[ClubConfig],
    sources: List[SourceConfig],
) -> None:
    ensure_indices(es, settings)
    rate_limiter = RateLimiter()
    for club in clubs:
        ingest_club(es, settings, club, sources, rate_limiter)


def run_daily_picks(
    es: Elasticsearch,
    settings: Settings,
    clubs: List[ClubConfig],
    date_str: str,
) -> None:
    ensure_indices(es, settings)
    for club in clubs:
        picked = select_daily_picks(es, settings, club.id, date_str)
        logger.info("daily_picks_selected", extra={"club": club.id, "picked": len(picked)})
