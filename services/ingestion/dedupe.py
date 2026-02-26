from __future__ import annotations

import logging
from typing import List, Tuple

from rapidfuzz import fuzz

from services.common.models import ArticleCandidate
from services.common.utils import normalize_text, sha1_text

logger = logging.getLogger("ingestion.dedupe")


def dedupe_candidates(candidates: List[ArticleCandidate], threshold: int) -> Tuple[List[ArticleCandidate], List[str]]:
    seen_url_hashes: set[str] = set()
    seen_titles: List[str] = []
    kept: List[ArticleCandidate] = []
    skipped: List[str] = []

    for cand in candidates:
        url_hash = sha1_text(cand.url)
        title_norm = normalize_text(cand.title)

        if url_hash in seen_url_hashes or title_norm in seen_titles:
            skipped.append(cand.url)
            continue

        is_similar = False
        for prev in seen_titles:
            if fuzz.ratio(title_norm, prev) >= threshold:
                is_similar = True
                break
        if is_similar:
            skipped.append(cand.url)
            continue

        seen_url_hashes.add(url_hash)
        seen_titles.append(title_norm)
        kept.append(cand)

    if skipped:
        logger.info("dedupe_skipped", extra={"count": len(skipped)})

    return kept, skipped
