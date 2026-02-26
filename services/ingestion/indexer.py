from __future__ import annotations

from datetime import datetime, timezone
from typing import List

from elasticsearch import Elasticsearch, helpers

from services.common.models import ArticleCandidate
from services.common.settings import Settings
from services.common.utils import chunk_text, normalize_text, sha1_text


def _article_doc(candidate: ArticleCandidate, settings: Settings) -> dict:
    return {
        "club_id": candidate.club_id,
        "club_name": candidate.club_name,
        "title": candidate.title,
        "title_normalized": normalize_text(candidate.title),
        "url": candidate.url,
        "url_hash": sha1_text(candidate.url),
        "source_id": candidate.source_id,
        "source_name": candidate.source_name,
        "summary": candidate.summary,
        "content_text": candidate.content_text,
        "snippet": candidate.snippet or candidate.summary,
        "published_at": candidate.published_at.isoformat(),
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "rank_score": candidate.rank_score,
        "keyword_hits": candidate.keyword_hits,
        "is_opinion": candidate.is_opinion,
    }


def index_articles_and_chunks(
    es: Elasticsearch,
    settings: Settings,
    candidates: List[ArticleCandidate],
) -> None:
    actions = []
    chunk_actions = []

    for candidate in candidates:
        doc_id = sha1_text(candidate.url)
        article_doc = _article_doc(candidate, settings)
        actions.append(
            {
                "_op_type": "update",
                "_index": settings.es_articles_index,
                "_id": doc_id,
                "doc": article_doc,
                "doc_as_upsert": True,
            }
        )

        for idx, chunk in enumerate(
            chunk_text(candidate.content_text, settings.ingest_chunk_size, settings.ingest_chunk_overlap)
        ):
            chunk_id = f"{doc_id}-{idx}"
            chunk_actions.append(
                {
                    "_op_type": "update",
                    "_index": settings.es_chunks_index,
                    "_id": chunk_id,
                    "doc": {
                        "article_id": doc_id,
                        "chunk_id": idx,
                        "club_id": candidate.club_id,
                        "club_name": candidate.club_name,
                        "title": candidate.title,
                        "url": candidate.url,
                        "source_id": candidate.source_id,
                        "source_name": candidate.source_name,
                        "published_at": candidate.published_at.isoformat(),
                        "text": chunk,
                    },
                    "doc_as_upsert": True,
                }
            )

    if actions:
        helpers.bulk(es, actions)
    if chunk_actions:
        helpers.bulk(es, chunk_actions)
