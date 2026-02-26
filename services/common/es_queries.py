from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from elasticsearch import Elasticsearch

from services.common.settings import Settings
from services.common.utils import sha1_text


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def search_news(
    es: Elasticsearch,
    settings: Settings,
    club: Optional[str],
    query: Optional[str],
    days: int,
    size: int,
) -> List[Dict[str, Any]]:
    must = []
    filt = []
    if query:
        must.append(
            {
                "multi_match": {
                    "query": query,
                    "fields": ["title^3", "summary^2", "content_text"],
                }
            }
        )
    if club:
        filt.append({"term": {"club_id": club}})
    if days and days > 0:
        since = _now_utc() - timedelta(days=days)
        filt.append({"range": {"published_at": {"gte": since.isoformat()}}})

    body = {
        "query": {"bool": {"must": must or [{"match_all": {}}], "filter": filt}},
        "size": size,
        "sort": [
            {"rank_score": "desc"},
            {"published_at": "desc"},
        ],
    }

    resp = es.search(index=settings.es_articles_index, body=body)
    return [hit["_source"] | {"_id": hit["_id"]} for hit in resp["hits"]["hits"]]


def get_daily_picks(
    es: Elasticsearch,
    settings: Settings,
    club: Optional[str],
    date_str: str,
    size: int,
) -> List[Dict[str, Any]]:
    filt = [
        {"term": {"daily_pick": True}},
        {"term": {"daily_pick_date": date_str}},
    ]
    if club:
        filt.append({"term": {"club_id": club}})

    body = {
        "query": {"bool": {"filter": filt}},
        "size": size,
        "sort": [
            {"rank_score": "desc"},
            {"published_at": "desc"},
        ],
    }

    resp = es.search(index=settings.es_articles_index, body=body)
    return [hit["_source"] | {"_id": hit["_id"]} for hit in resp["hits"]["hits"]]


def get_article_by_url(es: Elasticsearch, settings: Settings, url: str) -> Dict[str, Any] | None:
    doc_id = sha1_text(url)
    if es.exists(index=settings.es_articles_index, id=doc_id):
        resp = es.get(index=settings.es_articles_index, id=doc_id)
        return resp["_source"] | {"_id": resp["_id"]}
    return None
