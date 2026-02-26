from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import List

from elasticsearch import Elasticsearch

from services.common.settings import Settings


def _day_bounds(date_str: str, tz_name: str) -> tuple[datetime, datetime]:
    tz = ZoneInfo(tz_name)
    day = datetime.fromisoformat(date_str).replace(tzinfo=tz)
    start = day.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1)
    return start, end


def clear_daily_picks(es: Elasticsearch, settings: Settings, club_id: str, date_str: str) -> None:
    body = {
        "script": "ctx._source.daily_pick=false",
        "query": {
            "bool": {
                "filter": [
                    {"term": {"club_id": club_id}},
                    {"term": {"daily_pick_date": date_str}},
                ]
            }
        },
    }
    es.update_by_query(index=settings.es_articles_index, body=body, conflicts="proceed")


def select_daily_picks(
    es: Elasticsearch,
    settings: Settings,
    club_id: str,
    date_str: str,
    size: int = 3,
) -> List[str]:
    start, end = _day_bounds(date_str, settings.timezone)
    days_back = max(1, settings.ingest_days_back)
    query_start = start - timedelta(days=days_back - 1)

    body = {
        "query": {
            "bool": {
                "filter": [
                    {"term": {"club_id": club_id}},
                    {"range": {"published_at": {"gte": query_start.isoformat(), "lt": end.isoformat()}}},
                ]
            }
        },
        "size": max(10, size * 5),
        "sort": [
            {"rank_score": "desc"},
            {"published_at": "desc"},
        ],
    }

    resp = es.search(index=settings.es_articles_index, body=body)
    hits = resp["hits"]["hits"]
    picks = hits[:size]

    clear_daily_picks(es, settings, club_id, date_str)

    picked_ids = []
    for hit in picks:
        doc_id = hit["_id"]
        es.update(
            index=settings.es_articles_index,
            id=doc_id,
            doc={"daily_pick": True, "daily_pick_date": date_str},
        )
        picked_ids.append(doc_id)

    return picked_ids
