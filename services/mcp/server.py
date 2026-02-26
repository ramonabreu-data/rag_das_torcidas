from __future__ import annotations

from typing import Any, Dict, List, Optional

from mcp.server.fastmcp import FastMCP

from services.common.es import get_es_client
from services.common.es_queries import (
    get_article_by_url,
    get_daily_picks as es_get_daily_picks,
    search_news as es_search_news,
)
from services.common.logging import setup_logging
from services.common.settings import Settings

settings = Settings()
setup_logging(settings.log_level)

es = get_es_client(settings)

mcp = FastMCP("torcida-news-rag")


@mcp.tool()
def health() -> Dict[str, Any]:
    try:
        es.ping()
        return {"status": "ok", "elasticsearch": "up"}
    except Exception:  # noqa: BLE001
        return {"status": "degraded", "elasticsearch": "down"}


@mcp.tool()
def search_news(club: str, days: int = 3, query: Optional[str] = None) -> List[Dict[str, Any]]:
    return es_search_news(es, settings, club, query, days, size=10)


@mcp.tool()
def get_daily_picks(club: str, date: str) -> List[Dict[str, Any]]:
    return es_get_daily_picks(es, settings, club, date, size=3)


@mcp.tool()
def get_article(url: str) -> Dict[str, Any]:
    doc = get_article_by_url(es, settings, url)
    return doc or {}


if __name__ == "__main__":
    mcp.run(host=settings.mcp_host, port=settings.mcp_port)
