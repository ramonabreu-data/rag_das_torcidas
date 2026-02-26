from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, Query
from pydantic import BaseModel, Field
from zoneinfo import ZoneInfo

from services.common.config import load_clubs
from services.common.es import get_es_client
from services.common.es_queries import get_daily_picks, search_news
from services.common.logging import setup_logging
from services.common.settings import Settings

app = FastAPI(title="torcida-news-rag API", version="0.1.0")
settings = Settings()
setup_logging(settings.log_level)

es = get_es_client(settings)


class BriefingRequest(BaseModel):
    club: str
    days: int = Field(default=3, ge=1, le=14)
    topic: Optional[str] = None
    tone: Optional[str] = "neutro"
    wordpress: bool = True


class BriefingResponse(BaseModel):
    resumo: str
    bullets: List[str]
    fontes: List[str]
    draft_structure: Dict[str, Any]


@app.get("/health")
def health() -> Dict[str, Any]:
    try:
        es.ping()
        return {"status": "ok", "elasticsearch": "up"}
    except Exception:  # noqa: BLE001
        return {"status": "degraded", "elasticsearch": "down"}


@app.get("/clubs")
def clubs() -> Dict[str, Any]:
    enabled, optional = load_clubs(settings)
    return {
        "enabled": [c.__dict__ for c in enabled],
        "optional": [c.__dict__ for c in optional],
    }


@app.get("/news/search")
def news_search(
    club: Optional[str] = Query(default=None),
    q: Optional[str] = Query(default=None),
    days: int = Query(default=3, ge=1, le=30),
    size: int = Query(default=10, ge=1, le=50),
) -> List[Dict[str, Any]]:
    return search_news(es, settings, club, q, days, size)


@app.get("/news/daily-picks")
def news_daily_picks(
    club: Optional[str] = Query(default=None),
    date: Optional[str] = Query(default=None),
) -> List[Dict[str, Any]]:
    date_str = date
    if not date_str:
        tz = ZoneInfo(settings.timezone)
        date_str = datetime.now(tz).date().isoformat()
    return get_daily_picks(es, settings, club, date_str, size=3)


@app.post("/rag/briefing", response_model=BriefingResponse)
def rag_briefing(payload: BriefingRequest) -> BriefingResponse:
    results = search_news(es, settings, payload.club, payload.topic, payload.days, size=20)
    if not results:
        return BriefingResponse(resumo="Sem notícias recentes.", bullets=[], fontes=[], draft_structure={})

    fontes: List[str] = []
    bullets = []
    for item in results[:5]:
        if item.get("url"):
            fontes.append(item.get("url"))
        summary = item.get("summary") or item.get("snippet") or ""
        bullets.append(f"{item.get('title')} — {summary[:180]}".strip())

    fontes = list(dict.fromkeys(fontes))
    resumo_parts = [b.split(" — ")[0] for b in bullets[:3]]
    resumo = f"Panorama {payload.club}: " + "; ".join(resumo_parts)

    draft_structure = {
        "page_title": f"{payload.club} — Briefing diário",
        "tone": payload.tone,
        "sections": [
            {"type": "hero", "title": f"Resumo {payload.club}", "content": resumo},
            {"type": "highlights", "title": "Destaques", "items": bullets},
            {"type": "sources", "title": "Fontes", "items": fontes},
        ],
    }

    return BriefingResponse(
        resumo=resumo,
        bullets=bullets,
        fontes=fontes,
        draft_structure=draft_structure,
    )
