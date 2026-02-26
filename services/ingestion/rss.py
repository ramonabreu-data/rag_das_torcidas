from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Iterable, List, Optional

import feedparser
import requests
from bs4 import BeautifulSoup
from dateutil import parser as date_parser

from services.common.models import ArticleCandidate, ClubConfig, SourceConfig
from services.common.settings import Settings
from services.common.utils import clean_text, find_keywords, normalize_text

logger = logging.getLogger("ingestion.rss")

OPINION_TERMS = ["opinião", "opiniao", "coluna", "editorial", "blog", "análise", "analise"]


class RateLimiter:
    def __init__(self) -> None:
        self._last_request: dict[str, float] = {}

    def wait(self, source_id: str, rps: float) -> None:
        if rps <= 0:
            return
        min_interval = 1.0 / rps
        now = time.monotonic()
        last = self._last_request.get(source_id)
        if last is not None:
            elapsed = now - last
            if elapsed < min_interval:
                time.sleep(min_interval - elapsed)
        self._last_request[source_id] = time.monotonic()


def _parse_published(entry: dict) -> datetime:
    if entry.get("published_parsed"):
        return datetime(*entry["published_parsed"][:6], tzinfo=timezone.utc)
    if entry.get("updated_parsed"):
        return datetime(*entry["updated_parsed"][:6], tzinfo=timezone.utc)
    if entry.get("published"):
        return date_parser.parse(entry["published"]).astimezone(timezone.utc)
    return datetime.now(timezone.utc)


def _extract_summary(entry: dict) -> str:
    summary = entry.get("summary") or entry.get("description") or ""
    return clean_text(BeautifulSoup(summary, "html.parser").get_text(" ", strip=True))


def fetch_feed(
    source: SourceConfig,
    feed_url: str,
    settings: Settings,
    rate_limiter: RateLimiter,
    keywords: Optional[Iterable[str]] = None,
) -> List[dict]:
    rate_limiter.wait(source.id, source.rate_limit_rps)
    try:
        headers = {"User-Agent": settings.ingest_user_agent}
        resp = requests.get(feed_url, headers=headers, timeout=settings.request_timeout_seconds)
        resp.raise_for_status()
        parsed = feedparser.parse(resp.content)
    except Exception as exc:  # noqa: BLE001
        logger.warning("feed_parse_failed", extra={"source": source.id, "url": feed_url, "error": str(exc)})
        return []

    if getattr(parsed, "bozo", False):
        logger.warning("feed_bozo", extra={"source": source.id, "url": feed_url})

    entries = parsed.entries or []
    items = []
    for entry in entries:
        title = clean_text(entry.get("title") or "")
        link = entry.get("link") or entry.get("id")
        if not title or not link:
            continue
        summary = _extract_summary(entry)
        if keywords:
            hay = f"{title} {summary}".lower()
            if not any(kw.lower() in hay for kw in keywords):
                continue
        items.append(
            {
                "title": title,
                "url": link,
                "summary": summary,
                "published_at": _parse_published(entry),
            }
        )
    return items


def extract_article_content(url: str, settings: Settings, rate_limiter: RateLimiter, source_id: str, rps: float) -> tuple[str, str]:
    rate_limiter.wait(source_id, rps)
    headers = {"User-Agent": settings.ingest_user_agent}
    try:
        resp = requests.get(url, headers=headers, timeout=settings.request_timeout_seconds)
        resp.raise_for_status()
    except Exception as exc:  # noqa: BLE001
        logger.warning("article_fetch_failed", extra={"url": url, "error": str(exc)})
        return "", ""

    soup = BeautifulSoup(resp.text, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    meta_desc = ""
    meta = soup.find("meta", attrs={"name": "description"})
    if meta and meta.get("content"):
        meta_desc = meta["content"]
    og_desc = soup.find("meta", attrs={"property": "og:description"})
    if og_desc and og_desc.get("content"):
        meta_desc = meta_desc or og_desc["content"]

    paragraphs = [p.get_text(" ", strip=True) for p in soup.find_all("p")]
    text = clean_text(" ".join(paragraphs))
    if settings.ingest_content_max_chars:
        text = text[: settings.ingest_content_max_chars]
    snippet = clean_text(meta_desc) or (text[:300] if text else "")
    return text, snippet


def build_candidates(
    club: ClubConfig,
    sources: List[SourceConfig],
    settings: Settings,
    rate_limiter: RateLimiter,
) -> List[ArticleCandidate]:
    candidates: List[ArticleCandidate] = []
    for source in sources:
        feed_url = source.feeds.get(club.sources_alias_key)
        fallback_url = source.feeds.get("FutebolGeral")

        items: List[dict] = []
        if feed_url:
            items = fetch_feed(source, feed_url, settings, rate_limiter)
            if not items and fallback_url:
                items = fetch_feed(source, fallback_url, settings, rate_limiter, keywords=club.keywords)
        elif fallback_url:
            items = fetch_feed(source, fallback_url, settings, rate_limiter, keywords=club.keywords)

        if not items:
            logger.info("no_feed_items", extra={"source": source.id, "club": club.id})

        for item in items:
            candidates.append(
                ArticleCandidate(
                    club_id=club.id,
                    club_name=club.name,
                    source_id=source.id,
                    source_name=source.name,
                    source_base_score=source.base_score,
                    source_rate_limit_rps=source.rate_limit_rps,
                    title=item["title"],
                    url=item["url"],
                    summary=item["summary"],
                    published_at=item["published_at"],
                )
            )

    return candidates


def enrich_candidates(
    candidates: List[ArticleCandidate],
    settings: Settings,
    rate_limiter: RateLimiter,
    clubs: List[ClubConfig],
) -> None:
    club_keywords = {club.id: club.keywords for club in clubs}
    for candidate in candidates:
        content_text, snippet = extract_article_content(
            candidate.url,
            settings,
            rate_limiter,
            candidate.source_id,
            rps=candidate.source_rate_limit_rps,
        )
        candidate.content_text = content_text
        candidate.snippet = snippet or candidate.summary
        text_blob = f"{candidate.title} {candidate.summary} {candidate.content_text}"
        candidate.keyword_hits = find_keywords(text_blob, club_keywords.get(candidate.club_id, []))
        title_norm = normalize_text(candidate.title)
        candidate.is_opinion = any(term in title_norm for term in OPINION_TERMS)
