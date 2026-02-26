from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
from typing import Iterable, List, Optional
from urllib.parse import urljoin, urlparse

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


def _parse_datetime(value: str | None) -> datetime:
    if not value:
        return datetime.now(timezone.utc)
    try:
        parsed = date_parser.parse(value)
    except Exception:  # noqa: BLE001
        return datetime.now(timezone.utc)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _is_allowed_url(url: str, base_url: str) -> bool:
    try:
        base_host = urlparse(base_url).netloc
        target_host = urlparse(url).netloc
        if not base_host:
            return True
        return target_host == base_host or target_host.endswith(f".{base_host}")
    except Exception:  # noqa: BLE001
        return False


def _normalize_jsonld_item(item: dict, base_url: str) -> dict | None:
    if not isinstance(item, dict):
        return None
    title = clean_text(item.get("headline") or item.get("name") or "")
    url = item.get("url") or item.get("@id")
    main_entity = item.get("mainEntityOfPage")
    if not url and isinstance(main_entity, dict):
        url = main_entity.get("@id") or main_entity.get("url")
    if not url or not title:
        return None
    url = urljoin(base_url, url)
    summary = clean_text(item.get("description") or "")
    published_at = _parse_datetime(
        item.get("datePublished") or item.get("dateCreated") or item.get("dateModified")
    )
    return {
        "title": title,
        "url": url,
        "summary": summary,
        "published_at": published_at,
    }


def _extract_items_from_jsonld(data: object, base_url: str) -> List[dict]:
    items: List[dict] = []
    if isinstance(data, list):
        for entry in data:
            items.extend(_extract_items_from_jsonld(entry, base_url))
        return items
    if not isinstance(data, dict):
        return items

    if "@graph" in data:
        items.extend(_extract_items_from_jsonld(data["@graph"], base_url))

    data_type = data.get("@type")
    if isinstance(data_type, list):
        if "ItemList" in data_type:
            data_type = "ItemList"
        elif "NewsArticle" in data_type:
            data_type = "NewsArticle"

    if data_type == "ItemList":
        for element in data.get("itemListElement", []) or []:
            if isinstance(element, dict):
                element_item = element.get("item") or element
                normalized = _normalize_jsonld_item(element_item, base_url)
                if normalized:
                    items.append(normalized)
            elif isinstance(element, str):
                url = urljoin(base_url, element)
                if _is_allowed_url(url, base_url):
                    items.append(
                        {
                            "title": "",
                            "url": url,
                            "summary": "",
                            "published_at": datetime.now(timezone.utc),
                        }
                    )
    elif data_type in {"NewsArticle", "ReportageNewsArticle", "Article"}:
        normalized = _normalize_jsonld_item(data, base_url)
        if normalized:
            items.append(normalized)

    return items


def _extract_items_from_html(soup: BeautifulSoup, base_url: str) -> List[dict]:
    items: List[dict] = []
    for article in soup.find_all("article"):
        anchor = article.find("a", href=True)
        if not anchor:
            continue
        title = clean_text(anchor.get_text(" ", strip=True))
        if not title:
            headline = article.find(["h1", "h2", "h3"])
            if headline:
                title = clean_text(headline.get_text(" ", strip=True))
        if not title or len(title) < 12:
            continue
        url = urljoin(base_url, anchor["href"])
        if not _is_allowed_url(url, base_url):
            continue
        summary = ""
        paragraph = article.find("p")
        if paragraph:
            summary = clean_text(paragraph.get_text(" ", strip=True))
        time_tag = article.find("time")
        published_at = _parse_datetime(time_tag.get("datetime")) if time_tag else datetime.now(timezone.utc)
        items.append(
            {
                "title": title,
                "url": url,
                "summary": summary,
                "published_at": published_at,
            }
        )

    if items:
        return items

    for anchor in soup.find_all("a", href=True):
        title = clean_text(anchor.get_text(" ", strip=True))
        if not title or len(title) < 12:
            continue
        url = urljoin(base_url, anchor["href"])
        if not _is_allowed_url(url, base_url):
            continue
        items.append(
            {
                "title": title,
                "url": url,
                "summary": "",
                "published_at": datetime.now(timezone.utc),
            }
        )
    return items


def fetch_page(
    source: SourceConfig,
    page_url: str,
    settings: Settings,
    rate_limiter: RateLimiter,
    keywords: Optional[Iterable[str]] = None,
) -> List[dict]:
    rate_limiter.wait(source.id, source.rate_limit_rps)
    try:
        headers = {"User-Agent": settings.ingest_user_agent}
        resp = requests.get(page_url, headers=headers, timeout=settings.request_timeout_seconds)
        resp.raise_for_status()
    except Exception as exc:  # noqa: BLE001
        logger.warning("page_fetch_failed", extra={"source": source.id, "url": page_url, "error": str(exc)})
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    items: List[dict] = []
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        raw = script.string or script.get_text(strip=True)
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except Exception:  # noqa: BLE001
            continue
        items.extend(_extract_items_from_jsonld(data, page_url))

    items.extend(_extract_items_from_html(soup, page_url))

    seen_urls = set()
    filtered: List[dict] = []
    for item in items:
        url = item.get("url") or ""
        if not url or url in seen_urls:
            continue
        if not _is_allowed_url(url, page_url):
            continue
        seen_urls.add(url)
        title = clean_text(item.get("title") or "")
        if not title:
            continue
        summary = clean_text(item.get("summary") or "")
        if keywords:
            hay = f"{title} {summary}".lower()
            if not any(kw.lower() in hay for kw in keywords):
                continue
        filtered.append(
            {
                "title": title,
                "url": url,
                "summary": summary,
                "published_at": item.get("published_at") or datetime.now(timezone.utc),
            }
        )
        if len(filtered) >= settings.ingest_scrape_max_items:
            break

    return filtered


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
        if source.type == "rss":
            if feed_url:
                items = fetch_feed(source, feed_url, settings, rate_limiter)
                if not items and fallback_url:
                    items = fetch_feed(source, fallback_url, settings, rate_limiter, keywords=club.keywords)
            elif fallback_url:
                items = fetch_feed(source, fallback_url, settings, rate_limiter, keywords=club.keywords)
        elif source.type == "scrape":
            if feed_url:
                items = fetch_page(source, feed_url, settings, rate_limiter)
                if not items and fallback_url:
                    items = fetch_page(source, fallback_url, settings, rate_limiter, keywords=club.keywords)
            elif fallback_url:
                items = fetch_page(source, fallback_url, settings, rate_limiter, keywords=club.keywords)
        else:
            logger.warning("unknown_source_type", extra={"source": source.id, "type": source.type})

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
