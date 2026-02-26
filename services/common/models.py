from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List


@dataclass
class SourceConfig:
    id: str
    name: str
    type: str
    base_score: float
    rate_limit_rps: float
    feeds: Dict[str, str]


@dataclass
class ClubConfig:
    id: str
    name: str
    enabled: bool
    keywords: List[str]
    sources_alias_key: str


@dataclass
class ArticleCandidate:
    club_id: str
    club_name: str
    source_id: str
    source_name: str
    source_base_score: float
    source_rate_limit_rps: float
    title: str
    url: str
    summary: str
    published_at: datetime
    content_text: str = ""
    snippet: str = ""
    rank_score: float = 0.0
    keyword_hits: List[str] = field(default_factory=list)
    is_opinion: bool = False
