from __future__ import annotations

from pathlib import Path
from typing import List, Tuple

import yaml

from services.common.models import ClubConfig, SourceConfig
from services.common.settings import Settings


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def load_sources() -> List[SourceConfig]:
    path = _repo_root() / "services" / "ingestion" / "config" / "sources.yaml"
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    sources = []
    for item in data.get("sources", []):
        sources.append(SourceConfig(**item))
    return sources


def load_clubs(settings: Settings) -> Tuple[List[ClubConfig], List[ClubConfig]]:
    path = _repo_root() / "services" / "ingestion" / "config" / "clubs.yaml"
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    base_clubs = [ClubConfig(**item) for item in data.get("clubs", [])]
    optional = [ClubConfig(**item) for item in data.get("optional_clubs", [])]

    if settings.enable_fortaleza:
        fortaleza = next((c for c in optional if c.id == "fortaleza"), None)
        if fortaleza:
            fortaleza.enabled = True
            for club in base_clubs:
                if club.id == settings.fortaleza_replace_club_id:
                    club.enabled = False
            base_clubs.append(fortaleza)

    enabled = [c for c in base_clubs if c.enabled]
    return enabled, optional
