from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

from elasticsearch import Elasticsearch

from services.common.settings import Settings


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def get_es_client(settings: Settings) -> Elasticsearch:
    kwargs: Dict[str, Any] = {
        "hosts": [settings.elasticsearch_url],
        "verify_certs": settings.elastic_verify_certs,
        "request_timeout": 30,
    }
    if settings.elastic_username and settings.elastic_password:
        kwargs["basic_auth"] = (settings.elastic_username, settings.elastic_password)
    return Elasticsearch(**kwargs)


def _load_mapping(name: str) -> Dict[str, Any]:
    path = _repo_root() / "elastic" / "mappings" / f"{name}.json"
    return json.loads(path.read_text(encoding="utf-8"))


def ensure_indices(es: Elasticsearch, settings: Settings) -> None:
    indices = {
        settings.es_articles_index: "news_articles",
        settings.es_chunks_index: "news_chunks",
    }
    for index_name, mapping_name in indices.items():
        if not es.indices.exists(index=index_name):
            mapping = _load_mapping(mapping_name)
            es.indices.create(index=index_name, **mapping)
