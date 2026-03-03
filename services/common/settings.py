from __future__ import annotations

import os
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=os.getenv("ENV_FILE", ".env"),
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    app_name: str = "torcida-news-rag"
    env: str = "dev"
    timezone: str = Field(default="America/Fortaleza", validation_alias="TZ")
    log_level: str = "INFO"

    elasticsearch_url: str = "http://elasticsearch:9200"
    elastic_username: str | None = None
    elastic_password: str | None = None
    elastic_verify_certs: bool = False
    es_articles_index: str = "news_articles"
    es_chunks_index: str = "news_chunks"
    embeddings_enabled: bool = False
    embedding_dims: int = 768

    ingest_content_max_chars: int = 6000
    ingest_chunk_size: int = 1000
    ingest_chunk_overlap: int = 100
    ingest_days_back: int = 2
    ingest_similarity_threshold: int = 92
    ingest_opinion_penalty: float = 0.20
    ingest_keyword_boost: float = 0.08
    ingest_recency_half_life_hours: int = 36
    ingest_recency_weight: float = 0.60
    ingest_start_date: str | None = None
    ingest_user_agent: str = "torcida-news-rag/0.1 (+https://example.com)"
    request_timeout_seconds: int = 12
    ingest_scrape_max_items: int = 40

    enable_fortaleza: bool = True
    fortaleza_replace_club_id: str = ""

    api_host: str = "0.0.0.0"
    api_port: int = 8000

    mcp_host: str = "0.0.0.0"
    mcp_port: int = 7010
