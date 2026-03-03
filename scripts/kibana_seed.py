#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from typing import Any

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CLUBS_PATH = os.path.join(REPO_ROOT, "services", "ingestion", "config", "clubs.yaml")

KIBANA_URL = os.getenv("KIBANA_URL", "http://localhost:5601").rstrip("/")
INDEX_NAME = os.getenv("KIBANA_INDEX", "news_articles")
TIME_FIELD = os.getenv("KIBANA_TIME_FIELD", "published_at")
START_DATE = os.getenv("KIBANA_START_DATE", "2026-02-27T00:00:00Z")


def _read_club_ids() -> list[str]:
    ids: list[str] = []
    try:
        with open(CLUBS_PATH, "r", encoding="utf-8") as handle:
            for line in handle:
                match = re.match(r"^\s*-\s+id:\s*\"?([a-z0-9-]+)\"?\s*$", line)
                if match:
                    ids.append(match.group(1))
    except FileNotFoundError:
        pass
    return ids


def _request(method: str, path: str, body: dict[str, Any] | None = None) -> dict[str, Any]:
    url = f"{KIBANA_URL}{path}"
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(
        url,
        method=method,
        data=data,
        headers={"kbn-xsrf": "true", "content-type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        raw = resp.read().decode("utf-8")
        return json.loads(raw) if raw else {}


def _wait_kibana() -> None:
    for _ in range(30):
        try:
            _request("GET", "/api/status")
            return
        except Exception:
            time.sleep(1)
    raise RuntimeError("Kibana did not become ready in time.")


def _get_or_create_data_view() -> str:
    existing = _request("GET", "/api/data_views")
    for view in existing.get("data_view", []):
        if view.get("title") == INDEX_NAME:
            return view.get("id")

    payload = {
        "data_view": {
            "name": INDEX_NAME,
            "title": INDEX_NAME,
            "timeFieldName": TIME_FIELD,
        }
    }
    try:
        created = _request("POST", "/api/data_views/data_view", payload)
        return created["data_view"]["id"]
    except urllib.error.HTTPError as exc:
        try:
            body = exc.read().decode("utf-8")
            if "Duplicate data view" in body:
                existing = _request("GET", "/api/data_views")
                for view in existing.get("data_view", []):
                    if view.get("title") == INDEX_NAME:
                        return view.get("id")
        except Exception:
            pass
        raise


def _upsert_search(
    search_id: str,
    title: str,
    query: str,
    data_view_id: str,
) -> None:
    search_source = {
        "query": {"language": "kuery", "query": query},
        "filter": [
            {
                "meta": {
                    "type": "time",
                    "key": TIME_FIELD,
                    "disabled": False,
                    "negate": False,
                    "alias": f"desde {START_DATE}",
                },
                "query": {"range": {TIME_FIELD: {"gte": START_DATE}}},
            }
        ],
        "indexRefName": "kibanaSavedObjectMeta.searchSourceJSON.index",
    }
    payload = {
        "attributes": {
            "title": title,
            "description": f"Filtro por clube e data >= {START_DATE}",
            "columns": ["published_at", "club_id", "source_name", "title", "url"],
            "sort": [[TIME_FIELD, "desc"]],
            "kibanaSavedObjectMeta": {"searchSourceJSON": json.dumps(search_source)},
            "timeRestore": True,
            "timeRange": {"from": START_DATE, "to": "now"},
        },
        "references": [
            {
                "type": "index-pattern",
                "id": data_view_id,
                "name": "kibanaSavedObjectMeta.searchSourceJSON.index",
            }
        ],
    }
    _request("POST", f"/api/saved_objects/search/{search_id}?overwrite=true", payload)


def main() -> int:
    _wait_kibana()
    data_view_id = _get_or_create_data_view()

    club_ids = _read_club_ids()
    if not club_ids:
        print("No clubs found in clubs.yaml.", file=sys.stderr)
        return 1

    _upsert_search(
        "search-all-clubs",
        f"Noticias (todas) desde {START_DATE}",
        "",
        data_view_id,
    )

    for club_id in club_ids:
        _upsert_search(
            f"search-club-{club_id}",
            f"Noticias {club_id} desde {START_DATE}",
            f"club_id: {club_id}",
            data_view_id,
        )

    print("Kibana data view and saved searches created/updated.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
