from __future__ import annotations

import argparse
from datetime import datetime
from zoneinfo import ZoneInfo

from services.common.config import load_clubs, load_sources
from services.common.es import get_es_client
from services.common.logging import setup_logging
from services.common.settings import Settings
from services.ingestion.pipeline import run_daily_picks, run_ingestion


def _today_str(tz_name: str) -> str:
    tz = ZoneInfo(tz_name)
    return datetime.now(tz).date().isoformat()


def main() -> None:
    parser = argparse.ArgumentParser(description="torcida-news-rag ingestion")
    parser.add_argument("--club", help="Club id to ingest (optional)")
    parser.add_argument("--date", help="Date for daily picks (YYYY-MM-DD)")
    parser.add_argument(
        "--mode",
        choices=["ingest", "select", "both"],
        default="both",
        help="Pipeline stage to run",
    )
    args = parser.parse_args()

    settings = Settings()
    setup_logging(settings.log_level)

    sources = load_sources()
    clubs, _ = load_clubs(settings)

    if args.club:
        clubs = [club for club in clubs if club.id == args.club]

    if not clubs:
        raise SystemExit("No clubs enabled for ingestion")

    es = get_es_client(settings)

    if args.mode in ("ingest", "both"):
        run_ingestion(es, settings, clubs, sources)

    if args.mode in ("select", "both"):
        date_str = args.date or _today_str(settings.timezone)
        run_daily_picks(es, settings, clubs, date_str)


if __name__ == "__main__":
    main()
