from __future__ import annotations

from datetime import datetime, timezone

import requests

from app.config import get_settings


def fetch_odds(
    sport_key: str,
    markets: list[str],
    regions: str,
    odds_format: str = "american",
) -> tuple[list[dict], dict]:
    settings = get_settings()
    if not settings.odds_api_key:
        raise ValueError("ODDS_API_KEY is required for ingestion endpoints")

    response = requests.get(
        f"{settings.odds_api_base_url}/sports/{sport_key}/odds",
        params={
            "apiKey": settings.odds_api_key,
            "regions": regions,
            "markets": ",".join(markets),
            "oddsFormat": odds_format,
        },
        timeout=20,
    )
    response.raise_for_status()

    fetched_at = datetime.now(timezone.utc)
    quota_headers = {
        key: value
        for key, value in response.headers.items()
        if key.lower().startswith("x-requests-")
    }
    quota_info = {"headers": quota_headers, "fetched_at": fetched_at}
    return response.json(), quota_info
