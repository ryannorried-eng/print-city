from dataclasses import dataclass
from functools import lru_cache
import os


@dataclass(frozen=True)
class Settings:
    app_name: str
    app_env: str
    database_url: str
    odds_api_key: str
    odds_api_base_url: str
    odds_sports_whitelist: tuple[str, ...]
    odds_markets: tuple[str, ...]
    odds_regions: str
    bookmaker_whitelist: tuple[str, ...]
    delta_hash_strict: bool


def _csv_env(name: str, default: str = "") -> tuple[str, ...]:
    raw = os.getenv(name, default)
    if not raw.strip():
        return ()
    return tuple(part.strip() for part in raw.split(",") if part.strip())


def _bool_env(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


@lru_cache
def get_settings() -> Settings:
    return Settings(
        app_name=os.getenv("APP_NAME", "print-city-backend"),
        app_env=os.getenv("APP_ENV", "development"),
        database_url=os.getenv(
            "DATABASE_URL",
            "postgresql+psycopg://postgres:postgres@localhost:5432/print_city",
        ),
        odds_api_key=os.getenv("ODDS_API_KEY", ""),
        odds_api_base_url=os.getenv("ODDS_API_BASE_URL", "https://api.the-odds-api.com/v4"),
        odds_sports_whitelist=_csv_env(
            "ODDS_SPORTS_WHITELIST",
            "basketball_nba,basketball_ncaab,icehockey_nhl,americanfootball_ncaaf",
        ),
        odds_markets=_csv_env("ODDS_MARKETS", "h2h,spreads,totals"),
        odds_regions=os.getenv("ODDS_REGIONS", "us"),
        bookmaker_whitelist=_csv_env("BOOKMAKER_WHITELIST", ""),
        delta_hash_strict=_bool_env("DELTA_HASH_STRICT", True),
    )
