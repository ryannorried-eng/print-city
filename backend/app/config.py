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
    sharp_books: tuple[str, ...]
    sharp_weight: float
    standard_weight: float
    consensus_min_books: int
    consensus_eps: float
    pick_min_ev: float
    pick_min_books: int
    pick_max_per_run: int
    bankroll_paper: float
    kelly_multiplier: float
    kelly_max_cap: float
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


def _float_env(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    return float(raw.strip())


def _int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    return int(raw.strip())


@lru_cache
def get_settings() -> Settings:
    # Conservative defaults:
    # - prioritize hit-rate and market-confirmed edges
    # - require wider bookmaker coverage
    # - quarter-Kelly sizing for risk control
    consensus_min_books = _int_env("CONSENSUS_MIN_BOOKS", 5)
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
        sharp_books=_csv_env("SHARP_BOOKS", "pinnacle,circa,betonlineag,bovada"),
        sharp_weight=_float_env("SHARP_WEIGHT", 2.0),
        standard_weight=_float_env("STANDARD_WEIGHT", 1.0),
        consensus_min_books=consensus_min_books,
        consensus_eps=_float_env("CONSENSUS_EPS", 1e-9),
        pick_min_ev=_float_env("PICK_MIN_EV", 0.015),
        pick_min_books=_int_env("PICK_MIN_BOOKS", consensus_min_books),
        pick_max_per_run=_int_env("PICK_MAX_PER_RUN", 50),
        bankroll_paper=_float_env("BANKROLL_PAPER", 10000.0),
        kelly_multiplier=_float_env("KELLY_MULTIPLIER", 0.25),
        kelly_max_cap=_float_env("KELLY_MAX_CAP", 0.05),
        delta_hash_strict=_bool_env("DELTA_HASH_STRICT", True),
    )
