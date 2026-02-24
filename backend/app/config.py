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
    enable_scheduler: bool
    sched_ingest_interval_sec: int
    sched_picks_interval_sec: int
    sched_clv_interval_sec: int
    sched_jitter_sec: int
    sched_max_concurrent: int
    sports_autorun: str
    markets_autorun: str
    markets_unlock_clv_min: int
    markets_unlock_mode: str
    sched_require_db: bool
    pqs_version: str
    pqs_enabled: bool
    pqs_fail_mode: str
    clv_prior_window: int
    clv_min_n_for_prior: int
    sport_default_min_pqs: float
    sport_default_max_picks: int
    run_max_picks_total: int
    min_books: int
    sharp_book_min: int
    max_price_dispersion: float
    min_agreement: float
    min_minutes_to_start: int
    time_decay_half_life_min: int
    ev_floor: float
    kelly_cap: float
    pqs_weight_ev: float
    pqs_weight_agreement: float
    pqs_weight_dispersion: float
    pqs_weight_coverage: float
    pqs_weight_sharp_presence: float
    pqs_weight_clv_prior: float
    pqs_weight_time_to_start: float


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
        enable_scheduler=_bool_env("ENABLE_SCHEDULER", False),
        sched_ingest_interval_sec=_int_env("SCHED_INGEST_INTERVAL_SEC", 600),
        sched_picks_interval_sec=_int_env("SCHED_PICKS_INTERVAL_SEC", 600),
        sched_clv_interval_sec=_int_env("SCHED_CLV_INTERVAL_SEC", 1800),
        sched_jitter_sec=_int_env("SCHED_JITTER_SEC", 30),
        sched_max_concurrent=_int_env("SCHED_MAX_CONCURRENT", 1),
        sports_autorun=os.getenv("SPORTS_AUTORUN", ""),
        markets_autorun=os.getenv("MARKETS_AUTORUN", "h2h"),
        markets_unlock_clv_min=_int_env("MARKETS_UNLOCK_CLV_MIN", 100),
        markets_unlock_mode=os.getenv("MARKETS_UNLOCK_MODE", "gate").strip().lower() or "gate",
        sched_require_db=_bool_env("SCHED_REQUIRE_DB", True),
        pqs_version=os.getenv("PQS_VERSION", "pqs_v1"),
        pqs_enabled=_bool_env("PQS_ENABLED", True),
        pqs_fail_mode=os.getenv("PQS_FAIL_MODE", "fail").strip().lower() or "fail",
        clv_prior_window=_int_env("CLV_PRIOR_WINDOW", 200),
        clv_min_n_for_prior=_int_env("CLV_MIN_N_FOR_PRIOR", 30),
        sport_default_min_pqs=_float_env("SPORT_DEFAULT_MIN_PQS", 0.65),
        sport_default_max_picks=_int_env("SPORT_DEFAULT_MAX_PICKS", 3),
        run_max_picks_total=_int_env("RUN_MAX_PICKS_TOTAL", 8),
        min_books=_int_env("MIN_BOOKS", 6),
        sharp_book_min=_int_env("SHARP_BOOK_MIN", 1),
        max_price_dispersion=_float_env("MAX_PRICE_DISPERSION", 0.08),
        min_agreement=_float_env("MIN_AGREEMENT", 0.60),
        min_minutes_to_start=_int_env("MIN_MINUTES_TO_START", 15),
        time_decay_half_life_min=_int_env("TIME_DECAY_HALF_LIFE_MIN", 240),
        ev_floor=_float_env("EV_FLOOR", 0.0),
        kelly_cap=_float_env("KELLY_CAP", 0.01),
        pqs_weight_ev=_float_env("PQS_WEIGHT_EV", 0.30),
        pqs_weight_agreement=_float_env("PQS_WEIGHT_AGREEMENT", 0.20),
        pqs_weight_dispersion=_float_env("PQS_WEIGHT_DISPERSION", 0.15),
        pqs_weight_coverage=_float_env("PQS_WEIGHT_COVERAGE", 0.10),
        pqs_weight_sharp_presence=_float_env("PQS_WEIGHT_SHARP_PRESENCE", 0.10),
        pqs_weight_clv_prior=_float_env("PQS_WEIGHT_CLV_PRIOR", 0.10),
        pqs_weight_time_to_start=_float_env("PQS_WEIGHT_TIME_TO_START", 0.05),
    )
