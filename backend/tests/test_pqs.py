from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.config import get_settings
from app.domain.enums import Side
from app.intelligence.features import PickFeatures, build_dispersion_inputs, compute_price_dispersion
from app.intelligence.pqs import score_pick
from app.intelligence.priors import recompute_clv_sport_stats
from app.models import Base, ClvSportStat, Game, OddsSnapshot, Pick
from app.services.picks import _select_final_keep_ids, generate_consensus_picks


def _seed_market(session: Session, event_id: str, commence_delta_min: int = 180) -> None:
    game = Game(
        sport_key="basketball_nba",
        event_id=event_id,
        commence_time=datetime.now(timezone.utc) + timedelta(minutes=commence_delta_min),
        home_team="home",
        away_team="away",
    )
    session.add(game)
    session.flush()
    t0 = datetime.now(timezone.utc)
    for idx, book in enumerate(["pinnacle", "book2", "book3", "book4", "book5", "book6"]):
        session.add(OddsSnapshot(game_id=game.id,captured_at=t0,market_key="h2h",bookmaker=book,side="HOME",point=None,american=-110,decimal=Decimal("2.10"),implied_prob=Decimal("0.50"),fair_prob=Decimal("0.53"),group_hash=f"{event_id}-{idx}-h"))
        session.add(OddsSnapshot(game_id=game.id,captured_at=t0,market_key="h2h",bookmaker=book,side="AWAY",point=None,american=-110,decimal=Decimal("1.90"),implied_prob=Decimal("0.50"),fair_prob=Decimal("0.47"),group_hash=f"{event_id}-{idx}-a"))


def _market_odds(home_decimals: list[float], away_decimals: list[float]) -> dict[str, dict[Side, float]]:
    return {
        f"book{idx}": {Side.HOME: home, Side.AWAY: away}
        for idx, (home, away) in enumerate(zip(home_decimals, away_decimals, strict=True), start=1)
    }


def test_priors_weak_neutral(monkeypatch) -> None:
    monkeypatch.setenv("CLV_MIN_N_FOR_PRIOR", "30")
    monkeypatch.setenv("CLV_PRIOR_WINDOW", "200")
    get_settings.cache_clear()
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        stats = recompute_clv_sport_stats(session, get_settings())
        assert stats["inserted"] == 0


def test_generate_picks_creates_scores(monkeypatch) -> None:
    monkeypatch.setenv("CONSENSUS_MIN_BOOKS", "3")
    monkeypatch.setenv("PICK_MIN_BOOKS", "3")
    monkeypatch.setenv("MIN_BOOKS", "6")
    monkeypatch.setenv("SHARP_BOOKS", "pinnacle")
    monkeypatch.setenv("RUN_MAX_PICKS_TOTAL", "2")
    get_settings.cache_clear()

    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        _seed_market(session, "evt1")
        _seed_market(session, "evt2")
        session.commit()
        summary = generate_consensus_picks(session, "basketball_nba", "h2h")
        assert summary["scored"] >= 2
        assert session.query(Pick).count() >= summary["inserted"]
        assert session.query(ClvSportStat).count() == 0


def test_price_dispersion_tight_market_is_small() -> None:
    odds = _market_odds(
        home_decimals=[2.00, 2.01, 1.99, 2.02, 2.00, 1.98],
        away_decimals=[1.99, 1.98, 2.00, 1.97, 1.99, 2.01],
    )
    odds_by_book, other_side_odds_by_book = build_dispersion_inputs(side=Side.HOME, book_odds=odds)
    dispersion = compute_price_dispersion(odds_by_book=odds_by_book, other_side_odds_by_book=other_side_odds_by_book)
    assert 0.0 <= dispersion < 0.05


def test_price_dispersion_realistic_h2h_market_below_gate() -> None:
    odds = _market_odds(
        home_decimals=[1.86, 1.89, 1.91, 1.88, 1.90, 1.87],
        away_decimals=[2.02, 1.99, 1.97, 2.00, 1.98, 2.01],
    )
    odds_by_book, other_side_odds_by_book = build_dispersion_inputs(side=Side.HOME, book_odds=odds)
    dispersion = compute_price_dispersion(odds_by_book=odds_by_book, other_side_odds_by_book=other_side_odds_by_book)
    assert 0.0 <= dispersion < 0.15


def test_price_dispersion_noisy_market_is_large() -> None:
    odds = _market_odds(
        home_decimals=[1.70, 1.82, 1.95, 2.15, 2.35, 2.60],
        away_decimals=[2.25, 2.10, 1.95, 1.75, 1.60, 1.47],
    )
    odds_by_book, other_side_odds_by_book = build_dispersion_inputs(side=Side.HOME, book_odds=odds)
    dispersion = compute_price_dispersion(odds_by_book=odds_by_book, other_side_odds_by_book=other_side_odds_by_book)
    assert dispersion > 0.15


def test_price_dispersion_is_deterministic() -> None:
    odds = _market_odds(
        home_decimals=[1.88, 1.93, 1.91, 1.95, 1.89, 1.92],
        away_decimals=[2.00, 1.97, 1.99, 1.95, 2.01, 1.98],
    )
    odds_by_book, other_side_odds_by_book = build_dispersion_inputs(side=Side.HOME, book_odds=odds)
    first = compute_price_dispersion(odds_by_book=odds_by_book, other_side_odds_by_book=other_side_odds_by_book)
    second = compute_price_dispersion(odds_by_book=odds_by_book, other_side_odds_by_book=other_side_odds_by_book)
    assert first == second


def test_price_dispersion_extreme_two_way_market_stays_tight_per_side() -> None:
    odds = _market_odds(
        home_decimals=[1.01, 1.02, 1.01],
        away_decimals=[20.00, 19.50, 21.00],
    )

    home_odds, home_other = build_dispersion_inputs(side=Side.HOME, book_odds=odds)
    home_dispersion = compute_price_dispersion(odds_by_book=home_odds, other_side_odds_by_book=home_other)

    away_odds, away_other = build_dispersion_inputs(side=Side.AWAY, book_odds=odds)
    away_dispersion = compute_price_dispersion(odds_by_book=away_odds, other_side_odds_by_book=away_other)

    assert home_dispersion < 0.05
    assert away_dispersion < 0.05


def test_sane_candidate_passes_while_noisy_candidate_drops(monkeypatch) -> None:
    monkeypatch.setenv("MAX_PRICE_DISPERSION", "0.12")
    get_settings.cache_clear()
    settings = get_settings()

    base = dict(
        ev=0.03,
        kelly_fraction=0.01,
        book_count=8,
        sharp_book_count=2,
        agreement_strength=0.9,
        best_vs_consensus_edge=0.02,
        time_to_start_minutes=180.0,
        market_liquidity_proxy=12.0,
    )

    sane = PickFeatures(price_dispersion=0.03, **base)
    noisy = PickFeatures(price_dispersion=0.22, **base)

    sane_result = score_pick(features=sane, settings=settings, prior=None)
    noisy_result = score_pick(features=noisy, settings=settings, prior=None)

    assert sane_result.decision.value in {"KEEP", "WARN"}
    assert noisy_result.decision.value == "DROP"
    assert noisy_result.drop_reason == "max_price_dispersion"


def test_adaptive_dispersion_allows_book_count_8_market(monkeypatch) -> None:
    monkeypatch.delenv("MAX_PRICE_DISPERSION", raising=False)
    monkeypatch.setenv("SPORT_DEFAULT_MIN_PQS", "0.0")
    get_settings.cache_clear()
    settings = get_settings()

    features = PickFeatures(
        ev=0.03,
        kelly_fraction=0.01,
        book_count=8,
        sharp_book_count=1,
        agreement_strength=0.9,
        price_dispersion=0.16,
        best_vs_consensus_edge=0.02,
        time_to_start_minutes=180.0,
        market_liquidity_proxy=12.0,
    )
    result = score_pick(features=features, settings=settings, prior=None)

    assert result.decision.value == "KEEP"


def test_adaptive_dispersion_allows_sharp_high_ev_market(monkeypatch) -> None:
    monkeypatch.setenv("SPORT_DEFAULT_MIN_PQS", "0.0")
    get_settings.cache_clear()
    settings = get_settings()

    features = PickFeatures(
        ev=0.06,
        kelly_fraction=0.01,
        book_count=8,
        sharp_book_count=2,
        agreement_strength=0.9,
        price_dispersion=0.24,
        best_vs_consensus_edge=0.02,
        time_to_start_minutes=180.0,
        market_liquidity_proxy=12.0,
    )
    result = score_pick(features=features, settings=settings, prior=None)

    assert result.decision.value == "KEEP"


def test_adaptive_dispersion_hard_ceiling_always_drops(monkeypatch) -> None:
    monkeypatch.setenv("SPORT_DEFAULT_MIN_PQS", "0.0")
    get_settings.cache_clear()
    settings = get_settings()

    features = PickFeatures(
        ev=0.10,
        kelly_fraction=0.01,
        book_count=12,
        sharp_book_count=3,
        agreement_strength=0.9,
        price_dispersion=0.31,
        best_vs_consensus_edge=0.02,
        time_to_start_minutes=180.0,
        market_liquidity_proxy=12.0,
    )
    result = score_pick(features=features, settings=settings, prior=None)

    assert result.decision.value == "DROP"
    assert result.drop_reason == "max_price_dispersion"


def test_adaptive_min_minutes_to_start_relaxes_for_tight_well_covered_market(monkeypatch) -> None:
    monkeypatch.setenv("SPORT_DEFAULT_MIN_PQS", "0.0")
    get_settings.cache_clear()
    settings = get_settings()

    features = PickFeatures(
        ev=0.03,
        kelly_fraction=0.01,
        book_count=8,
        sharp_book_count=1,
        agreement_strength=0.9,
        price_dispersion=0.05,
        best_vs_consensus_edge=0.02,
        time_to_start_minutes=20.0,
        market_liquidity_proxy=12.0,
    )
    result = score_pick(features=features, settings=settings, prior=None)

    assert result.decision.value == "KEEP"


def test_cap_throttle_deterministically_keeps_top_n_by_pqs() -> None:
    now = datetime(2025, 1, 1, tzinfo=timezone.utc)
    kept_candidates = [
        (0.91, "basketball_ncaab", "h2h", "evt-1", "HOME", now, 101, 2),
        (0.85, "basketball_ncaab", "h2h", "evt-2", "HOME", now, 102, 2),
        (0.85, "basketball_ncaab", "h2h", "evt-2", "AWAY", now, 103, 2),
        (0.70, "basketball_ncaab", "h2h", "evt-3", "HOME", now, 104, 2),
    ]

    final_keep_ids = _select_final_keep_ids(kept_candidates, run_max_picks_total=10)

    assert final_keep_ids == {101, 102}
