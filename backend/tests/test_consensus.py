from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.domain.enums import Side
from app.models import Base, Game, OddsSnapshot
from app.services.consensus import (
    build_market_views,
    compute_consensus_for_view,
    get_latest_group_rows,
)


def _insert_game(session: Session, *, event_id: str) -> Game:
    game = Game(
        sport_key="basketball_nba",
        event_id=event_id,
        commence_time=datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc),
        home_team=f"{event_id}-home",
        away_team=f"{event_id}-away",
    )
    session.add(game)
    session.flush()
    return game


def _add_snapshot(
    session: Session,
    *,
    game_id: int,
    captured_at: datetime,
    market_key: str,
    bookmaker: str,
    side: str,
    point: Decimal | None,
    decimal: Decimal,
    fair_prob: Decimal,
) -> None:
    session.add(
        OddsSnapshot(
            game_id=game_id,
            captured_at=captured_at,
            market_key=market_key,
            bookmaker=bookmaker,
            side=side,
            point=point,
            american=-110,
            decimal=decimal,
            implied_prob=Decimal("0.52"),
            fair_prob=fair_prob,
            group_hash=f"{bookmaker}-{side}-{captured_at.timestamp()}",
        )
    )


def test_consensus_pipeline_h2h_with_latest_per_side_and_best_odds(monkeypatch) -> None:
    monkeypatch.setenv("SHARP_BOOKS", "pinnacle,circa,betonlineag,bovada")
    monkeypatch.setenv("CONSENSUS_MIN_BOOKS", "3")
    monkeypatch.setenv("SHARP_WEIGHT", "2.0")
    monkeypatch.setenv("STANDARD_WEIGHT", "1.0")

    from app.config import get_settings

    get_settings.cache_clear()

    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        g1 = _insert_game(session, event_id="evt_1")
        g2 = _insert_game(session, event_id="evt_2")
        t0 = datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc)

        # Event 1: 4 books, one sharp (pinnacle) pulling consensus toward HOME
        for book, home, away, home_dec, away_dec in [
            ("pinnacle", Decimal("0.62"), Decimal("0.38"), Decimal("2.10"), Decimal("1.95")),
            ("fanduel", Decimal("0.50"), Decimal("0.50"), Decimal("2.00"), Decimal("2.05")),
            ("draftkings", Decimal("0.50"), Decimal("0.50"), Decimal("1.99"), Decimal("2.08")),
            ("circa", Decimal("0.50"), Decimal("0.50"), Decimal("2.02"), Decimal("2.02")),
        ]:
            _add_snapshot(
                session,
                game_id=g1.id,
                captured_at=t0,
                market_key="h2h",
                bookmaker=book,
                side="HOME",
                point=None,
                decimal=home_dec,
                fair_prob=home,
            )
            _add_snapshot(
                session,
                game_id=g1.id,
                captured_at=t0,
                market_key="h2h",
                bookmaker=book,
                side="AWAY",
                point=None,
                decimal=away_dec,
                fair_prob=away,
            )

        # Later update only one side for draftkings to prove latest per (book, side) selection
        _add_snapshot(
            session,
            game_id=g1.id,
            captured_at=t0 + timedelta(minutes=10),
            market_key="h2h",
            bookmaker="draftkings",
            side="HOME",
            point=None,
            decimal=Decimal("2.25"),
            fair_prob=Decimal("0.48"),
        )

        # Event 2: only 2 complete books -> insufficient
        for book in ["fanduel", "betmgm"]:
            _add_snapshot(
                session,
                game_id=g2.id,
                captured_at=t0,
                market_key="h2h",
                bookmaker=book,
                side="HOME",
                point=None,
                decimal=Decimal("1.91"),
                fair_prob=Decimal("0.50"),
            )
            _add_snapshot(
                session,
                game_id=g2.id,
                captured_at=t0,
                market_key="h2h",
                bookmaker=book,
                side="AWAY",
                point=None,
                decimal=Decimal("1.91"),
                fair_prob=Decimal("0.50"),
            )

        session.commit()

        rows = get_latest_group_rows(session, "basketball_nba", "h2h")
        views = build_market_views(rows)

        event1_key = ("evt_1", "h2h", None)
        event2_key = ("evt_2", "h2h", None)

        result1 = compute_consensus_for_view(views[event1_key])
        result2 = compute_consensus_for_view(views[event2_key])

        assert result1.consensus_probs is not None
        assert sum(result1.consensus_probs.values()) == 1.0
        assert all(0.0 <= p <= 1.0 for p in result1.consensus_probs.values())
        # Sharp weighting should pull HOME above simple unweighted average (~0.53)
        assert result1.consensus_probs[Side.HOME] > 0.53

        assert result1.best_decimal[Side.HOME] == 2.25
        assert result1.best_book[Side.HOME] == "draftkings"
        assert result1.best_decimal[Side.AWAY] == 2.08
        assert result1.best_book[Side.AWAY] == "draftkings"
        assert result1.captured_at_min == t0.replace(tzinfo=None)
        assert result1.captured_at_max == (t0 + timedelta(minutes=10)).replace(tzinfo=None)

        assert result2.consensus_probs is None
        assert result2.consensus_reason == "insufficient_books"
        assert result2.included_books == 2


def test_missing_side_excludes_bookmaker_group(monkeypatch) -> None:
    monkeypatch.setenv("CONSENSUS_MIN_BOOKS", "3")
    monkeypatch.setenv("SHARP_BOOKS", "pinnacle")

    from app.config import get_settings

    get_settings.cache_clear()

    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        game = _insert_game(session, event_id="evt_3")
        t0 = datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc)

        # Complete books (3)
        for book in ["pinnacle", "fanduel", "draftkings"]:
            _add_snapshot(
                session,
                game_id=game.id,
                captured_at=t0,
                market_key="totals",
                bookmaker=book,
                side="OVER",
                point=Decimal("210.5"),
                decimal=Decimal("1.95"),
                fair_prob=Decimal("0.51"),
            )
            _add_snapshot(
                session,
                game_id=game.id,
                captured_at=t0,
                market_key="totals",
                bookmaker=book,
                side="UNDER",
                point=Decimal("210.5"),
                decimal=Decimal("1.95"),
                fair_prob=Decimal("0.49"),
            )

        # Incomplete book (only OVER)
        _add_snapshot(
            session,
            game_id=game.id,
            captured_at=t0,
            market_key="totals",
            bookmaker="betmgm",
            side="OVER",
            point=Decimal("210.5"),
            decimal=Decimal("2.01"),
            fair_prob=Decimal("0.55"),
        )

        session.commit()

        rows = get_latest_group_rows(session, "basketball_nba", "totals")
        views = build_market_views(rows)
        result = compute_consensus_for_view(views[("evt_3", "totals", 210.5)])

        assert result.included_books == 3
        assert result.consensus_probs is not None
        # best odds still derived from latest rows in the same view, including incomplete book side
        assert result.best_decimal[Side.OVER] == 2.01
        assert result.best_book[Side.OVER] == "betmgm"
