from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.config import get_settings
from app.core.math import ev_percent, kelly_fraction
from app.domain.enums import Side
from app.models import Base, Game, OddsSnapshot, Pick
from app.services.picks import generate_consensus_picks


def _insert_game(session: Session, *, event_id: str) -> Game:
    game = Game(
        sport_key="basketball_nba",
        event_id=event_id,
        commence_time=datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc),
        home_team="home",
        away_team="away",
    )
    session.add(game)
    session.flush()
    return game


def _add_snapshot(
    session: Session,
    *,
    game_id: int,
    bookmaker: str,
    side: str,
    fair_prob: str,
    decimal: str,
    captured_at: datetime,
) -> None:
    session.add(
        OddsSnapshot(
            game_id=game_id,
            captured_at=captured_at,
            market_key="h2h",
            bookmaker=bookmaker,
            side=side,
            point=None,
            american=-110,
            decimal=Decimal(decimal),
            implied_prob=Decimal("0.50"),
            fair_prob=Decimal(fair_prob),
            group_hash=f"{bookmaker}-{captured_at.isoformat()}-{side}",
        )
    )


def test_generate_picks_ev_kelly_idempotent_and_min_books(monkeypatch) -> None:
    monkeypatch.setenv("CONSENSUS_MIN_BOOKS", "3")
    monkeypatch.setenv("PICK_MIN_BOOKS", "3")
    monkeypatch.setenv("PICK_MIN_EV", "0.015")
    monkeypatch.setenv("BANKROLL_PAPER", "10000")
    monkeypatch.setenv("KELLY_MULTIPLIER", "0.25")
    monkeypatch.setenv("KELLY_MAX_CAP", "0.05")
    monkeypatch.setenv("SHARP_BOOKS", "")
    get_settings.cache_clear()

    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        t0 = datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc)
        g1 = _insert_game(session, event_id="evt_good")
        g2 = _insert_game(session, event_id="evt_low_ev")
        g3 = _insert_game(session, event_id="evt_few_books")

        # g1: HOME should pass threshold, AWAY should fail threshold.
        for book, home_dec, away_dec in [
            ("pinnacle", "2.10", "2.02"),
            ("fanduel", "2.00", "2.00"),
            ("draftkings", "2.02", "1.98"),
        ]:
            _add_snapshot(session, game_id=g1.id, bookmaker=book, side="HOME", fair_prob="0.53", decimal=home_dec, captured_at=t0)
            _add_snapshot(session, game_id=g1.id, bookmaker=book, side="AWAY", fair_prob="0.47", decimal=away_dec, captured_at=t0)

        # g2: both sides too low EV.
        for book in ["booka", "bookb", "bookc"]:
            _add_snapshot(session, game_id=g2.id, bookmaker=book, side="HOME", fair_prob="0.50", decimal="1.98", captured_at=t0)
            _add_snapshot(session, game_id=g2.id, bookmaker=book, side="AWAY", fair_prob="0.50", decimal="1.98", captured_at=t0)

        # g3: insufficient books for picks generation.
        for book in ["bookx", "booky"]:
            _add_snapshot(session, game_id=g3.id, bookmaker=book, side="HOME", fair_prob="0.60", decimal="2.30", captured_at=t0)
            _add_snapshot(session, game_id=g3.id, bookmaker=book, side="AWAY", fair_prob="0.40", decimal="2.10", captured_at=t0)

        session.commit()

        summary1 = generate_consensus_picks(session, sport_key="basketball_nba", market_key="h2h")
        picks = session.query(Pick).order_by(Pick.id.asc()).all()

        assert summary1["total_views"] == 3
        assert summary1["inserted"] == 1
        assert summary1["skipped_low_ev"] == 3
        assert summary1["skipped_insufficient_books"] == 1

        pick = picks[0]
        assert pick.side in {side.value for side in Side}
        assert pick.side == Side.HOME.value
        assert pick.source == "CONSENSUS"

        expected_ev = ev_percent(0.53, 2.10)
        expected_kelly = kelly_fraction(0.53, 2.10, kelly_multiplier=0.25, max_cap=0.05)

        assert float(pick.ev) == round(expected_ev, 8)
        assert float(pick.kelly_fraction) == round(expected_kelly, 8)
        assert float(pick.stake) == round(10000.0 * expected_kelly, 4)

        # Idempotent: same snapshots should not insert duplicate records.
        summary2 = generate_consensus_picks(session, sport_key="basketball_nba", market_key="h2h")
        assert summary2["inserted"] == 0
        assert summary2["skipped_existing"] == 1
        assert session.query(Pick).count() == 1


def test_generate_picks_no_views_returns_empty_summary(monkeypatch) -> None:
    monkeypatch.setenv("PICK_MIN_BOOKS", "3")
    get_settings.cache_clear()

    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        summary = generate_consensus_picks(session, sport_key="basketball_nba", market_key="h2h")
        assert summary == {
            "total_views": 0,
            "candidates": 0,
            "inserted": 0,
            "skipped_existing": 0,
            "skipped_low_ev": 0,
            "skipped_insufficient_books": 0,
        }
