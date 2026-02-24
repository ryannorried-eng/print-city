from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.config import get_settings
from app.models import Base, Game, OddsSnapshot, Pick
from app.services.clv import compute_clv_for_date, compute_pick_clv


def _insert_game(session: Session, *, event_id: str, sport_key: str = "basketball_nba") -> Game:
    game = Game(
        sport_key=sport_key,
        event_id=event_id,
        commence_time=datetime(2025, 1, 2, 0, 0, tzinfo=timezone.utc),
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
    captured_at: datetime,
    market_key: str,
    bookmaker: str,
    side: str,
    fair_prob: Decimal,
    decimal: Decimal,
    point: Decimal | None = None,
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
            implied_prob=Decimal("0.50"),
            fair_prob=fair_prob,
            group_hash=f"{bookmaker}-{captured_at.timestamp()}-{side}",
        )
    )


def _insert_pick(session: Session, *, game_id: int, side: str, market_key: str = "h2h", best_book: str = "booka") -> Pick:
    pick = Pick(
        game_id=game_id,
        market_key=market_key,
        side=side,
        point=None,
        source="CONSENSUS",
        consensus_prob=Decimal("0.55000000"),
        best_decimal=Decimal("2.10000"),
        best_book=best_book,
        ev=Decimal("0.01000000"),
        kelly_fraction=Decimal("0.02000000"),
        stake=Decimal("10.0000"),
        consensus_books=2,
        sharp_books=0,
        captured_at_min=datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc),
        captured_at_max=datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc),
    )
    session.add(pick)
    session.flush()
    return pick


def test_closing_selection_uses_latest_before_commence(monkeypatch) -> None:
    monkeypatch.setenv("CONSENSUS_MIN_BOOKS", "2")
    monkeypatch.setenv("SHARP_BOOKS", "")
    get_settings.cache_clear()

    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        game = _insert_game(session, event_id="evt1")
        before_close = game.commence_time - timedelta(minutes=1)
        after_start = game.commence_time + timedelta(minutes=1)

        _add_snapshot(session, game_id=game.id, captured_at=before_close, market_key="h2h", bookmaker="booka", side="HOME", fair_prob=Decimal("0.55"), decimal=Decimal("1.95"))
        _add_snapshot(session, game_id=game.id, captured_at=before_close, market_key="h2h", bookmaker="booka", side="AWAY", fair_prob=Decimal("0.45"), decimal=Decimal("1.95"))
        _add_snapshot(session, game_id=game.id, captured_at=before_close, market_key="h2h", bookmaker="bookb", side="HOME", fair_prob=Decimal("0.60"), decimal=Decimal("1.90"))
        _add_snapshot(session, game_id=game.id, captured_at=before_close, market_key="h2h", bookmaker="bookb", side="AWAY", fair_prob=Decimal("0.40"), decimal=Decimal("1.90"))

        _add_snapshot(session, game_id=game.id, captured_at=after_start, market_key="h2h", bookmaker="booka", side="HOME", fair_prob=Decimal("0.90"), decimal=Decimal("1.50"))
        _add_snapshot(session, game_id=game.id, captured_at=after_start, market_key="h2h", bookmaker="booka", side="AWAY", fair_prob=Decimal("0.10"), decimal=Decimal("3.00"))

        pick = _insert_pick(session, game_id=game.id, side="HOME", best_book="booka")
        session.commit()

        assert compute_pick_clv(session, pick) is True
        session.commit()

        assert float(pick.closing_consensus_prob) == 0.575
        assert float(pick.market_clv) == 0.025


def test_book_clv_uses_same_book_only(monkeypatch) -> None:
    monkeypatch.setenv("CONSENSUS_MIN_BOOKS", "2")
    monkeypatch.setenv("SHARP_BOOKS", "")
    get_settings.cache_clear()

    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        game = _insert_game(session, event_id="evt2")
        close_time = game.commence_time - timedelta(minutes=1)

        for side, fair in [("HOME", Decimal("0.55")), ("AWAY", Decimal("0.45"))]:
            _add_snapshot(session, game_id=game.id, captured_at=close_time, market_key="h2h", bookmaker="booka", side=side, fair_prob=fair, decimal=Decimal("1.95"))
            _add_snapshot(session, game_id=game.id, captured_at=close_time, market_key="h2h", bookmaker="bookb", side=side, fair_prob=fair, decimal=Decimal("2.20") if side == "HOME" else Decimal("1.70"))

        pick = _insert_pick(session, game_id=game.id, side="HOME", best_book="booka")
        session.commit()

        compute_pick_clv(session, pick)
        session.commit()

        assert float(pick.closing_book_decimal) == 1.95
        assert float(pick.book_clv) > 0


def test_soccer_draw_market_clv(monkeypatch) -> None:
    monkeypatch.setenv("CONSENSUS_MIN_BOOKS", "2")
    monkeypatch.setenv("SHARP_BOOKS", "")
    get_settings.cache_clear()

    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        game = _insert_game(session, event_id="evt3", sport_key="soccer_epl")
        close_time = game.commence_time - timedelta(minutes=1)

        for book, probs in [
            ("booka", {"HOME": Decimal("0.40"), "DRAW": Decimal("0.30"), "AWAY": Decimal("0.30")}),
            ("bookb", {"HOME": Decimal("0.42"), "DRAW": Decimal("0.28"), "AWAY": Decimal("0.30")}),
        ]:
            for side, prob in probs.items():
                _add_snapshot(session, game_id=game.id, captured_at=close_time, market_key="h2h", bookmaker=book, side=side, fair_prob=prob, decimal=Decimal("3.40") if side == "DRAW" else Decimal("2.10"))

        pick = _insert_pick(session, game_id=game.id, side="DRAW", best_book="booka")
        pick.consensus_prob = Decimal("0.25000000")
        pick.best_decimal = Decimal("3.50000")
        session.commit()

        assert compute_pick_clv(session, pick) is True
        session.commit()

        assert float(pick.closing_consensus_prob) == 0.29
        assert float(pick.market_clv) == 0.04


def test_missing_closing_book_still_computes_market_clv(monkeypatch) -> None:
    monkeypatch.setenv("CONSENSUS_MIN_BOOKS", "2")
    monkeypatch.setenv("SHARP_BOOKS", "")
    get_settings.cache_clear()

    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        game = _insert_game(session, event_id="evt4")
        close_time = game.commence_time - timedelta(minutes=1)

        for side, fair in [("HOME", Decimal("0.56")), ("AWAY", Decimal("0.44"))]:
            _add_snapshot(session, game_id=game.id, captured_at=close_time, market_key="h2h", bookmaker="booka", side=side, fair_prob=fair, decimal=Decimal("1.95"))
            _add_snapshot(session, game_id=game.id, captured_at=close_time, market_key="h2h", bookmaker="bookb", side=side, fair_prob=fair, decimal=Decimal("1.90"))

        pick = _insert_pick(session, game_id=game.id, side="HOME", best_book="missing-book")
        session.commit()

        assert compute_pick_clv(session, pick) is True
        session.commit()

        assert pick.book_clv is None
        assert pick.closing_book_decimal is None
        assert pick.market_clv is not None


def test_compute_clv_for_date_idempotency_and_force(monkeypatch) -> None:
    monkeypatch.setenv("CONSENSUS_MIN_BOOKS", "2")
    monkeypatch.setenv("SHARP_BOOKS", "")
    get_settings.cache_clear()

    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        game = _insert_game(session, event_id="evt5")
        close_time = game.commence_time - timedelta(minutes=1)
        for side, fair in [("HOME", Decimal("0.55")), ("AWAY", Decimal("0.45"))]:
            _add_snapshot(session, game_id=game.id, captured_at=close_time, market_key="h2h", bookmaker="booka", side=side, fair_prob=fair, decimal=Decimal("1.95"))
            _add_snapshot(session, game_id=game.id, captured_at=close_time, market_key="h2h", bookmaker="bookb", side=side, fair_prob=fair, decimal=Decimal("1.90"))

        pick = _insert_pick(session, game_id=game.id, side="HOME")
        session.commit()

        summary1 = compute_clv_for_date(session, game.commence_time.date())
        first_computed_at = pick.clv_computed_at
        summary2 = compute_clv_for_date(session, game.commence_time.date())
        summary3 = compute_clv_for_date(session, game.commence_time.date(), force=True)

        assert summary1["updated"] == 1
        assert summary2["skipped_already_computed"] == 1
        assert summary3["updated"] == 1
        assert pick.clv_computed_at is not None
        assert first_computed_at is not None
        assert pick.clv_computed_at >= first_computed_at
