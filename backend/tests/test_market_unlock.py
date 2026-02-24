from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from fastapi import HTTPException
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.config import get_settings
from app.models import Base, Game, Pick
from app.api.picks import generate_picks
from app.domain.enums import MarketKey
from app.api.system import market_status
from app.services import pipeline
from app.services.market_unlock import allowed_markets


def _insert_game(session: Session, event_id: str = "evt") -> Game:
    game = Game(
        sport_key="basketball_nba",
        event_id=event_id,
        commence_time=datetime(2025, 1, 1, tzinfo=timezone.utc),
        home_team="home",
        away_team="away",
    )
    session.add(game)
    session.flush()
    return game


def _insert_clv_pick(session: Session, game_id: int, idx: int, clv_done: bool) -> None:
    ts = datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc)
    session.add(
        Pick(
            game_id=game_id,
            created_at=ts,
            market_key="h2h",
            side="HOME",
            point=None,
            source="CONSENSUS",
            consensus_prob=Decimal("0.55"),
            best_decimal=Decimal("2.00"),
            best_book=f"book{idx}",
            ev=Decimal("0.02"),
            kelly_fraction=Decimal("0.01"),
            stake=Decimal("10"),
            consensus_books=3,
            sharp_books=1,
            captured_at_min=ts,
            captured_at_max=ts,
            clv_computed_at=ts if clv_done else None,
        )
    )




def test_market_unlock_gate_mode_with_zero_clv(monkeypatch) -> None:
    monkeypatch.setenv("ODDS_SPORTS_WHITELIST", "basketball_nba")
    monkeypatch.setenv("MARKETS_AUTORUN", "h2h,spreads,totals")
    monkeypatch.setenv("MARKETS_UNLOCK_MODE", "gate")
    get_settings.cache_clear()
    settings = get_settings()

    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        game = _insert_game(session)
        _insert_clv_pick(session, game.id, 1, clv_done=False)
        session.commit()

        assert allowed_markets(session, settings) == ["h2h"]

        calls: list[tuple[str, str]] = []
        monkeypatch.setattr(
            pipeline,
            "generate_consensus_picks",
            lambda **kwargs: calls.append((kwargs["sport_key"], kwargs["market_key"])) or {
                "total_views": 0,
                "candidates": 0,
                "inserted": 0,
                "skipped_existing": 0,
                "skipped_low_ev": 0,
                "skipped_insufficient_books": 0,
            },
        )

        stats = pipeline.run_picks(session, settings)

    assert calls == [("basketball_nba", "h2h")]
    assert stats["market_lock"]["used_markets"] == ["h2h"]
    assert sorted(stats["market_lock"]["skipped_markets"]) == ["spreads", "totals"]

    with Session(engine) as session:
        try:
            generate_picks(sport_key="basketball_nba", market_key=MarketKey.SPREADS, db=session)
            raise AssertionError("expected HTTPException")
        except HTTPException as exc:
            assert exc.status_code == 400
            assert "locked until clv_computed_count" in str(exc.detail)


def test_market_unlock_unlocked_at_threshold_and_system_status(monkeypatch) -> None:
    monkeypatch.setenv("ODDS_SPORTS_WHITELIST", "basketball_nba")
    monkeypatch.setenv("MARKETS_AUTORUN", "h2h,spreads,totals")
    monkeypatch.setenv("MARKETS_UNLOCK_MODE", "gate")
    get_settings.cache_clear()
    settings = get_settings()

    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        game = _insert_game(session, event_id="evt-threshold")
        for idx in range(101):
            _insert_clv_pick(session, game.id, idx, clv_done=True)
        session.commit()

        assert allowed_markets(session, settings) == ["h2h", "spreads", "totals"]

        calls: list[tuple[str, str]] = []
        monkeypatch.setattr(
            pipeline,
            "generate_consensus_picks",
            lambda **kwargs: calls.append((kwargs["sport_key"], kwargs["market_key"])) or {
                "total_views": 0,
                "candidates": 0,
                "inserted": 0,
                "skipped_existing": 0,
                "skipped_low_ev": 0,
                "skipped_insufficient_books": 0,
            },
        )

        stats = pipeline.run_picks(session, settings)

    assert calls == [
        ("basketball_nba", "h2h"),
        ("basketball_nba", "spreads"),
        ("basketball_nba", "totals"),
    ]
    assert stats["market_lock"]["skipped_markets"] == []

    with Session(engine) as session:
        payload = market_status(db=session)
    assert payload["spreads_enabled"] is True
    assert payload["totals_enabled"] is True
    assert payload["allowed_markets"] == ["h2h", "spreads", "totals"]


def test_market_unlock_warn_mode_allows_spreads_with_warning(monkeypatch) -> None:
    monkeypatch.setenv("MARKETS_UNLOCK_MODE", "warn")
    get_settings.cache_clear()

    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        game = _insert_game(session, event_id="evt-warn")
        _insert_clv_pick(session, game.id, 1, clv_done=False)
        session.commit()

    with Session(engine) as session:
        payload = generate_picks(sport_key="basketball_nba", market_key=MarketKey.SPREADS, db=session)
    assert "warning" in payload
