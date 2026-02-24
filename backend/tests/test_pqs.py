from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.config import get_settings
from app.intelligence.priors import recompute_clv_sport_stats
from app.models import Base, ClvSportStat, Game, OddsSnapshot, Pick
from app.services.picks import generate_consensus_picks


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
