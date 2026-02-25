from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.api.dashboard import dashboard
from app.api.picks import recommended_picks
from app.models import Base, Game, Pick, PickScore


def _seed_db(session: Session) -> None:
    game = Game(
        sport_key="basketball_nba",
        event_id="evt_dash_1",
        commence_time=datetime(2030, 1, 1, tzinfo=timezone.utc),
        home_team="home",
        away_team="away",
    )
    session.add(game)
    session.flush()
    pick = Pick(
        game_id=game.id,
        market_key="h2h",
        side="HOME",
        point=None,
        source="CONSENSUS",
        consensus_prob=Decimal("0.55"),
        best_decimal=Decimal("2.05"),
        best_book="pinnacle",
        ev=Decimal("0.06"),
        kelly_fraction=Decimal("0.01"),
        stake=Decimal("100.00"),
        consensus_books=8,
        sharp_books=2,
        captured_at_min=datetime(2029, 12, 31, 22, 0, tzinfo=timezone.utc),
        captured_at_max=datetime(2029, 12, 31, 22, 5, tzinfo=timezone.utc),
    )
    session.add(pick)
    session.flush()
    session.add(
        PickScore(
            pick_id=pick.id,
            scored_at=datetime(2029, 12, 31, 22, 6, tzinfo=timezone.utc),
            version="pqs_v1",
            pqs=Decimal("0.89"),
            components_json={"adaptive_max_picks": 3},
            features_json={
                "book_count": 8,
                "sharp_book_count": 2,
                "price_dispersion": 0.02,
                "time_to_start_minutes": 380.0,
                "best_vs_consensus_edge": 0.03,
            },
            decision="KEEP",
            drop_reason=None,
        )
    )
    session.commit()


def test_dashboard_returns_html() -> None:
    response = dashboard()

    assert response.status_code == 200
    assert "Recommended Picks" in response.body.decode("utf-8")


def test_recommended_picks_shape() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        _seed_db(session)
        data = recommended_picks(sport_key=None, market_key=None, limit=20, db=session)

    assert isinstance(data, list)
    expected = {
        "pick_id",
        "sport_key",
        "market_key",
        "side",
        "point",
        "created_at",
        "pqs",
        "ev",
        "book_count",
        "sharp_book_count",
        "price_dispersion",
        "time_to_start_minutes",
        "best_vs_consensus_edge",
        "why",
    }
    if data:
        assert expected.issubset(set(data[0].keys()))
