from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.config import get_settings
from app.models import Base, Game, Pick, PipelineRun
from app.services import pipeline


def _game(session: Session, event_id: str, commence_time: datetime) -> Game:
    game = Game(
        sport_key="basketball_nba",
        event_id=event_id,
        commence_time=commence_time,
        home_team="home",
        away_team="away",
    )
    session.add(game)
    session.flush()
    return game


def _pick(session: Session, game_id: int, created_at: datetime, clv_done: bool = False) -> Pick:
    pick = Pick(
        game_id=game_id,
        created_at=created_at,
        market_key="h2h",
        side="HOME",
        point=None,
        source="CONSENSUS",
        consensus_prob=Decimal("0.55000000"),
        best_decimal=Decimal("2.00000"),
        best_book="booka",
        ev=Decimal("0.02000000"),
        kelly_fraction=Decimal("0.01000000"),
        stake=Decimal("10.0000"),
        consensus_books=2,
        sharp_books=0,
        captured_at_min=created_at,
        captured_at_max=created_at,
        clv_computed_at=created_at if clv_done else None,
    )
    session.add(pick)
    session.flush()
    return pick


def test_run_cycle_and_logging(monkeypatch) -> None:
    monkeypatch.setenv("ODDS_SPORTS_WHITELIST", "basketball_nba,icehockey_nhl")
    monkeypatch.setenv("MARKETS_AUTORUN", "h2h,totals")
    get_settings.cache_clear()
    settings = get_settings()

    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)

    ingest_calls: list[str] = []
    picks_calls: list[tuple[str, str]] = []

    monkeypatch.setattr(
        pipeline,
        "ingest_odds_for_sport",
        lambda _session, sport: ingest_calls.append(sport) or {"games_upserted": 1, "groups_changed": 1, "snapshot_rows_inserted": 2, "groups_skipped": 0},
    )
    monkeypatch.setattr(
        pipeline,
        "generate_consensus_picks",
        lambda **kwargs: picks_calls.append((kwargs["sport_key"], kwargs["market_key"])) or {"total_views": 1, "candidates": 1, "inserted": 1, "skipped_existing": 0, "skipped_low_ev": 0, "skipped_insufficient_books": 0},
    )
    monkeypatch.setattr(pipeline, "compute_pick_clv", lambda _session, _pick: False)

    with Session(engine) as session:
        summary = pipeline.run_cycle(session, settings)
        runs = session.query(PipelineRun).all()

    assert summary["errors_count"] == 0
    assert ingest_calls == ["basketball_nba", "icehockey_nhl"]
    assert picks_calls == [
        ("basketball_nba", "h2h"),
        ("icehockey_nhl", "h2h"),
    ]
    assert len(runs) == 4
    assert sorted(run.run_type for run in runs) == ["clv", "cycle", "ingest", "picks"]


def test_run_clv_only_due_picks(monkeypatch) -> None:
    get_settings.cache_clear()
    settings = get_settings()
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)

    now = datetime.now(timezone.utc)
    with Session(engine) as session:
        past_game = _game(session, "past", now - timedelta(hours=1))
        future_game = _game(session, "future", now + timedelta(hours=1))
        due_pick = _pick(session, past_game.id, now - timedelta(hours=2), clv_done=False)
        due_pick_id = due_pick.id
        _pick(session, past_game.id, now - timedelta(hours=2), clv_done=True)
        _pick(session, future_game.id, now - timedelta(hours=2), clv_done=False)
        session.commit()

        updated_ids: list[int] = []

        def fake_compute(_session: Session, pick: Pick) -> bool:
            updated_ids.append(pick.id)
            pick.clv_computed_at = datetime.now(timezone.utc)
            return True

        monkeypatch.setattr(pipeline, "compute_pick_clv", fake_compute)

        summary = pipeline.run_clv(session, settings)

    assert summary["processed"] == 1
    assert summary["updated"] == 1
    assert due_pick_id in updated_ids
    assert len(updated_ids) == 1
