from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.eval.calibration import propose_calibration
from app.eval.service import pqs_clv_report, query_eval_dataset
from app.api.eval import eval_dataset, eval_dataset_csv
from app.models import Base, CalibrationRun, Game, Pick, PickScore, PipelineRun


def _seed(session: Session) -> None:
    g = Game(
        sport_key="basketball_nba",
        event_id="evt-1",
        commence_time=datetime.now(timezone.utc) + timedelta(hours=2),
        home_team="h",
        away_team="a",
    )
    session.add(g)
    session.flush()
    base = datetime.now(timezone.utc) - timedelta(days=1)
    for i in range(10):
        pick = Pick(
            game_id=g.id,
            created_at=base + timedelta(minutes=i),
            market_key="h2h",
            side="HOME",
            point=None,
            source="CONSENSUS",
            consensus_prob=Decimal("0.55"),
            best_decimal=Decimal("2.1"),
            best_book="book",
            ev=Decimal("0.02"),
            kelly_fraction=Decimal("0.01"),
            stake=Decimal("1"),
            consensus_books=6,
            sharp_books=1,
            captured_at_min=base,
            captured_at_max=base,
            market_clv=Decimal(str((i - 4) / 1000)),
            book_clv=Decimal(str((i - 5) / 1000)),
            clv_computed_at=base + timedelta(hours=1),
        )
        session.add(pick)
        session.flush()
        session.add(
            PickScore(
                pick_id=pick.id,
                scored_at=base + timedelta(minutes=i),
                version="pqs_v1",
                pqs=Decimal(f"0.{50+i:02d}"),
                components_json={"adaptive_min_pqs": 0.65, "adaptive_max_picks": 3},
                features_json={"book_count": 6},
                decision="KEEP" if i % 3 else "DROP",
                drop_reason=None if i % 3 else "min_books",
            )
        )
    for r in range(6):
        session.add(PipelineRun(run_type="manual", status="ok", sports="nba", markets="h2h", stats_json='{"kept": 8}' if r % 2 else '{"kept": 2}', error=None))


def test_dataset_ordering_and_filters() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        _seed(session)
        session.commit()
        payload = query_eval_dataset(session, start=None, end=None, sport_key="basketball_nba", market_key="h2h", decision=("KEEP", "DROP"), min_n=1, limit=5, offset=0)
        ids = [row["pick_id"] for row in payload["rows"]]
        assert ids == sorted(ids)
        assert payload["n"] == 10


def test_spearman_deterministic() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        _seed(session)
        session.commit()
        r1 = pqs_clv_report(session, min_n=2)
        r2 = pqs_clv_report(session, min_n=2)
        assert r1["spearman"] == r2["spearman"]
        assert len(r1["bins"]) == 5


def test_calibration_bounded_and_deterministic() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        _seed(session)
        session.commit()
        p1 = propose_calibration(session, target_n=10)
        p2 = propose_calibration(session, target_n=10)
        assert p1["patch"] == p2["patch"]
        if "PQS_WEIGHT_EV" in p1["patch"]:
            assert abs(p1["patch"]["PQS_WEIGHT_EV"] - 0.30) <= 0.05
        assert session.query(CalibrationRun).count() == 2


def test_eval_endpoints_filters_stable() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        _seed(session)
        session.commit()
        data = eval_dataset(start=None, end=None, sport_key=None, market_key=None, decision=["KEEP", "DROP"], limit=3, offset=0, min_n=1, db=session)
        assert data["limit"] == 3
        assert len(data["rows"]) == 3
        csv_txt = eval_dataset_csv(start=None, end=None, sport_key=None, market_key=None, decision=["KEEP", "DROP"], limit=2, offset=0, min_n=1, db=session)
        assert csv_txt.splitlines()[0].startswith("pick_id,")
