from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, Query
from fastapi.responses import PlainTextResponse
from sqlalchemy.orm import Session

from app.db import get_db
from app.eval.service import dataset_csv, gates_report, pqs_clv_report, query_eval_dataset, sports_report, volume_report

router = APIRouter(tags=["eval"])


@router.get("/eval/dataset")
def eval_dataset(
    start: datetime | None = Query(None),
    end: datetime | None = Query(None),
    sport_key: str | None = Query(None),
    market_key: str | None = Query(None),
    decision: list[str] = Query(["KEEP", "WARN"]),
    min_n: int = Query(1, ge=1),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
) -> dict[str, object]:
    return query_eval_dataset(
        db,
        start=start,
        end=end,
        sport_key=sport_key,
        market_key=market_key,
        decision=tuple(decision),
        min_n=min_n,
        limit=limit,
        offset=offset,
    )


@router.get("/eval/dataset.csv", response_class=PlainTextResponse)
def eval_dataset_csv(
    start: datetime | None = Query(None),
    end: datetime | None = Query(None),
    sport_key: str | None = Query(None),
    market_key: str | None = Query(None),
    decision: list[str] = Query(["KEEP", "WARN"]),
    min_n: int = Query(1, ge=1),
    limit: int = Query(1000, ge=1, le=5000),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
) -> str:
    payload = query_eval_dataset(
        db,
        start=start,
        end=end,
        sport_key=sport_key,
        market_key=market_key,
        decision=tuple(decision),
        min_n=min_n,
        limit=limit,
        offset=offset,
    )
    return dataset_csv(payload)


@router.get("/eval/pqs_clv")
def eval_pqs_clv(min_n: int = Query(20, ge=1), db: Session = Depends(get_db)) -> dict[str, object]:
    return pqs_clv_report(db, min_n=min_n)


@router.get("/eval/gates")
def eval_gates(min_n: int = Query(20, ge=1), db: Session = Depends(get_db)) -> dict[str, object]:
    return gates_report(db, min_n=min_n)


@router.get("/eval/sports")
def eval_sports(min_n: int = Query(20, ge=1), db: Session = Depends(get_db)) -> dict[str, object]:
    return sports_report(db, min_n=min_n)


@router.get("/eval/volume")
def eval_volume(min_n: int = Query(5, ge=1), db: Session = Depends(get_db)) -> dict[str, object]:
    return volume_report(db, min_n=min_n)
