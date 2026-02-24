from __future__ import annotations

from datetime import date

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.db import get_db
from app.services.clv import compute_clv_for_date, list_clv_sport_stats, list_latest_clv

router = APIRouter(tags=["clv"])


@router.post("/clv/compute")
def clv_compute(
    date_utc: date = Query(..., description="UTC date (YYYY-MM-DD) for games commence_time"),
    force: bool = Query(False),
    db: Session = Depends(get_db),
) -> dict[str, int]:
    return compute_clv_for_date(db, date_utc=date_utc, force=force)


@router.get("/clv/latest")
def clv_latest(limit: int = Query(50, ge=1, le=500), db: Session = Depends(get_db)) -> list[dict[str, object]]:
    return list_latest_clv(db, limit=limit)


@router.get("/stats/clv/sport")
def clv_sport_stats(limit: int = Query(100, ge=1, le=500), db: Session = Depends(get_db)) -> list[dict[str, object]]:
    return list_clv_sport_stats(db, limit=limit)
