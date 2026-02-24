from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.db import get_db
from app.services.clv import compute_clv_for_date, list_latest_clv

router = APIRouter(tags=["clv"])


@router.post("/clv/compute")
def compute_clv(
    date: str = Query(..., description="UTC date in YYYY-MM-DD format"),
    force: bool = Query(False),
    db: Session = Depends(get_db),
) -> dict[str, int]:
    target_date = datetime.strptime(date, "%Y-%m-%d").date()
    return compute_clv_for_date(session=db, date_utc=target_date, force=force)


@router.get("/clv/latest")
def latest_clv(
    limit: int = Query(50, ge=1, le=500),
    db: Session = Depends(get_db),
) -> list[dict[str, object]]:
    return list_latest_clv(session=db, limit=limit)
