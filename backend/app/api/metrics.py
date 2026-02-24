from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.db import get_db
from app.services.metrics import compute_clv_health

router = APIRouter(tags=["metrics"])


@router.get("/metrics/clv")
def clv_metrics(days: int = Query(7, ge=1, le=90), db: Session = Depends(get_db)) -> dict[str, object]:
    return compute_clv_health(db, days=days)
