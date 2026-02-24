from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.config import get_settings
from app.db import get_db
from app.intelligence.priors import recompute_clv_sport_stats
from app.services.picks import list_pick_scores

router = APIRouter(tags=["pqs"])


@router.get("/pqs/latest")
def latest_pqs(
    sport_key: str | None = Query(None),
    decision: str | None = Query(None),
    min_pqs: float | None = Query(None),
    version: str | None = Query(None),
    limit: int = Query(100, ge=1, le=500),
    db: Session = Depends(get_db),
) -> list[dict[str, object]]:
    settings = get_settings()
    return list_pick_scores(
        db,
        sport_key=sport_key,
        decision=decision,
        min_pqs=min_pqs,
        version=version or settings.pqs_version,
        limit=limit,
    )


@router.post("/pqs/score")
def pqs_score(db: Session = Depends(get_db)) -> dict[str, object]:
    settings = get_settings()
    return recompute_clv_sport_stats(db, settings)
