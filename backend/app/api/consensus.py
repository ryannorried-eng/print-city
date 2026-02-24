from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.db import get_db
from app.domain.enums import MarketKey
from app.services.consensus import (
    ConsensusResult,
    build_market_views,
    compute_consensus_for_view,
    get_latest_group_rows,
)

router = APIRouter(tags=["consensus"])


@router.get("/consensus/latest", response_model=list[ConsensusResult])
def latest_consensus(
    sport_key: str = Query(...), market_key: MarketKey = Query(...), db: Session = Depends(get_db)
) -> list[ConsensusResult]:
    rows = get_latest_group_rows(session=db, sport_key=sport_key, market_key=market_key.value)
    views = build_market_views(rows)
    return [compute_consensus_for_view(views[key]) for key in sorted(views.keys())]
