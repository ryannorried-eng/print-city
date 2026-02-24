from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.db import get_db
from app.domain.enums import MarketKey
from app.services.picks import generate_consensus_picks, list_picks

router = APIRouter(tags=["picks"])


@router.post("/picks/generate")
def generate_picks(
    sport_key: str = Query(...),
    market_key: MarketKey = Query(...),
    db: Session = Depends(get_db),
) -> dict[str, int]:
    return generate_consensus_picks(session=db, sport_key=sport_key, market_key=market_key.value)


@router.get("/picks/latest")
def latest_picks(
    sport_key: str | None = Query(None),
    market_key: MarketKey | None = Query(None),
    date: str | None = Query(None, description="UTC date in YYYY-MM-DD format"),
    limit: int = Query(100, ge=1, le=500),
    db: Session = Depends(get_db),
) -> list[dict[str, object]]:
    return list_picks(
        session=db,
        sport_key=sport_key,
        market_key=market_key.value if market_key is not None else None,
        date=date,
        limit=limit,
    )
