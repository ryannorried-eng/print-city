from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.config import get_settings
from app.db import get_db
from app.domain.enums import MarketKey
from app.services.market_unlock import enforce_market_allowed
from app.services.picks import generate_consensus_picks, list_picks, list_recommended_picks

router = APIRouter(tags=["picks"])


@router.post("/picks/generate")
def generate_picks(
    sport_key: str = Query(...),
    market_key: MarketKey = Query(...),
    db: Session = Depends(get_db),
) -> dict[str, object]:
    settings = get_settings()
    allowed, reason = enforce_market_allowed(db, settings, market_key.value)
    if not allowed and settings.markets_unlock_mode == "gate":
        assert reason is not None
        raise HTTPException(
            status_code=400,
            detail=(
                f"market '{market_key.value}' locked until clv_computed_count >= {reason['threshold']} "
                f"(currently {reason['clv_computed_count']}). allowed: {reason['allowed_markets']}"
            ),
        )

    result = generate_consensus_picks(session=db, sport_key=sport_key, market_key=market_key.value)
    if not allowed and settings.markets_unlock_mode == "warn":
        assert reason is not None
        return {
            **result,
            "warning": (
                f"market '{market_key.value}' locked until clv_computed_count >= {reason['threshold']} "
                f"(currently {reason['clv_computed_count']}). allowed: {reason['allowed_markets']}"
            ),
        }
    return result


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


@router.get("/picks/recommended")
def recommended_picks(
    sport_key: str | None = Query(None),
    market_key: MarketKey | None = Query(None),
    limit: int = Query(20, ge=1, le=200),
    db: Session = Depends(get_db),
) -> list[dict[str, object]]:
    return list_recommended_picks(
        session=db,
        sport_key=sport_key,
        market_key=market_key.value if market_key is not None else None,
        limit=limit,
    )
