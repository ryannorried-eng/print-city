from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.config import get_settings
from app.db import get_db
from app.services.market_unlock import allowed_markets, get_clv_computed_count

router = APIRouter(tags=["system"])


@router.get("/system/market_status")
def market_status(db: Session = Depends(get_db)) -> dict[str, object]:
    settings = get_settings()
    allowed = allowed_markets(db, settings)
    allowed_set = set(allowed)
    return {
        "clv_computed_count": get_clv_computed_count(db),
        "threshold": settings.markets_unlock_clv_min,
        "h2h_enabled": "h2h" in allowed_set,
        "spreads_enabled": "spreads" in allowed_set,
        "totals_enabled": "totals" in allowed_set,
        "allowed_markets": allowed,
        "mode": settings.markets_unlock_mode,
    }
