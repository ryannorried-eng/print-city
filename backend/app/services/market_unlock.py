from __future__ import annotations

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.config import Settings
from app.models import Pick


def get_clv_computed_count(session: Session) -> int:
    stmt = select(func.count()).select_from(Pick).where(Pick.clv_computed_at.is_not(None))
    return int(session.execute(stmt).scalar_one())


def allowed_markets(session: Session, settings: Settings) -> list[str]:
    clv_count = get_clv_computed_count(session)
    if clv_count < settings.markets_unlock_clv_min:
        return ["h2h"]
    return ["h2h", "spreads", "totals"]


def enforce_market_allowed(session: Session, settings: Settings, requested_market: str) -> tuple[bool, dict[str, object] | None]:
    requested = requested_market.strip().lower()
    allowed = allowed_markets(session, settings)
    clv_count = get_clv_computed_count(session)
    if requested in allowed:
        return True, None
    reason: dict[str, object] = {
        "code": "market_locked_until_clv_100",
        "requested_market": requested,
        "clv_computed_count": clv_count,
        "threshold": settings.markets_unlock_clv_min,
        "allowed_markets": allowed,
    }
    return False, reason
