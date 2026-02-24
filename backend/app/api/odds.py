from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import and_, func, select
from sqlalchemy.orm import Session

from app.config import get_settings
from app.db import get_db
from app.models import Game, OddsSnapshot
from app.services.ingest import ingest_odds_for_sport
from app.services.quota import get_quota_state

router = APIRouter(tags=["odds"])


@router.post("/odds/ingest")
def ingest_once(sport_key: str = Query(...), db: Session = Depends(get_db)) -> dict:
    settings = get_settings()
    if not settings.odds_api_key:
        raise HTTPException(status_code=400, detail="ODDS_API_KEY is required for ingestion endpoints")
    try:
        return ingest_odds_for_sport(db, sport_key)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/odds/latest")
def latest_odds(
    sport_key: str = Query(...), market_key: str = Query(...), db: Session = Depends(get_db)
) -> dict:
    latest_subq = (
        select(
            OddsSnapshot.game_id.label("game_id"),
            OddsSnapshot.market_key.label("market_key"),
            OddsSnapshot.bookmaker.label("bookmaker"),
            func.coalesce(OddsSnapshot.point, -999999).label("point_key"),
            func.max(OddsSnapshot.captured_at).label("captured_at"),
        )
        .group_by(
            OddsSnapshot.game_id,
            OddsSnapshot.market_key,
            OddsSnapshot.bookmaker,
            func.coalesce(OddsSnapshot.point, -999999),
        )
        .subquery()
    )

    rows = db.execute(
        select(Game, OddsSnapshot)
        .join(OddsSnapshot, OddsSnapshot.game_id == Game.id)
        .join(
            latest_subq,
            and_(
                OddsSnapshot.game_id == latest_subq.c.game_id,
                OddsSnapshot.market_key == latest_subq.c.market_key,
                OddsSnapshot.bookmaker == latest_subq.c.bookmaker,
                func.coalesce(OddsSnapshot.point, -999999) == latest_subq.c.point_key,
                OddsSnapshot.captured_at == latest_subq.c.captured_at,
            ),
        )
        .where(Game.sport_key == sport_key, OddsSnapshot.market_key == market_key)
    ).all()

    data: dict[str, dict] = {}
    for game, snap in rows:
        event = data.setdefault(
            game.event_id,
            {
                "event_id": game.event_id,
                "home_team": game.home_team,
                "away_team": game.away_team,
                "commence_time": game.commence_time.isoformat(),
                "groups": {},
            },
        )
        group_key = f"{snap.bookmaker}:{snap.point}"
        group = event["groups"].setdefault(
            group_key,
            {
                "bookmaker": snap.bookmaker,
                "point": float(snap.point) if snap.point is not None else None,
                "captured_at": snap.captured_at.isoformat(),
                "sides": [],
            },
        )
        group["sides"].append(
            {
                "side": snap.side,
                "american": snap.american,
                "decimal": float(snap.decimal) if snap.decimal is not None else None,
                "implied_prob": float(snap.implied_prob),
                "fair_prob": float(snap.fair_prob),
            }
        )

    return {"sport_key": sport_key, "market_key": market_key, "events": list(data.values())}


@router.get("/system/quota", tags=["system"])
def system_quota() -> dict:
    return get_quota_state()
