from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from sqlalchemy import and_, desc, func, select
from sqlalchemy.orm import Session

from app.config import get_settings
from app.core.math import ev_percent, kelly_fraction
from app.models import Game, Pick
from app.services.consensus import (
    build_market_views,
    compute_consensus_for_view,
    get_latest_group_rows,
)


def _to_decimal(value: float, places: str) -> Decimal:
    return Decimal(str(value)).quantize(Decimal(places))


def generate_consensus_picks(
    session: Session,
    sport_key: str,
    market_key: str,
    as_of: datetime | None = None,
) -> dict[str, int]:
    settings = get_settings()
    rows = get_latest_group_rows(session=session, sport_key=sport_key, market_key=market_key)
    views = build_market_views(rows)

    results = [compute_consensus_for_view(views[key]) for key in sorted(views.keys())]
    event_to_game_id = {
        event_id: game_id
        for event_id, game_id in session.execute(
            select(Game.event_id, Game.id).where(
                Game.event_id.in_([result.event_id for result in results])
            )
        ).all()
    }

    summary = {
        "total_views": len(results),
        "candidates": 0,
        "inserted": 0,
        "skipped_existing": 0,
        "skipped_low_ev": 0,
        "skipped_insufficient_books": 0,
    }

    for result in results:
        if summary["inserted"] >= settings.pick_max_per_run:
            break

        if result.consensus_probs is None or result.included_books < settings.pick_min_books:
            summary["skipped_insufficient_books"] += 1
            continue

        game_id = event_to_game_id.get(result.event_id)
        if game_id is None:
            continue

        for side, probability in result.consensus_probs.items():
            if summary["inserted"] >= settings.pick_max_per_run:
                break

            best_decimal = result.best_decimal.get(side)
            best_book = result.best_book.get(side)
            if best_decimal is None or best_book is None:
                continue

            summary["candidates"] += 1
            ev = ev_percent(probability, best_decimal)
            if ev < settings.pick_min_ev:
                summary["skipped_low_ev"] += 1
                continue

            kelly = kelly_fraction(
                probability,
                best_decimal,
                kelly_multiplier=settings.kelly_multiplier,
                max_cap=settings.kelly_max_cap,
            )
            if kelly <= 0:
                continue

            stake = settings.bankroll_paper * kelly
            point_value = Decimal(str(result.point)) if result.point is not None else None
            pick = Pick(
                game_id=game_id,
                market_key=result.market_key,
                side=side.value,
                point=point_value,
                source="CONSENSUS",
                consensus_prob=_to_decimal(probability, "0.00000001"),
                best_decimal=_to_decimal(best_decimal, "0.00001"),
                best_book=best_book,
                ev=_to_decimal(ev, "0.00000001"),
                kelly_fraction=_to_decimal(kelly, "0.00000001"),
                stake=_to_decimal(stake, "0.0001"),
                consensus_books=result.included_books,
                sharp_books=result.sharp_books_included,
                captured_at_min=result.captured_at_min,
                captured_at_max=result.captured_at_max,
            )
            if as_of is not None:
                pick.created_at = as_of

            exists_conditions = [
                Pick.game_id == game_id,
                Pick.market_key == result.market_key,
                Pick.side == side.value,
                Pick.best_book == best_book,
                Pick.captured_at_max == result.captured_at_max,
            ]
            if point_value is None:
                exists_conditions.append(Pick.point.is_(None))
            else:
                exists_conditions.append(Pick.point == point_value)

            exists_stmt = select(Pick.id).where(and_(*exists_conditions))
            if session.execute(exists_stmt).scalar_one_or_none() is not None:
                summary["skipped_existing"] += 1
                continue

            session.add(pick)
            summary["inserted"] += 1

    session.commit()
    return summary


def list_picks(
    session: Session,
    sport_key: str | None,
    market_key: str | None,
    date: str | None,
    limit: int = 100,
) -> list[dict[str, object]]:
    stmt = (
        select(Pick, Game)
        .join(Game, Pick.game_id == Game.id)
        .order_by(desc(Pick.created_at), desc(Pick.id))
        .limit(limit)
    )

    conditions = []
    if sport_key:
        conditions.append(Game.sport_key == sport_key)
    if market_key:
        conditions.append(Pick.market_key == market_key)
    if date:
        day = datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        day_end = day + timedelta(days=1)
        conditions.append(and_(Pick.created_at >= day, Pick.created_at < day_end))

    if conditions:
        stmt = stmt.where(and_(*conditions))

    rows = session.execute(stmt).all()
    output: list[dict[str, object]] = []
    for pick, game in rows:
        output.append(
            {
                "id": pick.id,
                "created_at": pick.created_at,
                "sport_key": game.sport_key,
                "event_id": game.event_id,
                "commence_time": game.commence_time,
                "home_team": game.home_team,
                "away_team": game.away_team,
                "market_key": pick.market_key,
                "side": pick.side,
                "point": float(pick.point) if pick.point is not None else None,
                "source": pick.source,
                "consensus_prob": float(pick.consensus_prob),
                "best_decimal": float(pick.best_decimal),
                "best_book": pick.best_book,
                "ev": float(pick.ev),
                "kelly_fraction": float(pick.kelly_fraction),
                "stake": float(pick.stake),
                "consensus_books": pick.consensus_books,
                "sharp_books": pick.sharp_books,
                "captured_at_min": pick.captured_at_min,
                "captured_at_max": pick.captured_at_max,
            }
        )

    return output
