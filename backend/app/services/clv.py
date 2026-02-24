from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal

from sqlalchemy import and_, desc, func, select
from sqlalchemy.orm import Session

from app.config import get_settings
from app.core.math import book_clv, consensus_fair_prob, market_clv
from app.models import Game, OddsSnapshot, Pick


@dataclass
class ClosingMarketView:
    consensus_probs: dict[str, float]
    best_decimal_by_side: dict[str, float]
    rows: list[OddsSnapshot]
    captured_at_used: datetime


def _required_sides(sport_key: str, market_key: str) -> tuple[str, ...]:
    if market_key == "h2h" and sport_key.startswith("soccer_"):
        return ("AWAY", "DRAW", "HOME")
    if market_key == "h2h" or market_key == "spreads":
        return ("AWAY", "HOME")
    return ("OVER", "UNDER")


def _to_decimal(value: float, places: str) -> Decimal:
    return Decimal(str(value)).quantize(Decimal(places))


def get_closing_market_view(session: Session, pick: Pick) -> ClosingMarketView | None:
    game = session.get(Game, pick.game_id)
    if game is None:
        return None

    required_sides = _required_sides(game.sport_key, pick.market_key)
    required_side_values = list(required_sides)
    point_match = OddsSnapshot.point.is_(None) if pick.point is None else OddsSnapshot.point == pick.point

    per_timestamp = (
        select(
            OddsSnapshot.bookmaker.label("bookmaker"),
            OddsSnapshot.captured_at.label("captured_at"),
            func.count(func.distinct(OddsSnapshot.side)).label("side_count"),
        )
        .where(
            OddsSnapshot.game_id == pick.game_id,
            OddsSnapshot.market_key == pick.market_key,
            point_match,
            OddsSnapshot.captured_at < game.commence_time,
            OddsSnapshot.side.in_(required_side_values),
        )
        .group_by(OddsSnapshot.bookmaker, OddsSnapshot.captured_at)
        .subquery()
    )

    latest_complete = (
        select(
            per_timestamp.c.bookmaker,
            func.max(per_timestamp.c.captured_at).label("captured_at"),
        )
        .where(per_timestamp.c.side_count == len(required_sides))
        .group_by(per_timestamp.c.bookmaker)
        .subquery()
    )

    rows = (
        session.execute(
            select(OddsSnapshot)
            .join(
                latest_complete,
                and_(
                    OddsSnapshot.bookmaker == latest_complete.c.bookmaker,
                    OddsSnapshot.captured_at == latest_complete.c.captured_at,
                ),
            )
            .where(
                OddsSnapshot.game_id == pick.game_id,
                OddsSnapshot.market_key == pick.market_key,
                point_match,
                OddsSnapshot.side.in_(required_side_values),
            )
            .order_by(
                OddsSnapshot.bookmaker.asc(),
                OddsSnapshot.captured_at.desc(),
                OddsSnapshot.side.asc(),
                OddsSnapshot.id.desc(),
            )
        )
        .scalars()
        .all()
    )

    if not rows:
        return None

    by_book: dict[str, dict[str, OddsSnapshot]] = {}
    for row in rows:
        by_book.setdefault(row.bookmaker, {})[row.side] = row

    complete_books = {
        bookmaker: side_rows
        for bookmaker, side_rows in by_book.items()
        if all(side in side_rows for side in required_sides)
    }
    if not complete_books:
        return None

    settings = get_settings()
    sharp_books = {book.lower() for book in settings.sharp_books}
    included_books = sorted(complete_books.keys())
    weights = [
        settings.sharp_weight if book.lower() in sharp_books else settings.standard_weight
        for book in included_books
    ]
    books_probs = [
        {side: float(complete_books[book][side].fair_prob) for side in required_sides}
        for book in included_books
    ]

    if len(included_books) < settings.consensus_min_books:
        return None

    consensus_probs = consensus_fair_prob(books_probs=books_probs, weights=weights)

    best_decimal_by_side: dict[str, float] = {}
    for side in required_sides:
        best = max(
            (
                (float(side_rows[side].decimal), bookmaker)
                for bookmaker, side_rows in complete_books.items()
                if side_rows[side].decimal is not None
            ),
            default=None,
            key=lambda item: (item[0], item[1]),
        )
        if best is not None:
            best_decimal_by_side[side] = best[0]

    captured_at_used = max(row.captured_at for row in rows)

    return ClosingMarketView(
        consensus_probs=consensus_probs,
        best_decimal_by_side=best_decimal_by_side,
        rows=rows,
        captured_at_used=captured_at_used,
    )


def compute_pick_clv(session: Session, pick: Pick) -> bool:
    side = pick.side
    closing_view = get_closing_market_view(session, pick)
    if closing_view is None:
        return False

    closing_consensus_prob = closing_view.consensus_probs.get(side)
    if closing_consensus_prob is None:
        return False

    pick_time_implied_prob = 1.0 / float(pick.best_decimal)

    closing_book_decimal: float | None = None
    for row in closing_view.rows:
        if row.bookmaker == pick.best_book and row.side == side and row.decimal is not None:
            closing_book_decimal = float(row.decimal)
            break

    closing_book_implied_prob: float | None = None
    computed_book_clv: float | None = None
    if closing_book_decimal is not None:
        closing_book_implied_prob = 1.0 / closing_book_decimal
        computed_book_clv = book_clv(closing_book_implied_prob, pick_time_implied_prob)

    pick.closing_consensus_prob = _to_decimal(closing_consensus_prob, "0.00000001")
    pick.market_clv = _to_decimal(
        market_clv(closing_consensus_prob, float(pick.consensus_prob)), "0.00000001"
    )
    pick.closing_book_decimal = (
        _to_decimal(closing_book_decimal, "0.00001") if closing_book_decimal is not None else None
    )
    pick.closing_book_implied_prob = (
        _to_decimal(closing_book_implied_prob, "0.00000001")
        if closing_book_implied_prob is not None
        else None
    )
    pick.book_clv = _to_decimal(computed_book_clv, "0.00000001") if computed_book_clv is not None else None
    pick.clv_computed_at = datetime.now(timezone.utc)
    return True


def compute_clv_for_date(session: Session, date_utc: date, force: bool = False) -> dict[str, int]:
    day_start = datetime.combine(date_utc, time.min, tzinfo=timezone.utc)
    day_end = day_start + timedelta(days=1)

    rows = session.execute(
        select(Pick)
        .join(Game, Pick.game_id == Game.id)
        .where(and_(Game.commence_time >= day_start, Game.commence_time < day_end))
        .order_by(desc(Pick.created_at), desc(Pick.id))
    ).scalars().all()

    summary = {
        "processed": len(rows),
        "updated": 0,
        "skipped_no_close": 0,
        "skipped_already_computed": 0,
    }

    for pick in rows:
        if pick.clv_computed_at is not None and not force:
            summary["skipped_already_computed"] += 1
            continue

        updated = compute_pick_clv(session, pick)
        if updated:
            summary["updated"] += 1
        else:
            summary["skipped_no_close"] += 1

    session.commit()
    return summary


def list_latest_clv(session: Session, limit: int = 50) -> list[dict[str, object]]:
    rows = (
        session.execute(
            select(Pick, Game)
            .join(Game, Pick.game_id == Game.id)
            .where(Pick.clv_computed_at.is_not(None))
            .order_by(desc(Pick.clv_computed_at), desc(Pick.id))
            .limit(limit)
        )
        .all()
    )

    output: list[dict[str, object]] = []
    for pick, game in rows:
        output.append(
            {
                "id": pick.id,
                "event_id": game.event_id,
                "sport_key": game.sport_key,
                "market_key": pick.market_key,
                "side": pick.side,
                "best_book": pick.best_book,
                "consensus_prob": float(pick.consensus_prob),
                "closing_consensus_prob": float(pick.closing_consensus_prob),
                "market_clv": float(pick.market_clv),
                "closing_book_decimal": float(pick.closing_book_decimal)
                if pick.closing_book_decimal is not None
                else None,
                "closing_book_implied_prob": float(pick.closing_book_implied_prob)
                if pick.closing_book_implied_prob is not None
                else None,
                "book_clv": float(pick.book_clv) if pick.book_clv is not None else None,
                "clv_computed_at": pick.clv_computed_at,
            }
        )

    return output



def list_clv_sport_stats(session: Session, limit: int = 100) -> list[dict[str, object]]:
    from app.models import ClvSportStat

    rows = (
        session.execute(
            select(ClvSportStat)
            .order_by(desc(ClvSportStat.as_of), desc(ClvSportStat.id))
            .limit(limit)
        )
        .scalars()
        .all()
    )
    return [
        {
            "sport_key": row.sport_key,
            "market_key": row.market_key,
            "side_type": row.side_type,
            "window_size": row.window_size,
            "as_of": row.as_of,
            "n": row.n,
            "mean_market_clv_bps": float(row.mean_market_clv_bps),
            "median_market_clv_bps": float(row.median_market_clv_bps),
            "pct_positive_market_clv": float(row.pct_positive_market_clv),
            "mean_same_book_clv_bps": float(row.mean_same_book_clv_bps) if row.mean_same_book_clv_bps is not None else None,
            "sharpe_like": float(row.sharpe_like) if row.sharpe_like is not None else None,
            "is_weak": bool(row.is_weak),
            "last_updated_at": row.last_updated_at,
        }
        for row in rows
    ]
