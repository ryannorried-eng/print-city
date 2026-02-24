from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel
from sqlalchemy import Numeric, and_, bindparam, func, select
from sqlalchemy.orm import Session

from app.config import get_settings
from app.core.math import consensus_fair_prob
from app.domain.enums import MarketKey, Side
from app.models import Game, OddsSnapshot


@dataclass(frozen=True)
class OddsSnapshotRow:
    event_id: str
    sport_key: str
    commence_time: datetime
    home_team: str
    away_team: str
    market_key: str
    bookmaker: str
    side: Side
    point: Decimal | None
    captured_at: datetime
    american: int | None
    decimal: Decimal | None
    fair_prob: Decimal


@dataclass
class MarketView:
    event_id: str
    sport_key: str
    commence_time: datetime
    home_team: str
    away_team: str
    market_key: str
    point: float | None
    rows: list[OddsSnapshotRow]
    bookmaker_fair_probs: dict[str, dict[Side, float]]
    book_odds: dict[str, dict[Side, float]]
    total_books: int
    sharp_books: int
    book_list: list[str]


class ConsensusResult(BaseModel):
    event_id: str
    sport_key: str
    commence_time: datetime
    home_team: str
    away_team: str
    market_key: str
    point: float | None
    consensus_probs: dict[Side, float] | None
    consensus_reason: str | None = None
    included_books: int
    sharp_books_included: int
    best_decimal: dict[Side, float]
    best_book: dict[Side, str]
    captured_at_min: datetime
    captured_at_max: datetime


def _required_sides(market_key: str) -> set[Side]:
    market = MarketKey(market_key)
    if market in {MarketKey.H2H, MarketKey.SPREADS}:
        return {Side.HOME, Side.AWAY}
    return {Side.OVER, Side.UNDER}


POINT_SENTINEL = -999999


def _build_latest_group_rows_stmt(sport_key: str, market_key: str):
    market = MarketKey(market_key)

    # Reuse one shared bindparam object for coalesce(point, sentinel) across
    # SELECT/GROUP BY/JOIN so Postgres sees the exact same expression and
    # avoids GROUP BY mismatches from auto-generated bind names.
    sentinel = bindparam("point_sentinel", POINT_SENTINEL, type_=Numeric())
    point_key_expr = func.coalesce(OddsSnapshot.point, sentinel)

    per_timestamp_subq = (
        select(
            OddsSnapshot.game_id.label("game_id"),
            OddsSnapshot.market_key.label("market_key"),
            OddsSnapshot.bookmaker.label("bookmaker"),
            point_key_expr.label("point_key"),
            OddsSnapshot.captured_at.label("captured_at"),
            func.count(func.distinct(OddsSnapshot.side)).label("side_count"),
        )
        .join(Game, Game.id == OddsSnapshot.game_id)
        .where(Game.sport_key == sport_key, OddsSnapshot.market_key == market.value)
        .group_by(
            OddsSnapshot.game_id,
            OddsSnapshot.market_key,
            OddsSnapshot.bookmaker,
            point_key_expr,
            OddsSnapshot.captured_at,
        )
        .subquery()
    )

    latest_complete_subq = (
        select(
            per_timestamp_subq.c.game_id,
            per_timestamp_subq.c.market_key,
            per_timestamp_subq.c.bookmaker,
            per_timestamp_subq.c.point_key,
            func.max(per_timestamp_subq.c.captured_at).label("captured_at"),
        )
        .where(per_timestamp_subq.c.side_count >= 2)
        .group_by(
            per_timestamp_subq.c.game_id,
            per_timestamp_subq.c.market_key,
            per_timestamp_subq.c.bookmaker,
            per_timestamp_subq.c.point_key,
        )
        .subquery()
    )

    return (
        select(Game, OddsSnapshot)
        .join(OddsSnapshot, OddsSnapshot.game_id == Game.id)
        .join(
            latest_complete_subq,
            and_(
                OddsSnapshot.game_id == latest_complete_subq.c.game_id,
                OddsSnapshot.market_key == latest_complete_subq.c.market_key,
                OddsSnapshot.bookmaker == latest_complete_subq.c.bookmaker,
                point_key_expr == latest_complete_subq.c.point_key,
                OddsSnapshot.captured_at == latest_complete_subq.c.captured_at,
            ),
        )
        .where(Game.sport_key == sport_key, OddsSnapshot.market_key == market.value)
        .order_by(
            Game.event_id.asc(),
            OddsSnapshot.market_key.asc(),
            OddsSnapshot.bookmaker.asc(),
            OddsSnapshot.point.asc().nullsfirst(),
            OddsSnapshot.captured_at.desc(),
            OddsSnapshot.side.asc(),
            OddsSnapshot.id.desc(),
        )
    )


def get_latest_group_rows(session: Session, sport_key: str, market_key: str) -> list[OddsSnapshotRow]:
    stmt = _build_latest_group_rows_stmt(sport_key=sport_key, market_key=market_key)
    rows = session.execute(stmt).all()

    return [
        OddsSnapshotRow(
            event_id=game.event_id,
            sport_key=game.sport_key,
            commence_time=game.commence_time,
            home_team=game.home_team,
            away_team=game.away_team,
            market_key=snap.market_key,
            bookmaker=snap.bookmaker,
            side=Side(snap.side),
            point=snap.point,
            captured_at=snap.captured_at,
            american=snap.american,
            decimal=snap.decimal,
            fair_prob=snap.fair_prob,
        )
        for game, snap in rows
    ]


def build_market_views(rows: list[OddsSnapshotRow]) -> dict[tuple[str, str, float | None], MarketView]:
    settings = get_settings()
    sharp_books_set = {book.lower() for book in settings.sharp_books}

    grouped: dict[tuple[str, str, float | None], list[OddsSnapshotRow]] = {}
    for row in rows:
        point = float(row.point) if row.point is not None else None
        key = (row.event_id, row.market_key, point)
        grouped.setdefault(key, []).append(row)

    output: dict[tuple[str, str, float | None], MarketView] = {}
    for key in sorted(grouped.keys(), key=lambda x: (x[0], x[1], x[2] is not None, x[2] if x[2] is not None else -1)):
        view_rows = grouped[key]
        required = _required_sides(view_rows[0].market_key)

        by_book: dict[str, dict[Side, OddsSnapshotRow]] = {}
        for row in view_rows:
            by_book.setdefault(row.bookmaker, {})[row.side] = row

        complete_books = {
            bookmaker: sides
            for bookmaker, sides in by_book.items()
            if required.issubset(set(sides.keys()))
        }

        bookmaker_fair_probs = {
            book: {side: float(sides[side].fair_prob) for side in sorted(required, key=lambda s: s.value)}
            for book, sides in sorted(complete_books.items())
        }
        book_odds = {
            book: {
                side: float(sides[side].decimal)
                for side in sorted(required, key=lambda s: s.value)
                if sides[side].decimal is not None
            }
            for book, sides in sorted(complete_books.items())
        }

        book_list = sorted(complete_books.keys())
        output[key] = MarketView(
            event_id=view_rows[0].event_id,
            sport_key=view_rows[0].sport_key,
            commence_time=view_rows[0].commence_time,
            home_team=view_rows[0].home_team,
            away_team=view_rows[0].away_team,
            market_key=view_rows[0].market_key,
            point=key[2],
            rows=sorted(
                view_rows,
                key=lambda r: (
                    r.bookmaker,
                    r.side.value,
                    r.captured_at,
                ),
            ),
            bookmaker_fair_probs=bookmaker_fair_probs,
            book_odds=book_odds,
            total_books=len(book_list),
            sharp_books=sum(1 for b in book_list if b.lower() in sharp_books_set),
            book_list=book_list,
        )

    return output


def compute_consensus_for_view(view: MarketView) -> ConsensusResult:
    settings = get_settings()
    sharp_books_set = {book.lower() for book in settings.sharp_books}

    included_books = sorted(view.bookmaker_fair_probs.keys())
    weights = [
        settings.sharp_weight if bookmaker.lower() in sharp_books_set else settings.standard_weight
        for bookmaker in included_books
    ]
    books_probs = [view.bookmaker_fair_probs[bookmaker] for bookmaker in included_books]

    consensus_probs: dict[Side, float] | None = None
    reason: str | None = None

    if len(included_books) < settings.consensus_min_books:
        reason = "insufficient_books"
    else:
        consensus_probs = consensus_fair_prob(books_probs=books_probs, weights=weights)
        total = sum(consensus_probs.values())
        drift = abs(total - 1.0)
        if drift > settings.consensus_eps:
            consensus_probs = {side: prob / total for side, prob in consensus_probs.items()}
        assert abs(sum(consensus_probs.values()) - 1.0) <= settings.consensus_eps

    best_decimal: dict[Side, float] = {}
    best_book: dict[Side, str] = {}
    for row in view.rows:
        if row.decimal is None:
            continue
        dec = float(row.decimal)
        existing = best_decimal.get(row.side)
        if existing is None or dec > existing:
            best_decimal[row.side] = dec
            best_book[row.side] = row.bookmaker

    captured_at_min = min(row.captured_at for row in view.rows)
    captured_at_max = max(row.captured_at for row in view.rows)

    return ConsensusResult(
        event_id=view.event_id,
        sport_key=view.sport_key,
        commence_time=view.commence_time,
        home_team=view.home_team,
        away_team=view.away_team,
        market_key=view.market_key,
        point=view.point,
        consensus_probs=consensus_probs,
        consensus_reason=reason,
        included_books=len(included_books),
        sharp_books_included=sum(1 for b in included_books if b.lower() in sharp_books_set),
        best_decimal=best_decimal,
        best_book=best_book,
        captured_at_min=captured_at_min,
        captured_at_max=captured_at_max,
    )
