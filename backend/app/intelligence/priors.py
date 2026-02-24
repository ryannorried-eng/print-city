from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from statistics import mean, median, pstdev

from sqlalchemy import and_, delete, desc, select
from sqlalchemy.orm import Session

from app.config import Settings
from app.models import ClvSportStat, Game, Pick


def _bps(value: float) -> float:
    return value * 10000.0


def recompute_clv_sport_stats(session: Session, settings: Settings) -> dict[str, int]:
    as_of = datetime.now(timezone.utc).replace(microsecond=0)
    rows = (
        session.execute(
            select(Pick, Game)
            .join(Game, Pick.game_id == Game.id)
            .where(and_(Pick.clv_computed_at.is_not(None), Pick.market_clv.is_not(None)))
            .order_by(desc(Pick.clv_computed_at), desc(Pick.id))
        )
        .all()
    )

    grouped: dict[tuple[str, str], list[tuple[Pick, Game]]] = {}
    for pick, game in rows:
        key = (game.sport_key, pick.market_key)
        if len(grouped.get(key, [])) < settings.clv_prior_window:
            grouped.setdefault(key, []).append((pick, game))

    session.execute(delete(ClvSportStat).where(ClvSportStat.window_size == settings.clv_prior_window))

    inserted = 0
    for (sport_key, market_key), picks in sorted(grouped.items()):
        market_vals = [_bps(float(p.market_clv)) for p, _ in picks if p.market_clv is not None]
        book_vals = [_bps(float(p.book_clv)) for p, _ in picks if p.book_clv is not None]
        n = len(market_vals)
        weak = n < settings.clv_min_n_for_prior
        if n == 0:
            mean_market = 0.0
            median_market = 0.0
            pct_positive = 0.5
            sharpe = 0.0
        else:
            mean_market = mean(market_vals) if not weak else 0.0
            median_market = median(market_vals) if not weak else 0.0
            pct_positive = (sum(1 for v in market_vals if v > 0) / n) if not weak else 0.5
            vol = pstdev(market_vals) if n > 1 else 0.0
            sharpe = (mean(market_vals) / vol) if vol > 0 else 0.0

        session.add(
            ClvSportStat(
                sport_key=sport_key,
                market_key=market_key,
                side_type=None,
                window_size=settings.clv_prior_window,
                as_of=as_of,
                n=n,
                mean_market_clv_bps=Decimal(str(round(mean_market, 4))),
                median_market_clv_bps=Decimal(str(round(median_market, 4))),
                pct_positive_market_clv=Decimal(str(round(pct_positive, 6))),
                mean_same_book_clv_bps=Decimal(str(round(mean(book_vals), 4))) if book_vals else None,
                sharpe_like=Decimal(str(round(sharpe, 6))),
                is_weak=1 if weak else 0,
                last_updated_at=as_of,
            )
        )
        inserted += 1

    session.commit()
    return {"inserted": inserted, "as_of": as_of.isoformat()}


def get_latest_prior(session: Session, *, sport_key: str, market_key: str, window_size: int) -> ClvSportStat | None:
    return (
        session.execute(
            select(ClvSportStat)
            .where(
                ClvSportStat.sport_key == sport_key,
                ClvSportStat.market_key == market_key,
                ClvSportStat.side_type.is_(None),
                ClvSportStat.window_size == window_size,
            )
            .order_by(desc(ClvSportStat.as_of), desc(ClvSportStat.id))
            .limit(1)
        )
        .scalars()
        .first()
    )
