from __future__ import annotations

from datetime import datetime, timedelta, timezone
from statistics import mean, median

from sqlalchemy import and_, select
from sqlalchemy.orm import Session

from app.eval.service import gates_report, pqs_clv_report
from app.models import Game, Pick, PickScore


def _safe_float(value: object) -> float | None:
    if value is None:
        return None
    return float(value)


def _compute_bucket(rows: list[tuple[Pick, Game]]) -> dict[str, object]:
    total = len(rows)
    computed = [pick for pick, _game in rows if pick.clv_computed_at is not None]
    market_values = [float(pick.market_clv) for pick in computed if pick.market_clv is not None]
    book_values = [float(pick.book_clv) for pick in computed if pick.book_clv is not None]
    positives = [value for value in market_values if value > 0]

    return {
        "total_picks": total,
        "clv_computed_count": len(computed),
        "clv_coverage_rate": (len(computed) / total) if total else 0.0,
        "median_market_clv": median(market_values) if market_values else None,
        "mean_market_clv": mean(market_values) if market_values else None,
        "median_book_clv": median(book_values) if book_values else None,
        "mean_book_clv": mean(book_values) if book_values else None,
        "pct_positive_market_clv": (len(positives) / len(market_values)) if market_values else 0.0,
    }


def compute_clv_health(session: Session, days: int = 7) -> dict[str, object]:
    now_utc = datetime.now(timezone.utc)
    window_start = now_utc - timedelta(days=days)

    rows = (
        session.execute(
            select(Pick, Game)
            .join(Game, Pick.game_id == Game.id)
            .where(and_(Pick.created_at >= window_start, Pick.created_at <= now_utc))
            .order_by(Game.sport_key.asc(), Pick.created_at.asc(), Pick.id.asc())
        )
        .all()
    )

    overall = _compute_bucket(rows)
    by_sport: dict[str, dict[str, object]] = {}
    sport_keys = sorted({game.sport_key for _pick, game in rows})
    for sport_key in sport_keys:
        sport_rows = [(pick, game) for pick, game in rows if game.sport_key == sport_key]
        by_sport[sport_key] = _compute_bucket(sport_rows)

    latest_scores = session.execute(select(PickScore)).scalars().all()
    kept = [r for r in latest_scores if r.decision in {"KEEP", "WARN"}]
    keep_rate = (len(kept) / len(latest_scores)) if latest_scores else 0.0

    eval_pqs = pqs_clv_report(session, min_n=5)
    eval_gates = gates_report(session, min_n=5)

    return {
        "days": days,
        "window_start": window_start,
        "window_end": now_utc,
        **overall,
        "by_sport": by_sport,
        "keep_rate": keep_rate,
        "avg_pqs": (sum(float(r.pqs) for r in latest_scores) / len(latest_scores)) if latest_scores else 0.0,
        "eval_summary": {
            "eval_window_start": window_start,
            "eval_window_end": now_utc,
            "pqs_spearman": eval_pqs.get("spearman") if isinstance(eval_pqs, dict) else None,
            "pqs_bin_table": eval_pqs.get("bins", [])[:3] if isinstance(eval_pqs, dict) else [],
            "top_drop_reasons": eval_gates.get("drop_reasons", [])[:3] if isinstance(eval_gates, dict) else [],
        },
    }
