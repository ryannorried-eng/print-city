from __future__ import annotations

import csv
import io
import math
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from statistics import mean, median

from sqlalchemy import and_, select
from sqlalchemy.orm import Session

from app.config import get_settings
from app.models import Game, Pick, PickScore, PipelineRun

KEEP_WARN = {"KEEP", "WARN"}


@dataclass(frozen=True)
class EvalRow:
    pick_id: int
    created_at: datetime
    clv_computed_at: datetime | None
    sport_key: str
    market_key: str
    event_id: str
    commence_time_utc: datetime
    side: str
    point: float | None
    pqs: float
    decision: str
    drop_reason: str | None
    market_clv_bps: float | None
    same_book_clv_bps: float | None
    closing_snapshot_at: datetime | None


def _to_bps(value: object) -> float | None:
    if value is None:
        return None
    return round(float(value) * 10000.0, 4)


def _latest_closing_snapshot(session: Session, pick: Pick, game: Game) -> datetime | None:
    return session.execute(
        select(Pick.captured_at_max)
        .where(Pick.id == pick.id)
        .limit(1)
    ).scalar_one_or_none()


def query_eval_dataset(
    session: Session,
    *,
    start: datetime | None,
    end: datetime | None,
    sport_key: str | None,
    market_key: str | None,
    decision: tuple[str, ...],
    min_n: int,
    limit: int,
    offset: int,
) -> dict[str, object]:
    settings = get_settings()
    stmt = (
        select(Pick, PickScore, Game)
        .join(PickScore, and_(PickScore.pick_id == Pick.id, PickScore.version == settings.pqs_version))
        .join(Game, Game.id == Pick.game_id)
    )
    if start:
        stmt = stmt.where(Pick.created_at >= start)
    if end:
        stmt = stmt.where(Pick.created_at <= end)
    if sport_key:
        stmt = stmt.where(Game.sport_key == sport_key)
    if market_key:
        stmt = stmt.where(Pick.market_key == market_key)
    if decision:
        stmt = stmt.where(PickScore.decision.in_(decision))

    rows = session.execute(
        stmt.order_by(Pick.created_at.asc(), Pick.id.asc())
    ).all()

    if len(rows) < min_n:
        return {"insufficient_n": True, "n": len(rows), "rows": []}

    total = len(rows)
    sliced = rows[offset: offset + limit]
    out: list[dict[str, object]] = []
    for pick, score, game in sliced:
        out.append(
            {
                "pick_id": pick.id,
                "created_at": pick.created_at,
                "clv_computed_at": pick.clv_computed_at,
                "sport_key": game.sport_key,
                "market_key": pick.market_key,
                "event_id": game.event_id,
                "commence_time_utc": game.commence_time,
                "side": pick.side,
                "point": float(pick.point) if pick.point is not None else None,
                "pqs": float(score.pqs),
                "decision": score.decision,
                "drop_reason": score.drop_reason,
                "market_clv_bps": _to_bps(pick.market_clv),
                "same_book_clv_bps": _to_bps(pick.book_clv),
                "closing_snapshot_at": _latest_closing_snapshot(session, pick, game),
            }
        )
    return {"insufficient_n": False, "n": total, "rows": out, "limit": limit, "offset": offset}


def dataset_csv(payload: dict[str, object]) -> str:
    columns = [
        "pick_id","created_at","clv_computed_at","sport_key","market_key","event_id","commence_time_utc","side","point","pqs","decision","drop_reason","market_clv_bps","same_book_clv_bps","closing_snapshot_at"
    ]
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=columns)
    writer.writeheader()
    for row in payload.get("rows", []):
        writer.writerow({c: row.get(c) for c in columns})
    return buf.getvalue()


def _spearman(pairs: list[tuple[float, float, int]]) -> float | None:
    if len(pairs) < 2:
        return None
    by_x = sorted(pairs, key=lambda item: (item[0], item[2]))
    by_y = sorted(pairs, key=lambda item: (item[1], item[2]))
    rx = {pid: idx + 1 for idx, (_, _, pid) in enumerate(by_x)}
    ry = {pid: idx + 1 for idx, (_, _, pid) in enumerate(by_y)}
    n = len(pairs)
    mean_rank = (n + 1) / 2
    num = sum((rx[pid] - mean_rank) * (ry[pid] - mean_rank) for _, _, pid in pairs)
    denx = math.sqrt(sum((rx[pid] - mean_rank) ** 2 for _, _, pid in pairs))
    deny = math.sqrt(sum((ry[pid] - mean_rank) ** 2 for _, _, pid in pairs))
    if denx == 0 or deny == 0:
        return None
    return round(num / (denx * deny), 6)


def pqs_clv_report(session: Session, *, min_n: int = 20) -> dict[str, object]:
    settings = get_settings()
    rows = session.execute(
        select(Pick.id, PickScore.pqs, Pick.market_clv)
        .join(PickScore, and_(PickScore.pick_id == Pick.id, PickScore.version == settings.pqs_version))
        .where(Pick.clv_computed_at.is_not(None), Pick.market_clv.is_not(None))
        .order_by(PickScore.pqs.asc(), Pick.id.asc())
    ).all()
    if len(rows) < min_n:
        return {"insufficient_n": True, "n": len(rows)}

    pairs = [(float(pqs), _to_bps(clv) or 0.0, pid) for pid, pqs, clv in rows]
    bins = [{"n": 0, "market": [], "book": []} for _ in range(5)]
    total = len(rows)
    for idx, (pid, pqs, clv) in enumerate(rows):
        bin_idx = min(4, (idx * 5) // total)
        bins[bin_idx]["n"] += 1
        bins[bin_idx]["market"].append(_to_bps(clv) or 0.0)

    table = []
    for idx, b in enumerate(bins, start=1):
        market_vals = b["market"]
        table.append(
            {
                "bin": idx,
                "n": b["n"],
                "mean_market_clv_bps": round(mean(market_vals), 4) if market_vals else None,
                "median_market_clv_bps": round(median(market_vals), 4) if market_vals else None,
                "pct_positive_market_clv": round(sum(1 for v in market_vals if v > 0) / len(market_vals), 6) if market_vals else 0.0,
            }
        )
    x = [row["bin"] for row in table if row["mean_market_clv_bps"] is not None]
    y = [row["mean_market_clv_bps"] for row in table if row["mean_market_clv_bps"] is not None]
    slope = None
    if len(x) >= 2:
        mx = mean(x)
        my = mean(y)
        denom = sum((xi - mx) ** 2 for xi in x)
        if denom > 0:
            slope = round(sum((xi - mx) * (yi - my) for xi, yi in zip(x, y)) / denom, 6)
    return {
        "insufficient_n": False,
        "n": len(rows),
        "spearman": _spearman(pairs),
        "bin_mean_slope": slope,
        "bins": table,
    }


def gates_report(session: Session, *, min_n: int = 20) -> dict[str, object]:
    settings = get_settings()
    rows = session.execute(
        select(PickScore.decision, PickScore.drop_reason, Pick.market_clv)
        .join(Pick, Pick.id == PickScore.pick_id)
        .where(PickScore.version == settings.pqs_version)
        .order_by(PickScore.id.asc())
    ).all()
    if len(rows) < min_n:
        return {"insufficient_n": True, "n": len(rows)}
    reasons = Counter((drop_reason or "none") for _d, drop_reason, _clv in rows)
    kept = [_to_bps(clv) for dec, _r, clv in rows if dec in KEEP_WARN and clv is not None]
    dropped = [_to_bps(clv) for dec, _r, clv in rows if dec == "DROP" and clv is not None]
    return {
        "insufficient_n": False,
        "n": len(rows),
        "drop_reasons": [{"reason": k, "count": v, "rate": round(v / len(rows), 6)} for k, v in sorted(reasons.items())],
        "kept_market_clv_bps_mean": round(mean([v for v in kept if v is not None]), 4) if kept else None,
        "dropped_market_clv_bps_mean": round(mean([v for v in dropped if v is not None]), 4) if dropped else None,
        "gate_parameters": {
            "MIN_BOOKS": settings.min_books,
            "SHARP_BOOK_MIN": settings.sharp_book_min,
            "MIN_MINUTES_TO_START": settings.min_minutes_to_start,
            "MAX_PRICE_DISPERSION": settings.max_price_dispersion,
            "MIN_AGREEMENT": settings.min_agreement,
        },
    }


def sports_report(session: Session, *, min_n: int = 20) -> dict[str, object]:
    settings = get_settings()
    rows = session.execute(
        select(Game.sport_key, Pick.market_key, PickScore.pqs, PickScore.decision, Pick.market_clv)
        .join(Pick, Pick.game_id == Game.id)
        .join(PickScore, and_(PickScore.pick_id == Pick.id, PickScore.version == settings.pqs_version))
        .order_by(Game.sport_key.asc(), Pick.market_key.asc(), Pick.id.asc())
    ).all()
    if len(rows) < min_n:
        return {"insufficient_n": True, "n": len(rows)}
    grouped: dict[tuple[str, str], list[tuple[float, str, float | None]]] = {}
    for sport, market, pqs, decision, mclv in rows:
        grouped.setdefault((sport, market), []).append((float(pqs), decision, _to_bps(mclv)))
    out = []
    for (sport, market), vals in sorted(grouped.items()):
        kept = [v for _p, d, v in vals if d in KEEP_WARN]
        out.append({
            "sport_key": sport,
            "market_key": market,
            "n": len(vals),
            "keep_rate": round(sum(1 for _p, d, _v in vals if d in KEEP_WARN) / len(vals), 6),
            "avg_pqs": round(mean(p for p, _d, _v in vals), 6),
            "mean_market_clv_bps": round(mean([v for v in kept if v is not None]), 4) if kept else None,
            "median_market_clv_bps": round(median([v for v in kept if v is not None]), 4) if kept else None,
            "pct_positive_clv": round(sum(1 for v in kept if (v or 0) > 0) / len([v for v in kept if v is not None]), 6) if [v for v in kept if v is not None] else 0.0,
            "adaptive_min_pqs": settings.sport_default_min_pqs,
            "adaptive_max_picks": settings.sport_default_max_picks,
        })
    return {"insufficient_n": False, "n": len(rows), "sports": out}


def volume_report(session: Session, *, min_n: int = 5) -> dict[str, object]:
    runs = session.execute(select(PipelineRun).order_by(PipelineRun.id.asc())).scalars().all()
    if len(runs) < min_n:
        return {"insufficient_n": True, "n": len(runs)}
    kept_per_run: list[int] = []
    hit_caps = 0
    settings = get_settings()
    for run in runs:
        try:
            import json
            stats = json.loads(run.stats_json or "{}")
        except Exception:
            stats = {}
        kept = int(stats.get("kept", stats.get("inserted", 0)))
        kept_per_run.append(kept)
        if kept >= settings.run_max_picks_total:
            hit_caps += 1
    return {
        "insufficient_n": False,
        "n": len(runs),
        "kept_per_run_mean": round(mean(kept_per_run), 4) if kept_per_run else 0.0,
        "kept_per_run_median": round(median(kept_per_run), 4) if kept_per_run else 0.0,
        "runs_hitting_caps_pct": round(hit_caps / len(runs), 6) if runs else 0.0,
    }
