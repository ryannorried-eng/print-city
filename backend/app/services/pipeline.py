from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Callable

from sqlalchemy import and_, desc, select
from sqlalchemy.orm import Session

from app.config import Settings
from app.models import Game, Pick, PipelineRun
from app.services.clv import compute_pick_clv
from app.services.ingest import ingest_odds_for_sport
from app.services.picks import generate_consensus_picks


def resolve_sports(settings: Settings) -> list[str]:
    if settings.sports_autorun.strip():
        sports = [part.strip() for part in settings.sports_autorun.split(",") if part.strip()]
    else:
        sports = list(settings.odds_sports_whitelist)
    return sorted(set(sports))


def resolve_markets(settings: Settings) -> list[str]:
    markets = [part.strip() for part in settings.markets_autorun.split(",") if part.strip()]
    if not markets:
        markets = ["h2h"]
    return sorted(dict.fromkeys(markets))


def _log_run(
    session: Session,
    *,
    run_type: str,
    status: str,
    sports: list[str],
    markets: list[str],
    stats: dict,
    error: str | None = None,
) -> None:
    session.add(
        PipelineRun(
            run_type=run_type,
            status=status,
            sports=",".join(sports),
            markets=",".join(markets),
            stats_json=json.dumps(stats, sort_keys=True),
            error=error,
        )
    )
    session.commit()


def _run_per_sport(
    sports: list[str],
    handler: Callable[[str], dict],
) -> tuple[dict[str, dict], dict[str, str]]:
    per_sport: dict[str, dict] = {}
    errors: dict[str, str] = {}
    for sport in sports:
        try:
            per_sport[sport] = handler(sport)
        except Exception as exc:  # noqa: BLE001
            errors[sport] = str(exc)
    return per_sport, errors


def run_ingest(session: Session, settings: Settings) -> dict:
    sports = resolve_sports(settings)
    per_sport, errors = _run_per_sport(sports, lambda sport: ingest_odds_for_sport(session, sport))

    totals = {
        "games_upserted": sum(item.get("games_upserted", 0) for item in per_sport.values()),
        "groups_changed": sum(item.get("groups_changed", 0) for item in per_sport.values()),
        "snapshot_rows_inserted": sum(item.get("snapshot_rows_inserted", 0) for item in per_sport.values()),
        "groups_skipped": sum(item.get("groups_skipped", 0) for item in per_sport.values()),
    }
    return {
        **totals,
        "sports": sports,
        "markets": [],
        "per_sport": per_sport,
        "errors": errors,
        "errors_count": len(errors),
    }


def run_picks(session: Session, settings: Settings) -> dict:
    sports = resolve_sports(settings)
    markets = resolve_markets(settings)
    per_pair: dict[str, dict] = {}
    errors: dict[str, str] = {}

    for sport in sports:
        for market in markets:
            key = f"{sport}:{market}"
            try:
                per_pair[key] = generate_consensus_picks(session=session, sport_key=sport, market_key=market)
            except Exception as exc:  # noqa: BLE001
                errors[key] = str(exc)

    totals = {
        "total_views": sum(item.get("total_views", 0) for item in per_pair.values()),
        "candidates": sum(item.get("candidates", 0) for item in per_pair.values()),
        "inserted": sum(item.get("inserted", 0) for item in per_pair.values()),
        "skipped_existing": sum(item.get("skipped_existing", 0) for item in per_pair.values()),
        "skipped_low_ev": sum(item.get("skipped_low_ev", 0) for item in per_pair.values()),
        "skipped_insufficient_books": sum(
            item.get("skipped_insufficient_books", 0) for item in per_pair.values()
        ),
    }

    return {
        **totals,
        "sports": sports,
        "markets": markets,
        "per_sport_market": per_pair,
        "errors": errors,
        "errors_count": len(errors),
    }


def run_clv(session: Session, settings: Settings, force: bool = False) -> dict:
    now_utc = datetime.now(timezone.utc)
    stmt = (
        select(Pick)
        .join(Game, Pick.game_id == Game.id)
        .where(Game.commence_time <= now_utc)
        .order_by(Game.commence_time.asc(), Pick.id.asc())
    )
    if not force:
        stmt = stmt.where(Pick.clv_computed_at.is_(None))

    picks = session.execute(stmt).scalars().all()
    summary = {
        "processed": len(picks),
        "updated": 0,
        "skipped_no_close": 0,
        "skipped_already_computed": 0,
        "sports": resolve_sports(settings),
        "markets": resolve_markets(settings),
        "errors": {},
        "errors_count": 0,
    }

    for pick in picks:
        if pick.clv_computed_at is not None and not force:
            summary["skipped_already_computed"] += 1
            continue

        if compute_pick_clv(session, pick):
            summary["updated"] += 1
        else:
            summary["skipped_no_close"] += 1

    session.commit()
    return summary


def run_cycle(session: Session, settings: Settings, force: bool = False) -> dict:
    sports = resolve_sports(settings)
    markets = resolve_markets(settings)
    cycle_stats: dict[str, dict] = {}
    cycle_errors: dict[str, str] = {}

    for step in ["ingest", "picks", "clv"]:
        try:
            if step == "ingest":
                result = run_ingest(session, settings)
            elif step == "picks":
                result = run_picks(session, settings)
            else:
                result = run_clv(session, settings, force=force)
            cycle_stats[step] = result
            _log_run(
                session,
                run_type=step,
                status="ok",
                sports=sports,
                markets=markets,
                stats=result,
            )
        except Exception as exc:  # noqa: BLE001
            message = str(exc)
            cycle_errors[step] = message
            cycle_stats[step] = {"error": message}
            _log_run(
                session,
                run_type=step,
                status="error",
                sports=sports,
                markets=markets,
                stats=cycle_stats[step],
                error=message,
            )

    cycle_summary = {
        "sports": sports,
        "markets": markets,
        "steps": cycle_stats,
        "errors": cycle_errors,
        "errors_count": len(cycle_errors),
    }
    _log_run(
        session,
        run_type="cycle",
        status="ok" if not cycle_errors else "error",
        sports=sports,
        markets=markets,
        stats=cycle_summary,
        error=json.dumps(cycle_errors, sort_keys=True) if cycle_errors else None,
    )
    return cycle_summary


def list_pipeline_runs(session: Session, limit: int = 50) -> list[dict[str, object]]:
    rows = (
        session.execute(select(PipelineRun).order_by(desc(PipelineRun.created_at), desc(PipelineRun.id)).limit(limit))
        .scalars()
        .all()
    )
    return [
        {
            "id": row.id,
            "created_at": row.created_at,
            "run_type": row.run_type,
            "status": row.status,
            "sports": row.sports,
            "markets": row.markets,
            "stats_json": row.stats_json,
            "error": row.error,
        }
        for row in rows
    ]


def latest_run_statuses(session: Session) -> dict[str, dict[str, object] | None]:
    output: dict[str, dict[str, object] | None] = {}
    for run_type in ["ingest", "picks", "clv"]:
        row = (
            session.execute(
                select(PipelineRun)
                .where(PipelineRun.run_type == run_type)
                .order_by(desc(PipelineRun.created_at), desc(PipelineRun.id))
                .limit(1)
            )
            .scalars()
            .first()
        )
        output[run_type] = (
            {
                "status": row.status,
                "created_at": row.created_at,
                "error": row.error,
            }
            if row is not None
            else None
        )
    return output


def run_and_log(session: Session, settings: Settings, run_type: str, force: bool = False) -> dict:
    sports = resolve_sports(settings)
    markets = resolve_markets(settings)
    try:
        if run_type == "ingest":
            result = run_ingest(session, settings)
        elif run_type == "picks":
            result = run_picks(session, settings)
        elif run_type == "clv":
            result = run_clv(session, settings, force=force)
        else:
            raise ValueError(f"Unsupported run_type '{run_type}'")
        _log_run(
            session,
            run_type=run_type,
            status="ok",
            sports=sports,
            markets=markets,
            stats=result,
        )
        return result
    except Exception as exc:  # noqa: BLE001
        message = str(exc)
        _log_run(
            session,
            run_type=run_type,
            status="error",
            sports=sports,
            markets=markets,
            stats={"error": message},
            error=message,
        )
        raise
