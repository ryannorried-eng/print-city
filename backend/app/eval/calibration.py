from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import and_, select
from sqlalchemy.orm import Session

from app.config import get_settings
from app.eval.service import gates_report, pqs_clv_report, sports_report
from app.models import CalibrationRun, Pick, PickScore


def _snapshot(settings) -> dict[str, object]:
    return {
        "pqs_weights": {
            "ev": settings.pqs_weight_ev,
            "agreement": settings.pqs_weight_agreement,
            "dispersion": settings.pqs_weight_dispersion,
            "coverage": settings.pqs_weight_coverage,
            "sharp_presence": settings.pqs_weight_sharp_presence,
            "clv_prior": settings.pqs_weight_clv_prior,
            "time_to_start": settings.pqs_weight_time_to_start,
        },
        "gates": {
            "min_books": settings.min_books,
            "sharp_book_min": settings.sharp_book_min,
            "min_minutes_to_start": settings.min_minutes_to_start,
            "max_price_dispersion": settings.max_price_dispersion,
            "min_agreement": settings.min_agreement,
        },
        "sport_defaults": {
            "min_pqs": settings.sport_default_min_pqs,
            "max_picks": settings.sport_default_max_picks,
        },
    }


def propose_calibration(session: Session, *, target_n: int = 200) -> dict[str, object]:
    settings = get_settings()
    rows = session.execute(
        select(Pick.created_at, Pick.clv_computed_at)
        .join(PickScore, and_(PickScore.pick_id == Pick.id, PickScore.version == settings.pqs_version))
        .where(Pick.clv_computed_at.is_not(None))
        .order_by(Pick.clv_computed_at.desc(), Pick.id.desc())
        .limit(max(target_n, 1))
    ).all()
    if not rows:
        return {"status": "insufficient_n", "reason": "no_clv_scored_picks"}
    eval_end = max(row[1] for row in rows if row[1] is not None)
    eval_start = min(row[0] for row in rows)

    pqs = pqs_clv_report(session, min_n=min(20, target_n))
    gates = gates_report(session, min_n=min(20, target_n))
    sports = sports_report(session, min_n=min(20, target_n))

    patch: dict[str, object] = {}
    rationale: dict[str, object] = {"pqs": pqs, "gates": gates, "sports": sports}

    if not pqs.get("insufficient_n") and (pqs.get("bin_mean_slope") or 0) <= 0:
        patch["PQS_WEIGHT_EV"] = round(max(0.05, settings.pqs_weight_ev - 0.02), 4)
        patch["PQS_WEIGHT_CLV_PRIOR"] = round(min(0.3, settings.pqs_weight_clv_prior + 0.02), 4)

    if not gates.get("insufficient_n"):
        kept = gates.get("kept_market_clv_bps_mean")
        dropped = gates.get("dropped_market_clv_bps_mean")
        if kept is not None and dropped is not None and kept < dropped:
            patch["MIN_BOOKS"] = max(4, settings.min_books + 1)
        elif kept is not None and kept > 0 and gates.get("n", 0) > 0 and sum(r["count"] for r in gates.get("drop_reasons", []) if r["reason"] != "none") / gates["n"] > 0.6:
            patch["MIN_BOOKS"] = max(4, settings.min_books - 1)

    if not sports.get("insufficient_n"):
        poor = [s for s in sports.get("sports", []) if s.get("pct_positive_clv", 1) < 0.45]
        if poor:
            patch["SPORT_DEFAULT_MIN_PQS"] = round(min(0.9, settings.sport_default_min_pqs + 0.03), 4)
            patch["SPORT_DEFAULT_MAX_PICKS"] = max(1, settings.sport_default_max_picks - 1)

    run = CalibrationRun(
        created_at=datetime.now(timezone.utc),
        eval_window_start=eval_start,
        eval_window_end=eval_end,
        pqs_version=settings.pqs_version,
        current_config_snapshot_json=_snapshot(settings),
        proposed_config_patch_json=patch,
        rationale_json=rationale,
        status="PROPOSED",
        applied_at=None,
    )
    session.add(run)
    session.commit()
    return {"id": run.id, "status": run.status, "patch": patch, "rationale": rationale}


def apply_calibration(session: Session, run_id: int) -> dict[str, object]:
    run = session.execute(select(CalibrationRun).where(CalibrationRun.id == run_id)).scalar_one()
    run.status = "APPLIED"
    run.applied_at = datetime.now(timezone.utc)
    session.commit()
    return {"id": run.id, "status": run.status, "applied_at": run.applied_at, "patch": run.proposed_config_patch_json}
