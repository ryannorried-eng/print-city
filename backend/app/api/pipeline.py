from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.config import get_settings
from app.core.scheduler import scheduler_is_running, scheduler_next_run_times
from app.db import get_db
from app.services.pipeline import (
    latest_run_statuses,
    list_pipeline_runs,
    run_and_log,
    run_cycle,
)
from app.services.quota import get_quota_state

router = APIRouter(tags=["pipeline"])


@router.post("/pipeline/run")
def pipeline_run(
    run_type: str = Query("cycle", pattern="^(cycle|ingest|picks|clv)$"),
    force: bool = Query(False),
    db: Session = Depends(get_db),
) -> dict[str, object]:
    settings = get_settings()
    if run_type in {"ingest", "picks", "clv"}:
        return run_and_log(db, settings, run_type=run_type, force=force)
    return run_cycle(db, settings, force=force)


@router.get("/pipeline/runs")
def pipeline_runs(limit: int = Query(50, ge=1, le=500), db: Session = Depends(get_db)) -> list[dict[str, object]]:
    return list_pipeline_runs(db, limit=limit)


@router.get("/pipeline/health")
def pipeline_health(db: Session = Depends(get_db)) -> dict[str, object]:
    return {
        "scheduler_enabled": scheduler_is_running(),
        "next_run_times": scheduler_next_run_times(),
        "last_run_statuses": latest_run_statuses(db),
        "quota": get_quota_state(),
    }
