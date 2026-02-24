from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db import get_db
from app.eval.calibration import apply_calibration, propose_calibration
from app.models import CalibrationRun

router = APIRouter(tags=["calibration"])


@router.post("/calibration/propose")
def calibration_propose(target_n: int = Query(200, ge=1, le=1000), db: Session = Depends(get_db)) -> dict[str, object]:
    return propose_calibration(db, target_n=target_n)


@router.post("/calibration/apply/{run_id}")
def calibration_apply(run_id: int, db: Session = Depends(get_db)) -> dict[str, object]:
    return apply_calibration(db, run_id)


@router.get("/calibration/runs")
def calibration_runs(db: Session = Depends(get_db)) -> list[dict[str, object]]:
    rows = db.execute(select(CalibrationRun).order_by(CalibrationRun.id.asc())).scalars().all()
    return [
        {
            "id": r.id,
            "created_at": r.created_at,
            "eval_window_start": r.eval_window_start,
            "eval_window_end": r.eval_window_end,
            "pqs_version": r.pqs_version,
            "status": r.status,
            "applied_at": r.applied_at,
            "proposed_patch": r.proposed_config_patch_json,
        }
        for r in rows
    ]
