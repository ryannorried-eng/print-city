from fastapi import APIRouter

from app.api.calibration import router as calibration_router
from app.api.clv import router as clv_router
from app.api.eval import router as eval_router
from app.api.consensus import router as consensus_router
from app.api.dashboard import router as dashboard_router
from app.api.odds import router as odds_router
from app.api.picks import router as picks_router
from app.api.pipeline import router as pipeline_router
from app.api.pqs import router as pqs_router
from app.api.metrics import router as metrics_router
from app.api.system import router as system_router

api_router = APIRouter()
api_router.include_router(odds_router)
api_router.include_router(consensus_router)
api_router.include_router(picks_router)
api_router.include_router(clv_router)

api_router.include_router(pipeline_router)
api_router.include_router(metrics_router)
api_router.include_router(system_router)

api_router.include_router(pqs_router)

api_router.include_router(eval_router)
api_router.include_router(calibration_router)
api_router.include_router(dashboard_router)
