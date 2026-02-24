from fastapi import APIRouter

from app.api.clv import router as clv_router
from app.api.consensus import router as consensus_router
from app.api.odds import router as odds_router
from app.api.picks import router as picks_router

api_router = APIRouter()
api_router.include_router(odds_router)
api_router.include_router(consensus_router)
api_router.include_router(picks_router)
api_router.include_router(clv_router)
