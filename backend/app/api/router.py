from fastapi import APIRouter

from app.api.odds import router as odds_router

api_router = APIRouter()
api_router.include_router(odds_router)
