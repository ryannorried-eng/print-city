from fastapi import FastAPI

from app.api.router import api_router
from app.config import get_settings
from app.core.scheduler import start_scheduler, stop_scheduler

settings = get_settings()
app = FastAPI(title=settings.app_name)
app.include_router(api_router)


@app.on_event("startup")
def startup_event() -> None:
    start_scheduler(settings)


@app.on_event("shutdown")
def shutdown_event() -> None:
    stop_scheduler()


@app.get("/health", tags=["health"])
def health() -> dict[str, str]:
    return {"status": "ok", "environment": settings.app_env}
