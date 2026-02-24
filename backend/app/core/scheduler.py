from __future__ import annotations

import logging
import threading
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import text

from app.config import Settings
from app.db import SessionLocal, engine
from app.services.pipeline import run_and_log

logger = logging.getLogger(__name__)

try:
    from apscheduler.schedulers.background import BackgroundScheduler
except Exception:  # noqa: BLE001
    class _SimpleJob:
        def __init__(self, job_id: str, next_run_time: datetime | None) -> None:
            self.id = job_id
            self.next_run_time = next_run_time

    class BackgroundScheduler:  # type: ignore[override]
        def __init__(self, timezone: Any) -> None:
            self.timezone = timezone
            self.running = False
            self._jobs: list[_SimpleJob] = []

        def add_job(self, _func: Any, _trigger: str, args: list[Any], id: str, **kwargs: Any) -> None:
            self._jobs = [job for job in self._jobs if job.id != id]
            self._jobs.append(_SimpleJob(id, kwargs.get("next_run_time")))

        def start(self) -> None:
            self.running = True

        def shutdown(self, wait: bool = False) -> None:  # noqa: ARG002
            self.running = False

        def get_jobs(self) -> list[_SimpleJob]:
            return list(self._jobs)


_scheduler: BackgroundScheduler | None = None
_run_lock = threading.Semaphore(1)


def _can_reach_db() -> bool:
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception:  # noqa: BLE001
        return False


def _run_job(run_type: str, settings: Settings) -> None:
    if not _run_lock.acquire(blocking=False):
        logger.info("Skipping %s job because another run is in progress", run_type)
        return

    try:
        with SessionLocal() as session:
            run_and_log(session, settings, run_type=run_type)
    except Exception:  # noqa: BLE001
        logger.exception("Scheduler %s job failed", run_type)
    finally:
        _run_lock.release()


def start_scheduler(settings: Settings) -> bool:
    global _scheduler

    if not settings.enable_scheduler:
        logger.info("Scheduler disabled by ENABLE_SCHEDULER=false")
        return False

    if settings.sched_require_db and (not settings.database_url or not _can_reach_db()):
        logger.warning("Scheduler not started: DB unavailable and SCHED_REQUIRE_DB=true")
        return False

    if _scheduler is not None and _scheduler.running:
        return True

    _scheduler = BackgroundScheduler(timezone=timezone.utc)
    now = datetime.now(timezone.utc)
    _scheduler.add_job(
        _run_job,
        "interval",
        args=["ingest", settings],
        id="ingest_job",
        seconds=settings.sched_ingest_interval_sec,
        jitter=settings.sched_jitter_sec,
        max_instances=settings.sched_max_concurrent,
        next_run_time=now,
        replace_existing=True,
    )
    _scheduler.add_job(
        _run_job,
        "interval",
        args=["picks", settings],
        id="picks_job",
        seconds=settings.sched_picks_interval_sec,
        jitter=settings.sched_jitter_sec,
        max_instances=settings.sched_max_concurrent,
        next_run_time=now + timedelta(seconds=60),
        replace_existing=True,
    )
    _scheduler.add_job(
        _run_job,
        "interval",
        args=["clv", settings],
        id="clv_job",
        seconds=settings.sched_clv_interval_sec,
        jitter=settings.sched_jitter_sec,
        max_instances=settings.sched_max_concurrent,
        next_run_time=now + timedelta(seconds=120),
        replace_existing=True,
    )
    _scheduler.start()
    return True


def stop_scheduler() -> None:
    global _scheduler
    if _scheduler is not None:
        _scheduler.shutdown(wait=False)
        _scheduler = None


def scheduler_is_running() -> bool:
    return _scheduler is not None and _scheduler.running


def scheduler_next_run_times() -> dict[str, datetime | None]:
    if _scheduler is None:
        return {}
    return {job.id: job.next_run_time for job in _scheduler.get_jobs()}
