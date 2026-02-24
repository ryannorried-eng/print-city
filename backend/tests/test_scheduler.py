from __future__ import annotations

from app.config import get_settings
from app.core import scheduler as sched


def test_scheduler_disabled(monkeypatch) -> None:
    monkeypatch.setenv("ENABLE_SCHEDULER", "false")
    get_settings.cache_clear()
    settings = get_settings()

    started = sched.start_scheduler(settings)
    assert started is False
    assert sched.scheduler_is_running() is False


def test_scheduler_skips_when_db_unreachable(monkeypatch) -> None:
    monkeypatch.setenv("ENABLE_SCHEDULER", "true")
    monkeypatch.setenv("SCHED_REQUIRE_DB", "true")
    get_settings.cache_clear()
    settings = get_settings()

    monkeypatch.setattr(sched, "_can_reach_db", lambda: False)

    started = sched.start_scheduler(settings)
    assert started is False
    assert sched.scheduler_is_running() is False

