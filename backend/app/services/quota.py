from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

_quota_state: dict[str, Any] = {"headers": {}, "fetched_at": None}


def record_quota(headers: dict[str, str], fetched_at: datetime) -> None:
    _quota_state["headers"] = headers
    _quota_state["fetched_at"] = fetched_at.astimezone(timezone.utc).isoformat()


def get_quota_state() -> dict[str, Any]:
    return {"headers": dict(_quota_state["headers"]), "fetched_at": _quota_state["fetched_at"]}
