from enum import StrEnum


class MarketKey(StrEnum):
    H2H = "h2h"
    SPREADS = "spreads"
    TOTALS = "totals"


class Side(StrEnum):
    HOME = "HOME"
    AWAY = "AWAY"
    OVER = "OVER"
    UNDER = "UNDER"


class PickScoreDecision(StrEnum):
    KEEP = "KEEP"
    DROP = "DROP"
    WARN = "WARN"
