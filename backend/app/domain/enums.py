from enum import StrEnum


class MarketKey(StrEnum):
    H2H = "h2h"
    SPREADS = "spreads"
    TOTALS = "totals"


class Side(StrEnum):
    HOME = "HOME"
    AWAY = "AWAY"
    DRAW = "DRAW"
    OVER = "OVER"
    UNDER = "UNDER"
