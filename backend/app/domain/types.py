from dataclasses import dataclass

from app.domain.enums import MarketKey, Side


@dataclass(frozen=True, slots=True)
class Leg:
    event_id: str
    market_key: MarketKey
    side: Side
    decimal_odds: float
    fair_prob: float
    point: float | None = None
    book: str | None = None

    def __post_init__(self) -> None:
        if not self.event_id.strip():
            raise ValueError("event_id must not be empty")
        if self.decimal_odds <= 1.0:
            raise ValueError("decimal_odds must be greater than 1")
        if not (0.0 <= self.fair_prob <= 1.0):
            raise ValueError("fair_prob must be between 0 and 1 inclusive")
