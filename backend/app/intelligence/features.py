from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from app.services.consensus import ConsensusResult


@dataclass(frozen=True)
class PickFeatures:
    ev: float
    kelly_fraction: float
    book_count: int
    sharp_book_count: int
    agreement_strength: float
    price_dispersion: float
    best_vs_consensus_edge: float
    time_to_start_minutes: float
    market_liquidity_proxy: float


def compute_features(
    *,
    result: ConsensusResult,
    side_probabilities: list[float],
    ev: float,
    kelly_fraction: float,
    best_decimal: float,
    side_consensus_prob: float,
    now_utc: datetime,
) -> PickFeatures:
    consensus_implied = side_consensus_prob
    dispersion = max(side_probabilities) - min(side_probabilities) if side_probabilities else 1.0
    agreement = max(0.0, min(1.0, 1.0 - (dispersion / 0.5)))
    commence_time = result.commence_time if result.commence_time.tzinfo is not None else result.commence_time.replace(tzinfo=timezone.utc)
    return PickFeatures(
        ev=ev,
        kelly_fraction=kelly_fraction,
        book_count=result.included_books,
        sharp_book_count=result.sharp_books_included,
        agreement_strength=agreement,
        price_dispersion=dispersion,
        best_vs_consensus_edge=consensus_implied - (1.0 / best_decimal),
        time_to_start_minutes=(commence_time - now_utc).total_seconds() / 60.0,
        market_liquidity_proxy=float(result.included_books) + (2.0 * float(result.sharp_books_included)),
    )
