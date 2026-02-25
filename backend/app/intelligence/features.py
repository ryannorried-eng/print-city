from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import logging

from app.core.math import remove_vig
from app.domain.enums import Side
from app.services.consensus import ConsensusResult

logger = logging.getLogger(__name__)


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


def _clamp(value: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, value))


def _percentile(sorted_values: list[float], q: float) -> float:
    if not sorted_values:
        raise ValueError("sorted_values must not be empty")
    if len(sorted_values) == 1:
        return sorted_values[0]

    position = (len(sorted_values) - 1) * q
    lower_index = int(position)
    upper_index = min(lower_index + 1, len(sorted_values) - 1)
    fraction = position - lower_index
    return sorted_values[lower_index] + ((sorted_values[upper_index] - sorted_values[lower_index]) * fraction)


def _opposite_side(side: Side) -> Side:
    if side == Side.HOME:
        return Side.AWAY
    if side == Side.AWAY:
        return Side.HOME
    if side == Side.OVER:
        return Side.UNDER
    return Side.OVER


def compute_price_dispersion(*, side: Side, book_odds: dict[str, dict[Side, float]]) -> float:
    probabilities: list[float] = []
    opposite = _opposite_side(side)
    contributing_odds: list[dict[str, float | str]] = []

    for book in sorted(book_odds.keys()):
        per_book = book_odds[book]
        side_decimal = per_book.get(side)
        if side_decimal is None or side_decimal <= 1.0:
            continue

        side_implied = 1.0 / side_decimal
        opposite_decimal = per_book.get(opposite)
        if opposite_decimal is not None and opposite_decimal > 1.0:
            opposite_implied = 1.0 / opposite_decimal
            side_implied = remove_vig([side_implied, opposite_implied])[0]

        clamped = _clamp(side_implied)
        probabilities.append(clamped)
        contributing_odds.append(
            {
                "book": book,
                "side_decimal": side_decimal,
                "opposite_decimal": opposite_decimal if opposite_decimal is not None else float("nan"),
                "fair_prob": clamped,
            }
        )

    if len(probabilities) < 3:
        return 1.0

    ordered = sorted(probabilities)
    dispersion = _percentile(ordered, 0.9) - _percentile(ordered, 0.1)
    dispersion = _clamp(dispersion)

    if dispersion > 0.25:
        logger.warning(
            "High price dispersion detected: side=%s dispersion=%.6f details=%s",
            side.value,
            dispersion,
            contributing_odds,
        )

    return dispersion


def compute_features(
    *,
    result: ConsensusResult,
    side: Side,
    per_book_odds: dict[str, dict[Side, float]],
    ev: float,
    kelly_fraction: float,
    best_decimal: float,
    side_consensus_prob: float,
    now_utc: datetime,
) -> PickFeatures:
    consensus_implied = side_consensus_prob
    dispersion = compute_price_dispersion(side=side, book_odds=per_book_odds)
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
