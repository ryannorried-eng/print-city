from __future__ import annotations

import math
from collections.abc import Sequence

from app.domain.enums import Side
from app.domain.types import Leg

EPS = 1e-9


def _ensure_finite(value: float, name: str) -> float:
    if not math.isfinite(value):
        raise ValueError(f"{name} must be finite")
    return value


def _validate_probability(value: float, name: str) -> float:
    value = _ensure_finite(value, name)
    if value < 0.0 or value > 1.0:
        raise ValueError(f"{name} must be between 0 and 1 inclusive")
    return value


def american_to_decimal(american_odds: float) -> float:
    american_odds = _ensure_finite(float(american_odds), "american_odds")
    if abs(american_odds) < 100:
        raise ValueError("american_odds must be <= -100 or >= 100")
    if american_odds > 0:
        return 1.0 + (american_odds / 100.0)
    return 1.0 + (100.0 / abs(american_odds))


def decimal_to_american(decimal_odds: float) -> int:
    decimal_odds = _ensure_finite(float(decimal_odds), "decimal_odds")
    if decimal_odds <= 1.0:
        raise ValueError("decimal_odds must be greater than 1")
    if decimal_odds >= 2.0:
        return int(round((decimal_odds - 1.0) * 100.0))
    return int(round(-100.0 / (decimal_odds - 1.0)))


def american_to_implied_prob(american_odds: float) -> float:
    american_odds = _ensure_finite(float(american_odds), "american_odds")
    if abs(american_odds) < 100:
        raise ValueError("american_odds must be <= -100 or >= 100")
    if american_odds > 0:
        return 100.0 / (american_odds + 100.0)
    return abs(american_odds) / (abs(american_odds) + 100.0)


def remove_vig(probs: Sequence[float]) -> list[float]:
    if not probs:
        raise ValueError("probs must not be empty")
    validated = [_validate_probability(float(p), "probability") for p in probs]
    total = sum(validated)
    if total <= EPS:
        raise ValueError("sum of probabilities must be greater than zero")
    return [p / total for p in validated]


def consensus_fair_prob(
    books_probs: list[dict[Side, float]], weights: list[float]
) -> dict[Side, float]:
    if not books_probs:
        raise ValueError("books_probs must not be empty")
    if len(books_probs) != len(weights):
        raise ValueError("books_probs and weights lengths must match")

    cleaned_weights = [_ensure_finite(float(w), "weight") for w in weights]
    if any(w < 0 for w in cleaned_weights):
        raise ValueError("weights must be non-negative")
    total_weight = sum(cleaned_weights)
    if total_weight <= EPS:
        raise ValueError("weights must sum to greater than zero")

    sides = list(books_probs[0].keys())
    if not sides:
        raise ValueError("each books_probs entry must contain at least one side")
    for book in books_probs:
        if set(book.keys()) != set(sides):
            raise ValueError("all books_probs entries must have the same sides")

    weighted: dict[Side, float] = {side: 0.0 for side in sides}
    for book_probs, weight in zip(books_probs, cleaned_weights, strict=True):
        for side, prob in book_probs.items():
            weighted[side] += _validate_probability(float(prob), f"probability[{side}]") * weight

    normalized = remove_vig([weighted[side] / total_weight for side in sides])
    return {side: normalized[idx] for idx, side in enumerate(sides)}


def ev_percent(fair_prob: float, best_decimal_odds: float) -> float:
    fair_prob = _validate_probability(float(fair_prob), "fair_prob")
    best_decimal_odds = _ensure_finite(float(best_decimal_odds), "best_decimal_odds")
    if best_decimal_odds <= 1.0:
        raise ValueError("best_decimal_odds must be greater than 1")
    return (fair_prob * best_decimal_odds) - 1.0


def kelly_fraction(
    fair_prob: float,
    best_decimal_odds: float,
    kelly_multiplier: float = 0.25,
    max_cap: float = 0.05,
) -> float:
    fair_prob = _validate_probability(float(fair_prob), "fair_prob")
    best_decimal_odds = _ensure_finite(float(best_decimal_odds), "best_decimal_odds")
    kelly_multiplier = _ensure_finite(float(kelly_multiplier), "kelly_multiplier")
    max_cap = _ensure_finite(float(max_cap), "max_cap")

    if best_decimal_odds <= 1.0:
        raise ValueError("best_decimal_odds must be greater than 1")
    if kelly_multiplier < 0.0:
        raise ValueError("kelly_multiplier must be non-negative")
    if max_cap < 0.0:
        raise ValueError("max_cap must be non-negative")

    b = best_decimal_odds - 1.0
    q = 1.0 - fair_prob
    full_kelly = ((b * fair_prob) - q) / b
    if full_kelly <= 0.0:
        return 0.0
    return min(max_cap, kelly_multiplier * full_kelly)


def _validate_legs(legs: Sequence[Leg]) -> None:
    if not legs:
        raise ValueError("legs must not be empty")


def parlay_decimal_odds(legs: Sequence[Leg]) -> float:
    _validate_legs(legs)
    product = 1.0
    for leg in legs:
        product *= _ensure_finite(float(leg.decimal_odds), "leg.decimal_odds")
    if product <= 1.0:
        raise ValueError("parlay decimal odds must be greater than 1")
    return product


def parlay_prob(legs: Sequence[Leg]) -> float:
    _validate_legs(legs)
    product = 1.0
    for leg in legs:
        product *= _validate_probability(float(leg.fair_prob), "leg.fair_prob")
    return product


def parlay_ev(legs: Sequence[Leg]) -> float:
    return ev_percent(parlay_prob(legs), parlay_decimal_odds(legs))


def market_clv(closing_consensus_prob: float, pick_time_consensus_prob: float) -> float:
    closing_consensus_prob = _validate_probability(
        float(closing_consensus_prob), "closing_consensus_prob"
    )
    pick_time_consensus_prob = _validate_probability(
        float(pick_time_consensus_prob), "pick_time_consensus_prob"
    )
    return closing_consensus_prob - pick_time_consensus_prob


def book_clv(closing_book_implied_prob: float, pick_time_book_implied_prob: float) -> float:
    closing_book_implied_prob = _validate_probability(
        float(closing_book_implied_prob), "closing_book_implied_prob"
    )
    pick_time_book_implied_prob = _validate_probability(
        float(pick_time_book_implied_prob), "pick_time_book_implied_prob"
    )
    return closing_book_implied_prob - pick_time_book_implied_prob
