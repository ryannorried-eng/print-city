import math

import pytest

from app.core.math import (
    american_to_decimal,
    american_to_implied_prob,
    book_clv,
    consensus_fair_prob,
    decimal_to_american,
    ev_percent,
    kelly_fraction,
    market_clv,
    parlay_decimal_odds,
    parlay_ev,
    parlay_prob,
    remove_vig,
)
from app.domain.enums import MarketKey, Side
from app.domain.types import Leg


def test_american_to_decimal_and_back_canonical_cases() -> None:
    canonical = [100, 150, 250, -110, -150, -200]
    for american in canonical:
        decimal = american_to_decimal(american)
        round_trip = decimal_to_american(decimal)
        assert round_trip == pytest.approx(american, abs=1)


def test_american_to_implied_prob_known_values() -> None:
    assert american_to_implied_prob(-110) == pytest.approx(110 / 210)
    assert american_to_implied_prob(150) == pytest.approx(100 / 250)


def test_remove_vig_sums_to_one() -> None:
    devigged = remove_vig([0.54, 0.52])
    assert sum(devigged) == pytest.approx(1.0)
    assert devigged[0] == pytest.approx(0.54 / 1.06)


def test_consensus_fair_prob_weighted_and_normalized() -> None:
    result = consensus_fair_prob(
        books_probs=[
            {Side.HOME: 0.60, Side.AWAY: 0.45},
            {Side.HOME: 0.58, Side.AWAY: 0.47},
        ],
        weights=[2.0, 1.0],
    )
    assert sum(result.values()) == pytest.approx(1.0)
    assert result[Side.HOME] > result[Side.AWAY]


def test_ev_percent_zero_at_breakeven() -> None:
    assert ev_percent(0.5, 2.0) == pytest.approx(0.0)


def test_kelly_fraction_behavior() -> None:
    assert kelly_fraction(0.5, 2.0) == pytest.approx(0.0)
    assert kelly_fraction(0.4, 2.0) == pytest.approx(0.0)

    fair_prob = 0.60
    odds = 2.0
    b = odds - 1
    full_kelly = ((b * fair_prob) - (1 - fair_prob)) / b
    quarter = 0.25 * full_kelly
    assert kelly_fraction(fair_prob, odds, max_cap=1.0) == pytest.approx(quarter)
    assert kelly_fraction(fair_prob, odds, max_cap=0.01) == pytest.approx(0.01)


def _legs() -> list[Leg]:
    return [
        Leg(
            event_id="1",
            market_key=MarketKey.H2H,
            side=Side.HOME,
            decimal_odds=1.91,
            fair_prob=0.55,
        ),
        Leg(
            event_id="2",
            market_key=MarketKey.TOTALS,
            side=Side.OVER,
            point=45.5,
            decimal_odds=1.87,
            fair_prob=0.52,
        ),
        Leg(
            event_id="3",
            market_key=MarketKey.SPREADS,
            side=Side.AWAY,
            point=3.5,
            decimal_odds=2.05,
            fair_prob=0.48,
        ),
    ]


def test_parlay_math_two_and_three_legs_matches_manual() -> None:
    legs = _legs()
    two_leg_manual_odds = legs[0].decimal_odds * legs[1].decimal_odds
    two_leg_manual_prob = legs[0].fair_prob * legs[1].fair_prob
    assert parlay_decimal_odds(legs[:2]) == pytest.approx(two_leg_manual_odds)
    assert parlay_prob(legs[:2]) == pytest.approx(two_leg_manual_prob)
    assert parlay_ev(legs[:2]) == pytest.approx((two_leg_manual_prob * two_leg_manual_odds) - 1)

    three_leg_manual_odds = math.prod([leg.decimal_odds for leg in legs])
    three_leg_manual_prob = math.prod([leg.fair_prob for leg in legs])
    assert parlay_decimal_odds(legs) == pytest.approx(three_leg_manual_odds)
    assert parlay_prob(legs) == pytest.approx(three_leg_manual_prob)
    assert parlay_ev(legs) == pytest.approx((three_leg_manual_prob * three_leg_manual_odds) - 1)


def test_clv_sign_correctness() -> None:
    assert market_clv(0.57, 0.53) > 0
    assert market_clv(0.50, 0.55) < 0
    assert book_clv(0.56, 0.52) > 0
    assert book_clv(0.48, 0.52) < 0


def test_validation_failures() -> None:
    with pytest.raises(ValueError):
        american_to_decimal(-99)
    with pytest.raises(ValueError):
        decimal_to_american(1.0)
    with pytest.raises(ValueError):
        remove_vig([])
    with pytest.raises(ValueError):
        consensus_fair_prob([], [])
    with pytest.raises(ValueError):
        parlay_prob([])
