import pytest

from app.domain.enums import Side
from app.services.ingest import compute_fair_probs_for_group, normalize_side


def test_normalize_side_h2h_case_insensitive() -> None:
    assert normalize_side("Boston Celtics", "Miami Heat", "h2h", "boston celtics") == Side.HOME
    assert normalize_side("Boston Celtics", "Miami Heat", "spreads", "MIAMI HEAT") == Side.AWAY


def test_normalize_side_totals() -> None:
    assert normalize_side("A", "B", "totals", "Over") == Side.OVER
    assert normalize_side("A", "B", "totals", "under") == Side.UNDER


def test_normalize_side_raises_unknown_name() -> None:
    with pytest.raises(ValueError):
        normalize_side("Boston Celtics", "Miami Heat", "h2h", "Celtics")


def test_normalize_side_soccer_h2h_draw() -> None:
    assert normalize_side("Arsenal", "Chelsea", "h2h", "Draw", sport_key="soccer_epl") == Side.DRAW


def test_normalize_side_non_soccer_h2h_draw_raises() -> None:
    with pytest.raises(ValueError):
        normalize_side("Boston Celtics", "Miami Heat", "h2h", "Draw", sport_key="basketball_nba")


def test_compute_fair_probs_group_sums_to_one() -> None:
    fair_probs = compute_fair_probs_for_group([0.52, 0.54])
    assert sum(fair_probs) == pytest.approx(1.0)


def test_soccer_draw_side_value_is_persistable() -> None:
    side = normalize_side("Arsenal", "Chelsea", "h2h", "Draw", sport_key="soccer_epl")
    assert side.value == "DRAW"
