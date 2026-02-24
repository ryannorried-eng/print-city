from app.services.ingest import build_normalized_group_representation


def test_group_hash_stability_same_input_same_hash() -> None:
    side_prices = [
        {"side": "AWAY", "american": -110, "decimal": 1.9090909},
        {"side": "HOME", "american": -110, "decimal": 1.9090909},
    ]
    _, hash_one = build_normalized_group_representation("event1", "h2h", "draftkings", None, side_prices)
    _, hash_two = build_normalized_group_representation("event1", "h2h", "draftkings", None, list(reversed(side_prices)))
    assert hash_one == hash_two


def test_group_hash_changes_when_price_changes() -> None:
    original = [
        {"side": "HOME", "american": -110, "decimal": 1.9090909},
        {"side": "AWAY", "american": -110, "decimal": 1.9090909},
    ]
    changed = [
        {"side": "HOME", "american": -115, "decimal": 1.8695652},
        {"side": "AWAY", "american": -105, "decimal": 1.9523810},
    ]
    _, hash_one = build_normalized_group_representation("event1", "h2h", "draftkings", None, original)
    _, hash_two = build_normalized_group_representation("event1", "h2h", "draftkings", None, changed)
    assert hash_one != hash_two
