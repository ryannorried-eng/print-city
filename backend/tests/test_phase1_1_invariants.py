from __future__ import annotations

from datetime import timezone
import sys
from types import SimpleNamespace

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.domain.enums import Side
from app.models import Base, Game, OddsGroup, OddsSnapshot
from app.services.ingest import (
    build_normalized_group_representation,
    ingest_odds_for_sport,
    parse_commence_time_to_utc,
)


def test_side_enum_values_are_canonical_uppercase() -> None:
    assert [side.value for side in Side] == ["HOME", "AWAY", "DRAW", "OVER", "UNDER"]


def test_parse_commence_time_normalizes_to_utc_aware() -> None:
    parsed = parse_commence_time_to_utc("2025-01-03T19:30:00-05:00")
    assert parsed.tzinfo is not None
    assert parsed.tzinfo == timezone.utc
    assert parsed.isoformat() == "2025-01-04T00:30:00+00:00"


def test_group_hash_ignores_derived_fields_but_changes_on_price_or_point() -> None:
    baseline = [
        {
            "side": "HOME",
            "american": -110,
            "decimal": 1.9090909,
            "implied_prob": 0.5238,
            "fair_prob": 0.5,
            "captured_at": "2025-01-01T00:00:00Z",
        },
        {
            "side": "AWAY",
            "american": -110,
            "decimal": 1.9090909,
            "implied_prob": 0.5238,
            "fair_prob": 0.5,
            "captured_at": "2025-01-01T00:00:00Z",
        },
    ]
    changed_only_derived = [
        {
            "side": "HOME",
            "american": -110,
            "decimal": 1.9090909,
            "implied_prob": 0.999,
            "fair_prob": 0.001,
            "captured_at": "2030-01-01T00:00:00Z",
        },
        {
            "side": "AWAY",
            "american": -110,
            "decimal": 1.9090909,
            "implied_prob": 0.001,
            "fair_prob": 0.999,
            "captured_at": "2030-01-01T00:00:00Z",
        },
    ]

    _, hash_one = build_normalized_group_representation("event1", "h2h", "draftkings", None, baseline)
    _, hash_two = build_normalized_group_representation(
        "event1", "h2h", "draftkings", None, changed_only_derived
    )
    assert hash_one == hash_two

    changed_price = [
        {"side": "HOME", "american": -115, "decimal": 1.8695652},
        {"side": "AWAY", "american": -105, "decimal": 1.9523810},
    ]
    _, hash_three = build_normalized_group_representation("event1", "h2h", "draftkings", None, changed_price)
    assert hash_three != hash_one

    _, hash_four = build_normalized_group_representation("event1", "h2h", "draftkings", 1.5, baseline)
    assert hash_four != hash_one


def test_group_hash_stable_with_different_side_ordering() -> None:
    side_prices = [
        {"side": "UNDER", "american": -105, "decimal": 1.9523810},
        {"side": "OVER", "american": -115, "decimal": 1.8695652},
    ]
    _, hash_one = build_normalized_group_representation("event1", "totals", "fanduel", 210.5, side_prices)
    _, hash_two = build_normalized_group_representation("event1", "totals", "fanduel", 210.5, list(reversed(side_prices)))
    assert hash_one == hash_two


def test_identical_payload_second_ingest_has_zero_changes(monkeypatch) -> None:
    payload = [
        {
            "id": "evt_1",
            "sport_key": "basketball_nba",
            "commence_time": "2025-01-04T00:30:00Z",
            "home_team": "Boston Celtics",
            "away_team": "Miami Heat",
            "bookmakers": [
                {
                    "key": "fanduel",
                    "markets": [
                        {
                            "key": "h2h",
                            "outcomes": [
                                {"name": "Miami Heat", "price": 102},
                                {"name": "Boston Celtics", "price": -120},
                            ],
                        }
                    ],
                },
                {
                    "key": "draftkings",
                    "markets": [
                        {
                            "key": "totals",
                            "outcomes": [
                                {"name": "Under", "price": -105, "point": 210.5},
                                {"name": "Over", "price": -115, "point": 210.5},
                            ],
                        }
                    ],
                },
            ],
        }
    ]

    monkeypatch.setenv("ODDS_SPORTS_WHITELIST", "basketball_nba")
    monkeypatch.setenv("ODDS_MARKETS", "h2h,totals")

    from app.config import get_settings

    get_settings.cache_clear()

    def fake_fetch_odds(*, sport_key, markets, regions, odds_format):
        assert sport_key == "basketball_nba"
        assert set(markets) == {"h2h", "totals"}
        return payload, {"headers": {}, "fetched_at": "2025-01-04T00:00:00Z"}

    monkeypatch.setitem(sys.modules, "app.integrations.odds_api", SimpleNamespace(fetch_odds=fake_fetch_odds))
    monkeypatch.setattr("app.services.ingest.record_quota", lambda *_args, **_kwargs: None)

    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        first_summary = ingest_odds_for_sport(session=session, sport_key="basketball_nba")
        second_summary = ingest_odds_for_sport(session=session, sport_key="basketball_nba")

        assert first_summary["groups_changed"] == 2
        assert first_summary["snapshot_rows_inserted"] == 4
        assert second_summary["groups_changed"] == 0
        assert second_summary["snapshot_rows_inserted"] == 0
        assert second_summary["groups_skipped"] == 2

        game = session.query(Game).filter_by(event_id="evt_1").one()
        assert game is not None


def test_datetime_columns_are_timezone_aware() -> None:
    assert Game.__table__.c.commence_time.type.timezone is True
    assert OddsGroup.__table__.c.last_captured_at.type.timezone is True
    assert OddsSnapshot.__table__.c.captured_at.type.timezone is True
