from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

from app.config import get_settings
from app.core.math import american_to_decimal, american_to_implied_prob, remove_vig
from app.domain.enums import Side
from app.services.quota import record_quota


def normalize_side(
    event_home: str,
    event_away: str,
    market_key: str,
    outcome_name: str,
    sport_key: str | None = None,
) -> Side:
    market = market_key.lower()
    normalized_outcome = outcome_name.strip().lower()
    if market in {"h2h", "spreads"}:
        if normalized_outcome == event_home.strip().lower():
            return Side.HOME
        if normalized_outcome == event_away.strip().lower():
            return Side.AWAY
        if market == "h2h" and sport_key is not None and sport_key.startswith("soccer_") and normalized_outcome == "draw":
            return Side.DRAW
        raise ValueError(
            f"Could not map team outcome '{outcome_name}' to home='{event_home}' or away='{event_away}'"
        )

    if market == "totals":
        normalized = outcome_name.strip().lower()
        if normalized == "over":
            return Side.OVER
        if normalized == "under":
            return Side.UNDER
        raise ValueError(f"Could not map totals outcome '{outcome_name}' to OVER/UNDER")

    raise ValueError(f"Unsupported market_key '{market_key}'")


def build_normalized_group_representation(
    event_id: str,
    market_key: str,
    bookmaker: str,
    point: float | None,
    side_prices: list[dict],
) -> tuple[dict, str]:
    normalized = {
        "event_id": event_id,
        "market_key": market_key,
        "bookmaker": bookmaker,
        "point": point,
        "sides": sorted(
            [
                {
                    "side": side_price["side"],
                    "american": side_price.get("american"),
                    "decimal": side_price.get("decimal"),
                }
                for side_price in side_prices
            ],
            key=lambda x: x["side"],
        ),
    }
    payload = json.dumps(normalized, sort_keys=True, separators=(",", ":"))
    group_hash = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    return normalized, group_hash


def compute_fair_probs_for_group(implied_probs: list[float], epsilon: float = 1e-6) -> list[float]:
    fair_probs = remove_vig(implied_probs)
    if abs(sum(fair_probs) - 1.0) > epsilon:
        raise ValueError("Devigged probabilities must sum to ~1.0")
    return fair_probs


def parse_commence_time_to_utc(commence_time: str) -> datetime:
    parsed = datetime.fromisoformat(commence_time.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def sort_group_key(event_id: str, group_key: tuple[str, str, float | None]) -> tuple[str, str, str, float]:
    market_key, bookmaker, point = group_key
    return (event_id, market_key, bookmaker, float("-inf") if point is None else point)


def ingest_odds_for_sport(session: "Session", sport_key: str) -> dict:
    from sqlalchemy import select

    from app.integrations.odds_api import fetch_odds
    from app.models import Game, OddsGroup, OddsSnapshot

    settings = get_settings()
    if sport_key not in settings.odds_sports_whitelist:
        raise ValueError(f"sport_key '{sport_key}' is not in ODDS_SPORTS_WHITELIST")

    events, quota_info = fetch_odds(
        sport_key=sport_key,
        markets=list(settings.odds_markets),
        regions=settings.odds_regions,
        odds_format="american",
    )
    record_quota(quota_info["headers"], quota_info["fetched_at"])

    summary = {
        "games_upserted": 0,
        "groups_changed": 0,
        "snapshot_rows_inserted": 0,
        "groups_skipped": 0,
        "errors_count": 0,
    }

    bookmaker_filter = set(settings.bookmaker_whitelist)
    captured_at = datetime.now(timezone.utc)

    for event in events:
        try:
            game = session.execute(select(Game).where(Game.event_id == event["id"])).scalar_one_or_none()
            if game is None:
                game = Game(
                    sport_key=event["sport_key"],
                    event_id=event["id"],
                    commence_time=parse_commence_time_to_utc(event["commence_time"]),
                    home_team=event["home_team"],
                    away_team=event["away_team"],
                )
                session.add(game)
                session.flush()
                summary["games_upserted"] += 1
            else:
                game.sport_key = event["sport_key"]
                game.commence_time = parse_commence_time_to_utc(event["commence_time"])
                game.home_team = event["home_team"]
                game.away_team = event["away_team"]
                summary["games_upserted"] += 1

            for book in sorted(event.get("bookmakers", []), key=lambda b: b["key"]):
                bookmaker_key = book["key"]
                if bookmaker_filter and bookmaker_key not in bookmaker_filter:
                    continue

                grouped_outcomes: dict[tuple[str, str, float | None], list[dict]] = defaultdict(list)
                for market in sorted(book.get("markets", []), key=lambda m: m["key"]):
                    market_key = market["key"]
                    if market_key not in settings.odds_markets:
                        continue
                    for outcome in market.get("outcomes", []):
                        side = normalize_side(
                            event_home=event["home_team"],
                            event_away=event["away_team"],
                            market_key=market_key,
                            outcome_name=outcome["name"],
                            sport_key=event["sport_key"],
                        )
                        point = outcome.get("point")
                        american = outcome.get("price")
                        decimal = american_to_decimal(american) if american is not None else None
                        implied = american_to_implied_prob(american) if american is not None else None
                        grouped_outcomes[(market_key, bookmaker_key, point)].append(
                            {
                                "side": side.value,
                                "american": american,
                                "decimal": decimal,
                                "implied_prob": implied,
                                "point": point,
                            }
                        )

                for market_key, bookmaker, point in sorted(
                    grouped_outcomes.keys(), key=lambda key: sort_group_key(event["id"], key)
                ):
                    side_prices = sorted(grouped_outcomes[(market_key, bookmaker, point)], key=lambda sp: sp["side"])
                    _, group_hash = build_normalized_group_representation(
                        event_id=event["id"],
                        market_key=market_key,
                        bookmaker=bookmaker,
                        point=point,
                        side_prices=side_prices,
                    )
                    existing_group = session.execute(
                        select(OddsGroup).where(
                            OddsGroup.game_id == game.id,
                            OddsGroup.market_key == market_key,
                            OddsGroup.bookmaker == bookmaker,
                            OddsGroup.point == point,
                        )
                    ).scalar_one_or_none()

                    if existing_group and existing_group.last_hash == group_hash:
                        summary["groups_skipped"] += 1
                        continue

                    implied_probs = [sp["implied_prob"] for sp in side_prices if sp["implied_prob"] is not None]
                    fair_probs = compute_fair_probs_for_group(implied_probs)

                    for side_price, fair_prob in zip(side_prices, fair_probs, strict=True):
                        snapshot = OddsSnapshot(
                            game_id=game.id,
                            captured_at=captured_at,
                            market_key=market_key,
                            bookmaker=bookmaker,
                            side=side_price["side"],
                            point=Decimal(str(side_price["point"])) if side_price["point"] is not None else None,
                            american=side_price["american"],
                            decimal=Decimal(str(side_price["decimal"])) if side_price["decimal"] is not None else None,
                            implied_prob=Decimal(str(side_price["implied_prob"])),
                            fair_prob=Decimal(str(fair_prob)),
                            group_hash=group_hash,
                        )
                        session.add(snapshot)
                        summary["snapshot_rows_inserted"] += 1

                    if existing_group is None:
                        session.add(
                            OddsGroup(
                                game_id=game.id,
                                market_key=market_key,
                                bookmaker=bookmaker,
                                point=Decimal(str(point)) if point is not None else None,
                                last_hash=group_hash,
                                last_captured_at=captured_at,
                            )
                        )
                    else:
                        existing_group.last_hash = group_hash
                        existing_group.last_captured_at = captured_at
                    summary["groups_changed"] += 1

        except Exception:
            summary["errors_count"] += 1
            if settings.delta_hash_strict:
                raise

    session.commit()
    return summary
