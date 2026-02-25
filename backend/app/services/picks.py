from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from sqlalchemy import and_, desc, select
from sqlalchemy.orm import Session

from app.config import get_settings
from app.core.math import ev_percent, kelly_fraction
from app.domain.enums import PickScoreDecision
from app.intelligence.features import compute_features
from app.intelligence.pqs import adaptive_thresholds, score_pick
from app.intelligence.priors import get_latest_prior
from app.models import Game, Pick, PickScore
from app.services.consensus import build_market_views, compute_consensus_for_view, get_latest_group_rows


def _to_decimal(value: float, places: str) -> Decimal:
    return Decimal(str(value)).quantize(Decimal(places))


def _base_summary() -> dict[str, int]:
    return {
        "total_views": 0,
        "candidates": 0,
        "inserted": 0,
        "scored": 0,
        "kept": 0,
        "dropped": 0,
        "skipped_existing": 0,
        "skipped_low_ev": 0,
        "skipped_insufficient_books": 0,
    }




def _select_final_keep_ids(
    kept_candidates: list[tuple[float, str, str, str, str, datetime, int, int]],
    run_max_picks_total: int,
) -> set[int]:
    kept_sorted = sorted(
        kept_candidates,
        key=lambda item: (-item[0], item[1], item[2], item[3], item[5], item[6]),
    )
    per_sport: dict[str, int] = {}
    final_keep_ids: set[int] = set()
    for _pqs, sport, _market, _event, _side, _created_at, pick_id, adaptive_max_picks in kept_sorted:
        max_sport = max(1, adaptive_max_picks)
        if per_sport.get(sport, 0) >= max_sport:
            continue
        if len(final_keep_ids) >= run_max_picks_total:
            break
        per_sport[sport] = per_sport.get(sport, 0) + 1
        final_keep_ids.add(pick_id)
    return final_keep_ids

def generate_consensus_picks(session: Session, sport_key: str, market_key: str, as_of: datetime | None = None) -> dict[str, int]:
    settings = get_settings()
    rows = get_latest_group_rows(session=session, sport_key=sport_key, market_key=market_key)
    views = build_market_views(rows)
    results = [compute_consensus_for_view(views[key]) for key in sorted(views.keys())]
    summary = _base_summary()
    summary["total_views"] = len(results)
    if not results:
        return summary

    event_to_game_id = {
        event_id: game_id
        for event_id, game_id in session.execute(
            select(Game.event_id, Game.id).where(Game.event_id.in_([result.event_id for result in results]))
        ).all()
    }

    kept_candidates: list[tuple[float, str, str, str, str, datetime, int, int]] = []
    new_pick_ids: set[int] = set()

    now_utc = datetime.now(timezone.utc)
    for result in results:
        view_key = (result.event_id, result.market_key, result.point)
        market_view = views.get(view_key)
        if market_view is None:
            continue

        if result.consensus_probs is None or result.included_books < settings.pick_min_books:
            summary["skipped_insufficient_books"] += 1
            continue

        game_id = event_to_game_id.get(result.event_id)
        if game_id is None:
            continue

        for side, probability in result.consensus_probs.items():
            best_decimal = result.best_decimal.get(side)
            best_book = result.best_book.get(side)
            if best_decimal is None or best_book is None:
                continue
            summary["candidates"] += 1

            ev = ev_percent(probability, best_decimal)
            if ev < settings.pick_min_ev:
                summary["skipped_low_ev"] += 1
                continue

            kelly = min(
                settings.kelly_cap,
                kelly_fraction(probability, best_decimal, kelly_multiplier=settings.kelly_multiplier, max_cap=settings.kelly_max_cap),
            )
            if kelly <= 0:
                continue

            point_value = Decimal(str(result.point)) if result.point is not None else None
            exists_conditions = [
                Pick.game_id == game_id,
                Pick.market_key == result.market_key,
                Pick.side == side.value,
                Pick.best_book == best_book,
                Pick.captured_at_max == result.captured_at_max,
            ]
            exists_conditions.append(Pick.point.is_(None) if point_value is None else Pick.point == point_value)
            existing_pick = session.execute(select(Pick).where(and_(*exists_conditions)).limit(1)).scalars().first()

            if existing_pick is not None:
                pick = existing_pick
                summary["skipped_existing"] += 1
            else:
                stake = settings.bankroll_paper * kelly
                pick = Pick(
                    game_id=game_id,
                    market_key=result.market_key,
                    side=side.value,
                    point=point_value,
                    source="CONSENSUS",
                    consensus_prob=_to_decimal(probability, "0.00000001"),
                    best_decimal=_to_decimal(best_decimal, "0.00001"),
                    best_book=best_book,
                    ev=_to_decimal(ev, "0.00000001"),
                    kelly_fraction=_to_decimal(kelly, "0.00000001"),
                    stake=_to_decimal(stake, "0.0001"),
                    consensus_books=result.included_books,
                    sharp_books=result.sharp_books_included,
                    captured_at_min=result.captured_at_min,
                    captured_at_max=result.captured_at_max,
                )
                if as_of is not None:
                    pick.created_at = as_of
                session.add(pick)
                session.flush()
                new_pick_ids.add(pick.id)

            prior = get_latest_prior(session, sport_key=sport_key, market_key=market_key, window_size=settings.clv_prior_window)
            features = compute_features(
                result=result,
                side=side,
                per_book_odds=market_view.book_odds,
                ev=ev,
                kelly_fraction=kelly,
                best_decimal=best_decimal,
                side_consensus_prob=probability,
                now_utc=now_utc,
                pick_id=pick.id,
            )
            pqs_result = score_pick(features=features, settings=settings, prior=prior, sport_key=sport_key)
            summary["scored"] += 1

            components = dict(pqs_result.components)
            min_pqs, max_picks = adaptive_thresholds(settings, prior, sport_key=sport_key)
            components["adaptive_min_pqs"] = min_pqs
            components["adaptive_max_picks"] = float(max_picks)

            score_exists = session.execute(
                select(PickScore.id).where(PickScore.pick_id == pick.id, PickScore.version == settings.pqs_version)
            ).scalar_one_or_none()
            if score_exists is None:
                session.add(
                    PickScore(
                        pick_id=pick.id,
                        scored_at=now_utc,
                        version=settings.pqs_version,
                        pqs=_to_decimal(pqs_result.pqs, "0.000001"),
                        components_json=components,
                        features_json={
                            "ev": round(features.ev, 8),
                            "kelly_fraction": round(features.kelly_fraction, 8),
                            "book_count": features.book_count,
                            "sharp_book_count": features.sharp_book_count,
                            "agreement_strength": round(features.agreement_strength, 8),
                            "price_dispersion": round(features.price_dispersion, 8),
                            "best_vs_consensus_edge": round(features.best_vs_consensus_edge, 8),
                            "time_to_start_minutes": round(features.time_to_start_minutes, 6),
                            "market_liquidity_proxy": round(features.market_liquidity_proxy, 6),
                        },
                        decision=pqs_result.decision.value,
                        drop_reason=pqs_result.drop_reason,
                    )
                )

            if pqs_result.decision == PickScoreDecision.KEEP:
                summary["kept"] += 1
                kept_candidates.append(
                    (
                        pqs_result.pqs,
                        sport_key,
                        result.market_key,
                        result.event_id,
                        side.value,
                        pick.created_at,
                        pick.id,
                        int(components.get("adaptive_max_picks", settings.sport_default_max_picks)),
                    )
                )
            else:
                summary["dropped"] += 1

    final_keep_ids = _select_final_keep_ids(kept_candidates, settings.run_max_picks_total)

    if settings.pqs_enabled:
        throttled_candidates = sorted(
            kept_candidates,
            key=lambda item: (-item[0], item[1], item[2], item[3], item[5], item[6]),
        )
        for item in throttled_candidates:
            pick_id = item[6]
            if pick_id not in final_keep_ids:
                score_row = session.execute(
                    select(PickScore).where(PickScore.pick_id == pick_id, PickScore.version == settings.pqs_version)
                ).scalars().first()
                if score_row is not None:
                    score_row.decision = PickScoreDecision.DROP.value
                    score_row.drop_reason = "cap_throttle"

    summary["inserted"] = len(final_keep_ids.intersection(new_pick_ids))
    session.commit()
    return summary


def list_picks(session: Session, sport_key: str | None, market_key: str | None, date: str | None, limit: int = 100) -> list[dict[str, object]]:
    stmt = select(Pick, Game).join(Game, Pick.game_id == Game.id).order_by(desc(Pick.created_at), desc(Pick.id)).limit(limit)
    conditions = []
    if sport_key:
        conditions.append(Game.sport_key == sport_key)
    if market_key:
        conditions.append(Pick.market_key == market_key)
    if date:
        day = datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        day_end = day + timedelta(days=1)
        conditions.append(and_(Pick.created_at >= day, Pick.created_at < day_end))
    if conditions:
        stmt = stmt.where(and_(*conditions))

    rows = session.execute(stmt).all()
    output: list[dict[str, object]] = []
    for pick, game in rows:
        score = (
            session.execute(
                select(PickScore)
                .where(PickScore.pick_id == pick.id, PickScore.version == get_settings().pqs_version)
                .order_by(desc(PickScore.scored_at), desc(PickScore.id))
                .limit(1)
            )
            .scalars()
            .first()
        )
        if score is None or score.decision not in {PickScoreDecision.KEEP.value, PickScoreDecision.WARN.value}:
            continue
        output.append({
            "id": pick.id,
            "created_at": pick.created_at,
            "sport_key": game.sport_key,
            "event_id": game.event_id,
            "commence_time": game.commence_time,
            "home_team": game.home_team,
            "away_team": game.away_team,
            "market_key": pick.market_key,
            "side": pick.side,
            "point": float(pick.point) if pick.point is not None else None,
            "source": pick.source,
            "consensus_prob": float(pick.consensus_prob),
            "best_decimal": float(pick.best_decimal),
            "best_book": pick.best_book,
            "ev": float(pick.ev),
            "kelly_fraction": float(pick.kelly_fraction),
            "stake": float(pick.stake),
            "consensus_books": pick.consensus_books,
            "sharp_books": pick.sharp_books,
            "captured_at_min": pick.captured_at_min,
            "captured_at_max": pick.captured_at_max,
            "pqs": float(score.pqs),
            "pqs_decision": score.decision,
        })
    return output


def list_pick_scores(
    session: Session,
    *,
    sport_key: str | None,
    decision: str | None,
    min_pqs: float | None,
    version: str,
    limit: int,
) -> list[dict[str, object]]:
    stmt = (
        select(PickScore, Pick, Game)
        .join(Pick, PickScore.pick_id == Pick.id)
        .join(Game, Pick.game_id == Game.id)
        .where(PickScore.version == version)
        .order_by(desc(PickScore.scored_at), desc(PickScore.id))
        .limit(limit)
    )
    if sport_key:
        stmt = stmt.where(Game.sport_key == sport_key)
    if decision:
        stmt = stmt.where(PickScore.decision == decision)
    if min_pqs is not None:
        stmt = stmt.where(PickScore.pqs >= _to_decimal(min_pqs, "0.000001"))

    rows = session.execute(stmt).all()
    return [
        {
            "pick_id": pick.id,
            "event_id": game.event_id,
            "sport_key": game.sport_key,
            "market_key": pick.market_key,
            "side": pick.side,
            "pqs": float(score.pqs),
            "version": score.version,
            "decision": score.decision,
            "drop_reason": score.drop_reason,
            "components": score.components_json,
            "features": score.features_json,
            "scored_at": score.scored_at,
        }
        for score, pick, game in rows
    ]
