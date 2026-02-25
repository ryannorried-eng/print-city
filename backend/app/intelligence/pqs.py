from __future__ import annotations

from dataclasses import dataclass

from app.config import Settings
from app.domain.enums import PickScoreDecision
from app.intelligence.features import PickFeatures
from app.models import ClvSportStat


@dataclass(frozen=True)
class PQSResult:
    pqs: float
    decision: PickScoreDecision
    drop_reason: str | None
    components: dict[str, float]


def _clamp(v: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, v))


def adaptive_thresholds(settings: Settings, prior: ClvSportStat | None, sport_key: str | None = None) -> tuple[float, int]:
    min_pqs = settings.sport_default_min_pqs
    max_picks = settings.ncaab_default_max_picks if sport_key == "basketball_ncaab" else settings.sport_default_max_picks
    if prior is None:
        return min_pqs, max_picks

    pct = float(prior.pct_positive_market_clv)
    if pct < 0.45:
        min_pqs = min(0.9, min_pqs + 0.05)
        max_picks = max(1, max_picks - 1)
    elif pct > 0.6 and prior.is_weak == 0:
        min_pqs = max(0.55, min_pqs - 0.02)
    return round(min_pqs, 6), max_picks




def adaptive_max_price_dispersion(settings: Settings, features: PickFeatures) -> float:
    adaptive_max = settings.max_price_dispersion
    if features.book_count >= 8:
        adaptive_max = max(adaptive_max, settings.max_price_dispersion_book_count_8)
    if features.sharp_book_count >= 2 and features.ev >= 0.05:
        adaptive_max = max(adaptive_max, settings.max_price_dispersion_sharp_ev)
    return adaptive_max


def adaptive_min_minutes_to_start(settings: Settings, features: PickFeatures) -> int:
    if (
        features.book_count >= settings.min_minutes_to_start_relaxed_min_books
        and features.price_dispersion <= settings.min_minutes_to_start_relaxed_max_dispersion
    ):
        return settings.min_minutes_to_start_relaxed
    return settings.min_minutes_to_start

def score_pick(
    *,
    features: PickFeatures,
    settings: Settings,
    prior: ClvSportStat | None,
    sport_key: str | None = None,
) -> PQSResult:
    if features.book_count < settings.min_books:
        return PQSResult(0.0, PickScoreDecision.DROP, "min_books", {})
    if features.sharp_book_count < settings.sharp_book_min:
        return PQSResult(0.0, PickScoreDecision.DROP, "sharp_book_min", {})
    if features.time_to_start_minutes < 0:
        return PQSResult(0.0, PickScoreDecision.DROP, "min_minutes_to_start", {})
    min_minutes_to_start = adaptive_min_minutes_to_start(settings, features)
    if features.time_to_start_minutes < min_minutes_to_start:
        return PQSResult(0.0, PickScoreDecision.DROP, "min_minutes_to_start", {})

    if features.price_dispersion > settings.max_price_dispersion_hard_ceiling:
        return PQSResult(0.0, PickScoreDecision.DROP, "max_price_dispersion", {})
    adaptive_max_price = adaptive_max_price_dispersion(settings, features)
    if features.price_dispersion > adaptive_max_price:
        return PQSResult(0.0, PickScoreDecision.DROP, "max_price_dispersion", {})
    if features.agreement_strength < settings.min_agreement:
        return PQSResult(0.0, PickScoreDecision.DROP, "min_agreement", {})
    if features.ev < settings.ev_floor:
        return PQSResult(0.0, PickScoreDecision.DROP, "ev_floor", {})

    ev_score = _clamp(features.ev / 0.05)
    agreement_score = _clamp(features.agreement_strength)
    dispersion_score = _clamp(1.0 - (features.price_dispersion / max(adaptive_max_price, 1e-9)))
    coverage_score = _clamp(features.book_count / max(settings.min_books, 10))
    sharp_score = 1.0 if features.sharp_book_count >= settings.sharp_book_min else 0.0
    prior_score = 0.5
    if prior is not None:
        prior_score = _clamp((float(prior.pct_positive_market_clv) - 0.5) * 2.0 + 0.5)
    time_score = _clamp(features.time_to_start_minutes / max(settings.time_decay_half_life_min, 1))

    components = {
        "ev_score": round(ev_score, 6),
        "agreement_score": round(agreement_score, 6),
        "dispersion_score": round(dispersion_score, 6),
        "coverage_score": round(coverage_score, 6),
        "sharp_presence_score": round(sharp_score, 6),
        "clv_prior_score": round(prior_score, 6),
        "time_score": round(time_score, 6),
        "adaptive_min_pqs": 0.0,
        "adaptive_max_picks": 0.0,
    }

    pqs = (
        settings.pqs_weight_ev * ev_score
        + settings.pqs_weight_agreement * agreement_score
        + settings.pqs_weight_dispersion * dispersion_score
        + settings.pqs_weight_coverage * coverage_score
        + settings.pqs_weight_sharp_presence * sharp_score
        + settings.pqs_weight_clv_prior * prior_score
        + settings.pqs_weight_time_to_start * time_score
    )

    min_pqs, max_picks = adaptive_thresholds(settings, prior, sport_key=sport_key)
    components["adaptive_min_pqs"] = min_pqs
    components["adaptive_max_picks"] = float(max_picks)
    components["adaptive_max_price_dispersion"] = round(adaptive_max_price, 6)
    components["adaptive_min_minutes_to_start"] = float(min_minutes_to_start)

    decision = PickScoreDecision.KEEP if pqs >= min_pqs else PickScoreDecision.DROP
    drop_reason = None if decision == PickScoreDecision.KEEP else "below_min_pqs"
    return PQSResult(round(_clamp(pqs), 6), decision, drop_reason, components)
