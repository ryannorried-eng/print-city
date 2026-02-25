from app.config import get_settings


def test_settings_defaults_for_conservative_mode(monkeypatch) -> None:
    for key in [
        "CONSENSUS_MIN_BOOKS",
        "PICK_MIN_BOOKS",
        "PICK_MIN_EV",
        "SHARP_WEIGHT",
        "STANDARD_WEIGHT",
        "KELLY_MULTIPLIER",
        "KELLY_MAX_CAP",
    ]:
        monkeypatch.delenv(key, raising=False)

    get_settings.cache_clear()
    settings = get_settings()

    assert settings.consensus_min_books == 5
    assert settings.pick_min_books == 5
    assert settings.pick_min_ev == 0.015
    assert settings.sharp_weight == 2.0
    assert settings.standard_weight == 1.0
    assert settings.kelly_multiplier == 0.25
    assert settings.kelly_max_cap == 0.05
    assert settings.enable_scheduler is False
    assert settings.sched_ingest_interval_sec == 600
    assert settings.sched_picks_interval_sec == 600
    assert settings.sched_clv_interval_sec == 1800
    assert settings.sched_jitter_sec == 30
    assert settings.sched_max_concurrent == 1
    assert settings.sports_autorun == ""
    assert settings.markets_autorun == "h2h"
    assert settings.sched_require_db is True
    assert settings.min_minutes_to_start == 30
    assert settings.ncaab_default_max_picks == 8
    assert settings.max_price_dispersion_sharp_ev == 0.24


def test_settings_env_overrides_defaults(monkeypatch) -> None:
    monkeypatch.setenv("CONSENSUS_MIN_BOOKS", "7")
    monkeypatch.setenv("PICK_MIN_BOOKS", "8")
    monkeypatch.setenv("PICK_MIN_EV", "0.02")
    monkeypatch.setenv("SHARP_WEIGHT", "2.5")
    monkeypatch.setenv("STANDARD_WEIGHT", "0.9")
    monkeypatch.setenv("KELLY_MULTIPLIER", "0.2")
    monkeypatch.setenv("KELLY_MAX_CAP", "0.04")
    monkeypatch.setenv("ENABLE_SCHEDULER", "true")
    monkeypatch.setenv("SCHED_INGEST_INTERVAL_SEC", "300")

    get_settings.cache_clear()
    settings = get_settings()

    assert settings.consensus_min_books == 7
    assert settings.pick_min_books == 8
    assert settings.pick_min_ev == 0.02
    assert settings.sharp_weight == 2.5
    assert settings.standard_weight == 0.9
    assert settings.kelly_multiplier == 0.2
    assert settings.kelly_max_cap == 0.04
    assert settings.enable_scheduler is True
    assert settings.sched_ingest_interval_sec == 300
