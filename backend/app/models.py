from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from sqlalchemy import JSON, DateTime, ForeignKey, Index, Integer, Numeric, String, Text, UniqueConstraint, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

from app.domain.enums import PickScoreDecision


class Base(DeclarativeBase):
    pass


class Game(Base):
    __tablename__ = "games"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    sport_key: Mapped[str] = mapped_column(Text, nullable=False)
    event_id: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    commence_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    home_team: Mapped[str] = mapped_column(Text, nullable=False)
    away_team: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    odds_groups: Mapped[list[OddsGroup]] = relationship(back_populates="game")
    odds_snapshots: Mapped[list[OddsSnapshot]] = relationship(back_populates="game")
    picks: Mapped[list[Pick]] = relationship(back_populates="game")


class OddsGroup(Base):
    __tablename__ = "odds_groups"
    __table_args__ = (
        UniqueConstraint("game_id", "market_key", "bookmaker", "point", name="uq_odds_group"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    game_id: Mapped[int] = mapped_column(ForeignKey("games.id"), nullable=False, index=True)
    market_key: Mapped[str] = mapped_column(String(32), nullable=False)
    bookmaker: Mapped[str] = mapped_column(Text, nullable=False)
    point: Mapped[Decimal | None] = mapped_column(Numeric(10, 3), nullable=True)
    last_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    last_captured_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    game: Mapped[Game] = relationship(back_populates="odds_groups")


class OddsSnapshot(Base):
    __tablename__ = "odds_snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    game_id: Mapped[int] = mapped_column(ForeignKey("games.id"), nullable=False, index=True)
    captured_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    market_key: Mapped[str] = mapped_column(String(32), nullable=False)
    bookmaker: Mapped[str] = mapped_column(Text, nullable=False)
    side: Mapped[str] = mapped_column(String(16), nullable=False)
    point: Mapped[Decimal | None] = mapped_column(Numeric(10, 3), nullable=True)
    american: Mapped[int | None] = mapped_column(Integer, nullable=True)
    decimal: Mapped[Decimal | None] = mapped_column(Numeric(10, 5), nullable=True)
    implied_prob: Mapped[Decimal] = mapped_column(Numeric(12, 8), nullable=False)
    fair_prob: Mapped[Decimal] = mapped_column(Numeric(12, 8), nullable=False)
    group_hash: Mapped[str] = mapped_column(String(64), nullable=False, index=True)

    game: Mapped[Game] = relationship(back_populates="odds_snapshots")


class Pick(Base):
    __tablename__ = "picks"
    __table_args__ = (
        UniqueConstraint(
            "game_id",
            "market_key",
            "point",
            "side",
            "best_book",
            "captured_at_max",
            name="uq_pick_snapshot",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    game_id: Mapped[int] = mapped_column(ForeignKey("games.id"), nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    market_key: Mapped[str] = mapped_column(Text, nullable=False)
    side: Mapped[str] = mapped_column(Text, nullable=False)
    point: Mapped[Decimal | None] = mapped_column(Numeric(10, 3), nullable=True)
    source: Mapped[str] = mapped_column(Text, nullable=False)
    consensus_prob: Mapped[Decimal] = mapped_column(Numeric(12, 8), nullable=False)
    best_decimal: Mapped[Decimal] = mapped_column(Numeric(12, 5), nullable=False)
    best_book: Mapped[str] = mapped_column(Text, nullable=False)
    ev: Mapped[Decimal] = mapped_column(Numeric(12, 8), nullable=False)
    kelly_fraction: Mapped[Decimal] = mapped_column(Numeric(12, 8), nullable=False)
    stake: Mapped[Decimal] = mapped_column(Numeric(12, 4), nullable=False)
    consensus_books: Mapped[int] = mapped_column(Integer, nullable=False)
    sharp_books: Mapped[int] = mapped_column(Integer, nullable=False)
    captured_at_min: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    captured_at_max: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    closing_consensus_prob: Mapped[Decimal | None] = mapped_column(Numeric(12, 8), nullable=True)
    closing_book_decimal: Mapped[Decimal | None] = mapped_column(Numeric(12, 5), nullable=True)
    closing_book_implied_prob: Mapped[Decimal | None] = mapped_column(Numeric(12, 8), nullable=True)
    market_clv: Mapped[Decimal | None] = mapped_column(Numeric(12, 8), nullable=True)
    book_clv: Mapped[Decimal | None] = mapped_column(Numeric(12, 8), nullable=True)
    clv_computed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    game: Mapped[Game] = relationship(back_populates="picks")


class PipelineRun(Base):
    __tablename__ = "pipeline_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    run_type: Mapped[str] = mapped_column(String(16), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    sports: Mapped[str] = mapped_column(Text, nullable=False, default="")
    markets: Mapped[str] = mapped_column(Text, nullable=False, default="")
    stats_json: Mapped[str] = mapped_column(Text, nullable=False, default="{}")
    error: Mapped[str | None] = mapped_column(Text, nullable=True)


class PickScore(Base):
    __tablename__ = "pick_scores"
    __table_args__ = (
        UniqueConstraint("pick_id", "version", name="uq_pick_scores_pick_version"),
        Index("ix_pick_scores_version_scored_at", "version", "scored_at"),
        Index("ix_pick_scores_decision", "decision"),
        Index("ix_pick_scores_pqs", "pqs"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    pick_id: Mapped[int] = mapped_column(ForeignKey("picks.id"), nullable=False, index=True)
    scored_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    version: Mapped[str] = mapped_column(String(32), nullable=False)
    pqs: Mapped[Decimal] = mapped_column(Numeric(12, 6), nullable=False)
    components_json: Mapped[dict[str, object]] = mapped_column(JSON, nullable=False)
    features_json: Mapped[dict[str, object]] = mapped_column(JSON, nullable=False)
    decision: Mapped[str] = mapped_column(String(16), nullable=False)
    drop_reason: Mapped[str | None] = mapped_column(String(128), nullable=True)


class ClvSportStat(Base):
    __tablename__ = "clv_sport_stats"
    __table_args__ = (
        UniqueConstraint(
            "sport_key",
            "market_key",
            "side_type",
            "window_size",
            "as_of",
            name="uq_clv_sport_stats_scope",
        ),
        Index("ix_clv_sport_stats_lookup", "sport_key", "market_key", "side_type", "as_of"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    sport_key: Mapped[str] = mapped_column(String(64), nullable=False)
    market_key: Mapped[str] = mapped_column(String(32), nullable=False)
    side_type: Mapped[str | None] = mapped_column(String(16), nullable=True)
    window_size: Mapped[int] = mapped_column(Integer, nullable=False)
    as_of: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    n: Mapped[int] = mapped_column(Integer, nullable=False)
    mean_market_clv_bps: Mapped[Decimal] = mapped_column(Numeric(12, 4), nullable=False)
    median_market_clv_bps: Mapped[Decimal] = mapped_column(Numeric(12, 4), nullable=False)
    pct_positive_market_clv: Mapped[Decimal] = mapped_column(Numeric(8, 6), nullable=False)
    mean_same_book_clv_bps: Mapped[Decimal | None] = mapped_column(Numeric(12, 4), nullable=True)
    sharpe_like: Mapped[Decimal | None] = mapped_column(Numeric(12, 6), nullable=True)
    is_weak: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
