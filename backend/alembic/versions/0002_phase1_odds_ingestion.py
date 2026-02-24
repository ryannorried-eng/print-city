"""phase 1 odds ingestion schema

Revision ID: 0002_phase1_odds_ingestion
Revises: 0001_initial_empty
Create Date: 2026-02-24 01:00:00

"""

from typing import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0002_phase1_odds_ingestion"
down_revision: str | None = "0001_initial_empty"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "games",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("sport_key", sa.Text(), nullable=False),
        sa.Column("event_id", sa.Text(), nullable=False, unique=True),
        sa.Column("commence_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("home_team", sa.Text(), nullable=False),
        sa.Column("away_team", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )

    op.create_table(
        "odds_groups",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("game_id", sa.Integer(), sa.ForeignKey("games.id"), nullable=False),
        sa.Column("market_key", sa.String(length=32), nullable=False),
        sa.Column("bookmaker", sa.Text(), nullable=False),
        sa.Column("point", sa.Numeric(10, 3), nullable=True),
        sa.Column("last_hash", sa.String(length=64), nullable=False),
        sa.Column("last_captured_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("game_id", "market_key", "bookmaker", "point", name="uq_odds_group"),
    )
    op.create_index("ix_odds_groups_game_id", "odds_groups", ["game_id"])

    op.create_table(
        "odds_snapshots",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("game_id", sa.Integer(), sa.ForeignKey("games.id"), nullable=False),
        sa.Column("captured_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("market_key", sa.String(length=32), nullable=False),
        sa.Column("bookmaker", sa.Text(), nullable=False),
        sa.Column("side", sa.String(length=16), nullable=False),
        sa.Column("point", sa.Numeric(10, 3), nullable=True),
        sa.Column("american", sa.Integer(), nullable=True),
        sa.Column("decimal", sa.Numeric(10, 5), nullable=True),
        sa.Column("implied_prob", sa.Numeric(12, 8), nullable=False),
        sa.Column("fair_prob", sa.Numeric(12, 8), nullable=False),
        sa.Column("group_hash", sa.String(length=64), nullable=False),
    )
    op.create_index("ix_odds_snapshots_game_id", "odds_snapshots", ["game_id"])
    op.create_index("ix_odds_snapshots_captured_at", "odds_snapshots", ["captured_at"])
    op.create_index("ix_odds_snapshots_group_hash", "odds_snapshots", ["group_hash"])


def downgrade() -> None:
    op.drop_index("ix_odds_snapshots_group_hash", table_name="odds_snapshots")
    op.drop_index("ix_odds_snapshots_captured_at", table_name="odds_snapshots")
    op.drop_index("ix_odds_snapshots_game_id", table_name="odds_snapshots")
    op.drop_table("odds_snapshots")
    op.drop_index("ix_odds_groups_game_id", table_name="odds_groups")
    op.drop_table("odds_groups")
    op.drop_table("games")
