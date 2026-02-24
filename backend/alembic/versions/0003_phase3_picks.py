"""phase 3 picks schema

Revision ID: 0003_phase3_picks
Revises: 0002_phase1_odds_ingestion
Create Date: 2026-02-24 02:00:00

"""

from typing import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0003_phase3_picks"
down_revision: str | None = "0002_phase1_odds_ingestion"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "picks",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("game_id", sa.Integer(), sa.ForeignKey("games.id"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("market_key", sa.Text(), nullable=False),
        sa.Column("side", sa.Text(), nullable=False),
        sa.Column("point", sa.Numeric(10, 3), nullable=True),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("consensus_prob", sa.Numeric(12, 8), nullable=False),
        sa.Column("best_decimal", sa.Numeric(12, 5), nullable=False),
        sa.Column("best_book", sa.Text(), nullable=False),
        sa.Column("ev", sa.Numeric(12, 8), nullable=False),
        sa.Column("kelly_fraction", sa.Numeric(12, 8), nullable=False),
        sa.Column("stake", sa.Numeric(12, 4), nullable=False),
        sa.Column("consensus_books", sa.Integer(), nullable=False),
        sa.Column("sharp_books", sa.Integer(), nullable=False),
        sa.Column("captured_at_min", sa.DateTime(timezone=True), nullable=False),
        sa.Column("captured_at_max", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint(
            "game_id",
            "market_key",
            "point",
            "side",
            "best_book",
            "captured_at_max",
            name="uq_pick_snapshot",
        ),
    )
    op.create_index("ix_picks_game_id", "picks", ["game_id"])


def downgrade() -> None:
    op.drop_index("ix_picks_game_id", table_name="picks")
    op.drop_table("picks")
