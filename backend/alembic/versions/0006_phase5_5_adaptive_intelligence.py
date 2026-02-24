"""phase 5.5 adaptive intelligence

Revision ID: 0006_phase5_5_adaptive_intelligence
Revises: 0005_phase5_pipeline_runs
Create Date: 2026-02-24 05:00:00

"""

from typing import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0006_phase5_5_adaptive_intelligence"
down_revision: str | None = "0005_phase5_pipeline_runs"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "pick_scores",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("pick_id", sa.Integer(), nullable=False),
        sa.Column("scored_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("version", sa.String(length=32), nullable=False),
        sa.Column("pqs", sa.Numeric(12, 6), nullable=False),
        sa.Column("components_json", sa.JSON(), nullable=False),
        sa.Column("features_json", sa.JSON(), nullable=False),
        sa.Column("decision", sa.String(length=16), nullable=False),
        sa.Column("drop_reason", sa.String(length=128), nullable=True),
        sa.ForeignKeyConstraint(["pick_id"], ["picks.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("pick_id", "version", name="uq_pick_scores_pick_version"),
    )
    op.create_index("ix_pick_scores_version_scored_at", "pick_scores", ["version", "scored_at"])
    op.create_index("ix_pick_scores_decision", "pick_scores", ["decision"])
    op.create_index("ix_pick_scores_pqs", "pick_scores", ["pqs"])

    op.create_table(
        "clv_sport_stats",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("sport_key", sa.String(length=64), nullable=False),
        sa.Column("market_key", sa.String(length=32), nullable=False),
        sa.Column("side_type", sa.String(length=16), nullable=True),
        sa.Column("window_size", sa.Integer(), nullable=False),
        sa.Column("as_of", sa.DateTime(timezone=True), nullable=False),
        sa.Column("n", sa.Integer(), nullable=False),
        sa.Column("mean_market_clv_bps", sa.Numeric(12, 4), nullable=False),
        sa.Column("median_market_clv_bps", sa.Numeric(12, 4), nullable=False),
        sa.Column("pct_positive_market_clv", sa.Numeric(8, 6), nullable=False),
        sa.Column("mean_same_book_clv_bps", sa.Numeric(12, 4), nullable=True),
        sa.Column("sharpe_like", sa.Numeric(12, 6), nullable=True),
        sa.Column("is_weak", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "sport_key", "market_key", "side_type", "window_size", "as_of", name="uq_clv_sport_stats_scope"
        ),
    )
    op.create_index(
        "ix_clv_sport_stats_lookup", "clv_sport_stats", ["sport_key", "market_key", "side_type", "as_of"]
    )


def downgrade() -> None:
    op.drop_index("ix_clv_sport_stats_lookup", table_name="clv_sport_stats")
    op.drop_table("clv_sport_stats")
    op.drop_index("ix_pick_scores_pqs", table_name="pick_scores")
    op.drop_index("ix_pick_scores_decision", table_name="pick_scores")
    op.drop_index("ix_pick_scores_version_scored_at", table_name="pick_scores")
    op.drop_table("pick_scores")
