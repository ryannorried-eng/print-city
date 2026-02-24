"""phase 5 pipeline runs

Revision ID: 0005_phase5_pipeline_runs
Revises: 0004_phase4_clv
Create Date: 2026-02-24 04:00:00

"""

from typing import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0005_phase5_pipeline_runs"
down_revision: str | None = "0004_phase4_clv"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "pipeline_runs",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("run_type", sa.String(length=16), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("sports", sa.Text(), nullable=False, server_default=""),
        sa.Column("markets", sa.Text(), nullable=False, server_default=""),
        sa.Column("stats_json", sa.Text(), nullable=False, server_default="{}"),
        sa.Column("error", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )


def downgrade() -> None:
    op.drop_table("pipeline_runs")
