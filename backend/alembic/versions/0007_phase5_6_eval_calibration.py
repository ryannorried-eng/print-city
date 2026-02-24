"""phase 5.6 eval and calibration

Revision ID: 0007_phase5_6_eval_calibration
Revises: 0006_phase5_5_adaptive_intelligence
Create Date: 2026-02-24 06:00:00

"""

from typing import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0007_phase5_6_eval_calibration"
down_revision: str | None = "0006_phase5_5_adaptive_intelligence"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "calibration_runs",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("eval_window_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("eval_window_end", sa.DateTime(timezone=True), nullable=False),
        sa.Column("pqs_version", sa.String(length=32), nullable=False),
        sa.Column("current_config_snapshot_json", sa.JSON(), nullable=False),
        sa.Column("proposed_config_patch_json", sa.JSON(), nullable=False),
        sa.Column("rationale_json", sa.JSON(), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("applied_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )


def downgrade() -> None:
    op.drop_table("calibration_runs")
