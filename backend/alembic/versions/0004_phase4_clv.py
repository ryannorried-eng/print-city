"""phase 4 clv fields

Revision ID: 0004_phase4_clv
Revises: 0003_phase3_picks
Create Date: 2026-02-24 03:00:00

"""

from typing import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0004_phase4_clv"
down_revision: str | None = "0003_phase3_picks"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("picks", sa.Column("closing_consensus_prob", sa.Numeric(12, 8), nullable=True))
    op.add_column("picks", sa.Column("closing_book_decimal", sa.Numeric(12, 5), nullable=True))
    op.add_column("picks", sa.Column("closing_book_implied_prob", sa.Numeric(12, 8), nullable=True))
    op.add_column("picks", sa.Column("market_clv", sa.Numeric(12, 8), nullable=True))
    op.add_column("picks", sa.Column("book_clv", sa.Numeric(12, 8), nullable=True))
    op.add_column("picks", sa.Column("clv_computed_at", sa.DateTime(timezone=True), nullable=True))


def downgrade() -> None:
    op.drop_column("picks", "clv_computed_at")
    op.drop_column("picks", "book_clv")
    op.drop_column("picks", "market_clv")
    op.drop_column("picks", "closing_book_implied_prob")
    op.drop_column("picks", "closing_book_decimal")
    op.drop_column("picks", "closing_consensus_prob")
