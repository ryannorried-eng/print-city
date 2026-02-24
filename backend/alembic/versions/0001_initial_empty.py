"""initial empty migration

Revision ID: 0001_initial_empty
Revises:
Create Date: 2026-02-24 00:00:00

"""

from typing import Sequence

# revision identifiers, used by Alembic.
revision: str = "0001_initial_empty"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
