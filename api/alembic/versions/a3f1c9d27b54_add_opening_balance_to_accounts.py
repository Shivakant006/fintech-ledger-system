"""add opening_balance to accounts

Revision ID: a3f1c9d27b54
Revises: 64b8d19eb3fb
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = 'a3f1c9d27b54'
down_revision: Union[str, Sequence[str], None] = '64b8d19eb3fb'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Phase 1: add the column with a temporary server_default so existing
    # rows get a value (NOT NULL on a populated table fails without one).
    op.add_column(
        'accounts',
        sa.Column('opening_balance', sa.BigInteger(), nullable=False, server_default='0'),
    )

    # Phase 2: backfill. Whatever isn't explained by ledger lines must
    # have been the starting money.
    op.execute("""
        UPDATE accounts
        SET opening_balance = balance - COALESCE(
            (SELECT SUM(amount) FROM ledger_lines WHERE ledger_lines.account_id = accounts.id),
            0
        )
    """)

    # Phase 3: drop the default so future inserts must state it explicitly.
    op.alter_column('accounts', 'opening_balance', server_default=None)


def downgrade() -> None:
    op.drop_column('accounts', 'opening_balance')