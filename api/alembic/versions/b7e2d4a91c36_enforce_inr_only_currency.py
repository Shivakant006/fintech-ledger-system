"""enforce INR-only currency on accounts

Revision ID: b7e2d4a91c36
Revises: a3f1c9d27b54
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = 'b7e2d4a91c36'
down_revision: Union[str, Sequence[str], None] = 'a3f1c9d27b54'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Phase 1: rows created with raw SQL have NULL currency. They were always
    # meant to be INR. Only NULLs are converted; any other value is left alone
    # so that Phase 3 fails loudly instead of silently rewriting money.
    op.execute("UPDATE accounts SET currency = 'INR' WHERE currency IS NULL")

    # Phase 2: no more NULLs, and raw-SQL inserts now default to INR too.
    op.alter_column('accounts', 'currency',
                    existing_type=sa.String(length=3),
                    nullable=False,
                    server_default='INR')

    # Phase 3: the ledger is single-currency. Postgres refuses anything else.
    op.create_check_constraint('check_currency_inr', 'accounts', "currency = 'INR'")


def downgrade() -> None:
    op.drop_constraint('check_currency_inr', 'accounts', type_='check')
    op.alter_column('accounts', 'currency',
                    existing_type=sa.String(length=3),
                    nullable=True,
                    server_default=None)