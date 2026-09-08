"""Generic per-user favourites.

What an `init`-era database gains here: `customer.favorite`, one row per
(user, kind, id). The favourited item may live outside this database entirely
— catalog items live in the STAC mirror — so `item_id` is an opaque text key
with no foreign key.

A fresh database is built by `init` from the current models, so the table is
already there and this revision passes through unchanged.

Revision ID: 0002_favorites
Revises: 0001_bundles_catalog
"""

import sys
from pathlib import Path

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

from core.core.config import settings

_ALEMBIC_DIR = str(Path(__file__).resolve().parents[1])
if _ALEMBIC_DIR not in sys.path:
    sys.path.append(_ALEMBIC_DIR)

import helpers as h  # noqa: E402

revision = "0002_favorites"
down_revision = "0001_bundles_catalog"
branch_labels = None
depends_on = None

S = settings.SCHEMA


def upgrade() -> None:
    if not h.table_exists("favorite", S):
        op.create_table(
            "favorite",
            sa.Column("user_id", sa.UUID(), nullable=False),
            sa.Column("item_type", sa.Text(), nullable=False),
            sa.Column("item_id", sa.Text(), nullable=False),
            sa.Column(
                "created_at",
                postgresql.TIMESTAMP(timezone=True),
                server_default=sa.text("now()"),
                nullable=False,
            ),
            sa.ForeignKeyConstraint(["user_id"], [f"{S}.user.id"], ondelete="CASCADE"),
            sa.PrimaryKeyConstraint("user_id", "item_type", "item_id"),
            schema=S,
        )
    h.create_index_if_missing(
        "ix_favorite_user_type", "favorite", ["user_id", "item_type"], S
    )


def downgrade() -> None:
    raise NotImplementedError(
        "downgrade is not supported for the consolidated migrations"
    )
