"""add xml_metadata to customer layer

Revision ID: c1d2e3f4a5b6
Revises: 8a1b2c3d4e5f
Create Date: 2026-03-26 12:00:00.000000

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "c1d2e3f4a5b6"
down_revision = "8a1b2c3d4e5f"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "layer",
        sa.Column("xml_metadata", sa.Text(), nullable=True),
        schema="customer",
    )


def downgrade():
    op.drop_column("layer", "xml_metadata", schema="customer")
