"""add organization analytics

Revision ID: c3a1d7f2e9b4
Revises: e8bd0e42b8eb
Create Date: 2026-04-29 14:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = 'c3a1d7f2e9b4'
# NOTE: temporarily pointed at e8bd0e42b8eb (the custom-domains migration)
# instead of the d1e2f3a4b5c6 mergepoint so this lands on dev as a single
# additional revision over the current dev pointer. The migration graph
# now has two heads (c3a1d7f2e9b4 + d1e2f3a4b5c6); resolve by adding a
# merge revision after this is verified on dev.
down_revision = 'e8bd0e42b8eb'
branch_labels = None
depends_on = None


def upgrade():
    # Per-project consent toggle. Default true is the safe choice for
    # German TDDDG / EU consent jurisdictions; customers in permissive
    # jurisdictions can opt out per-project via the Share dialog.
    op.add_column(
        'project_public',
        sa.Column(
            'tracking_require_consent',
            sa.Boolean(),
            server_default=sa.text('true'),
            nullable=False,
        ),
        schema='customer',
    )

    op.create_table(
        'organization_analytics',
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            'created_at',
            sa.DateTime(timezone=True),
            # Match DateTimeBase's SQLModel-side server_default so inserts
            # that omit created_at (the model passes it as None and relies
            # on the DB) succeed.
            server_default=sa.text(
                "to_char(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', "
                "'YYYY-MM-DD\"T\"HH24:MI:SSOF')::timestamptz"
            ),
            nullable=False,
        ),
        sa.Column(
            'id',
            postgresql.UUID(as_uuid=True),
            server_default=sa.text('uuid_generate_v4()'),
            nullable=False,
        ),
        sa.Column('organization_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('provider', sa.Text(), nullable=False),
        sa.Column('config', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.ForeignKeyConstraint(
            ['organization_id'],
            ['accounts.organization.id'],
            ondelete='CASCADE',
        ),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('organization_id'),
        schema='customer',
    )
    op.create_index(
        op.f('ix_customer_organization_analytics_organization_id'),
        'organization_analytics',
        ['organization_id'],
        unique=False,
        schema='customer',
    )


def downgrade():
    op.drop_index(
        op.f('ix_customer_organization_analytics_organization_id'),
        table_name='organization_analytics',
        schema='customer',
    )
    op.drop_table('organization_analytics', schema='customer')
    op.drop_column('project_public', 'tracking_require_consent', schema='customer')
