"""add layer_group table + datacatalog versioning anchors

Adds:
  * customer.layer_group         logical-dataset grouping (one per upstream resource)
  * customer.layer.layer_group_id FK so versions cluster under their group
  * customer.layer.ducklake_snapshot_id pins each layer to a DuckLake snapshot
  * customer.layer.version_num         monotonic per layer_group (1, 2, 3, …)
  * customer.layer_group.source_name / source_package_id / source_resource_id
        the stable (source, package, resource) triple that lets the harvester
        find-or-create the group when an external dataset is re-encountered;
        new file versions land as new customer.layer rows under the same group.

Revision ID: f4a5b6c7d8e9
Revises: e3f4a5b6c7d8
Create Date: 2026-04-10 00:00:00.000000

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB, UUID

revision = "f4a5b6c7d8e9"
down_revision = "e3f4a5b6c7d8"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "layer_group",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, server_default=sa.text("uuid_generate_v4()")),
        sa.Column("user_id", UUID(as_uuid=True), sa.ForeignKey("accounts.user.id", ondelete="CASCADE"), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("record_jsonb", JSONB, nullable=True),
        sa.Column("thumbnail_url", sa.Text(), nullable=True),
        sa.Column("source_name", sa.Text(), nullable=True),
        sa.Column("source_package_id", sa.Text(), nullable=True),
        sa.Column("source_resource_id", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        schema="customer",
    )
    op.create_index(
        "ix_layer_group_source_triple",
        "layer_group",
        ["source_name", "source_package_id", "source_resource_id"],
        unique=True,
        schema="customer",
        postgresql_where=sa.text("source_name IS NOT NULL"),
    )

    # Layer ↔ layer_group + per-version anchors
    op.add_column(
        "layer",
        sa.Column(
            "layer_group_id",
            UUID(as_uuid=True),
            sa.ForeignKey("customer.layer_group.id", ondelete="SET NULL"),
            nullable=True,
        ),
        schema="customer",
    )
    op.add_column(
        "layer",
        sa.Column("ducklake_snapshot_id", sa.BigInteger(), nullable=True),
        schema="customer",
    )
    op.add_column(
        "layer",
        sa.Column("version_num", sa.Integer(), nullable=True),
        schema="customer",
    )
    op.create_index(
        "ix_layer_group_id_version",
        "layer",
        ["layer_group_id", "version_num"],
        schema="customer",
    )


def downgrade() -> None:
    op.drop_index("ix_layer_group_id_version", table_name="layer", schema="customer")
    op.drop_column("layer", "version_num", schema="customer")
    op.drop_column("layer", "ducklake_snapshot_id", schema="customer")
    op.drop_column("layer", "layer_group_id", schema="customer")
    op.drop_index("ix_layer_group_source_triple", table_name="layer_group", schema="customer")
    op.drop_table("layer_group", schema="customer")
