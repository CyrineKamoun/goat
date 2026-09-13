"""Rename `layer.in_catalog` to `layer.public_read`.

The column never meant "listed in the catalog": the Catalog page lists STAC
items from the mirror, never `customer.layer` rows. It now means what a
dataset owner sets through the Share dialog: readable by every signed-in
user, in any organization, while the owner's and editors' rights stay as
they are. Catalog datasets are identified by `catalog_external_uid`, so no
existing row needs the flag and nothing is backfilled.

Idempotent, like the revisions before it.

Revision ID: 0008_layer_public_read
Revises: 0007_layer_project_indexes
"""

import sys
from pathlib import Path

from alembic import op
from core.core.config import settings

_ALEMBIC_DIR = str(Path(__file__).resolve().parents[1])
if _ALEMBIC_DIR not in sys.path:
    sys.path.append(_ALEMBIC_DIR)

import helpers as h  # noqa: E402

revision = "0008_layer_public_read"
down_revision = "0007_layer_project_indexes"
branch_labels = None
depends_on = None

S = settings.SCHEMA


def upgrade() -> None:
    if not h.table_exists("layer", S):
        return
    if h.column_exists("layer", "in_catalog", S) and not h.column_exists(
        "layer", "public_read", S
    ):
        op.alter_column("layer", "in_catalog", new_column_name="public_read", schema=S)


def downgrade() -> None:
    if h.column_exists("layer", "public_read", S) and not h.column_exists(
        "layer", "in_catalog", S
    ):
        op.alter_column("layer", "public_read", new_column_name="in_catalog", schema=S)
