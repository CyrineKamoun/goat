"""Bring a database with the pre-consolidation schema in line with the models.

A database that predates the squashed `init` baseline carries constraints the
baseline never had, and lacks two it declares. The chain up to `0008` runs
cleanly on such a database but leaves it out of step with the models:

* `folder_user_id_name_key`, UNIQUE (user_id, name): folders are unique by
  name within their space and parent, live rows only (`uq_folder_root_name`,
  `uq_folder_child_name`). A per-user uniqueness across all spaces, parents
  and soft-deleted rows rejects layouts the application allows -- the same
  name under two parents, in a personal and an organisation space, or a name
  reused after the earlier folder was moved to the trash. Dropped.
* `unique_user_project`, UNIQUE (project_id, user_id): one row per user and
  project is what the code assumes. Created where missing.
* `idx_resource_url_pattern_method`, UNIQUE (url_pattern, method): declared by
  the model, created where missing.
* `project_public.project_id`: NOT NULL in the model, set where nullable.

Idempotent, like the revisions before it: every step checks the catalog first,
so a database that already matches the models is untouched.

Revision ID: 0009_legacy_constraint_parity
Revises: 0008_layer_public_read
"""

import sys
from pathlib import Path

from alembic import op
from core.core.config import settings

_ALEMBIC_DIR = str(Path(__file__).resolve().parents[1])
if _ALEMBIC_DIR not in sys.path:
    sys.path.append(_ALEMBIC_DIR)

import helpers as h  # noqa: E402

revision = "0009_legacy_constraint_parity"
down_revision = "0008_layer_public_read"
branch_labels = None
depends_on = None

S = settings.SCHEMA


def upgrade() -> None:
    h.drop_constraint_if_present("folder_user_id_name_key", "folder", S, type_="unique")

    if h.table_exists("user_project", S) and not h.constraint_exists(
        "user_project", "unique_user_project", S
    ):
        op.create_unique_constraint(
            "unique_user_project", "user_project", ["project_id", "user_id"], schema=S
        )

    h.create_index_if_missing(
        "idx_resource_url_pattern_method",
        "resource",
        ["url_pattern", "method"],
        S,
        unique=True,
    )

    h.alter_column_nullable("project_public", "project_id", S, nullable=False)


def downgrade() -> None:
    # The dropped per-user folder uniqueness is not restored: live data may
    # legitimately violate it. The other three changes only add guarantees the
    # code relies on and are left in place as well.
    pass
