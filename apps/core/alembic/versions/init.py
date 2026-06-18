"""Squashed baseline — full current schema (tables + schemas only).

Collapses the previous core + accounts migration history into a single
starting point that builds the entire schema from zero, straight from the
SQLModel metadata.

Functions, triggers, pg_cron jobs and seed data are NOT created here — they
are installed separately by ``scripts/initial_data.py`` (``init_functions`` /
``init_triggers`` / the ``seed_*`` helpers), exactly as before this squash.

Revision ID: init
Revises:
"""

from alembic import op
from sqlmodel import SQLModel

import core.db.models  # noqa: F401  (imports all models -> populates metadata)

revision = "init"
down_revision = None
branch_labels = None
depends_on = None

SCHEMAS = ("basic", "customer", "accounts", "temporal")


def upgrade() -> None:
    conn = op.get_bind()
    for schema in SCHEMAS:
        op.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
    # Extensions required by the table DDL: PostGIS for geometry columns,
    # uuid-ossp for uuid_generate_v4() server defaults. (gen_random_uuid() is a
    # built-in in PG13+.) Platform extensions (citus, h3, …) are infra-managed.
    op.execute("CREATE EXTENSION IF NOT EXISTS postgis")
    op.execute('CREATE EXTENSION IF NOT EXISTS "uuid-ossp"')
    SQLModel.metadata.create_all(conn)


def downgrade() -> None:
    SQLModel.metadata.drop_all(op.get_bind())
