"""Spaces own content; folders nest; deletes are soft; transfers are recorded.

What an `init`-era database gains here:

* `customer.space` — one per user, team and organisation — plus `space_id` on
  folder, layer, project and bundle, `folder.parent_id`, `deleted_at` on the
  four content tables and `team.organization_id`. The backfill creates a
  personal space for every user, a team space (default editor) for every team
  and an organisation space (default viewer) for every organisation, then
  moves every existing row into its owner's personal space so nothing visible
  changes for the current UI. Catalog layers (`user_id IS NULL`) keep
  `space_id` NULL;
* `customer.content_transfer`, recording every ownership hand-over (who moved
  what from which space to which, with the datasets that went along), and
  `customer.content_shortcut`, the optional badged pointer left in the old
  location;
* `user_id` as "created by" on the content tables: nullable, with an
  `ON DELETE SET NULL` foreign key, so a deleted user no longer takes the
  content they created with them;
* the unique folder-name indexes (`uq_folder_root_name`, `uq_folder_child_name`),
  the `user_team` uniqueness of (user, team), and the `ix_accounts_*` indexes
  renamed to the `ix_customer_*` names the models declare;
* a root `home` folder for every team and organisation space. A space's root
  is a live folder named `home` with `parent_id IS NULL` and `user_id IS NULL`
  (the space owns it, no person does); without it "Add new" at a team root has
  no folder to file a project, dataset or document in.

`space_id` ends NOT NULL only on bundle. folder, project and layer are left
nullable: a dev-era database carries rows whose `user_id` points at a user
that no longer exists (nothing ever enforced that column), so the backfill has
no personal space to put them in. Those rows were already unreachable, and
deleting content as a side effect of a schema migration is out of scope.
Every application create path always supplies `space_id` regardless.

The same orphans are why the `user_id` foreign keys are added NOT VALID: they
bind every new write immediately, without a scan that would fail on rows
already in breach.

A fresh database is built by `init` from the current models, so every table,
column, index and constraint here already exists and the backfills find
nothing to do. The revision passes through unchanged.

Revision ID: 0004_spaces
Revises: 0003_sharing_grants
"""

import logging
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

logger = logging.getLogger(__name__)

revision = "0004_spaces"
down_revision = "0003_sharing_grants"
branch_labels = None
depends_on = None

S = settings.SCHEMA

_CREATED_AT_DEFAULT = sa.text(
    "to_char(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', "
    "'YYYY-MM-DD\"T\"HH24:MI:SSOF')::timestamptz"
)

# Every statement below names its schema through `S`, so it needs no SQL
# function or object beyond the tables this revision creates.
_SPACES_BACKFILL_SQL = [
    # one personal space per user
    f"""INSERT INTO "{S}".space (id, kind, user_id, default_role)
        SELECT uuid_generate_v4(), 'personal', u.id, 'editor' FROM "{S}"."user" u
        ON CONFLICT (user_id) DO NOTHING""",
    # team.organization_id from the team owner (fallback: any member)
    f"""UPDATE "{S}".team t SET organization_id = src.organization_id
        FROM (
            SELECT DISTINCT ON (ut.team_id) ut.team_id, u.organization_id
            FROM "{S}".user_team ut
            JOIN "{S}"."user" u ON u.id = ut.user_id
            JOIN "{S}".role r ON r.id = ut.role_id
            WHERE u.organization_id IS NOT NULL
            ORDER BY ut.team_id, (r.name = 'team-owner') DESC
        ) src
        WHERE t.id = src.team_id AND t.organization_id IS NULL""",
    # one team space per team (D8: default editor)
    f"""INSERT INTO "{S}".space (id, kind, team_id, default_role)
        SELECT uuid_generate_v4(), 'team', t.id, 'editor' FROM "{S}".team t
        ON CONFLICT (team_id) DO NOTHING""",
    # one organisation space per organisation (D8: default viewer)
    f"""INSERT INTO "{S}".space (id, kind, organization_id, default_role)
        SELECT uuid_generate_v4(), 'organization', o.id, 'viewer' FROM "{S}".organization o
        ON CONFLICT (organization_id) DO NOTHING""",
    # content -> the owner's personal space
    f"""UPDATE "{S}".folder f SET space_id = s.id FROM "{S}".space s
        WHERE s.kind = 'personal' AND s.user_id = f.user_id AND f.space_id IS NULL""",
    f"""UPDATE "{S}".project p SET space_id = s.id FROM "{S}".space s
        WHERE s.kind = 'personal' AND s.user_id = p.user_id AND p.space_id IS NULL""",
    f"""UPDATE "{S}".bundle b SET space_id = s.id FROM "{S}".space s
        WHERE s.kind = 'personal' AND s.user_id = b.user_id AND b.space_id IS NULL""",
    f"""UPDATE "{S}".layer l SET space_id = s.id FROM "{S}".space s
        WHERE s.kind = 'personal' AND s.user_id = l.user_id AND l.space_id IS NULL AND l.user_id IS NOT NULL""",
]

# A user can currently hold two rows for the same team; keep one per
# (user_id, team_id) so the unique constraint below can be created.
#
# Surviving row: the strongest role wins (owner > editor > viewer > anything
# else), ties broken by the lowest `user_team.id` — the oldest of the
# duplicates. The rank is spelled out inline on `role.name` rather than
# calling `customer.role_rank`: that function is installed by
# scripts/initial_data.py, not by a migration, so it need not exist when this
# revision runs. Only `user_team.id`/`role_id` and `role.name` are read, all
# of which exist at this revision. A LEFT JOIN keeps a row whose role_id
# points at nothing (rank 0) eligible to survive if it is the only one.
USER_TEAM_DEDUPE_SQL = f"""WITH ranked AS (
            SELECT ut.id,
                   ROW_NUMBER() OVER (
                       PARTITION BY ut.user_id, ut.team_id
                       ORDER BY CASE
                                    WHEN r.name = 'owner'  OR r.name LIKE '%-owner'  THEN 3
                                    WHEN r.name = 'editor' OR r.name LIKE '%-editor' THEN 2
                                    WHEN r.name = 'viewer' OR r.name LIKE '%-viewer' THEN 1
                                    ELSE 0
                                END DESC,
                                ut.id ASC
                   ) AS n
            FROM "{S}".user_team ut
            LEFT JOIN "{S}".role r ON r.id = ut.role_id
        )
        DELETE FROM "{S}".user_team ut USING ranked
        WHERE ut.id = ranked.id AND ranked.n > 1"""

# Duplicate folder names inside one space (root level) get a numeric suffix so
# the unique index can be created. The row source matches
# `uq_folder_root_name`'s predicate exactly, `space_id IS NOT NULL` included:
# a row with no space cannot collide in that index (NULL is distinct from
# NULL there), and a dev-era database carries hundreds of them — folders whose
# owning user no longer exists — which renaming would only churn.
FOLDER_NAME_DEDUPE_SQL = f"""WITH d AS (
            SELECT id, name, ROW_NUMBER() OVER (PARTITION BY space_id, name ORDER BY created_at, id) AS n
            FROM "{S}".folder WHERE parent_id IS NULL AND space_id IS NOT NULL AND deleted_at IS NULL
        )
        UPDATE "{S}".folder f SET name = d.name || ' (' || d.n || ')' FROM d WHERE f.id = d.id AND d.n > 1"""

# The whole backfill in the order it runs. The authz tests replay this list
# against a test schema.
BACKFILL_SQL = [
    *_SPACES_BACKFILL_SQL,
    USER_TEAM_DEDUPE_SQL,
    FOLDER_NAME_DEDUPE_SQL,
]

# The root folder of every team and organisation space. Idempotent: the
# partial unique index `uq_folder_root_name` and this NOT EXISTS both make
# the insert a no-op for a space that already has its `home` root.
ROOT_HOME_SQL = (
    f'INSERT INTO "{S}".folder (name, space_id, parent_id, user_id, updated_at) '
    f"SELECT 'home', s.id, NULL, NULL, now() FROM \"{S}\".space s "
    "WHERE s.kind <> 'personal' "
    f'AND NOT EXISTS (SELECT 1 FROM "{S}".folder f '
    "WHERE f.space_id = s.id AND f.parent_id IS NULL "
    "AND f.name = 'home' AND f.deleted_at IS NULL)"
)

# The models name every id index `ix_customer_*`; a dev-era database still
# carries the `ix_accounts_*` names from when these tables lived in their own
# schema.
_RENAMED_ID_INDEXES = (
    "cost",
    "invitation",
    "organization",
    "permission",
    "resource",
    "role",
    "team",
    "user",
)


def _create_space_tables() -> None:
    if not h.table_exists("space", S):
        op.create_table(
            "space",
            sa.Column(
                "created_at",
                sa.DateTime(timezone=True),
                server_default=_CREATED_AT_DEFAULT,
                nullable=False,
            ),
            sa.Column(
                "id",
                sa.UUID(),
                server_default=sa.text("uuid_generate_v4()"),
                nullable=False,
            ),
            sa.Column(
                "updated_at",
                sa.DateTime(timezone=True),
                server_default=sa.text("now()"),
                nullable=False,
            ),
            sa.Column("kind", sa.Text(), nullable=False),
            sa.Column("user_id", sa.UUID(), nullable=True),
            sa.Column("team_id", sa.UUID(), nullable=True),
            sa.Column("organization_id", sa.UUID(), nullable=True),
            sa.Column(
                "default_role",
                sa.Text(),
                server_default=sa.text("'editor'"),
                nullable=False,
            ),
            sa.CheckConstraint(
                "(kind = 'personal' AND user_id IS NOT NULL) OR (kind = 'team' AND team_id IS NOT NULL) OR (kind = 'organization' AND organization_id IS NOT NULL)",
                name="space_kind_matches_owner",
            ),
            sa.CheckConstraint(
                "(user_id IS NOT NULL)::int + (team_id IS NOT NULL)::int + (organization_id IS NOT NULL)::int = 1",
                name="space_exactly_one_owner",
            ),
            sa.ForeignKeyConstraint(
                ["organization_id"], [f"{S}.organization.id"], ondelete="CASCADE"
            ),
            sa.ForeignKeyConstraint(["team_id"], [f"{S}.team.id"], ondelete="CASCADE"),
            sa.ForeignKeyConstraint(["user_id"], [f"{S}.user.id"], ondelete="CASCADE"),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint("organization_id", name="space_organization_id_key"),
            sa.UniqueConstraint("team_id", name="space_team_id_key"),
            sa.UniqueConstraint("user_id", name="space_user_id_key"),
            schema=S,
        )

    if not h.table_exists("content_transfer", S):
        op.create_table(
            "content_transfer",
            sa.Column(
                "id",
                sa.UUID(),
                server_default=sa.text("uuid_generate_v4()"),
                nullable=False,
            ),
            sa.Column("item_type", sa.Text(), nullable=False),
            sa.Column("item_id", sa.UUID(), nullable=False),
            sa.Column("from_space_id", sa.UUID(), nullable=True),
            sa.Column("to_space_id", sa.UUID(), nullable=True),
            sa.Column("actor_id", sa.UUID(), nullable=True),
            sa.Column(
                "details",
                postgresql.JSONB(astext_type=sa.Text()),
                server_default=sa.text("'{}'::jsonb"),
                nullable=False,
            ),
            sa.Column(
                "created_at",
                sa.DateTime(timezone=True),
                server_default=sa.text("now()"),
                nullable=False,
            ),
            sa.ForeignKeyConstraint(
                ["actor_id"], [f"{S}.user.id"], ondelete="SET NULL"
            ),
            sa.ForeignKeyConstraint(
                ["from_space_id"], [f"{S}.space.id"], ondelete="SET NULL"
            ),
            sa.ForeignKeyConstraint(
                ["to_space_id"], [f"{S}.space.id"], ondelete="SET NULL"
            ),
            sa.PrimaryKeyConstraint("id"),
            schema=S,
        )
    h.create_index_if_missing(
        "idx_content_transfer_item", "content_transfer", ["item_type", "item_id"], S
    )

    if not h.table_exists("content_shortcut", S):
        op.create_table(
            "content_shortcut",
            sa.Column(
                "id",
                sa.UUID(),
                server_default=sa.text("uuid_generate_v4()"),
                nullable=False,
            ),
            sa.Column("space_id", sa.UUID(), nullable=False),
            sa.Column("folder_id", sa.UUID(), nullable=True),
            sa.Column("target_type", sa.Text(), nullable=False),
            sa.Column("target_id", sa.UUID(), nullable=False),
            sa.Column("created_by", sa.UUID(), nullable=True),
            sa.Column(
                "created_at",
                sa.DateTime(timezone=True),
                server_default=sa.text("now()"),
                nullable=False,
            ),
            sa.ForeignKeyConstraint(
                ["created_by"], [f"{S}.user.id"], ondelete="SET NULL"
            ),
            sa.ForeignKeyConstraint(
                ["folder_id"], [f"{S}.folder.id"], ondelete="CASCADE"
            ),
            sa.ForeignKeyConstraint(
                ["space_id"], [f"{S}.space.id"], ondelete="CASCADE"
            ),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint(
                "space_id",
                "folder_id",
                "target_type",
                "target_id",
                name="content_shortcut_unique",
            ),
            schema=S,
        )


def _add_content_columns() -> None:
    """`space_id` and `deleted_at` on the content tables, `folder.parent_id`."""
    for table, ondelete in (
        ("folder", "CASCADE"),
        ("project", "CASCADE"),
        ("bundle", "CASCADE"),
        ("layer", "SET NULL"),
    ):
        h.add_column_if_missing(table, sa.Column("space_id", sa.UUID()), S)
        h.ensure_fk(
            f"{table}_space_id_fkey",
            table,
            "space",
            ["space_id"],
            ["id"],
            S,
            ondelete=ondelete,
        )
        h.create_index_if_missing(
            f"ix_customer_{table}_space_id", table, ["space_id"], S
        )
        h.add_column_if_missing(
            table, sa.Column("deleted_at", sa.DateTime(timezone=True)), S
        )

    h.add_column_if_missing("folder", sa.Column("parent_id", sa.UUID()), S)
    h.ensure_fk(
        "folder_parent_id_fkey",
        "folder",
        "folder",
        ["parent_id"],
        ["id"],
        S,
        ondelete="CASCADE",
    )
    h.create_index_if_missing(
        "ix_customer_folder_parent_id", "folder", ["parent_id"], S
    )

    h.add_column_if_missing("team", sa.Column("organization_id", sa.UUID()), S)
    h.ensure_fk(
        "team_organization_id_fkey",
        "team",
        "organization",
        ["organization_id"],
        ["id"],
        S,
        ondelete="CASCADE",
    )
    h.create_index_if_missing(
        "ix_customer_team_organization_id", "team", ["organization_id"], S
    )


def _dedupe_user_team() -> None:
    """Resolve rows that would breach the (user_id, team_id) uniqueness.

    A no-op on a database that has none: the statement reads only tables and
    columns this revision has already created, so it needs nothing installed
    outside the migration chain and always runs.
    """
    op.execute(USER_TEAM_DEDUPE_SQL)


def _created_by_semantics() -> None:
    """`user_id` becomes "created by": nullable, and it survives the user."""
    for table in ("folder", "project", "bundle", "layer"):
        h.alter_column_nullable(table, "user_id", S, nullable=True)
        h.ensure_fk(
            f"{table}_user_id_fkey",
            table,
            "user",
            ["user_id"],
            ["id"],
            S,
            ondelete="SET NULL",
            not_valid=True,
        )


def upgrade() -> None:
    _create_space_tables()
    _add_content_columns()

    for stmt in _SPACES_BACKFILL_SQL:
        op.execute(stmt)
    _dedupe_user_team()
    op.execute(FOLDER_NAME_DEDUPE_SQL)

    # space_id is enforced NOT NULL only where the backfill can actually
    # guarantee every row got one. bundle has no such rows; folder, project
    # and layer each carry rows whose owning user no longer exists, and the
    # backfill has no personal space to put those in.
    missing = (
        op.get_bind()
        .execute(sa.text(f'SELECT count(*) FROM "{S}".folder WHERE space_id IS NULL'))
        .scalar_one()
    )
    if missing:
        logger.warning(
            "%d folder row(s) have no owning user and were left without a space",
            missing,
        )
    h.alter_column_nullable("bundle", "space_id", S, nullable=False)

    _created_by_semantics()

    if not h.constraint_exists("user_team", "user_team_user_id_team_id_key", S):
        op.create_unique_constraint(
            "user_team_user_id_team_id_key",
            "user_team",
            ["user_id", "team_id"],
            schema=S,
        )
    h.create_index_if_missing("idx_user_team", "user_team", ["user_id", "team_id"], S)
    h.create_index_if_missing("idx_user_team_team_id", "user_team", ["team_id"], S)
    h.create_index_if_missing("idx_user_team_user_id", "user_team", ["user_id"], S)

    # After the name dedupe in BACKFILL_SQL, which is what makes these
    # creatable on a database with duplicate root folder names.
    h.create_index_if_missing(
        "uq_folder_root_name",
        "folder",
        ["space_id", "name"],
        S,
        unique=True,
        postgresql_where=sa.text("parent_id IS NULL AND deleted_at IS NULL"),
    )
    h.create_index_if_missing(
        "uq_folder_child_name",
        "folder",
        ["space_id", "parent_id", "name"],
        S,
        unique=True,
        postgresql_where=sa.text("deleted_at IS NULL"),
    )

    for table in _RENAMED_ID_INDEXES:
        h.drop_index_if_present(f"ix_accounts_{table}_id", table, S)
        h.create_index_if_missing(f"ix_customer_{table}_id", table, ["id"], S)

    op.execute(ROOT_HOME_SQL)


def downgrade() -> None:
    raise NotImplementedError(
        "downgrade is not supported for the consolidated migrations"
    )
