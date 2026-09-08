"""Existence-guarded wrappers around the `op.*` DDL calls.

The consolidated migrations (`0001_bundles_catalog` … `0005_home_templates`)
run against two shapes of database. A fresh one is built by `init`, which
calls `SQLModel.metadata.create_all`, so every table, column, index and
constraint the models declare is already there and the whole chain must pass
through without doing anything. A dev- or prod-era one is stamped `init` on
the older schema and needs the statements to run. Routing each statement
through the matching `*_if_missing` / `*_if_present` wrapper here is what
makes one revision correct on both.

Every predicate calls `sa.inspect(op.get_bind())` again rather than holding
an inspector: an inspector caches its reflection, so a column added earlier
in the same migration would still look absent to a later check.

Plain functions with an explicit `schema` argument and no import of `core.*`
— the revision modules own the schema name.
"""

from typing import Any

import sqlalchemy as sa
from alembic import op


def inspector() -> sa.Inspector:
    """A fresh inspector on the migration's bind."""
    return sa.inspect(op.get_bind())


def table_exists(name: str, schema: str) -> bool:
    return name in set(inspector().get_table_names(schema=schema))


def column_exists(table: str, column: str, schema: str) -> bool:
    try:
        columns = inspector().get_columns(table, schema=schema)
    except sa.exc.NoSuchTableError:
        return False
    return any(c["name"] == column for c in columns)


def index_exists(table: str, index: str, schema: str) -> bool:
    try:
        indexes = inspector().get_indexes(table, schema=schema)
    except sa.exc.NoSuchTableError:
        return False
    return any(ix["name"] == index for ix in indexes)


def constraint_exists(table: str, name: str, schema: str) -> bool:
    """True if `table` carries a constraint called `name`, of any kind."""
    insp = inspector()
    try:
        names = {fk["name"] for fk in insp.get_foreign_keys(table, schema=schema)}
        names |= {u["name"] for u in insp.get_unique_constraints(table, schema=schema)}
        names |= {c["name"] for c in insp.get_check_constraints(table, schema=schema)}
        names.add(insp.get_pk_constraint(table, schema=schema).get("name"))
    except sa.exc.NoSuchTableError:
        return False
    return name in names


def foreign_key_on(table: str, column: str, schema: str) -> dict[str, Any] | None:
    """The foreign key constrained on exactly `column`, whatever it is called."""
    try:
        fks = inspector().get_foreign_keys(table, schema=schema)
    except sa.exc.NoSuchTableError:
        return None
    for fk in fks:
        if fk["constrained_columns"] == [column]:
            return fk
    return None


def add_column_if_missing(table: str, column: sa.Column, schema: str) -> None:
    if not table_exists(table, schema):
        return
    if column_exists(table, column.name, schema):
        return
    op.add_column(table, column, schema=schema)


def drop_column_if_present(table: str, column: str, schema: str) -> None:
    if not column_exists(table, column, schema):
        return
    op.drop_column(table, column, schema=schema)


def drop_table_if_present(name: str, schema: str) -> None:
    if not table_exists(name, schema):
        return
    op.drop_table(name, schema=schema)


def create_index_if_missing(
    name: str,
    table: str,
    columns: list[str],
    schema: str,
    unique: bool = False,
    **kw: Any,
) -> None:
    if not table_exists(table, schema):
        return
    if index_exists(table, name, schema):
        return
    op.create_index(name, table, columns, schema=schema, unique=unique, **kw)


def drop_index_if_present(name: str, table: str, schema: str) -> None:
    if not index_exists(table, name, schema):
        return
    op.drop_index(name, table_name=table, schema=schema)


def create_fk_if_missing(
    name: str,
    source: str,
    referent: str,
    local_cols: list[str],
    remote_cols: list[str],
    schema: str,
    **kw: Any,
) -> None:
    if not table_exists(source, schema):
        return
    if constraint_exists(source, name, schema):
        return
    op.create_foreign_key(
        name,
        source,
        referent,
        local_cols,
        remote_cols,
        source_schema=schema,
        referent_schema=schema,
        **kw,
    )


def drop_constraint_if_present(
    name: str, table: str, schema: str, type_: str | None = None
) -> None:
    if not constraint_exists(table, name, schema):
        return
    op.drop_constraint(name, table, schema=schema, type_=type_)


def alter_column_nullable(table: str, column: str, schema: str, nullable: bool) -> None:
    """Set `column`'s NOT NULL to match `nullable`, if it does not already."""
    try:
        columns = inspector().get_columns(table, schema=schema)
    except sa.exc.NoSuchTableError:
        return
    for info in columns:
        if info["name"] == column:
            if bool(info["nullable"]) != nullable:
                op.alter_column(table, column, nullable=nullable, schema=schema)
            return


def ensure_fk(
    name: str,
    source: str,
    referent: str,
    local_cols: list[str],
    remote_cols: list[str],
    schema: str,
    ondelete: str | None = None,
    not_valid: bool = False,
) -> None:
    """Make the foreign key on `local_cols` the one described here.

    Matched by column rather than by name, because the same relationship is
    named differently across the two starting points: a fresh database gets
    Postgres' own `<table>_<column>_fkey` from `create_all`, a dev-era one
    may carry a differently named constraint, the wrong `ON DELETE`, or
    none at all. An equivalent constraint is left untouched so the fresh
    path stays a genuine no-op; a mismatching one is dropped and replaced.

    `not_valid=True` adds the constraint without scanning the existing rows:
    it binds every new write immediately, and tolerates rows already in
    breach. Content tables carry such rows — a `user_id` pointing at a user
    that no longer exists, from before these foreign keys existed — and a
    validating ADD CONSTRAINT would fail on them.
    """
    if not table_exists(source, schema):
        return
    existing = foreign_key_on(source, local_cols[0], schema) if local_cols else None
    if existing is not None:
        current = (existing.get("options") or {}).get("ondelete")
        same_target = existing.get("referred_table") == referent
        same_rule = (current or "").upper() == (ondelete or "").upper()
        if same_target and same_rule:
            return
        op.drop_constraint(existing["name"], source, schema=schema, type_="foreignkey")
    if not_valid:
        cols = ", ".join(f'"{c}"' for c in local_cols)
        refs = ", ".join(f'"{c}"' for c in remote_cols)
        statement = (
            f'ALTER TABLE "{schema}"."{source}" ADD CONSTRAINT "{name}" '
            f'FOREIGN KEY ({cols}) REFERENCES "{schema}"."{referent}" ({refs})'
        )
        if ondelete:
            statement += f" ON DELETE {ondelete}"
        op.execute(statement + " NOT VALID")
        return
    op.create_foreign_key(
        name,
        source,
        referent,
        local_cols,
        remote_cols,
        source_schema=schema,
        referent_schema=schema,
        ondelete=ondelete,
    )
