"""One-time operator migration: collapse the legacy ``accounts`` schema into the
configured data schema (``settings.SCHEMA``, default ``customer``).

Context: the accounts→core merge kept two physical schemas (``accounts`` +
``customer``) and merged code only. After the code-side collapse to a single
``SCHEMA``, existing databases still carry the ``accounts`` schema. This script
relocates those tables into ``settings.SCHEMA`` and drops the empty
``accounts`` schema. ``ALTER TABLE ... SET SCHEMA`` is metadata-only and FK
constraints survive (they are tracked by OID, not name).

Safety:
  * **dry-run by default** — prints the plan (clean moves + reconciles + any
    blocking collisions), changes nothing. Pass ``--apply`` to execute.
  * **reconcile expected duplicates** — the legacy two-schema layout left a
    redundant ``customer.user`` mirror alongside the canonical ``accounts.user``
    (see ``RECONCILE_TABLES``). The canonical table wins: its columns, data and
    FKs are kept and it additionally absorbs the mirror's inbound FKs (content
    tables — folder, project, layer, … — are re-pointed at it). Pre-validated:
    every id in the mirror must already exist in the canonical table, else the
    run aborts rather than orphan a dependent.
  * **collision precheck** — any *unexpected* duplicate table name (outside
    ``RECONCILE_TABLES``) aborts the run; resolve those by hand first.
  * **data-table guard** — after moving, the run refuses to drop ``accounts``
    if any data-bearing relation (table/view/sequence) is still in it, and
    rolls back. The legacy authz/trigger *functions* that the old two-schema
    layout kept in ``accounts`` are expected: they are listed, then dropped via
    ``DROP SCHEMA ... CASCADE`` (along with the triggers that use them) and
    rebuilt into the data schema by ``initial_data``.
  * **single transaction** — a failure mid-way rolls the whole move back.
  * ``--rollback`` reverses a *clean* migration (recreates ``accounts`` and
    moves the canonical tables back). It does NOT resurrect a dropped legacy
    mirror — once a reconcile has run, restore from backup instead.

After ``--apply`` you MUST re-run ``initial_data`` so the PL/pgSQL functions
and triggers are reinstalled against the new schema.

Usage (from apps/core):
    uv run python -m core.scripts.collapse_accounts_schema            # dry-run
    uv run python -m core.scripts.collapse_accounts_schema --apply
    uv run python -m core.scripts.collapse_accounts_schema --rollback --apply
"""

import sys

import psycopg

from core.core.config import settings

LEGACY_SCHEMA = "accounts"

# Canonical set of accounts-origin tables (used for --rollback; the forward pass
# moves whatever is actually present in the legacy schema).
ACCOUNTS_TABLES = [
    "cost",
    "credit_usage",
    "invitation",
    "layer_organization",
    "layer_team",
    "layer_user",
    "organization",
    "permission",
    "project_organization",
    "project_team",
    "project_user",
    "resource",
    "resource_grant",
    "resource_permission",
    "role",
    "role_permission",
    "team",
    "user",
    "user_role",
    "user_team",
]

# Tables that exist in BOTH schemas where the legacy ``settings.SCHEMA`` copy is
# a redundant mirror and the ``accounts`` copy is canonical. For each, the move
# can't be a plain rename (the name is taken), so we: drop the inbound FKs that
# point at the legacy mirror, drop the mirror, move the canonical table into
# place, then re-create those inbound FKs against the canonical table. The
# canonical table keeps its own columns, data and FKs and additionally absorbs
# the legacy mirror's dependents. Pre-validated: every id in the mirror must
# already exist in the canonical table, so no reference is orphaned.
RECONCILE_TABLES = ("user",)


def _dsn() -> str:
    return (
        f"host={settings.POSTGRES_SERVER} port={settings.POSTGRES_PORT} "
        f"user={settings.POSTGRES_USER} password={settings.POSTGRES_PASSWORD} "
        f"dbname={settings.POSTGRES_DB}"
    )


def _tables_in(conn: "psycopg.Connection", schema: str) -> list[str]:
    rows = conn.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = %s AND table_type = 'BASE TABLE' ORDER BY table_name",
        (schema,),
    ).fetchall()
    return [r[0] for r in rows]


def _collisions(conn: "psycopg.Connection", target: str, tables: list[str]) -> list[str]:
    existing = set(_tables_in(conn, target))
    return sorted(set(tables) & existing)


def _inbound_fks(
    conn: "psycopg.Connection", schema: str, table: str
) -> list[tuple[str, str, str]]:
    """Foreign keys *pointing at* ``schema.table``, as (child, name, definition).

    ``search_path`` is emptied for the lookup so both the child relation name and
    the constraint definition render fully schema-qualified — the definition then
    references ``schema."table"`` by name, which after the swap resolves to the
    canonical table occupying that name.
    """
    conn.execute("SET LOCAL search_path = ''")
    rows = conn.execute(
        """
        SELECT conrelid::regclass::text AS child,
               conname,
               pg_get_constraintdef(oid) AS def
        FROM pg_constraint
        WHERE contype = 'f'
          AND confrelid = format('%%I.%%I', %s::text, %s::text)::regclass
        ORDER BY conrelid::regclass::text, conname
        """,
        (schema, table),
    ).fetchall()
    conn.execute("RESET search_path")
    return [(r[0], r[1], r[2]) for r in rows]


def _missing_ids(
    conn: "psycopg.Connection", canonical: str, mirror: str, table: str
) -> list[object]:
    """Ids present in the legacy ``mirror.table`` but absent from the canonical
    ``canonical.table`` — any such row would orphan a dependent FK on re-point."""
    rows = conn.execute(
        f'SELECT id FROM "{mirror}"."{table}" '
        f'EXCEPT SELECT id FROM "{canonical}"."{table}"'
    ).fetchall()
    return [r[0] for r in rows]


def _remaining_objects(
    conn: "psycopg.Connection", schema: str
) -> list[tuple[str, str]]:
    """Relations still living in ``schema`` (tables, partitioned tables, views,
    matviews, sequences, foreign tables) — used to refuse dropping the schema
    while anything is left behind."""
    rows = conn.execute(
        """
        SELECT c.relname, c.relkind
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s AND c.relkind IN ('r', 'p', 'v', 'm', 'S', 'f')
        ORDER BY c.relname
        """,
        (schema,),
    ).fetchall()
    return [(r[0], r[1]) for r in rows]


def _functions_in(conn: "psycopg.Connection", schema: str) -> list[str]:
    """Functions/procedures living in ``schema`` (legacy authz + trigger fns in
    the old two-schema layout). They are reinstalled into the data schema by
    ``initial_data``, so dropping them here is safe."""
    rows = conn.execute(
        """
        SELECT p.proname || '(' || pg_get_function_identity_arguments(p.oid) || ')'
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = %s
        ORDER BY 1
        """,
        (schema,),
    ).fetchall()
    return [r[0] for r in rows]


def _triggers_on_functions_in(
    conn: "psycopg.Connection", schema: str
) -> list[tuple[str, str]]:
    """User triggers (on any table) whose function lives in ``schema`` — these
    are dropped by the CASCADE and recreated by ``initial_data``'s
    ``init_triggers``."""
    rows = conn.execute(
        """
        SELECT t.tgname, t.tgrelid::regclass::text
        FROM pg_trigger t
        JOIN pg_proc p ON p.oid = t.tgfoid
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = %s AND NOT t.tgisinternal
        ORDER BY 2, 1
        """,
        (schema,),
    ).fetchall()
    return [(r[0], r[1]) for r in rows]


def forward(apply: bool) -> int:
    target = settings.SCHEMA
    if target == LEGACY_SCHEMA:
        print(f"settings.SCHEMA == '{LEGACY_SCHEMA}'; nothing to collapse.")
        return 0

    with psycopg.connect(_dsn(), autocommit=False) as conn:
        legacy_tables = _tables_in(conn, LEGACY_SCHEMA)
        if not legacy_tables:
            print(f"No '{LEGACY_SCHEMA}' schema tables found — already collapsed.")
            return 0

        collisions = _collisions(conn, target, legacy_tables)
        reconcilable = [t for t in collisions if t in RECONCILE_TABLES]
        blocking = [t for t in collisions if t not in RECONCILE_TABLES]

        print(f"Target schema: {target}")
        print(f"Tables to move ({len(legacy_tables)}): {legacy_tables}")

        if blocking:
            print(
                f"\nABORT — unexpected duplicate table name(s) in '{target}' "
                f"(resolve/drop the legacy duplicates by hand first): {blocking}"
            )
            return 1

        # Plan the reconcile for each expected duplicate (e.g. the legacy
        # `customer.user` mirror vs the canonical `accounts.user`).
        reconcile_plan: dict[str, list[tuple[str, str, str]]] = {}
        for t in reconcilable:
            missing = _missing_ids(conn, LEGACY_SCHEMA, target, t)
            if missing:
                print(
                    f"\nABORT — '{target}.{t}' (legacy mirror) holds {len(missing)} "
                    f"id(s) not present in canonical '{LEGACY_SCHEMA}.{t}'. Re-pointing "
                    f"would orphan their dependents. Reconcile these rows by hand "
                    f"first (sample: {missing[:5]})."
                )
                return 1
            inbound = _inbound_fks(conn, target, t)
            reconcile_plan[t] = inbound
            print(
                f"\nReconcile '{t}': drop legacy mirror '{target}.{t}', adopt "
                f"canonical '{LEGACY_SCHEMA}.{t}', and re-point {len(inbound)} "
                f"inbound FK(s): {[f'{c}.{n}' for c, n, _ in inbound]}"
            )

        # Legacy authz/trigger functions in the old two-schema layout live in
        # `accounts`; the move leaves them (and the triggers using them) behind.
        # They are dropped with the schema and rebuilt by `initial_data` (into
        # `basic`), so surface exactly what will be removed.
        legacy_funcs = _functions_in(conn, LEGACY_SCHEMA)
        dep_triggers = _triggers_on_functions_in(conn, LEGACY_SCHEMA)
        if legacy_funcs:
            print(
                f"\nLegacy '{LEGACY_SCHEMA}' functions to drop (rebuilt by "
                f"initial_data) — {len(legacy_funcs)}: {legacy_funcs}"
            )
        if dep_triggers:
            print(
                f"Dependent triggers to drop (rebuilt by initial_data) — "
                f"{len(dep_triggers)}: {[f'{tbl}:{nm}' for nm, tbl in dep_triggers]}"
            )

        if not apply:
            print("\n[dry-run] no changes made. Re-run with --apply to execute.")
            return 0

        # Detach the inbound FKs from the legacy mirrors, then drop the mirrors so
        # the canonical tables can claim their names.
        for t, inbound in reconcile_plan.items():
            for child, name, _def in inbound:
                conn.execute(f'ALTER TABLE {child} DROP CONSTRAINT "{name}"')
            conn.execute(f'DROP TABLE "{target}"."{t}"')

        # Move every accounts table into the data schema (canonical reconcile
        # tables included — they now own their names).
        for t in legacy_tables:
            conn.execute(f'ALTER TABLE {LEGACY_SCHEMA}."{t}" SET SCHEMA "{target}"')

        # Re-create the inbound FKs; their definitions reference `target."t"` by
        # name, which now resolves to the canonical table.
        for t, inbound in reconcile_plan.items():
            for child, name, def_ in inbound:
                conn.execute(f'ALTER TABLE {child} ADD CONSTRAINT "{name}" {def_}')

        # Never drop the schema while anything is still in it — abort and roll the
        # whole move back so '{LEGACY_SCHEMA}' is left exactly as we found it.
        remaining = _remaining_objects(conn, LEGACY_SCHEMA)
        if remaining:
            conn.rollback()
            print(
                f"\nABORT — '{LEGACY_SCHEMA}' still holds {len(remaining)} object(s) "
                f"after the move; refusing to drop it. Rolled back, nothing changed. "
                f"Left behind: {remaining}"
            )
            return 1

        # CASCADE: the data tables are gone (guarded above); what remains is the
        # legacy function/trigger layer, which initial_data rebuilds. CASCADE
        # drops those plus the triggers depending on them.
        conn.execute(f"DROP SCHEMA {LEGACY_SCHEMA} CASCADE")
        conn.commit()
        print(f"\nMoved {len(legacy_tables)} tables into '{target}' and dropped "
              f"'{LEGACY_SCHEMA}' (with {len(legacy_funcs)} legacy function(s)).")
        if reconcile_plan:
            print(f"Reconciled duplicate(s): {list(reconcile_plan)}.")
        print("NEXT: you MUST re-run initial_data now to reinstall functions/"
              "triggers against the new schema — authz is broken until you do.")
        return 0


def rollback(apply: bool) -> int:
    target = settings.SCHEMA
    with psycopg.connect(_dsn(), autocommit=False) as conn:
        present = [t for t in ACCOUNTS_TABLES if t in set(_tables_in(conn, target))]
        print(f"Will move back to '{LEGACY_SCHEMA}' ({len(present)}): {present}")
        if not apply:
            print("\n[dry-run] no changes made. Re-run with --apply to execute.")
            return 0
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {LEGACY_SCHEMA}")
        for t in present:
            conn.execute(
                f'ALTER TABLE "{target}"."{t}" SET SCHEMA {LEGACY_SCHEMA}'
            )
        conn.commit()
        print(f"\nMoved {len(present)} tables back to '{LEGACY_SCHEMA}'.")
        return 0


def main() -> int:
    apply = "--apply" in sys.argv
    if "--rollback" in sys.argv:
        return rollback(apply)
    return forward(apply)


if __name__ == "__main__":
    raise SystemExit(main())
