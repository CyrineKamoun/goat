"""Validation for SQL a user writes: a query over declared inputs, or a
boolean expression over one layer's columns.

User SQL runs on connections with the whole DuckLake attached, so a query may
read only the input tables a request declares (plus its own CTEs), and
neither may name a table by schema or catalog, read a file, call a table
function other than the number generators, or call a file, settings or
catalog function.

The checks walk DuckDB's own parse tree (`json_serialize_sql`), not a second
parser's: what is checked is exactly what DuckDB will run, so no quoting,
comment or dialect difference can hide a table from the check. Needs only
duckdb, so services can use it without the tool runtime.
"""

import json
import re
from collections.abc import Iterable
from typing import Any

import duckdb

# Functions that reach files, other databases, settings or the catalog of
# every table on the connection. The connections that run user SQL have
# DuckLake attached, so any of these would read data the query never declared.
FORBIDDEN_FUNCTION_PREFIXES = (
    "read_",
    "parquet_",
    "duckdb_",
    "ducklake_",
    "pragma_",
    "sniff_",
    "iceberg_",
    "delta_",
    "postgres_",
    "sqlite_",
    "mysql_",
    "st_read",
)
FORBIDDEN_FUNCTIONS = frozenset(
    {
        "glob",
        "query",
        "query_table",
        "getenv",
        "getvariable",
        "current_setting",
        "which_secret",
        "load_aws_credentials",
        "json_serialize_sql",
        "json_deserialize_sql",
        "json_execute_serialized_sql",
        "sql_auto_complete",
    }
)
# Table functions a query may call: they only generate rows from their
# arguments, and any subquery in those arguments is checked like the rest.
ALLOWED_TABLE_FUNCTIONS = frozenset({"range", "generate_series", "unnest"})

_PLAIN_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_QUERY_NODES = frozenset(
    {"SELECT_NODE", "SET_OPERATION_NODE", "RECURSIVE_CTE_NODE", "CTE_NODE"}
)
# FROM items other than a named table or a table function. Anything not
# listed (SHOW, DESCRIBE, column data) is refused.
_PLAIN_TABLE_REFS = frozenset(
    {"SUBQUERY", "JOIN", "EXPRESSION_LIST", "EMPTY", "EMPTY_FROM", "PIVOT"}
)


def _parse(sql: str) -> dict[str, Any]:
    """DuckDB's parse tree of exactly one SELECT statement."""
    connection = duckdb.connect()
    try:
        row = connection.execute("SELECT json_serialize_sql(?)", [sql]).fetchone()
    finally:
        connection.close()
    tree = json.loads(row[0]) if row else {"error": True}
    if tree.get("error"):
        message = tree.get("error_message") or "Could not parse the SQL"
        raise ValueError(f"Only a single SELECT query is allowed: {message}")
    statements = tree.get("statements") or []
    if len(statements) != 1:
        raise ValueError("Exactly one SELECT statement is allowed")
    node: dict[str, Any] = statements[0]["node"]
    if node.get("type") not in _QUERY_NODES:
        raise ValueError("Only SELECT statements are allowed")
    return node


def _check_function(node: dict[str, Any]) -> None:
    name = str(node.get("function_name") or "").lower()
    # Built-ins and list/struct literals resolve to `main` or `system.main`;
    # a function named in any other catalog or schema is refused.
    if node.get("schema") not in ("", "main", None) or node.get("catalog") not in (
        "",
        "system",
        None,
    ):
        raise ValueError(f"Function not allowed: {name}")
    if name.startswith(FORBIDDEN_FUNCTION_PREFIXES) or name in FORBIDDEN_FUNCTIONS:
        raise ValueError(f"Function not allowed: {name}")


def _check_tree(
    node: Any, allowed: set[str] | None, visible_ctes: frozenset[str]
) -> None:
    """Every table, table function and function anywhere in the tree."""
    if isinstance(node, list):
        for item in node:
            _check_tree(item, allowed, visible_ctes)
        return
    if not isinstance(node, dict):
        return

    # A CTE is visible to its own query and everything inside it, not to
    # the query around it.
    cte_map = node.get("cte_map")
    if isinstance(cte_map, dict):
        names = {str(entry.get("key", "")).lower() for entry in cte_map.get("map", [])}
        visible_ctes = visible_ctes | names

    kind = node.get("type")
    if "class" not in node:  # a FROM item or a query node, not an expression
        if kind == "BASE_TABLE":
            name = str(node.get("table_name") or "")
            if (
                node.get("schema_name")
                or node.get("catalog_name")
                or not _PLAIN_NAME.fullmatch(name)
            ):
                raise ValueError(
                    "Only the query's input tables can be read, by their plain name"
                )
            if allowed is not None and name.lower() not in allowed | visible_ctes:
                raise ValueError(f"Unknown table: {name}")
        elif kind == "TABLE_FUNCTION":
            function = node.get("function") or {}
            name = str(function.get("function_name") or "").lower()
            if name not in ALLOWED_TABLE_FUNCTIONS:
                raise ValueError(f"Table function not allowed: {name}")
        elif (
            isinstance(kind, str)
            and kind not in _PLAIN_TABLE_REFS
            and kind not in _QUERY_NODES
            and kind.isupper()
            and ("alias" in node and "sample" in node)
        ):
            raise ValueError(f"Not allowed in FROM: {kind}")
    elif node.get("class") == "FUNCTION":
        _check_function(node)

    for value in node.values():
        if isinstance(value, (dict, list)):
            _check_tree(value, allowed, visible_ctes)


def validate_sql_query(sql: str, allowed_tables: Iterable[str] | None = None) -> None:
    """A single SELECT that reads only its declared input tables.

    Args:
        sql: The SQL query to validate
        allowed_tables: The table names the query may read (its input
            aliases), besides the CTEs it defines. None checks only that every
            table is a plain name.

    Raises:
        ValueError: If the query is not one SELECT, or names a table,
            table function or function it may not
    """
    if not sql or not sql.strip():
        raise ValueError("SQL query cannot be empty")
    node = _parse(sql)
    allowed = (
        {name.lower() for name in allowed_tables}
        if allowed_tables is not None
        else None
    )
    _check_tree(node, allowed, frozenset())


def validate_sql_expression(expression: str) -> None:
    """A single SQL expression over the row's columns, nothing else.

    For conditions the server places inside its own query (the workflow if
    node, geoapi's expression preview filter): no subquery, no table, no
    clause beyond the one expression, and none of the file, database or
    catalog functions refused in queries.
    """
    if not expression or not expression.strip():
        raise ValueError("Expression cannot be empty")
    node = _parse(f"SELECT ({expression})")
    from_table = node.get("from_table") or {}
    if (
        node.get("type") != "SELECT_NODE"
        or len(node.get("select_list") or []) != 1
        or from_table.get("type") not in ("EMPTY", "EMPTY_FROM")
        or node.get("modifiers")
        or (node.get("cte_map") or {}).get("map")
        or any(
            node.get(clause)
            for clause in ("where_clause", "having", "qualify", "sample")
        )
        or node.get("group_expressions")
        or '"SUBQUERY"' in json.dumps(node)
    ):
        raise ValueError("The expression may not contain a query, a table or a clause")
    _check_tree(node, set(), frozenset())
