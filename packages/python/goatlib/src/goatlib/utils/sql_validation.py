"""Validation for SQL a user writes: a query over declared inputs, or a
boolean expression over one layer's columns.

User SQL runs on connections with the whole DuckLake attached, so a query may
read only the input tables a request declares (plus its own CTEs), and
neither may name a table by schema or catalog, read a file, call a table
function other than the number generators, or call a file, settings or
catalog function. Kept free of tool imports so services can use it without
the tool runtime.
"""

import re
from collections.abc import Iterable

import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError

# SQL keywords that indicate non-SELECT statements
FORBIDDEN_SQL_KEYWORDS = {
    "CREATE",
    "DROP",
    "INSERT",
    "UPDATE",
    "DELETE",
    "ALTER",
    "TRUNCATE",
    "COPY",
    "ATTACH",
    "DETACH",
    "EXPORT",
    "IMPORT",
    "LOAD",
    "INSTALL",
    "GRANT",
    "REVOKE",
    "PRAGMA",
    "SET",
    "CALL",
}


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
        "current_setting",
        "which_secret",
        "load_aws_credentials",
        "json_execute_serialized_sql",
        "sql_auto_complete",
    }
)
_PLAIN_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _check_sources(sql: str, allowed_tables: Iterable[str] | None) -> None:
    """Only the query's own inputs may be read.

    Every table must be a plain name (no schema or catalog, no file path, no
    table function other than the number generators) and, when `allowed_tables` is given, one of them or a CTE
    the query defines itself. File, database and catalog functions are
    refused everywhere in the query.
    """
    try:
        tree = sqlglot.parse_one(sql, read="duckdb")
    except ParseError as e:
        raise ValueError(f"Could not parse the SQL query: {e}") from e
    if tree is None:
        raise ValueError("SQL query cannot be empty")
    defined = {cte.alias_or_name.lower() for cte in tree.find_all(exp.CTE)}
    allowed = (
        {t.lower() for t in allowed_tables} | defined
        if allowed_tables is not None
        else None
    )
    for table in tree.find_all(exp.Table):
        # `range` / `generate_series` only generate numbers; any subquery in
        # their arguments is itself a table this loop checks.
        if isinstance(table.this, exp.GenerateSeries):
            continue
        name = table.name
        if (
            not isinstance(table.this, exp.Identifier)
            or table.args.get("db")
            or table.args.get("catalog")
            or not _PLAIN_NAME.match(name)
        ):
            raise ValueError(
                "Only the query's input tables can be read, by their plain name"
            )
        if allowed is not None and name.lower() not in allowed:
            raise ValueError(f"Unknown table: {name}")
    for function in tree.find_all(exp.Func):
        name = (
            function.name
            if isinstance(function, exp.Anonymous)
            else function.sql_name()
        ).lower()
        if name.startswith(FORBIDDEN_FUNCTION_PREFIXES) or name in FORBIDDEN_FUNCTIONS:
            raise ValueError(f"Function not allowed: {name}")


def validate_sql_expression(expression: str) -> None:
    """A single SQL expression over the row's columns, nothing else.

    For conditions the server places inside its own query (the workflow if
    node): no subquery, no table, and none of the file, database or catalog
    functions refused in queries.
    """
    try:
        tree = sqlglot.parse_one(f"SELECT ({expression})", read="duckdb")
    except ParseError as e:
        raise ValueError(f"Could not parse the expression: {e}") from e
    if (
        not isinstance(tree, exp.Select)
        or len(list(tree.find_all(exp.Select))) != 1
        or any(tree.find_all(exp.Table))
        or any(tree.find_all(exp.Subquery))
    ):
        raise ValueError("The expression may not contain a query or a table")
    for function in tree.find_all(exp.Func):
        name = (
            function.name
            if isinstance(function, exp.Anonymous)
            else function.sql_name()
        ).lower()
        if name.startswith(FORBIDDEN_FUNCTION_PREFIXES) or name in FORBIDDEN_FUNCTIONS:
            raise ValueError(f"Function not allowed: {name}")


def validate_sql_query(sql: str, allowed_tables: Iterable[str] | None = None) -> None:
    """Validate that a SQL query is a safe SELECT statement over its inputs.

    Args:
        sql: The SQL query to validate
        allowed_tables: The table names the query may read (its input
            aliases). None checks only that every table is a plain name.

    Raises:
        ValueError: If the query contains forbidden statements, tables or
            functions
    """
    if not sql or not sql.strip():
        raise ValueError("SQL query cannot be empty")

    # Normalize whitespace and strip comments
    cleaned = re.sub(r"--.*$", "", sql, flags=re.MULTILINE)  # line comments
    cleaned = re.sub(r"/\*.*?\*/", "", cleaned, flags=re.DOTALL)  # block comments
    cleaned = cleaned.strip()

    if not cleaned:
        raise ValueError("SQL query cannot be empty after removing comments")

    # Check that the query starts with SELECT or WITH (for CTEs)
    first_word = cleaned.split()[0].upper()
    if first_word not in ("SELECT", "WITH"):
        raise ValueError(f"Only SELECT statements are allowed. Got: {first_word}")

    # Check for forbidden keywords at statement boundaries
    # Split on semicolons to handle multi-statement injection attempts
    statements = [s.strip() for s in cleaned.split(";") if s.strip()]
    if len(statements) > 1:
        raise ValueError("Multiple SQL statements are not allowed")

    # Check for forbidden keywords as standalone words (not inside strings)
    # Remove string literals first to avoid false positives
    no_strings = re.sub(r"'[^']*'", "", cleaned)
    words = re.findall(r"\b[A-Za-z_]+\b", no_strings)
    upper_words = {w.upper() for w in words}

    forbidden_found = upper_words & FORBIDDEN_SQL_KEYWORDS
    if forbidden_found:
        raise ValueError(
            f"Forbidden SQL keywords found: {', '.join(sorted(forbidden_found))}"
        )

    _check_sources(cleaned, allowed_tables)
