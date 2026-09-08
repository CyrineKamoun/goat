"""CQL2 filter compilation (OGC API - Features Part 3: Filtering).

Parses a ``cql2-text`` or ``cql2-json`` filter expression (via pygeofilter)
and compiles it into a ``(sql_fragment, params)`` pair that
``catalog.services.search.SearchParams.cql`` accepts and ``build_filters``
ANDs verbatim into its WHERE clause, using DuckDB positional ``?``
placeholders.

The actual AST -> SQL evaluator lives in goatlib
(``goatlib.storage.cql_evaluator``) and is reused as-is; this module's own
job is: pick the right parser for the given language, validate that every
referenced property is one the catalog advertises as queryable
(``catalog.services.registry``, derived from the loaded table)
(anything else is a 400, never a silently-ignored filter), validate
``filter_crs`` (only CRS84 or unset), and translate any parse/evaluation
failure into ``ApiError(400, ...)`` so the router never has to know about
pygeofilter's own exception types.
"""

import json
from typing import Any

from goatlib.storage.cql_evaluator import cql2_to_duckdb_sql
from pygeofilter import ast
from pygeofilter.parsers.cql2_json import parse as parse_cql2_json
from pygeofilter.parsers.cql2_text import parse as parse_cql2_text

from catalog.errors import ApiError
from catalog.services.registry import QueryableRegistry

# The only filter-crs the API supports; anything else is a 400 (§7.4 of the
# OGC API - Features Part 1 core: CRS84 is the default/only-required CRS).
_CRS84 = "http://www.opengis.net/def/crs/OGC/1.3/CRS84"

_VALID_FILTER_LANGS = ("cql2-text", "cql2-json")


def _collect_attribute_names(node: Any, names: set[str]) -> None:
    """Recursively collect every ``ast.Attribute.name`` referenced in ``node``.

    Walks the generic ``Node.get_sub_nodes()`` tree (plus raw lists, since
    some sub-node slots -- e.g. an ``IN`` predicate's option list -- are
    plain Python lists rather than ``Node`` instances). Non-attribute leaves
    (string/number literals, geometries, ...) are simply ignored.
    """
    if isinstance(node, ast.Attribute):
        names.add(node.name)
        return
    if isinstance(node, ast.Node):
        for sub in node.get_sub_nodes():
            _collect_attribute_names(sub, names)
        return
    if isinstance(node, (list, tuple)):
        for item in node:
            _collect_attribute_names(item, names)


def _validate_queryables(cql_ast: Any, registry: QueryableRegistry) -> None:
    """Raise ``ApiError(400)`` naming any property the registry doesn't have.

    A filter on an unknown property is rejected rather than silently matched
    against nothing. rustac's DuckDB backend takes the other option (an
    unmatched property makes the whole search return no rows), which is what
    stac-api-validator expects; for a hand-written filter over a dataset
    catalog, naming the bad property beats an empty page that looks like a
    legitimate "no results".
    """
    names: set[str] = set()
    _collect_attribute_names(cql_ast, names)
    unknown = sorted(name for name in names if name not in registry)
    if unknown:
        raise ApiError(400, f"unknown queryable(s): {', '.join(unknown)}")


def compile_cql2(
    filter_value: str | dict[str, Any],
    filter_lang: str | None,
    filter_crs: str | None,
    registry: QueryableRegistry,
) -> tuple[str, list[Any]]:
    """Compile a CQL2 filter into a ``(sql_fragment, params)`` pair.

    ``filter_lang`` must be exactly ``"cql2-text"`` or ``"cql2-json"``;
    ``None`` defaults to ``"cql2-text"`` here -- choosing the per-verb
    default (cql2-text for GET, cql2-json for POST) is the router's job,
    not this function's. Raises ``ApiError(400)`` on: an invalid
    ``filter_lang``, a parse error, a reference to an unknown queryable, or
    an unsupported ``filter_crs``.
    """
    lang = filter_lang if filter_lang is not None else "cql2-text"
    if lang not in _VALID_FILTER_LANGS:
        raise ApiError(400, f"unsupported filter-lang: {filter_lang!r}")

    if filter_crs is not None and filter_crs != _CRS84:
        raise ApiError(400, f"unsupported filter-crs: {filter_crs!r}")

    try:
        if lang == "cql2-json":
            source: str | dict[str, Any] = (
                filter_value
                if isinstance(filter_value, (str, dict))
                else json.dumps(filter_value)
            )
            cql_ast = parse_cql2_json(source)
        else:
            if not isinstance(filter_value, str):
                raise ApiError(400, "cql2-text filter must be a string")
            cql_ast = parse_cql2_text(filter_value)
    except ApiError:
        raise
    except Exception as exc:
        raise ApiError(400, f"invalid CQL2 filter: {exc}") from exc

    _validate_queryables(cql_ast, registry)

    try:
        sql, params = cql2_to_duckdb_sql(
            cql_ast,
            registry.names,
            geometry_column="geometry",
            # The registry, not the evaluator's name-quoting, decides what a
            # property compiles to. Without this, a queryable that is not a
            # plain column (`year`, `goat:geographical_code`) and the
            # `properties.`-prefixed spelling both pass validation above and
            # then fail in the evaluator as an unknown field.
            field_exprs=registry.expr_map(),
        )
    except ValueError as exc:
        raise ApiError(400, str(exc)) from exc

    return sql, params
