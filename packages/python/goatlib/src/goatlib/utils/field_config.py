"""A layer's `field_config`: what each column means beyond its storage type.

Kept here rather than in a service because two writers produce it — geoapi when
a column is added to a layer that already exists, and the layer-create tool when
a layer is declared with its columns up front. A column created either way has
to mean the same thing afterwards, so the rules for building an entry live in
one place.
"""

from typing import Any


def coerce_allowed_values(kind: str | None, values: list[Any]) -> list[Any]:
    """The vocabulary as the column's own type, or a ValueError naming the culprit.

    A number column holding the string "30" would never match the 30 a write
    sends, so the dropdown would offer a value that then fails validation. The
    list is coerced once, here, rather than compared loosely everywhere.
    """
    if kind not in ("number", "integer"):
        return [str(v) for v in values]
    coerced: list[Any] = []
    for value in values:
        try:
            number = float(value)
        except (TypeError, ValueError):
            raise ValueError(
                f"'{value}' is not a number, so it cannot be an allowed value "
                "for a number column."
            ) from None
        coerced.append(int(number) if number.is_integer() else number)
    return coerced
