"""Classify DuckDB errors that mean "the pinned snapshot is behind"."""

from __future__ import annotations

# Substring groups (lowercase) of DuckDB error messages that indicate the
# pinned snapshot does not contain a requested object. Every substring in a
# group must appear for the group to match. Matching makes a force-refresh +
# retry worthwhile; column and syntax errors must NOT match, since refreshing
# cannot fix them and would only trigger useless refresh storms.
PIN_MISS_ERROR_PATTERNS: list[tuple[str, ...]] = [
    # Catalog Error: Table with name X does not exist!
    # Both parts are required so that name-conflict errors on the same phrase
    # ('Table with name X already exists!') do not match.
    ("table with name", "does not exist"),
    # Invalid Input Error: No snapshot found at version N
    # (raised at ATTACH time when the pinned SNAPSHOT_VERSION was expired).
    ("no snapshot found",),
]

_EXCLUDE_PATTERNS = [
    # Binder Error: Referenced column "X" not found in FROM clause!
    # Never treat a missing-column error as a pin miss.
    "referenced column",
]


def is_pin_miss_error(error: Exception) -> bool:
    """True if the error likely means the pinned snapshot is stale."""
    text = str(error).lower()
    if any(p in text for p in _EXCLUDE_PATTERNS):
        return False
    return any(all(part in text for part in group) for group in PIN_MISS_ERROR_PATTERNS)
