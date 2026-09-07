import subprocess
from pathlib import Path

import pytest
from core.core.config import settings
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

LEGACY = [
    "layer_user",
    "layer_team",
    "layer_organization",
    "project_user",
    "project_team",
    "project_organization",
]

# Absolute, not relative to cwd: this test must pass or fail the same way
# whether pytest is invoked from apps/core or the repo root. A relative path
# that doesn't resolve from the caller's cwd makes grep exit 2 ("No such
# file or directory") with empty stdout — indistinguishable from "no
# matches" if only stdout is checked, which would pass this test vacuously.
_CORE_SRC = Path(__file__).resolve().parents[2] / "src" / "core"
_GEOAPI_SRC = Path(__file__).resolve().parents[3] / "geoapi" / "src"
_GOATLIB_SRC = (
    Path(__file__).resolve().parents[4] / "packages" / "python" / "goatlib" / "src"
)
SEARCH_ROOTS = [_CORE_SRC, _GEOAPI_SRC, _GOATLIB_SRC]


@pytest.mark.asyncio
async def test_no_legacy_tables_in_metadata(db_session: AsyncSession) -> None:
    names = {
        r[0]
        for r in (
            await db_session.execute(
                text(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = :s"
                ),
                {"s": settings.SCHEMA},
            )
        ).all()
    }
    assert not (set(LEGACY) & names)


def test_no_code_references_legacy_tables() -> None:
    for root in SEARCH_ROOTS:
        assert root.is_dir(), f"search root does not exist: {root}"

    # Word-boundary pattern, not a bare substring join: "|".join(LEGACY)
    # would also match e.g. "layer_user_id" or "old_project_team_thing" —
    # \b anchors to the six exact legacy names only.
    pattern = r"\b(layer|project)_(user|team|organization)\b"
    result = subprocess.run(
        ["grep", "-rln", "-E", pattern, *(str(r) for r in SEARCH_ROOTS)],
        capture_output=True,
        text=True,
    )
    # grep exit codes: 0 = matches found, 1 = no matches, 2 = usage/IO error
    # (e.g. a bad path). Only 0 or 1 are valid outcomes here; 2 must fail
    # loudly instead of being swallowed by an empty-stdout check.
    assert result.returncode in (
        0,
        1,
    ), f"grep failed (exit {result.returncode}): {result.stderr}"
    assert result.stdout.strip() == "", f"still referenced in:\n{result.stdout}"
