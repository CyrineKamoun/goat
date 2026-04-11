from __future__ import annotations

import ctypes
import sys
from pathlib import Path


def _preload_duckdb_shared_lib() -> None:
    """Load bundled libduckdb globally so the C++ extension can resolve symbols."""
    candidates: list[Path] = []

    package_dir = Path(__file__).resolve().parent
    candidates.append(package_dir / "libduckdb.so")

    for entry in sys.path:
        path_entry = Path(entry)
        candidates.append(path_entry / "routing" / "libduckdb.so")

    seen: set[str] = set()
    for candidate in candidates:
        resolved = str(candidate)
        if resolved in seen:
            continue
        seen.add(resolved)

        if not candidate.exists():
            continue

        ctypes.CDLL(str(candidate), mode=ctypes.RTLD_GLOBAL)
        return


_preload_duckdb_shared_lib()

try:
    from ._routing import *
except ModuleNotFoundError:
    from _routing import *
