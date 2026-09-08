"""Small shared helpers. Most of what used to live here was dead; what is left
is what something imports."""

import os
import re

from core.utils.partial import optional  # canonical single impl (re-exported)

__all__ = ["optional", "sanitize_filename"]


def sanitize_filename(name: str) -> str:
    # Extract just the file name (no paths)
    name = os.path.basename(name)
    # Allow only safe chars
    name = re.sub(r"[^a-zA-Z0-9._()\-\s]", "_", name)
    return name
