#!/usr/bin/env python3
"""Download Google Fonts for local hosting in packages/js/ui/assets/fonts.

Fetches the latin and latin-ext subsets at weights 300/400/500/600/700 (whatever
the font actually offers) as woff2 and writes a font.css with relative URLs.

Re-run when the dashboard font list in apps/web/components/builder/SettingsTab.tsx
changes. Mulish is intentionally excluded — it's already shipped under
assets/fonts/mulish and loaded by next/font in apps/web/app/layout.tsx.
"""
from __future__ import annotations

import re
import sys
import urllib.request
from pathlib import Path

FONTS = [
    "Roboto",
    "Open Sans",
    "Lato",
    "Inter",
    "Poppins",
    "Montserrat",
    "Nunito",
    "Source Sans 3",
    "Raleway",
    "PT Sans",
    "Merriweather",
    "Playfair Display",
    "Lora",
]

WEIGHTS = [300, 400, 500, 600, 700]
KEEP_SUBSETS = {"latin", "latin-ext"}

CHROME_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

OUT_ROOT = Path(__file__).resolve().parents[1] / "packages/js/ui/assets/fonts"

FACE_RE = re.compile(
    r"/\*\s*([\w-]+)\s*\*/\s*(@font-face\s*\{[^}]*\})",
    re.MULTILINE,
)
URL_RE = re.compile(r"url\((https://[^)]+\.woff2)\)")


def slug(name: str) -> str:
    return name.lower().replace(" ", "-")


def fetch(url: str) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": CHROME_UA})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read()


def process(font: str) -> None:
    weights = ";".join(str(w) for w in WEIGHTS)
    css_url = (
        f"https://fonts.googleapis.com/css2?family={font.replace(' ', '+')}"
        f":wght@{weights}&display=swap"
    )
    print(f"→ {font}: fetching CSS", flush=True)
    css = fetch(css_url).decode("utf-8")

    out_dir = OUT_ROOT / slug(font)
    out_dir.mkdir(parents=True, exist_ok=True)

    rewritten: list[str] = []
    weight_seen: dict[tuple[str, int], int] = {}

    for subset, block in FACE_RE.findall(css):
        if subset not in KEEP_SUBSETS:
            continue
        weight_match = re.search(r"font-weight:\s*(\d+)", block)
        if not weight_match:
            continue
        weight = int(weight_match.group(1))
        url_match = URL_RE.search(block)
        if not url_match:
            continue
        url = url_match.group(1)

        key = (subset, weight)
        weight_seen[key] = weight_seen.get(key, 0) + 1
        suffix = f"-{weight_seen[key]}" if weight_seen[key] > 1 else ""
        filename = f"{slug(font)}-{weight}-{subset}{suffix}.woff2"

        target = out_dir / filename
        if not target.exists():
            print(f"  ↓ {filename}", flush=True)
            target.write_bytes(fetch(url))

        rewritten.append(
            f"/* {subset} */\n"
            + block.replace(url, f"./{filename}")
            + "\n"
        )

    (out_dir / "font.css").write_text("\n".join(rewritten), encoding="utf-8")


def main() -> int:
    for font in FONTS:
        try:
            process(font)
        except Exception as e:  # noqa: BLE001
            print(f"  ✗ {font}: {e}", file=sys.stderr)
            return 1
    print(f"\nDone. Output: {OUT_ROOT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
