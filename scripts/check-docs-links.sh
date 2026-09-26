#!/usr/bin/env bash
# Fail when a change adds an external link to the docs that does not work.
#
# Only URLs on lines the change adds, and not already on a line it removes,
# are checked, so editing a page never fails on an older link that broke
# elsewhere; the weekly docs-links workflow reports those. Links between docs
# pages are checked by the docs build. Links to the docs site itself are left
# out: a page the change adds is not published yet.
#
# Needs lychee on PATH (.github/actions/setup-lychee).
#
# Usage: scripts/check-docs-links.sh <base> [<head>]
#   e.g. scripts/check-docs-links.sh origin/main
set -euo pipefail

base=${1:?usage: $0 <base> [<head>]}
head=${2:-HEAD}
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

# Read from the commit under test, not the working tree: CI checks out only
# scripts/.
git show "$head:apps/docs/lychee.toml" >"$tmp/lychee.toml"

git diff -U0 --no-color --no-ext-diff "$base" "$head" -- \
  ':(glob)apps/docs/**/*.md' ':(glob)apps/docs/**/*.mdx' \
  ':(glob)apps/docs/src/**/*.js' ':(glob)apps/docs/src/**/*.tsx' \
  apps/docs/docusaurus.config.js >"$tmp/diff"

# Markdown files, so lychee reads link syntax and skips code spans.
lines() {
  grep -E "^\\$1" "$tmp/diff" | grep -vE '^(\+\+\+|---) ' | cut -c2- || true
}
lines + >"$tmp/added.md"
lines - >"$tmp/removed.md"

for side in added removed; do
  # Its warnings about root-relative links are expected, so stderr is shown
  # only when it fails.
  if ! lychee --config "$tmp/lychee.toml" --root-dir "$tmp" --dump "$tmp/$side.md" \
    >"$tmp/$side.urls" 2>"$tmp/$side.log"; then
    cat "$tmp/$side.log" >&2
    exit 1
  fi
  { grep -vE '^https?://goat\.plan4better\.de/docs(/|$)' "$tmp/$side.urls" || true; } |
    sort -u >"$tmp/$side.sorted"
done
comm -23 "$tmp/added.sorted" "$tmp/removed.sorted" >"$tmp/new.txt"

if [[ ! -s $tmp/new.txt ]]; then
  echo "No new external links in the docs."
  exit 0
fi
echo "Checking $(wc -l <"$tmp/new.txt") new external link(s):"
sed 's/^/  /' "$tmp/new.txt"
if ! lychee --config "$tmp/lychee.toml" "$tmp/new.txt"; then
  echo "::error::The change adds external links to the docs that do not work (listed above). Fix the URL, or link to an archived copy."
  exit 1
fi
