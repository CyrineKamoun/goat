#!/usr/bin/env bash
# Fail when a change adds or modifies a file beyond the size limit for its type.
#
# Every CI job and every clone downloads the whole tree, and git keeps each
# version of a file forever, so large binaries cost on every run long after
# they are replaced. Record screen captures as MP4 (the docs <Video> component)
# rather than GIF, and save screenshots as WebP.
#
# A file that really has to be larger can be listed in .github/large-files.txt
# (one glob per line, # for comments) in the same change, where review sees it.
#
# Usage: scripts/check-file-sizes.sh <base> [<head>]
#   e.g. scripts/check-file-sizes.sh origin/main
set -euo pipefail

base=${1:?usage: $0 <base> [<head>]}
head=${2:-HEAD}
allowlist=.github/large-files.txt
MB=$((1024 * 1024))

# Read from the commit under test, not the working tree: CI checks out only
# scripts/.
allowlist_globs=$(git show "$head:$allowlist" 2>/dev/null || true)

allowed() {
  local glob
  while IFS= read -r glob; do
    [[ -z $glob || $glob == \#* ]] && continue
    # shellcheck disable=SC2053 # $glob is matched as a pattern on purpose
    [[ $1 == $glob ]] && return 0
  done <<<"$allowlist_globs"
  return 1
}

failed=0
while IFS= read -r -d '' path; do
  case "${path,,}" in
    *.mp4 | *.webm) limit=$((4 * MB)) ;;
    *.png | *.jpg | *.jpeg | *.gif | *.webp) limit=$((1 * MB)) ;;
    *) limit=$((2 * MB)) ;;
  esac
  size=$(git cat-file -s "$head:$path")
  ((size > limit)) || continue
  allowed "$path" && continue
  printf '::error file=%s::%s is %s KB, over the %s MB limit for its type. Record screen captures as MP4 and save screenshots as WebP, or list the path in %s.\n' \
    "$path" "$path" $((size / 1024)) $((limit / MB)) "$allowlist"
  failed=1
done < <(git diff -z --name-only --diff-filter=AM "$base" "$head")

exit "$failed"
