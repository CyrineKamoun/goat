#!/usr/bin/env bash
#
# One-time local DuckLake catalog upgrade: format 0.3 -> 1.0 (DuckDB 1.4.x -> 1.5.4).
#
# Run this ONCE on your local dev machine after pulling the branch that bumps
# duckdb to >=1.5.4. It migrates your local DuckLake catalog in Postgres so your
# local geoapi / processes / windmill workers (now on duckdb 1.5.4) can attach it.
# A 1.4.x binary CANNOT attach a migrated 1.0 catalog and vice-versa, so do this
# together with switching your local services to the new code.
#
# Safe to re-run: if the catalog is already 1.0 it exits without touching anything.
#
# Reads connection settings from the repo-root .env (POSTGRES_*, DATA_DIR /
# DUCKLAKE_DATA_DIR, DUCKLAKE_CATALOG_SCHEMA, S3_* if you use MinIO).
#
# Usage:
#   ./scripts/db/upgrade-ducklake-v1-local.sh          # prompts before migrating
#   ./scripts/db/upgrade-ducklake-v1-local.sh -y        # no prompt
#   ./scripts/db/upgrade-ducklake-v1-local.sh --no-backup
#
set -euo pipefail

# ---- locate repo root + load .env -----------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"   # scripts/db/ -> repo root
ENV_FILE="${ENV_FILE:-$REPO_ROOT/.env}"

ASSUME_YES=0
DO_BACKUP=1
for arg in "$@"; do
  case "$arg" in
    -y|--yes) ASSUME_YES=1 ;;
    --no-backup) DO_BACKUP=0 ;;
    -h|--help) grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "unknown arg: $arg" >&2; exit 2 ;;
  esac
done

bold() { printf '\033[1m%s\033[0m\n' "$*"; }
info() { printf '\033[36m›\033[0m %s\n' "$*"; }
warn() { printf '\033[33m! %s\033[0m\n' "$*"; }
err()  { printf '\033[31m✗ %s\033[0m\n' "$*" >&2; }
ok()   { printf '\033[32m✓ %s\033[0m\n' "$*"; }

[ -f "$ENV_FILE" ] || { err "no .env found at $ENV_FILE (set ENV_FILE=... to override)"; exit 1; }
info "loading env from $ENV_FILE"

# Read one key from .env WITHOUT sourcing it — .env values (base64, multi-line,
# special chars) are safe for python-dotenv but not for bash `source`. Real env
# vars already set in the shell take precedence.
getenv() {
  local key="$1" line val
  if [ -n "${!key:-}" ]; then printf '%s' "${!key}"; return; fi
  line="$(grep -E "^[[:space:]]*(export[[:space:]]+)?${key}=" "$ENV_FILE" | tail -1 || true)"
  [ -n "$line" ] || return 0
  val="${line#*=}"; val="${val%$'\r'}"
  case "$val" in
    \"*\") val="${val#\"}"; val="${val%\"}" ;;
    \'*\') val="${val#\'}"; val="${val%\'}" ;;
  esac
  printf '%s' "$val"
}

# ---- resolve config (same defaults the app uses) ---------------------------
PGHOST="$(getenv POSTGRES_SERVER)";      PGHOST="${PGHOST:-localhost}"
PGPORT="$(getenv POSTGRES_PORT)";        PGPORT="${PGPORT:-5432}"
PGUSER="$(getenv POSTGRES_USER)";        PGUSER="${PGUSER:-postgres}"
PGDB="$(getenv POSTGRES_DB)";            PGDB="${PGDB:-goat}"
CATALOG_SCHEMA="$(getenv DUCKLAKE_CATALOG_SCHEMA)"; CATALOG_SCHEMA="${CATALOG_SCHEMA:-ducklake}"
POSTGRES_PASSWORD="$(getenv POSTGRES_PASSWORD)"
DUCKLAKE_DATA_DIR="$(getenv DUCKLAKE_DATA_DIR)"
DATA_DIR="$(getenv DATA_DIR)"
S3_ENDPOINT_URL="$(getenv S3_ENDPOINT_URL)"
S3_ACCESS_KEY_ID="$(getenv S3_ACCESS_KEY_ID)"
S3_SECRET_ACCESS_KEY="$(getenv S3_SECRET_ACCESS_KEY)"
if [ -n "$DUCKLAKE_DATA_DIR" ]; then
  DATA_PATH="$DUCKLAKE_DATA_DIR"
else
  DATA_PATH="${DATA_DIR:-/tmp}/ducklake"
fi

# Local devs commonly have POSTGRES_SERVER=db (the compose service name), which
# only resolves inside the docker network. If it's not reachable from the host,
# fall back to localhost.
if [ "$PGHOST" = "db" ]; then
  warn "POSTGRES_SERVER=db (compose network name) — using localhost from the host"
  PGHOST="localhost"
fi

[ -n "${POSTGRES_PASSWORD:-}" ] || { err "POSTGRES_PASSWORD not set in $ENV_FILE"; exit 1; }
export PGPASSWORD="$POSTGRES_PASSWORD"   # libpq (duckdb postgres ext + pg_dump) reads this

bold "DuckLake local upgrade 0.3 → 1.0"
info "postgres : $PGUSER@$PGHOST:$PGPORT/$PGDB  (schema: $CATALOG_SCHEMA)"
info "data path: $DATA_PATH"

# ---- 1. verify the local duckdb is 1.5.x -----------------------------------
command -v uv >/dev/null || { err "uv not found — install uv and run 'uv sync --all-packages'"; exit 1; }
DUCKVER="$(cd "$REPO_ROOT" && uv run python -c 'import duckdb; print(duckdb.__version__)' 2>/dev/null || true)"
case "$DUCKVER" in
  1.5.*|1.6.*|1.7.*|2.*) ok "local duckdb $DUCKVER" ;;
  "") err "could not import duckdb via 'uv run' — run 'uv sync --all-packages' first"; exit 1 ;;
  *) err "local duckdb is $DUCKVER, need >=1.5.4 — pull the upgrade branch + 'uv sync --all-packages'"; exit 1 ;;
esac

# ---- 2. best-effort backup (local data, so this is a convenience not a gate) -
if [ "$DO_BACKUP" = 1 ]; then
  if command -v pg_dump >/dev/null; then
    BK="$REPO_ROOT/ducklake_local_pre15_$(date +%Y%m%d_%H%M%S).dump"
    info "backing up the '$CATALOG_SCHEMA' schema → $BK"
    if pg_dump -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDB" -n "$CATALOG_SCHEMA" -Fc -f "$BK"; then
      ok "backup written ($(du -h "$BK" | cut -f1))"
    else
      warn "pg_dump failed — continuing without a backup (local dev data)"
    fi
  else
    warn "pg_dump not on PATH — skipping backup (local dev data, non-sensitive)"
  fi
fi

# ---- 3. confirm ------------------------------------------------------------
if [ "$ASSUME_YES" != 1 ]; then
  printf '\033[1mProceed with the migration? [y/N] \033[0m'
  read -r reply
  case "$reply" in y|Y|yes|YES) ;; *) info "aborted"; exit 0 ;; esac
fi

# ---- 4. migrate (all catalog-metadata work is done here, in one python run) -
# Config is passed via env (already exported / from .env); the heredoc is quoted
# so nothing is interpolated by bash — python reads os.environ.
export DL_HOST="$PGHOST" DL_PORT="$PGPORT" DL_USER="$PGUSER" DL_DB="$PGDB" \
       DL_SCHEMA="$CATALOG_SCHEMA" DL_DATA_PATH="$DATA_PATH" \
       DL_S3_ENDPOINT="${S3_ENDPOINT_URL:-}" DL_S3_KEY="${S3_ACCESS_KEY_ID:-}" \
       DL_S3_SECRET="${S3_SECRET_ACCESS_KEY:-}"

cd "$REPO_ROOT"
set +e
uv run python - <<'PY'
import os, sys, duckdb

host, port = os.environ["DL_HOST"], os.environ["DL_PORT"]
user, db = os.environ["DL_USER"], os.environ["DL_DB"]
schema = os.environ["DL_SCHEMA"]
data_path = os.environ["DL_DATA_PATH"]
# password comes from PGPASSWORD (libpq) — kept out of the conninfo string
conninfo = f"host={host} port={port} user={user} dbname={db}"

con = duckdb.connect()
for ext in ("postgres", "ducklake"):
    con.execute(f"INSTALL {ext}; LOAD {ext};")

# Optional S3 (MinIO) config — harmless for the migration (metadata-only) but
# keeps a consistent session if the catalog references s3 paths.
if os.environ.get("DL_S3_ENDPOINT"):
    ep = os.environ["DL_S3_ENDPOINT"].replace("http://", "").replace("https://", "")
    con.execute("SET s3_endpoint = ?;", [ep])
    if os.environ.get("DL_S3_KEY"):
        con.execute("SET s3_access_key_id = ?;", [os.environ["DL_S3_KEY"]])
    if os.environ.get("DL_S3_SECRET"):
        con.execute("SET s3_secret_access_key = ?;", [os.environ["DL_S3_SECRET"]])
    con.execute("SET s3_url_style = 'path'; SET s3_use_ssl = false;")

# --- read current catalog format + stored data_path (plain postgres attach) --
con.execute(f"ATTACH 'postgres:{conninfo}' AS pg")
def meta(key):
    row = con.execute(
        f"SELECT value FROM pg.{schema}.ducklake_metadata WHERE key = ?", [key]
    ).fetchone()
    return row[0] if row else None

fmt = meta("version")
stored_path = meta("data_path")
print(f"  current format   : {fmt}")
print(f"  stored data_path : {stored_path}")

if fmt == "1.0":
    print("✓ catalog is already format 1.0 — nothing to do.")
    sys.exit(10)  # distinct code: no-op (bash skips the "restart services" note)
if fmt != "0.3":
    print(f"✗ unexpected catalog format {fmt!r} (expected 0.3) — aborting.", file=sys.stderr)
    sys.exit(1)

# --- prune orphaned schema_versions BEFORE migrating (0.3 global-boundary) ---
# Rows older than the newest row at-or-before the oldest live snapshot are
# unreachable; removing them keeps the per-table backfill from exploding.
before = con.execute(
    f"SELECT count(*) FROM pg.{schema}.ducklake_schema_versions"
).fetchone()[0]
con.execute(f"""
    DELETE FROM pg.{schema}.ducklake_schema_versions
    WHERE begin_snapshot < (
      SELECT max(begin_snapshot) FROM pg.{schema}.ducklake_schema_versions
      WHERE begin_snapshot <= (SELECT min(snapshot_id) FROM pg.{schema}.ducklake_snapshot))
""")
after = con.execute(
    f"SELECT count(*) FROM pg.{schema}.ducklake_schema_versions"
).fetchone()[0]
print(f"  pruned schema_versions: {before} -> {after} ({before - after} removed)")
con.execute("DETACH pg")

# --- migrate: attach as ducklake with AUTOMATIC_MIGRATION ---------------------
# OVERRIDE_DATA_PATH only when the local path differs from what's stored — this
# both migrates AND repoints the catalog at your local data dir. A mismatched
# path WITHOUT override runs the migration and rolls it back (leaving bloat), so
# we always override when they differ.
opts = [f"DATA_PATH '{data_path}'", f"METADATA_SCHEMA '{schema}'",
        "AUTOMATIC_MIGRATION true"]
if stored_path and stored_path.rstrip("/") != data_path.rstrip("/"):
    print(f"  repointing data_path: {stored_path} -> {data_path} (OVERRIDE_DATA_PATH)")
    opts.append("OVERRIDE_DATA_PATH true")
con.execute(f"ATTACH 'ducklake:postgres:{conninfo}' AS lake ({', '.join(opts)})")
print("  migration attach OK")

# --- verify -------------------------------------------------------------------
con.execute(f"ATTACH 'postgres:{conninfo}' AS pgv (READ_ONLY)")
new_fmt = con.execute(
    f"SELECT value FROM pgv.{schema}.ducklake_metadata WHERE key='version'"
).fetchone()[0]
sv = con.execute(
    f"SELECT count(*) FROM pgv.{schema}.ducklake_schema_versions"
).fetchone()[0]
ext = con.execute(
    "SELECT extension_version FROM duckdb_extensions() WHERE extension_name='ducklake'"
).fetchone()[0]
print(f"  new format       : {new_fmt}")
print(f"  schema_versions  : {sv} rows")
print(f"  ducklake ext     : {ext}")
if new_fmt != "1.0":
    print("✗ migration did not land on 1.0 — check the output above.", file=sys.stderr)
    sys.exit(1)
print("✓ migrated to 1.0")
PY
rc=$?
set -e

case "$rc" in
  0)
    ok "migrated to 1.0 — restart your local geoapi / processes / windmill workers on the new code"
    echo
    info "note: your local catalog's schema_versions may look bloated right after this"
    info "(per-table backfill). It's harmless locally; running the ducklake_maintenance"
    info "task (or just leaving it) cleans it up. No VACUUM FULL needed at local scale."
    ;;
  10)
    ok "already up to date — nothing changed"
    ;;
  *)
    err "migration failed (exit $rc) — see the output above; your backup (if any) can restore"
    exit "$rc"
    ;;
esac
