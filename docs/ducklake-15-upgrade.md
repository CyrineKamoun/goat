# DuckLake catalog upgrade: DuckDB 1.4.4 → 1.5.4 (catalog format 0.3 → 1.0)

Runbook for upgrading the DuckLake stack on Kubernetes (dev, then prod).
Rehearsed and executed end-to-end on a copy of the dev catalog (11,957 tables,
481 snapshots) on 2026-07-14.

## Conventions

- `goatdev` / `goatprod` are the `kubectl` wrappers for the dev / prod cluster
  (correct context + `goat` namespace already baked in). Everything below is
  written with `goatdev`; for prod, substitute `goatprod` and run **only**
  inside the prod maintenance window.
- `<...>` are cluster-specific names to fill in (deployment/pod names may drift).
  Discover them first:
  ```bash
  goatdev get deploy            # geoapi, processes, windmill-worker-* names
  goatdev get pods | grep -E 'postgres|pgbouncer|db'   # catalog Postgres pod
  ```
- Catalog location: main Postgres, database `goat`, schema `ducklake`, user `rds`.
- SQL blocks below are run in your local SQL client (DBeaver) against the catalog
  DB. **Point the connection at the Postgres primary, not `goat-pgbouncer`** —
  pgbouncer transaction-pooling breaks session-scoped operations (notably
  `VACUUM FULL`). If the primary is only reachable in-cluster, port-forward it:
  ```bash
  goatdev port-forward <pg-pod> 5432:5432   # then connect DBeaver to localhost:5432
  ```
- Only backup/restore uses a CLI (`pg_dump`/`pg_restore` via `goatdev exec`) —
  DBeaver's dump tooling isn't reliable for a custom-format schema-only dump.

## What happens during the upgrade

Attaching a 0.3 catalog with DuckDB 1.5.x and `AUTOMATIC_MIGRATION true` rewrites
the `ducklake_*` metadata tables in Postgres to format 1.0. Data files (parquet)
are never touched. Key facts:

- **One-way door.** DuckDB 1.4.x cannot attach a migrated catalog. Everything
  that attaches DuckLake (geoapi, processes, Windmill workers) must move to
  1.5.4 in the **same cutover** — see the worker warning in Step 4.
- **The migration expands `ducklake_schema_versions` to per-table granularity**
  (`begin_snapshot, schema_version, table_id`) and backfills it for every
  historical row × every table alive at that snapshot. On a catalog polluted
  with orphaned history rows this explodes: dev-scale measured **80.6M rows /
  4 GB / 40 s** unpruned vs **5.78M rows / 289 MB / 3.2 s** after pruning.
  Prune first (Step 3) and check disk headroom on the catalog Postgres.
- Orphaned history exists because `ducklake_expire_snapshots` never prunes
  `ducklake_schema_versions` (verified in 0.3 and 1.0). The nightly maintenance
  task gains a prune step after the upgrade so it does not rebuild.
- Post-migration behavior changes measured at dev scale: plain attach drops to
  ~0.02 s (metadata now loads lazily per table; on 1.4.4 attach reloaded the
  full catalog). Full-catalog enumeration (`duckdb_tables()`, `SHOW ALL TABLES`)
  costs ~4 ms/table = ~45 s on 12k tables — upstream issue
  [duckdb/ducklake#1269](https://github.com/duckdb/ducklake/issues/1269). Avoid
  code paths that enumerate all tables. Pinned `SNAPSHOT_VERSION` attach works
  unchanged.
- Every function the nightly `ducklake_maintenance` / `ducklake_compact` tasks
  call works unchanged on 1.5.4 (`ducklake_expire_snapshots`,
  `ducklake_cleanup_old_files`, `ducklake_delete_orphaned_files`,
  `ducklake_snapshots`, `ducklake_table_info`, `ducklake_merge_adjacent_files`,
  `ducklake_flush_inlined_data`).

## Cutover order at a glance

The one invariant: **nothing on the old extension may attach the catalog after
migration, and nothing may write during it.** geoapi/processes attach at
startup, so they stay at 0 replicas until the migration + vacuum are done.
Windmill workers attach only when a job runs, so a paused-schedule worker is a
safe place to run the one-shot migration.

```
pause schedules → scale geoapi/processes to 0 → backup → prune (0.3)
  → migrate (one pod, new image) → VACUUM FULL (services still down)
  → roll out 1.5.4 images (geoapi, processes, workers) → scale up
  → unpause schedules → verify
```

## Step 0 — Preflight

Prerequisite: the 1.5.4 images must already be **built and pushed** (see Step 4)
before you start the window.

Catalog stats (DBeaver) — format, snapshots alive, schema_versions total vs
reachable:

```sql
SELECT
  (SELECT value FROM ducklake.ducklake_metadata WHERE key='version')      AS format,
  (SELECT count(*) FROM ducklake.ducklake_snapshot)                       AS snapshots,
  (SELECT count(*) FROM ducklake.ducklake_schema_versions)                AS sv_rows,
  (SELECT count(*) FROM ducklake.ducklake_schema_versions sv
     JOIN ducklake.ducklake_snapshot s ON s.snapshot_id = sv.begin_snapshot)
                                                                          AS sv_reachable;
```

Capture the **constraint + index baseline** now — Step 6 diffs against it to
prove the migration dropped nothing. Save the output:

```sql
SELECT 'constraint' AS kind, conrelid::regclass::text AS tbl,
       conname AS name, pg_get_constraintdef(oid) AS def
FROM pg_constraint WHERE connamespace = 'ducklake'::regnamespace
UNION ALL
SELECT 'index', tablename, indexname, indexdef
FROM pg_indexes WHERE schemaname = 'ducklake'
ORDER BY 1, 2, 3;
```

A healthy catalog has 5 primary keys and no foreign keys / secondary indexes
(DuckLake manages referential integrity itself). Expected PKs:
`ducklake_snapshot`, `ducklake_snapshot_changes`, `ducklake_data_file`,
`ducklake_delete_file`, `ducklake_schema`. **If any are missing here, fix that
BEFORE migrating** — a catalog without `ducklake_snapshot_pkey` allows duplicate
`snapshot_id`s (the "multiple snapshots" corruption); hand-restored catalogs
have shipped without PKs before.

- Expect `format = 0.3`. If `sv_rows >> snapshots`, pruning (Step 3) is essential.
- Rough disk estimate for the migration: `snapshots × live tables × 40 bytes`,
  plus the same again for indexes/WAL. Check free space on the Postgres PVC.

Then quiesce writers:

```bash
# 1. Pause the nightly DuckLake schedules in Windmill (UI: f/goat/tasks/
#    ducklake_maintenance + ducklake_compact → disable), or pick a window
#    far from 04:00 UTC.
# 2. Scale down everything that attaches at startup.
goatdev scale deploy/<geoapi> --replicas=0
goatdev scale deploy/<processes> --replicas=0
goatdev rollout status deploy/<geoapi> --timeout=120s
```

The migration (Step 5) must run from a **1.5.4** pod — the old 1.4.4 extension
cannot perform the 0.3→1.0 migration at all. So the executor is *not* the
running old worker; it's a pod on the new image. Two ways, both requiring the
1.5.4 worker image from Step 4 to be in the registry already:

- **(a) Roll one tools-worker to the new image** (what Step 5 shows): with
  schedules paused it comes up idle, and you exec the migration inside it. All
  replicas of that Deployment roll — fine, they're idle.
- **(b) One-off Job from the new image** (leaves the worker Deployments
  untouched until Step 6). Cleaner separation; use if you'd rather not touch the
  worker Deployment before the migration succeeds.

Either way there is no "old worker migrates" path — the image must be rebuilt
first.

## Step 1 — Run normal maintenance once

Trigger the Windmill task `f/goat/tasks/ducklake_maintenance` (defaults) once so
the snapshot set is already minimal before pruning. Wait for it to finish, then
confirm no DuckLake job is still running before continuing.

## Step 2 — Backup the catalog schema

```bash
goatdev exec <pg-pod> -- pg_dump -U rds -d goat -n ducklake -Fc \
  -f /tmp/ducklake_pre15_$(date +%Y%m%d).dump
# copy the dump off the pod
goatdev cp <pg-pod>:/tmp/ducklake_pre15_$(date +%Y%m%d).dump \
  ./ducklake_pre15_$(date +%Y%m%d).dump
```

Restore path if anything goes wrong (see Rollback):
`DROP SCHEMA ducklake CASCADE;` then `pg_restore -d goat <dump>`.

## Step 3 — Prune orphaned schema_versions rows (pre-migration, 0.3 format)

A row applies "from its `begin_snapshot` until the next row", so every row
strictly older than the newest row at-or-before the oldest live snapshot is
unreachable by any query. Keep that boundary row.

```sql
BEGIN;
DELETE FROM ducklake.ducklake_schema_versions
WHERE begin_snapshot < (
  SELECT max(begin_snapshot) FROM ducklake.ducklake_schema_versions
  WHERE begin_snapshot <= (SELECT min(snapshot_id) FROM ducklake.ducklake_snapshot));
-- sanity: remaining ≈ snapshots alive (± a few); if it deleted ~everything, ROLLBACK
SELECT count(*) FROM ducklake.ducklake_schema_versions;
COMMIT;
```

Dev rehearsal numbers: deleted 12,422 of 12,909; kept 487.

## Step 4 — Build the 1.5.4 images

Code change (already in the repo): `duckdb>=1.5.4` in
`packages/python/goatlib/pyproject.toml`, `apps/geoapi/pyproject.toml`,
`apps/processes/pyproject.toml` (+ regenerated `uv.lock`). The ducklake
extension installs from the default repo (`INSTALL ducklake`) and rides the
DuckDB version — no separate pin.

Build and push the images CI produces from this commit **before** the window:
`geoapi`, `processes`, and — critically — the Windmill worker images.

**Windmill workers are the sharp edge of the one-way door.** The `tools` and
`workflows` workers use the `windmill-worker-tools` image, and tool scripts run
`# py311` — i.e. in the image's baked-in `/venv`, NOT a per-script PyPI install.
So bumping the pyproject does nothing for them until the **worker image is
rebuilt** with duckdb ≥ 1.5.4 and redeployed. Until then, every tool job that
touches DuckLake — including interactive **layer import** — fails against a 1.0
catalog with:

> `NotImplementedException: Only DuckLake versions 0.1, 0.2, 0.3-dev1 and 0.3 are supported`

(that's the old 1.4.x extension refusing the migrated catalog). The worker image
rollout is therefore part of this cutover (Step 6), not a follow-up. Confirm the
built image is correct before the window:

```bash
docker run --rm --entrypoint /venv/bin/python \
  ghcr.io/plan4better/goat/windmill-worker-tools:<new-tag> \
  -c "import duckdb; print(duckdb.__version__)"     # → 1.5.4
```

## Step 5 — Controlled migration (one pod)

Services never pass `AUTOMATIC_MIGRATION` (keep it that way — a stray new pod
must never migrate the catalog under running old pods). Migrate with a single
explicit attach, run from the idle tools-worker (it has duckdb 1.5.4 once its
image is updated, the data PVC mounted at `/app/data/ducklake`, and DB access).

First update just that worker to the new image, keeping it idle (schedules
paused):

```bash
goatdev set image deploy/<windmill-worker-tools> <container>=ghcr.io/plan4better/goat/windmill-worker-tools:<new-tag>
goatdev rollout status deploy/<windmill-worker-tools> --timeout=180s
WPOD=$(goatdev get pod -l <worker-tools-selector> -o name | head -1)
```

Write the migration script into the pod and run it:

```bash
cat > /tmp/migrate.py <<'PY'
import duckdb
con = duckdb.connect()
con.execute("INSTALL ducklake; INSTALL postgres; LOAD ducklake; LOAD postgres;")
# Attach the Postgres PRIMARY directly, NOT goat-pgbouncer — the migration is a
# multi-statement catalog rewrite and transaction pooling breaks it.
con.execute(
    "ATTACH 'ducklake:postgres:dbname=goat host=<pg-primary-host> port=5432 "
    "user=rds password=<POSTGRES_PASSWORD>' AS lake "
    "(DATA_PATH '/app/data/ducklake', META_SCHEMA 'ducklake', AUTOMATIC_MIGRATION true)"
)
print("migrated; ext:", con.execute(
    "SELECT extension_version FROM duckdb_extensions() "
    "WHERE extension_name='ducklake'").fetchone())
PY
goatdev cp /tmp/migrate.py ${WPOD#pod/}:/tmp/migrate.py
goatdev exec $WPOD -- /venv/bin/python /tmp/migrate.py
```

Expected: completes in seconds (pruned). The process exits and closes its
connection, so it leaves no lingering catalog session.

**DATA_PATH gotcha:** if `DATA_PATH` differs from the path stored in the catalog
(`SELECT value FROM ducklake.ducklake_metadata WHERE key='data_path'` — on
dev/prod it is `/app/data/ducklake`), add `OVERRIDE_DATA_PATH true`. A failed
attach still **runs the full migration and rolls it back**, leaving the format
at 0.3 but bloating `ducklake_schema_versions` with millions of dead tuples — if
that happens, do the Step 6 `VACUUM FULL` and re-attach with the right path.

Verify (DBeaver):

```sql
SELECT value FROM ducklake.ducklake_metadata WHERE key='version';  -- → 1.0
SELECT count(*) FROM ducklake.ducklake_schema_versions;            -- ≈ snapshots × tables
```

**Integrity check — constraints/indexes survived the rewrite.** Re-run the
Step 0 baseline query and confirm the result is a **superset** of what you
captured (the migration must not drop any PK/index). In particular all 5 primary
keys must still be present:

```sql
SELECT conname FROM pg_constraint
WHERE connamespace = 'ducklake'::regnamespace AND contype = 'p'
ORDER BY conname;
-- expect exactly:
--   ducklake_data_file_pkey, ducklake_delete_file_pkey, ducklake_schema_pkey,
--   ducklake_snapshot_changes_pkey, ducklake_snapshot_pkey
```

(Verified on the local rehearsal: pre- and post-migration inventories were
identical — 5 PKs, 0 FKs, 0 secondary indexes.)

## Step 6 — Reclaim space, then roll out and scale up

While geoapi/processes are still at 0 replicas (so nothing holds old row
versions), reclaim the dead space the migration + prune left behind — in DBeaver
(connected to the primary):

```sql
VACUUM FULL ducklake.ducklake_schema_versions;
SELECT pg_size_pretty(pg_total_relation_size('ducklake.ducklake_schema_versions'));
-- dev: 288 MB → <1 MB
```

If `VACUUM FULL` reclaims nothing, a DuckLake attach is sitting **idle in
transaction** and pinning old row versions. With services scaled to 0 there
should be none; otherwise terminate the holders (they reconnect automatically),
then re-run the `VACUUM FULL`:

```sql
SELECT pg_terminate_backend(pid) FROM pg_stat_activity
WHERE datname='goat' AND state='idle in transaction' AND pid <> pg_backend_pid();
```

Then roll everything onto the 1.5.4 images and bring it back up:

```bash
goatdev set image deploy/<geoapi>    <container>=ghcr.io/plan4better/goat/geoapi:<new-tag>
goatdev set image deploy/<processes> <container>=ghcr.io/plan4better/goat/processes:<new-tag>
goatdev set image deploy/<windmill-worker-workflows> <container>=ghcr.io/plan4better/goat/windmill-worker-tools:<new-tag>
# tools-worker image was already updated in Step 5

goatdev scale deploy/<geoapi> --replicas=<N>
goatdev scale deploy/<processes> --replicas=<N>
goatdev rollout status deploy/<geoapi> --timeout=180s
```

Unpause the Windmill schedules. Smoke test:

- tiles + feature `items` on an existing layer (geoapi),
- one **layer import** (exercises the worker write path against the 1.0 catalog),
- one analytics tool run,
- `ducklake_maintenance` with `dry_run=true`.

```bash
goatdev exec $WPOD -- /venv/bin/python -c "import duckdb; print(duckdb.__version__)"  # → 1.5.4
```

## Step 7 — Post-upgrade: keep schema_versions pruned

`ducklake_expire_snapshots` still leaves `ducklake_schema_versions` rows behind
— now at per-table scale — so the nightly maintenance task prunes them (1.0
format variant: per-table ranges, keep each table's newest row at-or-before the
oldest live snapshot):

```sql
DELETE FROM ducklake.ducklake_schema_versions sv
WHERE sv.begin_snapshot < (
  SELECT max(sv2.begin_snapshot) FROM ducklake.ducklake_schema_versions sv2
  WHERE sv2.table_id = sv.table_id
    AND sv2.begin_snapshot <= (SELECT min(snapshot_id) FROM ducklake.ducklake_snapshot));
```

This runs as part of `goatlib/tasks/ducklake_maintenance.py` (added with this
upgrade) right after `ducklake_expire_snapshots`. Validated end-to-end on the
migrated catalog: a full run expired 481 snapshots, pruned 5,764,265 rows, and
five fingerprinted layers (row counts + checksums) were bit-identical
afterwards; geoapi kept serving throughout.

The one-time `VACUUM FULL` in Step 6 covers the migration bloat. Steady-state
nightly runs delete only small row counts, which autovacuum reclaims — **no
recurring `VACUUM FULL`** (it takes an ACCESS EXCLUSIVE lock and would stall
every DuckLake attach). If you want a safety net, alert on
`pg_total_relation_size('ducklake.ducklake_schema_versions')` in Grafana rather
than compacting on a schedule.

## Code changes required for 1.5.x (shipped with this upgrade)

On 1.5.x, table metadata loads **lazily per table** (one catalog query each).
Anything that enumerates the whole catalog therefore costs ~4 ms × table count
(~45 s at 12k tables, [ducklake#1269](https://github.com/duckdb/ducklake/issues/1269))
— and on 1.4.4 those same calls were free because attach had already
bulk-loaded everything. Three classes of code had to change:

1. **Pool warm query** (`goatlib/storage/ducklake.py`): was
   `duckdb_tables()` (full enumeration, ran on every pool build/rebuild —
   geoapi cold start took ~2 min); now `duckdb_schemas()` (0.3 s, schema-level
   only). geoapi cold start: **1.9 s**.
2. **Layer-ID → schema resolver** (`goatlib/utils/layer.py`): was
   `information_schema.tables WHERE table_name = ?` (full catalog scan on
   every uncached layer — request timeouts); now one indexed join on the
   catalog's own Postgres tables via `ATTACH ... AS pgmeta` (~30 ms).
   `DuckLakeManagerProtocol` gained `postgres_uri` / `catalog_schema`.
3. **Per-table column/existence lookups** (geoapi layer/feature-write/download,
   processes workflows, goatlib tools layer_export/layer_delete(_multi)/
   project_export, analysis aggregate_*/accessibility/buffer): all
   `information_schema.columns/tables` queries → `DESCRIBE lake."sch"."tbl"`
   (loads only that table). Note: **unqualified** information_schema spans
   every attached catalog, so even queries "against local views" paid the full
   lake scan. network_processor now uses `SHOW TABLES` (current schema only —
   also fixes a latent hazard where `table_schema='main'` matched lake.main).

**rowid caveat (feature IDs):** on plain DuckDB tables, 1.5.x moves rows to
NEW rowids on full-row rewrites (e.g. geometry UPDATE). **DuckLake tables are
unaffected** — logical row ids stay stable (verified on the migrated catalog),
so the rowid+1 feature-ID architecture is safe. Tests that fake the lake with
a plain `ATTACH ':memory:' AS lake` inherit the unstable behavior — fake it
with a file-backed `ducklake:` attach instead (see
`apps/geoapi/tests/integration/test_computed_columns.py`).

## Snapshot-pin machinery: keep it

The geoapi pinned-snapshot read pool stays on 1.5.x. Measured on the migrated
catalog under churn (52 schema-changing snapshots / 55 s): **pinned** tile p50
28 ms / items p50 18 ms vs **unpinned** tile p50 97 ms / items p50 641 ms.
1.5.x invalidation is finer-grained than 1.4.4 (no 30 s full-catalog reloads),
but an unpinned connection still re-syncs metadata on every request under churn.
Kill-switch `DUCKLAKE_PIN_SNAPSHOT=false` exists but should stay `true`.

## Rollback

Before Step 5 completes: nothing to roll back (prune only removes unreachable
rows; the backup covers even that). After migration: restore the schema dump
(Step 2). Drop the schema in DBeaver:

```sql
DROP SCHEMA ducklake CASCADE;
```

then restore the dump via CLI:

```bash
goatdev cp ./ducklake_pre15_<date>.dump <pg-pod>:/tmp/restore.dump
goatdev exec <pg-pod> -- pg_restore -U rds -d goat /tmp/restore.dump
```

— and roll the images back to the previous (1.4.4) tag. Data files were never
modified, so the 0.3 catalog + parquet set is exactly the pre-upgrade state.
Writes made after the migration are lost with the restore; don't reopen writes
(scale up / unpause schedules) until you have committed to going forward.

## Local rehearsal reference

The dev-mirror rehearsal used `goat-db18` (restored dev catalog) instead of a
pod. Equivalents: connect DBeaver to `localhost:5432` (goat-db18 exposes 5432),
or `docker exec goat-db18 psql -U rds -d goat` for the SQL steps;
`docker exec -u root <worker> sh -c 'VIRTUAL_ENV=/venv uv pip install
duckdb==1.5.4'` to upgrade a running local worker without an image rebuild
(ephemeral — no pip in the image, `uv` is at `/usr/local/bin`, venv is
root-owned). Migration attach used `DATA_PATH '/shared/majk/goat/ducklake'` with
`OVERRIDE_DATA_PATH true` because the local data path differs from the
`/app/data/ducklake` stored in the catalog.
