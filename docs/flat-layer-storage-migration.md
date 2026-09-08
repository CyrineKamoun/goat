# Flat layer storage: migration runbook

Runbook for moving DuckLake layer tables and PMTiles out of per-owner
directories into a flat layout, on dev then prod.

Rehearsed end-to-end on the local dev-02 lake on 2026-08-16: **11,786 tables
moved in ~60 s**, no data rewritten, no snapshots minted, verified by forced
full reads and a spatial query.

```
before:  DATA_DIR/ducklake/user_<uid>/t_<layer>/*.parquet
         DATA_DIR/tiles/user_<uid>/t_<layer>.pmtiles
after:   DATA_DIR/ducklake/t_<layer>/*.parquet
         DATA_DIR/tiles/t_<layer>.pmtiles
```

Ownership moves nowhere — it was always on `customer.layer.user_id`. The
directory name was a second, derived copy of it, and two tasks recovered
ownership by parsing it back out of a path.

## Conventions

- `goatdev` / `goatprod` are the `kubectl` wrappers (context + `goat`
  namespace baked in). Everything below is written with `goatdev`; for prod
  substitute `goatprod` and run **only** inside the maintenance window.
- The script lives at `scripts/flatten_layer_storage.py` and runs from a
  machine that can reach both the catalog Postgres and the lake filesystem.

## The rule that matters most

**The goatlib rollout and the data migration must happen in the same
window.** Workers running pre-migration code against migrated data compute
`lake.user_<uid>.t_<layer>` for tables that now live in `main`, and *every*
tool run fails. This is not theoretical — it is exactly what happened on
dev-02 on 2026-08-16, because the Windmill workers mount goatlib from a
different worktree than the one being edited.

Before starting, confirm what each worker actually loads:

```bash
goatdev get pods -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.spec.containers[*].image}{"\n"}{end}'
# locally: docker inspect <worker> --format '{{range .Mounts}}{{.Source}} -> {{.Destination}}{{"\n"}}{{end}}'
```

## Why the move is cheap

DuckLake composes a file's location from three stored, relative columns:

```
DATA_PATH + ducklake_schema.path + ducklake_table.path + ducklake_data_file.path
```

So pointing a table at a schema whose `path` is `''` puts it directly under
`DATA_PATH`. The migration is a directory `mv` plus one `UPDATE` per table —
the parquet keeps its name and bytes.

`ALTER TABLE ... SET SCHEMA` is **not implemented** in DuckLake 1.5.4
(`Not implemented Error: T_AlterObjectSchemaStmt`), and `CREATE TABLE AS
SELECT` into a new schema would rewrite every byte in the lake and mint a
snapshot per table. Hence the metadata edit.

That edit bypasses DuckLake's snapshot machinery, which is why the catalog
dump below is mandatory rather than advisory.

## 0. Prerequisites

- [ ] goatlib containing the flat layout **deployed to every worker and to
      geoapi** — see the rule above
- [ ] A maintenance window: core, workers and any other writer stopped
- [ ] `pg_dump` reachable. If not on the host, pass a command:
      `--pg-dump "docker exec -i <pg-container> pg_dump"` (it dumps to stdout,
      so the binary can live anywhere)
- [ ] An archive directory **on the same filesystem as the lake** for the
      default hard-linked archive; otherwise use `--archive-mode=copy` and
      budget the full tree size

## 1. Dry run (read-only, safe any time)

```bash
python scripts/flatten_layer_storage.py --dry-run --skip-row-counts
```

Check three things in the output:

1. **Preflight percentage.** `Preflight: N/M catalog tables (X%) have files
   under <dir>`. This should be high. A low number means the lake being read
   is not the lake the catalog describes — almost always a wrong
   `--data-dir`. The script refuses to `--apply` below 50%.
2. **The directory it printed.** On a host where the catalog's own
   `data_path` differs from the local mount (any container-based setup) the
   script logs the override. Confirm the path is the real lake. Note that
   `DATA_DIR` and `DUCKLAKE_DATA_DIR` are *different settings* and may point
   at different filesystems — on dev/prod both are `/app/data/...`, locally
   they may not be.
3. **Orphan count.** Tables with no `customer.layer` row. Their schema name
   is the only surviving record of who owned them; the inventory preserves it.

## 2. Migrate

```bash
python scripts/flatten_layer_storage.py \
  --apply \
  --archive /path/on/same/fs/pre-flatten-$(date +%F) \
  --skip-row-counts
```

`--apply` refuses to run without `--archive`. In order, the script:

1. Checks the archive fits (per mode) — refuses rather than half-filling a disk
2. Dumps the DuckLake catalog schema, verifies the dump is non-empty and
   contains `ducklake_schema` / `ducklake_table` / `ducklake_data_file`, and
   records the snapshot id it corresponds to
3. Writes `inventory.json` (every table's source, destination, and the orphan
   list) plus a hard-linked copy of the data
4. Points the flat schema's path at the DATA_PATH root
5. Per table: `mv` the directory, then `UPDATE ducklake_table SET schema_id`.
   On failure it rolls the metadata back, moves the files back, and continues
   to the next table
6. Moves PMTiles up a level and removes emptied directories
7. Re-counts moved tables against the pre-move inventory

Useful flags: `--limit N` for a staged rollout, and dropping
`--skip-row-counts` to enable the row-count verification (slower — it counts
every table first).

**A partial run is not a broken state.** Reads resolve from the catalog, so
moved tables answer from `main` and unmoved ones from their old schema. An
interruption is recovered by re-running, not by restoring.

## 3. Restart and verify

```bash
goatdev rollout restart deploy/geoapi     # clears the 1 h schema cache
goatdev rollout restart deploy/<workers>  # drops pinned DuckLake snapshots
```

geoapi caches layer→schema for an hour and pins a DuckLake snapshot, so a
running pod keeps resolving pre-migration paths until it refreshes.

Then check, in this order — each isolates a different half:

- [ ] **An existing layer renders on the map.** Proves read resolution and
      flat PMTiles. Blank tiles but working features ⇒ PMTiles side; neither
      ⇒ schema resolution.
- [ ] **Upload a new layer.** Should create `lake.main.t_<id>` with files at
      `DATA_DIR/ducklake/t_<id>/` and a flat `t_<id>.pmtiles`.
- [ ] **Run one analytics tool** on an existing layer. Exercises
      `export_layer_to_parquet` → `resolve_layer_table_path`.
- [ ] **Delete a layer.** Exercises resolution plus the legacy PMTiles sweep.

Verify data with a query that **reads files**, not `count(*)` alone:

```sql
SELECT count(*) FROM (SELECT * FROM lake.main."t_<id>");
```

A bare `count(*)` is answered from `ducklake_data_file.record_count` without
opening a parquet, so it stays green on a table whose data was left behind.
This caught a real bug during the rehearsal.

## 4. Rollback

Only needed if the metadata edit went wrong; the files themselves are moved,
never rewritten.

```bash
# 1. restore the catalog
psql "$URI" -c 'DROP SCHEMA ducklake CASCADE'
psql "$URI" -f <archive>/ducklake_catalog.sql

# 2. move the directories back, using inventory.json (src/dst per table)
python - <<'PY'
import json, shutil, pathlib
inv = json.load(open("<archive>/inventory.json"))
for t in inv["tables"]:
    dst, src = pathlib.Path(t["dst"]), pathlib.Path(t["src"])
    if dst.exists() and not src.exists():
        src.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(dst), str(src))
PY

# 3. restart geoapi and the workers again
```

The hard-linked archive is also a complete copy of the pre-migration tree,
so the files remain reachable through it even if a move went astray.

## Expected residuals

None of these are failures:

- **Tables whose files are already gone** report `source directory missing`
  and are skipped. On dev-02 that was 260 of 12,046.
- **Files with no catalog row** are left where they are — the script iterates
  the catalog, so it never sees them.
- **Empty layers** are skipped by `sync_pmtiles` (`Layer has no features`).
  An empty layer is a normal product state: the create flow makes them and a
  filter can match nothing.
- **Old `user_*` directories** may survive holding only unreferenced files.
  Safe to remove once you have confirmed nothing in the catalog names them.

## After the migration

`sync_pmtiles` and `rebuild_edited_pmtiles` both take ownership from a
`customer.layer` join rather than from the schema name, and no longer filter
on `schema_name LIKE 'user_%'` — without that removal they would find zero
layers post-migration. One behaviour change to be aware of: the join is
inner, so DuckLake tables with no layer row drop out of the enumeration
entirely.
